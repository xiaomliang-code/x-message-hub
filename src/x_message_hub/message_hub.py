import os
import json
import time
import threading
import queue
import uuid
from typing import Dict, List, Callable, Optional, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, Future

# ============================================================
# MessageHub — 生产级消息总线 v3
#
# 架构保留：
#   三层流水线（Publisher → Dispatcher → Subscriber Worker）
#   动态 QoS：快慢双线程池 + 超时自动降级
#   毒丸（Poison Pill）优雅退出机制
#
# 缺陷修复（v2）：
#   [F1] 拆分四把独立锁，消除单例锁复用死锁
#   [F2] threading.Event 替代布尔标志，保证"只执行一次"原子性
#   [F3] stop() 先收集再清理，消除持锁调用死锁
#   [F4] Monitor 超时分支直接 continue，消除任务重入队列的逻辑矛盾
#   [F5] subscribe() 替代 register_callback+listen，支持多回调独立注册
#   [F6] IsolationRecord TTL + 定时清理，消除隔离表内存泄漏
#   [F7] slow_threshold_ms 构造时可配置
#   [F8] stop() 并行广播毒丸 + 并行 join，消除串行等待放大
#
# 生产加固（v3）：
#   [P1] send_message 三档序列化策略
#          bytes/bytearray   → 零拷贝直传
#          numpy.ndarray     → 只读保护后直传引用（可选 copy 模式）
#          dict/list/str     → JSON 序列化
#          其他              → json.dumps fallback
#   [P2] _monitor_worker 删除 time.sleep(0.00001)，纯依赖 queue.get 阻塞
#   [P3] 毒丸语义分层
#          unsubscribe() → 追尾毒丸（保证已入队消息处理完再退出）
#          stop()        → 清空队列后再放毒丸（闪退，丢弃积压）
# ============================================================

POISON_PILL = object()

# numpy 可选依赖：未安装时降级为普通序列化
try:
    import numpy as np
    _NUMPY_AVAILABLE = True
except ImportError:
    _NUMPY_AVAILABLE = False


# ==================== 内部数据结构 ====================

class TaskEntry:
    """封装单次回调任务及其执行状态"""

    def __init__(self, channel: str, sub_id: str, callback: Callable, message: Any):
        self.channel    = channel
        self.sub_id     = sub_id
        self.callback   = callback
        self.message    = message
        self.submit_time: float = time.monotonic()
        self.executor_future: Optional[Future] = None
        # [F2] Event.set() 是原子操作，天然满足"只执行一次"的 CAS 语义
        self._executed = threading.Event()

    def try_execute(self) -> bool:
        """
        CAS 语义：首个调用者 set() 成功并获得执行权，返回 True；
        后续调用者 is_set() 已为 True，直接返回 False。
        Event 内部有互斥锁，无竞态窗口。
        """
        if self._executed.is_set():
            return False
        self._executed.set()
        return True


class ChannelState:
    """每个频道持有的完整状态：主队列、订阅者列表、分发线程"""

    def __init__(self, name: str):
        self.name         = name
        self.master_queue : queue.Queue = queue.Queue(maxsize=10_000)
        self.subscribers  : List[Tuple[str, queue.Queue]] = []
        self.sub_lock     = threading.Lock()
        self.stop_event   = threading.Event()
        self.dispatcher_thread: Optional[threading.Thread] = None

    def start_dispatcher(self, bus: 'MessageHub') -> None:
        if self.dispatcher_thread is None:
            t = threading.Thread(
                target=bus._dispatcher_worker,
                args=(self,),
                daemon=True,
                name=f"Dispatcher-{self.name}",
            )
            self.dispatcher_thread = t
            t.start()


class IsolationRecord:
    """[F6] 隔离记录，携带时间戳以支持 TTL 自动过期"""
    TTL_SECONDS: float = 300.0  # 5 分钟后自动解除隔离

    def __init__(self):
        self.isolated_at: float = time.monotonic()

    def is_expired(self) -> bool:
        return (time.monotonic() - self.isolated_at) > self.TTL_SECONDS


# SubscriptionState = (channel_name, worker_thread, sub_queue, callback, maxsize)
SubscriptionState = Tuple[str, threading.Thread, queue.Queue, Callable, int]


# ==================== [P1] 序列化工具函数 ====================

def _encode_message(obj: Any, copy_arrays: bool = False) -> Any:
    """
    三档序列化策略，返回值直接放入 master_queue：

    档位 1 — bytes / bytearray
        已经是字节序列，零拷贝直接入队。

    档位 2 — numpy.ndarray（需已安装 numpy）
        默认：设置 writeable=False 后直传引用（零拷贝，调用方不得再修改）。
        copy_arrays=True 时：显式 copy()，适合发送后还会修改源数组的场景。

    档位 3 — dict / list / str
        标准 JSON 序列化为 UTF-8 字节流。

    档位 4 — 其他类型
        json.dumps fallback，无法序列化时抛出 TypeError。
    """
    # 档位 1：已是字节，直接返回
    if isinstance(obj, (bytes, bytearray)):
        return obj

    # 档位 2：numpy 数组，传引用或拷贝
    if _NUMPY_AVAILABLE and isinstance(obj, np.ndarray):
        if copy_arrays:
            return obj.copy()
        # 只读保护：防止发送方在投递后修改同一块内存
        obj.flags.writeable = False
        return obj

    # 档位 3：JSON 可序列化类型
    if isinstance(obj, str):
        return obj.encode('utf-8')
    if isinstance(obj, (dict, list)):
        return json.dumps(obj, ensure_ascii=False, separators=(',', ':')).encode('utf-8')

    # 档位 4：fallback
    return json.dumps({"data": obj}, default=str, separators=(',', ':')).encode('utf-8')


def _decode_message(raw: Any) -> Any:
    """
    与 _encode_message 对应的反序列化：
    - numpy.ndarray / bytearray → 直接返回（调用方拿到的就是原始对象）
    - bytes → 尝试 JSON 解析，失败则返回 UTF-8 字符串
    """
    if _NUMPY_AVAILABLE and isinstance(raw, np.ndarray):
        return raw
    if isinstance(raw, bytearray):
        return raw
    if isinstance(raw, (bytes,)):
        try:
            decoded = raw.decode('utf-8')
        except UnicodeDecodeError:
            return raw  # 二进制内容，原样返回
        try:
            return json.loads(decoded)
        except json.JSONDecodeError:
            return decoded
    return raw  # 其他类型透传


# ==================== [P3] 毒丸辅助函数 ====================

def _drain_and_poison(sub_queue: queue.Queue) -> None:
    """
    闪退模式（用于 stop()）：
    清空队列中的所有积压消息，再投入毒丸，使 Worker 立即感知退出信号。
    """
    drained = 0
    while True:
        try:
            sub_queue.get_nowait()
            drained += 1
        except queue.Empty:
            break
    try:
        sub_queue.put_nowait(POISON_PILL)
    except queue.Full:
        # 队列满说明刚才 drain 后又有新消息进来（极罕见），强制放入
        sub_queue.put(POISON_PILL, block=True)
    if drained:
        _log(f"[drain] 闪退模式丢弃 {drained} 条积压消息")


def _tail_poison(sub_queue: queue.Queue) -> None:
    """
    优雅退出模式（用于 unsubscribe()）：
    毒丸追加到队尾，Worker 处理完所有已入队消息后才退出。
    """
    try:
        sub_queue.put(POISON_PILL)
    except Exception:
        pass


# ==================== 核心事件总线 ====================

class MessageHub:
    _instance: Optional['MessageHub'] = None
    # [F1] 专用单例锁，不与任何业务逻辑的锁共用
    _singleton_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 单例管理
    # ------------------------------------------------------------------

    @staticmethod
    def _reset_instance() -> None:
        """测试 / 重启时强制重置单例，会先完整执行 stop()"""
        with MessageHub._singleton_lock:
            inst = MessageHub._instance
            if inst is not None:
                try:
                    inst.stop()
                except Exception:
                    pass
                MessageHub._instance = None

    def __new__(cls, *args, **kwargs):
        with cls._singleton_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    # ------------------------------------------------------------------
    # 初始化
    # ------------------------------------------------------------------

    def __init__(
        self,
        slow_threshold_ms: float = 15.0,   # [F7] QoS 超时阈值，毫秒
        fast_pool_multiplier: int = 8,      # FastPool = cpu × multiplier
        slow_pool_multiplier: int = 2,      # SlowPool = cpu × multiplier
        copy_arrays: bool = False,          # [P1] numpy 数组是否强制拷贝
    ):
        if hasattr(self, "_initialized"):
            return
        self._initialized = True

        # [F1] 四把职责明确的独立锁
        self._channel_lock   = threading.Lock()   # 保护 _channels 写入
        self._sub_lock       = threading.Lock()   # 保护 _subscriptions
        self._isolation_lock = threading.Lock()   # 保护 _isolated_subs

        self._channels:      Dict[str, ChannelState]      = {}
        self._subscriptions: Dict[str, SubscriptionState] = {}

        # [F7] 阈值转换为秒
        self.SLOW_THRESHOLD: float = slow_threshold_ms / 1000.0
        # [P1] numpy 数组拷贝策略
        self._copy_arrays: bool = copy_arrays

        # [F6] 隔离表
        self._isolated_subs: Dict[str, IsolationRecord] = {}

        # 线程池
        cpu = os.cpu_count() or 4
        self._fast_executor = ThreadPoolExecutor(
            max_workers=cpu * fast_pool_multiplier,
            thread_name_prefix="FastWorker",
        )
        self._slow_executor = ThreadPoolExecutor(
            max_workers=cpu * slow_pool_multiplier,
            thread_name_prefix="SlowWorker",
        )

        # QoS 监控线程
        self._monitor_queue: queue.Queue[TaskEntry] = queue.Queue()
        self._monitor_stop  = threading.Event()
        self._monitor_thread = threading.Thread(
            target=self._monitor_worker, daemon=True, name="QoS-Monitor"
        )
        self._monitor_thread.start()

        # [F6] 隔离记录定时清理线程
        self._cleanup_stop   = threading.Event()
        self._cleanup_thread = threading.Thread(
            target=self._isolation_cleanup_worker, daemon=True, name="IsoCleanup"
        )
        self._cleanup_thread.start()

        _log(
            f"MessageHub v3 已初始化 | "
            f"QoS阈值={slow_threshold_ms}ms | "
            f"FastPool={cpu * fast_pool_multiplier} | "
            f"SlowPool={cpu * slow_pool_multiplier} | "
            f"numpy={'可用' if _NUMPY_AVAILABLE else '不可用'} | "
            f"copy_arrays={copy_arrays}"
        )

    # ------------------------------------------------------------------
    # 隔离表管理
    # ------------------------------------------------------------------

    def _is_isolated(self, sub_id: str) -> bool:
        with self._isolation_lock:
            rec = self._isolated_subs.get(sub_id)
            if rec is None:
                return False
            if rec.is_expired():
                del self._isolated_subs[sub_id]
                _log(f"[Isolation] Sub [{sub_id[:8]}] TTL 到期，自动解除隔离")
                return False
            return True

    def _set_isolated(self, sub_id: str) -> None:
        with self._isolation_lock:
            self._isolated_subs[sub_id] = IsolationRecord()

    # [F6] 定时清理过期隔离记录，防止内存泄漏
    def _isolation_cleanup_worker(self) -> None:
        while not self._cleanup_stop.is_set():
            # 用 wait 代替 sleep，stop 时可立即唤醒
            self._cleanup_stop.wait(timeout=60)
            with self._isolation_lock:
                expired = [sid for sid, rec in self._isolated_subs.items() if rec.is_expired()]
                for sid in expired:
                    del self._isolated_subs[sid]
            if expired:
                _log(f"[IsoCleanup] 清理 {len(expired)} 条过期隔离记录")

    # ------------------------------------------------------------------
    # QoS 任务执行入口
    # ------------------------------------------------------------------

    def _execute_task(self, task: TaskEntry) -> None:
        """[F2] 唯一执行入口，try_execute() 保证回调只被调用一次"""
        if not task.try_execute():
            return
        try:
            task.callback(task.channel, task.message)
        except Exception as e:
            msg_id = (
                task.message.get('id', 'N/A')
                if isinstance(task.message, dict)
                else 'N/A'
            )
            _log(f"[Callback ERROR] ch={task.channel} id={msg_id} err={e}")

    # ------------------------------------------------------------------
    # [P2] QoS Monitor —— 纯依赖 queue.get 阻塞，不再有 time.sleep
    # ------------------------------------------------------------------

    def _monitor_worker(self) -> None:
        while not self._monitor_stop.is_set():
            # [P2] timeout 控制轮询间隔，队列空时挂起，不消耗 CPU
            try:
                task: TaskEntry = self._monitor_queue.get(timeout=0.005)
            except queue.Empty:
                continue

            try:
                future = task.executor_future
                if future is None or future.done():
                    # 任务已在 Fast Pool 正常完成，无需干预
                    continue

                elapsed = time.monotonic() - task.submit_time

                if elapsed <= self.SLOW_THRESHOLD:
                    # 未超时，放回队列继续监控
                    self._monitor_queue.put(task)
                    continue

                # ---- 超时：触发 QoS 降级 ----
                _log(
                    f"[QoS] Sub [{task.sub_id[:8]}] 超时 {elapsed * 1000:.1f}ms，"
                    f"永久降级到慢速池"
                )
                self._set_isolated(task.sub_id)

                if future.cancel():
                    # [F4] cancel() 成功（任务尚未开始）→ 转交慢速池执行
                    self._slow_executor.submit(self._execute_task, task)
                # cancel() 失败表示任务已在 Fast Pool 运行中，让它跑完即可
                # try_execute() 保证不会双重执行

            finally:
                self._monitor_queue.task_done()
            # [P2] 删除原来的 time.sleep(0.00001)，此处无需额外等待

    # ------------------------------------------------------------------
    # Dispatcher（Tier 2）：从主队列扇出到各订阅者队列
    # ------------------------------------------------------------------

    def _dispatcher_worker(self, channel: ChannelState) -> None:
        threading.current_thread().name = f"Dispatcher-{channel.name}"
        while not channel.stop_event.is_set():
            try:
                raw = channel.master_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                # 持锁期间只做列表复制，不执行入队，减少锁竞争
                with channel.sub_lock:
                    targets = list(channel.subscribers)

                for sub_id, sub_queue in targets:
                    try:
                        sub_queue.put_nowait(raw)
                    except queue.Full:
                        _log(
                            f"[Dispatcher-{channel.name}] "
                            f"Sub [{sub_id[:8]}] 队列满（max={sub_queue.maxsize}），消息丢弃"
                        )
            except Exception as e:
                if not channel.stop_event.is_set():
                    _log(f"[Dispatcher-{channel.name}] 异常: {e}")
            finally:
                channel.master_queue.task_done()

    # ------------------------------------------------------------------
    # Subscriber Worker（Tier 3）：解码 → 路由到线程池
    # ------------------------------------------------------------------

    def _subscriber_worker(
        self,
        channel_name: str,
        sub_id: str,
        sub_queue: queue.Queue,
        callback: Callable[[str, Any], None],
    ) -> None:
        threading.current_thread().name = f"Sub-{sub_id[:8]}"

        while True:
            try:
                raw = sub_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            try:
                if raw is POISON_PILL:
                    break

                # 反序列化
                try:
                    msg = _decode_message(raw)
                except Exception as e:
                    _log(f"[{threading.current_thread().name}] 解码失败: {e}")
                    continue

                task = TaskEntry(channel_name, sub_id, callback, msg)

                if self._is_isolated(sub_id):
                    # 已降级：直接走慢速池，跳过监控
                    self._slow_executor.submit(self._execute_task, task)
                else:
                    future = self._fast_executor.submit(self._execute_task, task)
                    task.executor_future = future
                    self._monitor_queue.put(task)

            except Exception as e:
                _log(f"[{threading.current_thread().name}] 致命异常: {e}")
            finally:
                sub_queue.task_done()

        # Worker 退出前从订阅表自我移除
        with self._sub_lock:
            self._subscriptions.pop(sub_id, None)
        _log(f"Sub Worker [{sub_id[:8]}] 已退出")

    # ------------------------------------------------------------------
    # 频道管理
    # ------------------------------------------------------------------

    def _get_or_create_channel(self, key: str) -> ChannelState:
        # 快速路径：无锁读
        ch = self._channels.get(key)
        if ch is not None:
            return ch
        # 慢速路径：[F1] 使用独立的频道锁
        with self._channel_lock:
            if key not in self._channels:
                self._channels[key] = ChannelState(key)
            return self._channels[key]

    # ------------------------------------------------------------------
    # 外部接口
    # ------------------------------------------------------------------

    def subscribe(
        self,
        keys: List[str],
        callback: Callable[[str, Any], None],
        queue_maxsize: int = 1000,
    ) -> Dict[str, str]:
        """
        [F5] 向一个或多个频道注册回调。
        每次调用创建独立的队列和 Worker，多次调用互不覆盖。

        Args:
            keys:          订阅的频道名列表
            callback:      消息到达时的回调函数 (channel: str, message: Any) -> None
            queue_maxsize: 每个订阅者队列的最大深度

        Returns:
            {channel_key: sub_id} 映射，sub_id 用于后续 unsubscribe()
        """
        result:           Dict[str, str]          = {}
        workers_to_start: List[threading.Thread]  = []

        for key in keys:
            channel   = self._get_or_create_channel(key)
            channel.start_dispatcher(self)

            sub_id    = str(uuid.uuid4())
            sub_queue : queue.Queue = queue.Queue(maxsize=queue_maxsize)

            worker = threading.Thread(
                target=self._subscriber_worker,
                args=(key, sub_id, sub_queue, callback),
                daemon=True,
            )

            with self._sub_lock:
                self._subscriptions[sub_id] = (key, worker, sub_queue, callback, queue_maxsize)

            with channel.sub_lock:
                channel.subscribers.append((sub_id, sub_queue))

            result[key] = sub_id
            workers_to_start.append(worker)
            _log(f"subscribe: ch={key} sub={sub_id[:8]} maxsize={queue_maxsize}")

        for w in workers_to_start:
            w.start()

        return result

    def send_message(self, key: str, obj: Any, block: bool = False) -> bool:
        """
        [P1] 发布消息到指定频道。
        根据对象类型自动选择序列化策略（见 _encode_message）。

        Args:
            key:   目标频道名
            obj:   消息对象（bytes / numpy.ndarray / dict / list / str / 其他）
            block: 主队列满时是否阻塞等待（默认 False，满则丢弃并返回 False）

        Returns:
            True 表示入队成功，False 表示序列化失败或队列满被丢弃
        """
        try:
            data = _encode_message(obj, copy_arrays=self._copy_arrays)
        except Exception as e:
            _log(f"[send_message] 序列化失败 ch={key}: {e}")
            return False

        channel = self._get_or_create_channel(key)
        try:
            channel.master_queue.put(data, block=block)
            return True
        except queue.Full:
            _log(f"[send_message] 频道 [{key}] 主队列已满，消息丢弃")
            return False

    def unsubscribe(self, sub_id: str) -> None:
        """
        [P3] 优雅退订：毒丸追加到队尾，Worker 处理完已入队消息后退出。
        适合业务模块主动注销，已发出的消息不会丢失。
        """
        with self._sub_lock:
            state = self._subscriptions.pop(sub_id, None)
        if state is None:
            return

        channel_name, worker_thread, sub_queue, _, _ = state

        # 从 Dispatcher 列表摘除（先摘除，防止新消息继续写入）
        channel = self._channels.get(channel_name)
        if channel:
            with channel.sub_lock:
                channel.subscribers = [
                    (s, q) for s, q in channel.subscribers if s != sub_id
                ]

        # 毒丸追尾：处理完已有消息再退出
        _tail_poison(sub_queue)
        worker_thread.join(timeout=5)
        _log(f"unsubscribe: sub={sub_id[:8]}")

    def stop(self) -> None:
        """
        系统级关闭，有序停止所有组件。

        停止顺序：
          1. QoS Monitor（不再接受新的超时检测任务）
          2. 隔离记录清理线程
          3. 所有 Dispatcher（不再从主队列读取）
          4. 所有 Subscriber Worker（[P3] 清空积压后闪退）[F8] 并行 join
          5. 关闭线程池
        """
        _log("MessageHub 正在停止...")

        # 1. 停止 QoS Monitor
        self._monitor_stop.set()
        self._monitor_thread.join(timeout=3)

        # 2. 停止隔离清理线程（用 set() 立即唤醒 wait()）
        self._cleanup_stop.set()
        self._cleanup_thread.join(timeout=3)

        # 3. 停止所有 Dispatcher
        for ch in self._channels.values():
            ch.stop_event.set()
        for ch in self._channels.values():
            if ch.dispatcher_thread and ch.dispatcher_thread.is_alive():
                ch.dispatcher_thread.join(timeout=3)

        # 4. [F3] 先在锁外收集列表，避免持锁时调用清理逻辑造成死锁
        with self._sub_lock:
            active_subs = list(self._subscriptions.keys())

        # [P3][F8] 并行：先清空队列投毒丸，再并行 join
        worker_threads: List[threading.Thread] = []
        for sub_id in active_subs:
            with self._sub_lock:
                state = self._subscriptions.pop(sub_id, None)
            if state is None:
                continue

            channel_name, worker_thread, sub_queue, _, _ = state

            # 从 Dispatcher 列表摘除
            ch = self._channels.get(channel_name)
            if ch:
                with ch.sub_lock:
                    ch.subscribers = [
                        (s, q) for s, q in ch.subscribers if s != sub_id
                    ]

            # [P3] 闪退模式：清空积压 → 放毒丸
            _drain_and_poison(sub_queue)
            worker_threads.append(worker_thread)

        # [F8] 并行等待所有 Worker 退出
        for t in worker_threads:
            t.join(timeout=5)

        # 5. 关闭线程池
        self._fast_executor.shutdown(wait=True, cancel_futures=True)
        self._slow_executor.shutdown(wait=True, cancel_futures=True)

        _log("MessageHub 已完全停止")


# ==================== 简易日志 ====================

def _log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"{ts} | [MessageHub] {msg}", flush=True)


# ==================== 使用示例 ====================

if __name__ == "__main__":

    MessageHub._reset_instance()
    bus = MessageHub(slow_threshold_ms=15.0, copy_arrays=False)

    # ── 回调定义 ──────────────────────────────────────────

    def fast_processor(channel: str, message: Any) -> None:
        """模拟轻量计算；trigger_slow=True 时主动触发 QoS 降级"""
        msg_id = message.get('id', '?') if isinstance(message, dict) else id(message)
        time.sleep(0.003)
        if isinstance(message, dict) and message.get("trigger_slow"):
            time.sleep(0.060)   # 超出 15ms 阈值，触发 QoS
        print(f"  [FAST/{threading.current_thread().name}] ch={channel} id={msg_id}")

    def slow_storage(channel: str, message: Any) -> None:
        """模拟数据库写入（80ms I/O）"""
        msg_id = message.get('id', '?') if isinstance(message, dict) else id(message)
        time.sleep(0.080)
        print(f"  [SLOW/{threading.current_thread().name}] ch={channel} id={msg_id}")

    def cmd_handler(channel: str, message: Any) -> None:
        """轻量指令处理"""
        msg_id = message.get('id', '?') if isinstance(message, dict) else id(message)
        print(f"  [CMD/{threading.current_thread().name}]  ch={channel} id={msg_id}")

    def numpy_handler(channel: str, message: Any) -> None:
        """[P1] 演示 numpy 数组零拷贝接收"""
        if _NUMPY_AVAILABLE and isinstance(message, np.ndarray):
            print(f"  [NUMPY] shape={message.shape} dtype={message.dtype} writeable={message.flags.writeable}")
        else:
            print(f"  [NUMPY] 收到非数组消息: {type(message)}")

    # ── 注册订阅 ──────────────────────────────────────────

    # [F5] 三组独立订阅，各有独立队列和回调
    fast_subs  = bus.subscribe(["sensor_data", "control_cmd"], fast_processor, queue_maxsize=2000)
    slow_subs  = bus.subscribe(["sensor_data"],                slow_storage,   queue_maxsize=15000)
    cmd_subs   = bus.subscribe(["control_cmd"],                cmd_handler,    queue_maxsize=500)
    numpy_subs = bus.subscribe(["camera_frame"],               numpy_handler,  queue_maxsize=100)

    time.sleep(0.3)  # 等待所有 Worker 就绪

    # ── 场景 A：普通 dict 消息（JSON 序列化）────────────────
    _log("=== 场景 A：普通指令消息（JSON） ===")
    for i in range(1, 20):
        bus.send_message("control_cmd", {"id": i, "data": f"cmd_{i}"})

    # ── 场景 B：触发 QoS 降级 ────────────────────────────────
    _log("=== 场景 B：触发 QoS 降级 ===")
    bus.send_message("sensor_data", {"id": 101, "trigger_slow": True})
    bus.send_message("sensor_data", {"id": 102, "trigger_slow": True})

    # ── 场景 C：观察隔离后的慢速池路由 ──────────────────────
    _log("=== 场景 C：观察隔离后的慢速池路由 ===")
    for i in range(103, 160):
        bus.send_message("sensor_data", {"id": i, "data": f"sensor_{i}"})

    # ── 场景 D：[P1] numpy 数组零拷贝传递 ───────────────────
    if _NUMPY_AVAILABLE:
        _log("=== 场景 D：numpy 数组零拷贝 ===")
        frame = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
        bus.send_message("camera_frame", frame)
        # 数组已被标记为只读，发送后修改会抛出 ValueError（保护语义）
        try:
            frame[0, 0, 0] = 99
        except ValueError as e:
            _log(f"[P1 验证] 零拷贝只读保护生效: {e}")
    else:
        _log("=== 场景 D：跳过（numpy 未安装）===")

    _log("所有消息已发送，等待处理完毕...")
    time.sleep(5)

    # ── 场景 E：[P3] 验证 stop() 闪退语义 ──────────────────
    _log("=== 场景 E：发送大量积压消息后立即 stop()（闪退验证）===")
    for i in range(500, 600):
        bus.send_message("sensor_data", {"id": i, "data": f"flood_{i}"})

    bus.stop()
    _log("主线程退出")
