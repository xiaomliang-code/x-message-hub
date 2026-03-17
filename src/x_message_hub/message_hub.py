import os
import json
import logging
import time
import threading
import queue
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Callable, Optional, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, Future

# ============================================================
# MessageHub — 生产级消息总线 v3.1
# Author: Jason

# 架构保留：
#   三层流水线（Publisher → Dispatcher → Subscriber Worker）
#   动态 QoS：快慢双线程池 + 超时自动降级
#   毒丸（Poison Pill）优雅退出机制
#
# 历史修复（v2）：
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
#
# 新增修复（v3.1）：
#   [R1] try_execute() 用 Lock + bool 替代 Event 的 check-then-set，
#        彻底消除两线程同时通过 is_set() 检查的竞态窗口
#   [R2] IsolationRecord.TTL_SECONDS 改为实例参数，支持每个 Hub 单独配置
#   [R3] _tail_poison() 区分 queue.Full 与其他异常，Full 时降级为 put(block=True)，
#        确保毒丸必达，避免 unsubscribe() 后 Worker 永久阻塞
#   [R4] stop() 执行完毕后清除 _initialized，使单例可被重新初始化，
#        彻底解决 stop() 后无法重启的问题
#   [R5] SubscriptionState 由匿名 Tuple 改为具名 dataclass，消除魔法索引
#   [R6] 日志替换为 logging 模块，支持日志级别控制，避免高频 print 成为瓶颈
#   [R7] 线程池 max_workers 增加上限保护（默认 64），防止嵌入式平台线程爆炸
#   [R8] 消息丢弃计数器（原子），暴露 get_stats() 接口供监控使用
# ============================================================

# ---------------------------------------------------------------------------
# 日志配置（[R6]）
# ---------------------------------------------------------------------------
logger = logging.getLogger("MessageHub")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s | [%(name)s] %(message)s",
                                            datefmt="%H:%M:%S"))
    logger.addHandler(_handler)
    logger.setLevel(logging.DEBUG)

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
        self.channel         = channel
        self.sub_id          = sub_id
        self.callback        = callback
        self.message         = message
        self.submit_time: float          = time.monotonic()
        self.executor_future: Optional[Future] = None
        # [R1] 用 Lock + bool 实现真正的 CAS，消除 Event 的 check-then-set 竞态
        self._exec_lock  = threading.Lock()
        self._executed   = False

    def try_execute(self) -> bool:
        """
        真正的 CAS 语义：持锁检查并置位，保证"只执行一次"无竞态。
        首个调用者将 _executed 从 False 改为 True 并返回 True（获得执行权）；
        后续任何调用者都会看到 _executed 已为 True，返回 False。
        """
        with self._exec_lock:
            if self._executed:
                return False
            self._executed = True
            return True


class ChannelState:
    """每个频道持有的完整状态：主队列、订阅者列表、分发线程"""

    def __init__(self, name: str, master_queue_maxsize: int = 10_000):
        self.name          = name
        self.master_queue  = queue.Queue(maxsize=master_queue_maxsize)
        self.subscribers   : List[Tuple[str, queue.Queue]] = []
        self.sub_lock      = threading.Lock()
        self.stop_event    = threading.Event()
        self.dispatcher_thread: Optional[threading.Thread] = None

    def start_dispatcher(self, bus: 'MessageHub') -> None:
        with self.sub_lock:
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
    """[F6][R2] 隔离记录，携带时间戳以支持 TTL 自动过期；TTL 由实例参数控制"""

    def __init__(self, ttl_seconds: float = 300.0):
        self.isolated_at: float = time.monotonic()
        self._ttl = ttl_seconds

    def is_expired(self) -> bool:
        return (time.monotonic() - self.isolated_at) > self._ttl


# [R5] SubscriptionState 改为具名 dataclass，消除魔法索引访问
@dataclass
class SubscriptionState:
    channel_name : str
    worker_thread: threading.Thread
    sub_queue    : queue.Queue
    callback     : Callable
    queue_maxsize: int


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
    if isinstance(obj, (bytes, bytearray)):
        return obj

    if _NUMPY_AVAILABLE and isinstance(obj, np.ndarray):
        if copy_arrays:
            return obj.copy()
        # 只读保护：防止发送方在投递后修改同一块内存
        obj.flags.writeable = False
        return obj

    if isinstance(obj, str):
        return obj.encode('utf-8')
    if isinstance(obj, (dict, list)):
        return json.dumps(obj, ensure_ascii=False, separators=(',', ':')).encode('utf-8')

    # fallback
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
    if isinstance(raw, bytes):
        try:
            decoded = raw.decode('utf-8')
        except UnicodeDecodeError:
            return raw
        try:
            return json.loads(decoded)
        except json.JSONDecodeError:
            return decoded
    return raw


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
        # 极罕见：drain 后又有新消息进来，强制阻塞放入
        sub_queue.put(POISON_PILL, block=True)
    if drained:
        logger.debug("[drain] 闪退模式丢弃 %d 条积压消息", drained)


def _tail_poison(sub_queue: queue.Queue) -> None:
    """
    [R3] 优雅退出模式（用于 unsubscribe()）：
    毒丸追加到队尾，Worker 处理完所有已入队消息后才退出。
    区分 queue.Full（降级为阻塞 put，确保毒丸必达）与其他异常（记录日志）。
    """
    try:
        sub_queue.put_nowait(POISON_PILL)
    except queue.Full:
        # 队列满时改为阻塞等待，确保毒丸一定能送达
        logger.warning("[tail_poison] 订阅队列已满，阻塞等待投入毒丸...")
        try:
            sub_queue.put(POISON_PILL, block=True, timeout=10)
        except Exception as e:
            logger.error("[tail_poison] 毒丸投递失败，Worker 可能无法正常退出: %s", e)
    except Exception as e:
        logger.error("[tail_poison] 意外异常: %s", e)


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
        slow_threshold_ms   : float = 15.0,   # [F7] QoS 超时阈值，毫秒
        fast_pool_multiplier: int   = 8,       # FastPool = min(cpu × multiplier, max_pool_size)
        slow_pool_multiplier: int   = 2,       # SlowPool = min(cpu × multiplier, max_pool_size)
        max_pool_size       : int   = 64,      # [R7] 线程池上限，防止嵌入式平台线程爆炸
        copy_arrays         : bool  = False,   # [P1] numpy 数组是否强制拷贝
        isolation_ttl_s     : float = 300.0,   # [R2] 隔离记录 TTL，秒
        master_queue_maxsize: int   = 10_000,  # 每个频道主队列深度
    ):
        if hasattr(self, "_initialized"):
            return
        self._initialized = True

        # [F1] 四把职责明确的独立锁
        self._channel_lock   = threading.Lock()   # 保护 _channels 写入
        self._sub_lock       = threading.Lock()   # 保护 _subscriptions
        self._isolation_lock = threading.Lock()   # 保护 _isolated_subs

        self._channels     : Dict[str, ChannelState]      = {}
        self._subscriptions: Dict[str, SubscriptionState] = {}

        # [F7] 阈值转换为秒
        self.SLOW_THRESHOLD     : float = slow_threshold_ms / 1000.0
        self._copy_arrays       : bool  = copy_arrays
        self._isolation_ttl_s   : float = isolation_ttl_s
        self._master_queue_maxsize: int = master_queue_maxsize

        # [F6] 隔离表
        self._isolated_subs: Dict[str, IsolationRecord] = {}

        # [R8] 消息丢弃计数器（用 threading.Lock 保护原子递增）
        self._stats_lock        = threading.Lock()
        self._dropped_master    = 0   # 主队列满丢弃
        self._dropped_sub       = 0   # 订阅者队列满丢弃
        self._dropped_encode    = 0   # 序列化失败丢弃

        # [R7] 线程池大小加上限保护
        cpu = os.cpu_count() or 4
        fast_workers = min(cpu * fast_pool_multiplier, max_pool_size)
        slow_workers = min(cpu * slow_pool_multiplier, max_pool_size)

        self._fast_executor = ThreadPoolExecutor(
            max_workers=fast_workers,
            thread_name_prefix="FastWorker",
        )
        self._slow_executor = ThreadPoolExecutor(
            max_workers=slow_workers,
            thread_name_prefix="SlowWorker",
        )

        # QoS 监控线程
        self._monitor_queue : queue.Queue = queue.Queue()
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

        logger.info(
            "MessageHub v3.1 已初始化 | QoS阈值=%.1fms | FastPool=%d | SlowPool=%d | "
            "numpy=%s | copy_arrays=%s | isolation_ttl=%.0fs",
            slow_threshold_ms, fast_workers, slow_workers,
            "可用" if _NUMPY_AVAILABLE else "不可用", copy_arrays, isolation_ttl_s,
        )

    # ------------------------------------------------------------------
    # [R8] 统计接口
    # ------------------------------------------------------------------

    def get_stats(self) -> Dict[str, int]:
        """
        返回运行期间的消息丢弃统计，供外部监控系统采集。

        Returns:
            {
              "dropped_master": 主队列满丢弃数,
              "dropped_sub":    订阅者队列满丢弃数,
              "dropped_encode": 序列化失败丢弃数,
              "isolated_subs":  当前处于隔离状态的订阅者数,
            }
        """
        with self._stats_lock:
            d_master  = self._dropped_master
            d_sub     = self._dropped_sub
            d_encode  = self._dropped_encode
        with self._isolation_lock:
            n_iso = len(self._isolated_subs)
        return {
            "dropped_master" : d_master,
            "dropped_sub"    : d_sub,
            "dropped_encode" : d_encode,
            "isolated_subs"  : n_iso,
        }

    def reset_stats(self) -> None:
        """将统计计数器清零（例如每分钟采集后重置）"""
        with self._stats_lock:
            self._dropped_master = 0
            self._dropped_sub    = 0
            self._dropped_encode = 0

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
                logger.info("[Isolation] Sub [%s] TTL 到期，自动解除隔离", sub_id[:8])
                return False
            return True

    def _set_isolated(self, sub_id: str) -> None:
        with self._isolation_lock:
            self._isolated_subs[sub_id] = IsolationRecord(ttl_seconds=self._isolation_ttl_s)

    # [F6] 定时清理过期隔离记录，防止内存泄漏
    def _isolation_cleanup_worker(self) -> None:
        while not self._cleanup_stop.is_set():
            self._cleanup_stop.wait(timeout=60)
            with self._isolation_lock:
                expired = [sid for sid, rec in self._isolated_subs.items() if rec.is_expired()]
                for sid in expired:
                    del self._isolated_subs[sid]
            if expired:
                logger.info("[IsoCleanup] 清理 %d 条过期隔离记录", len(expired))

    # ------------------------------------------------------------------
    # QoS 任务执行入口
    # ------------------------------------------------------------------

    def _execute_task(self, task: TaskEntry) -> None:
        """[R1] 唯一执行入口，try_execute() 的真正 CAS 保证回调只被调用一次"""
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
            logger.error("[Callback ERROR] ch=%s id=%s err=%s", task.channel, msg_id, e)

    # ------------------------------------------------------------------
    # [P2] QoS Monitor —— 纯依赖 queue.get 阻塞，不再有 time.sleep
    # ------------------------------------------------------------------

    def _monitor_worker(self) -> None:
        while not self._monitor_stop.is_set():
            try:
                task: TaskEntry = self._monitor_queue.get(timeout=0.005)
            except queue.Empty:
                continue

            try:
                future = task.executor_future
                if future is None or future.done():
                    continue

                elapsed = time.monotonic() - task.submit_time

                if elapsed <= self.SLOW_THRESHOLD:
                    # 未超时，放回队列继续监控
                    self._monitor_queue.put(task)
                    continue

                # ---- 超时：触发 QoS 降级 ----
                logger.warning(
                    "[QoS] Sub [%s] 超时 %.1fms，永久降级到慢速池",
                    task.sub_id[:8], elapsed * 1000,
                )
                self._set_isolated(task.sub_id)

                if future.cancel():
                    # cancel() 成功（任务尚未开始）→ 转交慢速池执行
                    self._slow_executor.submit(self._execute_task, task)
                # cancel() 失败表示任务已在 Fast Pool 运行中，让它跑完
                # try_execute() 的 CAS 保证不会双重执行

            finally:
                self._monitor_queue.task_done()

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
                        with self._stats_lock:
                            self._dropped_sub += 1
                        logger.warning(
                            "[Dispatcher-%s] Sub [%s] 队列满（max=%d），消息丢弃",
                            channel.name, sub_id[:8], sub_queue.maxsize,
                        )
            except Exception as e:
                if not channel.stop_event.is_set():
                    logger.error("[Dispatcher-%s] 异常: %s", channel.name, e)
            finally:
                channel.master_queue.task_done()

    # ------------------------------------------------------------------
    # Subscriber Worker（Tier 3）：解码 → 路由到线程池
    # ------------------------------------------------------------------

    def _subscriber_worker(
        self,
        channel_name: str,
        sub_id      : str,
        sub_queue   : queue.Queue,
        callback    : Callable[[str, Any], None],
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

                try:
                    msg = _decode_message(raw)
                except Exception as e:
                    logger.error("[%s] 解码失败: %s", threading.current_thread().name, e)
                    continue

                task = TaskEntry(channel_name, sub_id, callback, msg)

                if self._is_isolated(sub_id):
                    # 已降级：直接走慢速池，跳过 Monitor
                    self._slow_executor.submit(self._execute_task, task)
                else:
                    future = self._fast_executor.submit(self._execute_task, task)
                    task.executor_future = future
                    self._monitor_queue.put(task)

            except Exception as e:
                logger.error("[%s] 致命异常: %s", threading.current_thread().name, e)
            finally:
                sub_queue.task_done()

        # Worker 退出前从订阅表自我移除
        with self._sub_lock:
            self._subscriptions.pop(sub_id, None)
        logger.debug("Sub Worker [%s] 已退出", sub_id[:8])

    # ------------------------------------------------------------------
    # 频道管理
    # ------------------------------------------------------------------

    def _get_or_create_channel(self, key: str) -> ChannelState:
        # 快速路径：依赖 GIL 保证 dict.get() 原子性，无需加锁
        ch = self._channels.get(key)
        if ch is not None:
            return ch
        # 慢速路径：[F1] 使用独立的频道锁，double-checked locking
        with self._channel_lock:
            if key not in self._channels:
                self._channels[key] = ChannelState(key, self._master_queue_maxsize)
            return self._channels[key]

    # ------------------------------------------------------------------
    # 外部接口
    # ------------------------------------------------------------------

    def subscribe(
        self,
        keys         : List[str],
        callback     : Callable[[str, Any], None],
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
        result          : Dict[str, str]         = {}
        workers_to_start: List[threading.Thread] = []

        for key in keys:
            channel  = self._get_or_create_channel(key)
            channel.start_dispatcher(self)

            sub_id    = str(uuid.uuid4())
            sub_queue = queue.Queue(maxsize=queue_maxsize)

            worker = threading.Thread(
                target=self._subscriber_worker,
                args=(key, sub_id, sub_queue, callback),
                daemon=True,
                name=f"Sub-{sub_id[:8]}",
            )

            state = SubscriptionState(
                channel_name=key,
                worker_thread=worker,
                sub_queue=sub_queue,
                callback=callback,
                queue_maxsize=queue_maxsize,
            )

            with self._sub_lock:
                self._subscriptions[sub_id] = state

            with channel.sub_lock:
                channel.subscribers.append((sub_id, sub_queue))

            result[key] = sub_id
            workers_to_start.append(worker)
            logger.debug("subscribe: ch=%s sub=%s maxsize=%d", key, sub_id[:8], queue_maxsize)

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
            with self._stats_lock:
                self._dropped_encode += 1
            logger.error("[send_message] 序列化失败 ch=%s: %s", key, e)
            return False

        channel = self._get_or_create_channel(key)
        try:
            channel.master_queue.put(data, block=block)
            return True
        except queue.Full:
            with self._stats_lock:
                self._dropped_master += 1
            logger.warning("[send_message] 频道 [%s] 主队列已满，消息丢弃", key)
            return False

    def unsubscribe(self, sub_id: str) -> None:
        """
        [P3][R3] 优雅退订：毒丸追加到队尾，Worker 处理完已入队消息后退出。
        _tail_poison() 保证毒丸必达，避免 Worker 永久阻塞。
        """
        with self._sub_lock:
            state = self._subscriptions.pop(sub_id, None)
        if state is None:
            return

        # 从 Dispatcher 列表摘除（先摘除，防止新消息继续写入）
        channel = self._channels.get(state.channel_name)
        if channel:
            with channel.sub_lock:
                channel.subscribers = [
                    (s, q) for s, q in channel.subscribers if s != sub_id
                ]

        # [R3] 毒丸追尾：确保必达，处理完已有消息再退出
        _tail_poison(state.sub_queue)
        state.worker_thread.join(timeout=5)
        if state.worker_thread.is_alive():
            logger.warning("unsubscribe: sub=%s Worker 未能在 5s 内退出", sub_id[:8])
        else:
            logger.debug("unsubscribe: sub=%s", sub_id[:8])

    def stop(self) -> None:
        """
        系统级关闭，有序停止所有组件。

        停止顺序：
          1. QoS Monitor（不再接受新的超时检测任务）
          2. 隔离记录清理线程
          3. 所有 Dispatcher（不再从主队列读取）
          4. 所有 Subscriber Worker（[P3] 清空积压后闪退）[F8] 并行 join
          5. 关闭线程池
          6. [R4] 清除 _initialized，允许单例重新初始化
        """
        logger.info("MessageHub 正在停止...")

        # 1. 停止 QoS Monitor
        self._monitor_stop.set()
        self._monitor_thread.join(timeout=3)

        # 2. 停止隔离清理线程（set() 立即唤醒 wait()）
        self._cleanup_stop.set()
        self._cleanup_thread.join(timeout=3)

        # 3. 停止所有 Dispatcher
        for ch in self._channels.values():
            ch.stop_event.set()
        for ch in self._channels.values():
            if ch.dispatcher_thread and ch.dispatcher_thread.is_alive():
                ch.dispatcher_thread.join(timeout=3)

        # 4. [F3] 先在锁内收集 sub_id 列表，锁外执行清理，避免持锁死锁
        with self._sub_lock:
            active_subs = list(self._subscriptions.keys())

        # [P3][F8] 并行：先清空队列投毒丸，再并行 join
        worker_threads: List[threading.Thread] = []
        for sub_id in active_subs:
            with self._sub_lock:
                state = self._subscriptions.pop(sub_id, None)
            if state is None:
                continue

            ch = self._channels.get(state.channel_name)
            if ch:
                with ch.sub_lock:
                    ch.subscribers = [
                        (s, q) for s, q in ch.subscribers if s != sub_id
                    ]

            # [P3] 闪退模式：清空积压 → 放毒丸
            _drain_and_poison(state.sub_queue)
            worker_threads.append(state.worker_thread)

        # [F8] 并行等待所有 Worker 退出
        for t in worker_threads:
            t.join(timeout=5)

        # 5. 关闭线程池
        self._fast_executor.shutdown(wait=True, cancel_futures=True)
        self._slow_executor.shutdown(wait=True, cancel_futures=True)

        # 6. [R4] 清除初始化标志，使单例在下次构造时可以重新初始化
        if hasattr(self, "_initialized"):
            del self._initialized

        logger.info("MessageHub 已完全停止")


# ==================== 使用示例 ====================

if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

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
            print(f"  [NUMPY] shape={message.shape} dtype={message.dtype} "
                  f"writeable={message.flags.writeable}")
        else:
            print(f"  [NUMPY] 收到非数组消息: {type(message)}")

    # ── 注册订阅 ──────────────────────────────────────────

    fast_subs  = bus.subscribe(["sensor_data", "control_cmd"], fast_processor, queue_maxsize=2000)
    slow_subs  = bus.subscribe(["sensor_data"],                slow_storage,   queue_maxsize=15000)
    cmd_subs   = bus.subscribe(["control_cmd"],                cmd_handler,    queue_maxsize=500)
    numpy_subs = bus.subscribe(["camera_frame"],               numpy_handler,  queue_maxsize=100)

    time.sleep(0.3)  # 等待所有 Worker 就绪

    # ── 场景 A：普通 dict 消息（JSON 序列化）────────────────
    logger.info("=== 场景 A：普通指令消息（JSON） ===")
    for i in range(1, 20):
        bus.send_message("control_cmd", {"id": i, "data": f"cmd_{i}"})

    # ── 场景 B：触发 QoS 降级 ────────────────────────────────
    logger.info("=== 场景 B：触发 QoS 降级 ===")
    bus.send_message("sensor_data", {"id": 101, "trigger_slow": True})
    bus.send_message("sensor_data", {"id": 102, "trigger_slow": True})

    # ── 场景 C：观察隔离后的慢速池路由 ──────────────────────
    logger.info("=== 场景 C：观察隔离后的慢速池路由 ===")
    for i in range(103, 160):
        bus.send_message("sensor_data", {"id": i, "data": f"sensor_{i}"})

    # ── 场景 D：[P1] numpy 数组零拷贝传递 ───────────────────
    if _NUMPY_AVAILABLE:
        logger.info("=== 场景 D：numpy 数组零拷贝 ===")
        frame = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
        bus.send_message("camera_frame", frame)
        try:
            frame[0, 0, 0] = 99
        except ValueError as e:
            logger.info("[P1 验证] 零拷贝只读保护生效: %s", e)
    else:
        logger.info("=== 场景 D：跳过（numpy 未安装）===")

    logger.info("所有消息已发送，等待处理完毕...")
    time.sleep(5)

    # ── 场景 E：[R8] 查看统计信息 ───────────────────────────
    logger.info("=== 场景 E：统计信息 ===")
    stats = bus.get_stats()
    logger.info("消息统计: %s", stats)

    # ── 场景 F：[R4] 验证 stop() 后可重新初始化 ─────────────
    logger.info("=== 场景 F：发送大量积压消息后立即 stop()（闪退验证）===")
    for i in range(500, 600):
        bus.send_message("sensor_data", {"id": i, "data": f"flood_{i}"})

    bus.stop()
    logger.info("stop() 完成，验证单例可重启...")

    # [R4] stop() 后重新初始化，不再返回已停止的实例
    bus2 = MessageHub(slow_threshold_ms=20.0)
    assert bus2 is bus, "单例指针应相同"
    assert hasattr(bus2, "_initialized"), "重新初始化应成功"
    logger.info("[R4 验证] 单例重启成功，新阈值=20ms")
    bus2.stop()

    logger.info("主线程退出")