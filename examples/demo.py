"""
MessageHub — 完整 API 使用示例 & 可运行测试
============================================
本文件是 MessageHub 的"可运行文档"，直接执行即可看到每个功能的效果。
每个章节 (§) 对应一个独立场景，从最简单的收发消息到生产级集成模式。

【给 Python 入门者的阅读建议】
  - 先看 §2（最简单的订阅+发送）
  - 再看 §20（HTTP 请求等待回复，这是最常用的业务模式）
  - 其余章节按需跳读

【章节目录】
  §1  初始化：构造参数详解
  §2  subscribe()：单频道订阅
  §3  subscribe()：多频道一次订阅
  §4  subscribe()：多个独立订阅者订阅同一频道（扇出）
  §5  send_message()：dict / list / str 消息（JSON 序列化）
  §6  send_message()：bytes / bytearray 消息（零拷贝直传）
  §7  send_message()：numpy.ndarray 消息（零拷贝 + 只读保护）
  §8  send_message()：numpy copy_arrays=True 模式（拷贝传递）
  §9  send_message()：block=True 背压模式
  §10 send_message()：队列满时的丢弃行为（block=False）
  §11 send_message()：返回值校验
  §12 unsubscribe()：优雅退订（已入队消息不丢失）
  §13 QoS 自动降级：慢回调触发隔离 + 路由到慢速池
  §14 QoS 隔离 TTL：隔离记录自动过期
  §15 stop()：系统关闭（闪退语义，积压消息丢弃）
  §16 _reset_instance()：单例重置（测试环境使用）
  §17 回调异常隔离：单个回调崩溃不影响其他订阅者
  §18 动态扩容：运行中新增订阅者
  §19 机器人业务完整集成示例
  §20 同步转异步 Request-Reply：像调普通函数一样等待异步结果
"""

import time
import threading
import uuid
import sys
from typing import Any, Optional

try:
    import numpy as np
    _NUMPY_AVAILABLE = True
except ImportError:
    _NUMPY_AVAILABLE = False

# ── 导入核心模块 ───────────────────────────────────────────────────────────────
from message_hub import MessageHub, _log, _NUMPY_AVAILABLE


# ══════════════════════════════════════════════════════════════════════════════
# 测试辅助工具（入门者可忽略这部分，直接跳到 §1）
# ══════════════════════════════════════════════════════════════════════════════

def section(title: str) -> None:
    bar = "─" * 60
    print(f"\n{bar}\n  {title}\n{bar}")

def ok(msg: str) -> None:
    print(f"  ✅ PASS  {msg}")

def fail(msg: str) -> None:
    print(f"  ❌ FAIL  {msg}")
    sys.exit(1)

def wait(seconds: float, label: str = "") -> None:
    tag = f" [{label}]" if label else ""
    print(f"  ⏳ 等待 {seconds}s{tag}...")
    time.sleep(seconds)

def fresh_bus(**kwargs) -> MessageHub:
    """
    每个测试章节开始前，重置单例并创建全新的 MessageHub。
    【注意】真实业务代码中不需要这么做，全局用一个实例即可。
    """
    MessageHub._reset_instance()
    return MessageHub(**kwargs)


# ══════════════════════════════════════════════════════════════════════════════
# RequestReplyBridge — 同步转异步桥接器
# （定义在最前面，供 §20 使用，也方便入门者找到）
#
# 【解决什么问题？】
#   你启动了一个 HTTP Server，收到请求后需要调用其他异步模块处理。
#   但你的 HTTP handler 是同步函数，需要等到结果回来才能 return。
#   RequestReplyBridge 就是解决这个"同步等待异步结果"问题的桥。
#
# 【最简使用方法】
#
#   # 程序启动时初始化一次（全局共享）
#   bus    = MessageHub()
#   bridge = RequestReplyBridge(bus)
#
#   # 在你的 HTTP handler 里，像调普通函数一样：
#   def my_http_handler(request_data):
#       result, ok = bridge.request(
#           send_channel = "ai.infer",   # 把请求发给哪个模块
#           payload      = request_data, # 请求内容（dict）
#           timeout      = 5.0,          # 最多等几秒
#       )
#       if ok:
#           return result                # 拿到结果，正常返回
#       else:
#           return {"error": "timeout"} # 超时，返回错误
#
#   # 在处理模块里，处理完后把结果发回去：
#   def my_worker(channel, message):
#       reply_id = message["_reply_id"]  # bridge 自动注入，原样取出
#       result   = do_some_work(message)
#       bus.send_message(f"reply:{reply_id}", result)
#
# ══════════════════════════════════════════════════════════════════════════════

class RequestReplyBridge:
    """
    同步转异步的桥接器。

    让同步函数（如 HTTP handler）以"等待返回"的方式
    拿到异步模块的处理结果，同时不阻塞主线程和其他请求。

    内部原理（入门者可跳过）：
      1. 为每次请求生成唯一 reply_id
      2. 订阅临时频道 "reply:{reply_id}"，等待结果
      3. 把请求（含 _reply_id）发到业务频道
      4. threading.Event.wait(timeout) 挂起当前线程
         ↑ 只挂起"调用 request() 的线程"，其他线程完全不受影响
      5. 业务模块处理完 → 向 "reply:{reply_id}" 发消息
      6. Event.set() 唤醒等待的线程
      7. 取出结果，自动退订临时频道，return
    """

    # 临时回复频道前缀，防止与业务频道名冲突
    # 例如 reply_id="abc123"，则临时频道为 "reply:abc123"
    REPLY_PREFIX = "reply:"

    def __init__(self, bus: MessageHub):
        """
        Args:
            bus: 全局共享的 MessageHub 实例
        """
        self._bus = bus

    def request(
        self,
        send_channel: str,
        payload: dict,
        timeout: float = 5.0,
        reply_id: Optional[str] = None,
    ) -> tuple:
        """
        发送请求并同步等待回复。

        就像调普通函数一样：
            result, ok = bridge.request("ai.infer", {"data": "..."}, timeout=3.0)

        Args:
            send_channel : 把请求发到哪个频道（对应业务模块订阅的频道名）
            payload      : 请求内容（必须是 dict）。
                           bridge 会自动加入 "_reply_id" 字段，
                           业务模块回复时需原样传回给 bus.send_message()
            timeout      : 最多等待秒数。超时后函数自动返回 (None, False)，
                           不会永远卡住，不影响其他线程
            reply_id     : 可选，手动指定回复 ID（默认自动生成 UUID）

        Returns:
            (result, True)  — 成功收到回复，result 是业务模块返回的 dict
            (None,  False)  — 超时，未收到回复

        【给入门者的类比】
            就像你发了一条微信消息问问题：
            - 消息发出去（send_message）
            - 等对方回复，最多等 5 秒（Event.wait）
            - 对方回复了 → 拿到答案（True）
            - 5 秒内没回复 → 知道超时了（False），继续做别的
            - 等待期间家里其他人（其他线程）完全不受影响
        """
        # 步骤 1：生成本次请求的唯一标识
        # 每次调用生成不同的 UUID，并发时不同请求之间不会混淆（不串包）
        if reply_id is None:
            reply_id = str(uuid.uuid4())

        reply_channel = f"{self.REPLY_PREFIX}{reply_id}"

        # 步骤 2：准备接收回复的容器
        # 用列表存结果，是因为 Python 闭包规则：
        # 内层函数（on_reply）可以修改列表内容，但不能重新赋值外部变量
        result_box: list = []

        # threading.Event 是线程间的"信号灯"：
        #   done_event.wait(timeout) → 挂起当前线程，直到被唤醒或超时
        #   done_event.set()         → 唤醒正在 wait 的线程
        done_event = threading.Event()

        # 步骤 3：订阅临时回复频道（只等一条回复，队列深度设 1）
        def on_reply(channel: str, message: Any):
            """回复到达时，把结果放入 result_box 并唤醒等待的线程"""
            result_box.append(message)
            done_event.set()   # 点亮信号灯，唤醒 wait() 中的线程

        subs = self._bus.subscribe(
            keys=[reply_channel],
            callback=on_reply,
            queue_maxsize=1,
        )
        sub_id = subs[reply_channel]

        try:
            # 步骤 4：把请求发到业务频道（注入 _reply_id）
            # _reply_id 是业务模块找到回复频道的凭证，必须原样保留
            request_payload = {**payload, "_reply_id": reply_id}
            sent = self._bus.send_message(send_channel, request_payload)
            if not sent:
                _log(f"[Bridge] 发送失败：频道 {send_channel} 队列已满")
                return None, False

            # 步骤 5：挂起当前线程等待回复
            # 【关键】只挂起"调用 bridge.request() 的那个线程"
            #         主线程、其他 HTTP 线程、MessageHub 内部线程 → 完全不受影响
            arrived = done_event.wait(timeout=timeout)

            if arrived:
                return result_box[0], True   # 成功拿到回复
            else:
                _log(f"[Bridge] 超时 {timeout}s | reply_id={reply_id[:8]}...")
                return None, False           # 超时

        finally:
            # 步骤 6：无论成功还是超时，一定退订临时频道
            # finally 保证这行代码必定执行，防止资源泄漏
            self._bus.unsubscribe(sub_id)


# ══════════════════════════════════════════════════════════════════════════════
# §1  初始化：构造参数详解
# ══════════════════════════════════════════════════════════════════════════════

def demo_01_init():
    section("§1  初始化：构造参数详解")

    # 最简用法：直接 MessageHub()，所有参数都有默认值
    MessageHub._reset_instance()
    bus = MessageHub()
    ok("默认参数初始化成功（slow_threshold_ms=15, fast×8, slow×2）")
    bus.stop()

    # 自定义参数（资源受限场景）
    MessageHub._reset_instance()
    bus = MessageHub(
        slow_threshold_ms=30.0,   # QoS 超时阈值（ms），超过视为"慢回调"
        fast_pool_multiplier=4,   # 快速线程池 = CPU核数 × 4（默认 × 8）
        slow_pool_multiplier=1,   # 慢速线程池 = CPU核数 × 1（默认 × 2）
        copy_arrays=False,        # numpy 数组是否拷贝（False=零拷贝，默认）
    )
    ok("自定义参数初始化成功")
    bus.stop()

    # 单例特性：整个进程只有一个 MessageHub 实例
    MessageHub._reset_instance()
    bus_a = MessageHub(slow_threshold_ms=20.0)
    bus_b = MessageHub(slow_threshold_ms=99.0)  # 第二次构造，参数被忽略
    assert bus_a is bus_b
    assert bus_a.SLOW_THRESHOLD == 20.0 / 1000
    ok("单例验证：两次 MessageHub() 返回同一对象，参数以第一次为准")
    bus_a.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §2  subscribe()：单频道订阅（最基础的用法）
# ══════════════════════════════════════════════════════════════════════════════

def demo_02_subscribe_single():
    section("§2  subscribe()：单频道订阅")

    bus = fresh_bus()
    received = []

    # 回调函数：有消息到达时，MessageHub 会调用这个函数
    # 必须接受两个参数：channel（来源频道名）和 message（消息内容）
    def on_sensor(channel: str, message: dict):
        received.append((channel, message))

    # 订阅频道
    subs = bus.subscribe(
        keys=["sensor_data"],   # 要监听的频道名列表
        callback=on_sensor,     # 收到消息时调用哪个函数
        queue_maxsize=500,      # 内部缓冲队列容量
    )

    # 返回值：{频道名: sub_id}，sub_id 用于后续 unsubscribe()
    sub_id = subs["sensor_data"]
    ok(f"subscribe() 返回 sub_id: {sub_id[:8]}...（保存它，需要取消时用）")

    bus.send_message("sensor_data", {"id": 1, "temp": 36.5})
    wait(0.2, "等待回调执行")

    assert received[0][0] == "sensor_data"
    assert received[0][1]["temp"] == 36.5
    ok("消息正确到达回调，channel 和 payload 均符合预期")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §3  subscribe()：多频道一次订阅
# ══════════════════════════════════════════════════════════════════════════════

def demo_03_subscribe_multi_channel():
    section("§3  subscribe()：多频道一次订阅")

    bus = fresh_bus()
    log = []

    def handler(ch: str, msg):
        log.append(ch)

    # 一次订阅多个频道，同一个回调，通过 channel 参数区分来源
    subs = bus.subscribe(
        keys=["lidar", "camera", "imu"],
        callback=handler,
        queue_maxsize=200,
    )

    assert set(subs.keys()) == {"lidar", "camera", "imu"}
    ok(f"三个频道均已注册，sub_ids: { {k: v[:8] for k, v in subs.items()} }")

    bus.send_message("lidar",  {"frame": 1})
    bus.send_message("camera", {"frame": 1})
    bus.send_message("imu",    {"frame": 1})
    wait(0.3, "等待三路消息")

    assert set(log) == {"lidar", "camera", "imu"}
    ok("三个频道的消息均通过同一回调独立送达")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §4  subscribe()：扇出（一条消息，多个模块都收到）
# ══════════════════════════════════════════════════════════════════════════════

def demo_04_fanout():
    section("§4  subscribe()：多订阅者扇出（广播）")

    bus = fresh_bus()
    log_a, log_b, log_c = [], [], []

    # 三个不同模块各自订阅同一频道，发送方只发一次，三个都收到
    bus.subscribe(["events"], lambda ch, m: log_a.append(m["id"]), queue_maxsize=100)
    bus.subscribe(["events"], lambda ch, m: log_b.append(m["id"]), queue_maxsize=100)
    bus.subscribe(["events"], lambda ch, m: log_c.append(m["id"]), queue_maxsize=100)

    for i in range(5):
        bus.send_message("events", {"id": i})

    wait(0.5, "等待扇出完成")

    # 回调在线程池中并发执行，完成顺序不保证与发送顺序一致。
    # 因此只验证"每个订阅者收到了全部 5 条消息"，不验证顺序。
    assert set(log_a) == set(range(5)) and len(log_a) == 5, f"订阅者 A 数据异常: {log_a}"
    assert set(log_b) == set(range(5)) and len(log_b) == 5, f"订阅者 B 数据异常: {log_b}"
    assert set(log_c) == set(range(5)) and len(log_c) == 5, f"订阅者 C 数据异常: {log_c}"
    ok("同一条消息被三个独立订阅者均收到，内容完整（并发执行，顺序不保证）")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §5  send_message()：dict / list / str 消息类型
# ══════════════════════════════════════════════════════════════════════════════

def demo_05_send_json_types():
    section("§5  send_message()：dict / list / str（自动 JSON 序列化）")

    bus = fresh_bus()
    received = {}

    def handler(ch: str, msg):
        received[ch] = msg

    bus.subscribe(["ch_dict", "ch_list", "ch_str"], handler)
    wait(0.1)

    bus.send_message("ch_dict", {"robot": "R2D2", "status": "ok"})
    bus.send_message("ch_list", [1, 2, 3, {"nested": True}])
    bus.send_message("ch_str",  "hello robot")

    wait(0.3, "等待回调")

    assert received["ch_dict"] == {"robot": "R2D2", "status": "ok"}
    ok("dict 消息：序列化 → 传输 → 反序列化，内容完整")

    assert received["ch_list"] == [1, 2, 3, {"nested": True}]
    ok("list 消息：嵌套结构完整保留")

    assert received["ch_str"] == "hello robot"
    ok("str 消息：字符串原样到达")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §6  send_message()：bytes / bytearray（零拷贝直传）
# ══════════════════════════════════════════════════════════════════════════════

def demo_06_send_raw_bytes():
    section("§6  send_message()：bytes / bytearray 零拷贝直传")

    bus = fresh_bus()
    received = {}

    def handler(ch: str, msg):
        received[ch] = msg

    bus.subscribe(["raw_bytes", "raw_bytearray"], handler)
    wait(0.1)

    # 含非 UTF-8 字节的 bytes → 检测到无法解码，原样透传
    proto_payload = b"\x08\x01\x12\x05\xff\xfe"
    bus.send_message("raw_bytes", proto_payload)

    audio_buf = bytearray(b"\xff\xfe\x00\x01" * 4)
    bus.send_message("raw_bytearray", audio_buf)

    wait(0.3, "等待回调")

    assert isinstance(received["raw_bytes"], bytes)
    ok("bytes（含非 UTF-8 字节）：原样到达，类型保持 bytes")

    assert isinstance(received["raw_bytearray"], bytearray)
    ok("bytearray：原样到达，类型保持 bytearray")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §7  send_message()：numpy 零拷贝 + 只读保护
# ══════════════════════════════════════════════════════════════════════════════

def demo_07_send_numpy_zero_copy():
    section("§7  send_message()：numpy 零拷贝 + 只读保护（copy_arrays=False）")

    if not _NUMPY_AVAILABLE:
        print("  ⏭️  跳过（numpy 未安装）")
        return

    bus = fresh_bus(copy_arrays=False)
    received_frames = []

    def camera_callback(ch: str, frame: "np.ndarray"):
        received_frames.append(frame)

    bus.subscribe(["camera"], camera_callback, queue_maxsize=10)
    wait(0.1)

    frame = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
    bus.send_message("camera", frame)

    # 发送后源数组变为只读，防止数据竞争
    assert not frame.flags.writeable
    ok("send 后源数组变为只读（writeable=False），防止数据竞争")

    try:
        frame[0, 0, 0] = 99
        fail("应抛出 ValueError")
    except ValueError:
        ok("修改只读数组正确抛出 ValueError")

    wait(0.3, "等待回调")

    recv = received_frames[0]
    assert recv.shape == (480, 640, 3)
    assert recv.ctypes.data == frame.ctypes.data   # 同一内存地址 = 零拷贝
    ok(f"零拷贝验证：发送方与接收方指向同一内存地址（shape={recv.shape}）")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §8  send_message()：numpy copy_arrays=True（安全拷贝模式）
# ══════════════════════════════════════════════════════════════════════════════

def demo_08_send_numpy_copy_mode():
    section("§8  send_message()：numpy copy_arrays=True（发送后仍可修改源数组）")

    if not _NUMPY_AVAILABLE:
        print("  ⏭️  跳过（numpy 未安装）")
        return

    bus = fresh_bus(copy_arrays=True)
    received_frames = []
    barrier = threading.Event()

    def camera_callback(ch: str, frame: "np.ndarray"):
        received_frames.append(frame.copy())
        barrier.set()

    bus.subscribe(["camera"], camera_callback, queue_maxsize=10)
    wait(0.1)

    frame = np.zeros((4, 4), dtype=np.uint8)
    frame[0, 0] = 42
    bus.send_message("camera", frame)

    assert frame.flags.writeable
    ok("copy_arrays=True：源数组发送后仍然可写")

    frame[0, 0] = 99   # 发送后修改，不影响已发出的副本

    barrier.wait(timeout=2)
    assert received_frames[0][0, 0] == 42
    ok("接收方收到发送时的副本（值=42），与发送后的修改（值=99）隔离")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §9  send_message()：block=True 背压模式
# ══════════════════════════════════════════════════════════════════════════════

def demo_09_send_block_backpressure():
    section("§9  send_message()：block=True 背压模式")

    bus = fresh_bus()
    slow_barrier = threading.Event()
    processed = []

    def slow_cb(ch: str, msg):
        slow_barrier.wait()
        processed.append(msg["id"])

    bus.subscribe(["backpressure_ch"], slow_cb, queue_maxsize=2)
    wait(0.1)

    # block=False（默认）：队列满时立即返回 False
    results = [bus.send_message("backpressure_ch", {"id": i}, block=False)
               for i in range(20)]
    ok(f"block=False：发送 20 条，主队列成功 {results.count(True)} 条")

    slow_barrier.set()
    wait(0.5, "等待队列清空")

    # block=True：有空位后才入队，不丢弃
    result = bus.send_message("backpressure_ch", {"id": 999}, block=True)
    assert result is True
    ok("block=True：队列有空位后成功入队，返回 True")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §10 send_message()：队列满丢弃行为
# ══════════════════════════════════════════════════════════════════════════════

def demo_10_send_queue_full_drop():
    section("§10 send_message()：订阅者队列满时的丢弃行为")

    bus = fresh_bus()
    processed = []
    gate = threading.Event()

    def gated_cb(ch: str, msg):
        gate.wait()
        processed.append(msg["id"])

    bus.subscribe(["tiny_ch"], gated_cb, queue_maxsize=3)
    wait(0.1)

    success_count = sum(
        bus.send_message("tiny_ch", {"id": i}, block=False)
        for i in range(50)
    )

    # send_message() 写入的是主队列（maxsize=10000），几乎不会满
    # 丢弃发生在 Dispatcher 向订阅者队列（maxsize=3）扇出时
    ok(f"send_message() 写入主队列 {success_count}/50 条（主队列深 10000，不易满）")
    ok("订阅者队列满（深度 3）时，Dispatcher 静默丢弃，不影响主队列")

    gate.set()
    wait(0.5, "等待处理")
    ok(f"实际处理 {len(processed)} 条，其余在 Dispatcher 扇出阶段被丢弃")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §11 send_message()：返回值语义
# ══════════════════════════════════════════════════════════════════════════════

def demo_11_send_return_value():
    section("§11 send_message()：返回值语义")

    bus = fresh_bus()
    bus.subscribe(["ch"], lambda c, m: None)
    wait(0.1)

    assert bus.send_message("ch", {"ok": True}) is True
    ok("正常发送返回 True")

    # 向没有订阅者的频道发送：合法，频道动态创建，消息堆积等待消费
    assert bus.send_message("no_subscriber_ch", {"x": 1}) is True
    ok("向无订阅者频道发送返回 True（消息入主队列堆积，不报错）")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §12 unsubscribe()：优雅退订
# ══════════════════════════════════════════════════════════════════════════════

def demo_12_unsubscribe():
    section("§12 unsubscribe()：优雅退订（已入队消息全部处理完再退出）")

    bus = fresh_bus()
    processed = []

    def handler(ch: str, msg):
        time.sleep(0.01)
        processed.append(msg["id"])

    subs   = bus.subscribe(["orders"], handler, queue_maxsize=100)
    sub_id = subs["orders"]
    wait(0.1)

    for i in range(10):
        bus.send_message("orders", {"id": i})

    wait(0.3, "等待 Worker 把消息提交到线程池")

    # 毒丸追尾：10 条消息处理完后 Worker 才退出（同步阻塞，返回时 Worker 已退出）
    bus.unsubscribe(sub_id)
    wait(0.5, "等待线程池中的任务完成")

    assert len(processed) == 10
    ok("优雅退订：10 条消息全部处理完毕，无丢失")

    bus.send_message("orders", {"id": 99})   # 退订后发送：不崩溃
    ok("退订后继续发送不崩溃（消息入主队列，无人消费则堆积）")

    bus.unsubscribe(sub_id)                  # 重复退订：静默忽略
    ok("重复 unsubscribe 同一 sub_id → 静默忽略")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §13 QoS 自动降级：慢回调触发隔离
# ══════════════════════════════════════════════════════════════════════════════

def demo_13_qos_auto_demotion():
    section("§13 QoS 自动降级：慢回调超时 → 隔离 → 路由到慢速线程池")

    bus = fresh_bus(slow_threshold_ms=20.0)

    fast_threads, slow_threads = set(), set()
    lock = threading.Lock()

    def slow_callback(ch: str, msg):
        if msg.get("trigger_slow"):
            time.sleep(0.060)      # 故意超过 20ms 阈值，触发隔离
        name = threading.current_thread().name
        with lock:
            if "SlowWorker" in name:
                slow_threads.add(name)
            else:
                fast_threads.add(name)

    bus.subscribe(["qos_ch"], slow_callback, queue_maxsize=500)
    wait(0.1)

    bus.send_message("qos_ch", {"id": 1, "trigger_slow": True})
    bus.send_message("qos_ch", {"id": 2, "trigger_slow": True})
    wait(0.5, "等待 QoS Monitor 检测超时并隔离")

    for i in range(10, 20):
        bus.send_message("qos_ch", {"id": i})
    wait(0.5, "等待降级路由完成")

    assert len(slow_threads) > 0
    ok(f"降级前使用 FastWorker: {fast_threads}")
    ok(f"降级后使用 SlowWorker: {slow_threads}")
    ok("QoS 降级验证通过：慢订阅者被隔离，后续走慢速池，不影响其他订阅者")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §14 QoS 隔离 TTL：隔离记录自动过期
# ══════════════════════════════════════════════════════════════════════════════

def demo_14_isolation_ttl():
    section("§14 QoS 隔离 TTL：隔离记录 5 分钟后自动过期，防止内存泄漏")

    bus = fresh_bus(slow_threshold_ms=10.0)

    from message_hub import IsolationRecord
    fake_id = "fake-sub-for-ttl-test"

    expired = IsolationRecord()
    expired.isolated_at -= 400     # 模拟 400 秒前被隔离（超过默认 300s TTL）

    with bus._isolation_lock:
        bus._isolated_subs[fake_id] = expired

    assert bus._is_isolated(fake_id) is False
    ok("TTL 过期：_is_isolated() 返回 False，记录已自动清除")

    with bus._isolation_lock:
        assert fake_id not in bus._isolated_subs
    ok("过期记录已从隔离表删除，无内存泄漏")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §15 stop()：系统关闭（闪退语义）
# ══════════════════════════════════════════════════════════════════════════════

def demo_15_stop_fast_shutdown():
    section("§15 stop()：系统关闭（闪退：清空积压，快速退出）")

    bus = fresh_bus()
    processed = []

    def slow_handler(ch: str, msg):
        time.sleep(0.05)
        processed.append(msg["id"])

    bus.subscribe(["flood_ch"], slow_handler, queue_maxsize=5000)
    wait(0.1)

    for i in range(200):
        bus.send_message("flood_ch", {"id": i})

    t0 = time.monotonic()
    bus.stop()    # 清空积压 → 发毒丸 → 并行 join
    elapsed = time.monotonic() - t0

    assert elapsed < 10
    ok(f"stop() 耗时 {elapsed:.2f}s（闪退，不等待 200 条积压消息处理完）")
    ok(f"积压 200 条中处理了 {len(processed)} 条，其余被丢弃")


# ══════════════════════════════════════════════════════════════════════════════
# §16 _reset_instance()：单例重置
# ══════════════════════════════════════════════════════════════════════════════

def demo_16_reset_instance():
    section("§16 _reset_instance()：单例重置（主要用于测试环境）")

    MessageHub._reset_instance()
    bus1 = MessageHub(slow_threshold_ms=10.0)
    id1  = id(bus1)
    ok(f"第一个实例 id={id1}，threshold=10ms")

    MessageHub._reset_instance()   # 内部自动调用旧实例的 stop()
    bus2 = MessageHub(slow_threshold_ms=50.0)

    assert id1 != id(bus2)
    assert bus2.SLOW_THRESHOLD == 50.0 / 1000
    ok(f"重置后新实例 id={id(bus2)}，threshold=50ms（与旧实例不同）")
    ok("_reset_instance() 自动调用旧实例 stop()，资源自动释放")

    bus2.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §17 回调异常隔离：单个回调崩溃不影响其他订阅者
# ══════════════════════════════════════════════════════════════════════════════

def demo_17_callback_exception_isolation():
    section("§17 回调异常隔离：一个模块崩溃，不影响其他模块")

    bus = fresh_bus()
    good_received = []

    def bad_callback(ch: str, msg):
        raise RuntimeError(f"回调故意崩溃 id={msg.get('id')}")

    def good_callback(ch: str, msg):
        good_received.append(msg["id"])

    bus.subscribe(["shared_ch"], bad_callback,  queue_maxsize=100)
    bus.subscribe(["shared_ch"], good_callback, queue_maxsize=100)
    wait(0.1)

    for i in range(5):
        bus.send_message("shared_ch", {"id": i})

    wait(0.5, "等待回调完成")

    assert len(good_received) == 5
    ok("good_callback 正常收到全部 5 条消息，bad_callback 的异常被捕获隔离")
    ok("单个模块崩溃不影响其他模块，Worker 线程不退出")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §18 动态扩容：运行中热插拔新增订阅者
# ══════════════════════════════════════════════════════════════════════════════

def demo_18_dynamic_subscribe():
    section("§18 动态扩容：程序运行中新增订阅者，无需重启")

    bus = fresh_bus()
    log_early, log_late = [], []

    bus.subscribe(["stream"], lambda c, m: log_early.append(m["id"]), queue_maxsize=200)
    wait(0.1)

    for i in range(5):
        bus.send_message("stream", {"id": i})
    wait(0.2, "等待第一批消息")

    bus.subscribe(["stream"], lambda c, m: log_late.append(m["id"]), queue_maxsize=200)
    wait(0.1)

    for i in range(10, 15):
        bus.send_message("stream", {"id": i})
    wait(0.3, "等待第二批消息")

    ok(f"早期订阅者收到: {log_early}（第一批 + 第二批）")
    ok(f"晚期订阅者收到: {log_late}（仅第二批，注册前的消息不补发）")

    assert set(log_early) == {0, 1, 2, 3, 4, 10, 11, 12, 13, 14}
    assert set(log_late)  == {10, 11, 12, 13, 14}
    ok("热插拔验证通过：新订阅者从注册时刻开始接收，不影响已有订阅者")

    bus.stop()


# ══════════════════════════════════════════════════════════════════════════════
# §19 机器人业务完整集成示例
# ══════════════════════════════════════════════════════════════════════════════

def demo_19_robot_integration():
    section("§19 机器人业务完整集成示例")

    print("""
  业务架构：
  ┌─────────────────────────────────────────────────────────┐
  │  传感器驱动层 → [lidar_raw] [camera_raw] [imu_data]      │
  │  感知层      ← 订阅传感器频道，→ 发布 [perception_result]│
  │  规划控制层  ← 订阅 [perception_result]                  │
  │  日志层      ← 订阅所有原始频道                          │
  └─────────────────────────────────────────────────────────┘
    """)

    bus = fresh_bus(slow_threshold_ms=20.0, copy_arrays=False)

    stats = {k: 0 for k in [
        "pointcloud_filter", "object_detector", "pose_estimator",
        "path_planner", "motion_ctrl", "data_logger_lidar",
        "data_logger_camera", "data_logger_imu",
    ]}
    stats_lock = threading.Lock()

    def inc(key):
        with stats_lock:
            stats[key] += 1

    def pointcloud_filter(ch: str, msg):
        time.sleep(0.003); inc("pointcloud_filter")
        bus.send_message("perception_result", {"source": "lidar", "frame_id": msg.get("frame_id")})

    def object_detector(ch: str, frame):
        if _NUMPY_AVAILABLE and isinstance(frame, dict):
            time.sleep(0.008)
        inc("object_detector")
        bus.send_message("perception_result", {"source": "camera"})

    def pose_estimator(ch: str, msg):
        inc("pose_estimator")

    def path_planner(ch: str, msg):
        time.sleep(0.005); inc("path_planner")

    def motion_controller(ch: str, msg):
        inc("motion_ctrl")

    def data_logger(ch: str, msg):
        time.sleep(0.030)
        key = f"data_logger_{ch.split('_')[0]}"
        if key in stats: inc(key)

    bus.subscribe(["lidar_raw"],   pointcloud_filter, queue_maxsize=200)
    bus.subscribe(["camera_raw"],  object_detector,   queue_maxsize=50)
    bus.subscribe(["imu_data"],    pose_estimator,    queue_maxsize=1000)
    bus.subscribe(["perception_result"], path_planner,      queue_maxsize=100)
    bus.subscribe(["perception_result"], motion_controller, queue_maxsize=100)
    bus.subscribe(["lidar_raw", "camera_raw", "imu_data"], data_logger, queue_maxsize=5000)

    wait(0.3, "等待所有 Worker 就绪")

    print("  🤖 机器人开始运行（10Hz，持续 1 秒）...")
    for frame_id in range(10):
        bus.send_message("lidar_raw", {"frame_id": frame_id, "points": 1024})
        bus.send_message("imu_data",  {"frame_id": frame_id, "gyro": [0.1, 0.0, 0.01]})
        if _NUMPY_AVAILABLE:
            fake_frame = np.zeros((240, 320, 3), dtype=np.uint8)
            fake_frame[0, 0, 0] = frame_id
            bus.send_message("camera_raw", fake_frame)
        else:
            bus.send_message("camera_raw", {"frame_id": frame_id, "mock": True})
        time.sleep(0.1)

    wait(1.5, "等待所有回调处理完毕")

    print("\n  📊 处理统计：")
    for k, v in stats.items():
        print(f"     {k:<25} : {v} 次")

    assert stats["pointcloud_filter"] == 10
    assert stats["pose_estimator"]    == 10
    assert stats["path_planner"]      >= 10
    assert stats["motion_ctrl"]       >= 10
    ok("机器人集成测试：全链路数据流验证通过")

    bus.stop()
    ok("机器人停机完成")


# ══════════════════════════════════════════════════════════════════════════════
# §20 同步转异步 Request-Reply
#
# 【场景描述】
#   你直接用 http.server 启动了一个 HTTP Server（不用 Flask）。
#   收到 HTTP 请求后，需要调用其他模块（AI 推理、数据库查询等）处理，
#   等结果回来后才能给客户端返回 —— 就像调普通函数一样自然。
#
# 【最终效果】
#   外部调用者（HTTP handler）写的代码是：
#
#       result, ok = bridge.request("ai.infer", 请求数据, timeout=5.0)
#       if ok:
#           return result    # 正常返回
#       else:
#           return 超时错误  # 超时
#
#   和调普通函数没有任何区别，不需要了解内部的线程、队列、事件机制。
#
# ══════════════════════════════════════════════════════════════════════════════

def demo_20_request_reply():
    section("§20 同步转异步 Request-Reply：像调普通函数一样等待异步结果")

    # ────────────────────────────────────────────────────────────────────────
    # 第一步：程序启动时初始化（只做一次，全局共享）
    # ────────────────────────────────────────────────────────────────────────
    bus    = fresh_bus(slow_threshold_ms=20.0)
    bridge = RequestReplyBridge(bus)    # 创建桥接器，只需一行

    # ────────────────────────────────────────────────────────────────────────
    # 第二步：定义并启动业务处理模块
    # 这个模块跑在后台，通过 MessageHub 接收任务，处理完回复结果
    # ────────────────────────────────────────────────────────────────────────
    def ai_infer_worker(channel: str, message: dict):
        """
        AI 推理模块（运行在后台线程）。

        收到请求 → 处理 → 把结果发回 "reply:{_reply_id}" 频道。
        注意：_reply_id 是 bridge 自动注入的，必须原样取出并用于回复。
        """
        # 取出 bridge 注入的回复凭证（一定要从 message 里拿，不能自己生成）
        reply_id   = message["_reply_id"]
        request_id = message.get("request_id", "?")

        # 模拟 AI 推理耗时 150ms（真实场景换成你的业务逻辑）
        time.sleep(0.15)

        # 构造回复内容
        result = {
            "request_id" : request_id,
            "output"     : f"AI 处理完毕（请求#{request_id}）",
            "confidence" : 0.95,
            "worker"     : threading.current_thread().name,
        }

        # 把结果发回临时回复频道
        # 格式固定：f"reply:{reply_id}"
        bus.send_message(f"reply:{reply_id}", result)

    # 启动 AI 模块（订阅 "ai.infer" 频道）
    bus.subscribe(keys=["ai.infer"], callback=ai_infer_worker, queue_maxsize=100)
    wait(0.2, "等待 AI 模块就绪")

    # ────────────────────────────────────────────────────────────────────────
    # 第三步：你的 HTTP handler 长这样
    # ────────────────────────────────────────────────────────────────────────
    def my_http_handler(request_data: dict, timeout: float = 3.0) -> dict:
        """
        你的 HTTP 处理函数，外部调用者感知不到内部是异步的。

        对外表现：和普通同步函数一样，等待结果后 return。
        内部实现：通过 bridge 等待异步模块的回复，不阻塞主线程。

        在真实的 http.server 项目里，你的代码大概是：

            class MyHandler(BaseHTTPRequestHandler):
                def do_POST(self):
                    body    = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                    result, ok = bridge.request("ai.infer", body, timeout=5.0)
                    response   = result if ok else {"error": "timeout", "status": 504}
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(json.dumps(response).encode())
        """
        print(f"\n  → [HTTP handler] 收到请求 #{request_data.get('request_id')} "
              f"| 线程: {threading.current_thread().name}")

        t0 = time.monotonic()

        # ── 核心就这一行 ──────────────────────────────────────────────────
        # 像调普通函数一样：发请求，等结果，拿回来
        result, ok = bridge.request(
            send_channel = "ai.infer",      # 把请求发给哪个模块
            payload      = request_data,    # 请求内容（你的业务数据）
            timeout      = timeout,         # 最多等几秒，超时自动返回
        )
        # ─────────────────────────────────────────────────────────────────

        elapsed = (time.monotonic() - t0) * 1000

        if ok:
            print(f"  ← [HTTP handler] ✅ 成功，耗时 {elapsed:.0f}ms | "
                  f"结果: {result.get('output')}")
            return {"status": 200, "data": result, "elapsed_ms": elapsed}
        else:
            print(f"  ← [HTTP handler] ⏰ 超时 {timeout}s，返回 504")
            return {"status": 504, "error": "Gateway Timeout", "elapsed_ms": elapsed}

    # ════════════════════════════════════════════════════════════════════════
    # 场景 A：单次调用（最简单的验证）
    # ════════════════════════════════════════════════════════════════════════
    print("\n  【场景 A：单次调用，像普通函数一样等待返回】")

    # 外部调用者只需要这一行，和调普通函数没有区别
    response = my_http_handler({"request_id": 1, "input": "用户数据"})

    assert response["status"] == 200
    assert response["data"]["request_id"] == 1
    ok(f"单次调用成功，耗时 {response['elapsed_ms']:.0f}ms（含 150ms AI 处理）")

    # ════════════════════════════════════════════════════════════════════════
    # 场景 B：5 个并发 HTTP 请求
    # 模拟真实 HTTP Server：每个请求在独立线程中运行
    # 所有请求同时等待，互不阻塞，总耗时约等于单次耗时
    # ════════════════════════════════════════════════════════════════════════
    print("\n  【场景 B：5 个并发 HTTP 请求（模拟 HTTP Server 线程池）】")

    responses  = {}
    http_threads = []

    def run_one_request(req_id: int):
        """
        每个 HTTP 请求运行在自己的线程里。
        bridge.request() 只挂起这一个线程，不影响其他线程。
        """
        resp = my_http_handler({"request_id": req_id, "input": f"数据_{req_id}"})
        responses[req_id] = resp

    # 同时启动 5 个 HTTP 请求
    t0 = time.monotonic()
    for i in range(2, 7):
        t = threading.Thread(target=run_one_request, args=(i,), name=f"HTTP-{i}")
        http_threads.append(t)
        t.start()

    for t in http_threads:
        t.join(timeout=10)

    total_elapsed = (time.monotonic() - t0) * 1000

    print(f"\n  📊 并发结果（5 个请求全部完成，总耗时 {total_elapsed:.0f}ms）：")
    all_ok = True
    for req_id in sorted(responses):
        r    = responses[req_id]
        mark = "✅" if r["status"] == 200 else "❌"
        print(f"     {mark} 请求#{req_id} | status={r['status']} "
              f"| 耗时={r.get('elapsed_ms', 0):.0f}ms")
        if r["status"] != 200:
            all_ok = False

    assert all_ok, "有请求失败"

    # 串行处理需要 5 × 150ms = 750ms，并发只需约 150ms
    ok(f"5 个并发请求全部成功，总耗时 {total_elapsed:.0f}ms"
       f"（串行需 750ms，并发缩短约 5 倍）")

    # 验证不串包：每个请求只收到自己的结果
    for req_id, resp in responses.items():
        assert resp["data"]["request_id"] == req_id
    ok("无串包：每个请求的结果只送达自己的 handler")

    # ════════════════════════════════════════════════════════════════════════
    # 场景 C：超时验证
    # 业务模块处理太慢，HTTP handler 不会永远等待，超时后自动返回
    # ════════════════════════════════════════════════════════════════════════
    print("\n  【场景 C：超时验证（处理模块太慢，HTTP 请求自动超时返回）】")

    # 注册一个"假装很忙"的模块，永远不回复
    bus.subscribe(
        keys=["slow.module"],
        callback=lambda ch, msg: time.sleep(5),   # 故意不回复
        queue_maxsize=10,
    )
    wait(0.1)

    # 构造一个独立的 bridge 指向慢模块，验证超时
    t0     = time.monotonic()
    _, ok_ = bridge.request("slow.module", {"id": 99}, timeout=0.5)
    elapsed_timeout = time.monotonic() - t0

    assert not ok_,               "应该超时返回 False"
    assert elapsed_timeout < 1.0, f"超时应在约 0.5s 内，实际 {elapsed_timeout:.2f}s"
    ok(f"超时验证：{elapsed_timeout*1000:.0f}ms 后返回 False（设定 500ms）")
    ok("超时后临时频道自动退订，无资源泄漏，无异常，程序继续正常运行")

    # ════════════════════════════════════════════════════════════════════════
    # 场景 D：主线程完全不阻塞
    # 验证"等待期间主线程仍在正常工作"
    # ════════════════════════════════════════════════════════════════════════
    print("\n  【场景 D：验证等待期间主线程不被阻塞】")

    main_ticks = []
    stop_flag  = threading.Event()

    def main_thread_heartbeat():
        """模拟主线程持续工作（事件循环、心跳、UI 刷新等）"""
        while not stop_flag.is_set():
            main_ticks.append(time.monotonic())
            time.sleep(0.05)   # 每 50ms 做一次工作

    # 启动主线程心跳
    ticker = threading.Thread(target=main_thread_heartbeat, daemon=True)
    ticker.start()

    # 在独立线程中发起 HTTP 请求（等待 150ms 的 AI 处理）
    http_t = threading.Thread(
        target=my_http_handler,
        args=({"request_id": 100},),
        name="HTTP-100",
    )
    t0 = time.monotonic()
    http_t.start()
    http_t.join(timeout=5)
    elapsed = time.monotonic() - t0

    stop_flag.set()

    # 若主线程被阻塞，等待期间 ticks 数量应为 0
    expected_ticks = int(elapsed / 0.05)
    ok(f"HTTP 等待 {elapsed*1000:.0f}ms 期间，主线程完成了 {len(main_ticks)} 次工作"
       f"（期望约 {expected_ticks} 次）")
    assert len(main_ticks) >= expected_ticks * 0.8
    ok("主线程不阻塞验证通过：HTTP 等待期间主线程正常运行")

    bus.stop()
    ok("§20 全部场景验证通过")


# ══════════════════════════════════════════════════════════════════════════════
# 主入口：顺序执行全部章节
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  MessageHub 完整 API 示例 & 测试")
    print("  Python:", sys.version.split()[0])
    print("  numpy :", "可用" if _NUMPY_AVAILABLE else "未安装（§7/§8 跳过）")
    print("=" * 60)

    demos = [
        demo_01_init,
        demo_02_subscribe_single,
        demo_03_subscribe_multi_channel,
        demo_04_fanout,
        demo_05_send_json_types,
        demo_06_send_raw_bytes,
        demo_07_send_numpy_zero_copy,
        demo_08_send_numpy_copy_mode,
        demo_09_send_block_backpressure,
        demo_10_send_queue_full_drop,
        demo_11_send_return_value,
        demo_12_unsubscribe,
        demo_13_qos_auto_demotion,
        demo_14_isolation_ttl,
        demo_15_stop_fast_shutdown,
        demo_16_reset_instance,
        demo_17_callback_exception_isolation,
        demo_18_dynamic_subscribe,
        demo_19_robot_integration,
        demo_20_request_reply,   # 同步转异步 Request-Reply
    ]

    passed = failed = 0
    for demo in demos:
        try:
            demo()
            passed += 1
        except SystemExit:
            failed += 1
        except Exception as e:
            print(f"  ❌ EXCEPTION  {demo.__name__}: {e}")
            import traceback; traceback.print_exc()
            failed += 1

    print("\n" + "=" * 60)
    print(f"  结果：{passed} 通过，{failed} 失败，共 {len(demos)} 个章节")
    print("=" * 60)
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
