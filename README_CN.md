# MessageHub 使用文档

> 生产级 Python 消息总线，适用于机器人、IoT、微服务等多模块通信场景。  
> 版本：v3.0.0 ｜ Python ≥ 3.9 ｜ 无强制第三方依赖

---

## 目录

- [快速开始](#快速开始)
- [安装](#安装)
- [核心概念](#核心概念)
- [API 参考](#api-参考)
  - [MessageHub()](#messagehub-构造)
  - [subscribe()](#subscribe-订阅)
  - [send_message()](#send_message-发送消息)
  - [unsubscribe()](#unsubscribe-退订)
  - [stop()](#stop-关闭)
  - [_reset_instance()](#_reset_instance-重置单例)
- [消息类型与序列化](#消息类型与序列化)
- [Request-Reply 模式](#request-reply-模式)
- [QoS 自动降级机制](#qos-自动降级机制)
- [常见使用场景](#常见使用场景)
- [注意事项与最佳实践](#注意事项与最佳实践)
- [架构说明](#架构说明)

---

## 快速开始

```python
from message_hub import MessageHub

# 1. 创建实例（全局只需一次，单例模式）
bus = MessageHub()

# 2. 定义回调函数（消息到达时自动调用）
def on_sensor_data(channel: str, message: dict):
    print(f"收到来自 [{channel}] 的消息：{message}")

# 3. 订阅频道
subs = bus.subscribe(["sensor_data"], callback=on_sensor_data)

# 4. 发送消息
bus.send_message("sensor_data", {"temp": 36.5, "id": 1})

# 5. 程序结束时关闭
bus.stop()
```

输出：
```
收到来自 [sensor_data] 的消息：{'temp': 36.5, 'id': 1}
```

---

## 安装

### 方式一：直接复制文件（最简单）

`message_hub.py` 是单文件，无强制第三方依赖，直接复制到项目即可：

```bash
cp message_hub.py your_project/
```

### 方式二：安装 whl 包

```bash
# 安装 whl 文件
pip install pnc_message_hub-3.0.0-py3-none-any.whl

# 安装后导入方式变为：
from pnc_message_hub import MessageHub
```

### numpy（可选）

安装 numpy 后，支持零拷贝传递大型数组（相机帧、点云等）：

```bash
pip install numpy
```

未安装 numpy 时，MessageHub 仍然完全可用，只是 numpy 数组会走 JSON 序列化。

---

## 核心概念

### 频道（Channel）

频道是消息的分类标签，用字符串表示，如 `"sensor_data"`、`"control_cmd"`。

- 频道**不需要预先创建**，发送或订阅时自动创建
- 同一频道可以有**多个订阅者**（广播/扇出）
- 不同频道完全独立，互不影响

### 订阅者（Subscriber）

每次调用 `subscribe()` 创建一个独立的订阅者，拥有：
- 自己的缓冲队列
- 自己的 Worker 线程
- 自己的回调函数

同一频道的多个订阅者**互不影响**——一个订阅者崩溃或变慢，不会拖累其他订阅者。

### 单例模式

整个进程中只有一个 `MessageHub` 实例。无论在哪个模块调用 `MessageHub()`，拿到的都是同一个对象：

```python
# 模块 A
bus = MessageHub()

# 模块 B（不同文件、不同类）
bus = MessageHub()  # 和模块 A 拿到的是同一个对象
```

---

## API 参考

### MessageHub 构造

```python
bus = MessageHub(
    slow_threshold_ms    = 15.0,   # QoS 超时阈值（毫秒）
    fast_pool_multiplier = 8,      # 快速线程池大小 = CPU核数 × 此值
    slow_pool_multiplier = 2,      # 慢速线程池大小 = CPU核数 × 此值
    copy_arrays          = False,  # numpy 数组是否强制拷贝
)
```

**参数说明：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `slow_threshold_ms` | float | `15.0` | 回调执行时间超过此值（毫秒）则触发 QoS 降级。I/O 密集型业务可调大到 30~50ms |
| `fast_pool_multiplier` | int | `8` | 快速线程池 = CPU核数 × 此值。资源受限设备可调小 |
| `slow_pool_multiplier` | int | `2` | 慢速线程池 = CPU核数 × 此值 |
| `copy_arrays` | bool | `False` | `False`=零拷贝（发送后源数组变为只读）；`True`=拷贝一份（发送后仍可修改源数组） |

> **注意：** 单例模式下，参数只在**第一次构造时**生效。后续调用 `MessageHub()` 返回已有实例，参数被忽略。

```python
bus_a = MessageHub(slow_threshold_ms=20.0)  # 生效
bus_b = MessageHub(slow_threshold_ms=99.0)  # 被忽略，返回同一实例
assert bus_a is bus_b  # True
```

---

### subscribe() 订阅

```python
subs = bus.subscribe(
    keys          = ["channel_name"],   # 要订阅的频道名列表
    callback      = my_function,        # 回调函数
    queue_maxsize = 1000,               # 订阅者队列深度（可选）
)
```

**参数说明：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `keys` | `List[str]` | 必填 | 频道名列表，可同时订阅多个 |
| `callback` | `Callable` | 必填 | 回调函数，签名为 `(channel: str, message: Any) -> None` |
| `queue_maxsize` | int | `1000` | 订阅者缓冲队列容量。队列满时新消息被丢弃（打印日志，不抛异常） |

**返回值：** `Dict[str, str]`，格式为 `{频道名: sub_id}`

```python
subs = bus.subscribe(["lidar", "camera", "imu"], my_callback)
# subs = {"lidar": "uuid-1", "camera": "uuid-2", "imu": "uuid-3"}
```

**回调函数签名：**

```python
def my_callback(channel: str, message: Any) -> None:
    # channel: 消息来自哪个频道
    # message: 消息内容（dict、str、bytes、numpy.ndarray 等）
    print(f"[{channel}] {message}")
```

**订阅多个频道，用 channel 参数区分来源：**

```python
def handler(channel: str, message: dict):
    if channel == "lidar":
        process_lidar(message)
    elif channel == "camera":
        process_camera(message)

bus.subscribe(["lidar", "camera"], handler)
```

**多个模块订阅同一频道（广播）：**

```python
# 感知模块
bus.subscribe(["sensor_data"], perception_callback)

# 日志模块（独立队列，互不影响）
bus.subscribe(["sensor_data"], logger_callback, queue_maxsize=10000)

# 监控模块
bus.subscribe(["sensor_data"], monitor_callback)
```

---

### send_message() 发送消息

```python
success = bus.send_message(
    key   = "channel_name",   # 目标频道
    obj   = {"data": "..."},  # 消息内容
    block = False,            # 主队列满时是否阻塞等待（可选）
)
```

**参数说明：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `key` | str | 必填 | 目标频道名 |
| `obj` | Any | 必填 | 消息内容，支持多种类型（见下方） |
| `block` | bool | `False` | `False`=队列满时丢弃并返回 False；`True`=阻塞等待有空位 |

**返回值：** `bool`
- `True`：消息成功进入主队列
- `False`：序列化失败，或主队列已满（block=False 时）

**支持的消息类型：**

```python
# dict / list → 自动 JSON 序列化，接收方得到原始类型
bus.send_message("ch", {"id": 1, "value": 3.14})
bus.send_message("ch", [1, 2, 3, {"nested": True}])

# str → UTF-8 编码传输，接收方得到字符串
bus.send_message("ch", "hello robot")

# bytes / bytearray → 零拷贝直传，接收方得到原始字节
bus.send_message("ch", b"\x08\x01\xff\xfe")          # protobuf 等二进制协议
bus.send_message("ch", bytearray(audio_buffer))       # 音频缓冲区

# numpy.ndarray → 零拷贝直传（发送后源数组变为只读）
import numpy as np
frame = np.zeros((480, 640, 3), dtype=np.uint8)
bus.send_message("camera", frame)
# 发送后 frame.flags.writeable == False
```

> **主队列 vs 订阅者队列：**  
> `send_message()` 写入的是**主队列**（容量 10,000），几乎不会满。  
> 真正的丢弃发生在 Dispatcher 向**订阅者队列**扇出时。  
> `send_message()` 返回 `True` 不代表所有订阅者都收到了消息。

---

### unsubscribe() 退订

```python
bus.unsubscribe(sub_id)
```

**优雅退订**：毒丸追加到队尾，Worker 处理完已入队的所有消息后才退出。  
适合业务模块主动注销，**已发出的消息不会丢失**。

```python
# 订阅时保存 sub_id
subs = bus.subscribe(["orders"], process_order)
sub_id = subs["orders"]

# 发送 10 条消息
for i in range(10):
    bus.send_message("orders", {"id": i})

# 退订：等待 10 条消息全部处理完再退出
bus.unsubscribe(sub_id)
# 此行之后，10 条消息均已处理完毕

# 多次 unsubscribe 同一 sub_id 是安全的（静默忽略）
bus.unsubscribe(sub_id)  # 无任何副作用
```

> **注意：** `unsubscribe()` 是**同步阻塞**调用，返回时 Worker 线程已退出。  
> 如果队列里积压了大量消息，等待时间可能较长。  
> 如需立即退出（丢弃积压），请使用 `stop()`。

---

### stop() 关闭

```python
bus.stop()
```

**闪退关闭**：清空所有订阅者队列中的积压消息，然后快速退出。  
适合程序结束时调用，**不等待积压消息处理完**。

停止顺序：
1. QoS 监控线程
2. 隔离记录清理线程
3. 所有 Dispatcher 线程
4. 清空各订阅者队列 → 发送毒丸 → 并行等待 Worker 退出
5. 关闭线程池

```python
# 程序入口
bus = MessageHub()
# ... 业务代码 ...

# 程序结束
bus.stop()
```

> **unsubscribe vs stop 的选择：**
>
> | 场景 | 使用 |
> |------|------|
> | 某个模块下线，已发消息要处理完 | `unsubscribe()` |
> | 程序退出，积压消息可以丢弃 | `stop()` |

---

### _reset_instance() 重置单例

```python
MessageHub._reset_instance()
```

强制重置单例，内部自动调用 `stop()` 释放旧实例资源，再清除单例引用。  
**主要用于测试环境**，让每个测试用例从干净状态开始。

```python
# 测试用例示例
def test_my_feature():
    MessageHub._reset_instance()   # 清除上一个测试的状态
    bus = MessageHub()
    # ... 测试代码 ...
    bus.stop()
```

> **生产代码中不需要调用此方法。**

---

## 消息类型与序列化

MessageHub 根据消息类型自动选择最优传输策略：

```
发送类型              传输方式              接收方得到的类型
─────────────────────────────────────────────────────────
dict / list        → JSON 序列化    →   dict / list
str                → UTF-8 编码    →   str
bytes              → 零拷贝直传    →   bytes（非 UTF-8）或 str
bytearray          → 零拷贝直传    →   bytearray
numpy.ndarray      → 零拷贝直传    →   numpy.ndarray（只读）
其他               → json.dumps    →   dict {"data": "..."}
```

### numpy 零拷贝详解

**默认模式（copy_arrays=False）：**

```python
bus = MessageHub(copy_arrays=False)  # 默认

frame = np.zeros((480, 640, 3), dtype=np.uint8)
bus.send_message("camera", frame)

# 发送后，源数组变为只读（防止数据竞争）
print(frame.flags.writeable)  # False

# 接收方和发送方共享同一块内存（零拷贝）
def on_frame(ch, msg):
    # msg 是同一块内存的引用，不可写
    print(msg.shape)  # (480, 640, 3)
```

**拷贝模式（copy_arrays=True）：**

```python
bus = MessageHub(copy_arrays=True)

frame = np.zeros((480, 640, 3), dtype=np.uint8)
bus.send_message("camera", frame)

# 发送后，源数组仍然可写
frame[0, 0, 0] = 255  # 不影响已发出的副本
```

> **如何选择：**
> - 发送后**不再修改**源数组 → `copy_arrays=False`（性能更好）
> - 发送后**还会修改**源数组 → `copy_arrays=True`（数据安全）

---

## Request-Reply 模式

**适用场景：** HTTP Server 收到请求后，需要等待其他模块（AI 推理、数据库查询等）处理完才能返回结果。

`RequestReplyBridge` 让同步函数以"等待返回"的方式拿到异步模块的结果，**不阻塞主线程和其他请求**。

> `RequestReplyBridge` 定义在 `message_hub_demo.py` 中，可直接复制到你的项目使用。

### 完整使用示例

**第一步：处理模块（接收请求，发回结果）**

```python
from message_hub import MessageHub

bus = MessageHub()

def ai_worker(channel: str, message: dict):
    """AI 推理模块，运行在后台"""
    reply_id = message["_reply_id"]   # bridge 自动注入，必须取出
    input_data = message.get("data")

    # 做你的业务处理（耗时操作）
    result = {"output": f"处理结果: {input_data}", "status": "ok"}

    # 把结果发回临时回复频道
    bus.send_message(f"reply:{reply_id}", result)

# 启动处理模块
bus.subscribe(["ai.infer"], ai_worker)
```

**第二步：HTTP Handler（发送请求，等待结果）**

```python
# bridge 定义（复制自 message_hub_demo.py）
bridge = RequestReplyBridge(bus)

def handle_http_request(request_data: dict) -> dict:
    """你的 HTTP 处理函数，外部看起来和普通函数一样"""

    # 核心就这一行：发出请求，等结果，最多等 5 秒
    result, ok = bridge.request(
        send_channel = "ai.infer",    # 发给哪个模块
        payload      = request_data,  # 请求内容
        timeout      = 5.0,           # 超时秒数
    )

    if ok:
        return {"status": 200, "data": result}
    else:
        return {"status": 504, "error": "处理超时"}
```

**第三步：在真实 HTTP Server 中使用**

```python
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

class MyHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers["Content-Length"])
        body   = json.loads(self.rfile.read(length))

        # 像调普通函数一样，等待异步结果
        response = handle_http_request(body)

        self.send_response(response["status"])
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())

server = HTTPServer(("0.0.0.0", 8080), MyHandler)
server.serve_forever()
```

### 工作原理

```
HTTP 请求进来
    ↓
1. 生成唯一 reply_id
2. 订阅临时频道 "reply:{reply_id}"
3. 发送请求到 "ai.infer"（含 _reply_id）
4. Event.wait(timeout=5.0) — 仅挂起当前 HTTP 线程
   （主线程、其他 HTTP 线程、MessageHub 内部线程 正常运行）
                    ↓                       ↓
             AI 模块处理中...          其他请求继续处理
                    ↓
5. AI 模块向 "reply:{reply_id}" 发送结果
6. Event.set() 唤醒 HTTP 线程
7. 取出结果，自动退订临时频道
8. return 结果给客户端
```

**并发效果（5 个并发请求，每个处理耗时 150ms）：**

```
串行处理：5 × 150ms = 750ms
并发处理：约 150ms + 少量开销（并发缩短约 5 倍）
```

---

## QoS 自动降级机制

MessageHub 内置 QoS（服务质量）保护，防止慢回调拖累整个系统。

### 工作流程

```
消息到达
    ↓
提交到快速线程池（FastPool）
    ↓
QoS Monitor 在后台计时
    ↓
执行时间 ≤ slow_threshold_ms？
   ✅ 是 → 正常完成，无影响
   ❌ 否 → 该订阅者被标记为"已隔离"
              ↓
         后续消息直接走慢速线程池（SlowPool）
         不再经过 QoS Monitor
         不影响其他订阅者
```

### 配置建议

| 业务类型 | 推荐阈值 | 说明 |
|----------|----------|------|
| 传感器数据处理（轻量计算） | 10~15ms | 默认值适用 |
| 指令解析、状态更新 | 15~30ms | 适当调大 |
| 数据库写入、网络请求 | 50~100ms | 这类回调天然适合慢速池 |

```python
# I/O 密集型业务，调大阈值
bus = MessageHub(slow_threshold_ms=50.0)
```

### 隔离 TTL

被隔离的订阅者在 **5 分钟后自动解除隔离**（如果业务恢复正常）。  
隔离记录定时清理，不占用内存。

---

## 常见使用场景

### 场景一：机器人多模块通信

```python
bus = MessageHub()

# 传感器驱动发布数据
bus.send_message("lidar_raw",  lidar_data)
bus.send_message("camera_raw", camera_frame)  # numpy 零拷贝
bus.send_message("imu_data",   imu_data)

# 感知模块处理
bus.subscribe(["lidar_raw"],   pointcloud_filter)
bus.subscribe(["camera_raw"],  object_detector)
bus.subscribe(["imu_data"],    pose_estimator)

# 日志模块同时记录（大队列，不影响感知模块）
bus.subscribe(
    ["lidar_raw", "camera_raw", "imu_data"],
    data_logger,
    queue_maxsize=50000,
)
```

### 场景二：HTTP Server 等待异步处理结果

见上方 [Request-Reply 模式](#request-reply-模式)。

### 场景三：动态热插拔模块

```python
bus = MessageHub()

# 系统运行中，随时新增订阅者，无需重启
# 新订阅者从注册时刻开始接收，历史消息不补发
new_subs = bus.subscribe(["sensor_data"], new_module_callback)

# 随时退订
bus.unsubscribe(new_subs["sensor_data"])
```

### 场景四：protobuf / ROS 消息传递

```python
import robot_pb2  # 你的 protobuf 定义

# 发送方：序列化为 bytes
msg = robot_pb2.SensorData()
msg.temp = 36.5
bus.send_message("sensor_pb", msg.SerializeToString())

# 接收方：反序列化
def on_sensor(ch: str, raw: bytes):
    msg = robot_pb2.SensorData()
    msg.ParseFromString(raw)
    print(msg.temp)

bus.subscribe(["sensor_pb"], on_sensor)
```

---

## 注意事项与最佳实践

### ✅ 推荐做法

**1. 全局只创建一次 MessageHub，所有模块共享**

```python
# app.py 或 main.py
bus = MessageHub()

# 其他任何模块里直接获取同一实例
bus = MessageHub()   # 返回同一对象，无副作用
```

**2. 回调函数保持轻量，耗时操作放到工作线程**

```python
# ❌ 不推荐：回调里直接做耗时 I/O
def bad_callback(ch, msg):
    time.sleep(1)          # 触发 QoS 降级
    save_to_database(msg)  # 阻塞 Worker

# ✅ 推荐：回调只做判断和转发，耗时操作交给其他线程
def good_callback(ch, msg):
    task_queue.put(msg)    # 快速入队，立即返回
```

**3. 根据消息优先级和量级设置合适的队列深度**

```python
# 实时控制指令：小队列，避免积压过期指令
bus.subscribe(["control_cmd"], cmd_handler,    queue_maxsize=50)

# 传感器数据：中等队列
bus.subscribe(["sensor_data"], data_processor, queue_maxsize=1000)

# 日志记录：大队列，允许积压
bus.subscribe(["all_data"],    logger,         queue_maxsize=100000)
```

**4. 保存 sub_id，需要时才退订**

```python
# 程序启动时订阅，保存 sub_id
self.subs = bus.subscribe(["events"], self.on_event)

# 模块销毁时退订
def cleanup(self):
    for sub_id in self.subs.values():
        bus.unsubscribe(sub_id)
```

### ❌ 避免的做法

**1. 不要在回调里调用 unsubscribe 或 stop**

```python
# ❌ 死锁风险：回调在 Worker 线程里执行，unsubscribe 会等待 Worker 退出
def bad_callback(ch, msg):
    bus.unsubscribe(sub_id)  # 会造成死锁！
```

**2. 不要假设回调的执行顺序**

```python
# ❌ 错误假设：多核机器上，5 条消息的回调完成顺序不保证
assert results == [0, 1, 2, 3, 4]   # 可能失败

# ✅ 正确：只验证内容，不验证顺序
assert set(results) == {0, 1, 2, 3, 4}
```

**3. numpy 零拷贝发送后不要修改源数组**

```python
frame = np.zeros((480, 640, 3), dtype=np.uint8)
bus.send_message("camera", frame)

# ❌ 发送后修改源数组 → ValueError（已变为只读）
frame[0, 0] = 255

# ✅ 如果需要继续修改，使用 copy_arrays=True 模式
bus = MessageHub(copy_arrays=True)
```

---

## 架构说明

MessageHub 采用三层流水线架构：

```
┌─────────────────────────────────────────────────────────┐
│  Tier 1：Publisher（你的代码）                           │
│    send_message() → 序列化 → 主队列（maxsize=10,000）    │
├─────────────────────────────────────────────────────────┤
│  Tier 2：Dispatcher（每频道一个线程）                    │
│    从主队列读取 → 扇出到各订阅者队列                      │
├─────────────────────────────────────────────────────────┤
│  Tier 3：Subscriber Worker（每订阅者一个线程）            │
│    从订阅者队列读取 → 反序列化 → 提交到线程池             │
├──────────────────────────────┬──────────────────────────┤
│  FastPool（CPU × 8 线程）    │  SlowPool（CPU × 2 线程） │
│  正常回调在此执行            │  QoS 降级后的回调在此执行  │
└──────────────────────────────┴──────────────────────────┘
                      ↑
              QoS Monitor（独立线程）
              监控 FastPool 的执行时间
              超时则将订阅者降级到 SlowPool
```

**各层职责：**

| 层 | 职责 | 线程数 |
|----|------|--------|
| Dispatcher | 消息扇出，不做业务处理 | 每频道 1 个 |
| Subscriber Worker | 消息解码，提交到线程池 | 每订阅者 1 个 |
| FastPool | 执行轻量回调 | CPU × 8 |
| SlowPool | 执行 I/O 密集型回调 | CPU × 2 |
| QoS Monitor | 监控超时，触发降级 | 1 个 |
| IsoCleanup | 定时清理过期隔离记录 | 1 个 |

---

## 文件清单

| 文件 | 说明 |
|------|------|
| `message_hub.py` | 核心库，单文件，无强制依赖 |
| `message_hub_demo.py` | 完整 API 示例 + `RequestReplyBridge` 定义 |
| `pnc_message_hub-3.0.0-py3-none-any.whl` | pip 安装包 |
| `pnc_message_hub-3.0.0.tar.gz` | 源码包 |

---

*MessageHub v3.0.0 ｜ Python ≥ 3.9 ｜ 仅依赖标准库*
