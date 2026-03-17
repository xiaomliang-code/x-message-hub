# x-message-hub

[![PyPI version](https://badge.fury.io/py/x-message-hub.svg)](https://pypi.org/project/x-message-hub/)
[![Python](https://img.shields.io/pypi/pyversions/x-message-hub)](https://pypi.org/project/x-message-hub/)
[![Ko-fi](https://img.shields.io/badge/donate-Ko--fi-FF5E5B?logo=ko-fi)](https://ko-fi.com/jmaasnodny)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Production-grade thread-safe message bus for Python.**  
Built for robots, IoT systems, and multi-module applications that need fast, reliable inter-module communication.

[中文文档](README_CN.md)

---

## Features

- **Three-tier pipeline** — Publisher → Dispatcher → Subscriber Worker, clean separation of concerns
- **Dynamic QoS** — slow callbacks auto-demoted to a slow thread pool, never blocking fast ones
- **Multiple message types** — dict/list/str (JSON), bytes/bytearray (zero-copy), numpy arrays (zero-copy)
- **Fanout / broadcast** — one message, multiple independent subscribers
- **Hot-plug** — add or remove subscribers at runtime without restarting
- **Singleton** — one global instance, accessible from any module with `MessageHub()`
- **No required dependencies** — only Python standard library (numpy optional)
- **Python 3.9+**, Windows / macOS / Linux

---

## Installation

```bash
pip install x-message-hub
```

With numpy support (for zero-copy array passing):

```bash
pip install x-message-hub[numpy]
```

---

## Quick Start

```python
from x_message_hub import MessageHub

# Create once — singleton, shared everywhere
bus = MessageHub()

# Define a callback
def on_sensor(channel: str, message: dict):
    print(f"[{channel}] temp={message['temp']}")

# Subscribe
subs = bus.subscribe(["sensor_data"], callback=on_sensor)

# Publish
bus.send_message("sensor_data", {"temp": 36.5, "id": 1})

# Unsubscribe when done
bus.unsubscribe(subs["sensor_data"])

# Shutdown
bus.stop()
```

---

## Core API

### `MessageHub()`

```python
bus = MessageHub(
    slow_threshold_ms    = 15.0,  # QoS timeout (ms) — callbacks slower than this are demoted
    fast_pool_multiplier = 8,     # Fast thread pool = CPU cores × this
    slow_pool_multiplier = 2,     # Slow thread pool = CPU cores × this
    copy_arrays          = False, # numpy: False=zero-copy, True=safe copy
)
```

### `subscribe(keys, callback, queue_maxsize=1000)`

```python
subs = bus.subscribe(
    keys          = ["lidar", "camera"],  # subscribe to multiple channels at once
    callback      = my_handler,           # def my_handler(channel: str, message: Any)
    queue_maxsize = 1000,                 # per-subscriber buffer depth
)
# returns {"lidar": sub_id_1, "camera": sub_id_2}
```

### `send_message(key, obj, block=False)`

```python
bus.send_message("sensor_data", {"id": 1, "val": 3.14})   # dict → JSON
bus.send_message("raw",         b"\x08\x01\xff")           # bytes → zero-copy
bus.send_message("camera",      numpy_frame)               # ndarray → zero-copy
```

### `unsubscribe(sub_id)`

Graceful: waits for all queued messages to be processed before stopping.

### `stop()`

Fast shutdown: drains queues immediately and stops all workers in parallel.

---

## Message Types

| Type | Transport | Received as |
|------|-----------|-------------|
| `dict` / `list` | JSON serialized | `dict` / `list` |
| `str` | UTF-8 encoded | `str` |
| `bytes` | Zero-copy | `bytes` |
| `bytearray` | Zero-copy | `bytearray` |
| `numpy.ndarray` | Zero-copy (read-only) | `numpy.ndarray` |

---

## Request-Reply Pattern

Make an async operation look like a regular function call — ideal for HTTP handlers:

```python
# Copy RequestReplyBridge from examples/demo.py into your project
bridge = RequestReplyBridge(bus)

# In your HTTP handler — blocks only this thread, not others
def handle_request(data: dict) -> dict:
    result, ok = bridge.request(
        send_channel = "ai.infer",
        payload      = data,
        timeout      = 5.0,
    )
    return result if ok else {"error": "timeout"}
```

The worker that processes the request:

```python
def ai_worker(channel: str, message: dict):
    reply_id = message["_reply_id"]          # injected by bridge
    result   = {"output": do_inference(message)}
    bus.send_message(f"reply:{reply_id}", result)

bus.subscribe(["ai.infer"], ai_worker)
```

---

## QoS Auto-Demotion

If a callback takes longer than `slow_threshold_ms`:

1. The subscriber is permanently marked as **isolated**
2. Future messages for that subscriber go directly to the **slow thread pool**
3. Other subscribers are **completely unaffected**

Isolation records expire after 5 minutes (TTL), so a subscriber can recover if it becomes fast again.

---

## Robot / IoT Example

```python
bus = MessageHub()

# Sensor drivers publish raw data
bus.send_message("lidar_raw",  lidar_scan)
bus.send_message("camera_raw", camera_frame)   # numpy, zero-copy
bus.send_message("imu_data",   {"gyro": [0.1, 0.0, 0.01]})

# Perception modules — each has its own queue
bus.subscribe(["lidar_raw"],  pointcloud_filter)
bus.subscribe(["camera_raw"], object_detector)
bus.subscribe(["imu_data"],   pose_estimator)

# Logger subscribes to all raw channels with a large buffer
bus.subscribe(
    ["lidar_raw", "camera_raw", "imu_data"],
    data_logger,
    queue_maxsize=50000,
)
```

---

## Running the Demo

```bash
git clone https://github.com/your-username/x-message-hub.git
cd x-message-hub
pip install -e .
python examples/demo.py
```

---

## Running Tests

```bash
pip install pytest
pytest tests/
```

---

## License

[MIT](LICENSE) © 2025 Jason
