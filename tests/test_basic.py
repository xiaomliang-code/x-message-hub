"""
Basic smoke tests for x-message-hub.
Run with: pytest tests/
"""
import time
import threading
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from x_message_hub import MessageHub


def fresh_bus(**kwargs) -> MessageHub:
    MessageHub._reset_instance()
    return MessageHub(**kwargs)


# ── Test 1：基础收发 ──────────────────────────────────────────────────────────

def test_basic_send_receive():
    bus = fresh_bus()
    received = []

    bus.subscribe(["ch"], lambda ch, msg: received.append(msg))
    bus.send_message("ch", {"id": 1})
    time.sleep(0.3)
    bus.stop()

    assert len(received) == 1
    assert received[0]["id"] == 1


# ── Test 2：多频道 ────────────────────────────────────────────────────────────

def test_multi_channel():
    bus = fresh_bus()
    log = []

    bus.subscribe(["a", "b", "c"], lambda ch, msg: log.append(ch))
    bus.send_message("a", {"x": 1})
    bus.send_message("b", {"x": 2})
    bus.send_message("c", {"x": 3})
    time.sleep(0.3)
    bus.stop()

    assert set(log) == {"a", "b", "c"}


# ── Test 3：扇出 ──────────────────────────────────────────────────────────────

def test_fanout():
    bus = fresh_bus()
    log_a, log_b = [], []

    bus.subscribe(["events"], lambda ch, m: log_a.append(m["id"]))
    bus.subscribe(["events"], lambda ch, m: log_b.append(m["id"]))

    for i in range(5):
        bus.send_message("events", {"id": i})
    time.sleep(0.5)
    bus.stop()

    assert set(log_a) == set(range(5)) and len(log_a) == 5
    assert set(log_b) == set(range(5)) and len(log_b) == 5


# ── Test 4：unsubscribe 优雅退订 ──────────────────────────────────────────────

def test_unsubscribe_graceful():
    bus = fresh_bus()
    processed = []

    subs = bus.subscribe(["orders"], lambda ch, m: processed.append(m["id"]))
    sub_id = subs["orders"]
    time.sleep(0.1)

    for i in range(5):
        bus.send_message("orders", {"id": i})

    time.sleep(0.3)
    bus.unsubscribe(sub_id)
    time.sleep(0.3)
    bus.stop()

    assert len(processed) == 5


# ── Test 5：回调异常不崩溃 ────────────────────────────────────────────────────

def test_callback_exception_isolation():
    bus = fresh_bus()
    good = []

    bus.subscribe(["ch"], lambda ch, m: (_ for _ in ()).throw(RuntimeError("crash")))
    bus.subscribe(["ch"], lambda ch, m: good.append(m["id"]))
    time.sleep(0.1)

    for i in range(3):
        bus.send_message("ch", {"id": i})
    time.sleep(0.5)
    bus.stop()

    assert set(good) == {0, 1, 2}


# ── Test 6：send_message 返回值 ───────────────────────────────────────────────

def test_send_return_value():
    bus = fresh_bus()
    bus.subscribe(["ch"], lambda ch, m: None)
    time.sleep(0.1)

    assert bus.send_message("ch", {"ok": True}) is True
    assert bus.send_message("no_one_here", {"ok": True}) is True
    bus.stop()


# ── Test 7：单例 ──────────────────────────────────────────────────────────────

def test_singleton():
    MessageHub._reset_instance()
    a = MessageHub(slow_threshold_ms=10.0)
    b = MessageHub(slow_threshold_ms=99.0)
    assert a is b
    assert a.SLOW_THRESHOLD == 10.0 / 1000
    a.stop()
