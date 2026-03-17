"""
x-message-hub
=============
Production-grade thread-safe message bus for Python.

Quick start:
    from x_message_hub import MessageHub

    bus = MessageHub()
    bus.subscribe(["my_channel"], lambda ch, msg: print(msg))
    bus.send_message("my_channel", {"hello": "world"})
    bus.stop()
"""

from .message_hub import MessageHub, _log

__version__ = "3.0.0"
__author__  = "Jason"
__license__ = "MIT"

__all__ = ["MessageHub", "_log"]
