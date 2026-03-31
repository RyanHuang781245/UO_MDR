from __future__ import annotations

import threading
from typing import Any, Callable


def start_daemon_job(target: Callable[..., Any], *args, **kwargs) -> threading.Thread:
    thread = threading.Thread(target=target, args=args, kwargs=kwargs, daemon=True)
    thread.start()
    return thread
