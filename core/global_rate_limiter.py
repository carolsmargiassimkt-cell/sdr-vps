from __future__ import annotations

import threading
import time


class GlobalRateLimiter:
    def __init__(self, rate: float = 5) -> None:
        if rate <= 0:
            raise ValueError("rate must be greater than zero")
        self.rate = rate
        self.lock = threading.Lock()
        self.last = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.time()
            delay = max(0.0, (1 / self.rate) - (now - self.last))
            if delay > 0:
                time.sleep(delay)
            self.last = time.time()


global_rate_limiter = GlobalRateLimiter(rate=5)
