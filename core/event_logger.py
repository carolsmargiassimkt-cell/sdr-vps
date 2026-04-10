from __future__ import annotations

import atexit
import logging
from datetime import datetime
from pathlib import Path
from threading import Lock, RLock, Timer
from typing import Any, Callable, List


class UnifiedEventLogger:
    def __init__(self, log_file: Path | str, *, flush_interval_sec: float = 5.0) -> None:
        self.log_file = Path(log_file)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._buffer: List[str] = []
        self._flush_interval_sec = float(max(1.0, flush_interval_sec))
        self._timer: Timer | None = None
        if not self.log_file.exists():
            self.log_file.write_text("", encoding="utf-8")
        atexit.register(self.flush)

    def log(
        self,
        event: str,
        *,
        phone: str = "",
        bot: str = "",
        message: str = "",
        result: str = "",
        timestamp: str = "",
    ) -> None:
        line = " | ".join(
            [
                (timestamp or datetime.utcnow().isoformat()),
                str(event or "").strip(),
                str(phone or "").strip(),
                str(bot or "").strip(),
                self._sanitize(message),
                str(result or "").strip(),
            ]
        )
        with self._lock:
            self._buffer.append(line)
            self._schedule_flush_locked()

    def flush(self) -> None:
        with self._lock:
            if not self._buffer:
                return
            self._cancel_timer_locked()
            payload = "\n".join(self._buffer) + "\n"
            self._buffer.clear()
        try:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            with self.log_file.open("a", encoding="utf-8") as handle:
                handle.write(payload)
        except Exception:
            return

    def _schedule_flush_locked(self) -> None:
        if self._timer is not None:
            return
        timer = Timer(self._flush_interval_sec, self.flush)
        timer.daemon = True
        self._timer = timer
        timer.start()

    def _cancel_timer_locked(self) -> None:
        if self._timer is None:
            return
        self._timer.cancel()
        self._timer = None

    @staticmethod
    def _sanitize(value: Any) -> str:
        return " ".join(str(value or "").strip().split())


class BufferedFileHandler(logging.Handler):
    def __init__(self, log_file: str | Path, *, flush_interval_sec: float = 5.0) -> None:
        super().__init__()
        self.log_file = Path(log_file)
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self.flush_interval_sec = float(max(1.0, flush_interval_sec))
        self._buffer: List[str] = []
        self._lock = RLock()
        self._timer: Timer | None = None
        atexit.register(self.flush)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
        except Exception:
            return
        with self._lock:
            self._buffer.append(line)
            self._schedule_flush_locked()

    def flush(self) -> None:
        with self._lock:
            if not self._buffer:
                return
            self._cancel_timer_locked()
            payload = "\n".join(self._buffer) + "\n"
            self._buffer.clear()
        try:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            with self.log_file.open("a", encoding="utf-8") as handle:
                handle.write(payload)
        except Exception:
            return

    def close(self) -> None:
        try:
            self.flush()
        finally:
            super().close()

    def _schedule_flush_locked(self) -> None:
        if self._timer is not None:
            return
        timer = Timer(self.flush_interval_sec, self.flush)
        timer.daemon = True
        self._timer = timer
        timer.start()

    def _cancel_timer_locked(self) -> None:
        if self._timer is None:
            return
        self._timer.cancel()
        self._timer = None


class EventLogger:
    def __init__(self, log_file: str) -> None:
        self._subscribers: List[Callable[[str], None]] = []
        self._logger = logging.getLogger("bot_sdr_ai")
        self._logger.setLevel(logging.INFO)
        self._logger.handlers.clear()
        self._logger.propagate = False

        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        for target in (log_file, "logs/carol_logs.txt", "logs/campaign.log"):
            handler = BufferedFileHandler(target, flush_interval_sec=5.0)
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    def subscribe(self, callback: Callable[[str], None]) -> None:
        self._subscribers.append(callback)

    def _emit(self, level: str, message: str) -> None:
        formatted = f"[{level}] {message}"
        for callback in self._subscribers:
            try:
                callback(formatted)
            except Exception:
                continue

    def info(self, message: str) -> None:
        self._logger.info(message)
        self._emit("INFO", message)

    def warning(self, message: str) -> None:
        self._logger.warning(message)
        self._emit("WARN", message)

    def error(self, message: str) -> None:
        self._logger.error(message)
        self._emit("ERROR", message)
