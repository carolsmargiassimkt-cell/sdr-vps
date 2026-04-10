from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from threading import Lock
from typing import Callable, Dict, List

from core.system_health import log_system_health
from core.worker_heartbeat import WorkerHeartbeat
from system_watchdog import SystemWatchdog


from core.runtime_state import RuntimeState


class Supervisor:
    def __init__(
        self,
        project_root: str | None = None,
        logger: logging.Logger | None = None,
        runtime_state: RuntimeState | None = None,
    ) -> None:
        root = Path(project_root or ".")
        self.project_root = root
        self.logger = logger or logging.getLogger("supervisor")
        self.watchdog = SystemWatchdog(root, logger=self.logger)
        self._workers: Dict[str, Dict[str, object]] = {}
        self.heartbeat = WorkerHeartbeat()
        self._heartbeat_thread: threading.Thread | None = None
        self.runtime_state = runtime_state
        self._lock = Lock()
        self._watchdog_thread: threading.Thread | None = None
        self._started = False

    def register_worker(
        self,
        name: str,
        check_alive: Callable[[], bool],
        restart_callback: Callable[[], None],
        priority: int = 0,
        max_retries: int = 5,
        retry_backoff_sec: float = 10.0,
    ) -> None:
        with self._lock:
            self._workers[name] = {
                "check": check_alive,
                "restart_callback": restart_callback,
                "priority": int(priority),
                "max_retries": int(max_retries),
                "retry_backoff_sec": float(retry_backoff_sec),
                "restart_attempts": 0,
                "last_restart_at": 0.0,
                "last_failure_at": 0.0,
            }
        self.watchdog.register_worker(
            name,
            check_alive,
            lambda worker_name=name: self.restart_worker(worker_name, reason="watchdog"),
            priority=priority,
        )

    def register_edge(self, name: str, port: int, reconnect: Callable[[], None]) -> None:
        self.watchdog.register_edge_monitor(name, port, reconnect)

    def register_queue_status(self, fn: Callable[[], Dict[str, object]]) -> None:
        self.watchdog.register_queue_status_fn(fn)

    def start(self) -> None:
        self._started = True

    def start_watchdog(self) -> None:
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            return
        self._watchdog_thread = threading.Thread(target=self.watchdog.monitor_loop, name="system-watchdog", daemon=True)
        self._watchdog_thread.start()
        self._start_heartbeat_monitor()

    def _start_heartbeat_monitor(self) -> None:
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_monitor_loop, name="worker-heartbeat-monitor", daemon=True)
        self._heartbeat_thread.start()

    def _heartbeat_monitor_loop(self) -> None:
        while True:
            self._check_heartbeats()
            time.sleep(15)

    def _check_heartbeats(self) -> None:
        now = time.time()
        for worker_name in list(self._workers):
            last_seen = self.heartbeat.get_last_seen(worker_name)
            if last_seen is None:
                continue
            if now - last_seen > 30:
                log_system_health(self.project_root, "worker_unresponsive_restart", worker_name=worker_name)
                self.logger.warning(f"heartbeat missing; restarting {worker_name}")
                self.restart_worker(worker_name, reason="heartbeat")

    def restart_worker(self, name: str, *, reason: str) -> bool:
        with self._lock:
            meta = self._workers.get(name)
            if not meta:
                self.logger.warning(f"Supervisor sem worker registrado: {name}")
                return False
            now = time.time()
            backoff = float(meta.get("retry_backoff_sec", 10.0) or 10.0)
            last_restart = float(meta.get("last_restart_at", 0.0) or 0.0)
            attempts = int(meta.get("restart_attempts", 0) or 0)
            max_retries = int(meta.get("max_retries", 5) or 5)
            if attempts >= max_retries and (now - last_restart) < backoff:
                self.logger.warning(f"Supervisor retry limit ativo | worker={name} | reason={reason}")
                return False
            if (now - last_restart) < backoff:
                return False
            meta["restart_attempts"] = attempts + 1
            meta["last_restart_at"] = now
            meta["last_failure_at"] = now
            restart_callback = meta.get("restart_callback")
        try:
            if callable(restart_callback):
                restart_callback()
            self.logger.warning(f"Supervisor restart | worker={name} | reason={reason}")
            if self.runtime_state:
                self.runtime_state.update_worker_restart(name)
            log_system_health(
                self.project_root,
                "worker_restart",
                worker=name,
                reason=reason,
                retry=int(self._workers.get(name, {}).get("restart_attempts", 0) or 0),
            )
            return True
        except Exception as exc:
            self.logger.error(f"Supervisor falhou ao reiniciar {name}: {exc}")
            return False

    def pick_restart_candidate(self) -> str:
        with self._lock:
            if not self._workers:
                return ""
            workers = sorted(
                self._workers.items(),
                key=lambda item: (
                    int(item[1].get("priority", 0) or 0),
                    float(item[1].get("last_restart_at", 0.0) or 0.0),
                ),
            )
        return str(workers[0][0])

    def handle_memory_pressure(self, worker_name: str, usage: int) -> bool:
        self.logger.warning(f"Supervisor memory pressure | worker={worker_name} | rss={usage}")
        return self.restart_worker(worker_name, reason="memory_guard")

    def get_registered_worker_names(self) -> List[str]:
        with self._lock:
            return list(self._workers.keys())
