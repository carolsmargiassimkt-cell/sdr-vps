from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import requests

from runtime.process_lock import ProcessAlreadyRunningError, ProcessLock


PROJECT_ROOT = Path(__file__).resolve().parent
RUNTIME_DIR = PROJECT_ROOT / "runtime"
LOGS_DIR = PROJECT_ROOT / "logs"
NODE_EXE = Path(r"C:\Progra~1\nodejs\node.exe")
PYTHON_EXE = Path(r"C:\Users\Asus\Python311\python.exe")
BAILEYS_SCRIPT = PROJECT_ROOT / "src" / "services" / "whatsapp_baileys.js"
WORKER_SCRIPT = PROJECT_ROOT / "whatsapp_worker_loop.py"
AUTH_CREDS = PROJECT_ROOT / "auth_info_baileys" / "creds.json"
STATUS_URL = "http://127.0.0.1:3000/status"
POLL_SEC = 10
HEALTH_TIMEOUT_SEC = 5
BOOT_GRACE_SEC = 90


class WhatsAppStackRunner:
    def __init__(self) -> None:
        self.node_proc: Optional[subprocess.Popen] = None
        self.worker_proc: Optional[subprocess.Popen] = None
        self._stopping = False
        self._node_started_at = 0.0
        RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        LOGS_DIR.mkdir(parents=True, exist_ok=True)

    def run(self) -> int:
        if not AUTH_CREDS.exists():
            print(f"[ERRO] Sessao Baileys ausente em {AUTH_CREDS}")
            return 1

        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)

        lock = ProcessLock("whatsapp_stack_runner")
        try:
            lock.acquire()
        except ProcessAlreadyRunningError:
            print("[OK] whatsapp_stack_runner ja esta em execucao")
            return 0

        print("[RUNNER] whatsapp stack iniciando")
        try:
            while not self._stopping:
                self._ensure_node()
                self._ensure_worker()
                time.sleep(POLL_SEC)
        finally:
            self._terminate(self.worker_proc, "safe_worker")
            self._terminate(self.node_proc, "baileys")
            lock.release()
        return 0

    def _handle_stop(self, *_args) -> None:
        self._stopping = True

    def _node_healthy(self) -> bool:
        if self.node_proc is None or self.node_proc.poll() is not None:
            return False
        try:
            response = requests.get(STATUS_URL, timeout=HEALTH_TIMEOUT_SEC)
            if not response.ok:
                return False
            payload = response.json() if response.content else {}
            if not isinstance(payload, dict):
                return False
            status = str(payload.get("status") or "").strip().lower()
            mode = str(payload.get("mode") or "").strip().lower()
            if bool(payload.get("connected")):
                return True
            if status in {"online", "degraded"}:
                return True
            if mode in {"booting", "reconnecting", "connected"}:
                return True
            return False
        except Exception:
            if self._node_started_at and (time.time() - self._node_started_at) < BOOT_GRACE_SEC:
                return True
            return False

    def _ensure_node(self) -> None:
        if self._node_healthy():
            return
        if self.node_proc is not None and self.node_proc.poll() is None:
            self._terminate(self.node_proc, "baileys")
        env = os.environ.copy()
        env["BAILEYS_ALLOW_PAIRING"] = "0"
        stdout = open(LOGS_DIR / "baileys.out.log", "a", encoding="utf-8")
        stderr = open(LOGS_DIR / "baileys.err.log", "a", encoding="utf-8")
        self.node_proc = subprocess.Popen(
            [str(NODE_EXE), str(BAILEYS_SCRIPT)],
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=stdout,
            stderr=stderr,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self._node_started_at = time.time()
        print(f"[RUNNER] baileys iniciado pid={self.node_proc.pid}")
        time.sleep(4)

    def _ensure_worker(self) -> None:
        if self.worker_proc is not None and self.worker_proc.poll() is None:
            return
        env = os.environ.copy()
        env.setdefault("WHATSAPP_MIN_DELAY_SEC", "3")
        env.setdefault("WHATSAPP_MAX_DELAY_SEC", "7")
        env.setdefault("WHATSAPP_AUTOPILOT_INTERVAL", "8")
        stdout = open(LOGS_DIR / "whatsapp.out.log", "a", encoding="utf-8")
        stderr = open(LOGS_DIR / "whatsapp.err.log", "a", encoding="utf-8")
        self.worker_proc = subprocess.Popen(
            [str(PYTHON_EXE), str(WORKER_SCRIPT)],
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=stdout,
            stderr=stderr,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        print(f"[RUNNER] safe_worker iniciado pid={self.worker_proc.pid}")

    @staticmethod
    def _terminate(proc: Optional[subprocess.Popen], name: str) -> None:
        if proc is None or proc.poll() is not None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        print(f"[RUNNER] {name} finalizado")


def main() -> int:
    return WhatsAppStackRunner().run()


if __name__ == "__main__":
    raise SystemExit(main())
