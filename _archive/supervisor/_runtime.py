from __future__ import annotations

import configparser
import json
import os
import shlex
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


STATE_SUBDIR = "supervisor_runtime"


@dataclass
class ProgramConfig:
    name: str
    command: str
    directory: str
    autostart: bool
    autorestart: bool
    stdout_logfile: str
    stderr_logfile: str


def _parse_bool(value: str, default: bool = False) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


def load_config(config_path: str) -> tuple[Path, List[ProgramConfig], Path, Path]:
    path = Path(config_path).resolve()
    parser = configparser.RawConfigParser()
    with path.open("r", encoding="utf-8") as handle:
        parser.read_file(handle)

    logs_dir = path.parent / "logs"
    if parser.has_section("supervisord"):
        childlogdir = parser.get("supervisord", "childlogdir", fallback=str(logs_dir))
        pidfile = Path(parser.get("supervisord", "pidfile", fallback=str(logs_dir / "supervisord.pid")))
    else:
        childlogdir = str(logs_dir)
        pidfile = logs_dir / "supervisord.pid"

    runtime_dir = Path(childlogdir) / STATE_SUBDIR
    runtime_dir.mkdir(parents=True, exist_ok=True)
    programs: List[ProgramConfig] = []
    for section in parser.sections():
        if not section.startswith("program:"):
            continue
        name = section.split(":", 1)[1]
        programs.append(
            ProgramConfig(
                name=name,
                command=parser.get(section, "command"),
                directory=parser.get(section, "directory", fallback=str(path.parent)),
                autostart=_parse_bool(parser.get(section, "autostart", fallback="false")),
                autorestart=_parse_bool(parser.get(section, "autorestart", fallback="true"), default=True),
                stdout_logfile=parser.get(section, "stdout_logfile", fallback=str(logs_dir / f"{name}.out.log")),
                stderr_logfile=parser.get(section, "stderr_logfile", fallback=str(logs_dir / f"{name}.err.log")),
            )
        )
    return path, programs, Path(pidfile), runtime_dir


def state_file(runtime_dir: Path) -> Path:
    return runtime_dir / "state.json"


def commands_dir(runtime_dir: Path) -> Path:
    path = runtime_dir / "commands"
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_json(path: Path, default):
    try:
        raw = path.read_text(encoding="utf-8")
        payload = json.loads(raw)
        return payload if isinstance(payload, type(default)) else default
    except Exception:
        return default


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f"{path.suffix}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def process_running(pid: int) -> bool:
    if int(pid or 0) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        pass
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {int(pid)}"],
            capture_output=True,
            text=True,
            timeout=10,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception:
        return False
    return str(int(pid)) in (result.stdout or "")


def read_state(runtime_dir: Path) -> Dict[str, object]:
    return read_json(state_file(runtime_dir), {"supervisord": {}, "programs": {}})


def write_state(runtime_dir: Path, payload: Dict[str, object]) -> None:
    write_json(state_file(runtime_dir), payload)


def start_program(program: ProgramConfig) -> Dict[str, object]:
    Path(program.stdout_logfile).parent.mkdir(parents=True, exist_ok=True)
    Path(program.stderr_logfile).parent.mkdir(parents=True, exist_ok=True)
    stdout_handle = open(program.stdout_logfile, "a", encoding="utf-8")
    stderr_handle = open(program.stderr_logfile, "a", encoding="utf-8")
    kwargs = {
        "cwd": program.directory,
        "stdout": stdout_handle,
        "stderr": stderr_handle,
        "stdin": subprocess.DEVNULL,
        "shell": True,
        "creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    }
    process = subprocess.Popen(program.command, **kwargs)
    return {
        "process": process,
        "stdout_handle": stdout_handle,
        "stderr_handle": stderr_handle,
        "status": "RUNNING",
        "pid": process.pid,
        "command": program.command,
        "directory": program.directory,
        "autostart": program.autostart,
        "autorestart": program.autorestart,
        "stdout_logfile": program.stdout_logfile,
        "stderr_logfile": program.stderr_logfile,
        "last_exit_code": None,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stopping": False,
    }


def stop_program(entry: Dict[str, object]) -> int:
    pid = int(entry.get("pid") or 0)
    if pid <= 0:
        return 0
    try:
        result = subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
            timeout=20,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return int(result.returncode or 0)
    except Exception:
        return 1


def close_handles(entry: Dict[str, object]) -> None:
    for key in ("stdout_handle", "stderr_handle"):
        handle = entry.get(key)
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass
            entry[key] = None


def public_program_state(entry: Dict[str, object]) -> Dict[str, object]:
    return {
        "pid": int(entry.get("pid") or 0),
        "status": str(entry.get("status") or "STOPPED"),
        "command": str(entry.get("command") or ""),
        "directory": str(entry.get("directory") or ""),
        "autostart": bool(entry.get("autostart")),
        "autorestart": bool(entry.get("autorestart")),
        "stdout_logfile": str(entry.get("stdout_logfile") or ""),
        "stderr_logfile": str(entry.get("stderr_logfile") or ""),
        "last_exit_code": entry.get("last_exit_code"),
        "started_at": str(entry.get("started_at") or ""),
    }


def build_initial_state(programs: List[ProgramConfig]) -> Dict[str, object]:
    return {
        "supervisord": {
            "pid": os.getpid(),
            "status": "RUNNING",
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        },
        "programs": {
            program.name: {
                "pid": 0,
                "status": "STOPPED",
                "command": program.command,
                "directory": program.directory,
                "autostart": program.autostart,
                "autorestart": program.autorestart,
                "stdout_logfile": program.stdout_logfile,
                "stderr_logfile": program.stderr_logfile,
                "last_exit_code": None,
                "started_at": "",
            }
            for program in programs
        },
    }


def parse_command(default_config: str) -> tuple[str, List[str]]:
    config_path = default_config
    args = sys.argv[1:]
    remaining: List[str] = []
    index = 0
    while index < len(args):
        current = args[index]
        if current in {"-c", "--configuration"} and index + 1 < len(args):
            config_path = args[index + 1]
            index += 2
            continue
        remaining.append(current)
        index += 1
    return config_path, remaining


def next_command_path(runtime_dir: Path) -> Path:
    return commands_dir(runtime_dir) / f"{int(time.time() * 1000)}-{uuid.uuid4().hex}.json"


def shell_split(command: str) -> List[str]:
    return shlex.split(command, posix=False)
