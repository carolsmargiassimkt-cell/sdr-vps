from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

from ._runtime import (
    ProgramConfig,
    build_initial_state,
    close_handles,
    commands_dir,
    load_config,
    parse_command,
    process_running,
    public_program_state,
    read_json,
    start_program,
    state_file,
    stop_program,
    write_json,
    write_state,
)


def _refresh_state(runtime_dir: Path, state: dict, running: dict) -> None:
    state["supervisord"]["pid"] = os.getpid()
    state["supervisord"]["status"] = "RUNNING"
    for name in list(state.get("programs", {}).keys()):
        if name in running:
            state["programs"][name] = public_program_state(running[name])
    write_state(runtime_dir, state)


def _handle_commands(runtime_dir: Path, programs: dict, running: dict, state: dict) -> None:
    for command_file in sorted(commands_dir(runtime_dir).glob("*.json")):
        payload = read_json(command_file, {})
        action = str(payload.get("action") or "").strip().lower()
        name = str(payload.get("program") or "").strip()
        if not action or not name or name not in programs:
            try:
                command_file.unlink()
            except OSError:
                pass
            continue

        if action == "start" and name not in running:
            running[name] = start_program(programs[name])
            state["programs"][name] = public_program_state(running[name])
        elif action == "stop" and name in running:
            running[name]["stopping"] = True
            stop_program(running[name])
        elif action == "restart":
            if name in running:
                running[name]["stopping"] = True
                stop_program(running[name])
            else:
                running[name] = start_program(programs[name])
                state["programs"][name] = public_program_state(running[name])

        try:
            command_file.unlink()
        except OSError:
            pass


def _reap_processes(programs: dict, running: dict, state: dict) -> None:
    for name in list(running.keys()):
        entry = running[name]
        process = entry["process"]
        code = process.poll()
        if code is None:
            continue
        close_handles(entry)
        entry["last_exit_code"] = int(code)
        should_restart = bool(entry.get("autorestart")) and not bool(entry.get("stopping"))
        if should_restart:
            running[name] = start_program(programs[name])
            state["programs"][name] = public_program_state(running[name])
            continue
        state["programs"][name] = public_program_state({
            **entry,
            "pid": 0,
            "status": "STOPPED",
        })
        del running[name]


def main() -> int:
    config_path, _ = parse_command("supervisord.conf")
    config_path, program_list, pidfile, runtime_dir = load_config(config_path)

    if pidfile.exists():
        try:
            existing_pid = int(pidfile.read_text(encoding="utf-8").strip() or "0")
        except Exception:
            existing_pid = 0
        if process_running(existing_pid):
            print(f"supervisord already running as pid {existing_pid}")
            return 0

    pidfile.parent.mkdir(parents=True, exist_ok=True)
    pidfile.write_text(str(os.getpid()), encoding="utf-8")

    programs = {program.name: program for program in program_list}
    running = {}
    state = build_initial_state(program_list)
    write_state(runtime_dir, state)

    for program in program_list:
        if program.autostart:
            running[program.name] = start_program(program)
            state["programs"][program.name] = public_program_state(running[program.name])
    write_state(runtime_dir, state)

    stop_requested = False

    def _stop_handler(signum, frame):  # noqa: ARG001
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, _stop_handler)
    signal.signal(signal.SIGTERM, _stop_handler)

    try:
        while not stop_requested:
            _handle_commands(runtime_dir, programs, running, state)
            _reap_processes(programs, running, state)
            _refresh_state(runtime_dir, state, running)
            time.sleep(1)
    finally:
        for entry in running.values():
            entry["stopping"] = True
            stop_program(entry)
            close_handles(entry)
        state["supervisord"]["status"] = "STOPPED"
        write_state(runtime_dir, state)
        try:
            pidfile.unlink()
        except OSError:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
