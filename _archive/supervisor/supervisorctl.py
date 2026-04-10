from __future__ import annotations

import sys
import time

from ._runtime import (
    load_config,
    next_command_path,
    parse_command,
    process_running,
    read_state,
    write_json,
)


def _print_status(state: dict) -> int:
    programs = state.get("programs", {})
    if not programs:
        print("No programs configured")
        return 0
    for name in sorted(programs.keys()):
        item = programs[name]
        pid = int(item.get("pid") or 0)
        status = str(item.get("status") or "STOPPED")
        line = f"{name}\t{status}"
        if pid > 0:
            line += f"\tpid {pid}"
        print(line)
    return 0


def _queue_command(runtime_dir, action: str, program: str) -> int:
    path = next_command_path(runtime_dir)
    write_json(path, {"action": action, "program": program, "created_at": time.time()})
    return 0


def main() -> int:
    config_path, remaining = parse_command("supervisord.conf")
    _, programs, pidfile, runtime_dir = load_config(config_path)
    configured = {program.name for program in programs}
    state = read_state(runtime_dir)

    daemon_pid = int((state.get("supervisord", {}) or {}).get("pid") or 0)
    daemon_running = process_running(daemon_pid)

    if not remaining or remaining[0].lower() == "status":
        return _print_status(state)

    action = remaining[0].lower()
    if action not in {"start", "stop", "restart"}:
        print(f"Unsupported action: {action}")
        return 1
    if len(remaining) < 2:
        print(f"Program name required for action: {action}")
        return 1

    program = remaining[1]
    if program not in configured:
        print(f"No such process: {program}")
        return 1
    if not daemon_running:
        print("supervisord is not running")
        return 1

    _queue_command(runtime_dir, action, program)
    time.sleep(2)
    return _print_status(read_state(runtime_dir))


if __name__ == "__main__":
    raise SystemExit(main())
