from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from crm.pipedrive_client import PipedriveClient


def log(message: str) -> None:
    print(str(message), flush=True)


def compute_deadline(*, timezone_name: str, deadline_hour: int) -> datetime:
    tz = ZoneInfo(timezone_name)
    now = datetime.now(tz)
    deadline = now.replace(hour=int(deadline_hour), minute=0, second=0, microsecond=0)
    if now >= deadline:
        deadline += timedelta(days=1)
    return deadline


def main() -> int:
    parser = argparse.ArgumentParser(description="Espera a API do CRM liberar antes de iniciar um app PM2.")
    parser.add_argument("--pm2-app", required=True)
    parser.add_argument("--pm2-config", default="/root/sdr-vps/ecosystem.vps.config.js")
    parser.add_argument("--pm2-bin", default="/usr/local/bin/pm2")
    parser.add_argument("--poll-sec", type=int, default=300)
    parser.add_argument("--deadline-hour", type=int, default=8)
    parser.add_argument("--timezone", default="America/Sao_Paulo")
    args = parser.parse_args()

    deadline = compute_deadline(timezone_name=str(args.timezone), deadline_hour=int(args.deadline_hour))
    client = PipedriveClient()
    wait_floor = max(60, int(args.poll_sec or 300))

    while True:
        now = datetime.now(deadline.tzinfo)
        if now >= deadline:
            log(
                f"[CRM_GATE_ABORT] app={args.pm2_app} motivo=deadline now={now.isoformat()} "
                f"deadline={deadline.isoformat()}"
            )
            return 1

        if client.test_connection():
            log(
                f"[CRM_GATE_OPEN] app={args.pm2_app} now={now.isoformat()} "
                f"deadline={deadline.isoformat()}"
            )
            command = [str(args.pm2_bin), "startOrRestart", str(args.pm2_config), "--only", str(args.pm2_app)]
            completed = subprocess.run(command, check=False, capture_output=True, text=True)
            if completed.stdout.strip():
                log(completed.stdout.strip())
            if completed.stderr.strip():
                print(completed.stderr.strip(), file=sys.stderr, flush=True)
            return int(completed.returncode or 0)

        remaining_deadline = max(0, int((deadline - now).total_seconds()))
        local_cooldown = max(
            0,
            int(float(PipedriveClient._rate_limit_until_ts or 0.0) - time.time()),
        )
        wait_sec = min(remaining_deadline, max(wait_floor, local_cooldown + 1))
        log(
            f"[CRM_GATE_WAIT] app={args.pm2_app} status={int(client.last_http_status or 0)} "
            f"aguardando={wait_sec}s now={now.isoformat()} deadline={deadline.isoformat()}"
        )
        time.sleep(max(1, wait_sec))


if __name__ == "__main__":
    raise SystemExit(main())
