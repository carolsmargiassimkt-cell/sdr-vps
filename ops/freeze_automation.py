from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.automation_freeze import (
    DEFAULT_TIMEZONE,
    freeze_reason,
    freeze_resume_at,
    is_automation_freeze_active,
    load_automation_freeze,
    save_automation_freeze,
)


def _timestamp() -> str:
    return datetime.now().isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description="Controla o freeze operacional do bot/CRM.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    enable = subparsers.add_parser("enable")
    enable.add_argument("--resume-at", required=True)
    enable.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    enable.add_argument("--reason", default="holiday_freeze")
    enable.add_argument("--drop-inbound", action="store_true")
    enable.add_argument(
        "--service",
        action="append",
        default=[],
        help="Servico congelado: all, whatsapp, sdr-bot, enrichment, inbound",
    )

    disable = subparsers.add_parser("disable")
    disable.add_argument("--reason", default="manual_resume")

    can_start = subparsers.add_parser("can-start")
    can_start.add_argument("--service", required=True)

    status = subparsers.add_parser("status")
    status.add_argument("--service", default="")

    args = parser.parse_args()

    if args.command == "enable":
        services = [str(item or "").strip().lower() for item in list(args.service or []) if str(item or "").strip()]
        payload = load_automation_freeze()
        payload.update(
            {
                "active": True,
                "resume_at": str(args.resume_at or "").strip(),
                "timezone": str(args.timezone or DEFAULT_TIMEZONE).strip() or DEFAULT_TIMEZONE,
                "services": services or ["all"],
                "drop_inbound": bool(args.drop_inbound),
                "reason": str(args.reason or "holiday_freeze").strip() or "holiday_freeze",
                "updated_at": _timestamp(),
                "created_at": str(payload.get("created_at") or _timestamp()),
            }
        )
        save_automation_freeze(payload)
        print(
            f"[FREEZE_ENABLED] active=1 resume_at={payload['resume_at']} timezone={payload['timezone']} "
            f"services={','.join(payload['services'])} drop_inbound={int(payload['drop_inbound'])} "
            f"reason={payload['reason']}"
        )
        return 0

    if args.command == "disable":
        payload = load_automation_freeze()
        payload.update(
            {
                "active": False,
                "drop_inbound": False,
                "updated_at": _timestamp(),
                "disabled_reason": str(args.reason or "manual_resume").strip() or "manual_resume",
            }
        )
        save_automation_freeze(payload)
        print(f"[FREEZE_DISABLED] active=0 reason={payload['disabled_reason']}")
        return 0

    if args.command == "can-start":
        allowed = not is_automation_freeze_active(service=str(args.service or "").strip().lower())
        print(
            f"[FREEZE_CAN_START] service={str(args.service or '').strip().lower()} "
            f"allowed={int(allowed)} resume_at={freeze_resume_at()} reason={freeze_reason() or '-'}"
        )
        return 0 if allowed else 1

    active = is_automation_freeze_active(service=str(args.service or "").strip().lower())
    payload = load_automation_freeze()
    print(
        f"[FREEZE_STATUS] active={int(active)} raw_active={int(bool(payload.get('active')))} "
        f"service={str(args.service or '').strip().lower() or 'all'} "
        f"resume_at={freeze_resume_at() or '-'} services={','.join(payload.get('services') or []) or '-'} "
        f"drop_inbound={int(bool(payload.get('drop_inbound')))} reason={freeze_reason() or '-'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
