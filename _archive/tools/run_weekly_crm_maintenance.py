from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.daily_crm_sync import main as daily_crm_sync_main
from tools.weekly_crm_cleanup import main as weekly_crm_cleanup_main


def main(argv: list[str] | None = None) -> int:
    argv = list(argv or sys.argv[1:])
    dry_run = "--dry-run" in argv
    sync_args = ["--dry-run"] if dry_run else []
    cleanup_args = ["--dry-run"] if dry_run else []
    sync_code = int(daily_crm_sync_main(sync_args))
    if sync_code != 0:
        return sync_code
    cleanup_code = int(weekly_crm_cleanup_main(cleanup_args))
    if cleanup_code != 0:
        return cleanup_code
    print("[WEEKLY_CRM_MAINTENANCE_OK]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
