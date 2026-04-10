from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.build_local_crm_base import main as build_local_base
from tools.enrich_local_crm_base import main as enrich_local_base
from tools.export_local_crm_base_csv import main as export_local_base

LOCAL_BASE_FILE = PROJECT_ROOT / "data" / "local_crm_base.json"
FRESHNESS_DAYS = 6


def _base_recently_enriched() -> bool:
    if not LOCAL_BASE_FILE.exists():
        return False
    try:
        payload = json.loads(LOCAL_BASE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return False
    meta = payload.get("meta") or {}
    enriched_at = str(meta.get("casa_dos_dados_enriched_at") or meta.get("updated_at") or "").strip()
    if not enriched_at:
        return False
    try:
        enriched_dt = datetime.fromisoformat(enriched_at.replace("Z", "+00:00"))
    except Exception:
        return False
    if enriched_dt.tzinfo is None:
        enriched_dt = enriched_dt.replace(tzinfo=timezone.utc)
    return enriched_dt >= datetime.now(timezone.utc) - timedelta(days=FRESHNESS_DAYS)


def main(argv: list[str] | None = None) -> int:
    argv = list(argv or sys.argv[1:])
    force = "--force" in argv
    if not force and _base_recently_enriched():
        export_code = int(export_local_base())
        if export_code != 0:
            return export_code
        print("[WEEKLY_BASE_PIPELINE_SKIP] base_recente")
        return 0
    build_code = int(build_local_base())
    if build_code != 0:
        return build_code
    enrich_args = ["--force"] if force else []
    enrich_code = int(enrich_local_base(enrich_args))
    if enrich_code not in {0, 1}:
        return enrich_code
    export_code = int(export_local_base())
    if export_code != 0:
        return export_code
    print("[WEEKLY_BASE_PIPELINE_OK]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
