from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from crm.pipedrive_client import PipedriveClient


INPUT_CSV = Path(r"C:\Users\Asus\deal_audit_output\crm_reorg_oldest_master.csv")
REPORT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\snapshot_org_sync_report.json")


def log(message: str) -> None:
    print(str(message), flush=True)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--delay", type=float, default=0.35)
    return parser.parse_args()


def int_value(value: Any) -> int:
    try:
        return int(str(value or "0").strip())
    except Exception:
        return 0


def load_master_rows() -> List[Dict[str, str]]:
    with INPUT_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = [dict(row) for row in csv.DictReader(handle)]
    masters = [row for row in rows if str(row.get("IsMaster") or "").strip().lower() == "true"]
    return masters


def fetch_live_deals(client: PipedriveClient) -> Dict[int, Dict[str, Any]]:
    deals: Dict[int, Dict[str, Any]] = {}
    for deal in client.get_deals(status="all_not_deleted", limit=3000):
        deal_id = int_value(deal.get("id"))
        if deal_id:
            deals[deal_id] = dict(deal)
    return deals


def deal_org_id(deal: Dict[str, Any]) -> int:
    org_field = deal.get("org_id")
    if isinstance(org_field, dict):
        return int_value(org_field.get("value") or org_field.get("id"))
    return int_value(org_field)


def save_report(payload: Dict[str, Any]) -> None:
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    PipedriveClient.GLOBAL_MIN_INTERVAL_SEC = max(0.2, float(args.delay))
    client = PipedriveClient()
    masters = load_master_rows()
    if args.limit:
        masters = masters[: max(1, int(args.limit))]
    live_by_id = fetch_live_deals(client)
    counters: Counter[str] = Counter()
    actions: List[Dict[str, Any]] = []

    for index, row in enumerate(masters, start=1):
        deal_id = int_value(row.get("DealId"))
        live = live_by_id.get(deal_id)
        if not live:
            counters["missing_live_deal"] += 1
            actions.append({"deal_id": deal_id, "action": "missing_live_deal"})
            continue

        org_id = deal_org_id(live)
        if not org_id:
            counters["missing_org_link"] += 1
            actions.append({"deal_id": deal_id, "action": "missing_org_link"})
            continue

        target_name = str(row.get("TargetDealTitle") or row.get("TargetOrgName") or "").strip()
        if not target_name:
            counters["missing_target_name"] += 1
            actions.append({"deal_id": deal_id, "org_id": org_id, "action": "missing_target_name"})
            continue

        org = client.get_organization(org_id)
        current_name = str(org.get("name") or "").strip()
        if current_name == target_name:
            counters["org_already_ok"] += 1
            actions.append({"deal_id": deal_id, "org_id": org_id, "action": "org_already_ok"})
            continue

        counters["org_update_plan"] += 1
        ok = True if not args.apply else client.update_organization(org_id, {"name": target_name})
        counters["org_update_ok" if ok else "org_update_fail"] += 1
        actions.append(
            {
                "deal_id": deal_id,
                "org_id": org_id,
                "action": "org_update",
                "ok": ok,
                "from": current_name,
                "to": target_name,
            }
        )

        if index % 50 == 0:
            log(f"[ORG_SYNC_PROGRESS] {index}/{len(masters)}")

    report = {
        "generated_at": utcnow_iso(),
        "apply": bool(args.apply),
        "masters": len(masters),
        "summary": dict(sorted(counters.items())),
        "actions": actions,
    }
    save_report(report)
    log("[ORG_SYNC_SUMMARY] " + " ".join(f"{key}={value}" for key, value in sorted(counters.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
