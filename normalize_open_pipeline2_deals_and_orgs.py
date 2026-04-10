from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from crm.pipedrive_client import PipedriveClient


SNAPSHOT_CSV = Path(r"C:\Users\Asus\deal_audit_output\crm_reorg_oldest_master.csv")
REPORT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\normalize_open_pipeline2_report.json")
PIPELINE_ID = 2


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


def normalize_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def clean_title(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"^\s*lead\s+mand\s+digital\s*-\s*", "", text, flags=re.I)
    text = re.sub(r"^\s*lead\s+mand\s+digital\s*", "", text, flags=re.I)
    text = re.sub(r"\s*-\s*neg[oó]cio\s*$", "", text, flags=re.I)
    text = re.sub(r"\s+neg[oó]cio\s*$", "", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip(" -")
    return text


def deal_org_id(deal: Dict[str, Any]) -> int:
    org = deal.get("org_id")
    if isinstance(org, dict):
        return int_value(org.get("value") or org.get("id"))
    return int_value(org)


def deal_org_name(deal: Dict[str, Any]) -> str:
    org = deal.get("org_id")
    if isinstance(org, dict):
        return str(org.get("name") or "").strip()
    return ""


def load_snapshot_targets() -> Dict[int, str]:
    targets: Dict[int, str] = {}
    with SNAPSHOT_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            deal_id = int_value(row.get("DealId"))
            target = str(row.get("TargetDealTitle") or row.get("StandardName") or "").strip()
            if deal_id and target:
                targets[deal_id] = target
    return targets


def fetch_open_pipeline2_deals(client: PipedriveClient) -> List[Dict[str, Any]]:
    deals = client.get_deals(status="all_not_deleted", limit=3000)
    return [
        dict(deal)
        for deal in deals
        if str(deal.get("status") or "").strip().lower() == "open"
        and int_value(deal.get("pipeline_id")) == PIPELINE_ID
    ]


def get_org_safe(client: PipedriveClient, org_id: int, cache: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
    if org_id <= 0:
        return {}
    if org_id not in cache:
        cache[org_id] = client.get_organization(org_id)
    return cache[org_id]


def choose_existing_org(client: PipedriveClient, target_name: str, search_cache: Dict[str, int]) -> int:
    key = normalize_key(target_name)
    if not key:
        return 0
    cached = search_cache.get(key, 0)
    if cached:
        return cached
    items = client.find_organization_by_name(target_name)
    for item in items:
        organization = dict(item.get("item") or {})
        org_id = int_value(organization.get("id"))
        org_name = str(organization.get("name") or "").strip()
        if org_id and normalize_key(org_name) == key and bool(organization.get("active_flag", True)):
            search_cache[key] = org_id
            return org_id
    return 0


def save_report(payload: Dict[str, Any]) -> None:
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    PipedriveClient.GLOBAL_MIN_INTERVAL_SEC = max(0.2, float(args.delay))
    client = PipedriveClient()
    snapshot_targets = load_snapshot_targets()
    deals = fetch_open_pipeline2_deals(client)
    if args.limit:
        deals = deals[: max(1, int(args.limit))]

    org_title_map: Dict[int, set[str]] = defaultdict(set)
    for deal in deals:
        org_id = deal_org_id(deal)
        title = str(deal.get("title") or "").strip()
        if org_id and title:
            org_title_map[org_id].add(normalize_key(clean_title(title)))

    org_cache: Dict[int, Dict[str, Any]] = {}
    search_cache: Dict[str, int] = {}
    counters: Counter[str] = Counter()
    actions: List[Dict[str, Any]] = []

    for index, deal in enumerate(deals, start=1):
        deal_id = int_value(deal.get("id"))
        current_title = str(deal.get("title") or "").strip()
        target_name = str(snapshot_targets.get(deal_id) or clean_title(current_title)).strip()
        current_org_id = deal_org_id(deal)
        current_org_name = deal_org_name(deal)
        title_dirty = current_title != target_name and bool(target_name)
        org_dirty = normalize_key(target_name) != normalize_key(current_org_name)
        missing_org = current_org_id <= 0

        if not target_name:
            counters["missing_target_name"] += 1
            actions.append({"deal_id": deal_id, "action": "missing_target_name", "title": current_title})
            continue

        if not (title_dirty or org_dirty or missing_org):
            counters["already_ok"] += 1
            continue

        if title_dirty:
            counters["deal_title_plan"] += 1
            ok = True if not args.apply else client.update_deal(deal_id, {"title": target_name})
            counters["deal_title_ok" if ok else "deal_title_fail"] += 1
            actions.append({"deal_id": deal_id, "action": "deal_title", "ok": ok, "from": current_title, "to": target_name})

        current_org = get_org_safe(client, current_org_id, org_cache) if current_org_id else {}
        current_org_active = bool(current_org.get("active_flag")) if current_org else False
        shared_org = len({item for item in org_title_map.get(current_org_id, set()) if item}) > 1
        resolved_org_id = 0

        if current_org_id and current_org_active and not shared_org:
            counters["org_update_plan"] += 1
            ok = True if not args.apply else client.update_organization(current_org_id, {"name": target_name})
            counters["org_update_ok" if ok else "org_update_fail"] += 1
            actions.append(
                {
                    "deal_id": deal_id,
                    "org_id": current_org_id,
                    "action": "org_update",
                    "ok": ok,
                    "from": str(current_org.get("name") or ""),
                    "to": target_name,
                }
            )
            resolved_org_id = current_org_id
        else:
            if current_org_id and not current_org_active:
                counters["org_deleted_or_inactive"] += 1
            elif shared_org:
                counters["org_shared_conflict"] += 1
            elif missing_org:
                counters["org_missing_link"] += 1

            resolved_org_id = choose_existing_org(client, target_name, search_cache)
            if resolved_org_id:
                counters["org_reuse_found"] += 1
            else:
                counters["org_create_plan"] += 1
                created = {"id": 0}
                if args.apply:
                    created = client.create_organization({"name": target_name})
                else:
                    created = {"id": -(deal_id or 1)}
                resolved_org_id = int_value(created.get("id"))
                if resolved_org_id:
                    counters["org_create_ok"] += 1
                    search_cache[normalize_key(target_name)] = resolved_org_id
                else:
                    counters["org_create_fail"] += 1
                actions.append({"deal_id": deal_id, "action": "org_create", "ok": resolved_org_id > 0 or not args.apply, "target": target_name})

            if resolved_org_id and resolved_org_id != current_org_id:
                counters["deal_org_relink_plan"] += 1
                ok = True if not args.apply else client.update_deal(deal_id, {"org_id": resolved_org_id})
                counters["deal_org_relink_ok" if ok else "deal_org_relink_fail"] += 1
                actions.append(
                    {
                        "deal_id": deal_id,
                        "action": "deal_org_relink",
                        "ok": ok,
                        "from_org_id": current_org_id,
                        "to_org_id": resolved_org_id,
                        "target": target_name,
                    }
                )

        if index % 50 == 0:
            log(f"[NORMALIZE_PROGRESS] {index}/{len(deals)}")

    report = {
        "generated_at": utcnow_iso(),
        "apply": bool(args.apply),
        "deals_scanned": len(deals),
        "summary": dict(sorted(counters.items())),
        "actions": actions,
    }
    save_report(report)
    log("[NORMALIZE_SUMMARY] " + " ".join(f"{key}={value}" for key, value in sorted(counters.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
