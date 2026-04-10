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


PIPELINE_SALES_ID = 1
PIPELINE_PROSPECTION_ID = 2
ENTRY_STAGE_ID = 8
ARCHIVED_STAGE_ID = 50
RECENT_DELETE_CUTOFF = datetime(2026, 3, 1, tzinfo=timezone.utc)
PRECIOSO_DUPLICATE_IDS = {508, 510, 511, 512, 523}
REPORT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\reconcile_archived_and_crossfunnel_live_report.json")
ACTION_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\reconcile_archived_and_crossfunnel_live_actions.csv")
TITLE_MAP_CSV = Path(r"C:\Users\Asus\deal_audit_output\crm_reorg_oldest_master.csv")


def log(message: str) -> None:
    print(str(message), flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--delay", type=float, default=0.8)
    return parser.parse_args()


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def int_value(value: Any) -> int:
    try:
        return int(str(value or "0").strip())
    except Exception:
        return 0


def str_value(value: Any) -> str:
    return str(value or "").strip()


def parse_dt(value: Any) -> datetime:
    text = str_value(value)
    if not text:
        return datetime.min.replace(tzinfo=timezone.utc)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S"):
        try:
            parsed = datetime.strptime(text[:19], fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def deal_org_id(deal: Dict[str, Any]) -> int:
    org = deal.get("org_id")
    if isinstance(org, dict):
        return int_value(org.get("value") or org.get("id"))
    return int_value(org)


def deal_org_name(deal: Dict[str, Any]) -> str:
    org = deal.get("org_id")
    if isinstance(org, dict):
        return str_value(org.get("name"))
    return ""


def clean_title(value: Any) -> str:
    text = str_value(value)
    text = re.sub(r"^\s*lead\s+mand\s+digital\s*-\s*", "", text, flags=re.I)
    text = re.sub(r"^\s*lead\s+mand\s+digital\s*", "", text, flags=re.I)
    text = re.sub(r"\s*-\s*neg[oó]cio\s*$", "", text, flags=re.I)
    text = re.sub(r"\s+neg[oó]cio\s*$", "", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip(" -")
    return text


def normalize_key(value: Any) -> str:
    text = clean_title(value).lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def is_obvious_junk_title(value: Any) -> bool:
    clean = clean_title(value).strip().lower()
    key = normalize_key(clean)
    if not key:
        return True
    if key in {"negocio", "teste"}:
        return True
    if clean.startswith("dup "):
        return True
    if " teste" in clean or clean.endswith(" teste"):
        return True
    return False


def build_action_row(deal: Dict[str, Any], action: str, note: str) -> Dict[str, Any]:
    return {
        "deal_id": int_value(deal.get("id")),
        "title": str_value(deal.get("title")),
        "clean_title": clean_title(deal.get("title")),
        "org_name": deal_org_name(deal),
        "pipeline_id": int_value(deal.get("pipeline_id")),
        "stage_id": int_value(deal.get("stage_id")),
        "status": str_value(deal.get("status")).lower(),
        "add_time": str_value(deal.get("add_time")),
        "action": action,
        "note": note,
    }


def load_target_title_map() -> Dict[int, str]:
    mapping: Dict[int, str] = {}
    if not TITLE_MAP_CSV.exists():
        return mapping
    with TITLE_MAP_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            deal_id = int_value(row.get("DealId"))
            title = str_value(row.get("TargetDealTitle") or row.get("StandardName"))
            if deal_id and title:
                mapping[deal_id] = title
    return mapping


def save_report(report: Dict[str, Any], actions: List[Dict[str, Any]]) -> None:
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = [
        "deal_id",
        "title",
        "clean_title",
        "org_name",
        "pipeline_id",
        "stage_id",
        "status",
        "add_time",
        "action",
        "note",
    ]
    with ACTION_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in actions:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def main() -> int:
    args = parse_args()
    PipedriveClient.GLOBAL_MIN_INTERVAL_SEC = max(0.2, float(args.delay))
    client = PipedriveClient()
    deals = client.get_deals(status="all_not_deleted", limit=3000)
    target_title_map = load_target_title_map()

    open_deals = [dict(deal) for deal in deals if str_value(deal.get("status")).lower() == "open"]
    archived_p2 = [
        dict(deal)
        for deal in deals
        if int_value(deal.get("pipeline_id")) == PIPELINE_PROSPECTION_ID
        and (str_value(deal.get("status")).lower() == "lost" or int_value(deal.get("stage_id")) == ARCHIVED_STAGE_ID)
    ]

    open_by_title: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    open_by_org: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    open_by_title_pipeline: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for deal in open_deals:
        title_key = normalize_key(deal.get("title"))
        org_key = normalize_key(deal_org_name(deal))
        if title_key:
            open_by_title[title_key].append(deal)
            open_by_title_pipeline[title_key].append(deal)
        if org_key:
            open_by_org[org_key].append(deal)

    actions: List[Dict[str, Any]] = []
    counters: Counter[str] = Counter()

    for deal in sorted(archived_p2, key=lambda item: (parse_dt(item.get("add_time")), int_value(item.get("id")))):
        deal_id = int_value(deal.get("id"))
        title_key = normalize_key(deal.get("title"))
        org_key = normalize_key(deal_org_name(deal))
        same_title_open = bool(title_key and open_by_title.get(title_key))
        same_org_open = bool(org_key and open_by_org.get(org_key))
        has_repeat = same_title_open or same_org_open
        add_time = parse_dt(deal.get("add_time"))
        recent_duplicate = bool(has_repeat and add_time >= RECENT_DELETE_CUTOFF)
        explicit_delete = deal_id in PRECIOSO_DUPLICATE_IDS

        if not has_repeat:
            if is_obvious_junk_title(target_title_map.get(deal_id) or deal.get("title")):
                counters["move_to_lost_junk_plan"] += 1
                actions.append(build_action_row(deal, "move_to_lost_junk", "titulo claramente lixo/teste"))
                if args.apply:
                    payload = {"status": "lost", "stage_id": ENTRY_STAGE_ID}
                    clean = str_value(target_title_map.get(deal_id)) or clean_title(deal.get("title"))
                    if clean:
                        payload["title"] = clean
                    ok = client.update_deal(deal_id, payload)
                    counters["move_to_lost_junk_ok" if ok else "move_to_lost_junk_fail"] += 1
                continue
            counters["rescue_to_entry_plan"] += 1
            action = build_action_row(deal, "rescue_to_entry", "sem repeticao aberta em nenhum funil")
            actions.append(action)
            if args.apply:
                payload = {"status": "open", "stage_id": ENTRY_STAGE_ID}
                clean = str_value(target_title_map.get(deal_id)) or clean_title(deal.get("title"))
                if clean:
                    payload["title"] = clean
                ok = client.update_deal(deal_id, payload)
                counters["rescue_to_entry_ok" if ok else "rescue_to_entry_fail"] += 1
            continue

        reason = "duplicado explicito do Precioso" if explicit_delete else (
            "duplicado recente com aberto em marco/abril" if recent_duplicate else "duplicado com deal aberto"
        )
        counters["move_to_lost_duplicate_plan"] += 1
        actions.append(build_action_row(deal, "move_to_lost_duplicate", reason))
        if args.apply:
            payload = {"status": "lost", "stage_id": ENTRY_STAGE_ID}
            clean = str_value(target_title_map.get(deal_id)) or clean_title(deal.get("title"))
            if clean:
                payload["title"] = clean
            ok = client.update_deal(deal_id, payload)
            counters["move_to_lost_duplicate_ok" if ok else "move_to_lost_duplicate_fail"] += 1

    grouped_open: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for deal in open_deals:
        title_key = normalize_key(deal.get("title"))
        if title_key:
            grouped_open[title_key].append(deal)

    for title_key, items in sorted(grouped_open.items()):
        pipelines = {int_value(item.get("pipeline_id")) for item in items}
        if PIPELINE_SALES_ID not in pipelines or PIPELINE_PROSPECTION_ID not in pipelines:
            continue
        prospection_items = [item for item in items if int_value(item.get("pipeline_id")) == PIPELINE_PROSPECTION_ID]
        sales_items = [item for item in items if int_value(item.get("pipeline_id")) == PIPELINE_SALES_ID]
        if not prospection_items or not sales_items:
            continue
        for deal in prospection_items:
            deal_id = int_value(deal.get("id"))
            counters["cross_funnel_move_to_lost_plan"] += 1
            note = f"duplicado aberto; manter vendas {[int_value(item.get('id')) for item in sales_items]}"
            actions.append(build_action_row(deal, "move_to_lost_cross_funnel_duplicate", note))
            if args.apply:
                ok = client.update_deal(deal_id, {"status": "lost", "stage_id": max(1, int_value(deal.get("stage_id")) or ENTRY_STAGE_ID)})
                counters["cross_funnel_move_to_lost_ok" if ok else "cross_funnel_move_to_lost_fail"] += 1

    report = {
        "generated_at": utcnow_iso(),
        "apply": bool(args.apply),
        "archived_pipeline2_scanned": len(archived_p2),
        "open_scanned": len(open_deals),
        "summary": dict(sorted(counters.items())),
        "report_csv": str(ACTION_CSV),
    }
    save_report(report, actions)
    log("[RECONCILE_SUMMARY] " + " ".join(f"{k}={v}" for k, v in sorted(counters.items())))
    log(f"[RECONCILE_REPORT] json={REPORT_JSON} csv={ACTION_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
