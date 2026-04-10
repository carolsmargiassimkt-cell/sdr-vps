from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from crm.pipedrive_client import PipedriveClient


INPUT_CSV = Path(r"C:\Users\Asus\deal_audit_output\crm_reorg_oldest_master.csv")
REPORT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\snapshot_deal_cleanup_report.json")
WON_REPORT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\won_deals_preserved.csv")
PIPELINE_ID = 2
ENTRY_STAGE_ID = 8
ARCHIVED_STAGE_ID = 50


def log(message: str) -> None:
    print(str(message), flush=True)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit-groups", type=int, default=0)
    parser.add_argument("--delay", type=float, default=0.35)
    return parser.parse_args()


def load_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def parse_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "sim"}


def parse_dt(value: Any) -> datetime:
    text = str(value or "").strip()
    if not text:
        return datetime.max.replace(tzinfo=timezone.utc)
    for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return datetime.max.replace(tzinfo=timezone.utc)


def int_value(value: Any) -> int:
    try:
        return int(str(value or "0").strip())
    except Exception:
        return 0


def fetch_live_deals(client: PipedriveClient) -> Dict[int, Dict[str, Any]]:
    live: Dict[int, Dict[str, Any]] = {}
    for deal in client.get_deals(status="all_not_deleted", limit=3000):
        deal_id = int_value(deal.get("id"))
        if deal_id:
            live[deal_id] = dict(deal)
    return live


def choose_group_master(rows: List[Dict[str, str]], live_by_id: Dict[int, Dict[str, Any]]) -> Dict[str, str]:
    candidates = []
    won_candidates = []
    for row in rows:
        deal_id = int_value(row.get("DealId"))
        live = live_by_id.get(deal_id)
        if not live:
            continue
        enriched = (parse_dt(live.get("add_time") or row.get("StageChanged")), deal_id, row)
        candidates.append(enriched)
        if str(live.get("status") or "").strip().lower() == "won":
            won_candidates.append(enriched)
    if won_candidates:
        return min(won_candidates, key=lambda item: (item[0], item[1]))[2]
    if candidates:
        return min(candidates, key=lambda item: (item[0], item[1]))[2]
    master_row = next((row for row in rows if parse_bool(row.get("IsMaster"))), None)
    return master_row or rows[0]


def update_deal_title(client: PipedriveClient, deal_id: int, target_title: str, apply: bool) -> bool:
    if not deal_id or not str(target_title or "").strip():
        return False
    if not apply:
        return True
    return client.update_deal(deal_id, {"title": str(target_title).strip()})


def rescue_master(client: PipedriveClient, deal_id: int, target_title: str, apply: bool) -> bool:
    payload = {"status": "open", "stage_id": ENTRY_STAGE_ID}
    if str(target_title or "").strip():
        payload["title"] = str(target_title).strip()
    if not apply:
        return True
    return client.update_deal(deal_id, payload)


def archive_duplicate(client: PipedriveClient, deal_id: int, apply: bool) -> bool:
    if not apply:
        return True
    return client.update_deal(deal_id, {"status": "lost", "stage_id": ARCHIVED_STAGE_ID})


def delete_duplicate(client: PipedriveClient, deal_id: int, apply: bool) -> bool:
    if not apply:
        return True
    return client.delete_deal(deal_id)


def write_won_report(rows: List[Dict[str, Any]]) -> None:
    WON_REPORT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["CoreTitle", "DealId", "Status", "Title", "StageId", "Action"]
    with WON_REPORT_CSV.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def save_report(payload: Dict[str, Any]) -> None:
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    rows = load_rows(INPUT_CSV)
    grouped_rows: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped_rows[str(row.get("CoreTitle") or "").strip()].append(row)
    group_items = sorted(grouped_rows.items(), key=lambda item: item[0])
    if args.limit_groups:
        group_items = group_items[: max(1, int(args.limit_groups))]

    PipedriveClient.GLOBAL_MIN_INTERVAL_SEC = max(0.2, float(args.delay))
    client = PipedriveClient()
    live_by_id = fetch_live_deals(client)
    counters: Counter[str] = Counter()
    won_rows: List[Dict[str, Any]] = []
    action_log: List[Dict[str, Any]] = []

    for index, (core_title, group_rows) in enumerate(group_items, start=1):
        master_row = choose_group_master(group_rows, live_by_id)
        master_id = int_value(master_row.get("DealId"))
        target_title = str(master_row.get("TargetDealTitle") or master_row.get("StandardName") or "").strip()

        for row in group_rows:
            deal_id = int_value(row.get("DealId"))
            live = live_by_id.get(deal_id)
            if not live:
                counters["missing_live"] += 1
                action_log.append({"core_title": core_title, "deal_id": deal_id, "action": "missing_live"})
                continue

            status = str(live.get("status") or "").strip().lower()
            pipeline_id = int_value(live.get("pipeline_id"))
            stage_id = int_value(live.get("stage_id"))
            title = str(live.get("title") or "").strip()
            is_master = deal_id == master_id

            if status == "won":
                counters["won_preserved"] += 1
                won_rows.append(
                    {
                        "CoreTitle": core_title,
                        "DealId": deal_id,
                        "Status": status,
                        "Title": title,
                        "StageId": stage_id,
                        "Action": "preserved_won",
                    }
                )
                action_log.append({"core_title": core_title, "deal_id": deal_id, "action": "preserved_won"})
                continue

            if pipeline_id != PIPELINE_ID:
                counters["outside_pipeline"] += 1
                action_log.append({"core_title": core_title, "deal_id": deal_id, "action": "outside_pipeline"})
                continue

            row_action = str(row.get("Action") or "").strip()
            if is_master:
                if row_action == "resgatar_master_antigo_e_padronizar":
                    counters["master_rescue_plan"] += 1
                    ok = rescue_master(client, deal_id, target_title, args.apply)
                    counters["master_rescue_ok" if ok else "master_rescue_fail"] += 1
                    action_log.append({"core_title": core_title, "deal_id": deal_id, "action": "master_rescue", "ok": ok})
                else:
                    if target_title and title != target_title:
                        counters["master_title_plan"] += 1
                        ok = update_deal_title(client, deal_id, target_title, args.apply)
                        counters["master_title_ok" if ok else "master_title_fail"] += 1
                        action_log.append({"core_title": core_title, "deal_id": deal_id, "action": "master_title", "ok": ok})
                    else:
                        counters["master_kept"] += 1
                        action_log.append({"core_title": core_title, "deal_id": deal_id, "action": "master_kept"})
                continue

            if row_action == "arquivar_duplicado_mais_novo":
                counters["archive_plan"] += 1
                ok = archive_duplicate(client, deal_id, args.apply)
                counters["archive_ok" if ok else "archive_fail"] += 1
                action_log.append({"core_title": core_title, "deal_id": deal_id, "action": "archive_duplicate", "ok": ok})
            elif row_action == "deletar_duplicado_arquivado":
                if status == "lost" or stage_id == ARCHIVED_STAGE_ID:
                    counters["delete_plan"] += 1
                    ok = delete_duplicate(client, deal_id, args.apply)
                    counters["delete_ok" if ok else "delete_fail"] += 1
                    action_log.append({"core_title": core_title, "deal_id": deal_id, "action": "delete_duplicate", "ok": ok})
                else:
                    counters["delete_skipped_not_archived"] += 1
                    action_log.append(
                        {"core_title": core_title, "deal_id": deal_id, "action": "delete_skipped_not_archived", "status": status, "stage_id": stage_id}
                    )
            else:
                counters["row_action_ignored"] += 1
                action_log.append({"core_title": core_title, "deal_id": deal_id, "action": "row_action_ignored", "row_action": row_action})

        if index % 50 == 0:
            log(f"[PROGRESS] groups={index}/{len(group_items)}")

    write_won_report(won_rows)
    report = {
        "generated_at": utcnow_iso(),
        "apply": bool(args.apply),
        "group_count": len(group_items),
        "input_rows": len(rows),
        "summary": dict(sorted(counters.items())),
        "won_preserved_count": len(won_rows),
        "won_report_csv": str(WON_REPORT_CSV),
        "actions": action_log,
    }
    save_report(report)
    log("[SNAPSHOT_CLEANUP_SUMMARY] " + " ".join(f"{key}={value}" for key, value in sorted(counters.items())))
    log(f"[SNAPSHOT_CLEANUP_WON] count={len(won_rows)} csv={WON_REPORT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
