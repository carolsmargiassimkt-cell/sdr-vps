from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

from crm.pipedrive_client import PipedriveClient
from enrich_missing_cnpj_from_xlsx import normalize_cnpj


ACTIONS_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\reconcile_archived_and_crossfunnel_live_actions.csv")
ENRICHED_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_oportunidados_enriched.csv")
REPORT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\fix_rescued_entry_contamination_report.json")
ORG_CNPJ_FIELD_KEY = "aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0"


def str_value(value: Any) -> str:
    return str(value or "").strip()


def int_value(value: Any) -> int:
    try:
        return int(str(value or "0").strip())
    except Exception:
        return 0


def normalize_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str_value(value).lower())


def clean_address(value: Any) -> str:
    text = str_value(value)
    if text.lower().startswith("fat: n/a") or text.lower().startswith("verificado via casa"):
        return ""
    return text


def load_rescue_actions() -> List[Dict[str, str]]:
    with ACTIONS_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle) if str_value(row.get("action")) == "rescue_to_entry"]


def load_enriched_rows() -> Dict[int, Dict[str, str]]:
    if not ENRICHED_CSV.exists():
        return {}
    with ENRICHED_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = {}
        for row in csv.DictReader(handle):
            deal_id = int_value(row.get("deal_id"))
            if deal_id:
                rows[deal_id] = dict(row)
        return rows


def org_cnpj(org: Dict[str, Any]) -> str:
    return normalize_cnpj(org.get(ORG_CNPJ_FIELD_KEY) or org.get("cnpj"))


def choose_target_org(client: PipedriveClient, target_title: str, enriched: Dict[str, str]) -> Dict[str, Any]:
    cnpj = normalize_cnpj(enriched.get("cnpj_enriquecido") or enriched.get("cnpj"))
    if cnpj:
        org = client.find_organization_by_cnpj(cnpj)
        if org and normalize_cnpj(org.get(ORG_CNPJ_FIELD_KEY) or org.get("cnpj")) == cnpj:
            return dict(org)
    for item in client.find_organization_by_name(target_title):
        org = dict(item.get("item") or {})
        if normalize_key(org.get("name")) == normalize_key(target_title) and bool(org.get("active_flag", True)):
            return org
    payload = {"name": target_title}
    if cnpj:
        payload[ORG_CNPJ_FIELD_KEY] = cnpj
    created = client.create_organization(payload)
    return dict(created or {})


def build_org_payload(target_title: str, enriched: Dict[str, str], current_org: Dict[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if target_title and normalize_key(current_org.get("name")) != normalize_key(target_title):
        payload["name"] = target_title
    cnpj = normalize_cnpj(enriched.get("cnpj_enriquecido") or enriched.get("cnpj"))
    if cnpj and org_cnpj(current_org) != cnpj:
        payload[ORG_CNPJ_FIELD_KEY] = cnpj
    address = clean_address(enriched.get("opp_endereco"))
    if address:
        current_address = clean_address(current_org.get("address") or current_org.get("address_formatted_address"))
        if normalize_key(current_address) != normalize_key(address):
            payload["address"] = address[:255]
    return payload


def save_report(payload: Dict[str, Any]) -> None:
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--delay", type=float, default=0.9)
    args = parser.parse_args()

    PipedriveClient.GLOBAL_MIN_INTERVAL_SEC = max(0.2, float(args.delay))
    client = PipedriveClient()
    actions = load_rescue_actions()
    enriched_rows = load_enriched_rows()
    counters: Counter[str] = Counter()
    report_actions: List[Dict[str, Any]] = []

    for row in actions:
        deal_id = int_value(row.get("deal_id"))
        target_title = str_value(row.get("clean_title") or row.get("title"))
        if not deal_id or not target_title:
            continue
        deal = client.get_deal_details(deal_id)
        if not deal:
            counters["missing_deal"] += 1
            continue
        current_org_field = deal.get("org_id")
        current_org_id = int_value(current_org_field.get("value") if isinstance(current_org_field, dict) else current_org_field)
        current_org = client.get_organization(current_org_id) if current_org_id else {}
        enriched = enriched_rows.get(deal_id, {})
        current_org_name = str_value(current_org.get("name") or (current_org_field.get("name") if isinstance(current_org_field, dict) else ""))

        target_org = current_org if current_org and normalize_key(current_org_name) == normalize_key(target_title) else choose_target_org(client, target_title, enriched)
        target_org_id = int_value(target_org.get("id"))

        if normalize_key(deal.get("title")) != normalize_key(target_title):
            counters["deal_title_plan"] += 1
            if args.apply and client.update_deal(deal_id, {"title": target_title}):
                counters["deal_title_ok"] += 1

        if target_org_id and target_org_id != current_org_id:
            counters["deal_org_relink_plan"] += 1
            if args.apply and client.update_deal(deal_id, {"org_id": target_org_id}):
                counters["deal_org_relink_ok"] += 1

        if target_org_id:
            refreshed_org = client.get_organization(target_org_id)
            org_payload = build_org_payload(target_title, enriched, refreshed_org or target_org)
            if org_payload:
                counters["org_update_plan"] += 1
                if args.apply and client.update_organization(target_org_id, org_payload):
                    counters["org_update_ok"] += 1

        report_actions.append(
            {
                "deal_id": deal_id,
                "target_title": target_title,
                "current_title": str_value(deal.get("title")),
                "current_org_id": current_org_id,
                "current_org_name": current_org_name,
                "target_org_id": target_org_id,
                "target_org_name": str_value(target_org.get("name")),
            }
        )

    save_report({"apply": bool(args.apply), "summary": dict(sorted(counters.items())), "actions": report_actions})
    print("[FIX_RESCUED_SUMMARY] " + " ".join(f"{k}={v}" for k, v in sorted(counters.items())), flush=True)
    print(f"[FIX_RESCUED_REPORT] {REPORT_JSON}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
