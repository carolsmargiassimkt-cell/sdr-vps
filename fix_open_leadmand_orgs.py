from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List

from crm.pipedrive_client import PipedriveClient
from enrich_missing_cnpj_from_xlsx import normalize_cnpj


ENRICHED_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_oportunidados_enriched.csv")
REPORT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\fix_open_leadmand_orgs_report.json")
ORG_CNPJ_FIELD_KEY = "aa3d1d254e5f80a2e5c791cdd390ef58ccfb68f0"
PIPELINE_ID = 2
CONTAMINATED_SHARED_CNPJ = "18012676000160"


def str_value(value: Any) -> str:
    return str(value or "").strip()


def int_value(value: Any) -> int:
    try:
        return int(str(value or "0").strip())
    except Exception:
        return 0


def normalize_key(value: Any) -> str:
    text = str_value(value).lower()
    text = re.sub(r"^\s*lead\s+mand\s+digital\s*-\s*", "", text, flags=re.I)
    text = re.sub(r"\s*-\s*neg[oó]cio\s*$", "", text, flags=re.I)
    return re.sub(r"[^a-z0-9]+", "", text)


def title_case_name(value: str) -> str:
    text = str_value(value)
    if not text:
        return ""
    small = {"de", "da", "do", "das", "dos", "e", "em", "a", "o"}
    parts = []
    for token in text.lower().split():
        parts.append(token if token in small else token[:1].upper() + token[1:])
    return " ".join(parts)


def preferred_title(row: Dict[str, str], fallback: str) -> str:
    title = str_value(row.get("nome_final")) or fallback
    return title_case_name(title)[:180]


def preferred_address(row: Dict[str, str]) -> str:
    for candidate in (str_value(row.get("opp_endereco")), str_value(row.get("casa_endereco"))):
        low = candidate.lower()
        if candidate and not low.startswith("fat: n/a") and "verificado via casa" not in low:
            return candidate[:255]
    return ""


def load_rows() -> Dict[int, Dict[str, str]]:
    with ENRICHED_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        return {int_value(row.get("deal_id")): dict(row) for row in csv.DictReader(handle) if int_value(row.get("deal_id"))}


def org_id_of(deal: Dict[str, Any]) -> int:
    org = deal.get("org_id")
    if isinstance(org, dict):
        return int_value(org.get("value") or org.get("id"))
    return int_value(org)


def org_name_of(deal: Dict[str, Any]) -> str:
    org = deal.get("org_id")
    if isinstance(org, dict):
        return str_value(org.get("name"))
    return ""


def choose_target_org(client: PipedriveClient, title: str, cnpj: str, contaminated: bool) -> Dict[str, Any]:
    if cnpj and not contaminated:
        found = client.find_organization_by_cnpj(cnpj)
        if found and normalize_cnpj(found.get(ORG_CNPJ_FIELD_KEY) or found.get("cnpj")) == cnpj:
            return dict(found)
    for item in client.find_organization_by_name(title):
        org = dict(item.get("item") or {})
        if normalize_key(org.get("name")) == normalize_key(title) and bool(org.get("active_flag", True)):
            return org
    created = client.create_organization({"name": title, **({ORG_CNPJ_FIELD_KEY: cnpj} if cnpj and not contaminated else {})})
    return dict(created or {})


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
    rows = load_rows()
    deals = client.get_deals(status="open", limit=3000, pipeline_id=PIPELINE_ID)
    contaminated_cnpj_rows = defaultdict(list)
    for row in rows.values():
        cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
        if cnpj:
            contaminated_cnpj_rows[cnpj].append(row)

    multi_name_cnpjs = {
        cnpj
        for cnpj, items in contaminated_cnpj_rows.items()
        if cnpj == CONTAMINATED_SHARED_CNPJ
        or len({normalize_key(str_value(item.get("nome_final"))) for item in items if normalize_key(str_value(item.get("nome_final")))}) > 1
    }

    target_deals = [dict(deal) for deal in deals if org_name_of(deal).lower().startswith("lead mand digital")]
    counters: Counter[str] = Counter()
    actions: List[Dict[str, Any]] = []

    for deal in sorted(target_deals, key=lambda item: int_value(item.get("id"))):
        deal_id = int_value(deal.get("id"))
        row = rows.get(deal_id, {})
        title = preferred_title(row, str_value(deal.get("title")))
        cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
        contaminated = cnpj in multi_name_cnpjs
        address = preferred_address(row)
        current_org_id = org_id_of(deal)
        current_org_name = org_name_of(deal)

        target_org = choose_target_org(client, title, cnpj, contaminated) if args.apply else {}
        target_org_id = int_value(target_org.get("id"))
        target_org_name = str_value(target_org.get("name"))

        if normalize_key(deal.get("title")) != normalize_key(title):
            counters["deal_title_plan"] += 1
            if args.apply and client.update_deal(deal_id, {"title": title}):
                counters["deal_title_ok"] += 1

        if target_org_id and target_org_id != current_org_id:
            counters["deal_org_relink_plan"] += 1
            if args.apply and client.update_deal(deal_id, {"org_id": target_org_id}):
                counters["deal_org_relink_ok"] += 1

        if target_org_id:
            live_org = client.get_organization(target_org_id)
            payload: Dict[str, Any] = {}
            live_org_name = str_value(live_org.get("name"))
            if live_org_name.lower().startswith("lead mand digital") or normalize_key(live_org_name) != normalize_key(title):
                payload["name"] = title
            if contaminated:
                existing = normalize_cnpj(live_org.get(ORG_CNPJ_FIELD_KEY) or live_org.get("cnpj"))
                if existing == CONTAMINATED_SHARED_CNPJ:
                    payload[ORG_CNPJ_FIELD_KEY] = ""
            else:
                if cnpj and normalize_cnpj(live_org.get(ORG_CNPJ_FIELD_KEY) or live_org.get("cnpj")) != cnpj:
                    payload[ORG_CNPJ_FIELD_KEY] = cnpj
                if address and normalize_key(live_org.get("address") or live_org.get("address_formatted_address")) != normalize_key(address):
                    payload["address"] = address
            if payload:
                counters["org_update_plan"] += 1
                if args.apply and client.update_organization(target_org_id, payload):
                    counters["org_update_ok"] += 1

        actions.append(
            {
                "deal_id": deal_id,
                "current_title": str_value(deal.get("title")),
                "target_title": title,
                "current_org_id": current_org_id,
                "current_org_name": current_org_name,
                "target_org_id": target_org_id,
                "target_org_name": target_org_name,
                "cnpj": cnpj,
                "contaminated": contaminated,
                "address": address,
            }
        )

    save_report({"apply": bool(args.apply), "deal_count": len(target_deals), "summary": dict(sorted(counters.items())), "actions": actions})
    print("[FIX_LEADMAND_ORGS_SUMMARY] " + " ".join(f"{k}={v}" for k, v in sorted(counters.items())), flush=True)
    print(f"[FIX_LEADMAND_ORGS_REPORT] {REPORT_JSON}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
