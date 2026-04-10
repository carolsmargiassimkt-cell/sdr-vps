from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set

from crm.pipedrive_client import PipedriveClient
from enrich_missing_cnpj_from_xlsx import normalize_cnpj


ENRICHED_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_oportunidados_enriched.csv")
OLD_SUSPICIOUS_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\people_org_audit\org_suspicious_addresses.csv")
REPORT_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\fix_mossoro_and_address_contamination_report.json")
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


def preferred_title(row: Dict[str, str]) -> str:
    text = str_value(row.get("nome_final"))
    if not text:
        text = str_value(row.get("nome_base"))
    return text[:180]


def preferred_address(row: Dict[str, str]) -> str:
    opp = str_value(row.get("opp_endereco"))
    casa = str_value(row.get("casa_endereco"))
    for candidate in (opp, casa):
        lower = candidate.lower()
        if candidate and not lower.startswith("fat: n/a") and "verificado via casa" not in lower:
            return candidate[:255]
    return ""


def preferred_emails(row: Dict[str, str]) -> List[str]:
    values: List[str] = []
    for raw in (
        str_value(row.get("opp_company_emails")),
        str_value(row.get("casa_emails")),
        str_value(row.get("email")),
    ):
        if not raw:
            continue
        for item in [part.strip().lower() for part in raw.split("|")]:
            if "@" in item and item not in values:
                values.append(item)
    return values


def preferred_phones(row: Dict[str, str]) -> List[str]:
    values: List[str] = []
    for raw in (
        str_value(row.get("opp_company_phones")),
        str_value(row.get("casa_telefones")),
        str_value(row.get("telefone")),
        str_value(row.get("telefone_novo_enriquecido")),
    ):
        if not raw:
            continue
        for item in [re.sub(r"\D+", "", part) for part in raw.split("|")]:
            if item and item not in values:
                values.append(item)
    return values


def load_old_mossoro_names() -> Set[str]:
    names: Set[str] = set()
    with OLD_SUSPICIOUS_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if "mossoro" in str_value(row.get("address")).lower() or "mossoró" in str_value(row.get("address")).lower():
                key = normalize_key(row.get("org_name"))
                if key:
                    names.add(key)
    return names


def load_enriched_rows() -> Dict[int, Dict[str, str]]:
    rows: Dict[int, Dict[str, str]] = {}
    with ENRICHED_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            deal_id = int_value(row.get("deal_id"))
            if deal_id:
                rows[deal_id] = dict(row)
    return rows


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


def search_org_by_name_exact(client: PipedriveClient, target_name: str) -> Dict[str, Any]:
    for item in client.find_organization_by_name(target_name):
        org = dict(item.get("item") or {})
        if normalize_key(org.get("name")) == normalize_key(target_name) and bool(org.get("active_flag", True)):
            return org
    return {}


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
    old_mossoro_names = load_old_mossoro_names()
    enriched_rows = load_enriched_rows()
    deals = client.get_deals(status="open", limit=3000, pipeline_id=PIPELINE_ID)

    target_deals: List[Dict[str, Any]] = []
    rows_by_cnpj: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for deal in deals:
        deal_id = int_value(deal.get("id"))
        title_key = normalize_key(deal.get("title"))
        org_key = normalize_key(org_name_of(deal))
        if title_key in old_mossoro_names or org_key in old_mossoro_names:
            row = enriched_rows.get(deal_id)
            if row:
                target_deals.append(dict(deal))
                cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
                if cnpj:
                    rows_by_cnpj[cnpj].append(row)

    contaminated_deal_ids: Set[int] = set()
    for cnpj, rows in rows_by_cnpj.items():
        unique_names = {normalize_key(preferred_title(row)) for row in rows if normalize_key(preferred_title(row))}
        if cnpj == CONTAMINATED_SHARED_CNPJ or len(unique_names) > 1:
            for row in rows:
                contaminated_deal_ids.add(int_value(row.get("deal_id")))

    counters: Counter[str] = Counter()
    report_actions: List[Dict[str, Any]] = []

    for deal in sorted(target_deals, key=lambda item: int_value(item.get("id"))):
        deal_id = int_value(deal.get("id"))
        row = enriched_rows.get(deal_id, {})
        target_title = preferred_title(row) or str_value(deal.get("title"))
        current_org_id = org_id_of(deal)
        current_org = client.get_organization(current_org_id) if current_org_id else {}
        current_org_name = str_value(current_org.get("name") or org_name_of(deal))
        cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
        address = preferred_address(row)
        contaminated = deal_id in contaminated_deal_ids

        target_org: Dict[str, Any] = {}
        if not contaminated and cnpj:
            found = client.find_organization_by_cnpj(cnpj)
            if found and normalize_cnpj(found.get(ORG_CNPJ_FIELD_KEY) or found.get("cnpj")) == cnpj:
                target_org = dict(found)
        if not target_org:
            target_org = search_org_by_name_exact(client, target_title)
        if not target_org and args.apply:
            create_payload = {"name": target_title}
            if cnpj and not contaminated:
                create_payload[ORG_CNPJ_FIELD_KEY] = cnpj
            target_org = client.create_organization(create_payload)

        target_org_id = int_value(target_org.get("id"))
        if target_title and normalize_key(deal.get("title")) != normalize_key(target_title):
            counters["deal_title_plan"] += 1
            if args.apply and client.update_deal(deal_id, {"title": target_title}):
                counters["deal_title_ok"] += 1

        if target_org_id and target_org_id != current_org_id:
            counters["deal_org_relink_plan"] += 1
            if args.apply and client.update_deal(deal_id, {"org_id": target_org_id}):
                counters["deal_org_relink_ok"] += 1

        if target_org_id:
            org_payload: Dict[str, Any] = {}
            target_org_live = client.get_organization(target_org_id)
            live_name = str_value(target_org_live.get("name") or target_org.get("name"))
            if target_title and normalize_key(live_name) != normalize_key(target_title):
                org_payload["name"] = target_title
            if contaminated:
                if normalize_cnpj(target_org_live.get(ORG_CNPJ_FIELD_KEY) or target_org_live.get("cnpj")) == CONTAMINATED_SHARED_CNPJ:
                    org_payload[ORG_CNPJ_FIELD_KEY] = ""
                if preferred_address(row):
                    org_payload["address"] = ""
            else:
                if cnpj and normalize_cnpj(target_org_live.get(ORG_CNPJ_FIELD_KEY) or target_org_live.get("cnpj")) != cnpj:
                    org_payload[ORG_CNPJ_FIELD_KEY] = cnpj
                if address and normalize_key(target_org_live.get("address") or target_org_live.get("address_formatted_address")) != normalize_key(address):
                    org_payload["address"] = address
            if org_payload:
                counters["org_update_plan"] += 1
                if args.apply and client.update_organization(target_org_id, org_payload):
                    counters["org_update_ok"] += 1

        report_actions.append(
            {
                "deal_id": deal_id,
                "target_title": target_title,
                "current_org_id": current_org_id,
                "current_org_name": current_org_name,
                "target_org_id": target_org_id,
                "target_org_name": str_value(target_org.get("name")),
                "cnpj": cnpj,
                "contaminated": contaminated,
                "address": address,
                "phones": preferred_phones(row),
                "emails": preferred_emails(row),
            }
        )

    save_report(
        {
            "apply": bool(args.apply),
            "target_deal_count": len(target_deals),
            "contaminated_deal_count": len(contaminated_deal_ids),
            "summary": dict(sorted(counters.items())),
            "actions": report_actions,
        }
    )
    print("[FIX_MOSSORO_SUMMARY] " + " ".join(f"{k}={v}" for k, v in sorted(counters.items())), flush=True)
    print(f"[FIX_MOSSORO_REPORT] {REPORT_JSON}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
