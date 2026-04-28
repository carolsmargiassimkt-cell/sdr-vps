#!/usr/bin/env python3
import json
import os
import re
import sys
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

import requests

BASE_URL = os.getenv("PIPEDRIVE_BASE_URL", "https://api.pipedrive.com/v1").rstrip("/")
API_TOKEN = (
    os.getenv("PIPEDRIVE_API_TOKEN")
    or os.getenv("PIPEDRIVE_API_KEY")
    or ""
).strip()

INPUT_JSON = os.getenv("INPUT_JSON", "/root/sdr-vps/runtime/enrich_342_final.json")
OUTPUT_JSON = os.getenv("OUTPUT_JSON", "/root/sdr-vps/runtime/cnpj_writeback_safe_result.json")
DRY_RUN = os.getenv("DRY_RUN", "1").strip() not in {"0", "false", "False", "no", "NO"}

# Ajuste se quiser forçar pipeline/stage na criação
DEFAULT_PIPELINE_ID = os.getenv("DEFAULT_PIPELINE_ID", "2").strip()
DEFAULT_STAGE_ID = os.getenv("DEFAULT_STAGE_ID", "").strip()

if not API_TOKEN:
    print("ERRO: defina PIPEDRIVE_API_TOKEN no ambiente.")
    sys.exit(1)


def digits(v: Any) -> str:
    return re.sub(r"\D+", "", str(v or ""))


def norm_title(company: str) -> str:
    company = (company or "").strip()
    return f"{company} - Lead" if company else "Lead"


def req(method: str, path: str, **kwargs) -> Dict[str, Any]:
    params = dict(kwargs.pop("params", {}) or {})
    params["api_token"] = API_TOKEN
    url = f"{BASE_URL}/{path.lstrip('/')}"
    r = requests.request(method, url, params=params, timeout=60, **kwargs)
    r.raise_for_status()
    return r.json() if r.content else {}


def paged_get(path: str, data_key: str, limit: int = 500, extra_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    start = 0
    while True:
        params = {"start": start, "limit": limit}
        if extra_params:
            params.update(extra_params)
        body = req("GET", path, params=params)
        data = body.get(data_key) or body.get("data") or []
        if not data:
            break
        out.extend(data)
        pagination = ((body.get("additional_data") or {}).get("pagination") or {})
        if not pagination.get("more_items_in_collection"):
            break
        start = int(pagination.get("next_start") or 0)
        time.sleep(0.2)
    return out


def discover_org_cnpj_field() -> str:
    fields = paged_get("organizationFields", "data", limit=200)
    candidates = []
    for f in fields:
        name = str(f.get("name") or "").strip().lower()
        key = str(f.get("key") or "").strip()
        if "cnpj" in name and key:
            candidates.append((name, key))
    if not candidates:
        raise RuntimeError("Nao encontrei campo de CNPJ em organizationFields.")
    # prioriza nome mais curto/exato
    candidates.sort(key=lambda x: (0 if x[0] == "cnpj" else 1, len(x[0])))
    return candidates[0][1]


def load_input() -> List[Dict[str, Any]]:
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise RuntimeError("INPUT_JSON precisa ser uma lista.")
    return [x for x in data if isinstance(x, dict)]


def build_org_index(orgs: List[Dict[str, Any]], cnpj_field: str) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for org in orgs:
        cnpj = digits(org.get(cnpj_field))
        if cnpj and cnpj not in idx:
            idx[cnpj] = org
    return idx


def build_open_deal_index(deals: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    idx: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for d in deals:
        org = d.get("org_id")

        if isinstance(org, dict):
            org_id = org.get("value")
        elif isinstance(org, int):
            org_id = org
        else:
            continue

        if not org_id:
            continue

        idx[int(org_id)].append(d)
    return idx


def create_org(company: str, cnpj: str, cnpj_field: str) -> Dict[str, Any]:
    payload = {"name": company or cnpj, cnpj_field: cnpj}
    if DRY_RUN:
        return {"id": -1, "name": payload["name"], cnpj_field: cnpj, "_dry_run": True}
    body = req("POST", "organizations", json=payload)
    return body.get("data") or {}


def create_deal(title: str, org_id: int) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"title": title, "org_id": org_id}
    if DEFAULT_PIPELINE_ID:
        payload["pipeline_id"] = int(DEFAULT_PIPELINE_ID)
    if DEFAULT_STAGE_ID:
        payload["stage_id"] = int(DEFAULT_STAGE_ID)
    if DRY_RUN:
        return {"id": -1, "title": title, "org_id": org_id, "_dry_run": True}
    body = req("POST", "deals", json=payload)
    return body.get("data") or {}


def update_deal_title(deal_id: int, title: str) -> None:
    if DRY_RUN:
        return
    req("PUT", f"deals/{deal_id}", json={"title": title})


def main() -> None:
    items = load_input()
    cnpj_field = discover_org_cnpj_field()

    print(f"[INFO] org_cnpj_field={cnpj_field}")
    print("[INFO] carregando orgs...")
    orgs = paged_get("organizations", "data", limit=500)
    print(f"[INFO] orgs={len(orgs)}")
    org_by_cnpj = build_org_index(orgs, cnpj_field)

    print("[INFO] carregando deals abertos...")
    open_deals = paged_get(
        "deals",
        "data",
        limit=500,
        extra_params={"status": "open", "pipeline_id": 2}
    )
    print(f"[INFO] open_deals={len(open_deals)}")
    open_deals_by_org = build_open_deal_index(open_deals)

    results = []
    counters = defaultdict(int)

    for item in items:
        company = (item.get("company") or item.get("empresa") or "").strip()
        title = (item.get("title") or norm_title(company)).strip()
        cnpj = digits(item.get("cnpj"))
        action = item.get("action") or ""
        row = {
            "company": company,
            "title": title,
            "cnpj": cnpj,
            "input_action": action,
            "result": "",
        }

        if not cnpj:
            row["result"] = "manual_review_no_cnpj"
            counters[row["result"]] += 1
            results.append(row)
            continue

        org = org_by_cnpj.get(cnpj)
        created_org = False
        if not org:
            org = create_org(company, cnpj, cnpj_field)
            created_org = True
            if org and org.get("id") not in (-1, None):
                org_by_cnpj[cnpj] = org

        org_id = int(org.get("id") or -1)
        row["org_id"] = org_id
        row["org_created"] = created_org

        org_deals = open_deals_by_org.get(org_id, []) if org_id > 0 else []

        if len(org_deals) == 1:
            deal = org_deals[0]
            deal_id = int(deal.get("id") or -1)
            row["deal_id"] = deal_id
            row["result"] = "reused_open_deal"
            existing_title = str(deal.get("title") or "").strip()
            if existing_title and existing_title != title:
                update_deal_title(deal_id, title)
                row["deal_title_updated"] = True
            else:
                row["deal_title_updated"] = False

        elif len(org_deals) == 0:
            if org_id <= 0 and DRY_RUN:
                row["result"] = "would_create_org_then_deal"
            else:
                new_deal = create_deal(title, org_id)
                row["deal_id"] = int(new_deal.get("id") or -1)
                row["result"] = "created_open_deal"

        else:
            row["deal_ids"] = [int(d.get("id") or -1) for d in org_deals]
            row["result"] = "manual_review_multiple_open_deals"

        counters[row["result"]] += 1
        results.append(row)
        print(row)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print("[FINAL_COUNTS]")
    for k, v in sorted(counters.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"{k}={v}")
    print(f"[OUTPUT_JSON] {OUTPUT_JSON}")
    print(f"[DRY_RUN] {DRY_RUN}")


if __name__ == "__main__":
    main()
