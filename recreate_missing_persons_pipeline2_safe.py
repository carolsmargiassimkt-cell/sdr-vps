#!/usr/bin/env python3
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional

import requests

BASE_URL = os.getenv("PIPEDRIVE_BASE_URL", "https://api.pipedrive.com/v1").rstrip("/")
API_TOKEN = (os.getenv("PIPEDRIVE_API_TOKEN") or "").strip()
PIPELINE_ID = int(os.getenv("PIPELINE_ID", "2"))
DRY_RUN = os.getenv("DRY_RUN", "1").strip() not in {"0", "false", "False", "no", "NO"}

OPORT_URL = os.getenv("OPORT_URL", "").strip()  # ex: https://app.oportunidados.com.br/api/v1/brazilian_companies
OPORT_TOKEN = os.getenv("OPORT_TOKEN", "").strip()

OUT_JSON = os.getenv(
    "OUT_JSON",
    "/root/sdr-vps/runtime/recreate_missing_persons_pipeline2_safe_result.json"
)

if not API_TOKEN:
    print("ERRO: defina PIPEDRIVE_API_TOKEN")
    sys.exit(1)

if not OPORT_URL:
    print("ERRO: defina OPORT_URL")
    sys.exit(1)


def digits(v: Any) -> str:
    return re.sub(r"\D+", "", str(v or ""))


def req(method: str, path: str, **kwargs) -> Dict[str, Any]:
    params = dict(kwargs.pop("params", {}) or {})
    params["api_token"] = API_TOKEN
    url = f"{BASE_URL}/{path.lstrip('/')}"
    r = requests.request(method, url, params=params, timeout=60, **kwargs)
    r.raise_for_status()
    return r.json() if r.content else {}


def paged_get(path: str, limit: int = 500, extra_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    start = 0
    while True:
        params = {"start": start, "limit": limit}
        if extra_params:
            params.update(extra_params)
        body = req("GET", path, params=params)
        data = body.get("data") or []
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
    fields = paged_get("organizationFields", limit=200)
    for f in fields:
        name = str(f.get("name") or "").strip().lower()
        key = str(f.get("key") or "").strip()
        if "cnpj" in name and key:
            return key
    raise RuntimeError("Nao encontrei campo de CNPJ em organizationFields")


def oportunidados_lookup(cnpj: str) -> Dict[str, Any]:
    headers = {"Accept": "application/json"}
    if OPORT_TOKEN:
        headers["Authorization"] = f"Bearer {OPORT_TOKEN}"
    url = f"{OPORT_URL.rstrip('/')}/{cnpj}/company"
    r = requests.get(url, headers=headers, timeout=60)
    status = r.status_code
    try:
        body = r.json() if r.content else {}
    except Exception:
        body = {"raw": (r.text or "")[:500]}
    return {"status": status, "body": body, "url": url}


def extract_contacts(op_body: Dict[str, Any]) -> Dict[str, str]:
    company = (op_body or {}).get("company") or {}
    extras = (op_body or {}).get("contatos_extras") or []

    phones: List[str] = []
    emails: List[str] = []

    def grab(node: Any):
        if isinstance(node, dict):
            for k, v in node.items():
                lk = str(k).lower()
                if "phone" in lk or "telefone" in lk or "celular" in lk:
                    if isinstance(v, list):
                        for x in v:
                            dv = digits(x.get("value") if isinstance(x, dict) else x)
                            if dv:
                                phones.append(dv)
                    else:
                        dv = digits(v)
                        if dv:
                            phones.append(dv)
                elif "email" in lk:
                    if isinstance(v, list):
                        for x in v:
                            ev = str(x.get("value") if isinstance(x, dict) else x).strip()
                            if ev:
                                emails.append(ev)
                    else:
                        ev = str(v or "").strip()
                        if ev:
                            emails.append(ev)
                else:
                    grab(v)
        elif isinstance(node, list):
            for item in node:
                grab(item)

    grab(company)
    grab(extras)

    phone = phones[0] if phones else ""
    email = emails[0] if emails else ""
    return {"phone": phone, "email": email}


def extract_contact_name(op_body: Dict[str, Any], org_name: str) -> str:
    socios = (op_body or {}).get("socios") or []
    for s in socios:
        if isinstance(s, dict):
            for key in ("nome", "name", "razao_social", "nome_socio"):
                val = str(s.get(key) or "").strip()
                if val:
                    return val
    org_name = str(org_name or "").strip()
    return f"Contato {org_name}" if org_name else "Contato"


def create_person(name: str, org_id: int, phone: str, email: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "name": name or "Contato",
        "org_id": org_id,
    }
    if phone:
        payload["phone"] = [{"value": phone, "primary": True}]
    if email:
        payload["email"] = [{"value": email, "primary": True}]
    if DRY_RUN:
        return {"id": -1, "name": payload["name"], "org_id": org_id, "_dry_run": True, "payload": payload}
    body = req("POST", "persons", json=payload)
    return body.get("data") or {}


def attach_person_to_deal(deal_id: int, person_id: int) -> None:
    if DRY_RUN:
        return
    req("PUT", f"deals/{deal_id}", json={"person_id": person_id})


def main() -> None:
    cnpj_field = discover_org_cnpj_field()
    print(f"[INFO] org_cnpj_field={cnpj_field}")

    deals = paged_get("deals", extra_params={"status": "open", "pipeline_id": PIPELINE_ID})
    print(f"[INFO] open_deals_pipeline_{PIPELINE_ID}={len(deals)}")

    results = []
    counts: Dict[str, int] = {}

    for d in deals:
        deal_id = int(d.get("id") or 0)
        title = str(d.get("title") or "").strip()

        org_obj = d.get("org_id") or {}
        org_id = int(org_obj.get("value") or 0) if isinstance(org_obj, dict) else int(org_obj or 0)

        person_obj = d.get("person_id") or {}
        person_id = int(person_obj.get("value") or 0) if isinstance(person_obj, dict) else int(person_obj or 0)

        row = {
            "deal_id": deal_id,
            "title": title,
            "org_id": org_id,
            "person_id": person_id,
            "result": "",
        }

        if person_id:
            row["result"] = "skip_already_has_person"
            results.append(row)
            counts[row["result"]] = counts.get(row["result"], 0) + 1
            continue

        if not org_id:
            row["result"] = "skip_missing_org"
            results.append(row)
            counts[row["result"]] = counts.get(row["result"], 0) + 1
            print(row)
            continue

        org = (req("GET", f"organizations/{org_id}").get("data") or {})
        org_name = str(org.get("name") or "").strip()
        cnpj = digits(org.get(cnpj_field))
        row["cnpj"] = cnpj
        row["org_name"] = org_name

        if not cnpj:
            row["result"] = "manual_review_no_cnpj"
            results.append(row)
            counts[row["result"]] = counts.get(row["result"], 0) + 1
            print(row)
            continue

        op = oportunidados_lookup(cnpj)
        row["oportunidados_status"] = op["status"]

        contacts = extract_contacts(op.get("body") or {})
        phone = contacts["phone"]
        email = contacts["email"]
        row["new_phone"] = phone
        row["new_email"] = email

        if not phone and not email:
            row["result"] = "no_contact_data"
            results.append(row)
            counts[row["result"]] = counts.get(row["result"], 0) + 1
            print(row)
            continue

        person_name = extract_contact_name(op.get("body") or {}, org_name)
        row["new_person_name"] = person_name

        created = create_person(person_name, org_id, phone, email)
        new_person_id = int(created.get("id") or -1)
        row["new_person_id"] = new_person_id

        attach_person_to_deal(deal_id, new_person_id if new_person_id > 0 else -1)

        row["result"] = "created_person_and_attached_to_deal"
        results.append(row)
        counts[row["result"]] = counts.get(row["result"], 0) + 1
        print(row)
        time.sleep(0.3)

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print("[FINAL_COUNTS]")
    for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"{k}={v}")
    print(f"[OUTPUT_JSON] {OUT_JSON}")
    print(f"[DRY_RUN] {DRY_RUN}")


if __name__ == "__main__":
    main()
