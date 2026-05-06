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

# endpoints/cred da sua API de enriquecimento
OPORT_URL = os.getenv("OPORT_URL", "").strip()
OPORT_TOKEN = os.getenv("OPORT_TOKEN", "").strip()

OUT_JSON = os.getenv("OUT_JSON", "/root/sdr-vps/runtime/re_enrich_contacts_pipeline2_safe_result.json")

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


def discover_person_phone_field(person: Dict[str, Any]) -> Optional[str]:
    for k, v in person.items():
        lk = str(k).lower()
        if lk in {"phone"}:
            return k
        if isinstance(v, list) and "phone" in lk:
            return k
    return "phone" if "phone" in person else None


def discover_person_email_field(person: Dict[str, Any]) -> Optional[str]:
    for k, v in person.items():
        lk = str(k).lower()
        if lk in {"email"}:
            return k
        if isinstance(v, list) and "email" in lk:
            return k
    return "email" if "email" in person else None


def first_value(field_val: Any) -> str:
    if isinstance(field_val, list):
        for item in field_val:
            if isinstance(item, dict):
                val = str(item.get("value") or "").strip()
                if val:
                    return val
            else:
                val = str(item or "").strip()
                if val:
                    return val
    return str(field_val or "").strip()


def build_contact_patch(person: Dict[str, Any], phone_new: str, email_new: str) -> Dict[str, Any]:
    patch: Dict[str, Any] = {}
    phone_key = discover_person_phone_field(person)
    email_key = discover_person_email_field(person)

    current_phone = first_value(person.get(phone_key)) if phone_key else ""
    current_email = first_value(person.get(email_key)) if email_key else ""

    if phone_new and not current_phone and phone_key:
        if isinstance(person.get(phone_key), list):
            patch[phone_key] = [{"value": phone_new, "primary": True}]
        else:
            patch[phone_key] = phone_new

    if email_new and not current_email and email_key:
        if isinstance(person.get(email_key), list):
            patch[email_key] = [{"value": email_new, "primary": True}]
        else:
            patch[email_key] = email_new

    return patch


def oportunidados_lookup(cnpj: str) -> Dict[str, Any]:
    headers = {"Accept": "application/json"}
    if OPORT_TOKEN:
        headers["Authorization"] = f"Bearer {OPORT_TOKEN}"
    # ajuste o parâmetro conforme sua API
    url = f"{OPORT_URL}/{cnpj}/company"
    r = requests.get(url, headers=headers, timeout=60)
    status = r.status_code
    try:
        body = r.json() if r.content else {}
    except Exception:
        body = {"raw": (r.text or "")[:500]}
    return {"status": status, "body": body}


def extract_contacts(op_body: Dict[str, Any]) -> Dict[str, str]:
    # tolerante a formatos diferentes
    body = op_body or {}
    candidates = [body]
    if isinstance(body.get("data"), dict):
        candidates.append(body["data"])
    if isinstance(body.get("result"), dict):
        candidates.append(body["result"])

    phones: List[str] = []
    emails: List[str] = []

    def grab(node: Any):
        if isinstance(node, dict):
            for k, v in node.items():
                lk = str(k).lower()
                if "phone" in lk or "telefone" in lk:
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

    for c in candidates:
        grab(c)

    phone = phones[0] if phones else ""
    email = emails[0] if emails else ""
    return {"phone": phone, "email": email}


def main() -> None:
    cnpj_field = discover_org_cnpj_field()
    print(f"[INFO] org_cnpj_field={cnpj_field}")
    print("[INFO] carregando deals abertos do funil 2...")
    deals = paged_get("deals", extra_params={"status": "open", "pipeline_id": PIPELINE_ID})
    print(f"[INFO] deals={len(deals)}")

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

        if not org_id or not person_id:
            row["result"] = "skip_missing_org_or_person"
            results.append(row)
            counts[row["result"]] = counts.get(row["result"], 0) + 1
            print(row)
            continue

        org = (req("GET", f"organizations/{org_id}").get("data") or {})
        person = (req("GET", f"persons/{person_id}").get("data") or {})

        cnpj = digits(org.get(cnpj_field))
        row["cnpj"] = cnpj

        phone_key = discover_person_phone_field(person)
        email_key = discover_person_email_field(person)

        current_phone = first_value(person.get(phone_key)) if phone_key else ""
        current_email = first_value(person.get(email_key)) if email_key else ""

        row["current_phone"] = current_phone
        row["current_email"] = current_email

        if current_phone or current_email:
            row["result"] = "skip_already_has_contact"
            results.append(row)
            counts[row["result"]] = counts.get(row["result"], 0) + 1
            print(row)
            continue

        if not cnpj:
            row["result"] = "manual_review_no_cnpj"
            results.append(row)
            counts[row["result"]] = counts.get(row["result"], 0) + 1
            print(row)
            continue

        op = oportunidados_lookup(cnpj)
        row["oportunidados_status"] = op["status"]

        contacts = extract_contacts(op.get("body") or {})
        phone_new = contacts["phone"]
        email_new = contacts["email"]
        row["new_phone"] = phone_new
        row["new_email"] = email_new

        if not phone_new and not email_new:
            row["result"] = "no_contact_data"
            results.append(row)
            counts[row["result"]] = counts.get(row["result"], 0) + 1
            print(row)
            continue

        patch = build_contact_patch(person, phone_new, email_new)
        row["patch"] = patch

        if not patch:
            row["result"] = "skip_patch_empty"
            results.append(row)
            counts[row["result"]] = counts.get(row["result"], 0) + 1
            print(row)
            continue

        if not DRY_RUN:
            req("PUT", f"persons/{person_id}", json=patch)

        row["result"] = "patched_person_contact"
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
