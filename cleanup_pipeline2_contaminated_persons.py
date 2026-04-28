#!/usr/bin/env python3
import os
import re
import sys
import json
import time
import requests
from urllib.parse import quote

TOKEN = os.getenv("PIPEDRIVE_TOKEN") or os.getenv("TOKEN") or ""
if not TOKEN:
    print("ERRO: defina PIPEDRIVE_TOKEN ou TOKEN no ambiente.")
    print("Exemplo:")
    print("export PIPEDRIVE_TOKEN='SEU_TOKEN_AQUI'")
    sys.exit(1)

BASE = "https://api.pipedrive.com/v1"
PIPELINE_ID = 2
APPLY = os.getenv("APPLY", "0") == "1"   # 0 = so audita, 1 = remove person_id dos deals contaminados
SLEEP = float(os.getenv("API_SLEEP", "0.15"))

session = requests.Session()

def req(method, path, **kwargs):
    url = f"{BASE}{path}"
    params = kwargs.pop("params", {})
    params["api_token"] = TOKEN
    r = session.request(method, url, params=params, timeout=30, **kwargs)
    r.raise_for_status()
    return r.json()

def normalize(s):
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def norm_upper(s):
    return normalize(s).upper()

def slug(s):
    s = norm_upper(s)
    repl = {
        "Á":"A","À":"A","Â":"A","Ã":"A","Ä":"A",
        "É":"E","È":"E","Ê":"E","Ë":"E",
        "Í":"I","Ì":"I","Î":"I","Ï":"I",
        "Ó":"O","Ò":"O","Ô":"O","Õ":"O","Ö":"O",
        "Ú":"U","Ù":"U","Û":"U","Ü":"U",
        "Ç":"C"
    }
    for a,b in repl.items():
        s = s.replace(a,b)
    s = re.sub(r"[^A-Z0-9]+", " ", s).strip()
    return s

def looks_like_company_name(name: str) -> bool:
    n = " " + slug(name) + " "
    if not n.strip():
        return True
    termos = [
        " LTDA ", " S A ", " SA ", " S A ", " EIRELI ", " HOLDING ", " INDUSTRIA ",
        " COMERCIO ", " DISTRIBUIDORA ", " CONDOMINIO ", " COOPERATIVA ", " ASSOCIACAO ",
        " FUNDO ", " SHOPPING ", " EMPRESARIAL ", " ADMINISTRADORA ", " PARTICIPACOES ",
        " LOGISTICA ", " TRANSPORTES ", " AGROINDUSTRIAL ", " SPE ", " CORP ",
        " CORPORATION ", " INC ", " LLC ", " BV ", " GMBH ", " PLC ", " CIA ",
        " COMPANHIA ", " PAPEL ", " FARMACIA ", " DROGARIA "
    ]
    if any(t in n for t in termos):
        return True
    if len(re.findall(r"\d", n)) >= 3:
        return True
    parts = n.split()
    if len(parts) >= 4:
        upperish = sum(1 for p in parts if len(p) > 2)
        if upperish >= max(3, len(parts)-1):
            return True
    return False

def looks_like_placeholder_name(name: str) -> bool:
    n = slug(name)
    placeholders = {
        "", "RESPONSAVEL", "RESPONSAVEL LEGAL", "CONTATO", "SEM NOME",
        "A DEFINIR", "NA", "N A", "NAO INFORMADO", "NAO SEI"
    }
    return n in placeholders

def valid_person_name(name: str) -> bool:
    n = normalize(name)
    if len(n) < 8:
        return False
    if len(n.split()) < 2:
        return False
    if looks_like_placeholder_name(n):
        return False
    if looks_like_company_name(n):
        return False
    return True

def extract_email(person):
    emails = person.get("email") or []
    if isinstance(emails, list) and emails:
        for e in emails:
            val = normalize(e.get("value"))
            if val:
                return val.lower()
    return ""

def extract_phone(person):
    phones = person.get("phone") or []
    if isinstance(phones, list) and phones:
        for p in phones:
            val = normalize(p.get("value"))
            if val:
                return val
    return ""

def org_name_from_deal(deal):
    org = deal.get("org_id")
    if isinstance(org, dict):
        return normalize(org.get("name"))
    return ""

def person_name_from_deal(deal):
    p = deal.get("person_id")
    if isinstance(p, dict):
        return normalize(p.get("name"))
    return ""

def person_id_from_deal(deal):
    p = deal.get("person_id")
    if isinstance(p, dict):
        return p.get("value")
    if isinstance(p, int):
        return p
    return None

def deal_title(deal):
    return normalize(deal.get("title"))

def get_all_deals_in_pipeline_2():
    start = 0
    limit = 100
    out = []
    while True:
        data = req("GET", "/deals", params={
            "status": "open",
            "start": start,
            "limit": limit,
            "pipeline_id": PIPELINE_ID,
            "sort": "id ASC"
        })
        items = data.get("data") or []
        if not items:
            break
        out.extend(items)
        pagination = (data.get("additional_data") or {}).get("pagination") or {}
        if not pagination.get("more_items_in_collection"):
            break
        start = pagination.get("next_start", start + limit)
        time.sleep(SLEEP)
    return out

def get_person(pid: int):
    data = req("GET", f"/persons/{pid}")
    return data.get("data") or {}

def put_deal_remove_person(deal_id: int):
    return req("PUT", f"/deals/{deal_id}", json={"person_id": None})

def domain_hint_from_org(org_name: str):
    s = slug(org_name)
    tokens = [t for t in s.split() if t and t not in {
        "LTDA","SA","S","A","EIRELI","HOLDING","INDUSTRIA","COMERCIO","DISTRIBUIDORA",
        "CONDOMINIO","COOPERATIVA","ASSOCIACAO","FUNDO","SHOPPING","EMPRESARIAL",
        "ADMINISTRADORA","PARTICIPACOES","LOGISTICA","TRANSPORTES","AGROINDUSTRIAL",
        "CIA","COMPANHIA","DE","DO","DA","DOS","DAS","E"
    }]
    return tokens

def email_conflicts_with_org(email: str, org_name: str):
    if not email or not org_name:
        return False
    domain = email.split("@")[-1].lower()
    dom_slug = slug(domain.replace(".com.br","").replace(".com","").replace(".br",""))
    org_tokens = domain_hint_from_org(org_name)
    if not org_tokens:
        return False
    # se nenhum token relevante da org aparece no domínio, marca suspeito
    hits = sum(1 for t in org_tokens[:3] if t and t in dom_slug)
    return hits == 0

def contaminated_reasons(deal, person):
    reasons = []

    d_title = deal_title(deal)
    d_org = org_name_from_deal(deal)
    p_name = normalize(person.get("name")) or person_name_from_deal(deal)
    p_email = extract_email(person)
    p_phone = extract_phone(person)
    owner_org_name = ""
    if isinstance(person.get("org_id"), dict):
        owner_org_name = normalize(person["org_id"].get("name"))

    if not person:
        reasons.append("person_missing")
        return reasons

    if not valid_person_name(p_name):
        reasons.append("invalid_person_name")

    if owner_org_name and d_org and slug(owner_org_name) != slug(d_org):
        reasons.append("person_org_mismatch")

    if p_email and d_org and email_conflicts_with_org(p_email, d_org):
        reasons.append("email_domain_conflicts_with_deal_org")

    title_slug = slug(d_title)
    org_slug = slug(d_org)
    pname_slug = slug(p_name)

    # caso bem contaminado: nome/email indicam uma marca e deal/org outra
    if p_email:
        em = slug(p_email.split("@")[-1])
        if org_slug and em and all(tok not in em for tok in domain_hint_from_org(d_org)[:2]):
            reasons.append("email_brand_mismatch")

    if pname_slug and org_slug and pname_slug == org_slug:
        reasons.append("person_name_equals_org")

    # placeholders ou dados claramente estranhos
    if not p_email and not p_phone:
        reasons.append("person_has_no_contact")

    return sorted(set(reasons))

def main():
    deals = get_all_deals_in_pipeline_2()
    print(json.dumps({
        "mode": "APPLY" if APPLY else "DRY_RUN",
        "pipeline_id": PIPELINE_ID,
        "open_deals_found": len(deals)
    }, ensure_ascii=False))

    contaminated = []
    clean = 0
    skipped_no_person = 0

    for deal in deals:
        pid = person_id_from_deal(deal)
        if not pid:
            skipped_no_person += 1
            print(json.dumps({
                "deal_id": deal.get("id"),
                "title": deal_title(deal),
                "org_name": org_name_from_deal(deal),
                "status": "skip_no_person"
            }, ensure_ascii=False))
            continue

        try:
            person = get_person(int(pid))
        except Exception as e:
            print(json.dumps({
                "deal_id": deal.get("id"),
                "person_id": pid,
                "status": "error_read_person",
                "error": str(e)[:300]
            }, ensure_ascii=False))
            continue

        reasons = contaminated_reasons(deal, person)
        row = {
            "deal_id": deal.get("id"),
            "title": deal_title(deal),
            "org_name": org_name_from_deal(deal),
            "person_id": pid,
            "person_name": normalize(person.get("name")),
            "person_email": extract_email(person),
            "person_phone": extract_phone(person),
            "person_org_name": normalize((person.get("org_id") or {}).get("name")) if isinstance(person.get("org_id"), dict) else "",
            "reasons": reasons
        }

        if reasons:
            if APPLY:
                try:
                    put_deal_remove_person(int(deal["id"]))
                    row["status"] = "person_removed_from_deal"
                except Exception as e:
                    row["status"] = "error_removing_person_from_deal"
                    row["error"] = str(e)[:300]
            else:
                row["status"] = "would_remove_person_from_deal"
            contaminated.append(row)
        else:
            row["status"] = "clean"
            clean += 1

        print(json.dumps(row, ensure_ascii=False))
        time.sleep(SLEEP)

    summary = {
        "mode": "APPLY" if APPLY else "DRY_RUN",
        "pipeline_id": PIPELINE_ID,
        "open_deals_found": len(deals),
        "clean": clean,
        "contaminated": len(contaminated),
        "skipped_no_person": skipped_no_person
    }
    print("SUMMARY=" + json.dumps(summary, ensure_ascii=False))

    out = "/root/sdr-vps/runtime/cleanup_pipeline2_contaminated_persons_summary.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump({
            "summary": summary,
            "contaminated": contaminated
        }, f, ensure_ascii=False, indent=2)
    print(f"Arquivo salvo em: {out}")

if __name__ == "__main__":
    main()
