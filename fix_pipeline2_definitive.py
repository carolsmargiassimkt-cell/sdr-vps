#!/usr/bin/env python3
import os, re, json, time, requests, unicodedata
from collections import defaultdict

TOKEN = os.getenv("PIPEDRIVE_TOKEN") or os.getenv("TOKEN") or ""
PIPELINE_ID = int(os.getenv("PIPELINE_ID", "2"))
APPLY = os.getenv("APPLY", "0") == "1"
SLEEP = float(os.getenv("API_SLEEP", "0.12"))

if not TOKEN:
    print("ERRO: export PIPEDRIVE_TOKEN='SEU_TOKEN'")
    raise SystemExit(1)

BASE = "https://api.pipedrive.com/v1"
session = requests.Session()
session.headers.update({"User-Agent": "crm-fix/1.0"})

# ---------- helpers ----------

def strip_accents(s):
    s = str(s or "")
    return ''.join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def norm(s):
    return re.sub(r"\s+", " ", str(s or "").strip())

def slug(s):
    s = strip_accents(norm(s)).upper()
    s = re.sub(r"[^A-Z0-9]+", " ", s).strip()
    return s

def clean_cnpj(x):
    x = re.sub(r"\D", "", str(x or ""))
    return x if len(x) == 14 else ""

def first_email(person):
    vals = person.get("email") or []
    if isinstance(vals, list):
        for v in vals:
            e = norm(v.get("value"))
            if e:
                return e.lower()
    return ""

def first_phone(person):
    vals = person.get("phone") or []
    if isinstance(vals, list):
        for v in vals:
            e = norm(v.get("value"))
            if e:
                return e
    return ""

def companyish_name(name):
    n = " " + slug(name) + " "
    if not n.strip():
        return False
    terms = [
        " LTDA ", " S A ", " SA ", " EIRELI ", " HOLDING ", " INDUSTRIA ",
        " COMERCIO ", " DISTRIBUIDORA ", " CONDOMINIO ", " EMPRESARIAL ",
        " SHOPPING ", " ASSOCIACAO ", " FUNDO ", " RADIO ", " CIA ",
        " COMPANHIA ", " COOPERATIVA ", " ADMINISTRADORA ", " LOGISTICA ",
        " TRANSPORTES ", " PAPELARIA ", " DROGARIA ", " FARMACIA ",
        " IMPORTACAO ", " EXPORTACAO ", " SPE ", " LLC ", " INC ", " BV "
    ]
    return any(t in n for t in terms)

def placeholder_name(name):
    n = slug(name)
    bad = {
        "", "CONTATO", "RESPONSAVEL", "RESPONSAVEL LEGAL", "SEM NOME",
        "A DEFINIR", "NAO INFORMADO", "N A", "NA", "LBN", "ALSCT"
    }
    if n in bad:
        return True
    if n.startswith("CONTATO "):
        return True
    return False

def person_like_name(name):
    n = norm(name)
    parts = [p for p in n.split() if p]
    if len(parts) < 2:
        return False
    if companyish_name(n) or placeholder_name(n):
        return False
    return True

def email_domain_slug(email):
    if "@" not in email:
        return ""
    dom = email.split("@", 1)[1].lower()
    dom = dom.replace(".com.br", "").replace(".com", "").replace(".br", "")
    return slug(dom)

def org_tokens(name):
    toks = [t for t in slug(name).split() if t]
    stop = {
        "DE","DO","DA","DOS","DAS","E","THE",
        "LTDA","S","A","SA","EIRELI","HOLDING","INDUSTRIA","COMERCIO",
        "DISTRIBUIDORA","CONDOMINIO","EMPRESARIAL","SHOPPING","ASSOCIACAO",
        "FUNDO","CIA","COMPANHIA","COOPERATIVA","ADMINISTRADORA","LOGISTICA",
        "TRANSPORTES","IMPORTACAO","EXPORTACAO","RADIO"
    }
    return [t for t in toks if t not in stop and len(t) >= 4]

def token_score(text_slug, tokens):
    if not text_slug or not tokens:
        return 0
    return sum(1 for t in tokens[:5] if t and t in text_slug)

def req(method, path, **kwargs):
    params = kwargs.pop("params", {})
    params["api_token"] = TOKEN
    r = session.request(method, BASE + path, params=params, timeout=35, **kwargs)
    r.raise_for_status()
    return r.json()

def paginate(path, params=None, data_key="data"):
    params = dict(params or {})
    start, limit = 0, 100
    out = []
    while True:
        q = dict(params)
        q["start"] = start
        q["limit"] = limit
        res = req("GET", path, params=q)
        items = res.get(data_key) or []
        if not items:
            break
        out.extend(items)
        pag = (res.get("additional_data") or {}).get("pagination") or {}
        if not pag.get("more_items_in_collection"):
            break
        start = pag.get("next_start", start + limit)
        time.sleep(SLEEP)
    return out

# ---------- field discovery ----------

def fetch_fields():
    deal_fields = req("GET", "/dealFields").get("data") or []
    org_fields = req("GET", "/organizationFields").get("data") or []
    def pick(fields):
        cands = []
        for f in fields:
            name = slug(f.get("name"))
            key = slug(f.get("key"))
            if "CNPJ" in name or "CNPJ" in key:
                cands.append(f["key"])
        return cands
    return pick(deal_fields), pick(org_fields)

DEAL_CNPJ_KEYS, ORG_CNPJ_KEYS = fetch_fields()

def extract_cnpj_from_obj(obj, keys):
    for k in keys:
        cnpj = clean_cnpj(obj.get(k))
        if cnpj:
            return cnpj
    return ""

# ---------- optional razão social lookup ----------
# 1) if RAZAO_LOOKUP_URL is set, uses .format(cnpj=...)
# 2) else tries BrasilAPI
def lookup_razao(cnpj):
    cnpj = clean_cnpj(cnpj)
    if not cnpj:
        return ""
    custom = os.getenv("RAZAO_LOOKUP_URL", "").strip()
    custom_token = os.getenv("RAZAO_LOOKUP_TOKEN", "").strip()
    if custom:
        try:
            url = custom.format(cnpj=cnpj)
            headers = {}
            if custom_token:
                headers["Authorization"] = f"Bearer {custom_token}"
            r = session.get(url, headers=headers, timeout=25)
            if r.ok:
                data = r.json()
                for key in ["razao_social", "nome_empresarial", "company_name", "nome"]:
                    if norm(data.get(key)):
                        return norm(data[key])
        except Exception:
            pass
    try:
        r = session.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}", timeout=25)
        if r.ok:
            data = r.json()
            for key in ["razao_social", "nome_fantasia"]:
                if norm(data.get(key)):
                    return norm(data[key])
    except Exception:
        pass
    return ""

# ---------- caches ----------
org_cache = {}
person_cache = {}

def get_org(oid):
    if not oid:
        return {}
    if oid not in org_cache:
        try:
            org_cache[oid] = req("GET", f"/organizations/{oid}").get("data") or {}
        except Exception:
            org_cache[oid] = {}
        time.sleep(SLEEP)
    return org_cache[oid]

def get_person(pid):
    if not pid:
        return {}
    if pid not in person_cache:
        try:
            person_cache[pid] = req("GET", f"/persons/{pid}").get("data") or {}
        except Exception:
            person_cache[pid] = {}
        time.sleep(SLEEP)
    return person_cache[pid]

# ---------- load deals ----------
deals = paginate("/deals", params={"pipeline_id": PIPELINE_ID, "status": "all_not_deleted"})

# build org index for contamination scoring
org_name_by_id = {}
org_tokens_by_id = {}
cnpj_to_deals = defaultdict(list)
person_to_deals = defaultdict(list)

for d in deals:
    org_obj = d.get("org_id") if isinstance(d.get("org_id"), dict) else {}
    org_id = org_obj.get("value") if isinstance(org_obj, dict) else None
    org_name = norm(org_obj.get("name")) if isinstance(org_obj, dict) else ""
    if org_id:
        org_name_by_id[org_id] = org_name
        org_tokens_by_id[org_id] = org_tokens(org_name)
    person_obj = d.get("person_id") if isinstance(d.get("person_id"), dict) else {}
    person_id = person_obj.get("value") if isinstance(person_obj, dict) else None
    if person_id:
        person_to_deals[person_id].append(d["id"])

# ---------- evaluation ----------
actions = []
summary = {
    "pipeline_id": PIPELINE_ID,
    "mode": "APPLY" if APPLY else "DRY_RUN",
    "deals_seen": len(deals),
    "unlinked_person_from_deal": 0,
    "org_name_updated": 0,
    "deal_title_updated": 0,
    "dup_cnpj_count": 0
}

def update_deal(deal_id, payload):
    return req("PUT", f"/deals/{deal_id}", json=payload)

def update_org(org_id, payload):
    return req("PUT", f"/organizations/{org_id}", json=payload)

for d in deals:
    deal_id = d["id"]
    title = norm(d.get("title"))
    org_obj = d.get("org_id") if isinstance(d.get("org_id"), dict) else {}
    org_id = org_obj.get("value") if isinstance(org_obj, dict) else None
    org_name = norm(org_obj.get("name")) if isinstance(org_obj, dict) else ""
    person_obj = d.get("person_id") if isinstance(d.get("person_id"), dict) else {}
    person_id = person_obj.get("value") if isinstance(person_obj, dict) else None
    person_name_from_deal = norm(person_obj.get("name")) if isinstance(person_obj, dict) else ""

    org = get_org(org_id) if org_id else {}
    person = get_person(person_id) if person_id else {}

    person_name = norm(person.get("name")) or person_name_from_deal
    person_email = first_email(person)
    person_phone = first_phone(person)

    cnpj = extract_cnpj_from_obj(d, DEAL_CNPJ_KEYS) or extract_cnpj_from_obj(org, ORG_CNPJ_KEYS)
    if cnpj:
        cnpj_to_deals[cnpj].append(deal_id)

    official_razao = lookup_razao(cnpj) if cnpj else ""
    canonical_name = official_razao or org_name or title.replace(" - Deal", "").strip()

    # --- contamination detection, conservative ---
    reasons = []
    email_slug = email_domain_slug(person_email)
    current_tokens = org_tokens(org_name)
    current_score = token_score(email_slug, current_tokens)

    best_other_org = ""
    best_other_score = 0
    for oid, toks in org_tokens_by_id.items():
        if oid == org_id:
            continue
        sc = token_score(email_slug, toks)
        if sc > best_other_score:
            best_other_score = sc
            best_other_org = org_name_by_id.get(oid, "")

    if person_id:
        if placeholder_name(person_name):
            reasons.append("placeholder_person")
        if companyish_name(person_name):
            reasons.append("company_as_person")
        if person_name and org_name and slug(person_name) == slug(org_name):
            reasons.append("person_equals_org")
        if not person_email and not person_phone and (placeholder_name(person_name) or companyish_name(person_name) or not person_like_name(person_name)):
            reasons.append("dead_person_record")
        # ex: Andrea + email Sherwin em org Indiana
        if person_email and current_score == 0 and best_other_score >= 1:
            reasons.append(f"email_matches_other_org:{best_other_org}")
        # mesma person em varios orgs no funil 2 + email apontando para outro org
        if person_id and len(person_to_deals.get(person_id, [])) > 1 and person_email and current_score == 0 and best_other_score >= 1:
            reasons.append("same_person_multi_org_conflict")

    unlink_person = False
    strong = {"placeholder_person", "company_as_person", "person_equals_org", "dead_person_record", "same_person_multi_org_conflict"}
    if any(r.split(":",1)[0] in strong for r in reasons):
        unlink_person = True
    elif any(r.startswith("email_matches_other_org:") for r in reasons) and (companyish_name(person_name) or not person_like_name(person_name)):
        unlink_person = True

    desired_title = f"{canonical_name} - Deal" if canonical_name else title

    row = {
        "deal_id": deal_id,
        "org_id": org_id,
        "person_id": person_id,
        "title_before": title,
        "org_before": org_name,
        "person_before": person_name,
        "person_email": person_email,
        "cnpj": cnpj,
        "canonical_name": canonical_name,
        "reasons": reasons,
        "actions": []
    }

    # fix contaminated person link
    if unlink_person and person_id:
        row["actions"].append("unlink_person_from_deal")
        if APPLY:
            try:
                update_deal(deal_id, {"person_id": None})
                summary["unlinked_person_from_deal"] += 1
            except Exception as e:
                row["unlink_error"] = str(e)[:300]

    # normalize org name by CNPJ / canonical name
    if org_id and canonical_name and slug(org_name) != slug(canonical_name):
        row["actions"].append(f"rename_org:{canonical_name}")
        if APPLY:
            try:
                update_org(org_id, {"name": canonical_name})
                summary["org_name_updated"] += 1
            except Exception as e:
                row["org_rename_error"] = str(e)[:300]

    # normalize deal title
    if desired_title and slug(title) != slug(desired_title):
        row["actions"].append(f"rename_deal:{desired_title}")
        if APPLY:
            try:
                update_deal(deal_id, {"title": desired_title})
                summary["deal_title_updated"] += 1
            except Exception as e:
                row["deal_rename_error"] = str(e)[:300]

    if row["actions"]:
        actions.append(row)
        print(json.dumps({
            "deal_id": deal_id,
            "title": title,
            "org": org_name,
            "person": person_name,
            "email": person_email,
            "cnpj": cnpj,
            "reasons": reasons,
            "actions": row["actions"],
            "status": "APPLIED" if APPLY else "WOULD_APPLY"
        }, ensure_ascii=False))
    time.sleep(SLEEP)

# duplicate CNPJ report only
dup_cnpj = {cnpj: ids for cnpj, ids in cnpj_to_deals.items() if cnpj and len(ids) > 1}
summary["dup_cnpj_count"] = len(dup_cnpj)

out_dir = "/root/sdr-vps/runtime"
with open(f"{out_dir}/fix_pipeline2_definitive_actions.json", "w", encoding="utf-8") as f:
    json.dump(actions, f, ensure_ascii=False, indent=2)
with open(f"{out_dir}/fix_pipeline2_definitive_dup_cnpj.json", "w", encoding="utf-8") as f:
    json.dump(dup_cnpj, f, ensure_ascii=False, indent=2)
with open(f"{out_dir}/fix_pipeline2_definitive_summary.json", "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print("SUMMARY=" + json.dumps(summary, ensure_ascii=False))
print(f"ACTIONS={out_dir}/fix_pipeline2_definitive_actions.json")
print(f"DUP_CNPJ={out_dir}/fix_pipeline2_definitive_dup_cnpj.json")
print(f"SUMMARY_FILE={out_dir}/fix_pipeline2_definitive_summary.json")
