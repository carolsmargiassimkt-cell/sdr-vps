from __future__ import annotations

import csv
import json
import re
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

from crm.pipedrive_client import PipedriveClient

INPUT_XLSX = Path("/root/BASE_LEADS_COM_LINKEDIN.xlsx")
OUTPUT_JSON = Path("/root/sdr-vps/runtime/reconcile_full_sheet_vs_crm_plan.json")
OUTPUT_CSV = Path("/root/sdr-vps/runtime/reconcile_full_sheet_vs_crm_plan.csv")

PIPELINE_ID = 2
LOST_STAGE_ID = 50  # ajuste se seu "perdido" for outro stage

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def log(msg: str) -> None:
    print(msg, flush=True)


def strip_accents(text: Any) -> str:
    s = str(text or "").strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def collapse_spaces(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def normalize_text(text: Any) -> str:
    s = strip_accents(text).lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def normalize_company_name(text: Any) -> str:
    s = collapse_spaces(strip_accents(text)).upper()
    s = re.sub(r"^LEAD\s+MAND\s+DIGITAL\s*-\s*", "", s, flags=re.I)
    s = re.sub(r"\s*-\s*LEAD\s*$", "", s, flags=re.I)
    s = re.sub(r"\s*-\s*DEAL\s*$", "", s, flags=re.I)
    s = re.sub(r"\s*-\s*NEGOCIO\s*$", "", s, flags=re.I)
    s = re.sub(r"\bNEGOCIO\b", " ", s, flags=re.I)
    s = collapse_spaces(s).strip(" -")
    return s


def normalize_cnpj(v: Any) -> str:
    d = re.sub(r"\D+", "", str(v or ""))
    return d if len(d) == 14 else ""


def normalize_phone(v: Any) -> str:
    d = re.sub(r"\D+", "", str(v or ""))
    if d.startswith("55") and len(d) > 11:
        d = d[2:]
    if len(d) not in {10, 11}:
        return ""
    if len(d) == 11 and d[2] != "9":
        return ""
    if len(set(d)) <= 2:
        return ""
    if "000000" in d or "999999" in d or "123456" in d:
        return ""
    return d


def normalize_email(v: Any) -> str:
    s = str(v or "").strip().lower()
    if "@" not in s:
        return ""
    if s.startswith(("http://", "https://")):
        return ""
    local, _, domain = s.partition("@")
    if not local or "." not in domain:
        return ""
    return s


def split_multi(v: Any) -> List[str]:
    s = str(v or "").strip()
    if not s:
        return []
    s = s.replace("\n", "|").replace("\r", "|")
    return [x.strip() for x in re.split(r"[|;,]+", s) if x.strip()]


def dedupe_keep_order(values: List[str]) -> List[str]:
    out = []
    seen = set()
    for v in values:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def parse_xlsx(path: Path) -> List[Dict[str, Any]]:
    with zipfile.ZipFile(path) as z:
        root = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))

    rows_raw: List[List[str]] = []
    for row in root.iter(NS + "row"):
        vals: List[str] = []
        for c in row:
            t = c.find(".//" + NS + "t")
            v = c.find(".//" + NS + "v")
            vals.append(t.text if t is not None else (v.text if v is not None else ""))
        if vals:
            rows_raw.append(vals)

    header = rows_raw[0]
    rows = []
    for vals in rows_raw[1:]:
        if len(vals) < len(header):
            vals += [""] * (len(header) - len(vals))
        rows.append(dict(zip(header, vals)))
    return rows


def extract_id(value: Any) -> int:
    if isinstance(value, dict):
        try:
            return int(value.get("value") or value.get("id") or 0)
        except Exception:
            return 0
    try:
        return int(value or 0)
    except Exception:
        return 0


def person_emails(person: Dict[str, Any]) -> List[str]:
    out = []
    raw = person.get("email") or []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                out.append(normalize_email(item.get("value")))
            else:
                out.append(normalize_email(item))
    elif isinstance(raw, str):
        out.append(normalize_email(raw))
    return dedupe_keep_order([x for x in out if x])


def person_phones(person: Dict[str, Any]) -> List[str]:
    out = []
    raw = person.get("phone") or []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                out.append(normalize_phone(item.get("value")))
            else:
                out.append(normalize_phone(item))
    elif isinstance(raw, str):
        out.append(normalize_phone(raw))
    return dedupe_keep_order([x for x in out if x])


def choose_master_deal(deals: List[Dict[str, Any]]) -> Dict[str, Any]:
    # master = deal mais antigo (menor id) dentro do pipeline 2
    deals = sorted(deals, key=lambda d: int(d.get("id") or 10**12))
    return deals[0]


def suspicious_person_name(name: str, company_name: str) -> bool:
    n = normalize_text(name)
    c = normalize_text(company_name)
    if not n:
        return True
    if n == c:
        return True
    corp_tokens = {
        "ltda", "sa", "empresa", "comercio", "industria", "holding",
        "grupo", "servicos", "distribuidora", "supermercado"
    }
    words = set(re.findall(r"[a-z0-9]+", strip_accents(name).lower()))
    if words & corp_tokens:
        return True
    return False


def main() -> int:
    client = PipedriveClient()
    sheet_rows = parse_xlsx(INPUT_XLSX)
    log(f"[SHEET_ROWS] {len(sheet_rows)}")

    all_deals = client.get_deals() or []
    p2_deals = [d for d in all_deals if int(d.get("pipeline_id") or 0) == PIPELINE_ID]
    log(f"[CRM_PIPELINE_2_DEALS] {len(p2_deals)}")

    # índice CRM
    deals_by_org: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    deals_by_title_norm: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for d in p2_deals:
        org_id = extract_id(d.get("org_id"))
        if org_id:
            deals_by_org[org_id].append(d)
        deals_by_title_norm[normalize_company_name(d.get("title"))].append(d)

    # agrupar planilha por CNPJ ou nome
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in sheet_rows:
        company = normalize_company_name(row.get("Organização - Nome"))
        cnpj = normalize_cnpj(row.get("cnpj_formatado"))
        key = f"cnpj:{cnpj}" if cnpj else f"name:{normalize_text(company)}"
        row["_canonical_name"] = company
        row["_cnpj"] = cnpj
        row["_phones"] = dedupe_keep_order([
            normalize_phone(row.get("Pessoa - Telefone - Trabalho")),
            normalize_phone(row.get("Pessoa - Telefone - Celular")),
            normalize_phone(row.get("API_TEL_OFICIAL")),
        ])
        row["_phones"] = [x for x in row["_phones"] if x]
        row["_emails"] = dedupe_keep_order([
            normalize_email(row.get("API_EMAIL")),
        ])
        row["_emails"] = [x for x in row["_emails"] if x]
        groups[key].append(row)

    plan: List[Dict[str, Any]] = []

    for idx, (group_key, rows) in enumerate(groups.items(), start=1):
        canonical_name = normalize_company_name(rows[0].get("_canonical_name"))
        cnpj = rows[0].get("_cnpj") or ""

        # coletar dados da planilha
        target_phones = dedupe_keep_order([p for r in rows for p in r.get("_phones", [])])
        target_emails = dedupe_keep_order([e for r in rows for e in r.get("_emails", [])])

        # achar deals CRM por nome / org / cnpj de forma conservadora
        candidate_deals: List[Dict[str, Any]] = []
        for d in p2_deals:
            title_norm = normalize_company_name(d.get("title"))
            org_name_norm = normalize_company_name((d.get("org_name") or "") if isinstance(d.get("org_name"), str) else "")
            if canonical_name and (
                canonical_name == title_norm or
                canonical_name in title_norm or
                title_norm in canonical_name or
                (org_name_norm and canonical_name == org_name_norm)
            ):
                candidate_deals.append(d)

        candidate_deals = dedupe_keep_order_objs(candidate_deals, key=lambda x: int(x.get("id") or 0))

        if not candidate_deals:
            plan.append({
                "group_key": group_key,
                "status": "no_live_deals",
                "canonical_name": canonical_name,
                "cnpj": cnpj,
                "master_org_id": None,
                "master_deal_id": None,
                "master_person_id": None,
                "duplicate_deal_ids": [],
                "target_phones": target_phones,
                "target_emails": target_emails,
                "people_rows": len(rows),
                "safe_to_apply": False,
                "apply_summary": {},
                "needs_review_reason": "sem deal ativo no funil 2"
            })
            continue

        master_deal = choose_master_deal(candidate_deals)
        duplicate_deals = [int(d.get("id") or 0) for d in candidate_deals if int(d.get("id") or 0) != int(master_deal.get("id") or 0)]
        master_org_id = extract_id(master_deal.get("org_id"))
        master_person_id = extract_id(master_deal.get("person_id"))

        contaminated = False
        reasons = []

        if not master_org_id:
            contaminated = True
            reasons.append("deal sem org")

        if master_person_id:
            person = client.get_person_details(master_person_id) or {}
            person_name = str(person.get("name") or "")
            if suspicious_person_name(person_name, canonical_name):
                reasons.append("pessoa principal suspeita/contaminada")

        org = client.get_organization(master_org_id) if master_org_id else {}
        org_name = str(org.get("name") or "")
        if org_name and canonical_name and normalize_company_name(org_name) != canonical_name:
            reasons.append("nome org difere do canonico")

        status = "safe_update"
        if duplicate_deals:
            status = "duplicate_deals"
        if reasons:
            status = "needs_review"

        apply_summary = {
            "deal_master_keep": int(master_deal.get("id") or 0),
            "duplicate_deals_mark_lost": duplicate_deals,
            "org_update_name": canonical_name if master_org_id else None,
            "person_merge_phones": target_phones,
            "person_merge_emails": target_emails,
        }

        plan.append({
            "group_key": group_key,
            "status": status,
            "canonical_name": canonical_name,
            "cnpj": cnpj,
            "master_org_id": master_org_id,
            "master_deal_id": int(master_deal.get("id") or 0),
            "master_person_id": master_person_id,
            "duplicate_deal_ids": duplicate_deals,
            "duplicate_org_ids": [],
            "duplicate_person_ids": [],
            "phones_current": [],
            "phones_new": target_phones,
            "emails_current": [],
            "emails_new": target_emails,
            "source_google": bool(target_phones),
            "source_oportunidados": bool(cnpj),
            "people_rows": len(rows),
            "safe_to_apply": status in {"safe_update", "duplicate_deals"},
            "apply_summary": apply_summary,
            "needs_review_reason": "; ".join(reasons),
        })

        if idx % 50 == 0:
            log(f"[PROGRESS] {idx}/{len(groups)}")

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "group_key","status","canonical_name","cnpj","master_org_id","master_deal_id",
            "master_person_id","duplicate_deal_ids","people_rows","safe_to_apply",
            "phones_new","emails_new","needs_review_reason"
        ])
        for r in plan:
            w.writerow([
                r["group_key"],
                r["status"],
                r["canonical_name"],
                r["cnpj"],
                r["master_org_id"],
                r["master_deal_id"],
                r["master_person_id"],
                "|".join(map(str, r["duplicate_deal_ids"])),
                r["people_rows"],
                int(bool(r["safe_to_apply"])),
                "|".join(r.get("phones_new", r.get("target_phones", []))),
                "|".join(r.get("emails_new", r.get("target_emails", []))),
                r["needs_review_reason"],
            ])

    counts = defaultdict(int)
    for r in plan:
        counts[r["status"]] += 1

    log("[FINAL_COUNTS]")
    for k, v in sorted(counts.items()):
        log(f"{k}={v}")

    log(f"[OUTPUT_JSON] {OUTPUT_JSON}")
    log(f"[OUTPUT_CSV] {OUTPUT_CSV}")
    return 0


def dedupe_keep_order_objs(items: List[Dict[str, Any]], key):
    out = []
    seen = set()
    for item in items:
        k = key(item)
        if k not in seen:
            seen.add(k)
            out.append(item)
    return out


if __name__ == "__main__":
    raise SystemExit(main())
