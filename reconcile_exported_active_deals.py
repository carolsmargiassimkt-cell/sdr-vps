from __future__ import annotations
import csv, json, re, unicodedata, zipfile, xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path
from crm.pipedrive_client import PipedriveClient

INPUT_XLSX = Path("/root/BASE_LEADS_COM_LINKEDIN.xlsx")
OUT_JSON = Path("/root/sdr-vps/runtime/reconcile_exported_active_deals.json")
OUT_CSV = Path("/root/sdr-vps/runtime/reconcile_exported_active_deals.csv")
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

def strip_accents(v):
    s = str(v or "").strip()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def collapse(v):
    return re.sub(r"\s+", " ", str(v or "").strip())

def norm_title(v):
    s = strip_accents(v).upper()
    s = collapse(s)
    return s

def norm_name(v):
    s = strip_accents(v).upper()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_cnpj(v):
    d = re.sub(r"\D+", "", str(v or ""))
    return d if len(d) == 14 else ""

def norm_phone(v):
    d = re.sub(r"\D+", "", str(v or ""))
    if d.startswith("55") and len(d) > 11:
        d = d[2:]
    if len(d) not in {10, 11}:
        return ""
    return d

def norm_email(v):
    s = str(v or "").strip().lower()
    return s if "@" in s else ""

def split_multi(v):
    s = str(v or "").strip()
    if not s:
        return []
    s = s.replace("\n", "|").replace("\r", "|")
    return [x.strip() for x in re.split(r"[|;,]+", s) if x.strip()]

def dedupe(vals):
    out, seen = [], set()
    for v in vals:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out

def extract_id(v):
    if isinstance(v, dict):
        try:
            return int(v.get("value") or v.get("id") or 0)
        except:
            return 0
    try:
        return int(v or 0)
    except:
        return 0

def parse_xlsx(path):
    with zipfile.ZipFile(path) as z:
        root = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))
    rows_raw = []
    for row in root.iter(NS + "row"):
        vals = []
        for c in row:
            t = c.find(".//" + NS + "t")
            v = c.find(".//" + NS + "v")
            vals.append(t.text if t is not None else (v.text if v is not None else ""))
        if vals:
            rows_raw.append(vals)
    header = rows_raw[0]
    out = []
    for vals in rows_raw[1:]:
        if len(vals) < len(header):
            vals += [""] * (len(header) - len(vals))
        row = dict(zip(header, vals))
        out.append(row)
    return out

client = PipedriveClient()
sheet = parse_xlsx(INPUT_XLSX)
deals = client.get_deals() or []

by_title = defaultdict(list)
for d in deals:
    by_title[norm_title(d.get("title"))].append(d)

plan = []

for row in sheet:
    company = row.get("Organização - Nome") or ""
    title = row.get("Negócio - Título") or ""
    person = row.get("Pessoa - Nome") or ""
    cnpj = norm_cnpj(row.get("cnpj_formatado"))
    phones = dedupe([
        norm_phone(row.get("Pessoa - Telefone - Trabalho")),
        norm_phone(row.get("Pessoa - Telefone - Celular")),
        norm_phone(row.get("API_TEL_OFICIAL")),
    ])
    phones = [x for x in phones if x]
    emails = dedupe([norm_email(row.get("API_EMAIL"))])
    emails = [x for x in emails if x]

    matches = by_title.get(norm_title(title), [])
    status = ""
    master_deal_id = None
    duplicate_deal_ids = []

    if not matches:
        status = "NO_MATCH_BY_TITLE"
    elif len(matches) == 1:
        status = "MATCH_BY_TITLE"
        master_deal_id = int(matches[0].get("id") or 0)
    else:
        status = "DUPLICATE_TITLE"
        matches = sorted(matches, key=lambda d: int(d.get("id") or 10**12))
        master_deal_id = int(matches[0].get("id") or 0)
        duplicate_deal_ids = [int(d.get("id") or 0) for d in matches[1:]]

    master = matches[0] if matches else {}
    plan.append({
        "company": company,
        "title": title,
        "person": person,
        "cnpj": cnpj,
        "status": status,
        "master_deal_id": master_deal_id,
        "duplicate_deal_ids": duplicate_deal_ids,
        "matched_count": len(matches),
        "org_id": extract_id(master.get("org_id")),
        "person_id": extract_id(master.get("person_id")),
        "pipeline_id": int(master.get("pipeline_id") or 0) if master else None,
        "stage_id": int(master.get("stage_id") or 0) if master else None,
        "phones_new": phones,
        "emails_new": emails,
        "api_razao": row.get("API_RAZAO") or "",
        "api_situacao": row.get("API_SITUACAO") or "",
        "linkedin_mkt": row.get("Linkedin Gerente Mkt") or "",
        "linkedin_vendas": row.get("Linkedin Gerente Vendas") or "",
    })

OUT_JSON.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow([
        "company","title","person","cnpj","status","master_deal_id","matched_count",
        "duplicate_deal_ids","org_id","person_id","pipeline_id","stage_id",
        "phones_new","emails_new","api_razao","api_situacao"
    ])
    for r in plan:
        w.writerow([
            r["company"], r["title"], r["person"], r["cnpj"], r["status"], r["master_deal_id"],
            r["matched_count"], "|".join(map(str, r["duplicate_deal_ids"])),
            r["org_id"], r["person_id"], r["pipeline_id"], r["stage_id"],
            "|".join(r["phones_new"]), "|".join(r["emails_new"]),
            r["api_razao"], r["api_situacao"]
        ])

print("FINAL_COUNTS =", dict(Counter(r["status"] for r in plan)))
print("OUT_JSON =", OUT_JSON)
print("OUT_CSV =", OUT_CSV)
