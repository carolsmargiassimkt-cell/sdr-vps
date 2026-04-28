import json
import re
import unicodedata
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
XLSX = Path("/root/BASE_LEADS_COM_LINKEDIN.xlsx")
MISSING_PERSONS = Path("/root/sdr-vps/runtime/enrich_missing_persons_from_sheet_result.json")
NO_MATCH = Path("/root/sdr-vps/runtime/reconcile_export_vs_full_pipeline2_api_v2_nomatch.json")
OUT_JSON = Path("/root/sdr-vps/runtime/oportunidades_targets.json")
OUT_CSV = Path("/root/sdr-vps/runtime/oportunidades_targets.csv")

def strip_accents(v):
    s = str(v or "").strip()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def norm(v):
    s = strip_accents(v).upper()
    s = re.sub(r"\s*-\s*LEAD\s*$", "", s, flags=re.I)
    s = re.sub(r"\s*-\s*DEAL\s*$", "", s, flags=re.I)
    s = re.sub(r"\s*LEADSTER\s*$", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip(" -")
    return s

def norm_cnpj(v):
    d = re.sub(r"\D+", "", str(v or ""))
    return d if len(d) == 14 else ""

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
        out.append(dict(zip(header, vals)))
    return out

sheet = parse_xlsx(XLSX)

by_company = {}
by_title = {}

for row in sheet:
    company = norm(row.get("Organização - Nome"))
    title = norm(row.get("Negócio - Título"))
    cnpj = norm_cnpj(row.get("cnpj_formatado"))
    rec = {
        "company": row.get("Organização - Nome") or "",
        "title": row.get("Negócio - Título") or "",
        "person": row.get("Pessoa - Nome") or "",
        "cnpj": cnpj,
        "api_email": row.get("API_EMAIL") or "",
        "api_tel_oficial": row.get("API_TEL_OFICIAL") or "",
        "api_razao": row.get("API_RAZAO") or "",
        "api_situacao": row.get("API_SITUACAO") or "",
    }
    if company and company not in by_company:
        by_company[company] = rec
    if title and title not in by_title:
        by_title[title] = rec

targets = []

if MISSING_PERSONS.exists():
    data = json.load(open(MISSING_PERSONS))
    for r in data:
        if r.get("status") != "needs_review":
            continue
        title = r.get("title") or ""
        key = norm(title)
        src = by_title.get(key) or by_company.get(key) or {}
        targets.append({
            "source": "missing_persons",
            "deal_id": r.get("deal_id"),
            "title": title,
            "company": src.get("company") or title,
            "cnpj": src.get("cnpj") or "",
            "api_email": src.get("api_email") or "",
            "api_tel_oficial": src.get("api_tel_oficial") or "",
            "api_razao": src.get("api_razao") or "",
            "api_situacao": src.get("api_situacao") or "",
            "reason": r.get("reason") or "",
        })

if NO_MATCH.exists():
    data = json.load(open(NO_MATCH))
    for r in data:
        targets.append({
            "source": "no_match",
            "deal_id": r.get("master_deal_id"),
            "title": r.get("title") or "",
            "company": r.get("company") or "",
            "cnpj": norm_cnpj(r.get("cnpj")),
            "api_email": "",
            "api_tel_oficial": "",
            "api_razao": "",
            "api_situacao": "",
            "reason": "no_match",
        })

# dedupe by (source,title,company,cnpj)
seen = set()
final = []
for t in targets:
    key = (t["source"], norm(t["title"]), norm(t["company"]), t["cnpj"])
    if key in seen:
        continue
    seen.add(key)
    final.append(t)

OUT_JSON.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")

with open(OUT_CSV, "w", encoding="utf-8") as f:
    f.write("source,deal_id,title,company,cnpj,reason,api_email,api_tel_oficial,api_razao,api_situacao\n")
    for r in final:
        vals = [
            r.get("source",""),
            str(r.get("deal_id","") or ""),
            r.get("title","").replace(",", " "),
            r.get("company","").replace(",", " "),
            r.get("cnpj",""),
            r.get("reason","").replace(",", " "),
            r.get("api_email","").replace(",", " "),
            r.get("api_tel_oficial","").replace(",", " "),
            r.get("api_razao","").replace(",", " "),
            r.get("api_situacao","").replace(",", " "),
        ]
        f.write(",".join(vals) + "\n")

with_cnpj = sum(1 for x in final if x.get("cnpj"))
without_cnpj = len(final) - with_cnpj
print("TOTAL_TARGETS =", len(final))
print("WITH_CNPJ =", with_cnpj)
print("WITHOUT_CNPJ =", without_cnpj)
print("OUT_JSON =", OUT_JSON)
print("OUT_CSV =", OUT_CSV)
