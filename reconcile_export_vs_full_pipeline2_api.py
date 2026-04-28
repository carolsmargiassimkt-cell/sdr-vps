from __future__ import annotations
import csv, json, re, unicodedata, zipfile, xml.etree.ElementTree as ET, requests
from collections import Counter, defaultdict
from pathlib import Path

INPUT_XLSX = Path("/root/BASE_LEADS_COM_LINKEDIN.xlsx")
OUT_JSON = Path("/root/sdr-vps/runtime/reconcile_export_vs_full_pipeline2_api.json")
OUT_CSV = Path("/root/sdr-vps/runtime/reconcile_export_vs_full_pipeline2_api.csv")
CFG = Path("/root/sdr-vps/config/system_config.json")
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

def strip_accents(v):
    s = str(v or "").strip()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def norm_title(v):
    return re.sub(r"\s+", " ", strip_accents(v).upper()).strip()

def norm_name(v):
    return re.sub(r"\s+", " ", strip_accents(v).upper()).strip()

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

cfg = json.load(open(CFG))
API_TOKEN = cfg.get("pipedrive_token") or cfg.get("pipedrive_api_token") or ""
BASE = "https://api.pipedrive.com/v1/deals"

all_deals = []
start = 0
limit = 500
while True:
    r = requests.get(BASE, params={
        "api_token": API_TOKEN,
        "start": start,
        "limit": limit,
        "status": "all_not_deleted",
    }, timeout=60)
    r.raise_for_status()
    payload = r.json()
    batch = payload.get("data") or []
    all_deals.extend(batch)
    pag = payload.get("additional_data", {}).get("pagination", {})
    if not pag.get("more_items_in_collection", False) or not batch:
        break
    start += limit

p2 = [d for d in all_deals if int(d.get("pipeline_id") or 0) == 2]

by_title = defaultdict(list)
for d in p2:
    by_title[norm_title(d.get("title"))].append(d)

sheet = parse_xlsx(INPUT_XLSX)
plan = []

for row in sheet:
    title = row.get("Negócio - Título") or ""
    company = row.get("Organização - Nome") or ""
    person = row.get("Pessoa - Nome") or ""
    cnpj = norm_cnpj(row.get("cnpj_formatado"))

    matches = by_title.get(norm_title(title), [])
    if not matches:
        status = "NO_MATCH_BY_TITLE"
        master_deal_id = None
        duplicate_deal_ids = []
    elif len(matches) == 1:
        status = "MATCH_BY_TITLE"
        master_deal_id = int(matches[0].get("id") or 0)
        duplicate_deal_ids = []
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
        "org_id": (master.get("org_id") or {}).get("value") if isinstance(master.get("org_id"), dict) else master.get("org_id"),
        "person_id": (master.get("person_id") or {}).get("value") if isinstance(master.get("person_id"), dict) else master.get("person_id"),
        "stage_id": master.get("stage_id"),
    })

OUT_JSON.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")
with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["company","title","person","cnpj","status","master_deal_id","matched_count","duplicate_deal_ids","org_id","person_id","stage_id"])
    for r in plan:
        w.writerow([
            r["company"], r["title"], r["person"], r["cnpj"], r["status"],
            r["master_deal_id"], r["matched_count"], "|".join(map(str, r["duplicate_deal_ids"])),
            r["org_id"], r["person_id"], r["stage_id"]
        ])

print("FINAL_COUNTS =", dict(Counter(r["status"] for r in plan)))
print("OUT_JSON =", OUT_JSON)
print("OUT_CSV =", OUT_CSV)
