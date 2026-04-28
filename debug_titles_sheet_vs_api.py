import json, re, unicodedata, zipfile, xml.etree.ElementTree as ET, requests
from pathlib import Path

INPUT_XLSX = Path("/root/BASE_LEADS_COM_LINKEDIN.xlsx")
CFG = Path("/root/sdr-vps/config/system_config.json")
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

def strip_accents(v):
    s = str(v or "").strip()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def norm(v):
    return re.sub(r"\s+", " ", strip_accents(v).upper()).strip()

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
sheet = parse_xlsx(INPUT_XLSX)

print("=== PLANILHA (20) ===")
for r in sheet[:20]:
    print("TITLE:", r.get("Negócio - Título"))
    print("ORG  :", r.get("Organização - Nome"))
    print("---")

print("\n=== API PIPELINE 2 (20) ===")
for d in p2[:20]:
    print("TITLE:", d.get("title"))
    print("ORG  :", d.get("org_name"))
    print("STAGE:", d.get("stage_id"))
    print("---")
