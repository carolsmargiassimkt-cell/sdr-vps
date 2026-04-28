from __future__ import annotations
import json, re, time, zipfile, xml.etree.ElementTree as ET
from pathlib import Path
from crm.pipedrive_client import PipedriveClient

PLAN_JSON = Path("/root/sdr-vps/runtime/reconcile_export_vs_full_pipeline2_api_v2.json")
INPUT_XLSX = Path("/root/BASE_LEADS_COM_LINKEDIN.xlsx")
STATE_JSON = Path("/root/sdr-vps/runtime/apply_reconcile_v2_state.json")

BATCH_LIMIT = 10
BATCH_DELAY_SEC = 5
LOST_STAGE_ID = 50  # ajuste se seu perdido for outro

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

def log(msg):
    print(msg, flush=True)

def normalize_phone(v):
    d = re.sub(r"\D+", "", str(v or ""))
    if d.startswith("55") and len(d) > 11:
        d = d[2:]
    if len(d) not in {10, 11}:
        return ""
    return d

def normalize_email(v):
    s = str(v or "").strip().lower()
    return s if "@" in s else ""

def dedupe(vals):
    out, seen = [], set()
    for v in vals:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out

def load_state():
    if not STATE_JSON.exists():
        return {"done": []}
    return json.loads(STATE_JSON.read_text(encoding="utf-8"))

def save_state(state):
    STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

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

def title_key(v):
    s = str(v or "").strip().upper()
    s = re.sub(r"\s*-\s*LEAD\s*$", "", s, flags=re.I)
    s = re.sub(r"\s*-\s*DEAL\s*$", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s).strip(" -")
    return s

sheet = parse_xlsx(INPUT_XLSX)
sheet_by_title = {}
for row in sheet:
    sheet_by_title[title_key(row.get("Negócio - Título"))] = row

plan = json.loads(PLAN_JSON.read_text(encoding="utf-8"))
state = load_state()
done = set(state.get("done", []))

client = PipedriveClient()
processed = 0
ok = 0
skip = 0
errors = 0

log(f"[START] total={len(plan)} done={len(done)}")

for item in plan:
    if item.get("status") not in {"MATCH", "DUPLICATE_MATCH"}:
        continue

    master_deal_id = int(item.get("master_deal_id") or 0)
    org_id = int(item.get("org_id") or 0) if item.get("org_id") else 0
    person_id = int(item.get("person_id") or 0) if item.get("person_id") else 0
    dups = item.get("duplicate_deal_ids") or []
    key = f"{item.get('title')}|{master_deal_id}"

    if key in done:
        continue

    row = sheet_by_title.get(title_key(item.get("title")))
    if not row or not master_deal_id or not org_id:
        log(f"[SKIP_INVALID] {item.get('title')}")
        skip += 1
        done.add(key)
        save_state({"done": sorted(done)})
        continue

    phones = dedupe([
        normalize_phone(row.get("Pessoa - Telefone - Trabalho")),
        normalize_phone(row.get("Pessoa - Telefone - Celular")),
        normalize_phone(row.get("API_TEL_OFICIAL")),
    ])
    phones = [x for x in phones if x]

    emails = dedupe([normalize_email(row.get("API_EMAIL"))])
    emails = [x for x in emails if x]

    org = client.get_organization(org_id) or {}
    org_payload = {}

    canonical_name = str(row.get("Organização - Nome") or "").strip().upper()
    if canonical_name and str(org.get("name") or "").strip().upper() != canonical_name:
        org_payload["name"] = canonical_name

    if org_payload:
        if not client.update_organization(org_id, org_payload):
            log(f"[ORG_ERROR] org={org_id} status={client.last_http_status}")
            errors += 1
            continue

    if person_id:
        person_payload = {}
        if phones:
            person_payload["phone"] = [{"value": p, "primary": i == 0} for i, p in enumerate(phones)]
        if emails:
            person_payload["email"] = [{"value": e, "primary": i == 0} for i, e in enumerate(emails)]

        if person_payload:
            log(f"[DEBUG_PERSON_PAYLOAD] {person_payload}")
            if not client.update_person(person_id, person_payload):
                log(f"[PERSON_ERROR] person={person_id} status={client.last_http_status}")
                errors += 1
                continue

    # deals duplicados mais novos -> perdido
    for dup_id in dups:
        try:
            dup_id = int(dup_id)
        except Exception:
            continue
        if dup_id and dup_id != master_deal_id:
            ok_stage = client.update_stage(deal_id=dup_id, stage_id=LOST_STAGE_ID)
            if ok_stage:
                log(f"[DUP_LOST_OK] master={master_deal_id} dup={dup_id}")
            else:
                log(f"[DUP_LOST_ERROR] master={master_deal_id} dup={dup_id} status={client.last_http_status}")

    done.add(key)
    save_state({"done": sorted(done)})
    ok += 1
    processed += 1
    log(f"[OK] deal={master_deal_id} org={org_id}")

    if processed >= BATCH_LIMIT:
        log(f"[PAUSA] {BATCH_DELAY_SEC}s")
        time.sleep(BATCH_DELAY_SEC)
        processed = 0

log(f"[FIM] ok={ok} skip={skip} errors={errors}")
