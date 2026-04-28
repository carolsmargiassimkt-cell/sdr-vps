import json
import time
from pathlib import Path
from crm.pipedrive_client import PipedriveClient

INPUT_JSON = Path("/root/sdr-vps/runtime/base_leads_linkedin_pipeline2_planned.json")
STATE_JSON = Path("/root/sdr-vps/runtime/apply_planned_writeback_state.json")

BATCH_LIMIT = 10
BATCH_DELAY_SEC = 5

def load_state():
    if not STATE_JSON.exists():
        return {"done": []}
    return json.loads(STATE_JSON.read_text())

def save_state(state):
    STATE_JSON.parent.mkdir(parents=True, exist_ok=True)
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2))

def log(msg):
    print(msg, flush=True)

def dedupe(seq):
    out = []
    seen = set()
    for x in seq or []:
        x = str(x).strip().lower()
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def main():
    rows = json.loads(INPUT_JSON.read_text())
    state = load_state()
    done = set(state.get("done", []))

    client = PipedriveClient()

    processed = 0

    log(f"[START] total={len(rows)} done={len(done)}")

    for row in rows:
        key = row.get("group_key")
        if key in done:
            continue

        deal_id = int(row.get("master_deal_id") or 0)
        org_id = int(row.get("target_org_id") or 0)
        person_id = int(row.get("target_person_id") or 0)

        if not deal_id or not org_id:
            log(f"[SKIP_INVALID] {key}")
            done.add(key)
            continue

        deal = client.get_deal_details(deal_id)
        if not deal:
            log(f"[ERRO_DEAL] {deal_id}")
            continue

        org = client.get_organization(org_id)
        if not org:
            log(f"[ERRO_ORG] {org_id}")
            continue

        # Atualiza nome da empresa
        canonical_name = str(row.get("canonical_name") or "").strip()
        if canonical_name and org.get("name") != canonical_name:
            client.update_organization(org_id, {"name": canonical_name})

        # Atualiza pessoa
        if person_id:
            person = client.get_person_details(person_id)
            if person:
                phones = dedupe(row.get("target_phones") or [])
                emails = dedupe(row.get("target_emails") or [])

                payload = {}

                if phones:
                    payload["phone"] = [{"value": p, "primary": i == 0} for i, p in enumerate(phones)]

                if emails:
                    payload["email"] = [{"value": e, "primary": i == 0} for i, e in enumerate(emails)]

                if payload:
                    client.update_person(person_id, payload)

        done.add(key)
        processed += 1
        save_state({"done": list(done)})

        log(f"[OK] deal={deal_id} org={org_id}")

        if processed >= BATCH_LIMIT:
            log(f"[PAUSA] {BATCH_DELAY_SEC}s")
            time.sleep(BATCH_DELAY_SEC)
            processed = 0

    log("[FIM]")

if __name__ == "__main__":
    main()
