import os, json, requests
from pathlib import Path

BASE = "https://api.pipedrive.com/v1"
EVENTS = Path("/root/sdr-vps/data/email_cadence_events.json")
SYNCED = Path("/root/sdr-vps/data/email_cadence_events_synced.json")

def token():
    return os.getenv("PIPEDRIVE_TOKEN") or os.getenv("PIPEDRIVE_API_TOKEN")

def api(method, path, **kwargs):
    t = token()
    if not t:
        print("[CRM_SYNC_SKIP] token ausente")
        return None
    params = kwargs.pop("params", {})
    params["api_token"] = t
    r = requests.request(method, BASE + path, params=params, timeout=20, **kwargs)
    if r.status_code >= 400:
        print("[CRM_SYNC_FAIL]", method, path, r.status_code, r.text[:300])
        return None
    return r.json()

def add_note(deal_id, content):
    return api("POST", "/notes", json={"deal_id": deal_id, "content": content})

def add_label_note(deal_id, label):
    # fallback seguro: registra no histórico mesmo se não conseguir aplicar label nativa
    return add_note(deal_id, f"TAG/CADÊNCIA aplicada: <b>{label}</b>")

def load_json(path, default):
    if not path.exists():
        return default
    try:
        return json.load(open(path, encoding="utf-8"))
    except Exception:
        return default

def save_json(path, obj):
    json.dump(obj, open(path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

def sync_email_events():
    events = load_json(EVENTS, [])
    synced = set(load_json(SYNCED, []))
    novos = 0

    for ev in events:
        key = ev.get("idempotency_key")
        if not key or key in synced:
            continue

        deal_id = ev.get("deal_id")
        step = ev.get("step")
        email = ev.get("email")
        subject = ev.get("subject")
        sent_at = ev.get("sent_at")
        label = f"EMAIL_CAD{step}"

        note = f"""
<b>Email de cadência enviado</b><br>
<b>Etapa:</b> {label}<br>
<b>Para:</b> {email}<br>
<b>Assunto/template:</b> {subject}<br>
<b>Enviado em:</b> {sent_at}<br>
<b>Tracking:</b> clique rastreado via /t/{deal_id}/{step}
"""
        add_note(deal_id, note)
        add_label_note(deal_id, label)

        synced.add(key)
        novos += 1
        print(f"[CRM_EMAIL_SYNCED] {key}")

    save_json(SYNCED, sorted(synced))
    print("[CRM_SYNC_DONE]", novos)

if __name__ == "__main__":
    sync_email_events()
