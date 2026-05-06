import os
import requests

PIPEDRIVE_API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")
PIPELINE_STAGE_ID = int(os.getenv("EMAIL_ENTRY_STAGE_ID", "51"))
BATCH_LIMIT = int(os.getenv("EMAIL_DAILY_BATCH_LIMIT", "50"))
API_URL = os.getenv("API_URL", "http://127.0.0.1:8001")

r = requests.get(
    "https://api.pipedrive.com/v1/deals",
    params={
        "api_token": PIPEDRIVE_API_TOKEN,
        "status": "open",
        "stage_id": PIPELINE_STAGE_ID,
        "limit": BATCH_LIMIT
    },
    timeout=60
)

data = r.json().get("data") or []

print(f"[BATCH] encontrados={len(data)}")

ok = 0

for d in data:
    did = d.get("id")
    if not did:
        continue

    try:
        rr = requests.post(
            f"{API_URL}/webhooks/email-cadence",
            json={"deal_id": did},
            timeout=60
        )

        print(f"[BATCH_ITEM] deal={did} status={rr.status_code} body={rr.text[:200]}")

        if rr.ok:
            ok += 1

    except Exception as e:
        print(f"[BATCH_FAIL] deal={did} err={e}")

print(f"[BATCH] adicionados={ok}")
