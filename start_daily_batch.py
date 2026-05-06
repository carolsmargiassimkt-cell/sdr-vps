import os
import requests
import time

PIPEDRIVE_API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")
PIPELINE_STAGE_ID = int(os.getenv("EMAIL_ENTRY_STAGE_ID", "51"))
BATCH_LIMIT = int(os.getenv("EMAIL_DAILY_BATCH_LIMIT", "50"))
API_URL = os.getenv("API_URL", "http://127.0.0.1:8001")

PAGE_LIMIT = int(os.getenv("EMAIL_BATCH_PAGE_LIMIT", "100"))
MAX_SCAN = int(os.getenv("EMAIL_BATCH_MAX_SCAN", "1000"))

def main():
    if not PIPEDRIVE_API_TOKEN:
        raise SystemExit("PIPEDRIVE_API_TOKEN ausente")

    queued = 0
    sem_email = 0
    already_queued = 0
    skipped = 0
    errors = 0
    scanned = 0
    start = 0

    while queued < BATCH_LIMIT and scanned < MAX_SCAN:
        r = requests.get(
            "https://api.pipedrive.com/v1/deals",
            params={
                "api_token": PIPEDRIVE_API_TOKEN,
                "status": "open",
                "stage_id": PIPELINE_STAGE_ID,
                "start": start,
                "limit": PAGE_LIMIT,
            },
            timeout=60,
        )
        r.raise_for_status()
        js = r.json() or {}
        data = js.get("data") or []

        if not data:
            break

        print(f"[BATCH_PAGE] start={start} encontrados={len(data)}")

        for d in data:
            if queued >= BATCH_LIMIT or scanned >= MAX_SCAN:
                break

            scanned += 1
            did = d.get("id")
            if not did:
                skipped += 1
                continue

            try:
                rr = requests.post(
                    f"{API_URL}/webhooks/email-cadence",
                    json={"deal_id": did},
                    timeout=60,
                )

                body = rr.text[:300]
                print(f"[BATCH_ITEM] deal={did} status={rr.status_code} body={body}")

                if not rr.ok:
                    errors += 1
                    continue

                out = rr.json()
                if out.get("queued"):
                    queued += 1
                elif out.get("skip") == "sem_email":
                    sem_email += 1
                elif out.get("skip") == "already_queued":
                    already_queued += 1
                else:
                    skipped += 1

            except Exception as e:
                errors += 1
                print(f"[BATCH_FAIL] deal={did} err={e}")

            time.sleep(0.1)

        more = (js.get("additional_data") or {}).get("pagination", {}).get("more_items_in_collection")
        next_start = (js.get("additional_data") or {}).get("pagination", {}).get("next_start")

        if not more or next_start is None:
            break

        start = int(next_start)

    print(
        f"[BATCH_SUMMARY] scanned={scanned} queued={queued} "
        f"sem_email={sem_email} already_queued={already_queued} "
        f"skipped={skipped} errors={errors} target={BATCH_LIMIT}"
    )

if __name__ == "__main__":
    main()
