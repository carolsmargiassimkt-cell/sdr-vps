from fastapi import APIRouter, Request

router = APIRouter()


@router.post("/webhooks/pipedrive")
async def webhook(req: Request):
    body = await req.json()
    deal_id = (body.get("current") or {}).get("id")
    print("[WA_WEBHOOK_ROOT_DISABLED]", deal_id or "-")
    return {
        "ok": True,
        "sent": False,
        "skip": "root_webhook_disabled_use_app_webhooks_pipedrive",
    }
