"""Legacy WhatsApp hook disabled."""


def enviar_whatsapp(*_args, **_kwargs):
    print("[LEGACY_WA_HOOK_DISABLED] use scripts/whatsapp_warm_cadence.py")
    return False
