"""Legacy direct WhatsApp sender disabled."""


def enviar_whatsapp(*_args, **_kwargs):
    print("[LEGACY_WA_SENDER_DISABLED] use scripts/whatsapp_warm_cadence.py")
    return False


def send_message(*args, **kwargs):
    return enviar_whatsapp(*args, **kwargs)
