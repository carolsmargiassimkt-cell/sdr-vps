"""Legacy direct WhatsApp sender disabled."""


def enviar_whatsapp(*_args, **_kwargs):
    print("[LEGACY_WA_SENDER_DISABLED] use scripts/whatsapp_warm_cadence.py")
    return False


if __name__ == "__main__":
    raise SystemExit(0)
