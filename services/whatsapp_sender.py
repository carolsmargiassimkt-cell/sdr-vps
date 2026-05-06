"""Compatibility shim for legacy imports.

Automatic WhatsApp sending is centralized in scripts/whatsapp_warm_cadence.py.
"""


def send_whatsapp(*_args, **_kwargs):
    print("[LEGACY_WA_SENDER_DISABLED] use scripts/whatsapp_warm_cadence.py")
    return False
