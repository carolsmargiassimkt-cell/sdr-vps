"""Legacy direct traffic WhatsApp sender disabled.

Traffic leads are now handled by forms/email ingestion and the single official
WhatsApp emitter: scripts/whatsapp_warm_cadence.py.
"""


def main() -> int:
    print("[LEGACY_WA_SENDER_DISABLED] use scripts/whatsapp_warm_cadence.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
