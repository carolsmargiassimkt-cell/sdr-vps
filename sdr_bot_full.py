"""Legacy all-in-one SDR bot disabled.

Production runtime is FastAPI + email_cadence + scripts/whatsapp_warm_cadence.py.
"""


def main() -> int:
    print("[LEGACY_SDR_BOT_DISABLED]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
