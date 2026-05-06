"""Legacy supervisor intentionally disabled.

The production SDR flow is native Python/FastAPI and uses a single automatic
WhatsApp emitter: scripts/whatsapp_warm_cadence.py.
"""

from __future__ import annotations


def main() -> int:
    print("[LEGACY_SUPERVISOR_DISABLED] Use scripts/whatsapp_warm_cadence.py for WhatsApp warm cadence.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
