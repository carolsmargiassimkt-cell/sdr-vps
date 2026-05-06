"""Legacy WhatsApp autopilot disabled.

This module is kept only to avoid import errors from old code paths. It must
not send WhatsApp messages.
"""


class AutopilotService:
    def __init__(self, *args, **kwargs):
        self.leads = []

    def run_forever(self):
        print("[LEGACY_AUTOPILOT_DISABLED] use scripts/whatsapp_warm_cadence.py")
        return None
