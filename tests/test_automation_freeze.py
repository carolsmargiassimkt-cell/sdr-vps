from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from core.automation_freeze import is_automation_freeze_active, load_automation_freeze, save_automation_freeze


class AutomationFreezeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.freeze_file = Path(self.tempdir.name) / "automation_freeze.json"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_freeze_stays_active_until_wednesday_0900(self) -> None:
        save_automation_freeze(
            {
                "active": True,
                "resume_at": "2026-04-22T09:00:00-03:00",
                "timezone": "America/Sao_Paulo",
                "services": ["whatsapp", "sdr-bot", "enrichment", "inbound"],
                "drop_inbound": True,
                "reason": "holiday_freeze",
            },
            path=self.freeze_file,
        )

        self.assertTrue(
            is_automation_freeze_active(
                service="whatsapp",
                now=datetime(2026, 4, 21, 12, 0, tzinfo=ZoneInfo("America/Sao_Paulo")),
                path=self.freeze_file,
            )
        )
        self.assertFalse(
            is_automation_freeze_active(
                service="whatsapp",
                now=datetime(2026, 4, 22, 9, 0, tzinfo=ZoneInfo("America/Sao_Paulo")),
                path=self.freeze_file,
            )
        )

    def test_load_freeze_defaults_when_missing(self) -> None:
        payload = load_automation_freeze(path=self.freeze_file)
        self.assertFalse(payload["active"])
        self.assertEqual(payload["services"], [])


if __name__ == "__main__":
    unittest.main()
