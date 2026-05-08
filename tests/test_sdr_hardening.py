from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core import human_handoff
from core.pipedrive_safe import _normalize_labels
from core.sdr_stop_rules import should_stop_all_automation
from email_inbox.import_email_leads import clean_org_name, extract_lead_info, is_blocked_test_email
from scripts.reconcile_wa_warm_delivery import is_test_email


class SdrHardeningTests(unittest.TestCase):
    def test_leadster_parser_and_anti_html_org(self):
        text = "Nome: Ana Email: ana@cliente.com.br Telefone: 31999998888 Empresa: Loja Boa Mensagem: Quero campanha Copa"
        lead = extract_lead_info(text, "leadster@leadster.com.br")
        self.assertEqual(lead["email"], "ana@cliente.com.br")
        self.assertEqual(lead["phone"], "5531999998888")
        self.assertEqual(clean_org_name("<style>body{padding:0}</style>"), "")

    def test_test_email_block(self):
        self.assertTrue(is_blocked_test_email("gabriel.test@ademicon.com.br"))
        self.assertTrue(is_blocked_test_email("lead@example.com"))
        self.assertTrue(is_test_email("foo.test@empresa.com.br"))
        self.assertFalse(is_test_email("gabriel.schultz@ademicon.com.br"))

    def test_human_handoff_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "human_handoff_state.json"
            with patch.object(human_handoff, "HANDOFF_FILE", target):
                self.assertFalse(human_handoff.is_human_handoff("41999193626"))
                human_handoff.set_human_handoff("41999193626", deal_id=3419, person_id=5923, org_id=2460)
                self.assertTrue(human_handoff.is_human_handoff("5541999193626"))
                human_handoff.clear_human_handoff("5541999193626")
                self.assertFalse(human_handoff.is_human_handoff("41999193626"))

    def test_stop_rules(self):
        self.assertTrue(should_stop_all_automation({"status": "won"}))
        self.assertTrue(should_stop_all_automation({"label": [196]}))
        self.assertTrue(should_stop_all_automation({"status_sdr": "meeting_booked"}))
        self.assertFalse(should_stop_all_automation({"status": "open", "label": [193]}))

    def test_label_normalization_preserves_existing(self):
        self.assertEqual(_normalize_labels("193,226"), ["193", "226"])
        self.assertEqual(_normalize_labels([193, 226]), [193, 226])


if __name__ == "__main__":
    unittest.main()

