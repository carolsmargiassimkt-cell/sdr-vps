from __future__ import annotations

import unittest

from crm.pipeline2_holiday_hygiene import (
    PIPELINE_ID,
    build_deal_hygiene_plan,
    merge_contact_values,
    normalize_cnpj,
    person_contact_summary,
    select_oldest_master,
)
from crm.pipedrive_client import PipedriveClient


class FakeClient(PipedriveClient):
    def __init__(self) -> None:
        super().__init__()


class Pipeline2HolidayHygieneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeClient()

    def test_normalize_cnpj_before_writeback(self) -> None:
        self.assertEqual(normalize_cnpj("12.345.678/0001-90"), "12345678000190")
        self.assertEqual(normalize_cnpj("123"), "")

    def test_select_master_prefers_oldest_record(self) -> None:
        records = [
            {"id": 12, "add_time": "2026-04-20 11:00:00"},
            {"id": 3, "add_time": "2026-04-19 08:00:00"},
            {"id": 9, "add_time": "2026-04-19 08:00:00"},
        ]
        master = select_oldest_master(records)
        self.assertEqual(master["id"], 3)

    def test_merge_contact_values_deduplicates_person_and_org_channels(self) -> None:
        merged = merge_contact_values(["11999990000", "11999990000"], ["contato@empresa.com"], ["contato@empresa.com"])
        self.assertEqual(merged, ["11999990000", "contato@empresa.com"])

    def test_person_contact_summary_requires_valid_contact_and_correct_org(self) -> None:
        person = {
            "id": 7,
            "org_id": {"value": 11},
            "phone": [{"value": "+55 11 99999-0000"}],
            "email": [{"value": "lead@empresa.com"}],
        }
        summary = person_contact_summary(person, target_org_id=11)
        self.assertTrue(summary["valid"])
        self.assertFalse(person_contact_summary(person, target_org_id=99)["valid"])

    def test_build_plan_keeps_stage_unchanged_and_only_pipeline2(self) -> None:
        deal = {
            "id": 101,
            "pipeline_id": PIPELINE_ID,
            "stage_id": 777,
            "org_id": {"value": 9},
            "person_id": {"value": 21},
        }
        organization = {"id": 9, "name": "Empresa", "cnpj": ""}
        primary_person = {"id": 21, "org_id": {"value": 9}, "phone": [{"value": "11999990000"}]}
        participants = [
            {"id": 5001, "person_id": {"value": 88}, "person": {"id": 88, "org_id": {"value": 999}, "phone": [{"value": "11988887777"}]}}
        ]
        plan = build_deal_hygiene_plan(
            client=self.client,
            deal=deal,
            organization=organization,
            primary_person=primary_person,
            participants=participants,
            org_group=[organization],
            person_group_index={},
        )
        self.assertTrue(plan["safe"])
        self.assertEqual(plan["stage_id_preserved"], 777)
        self.assertNotIn("stage_id", plan["deal_update"])
        self.assertIn(5001, plan["remove_participants"])

        outside_plan = build_deal_hygiene_plan(
            client=self.client,
            deal={**deal, "pipeline_id": 9},
            organization=organization,
            primary_person=primary_person,
            participants=[],
            org_group=[organization],
            person_group_index={},
        )
        self.assertFalse(outside_plan["safe"])
        self.assertEqual(outside_plan["skip_reason"], "outside_pipeline_2")


if __name__ == "__main__":
    unittest.main()
