from __future__ import annotations

from common import all_deals, severity, write_reports
from core.crm_hygiene import contamination_score, person_primary_email


def main():
    rows = []
    for deal in all_deals():
        person = deal.get("person_id") if isinstance(deal.get("person_id"), dict) else {}
        email = person_primary_email(person)
        if email:
            continue
        score, reasons = contamination_score(deal=deal)
        rows.append({
            "severity": severity(score),
            "risk_score": score,
            "deal_id": deal.get("id") or "",
            "deal_title": deal.get("title") or "",
            "status": deal.get("status") or "",
            "pipeline_id": deal.get("pipeline_id") or "",
            "stage_id": deal.get("stage_id") or "",
            "person_id": person.get("value") or person.get("id") or "",
            "person_name": person.get("name") or "",
            "org_id": (deal.get("org_id") or {}).get("value") if isinstance(deal.get("org_id"), dict) else deal.get("org_id") or "",
            "org_name": (deal.get("org_id") or {}).get("name") if isinstance(deal.get("org_id"), dict) else "",
            "reasons": ",".join(reasons),
        })
    rows.sort(key=lambda item: (-int(item["risk_score"]), str(item["deal_title"])))
    write_reports("deals_sem_email", rows)


if __name__ == "__main__":
    main()
