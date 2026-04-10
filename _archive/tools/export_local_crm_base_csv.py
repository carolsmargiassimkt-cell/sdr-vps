from __future__ import annotations

import csv
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_FILE = PROJECT_ROOT / "data" / "local_crm_base.json"
OUTPUT_FILE = PROJECT_ROOT / "data" / "local_crm_base_matrix.csv"
FAILED_FILE = PROJECT_ROOT / "data" / "local_crm_base_invalid_cnpjs.csv"


def main() -> int:
    payload = json.loads(SOURCE_FILE.read_text(encoding="utf-8"))
    records = [item for item in payload.get("records", []) if isinstance(item, dict)]
    fieldnames = [
        "empresa",
        "nome",
        "cnpj",
        "telefone",
        "email",
        "phones",
        "emails",
        "decision_makers",
        "websites",
        "aliases",
        "deal_id",
        "person_id",
        "origem_oficial",
        "super_minas",
        "tags",
        "status_bot",
        "blacklisted",
        "blacklist_reason",
        "source",
        "enriched_source",
        "enriched_at",
    ]
    with OUTPUT_FILE.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for record in records:
            row = {}
            for field in fieldnames:
                value = record.get(field)
                if isinstance(value, list):
                    value = " | ".join(str(item) for item in value if str(item).strip())
                row[field] = value
            writer.writerow(row)

    failed = [
        {
            "empresa": record.get("empresa"),
            "cnpj": record.get("cnpj"),
            "source": record.get("source"),
        }
        for record in records
        if record.get("cnpj")
        and not record.get("enriched_source")
        and not (record.get("telefone") or record.get("email") or record.get("phones") or record.get("emails"))
    ]
    with FAILED_FILE.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["empresa", "cnpj", "source"], delimiter=";")
        writer.writeheader()
        writer.writerows(failed)
    print(f"[LOCAL_CRM_EXPORT] matriz={OUTPUT_FILE} invalidos={FAILED_FILE} total={len(records)} invalidos_count={len(failed)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
