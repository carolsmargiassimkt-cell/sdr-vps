from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from enrich_missing_cnpj_from_xlsx import normalize_phone, parse_xlsx


PEOPLE_XLSX = Path(r"C:\Users\Asus\Downloads\people-25681240-15.xlsx")
ORGS_XLSX = Path(r"C:\Users\Asus\Downloads\organizations-25681240-16.xlsx")
OUT_DIR = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\people_org_audit")

POLLUTED_ORG_TOKENS = (
    "BRASIMAC",
    "PRECIOSO FRUTO",
    "CEBRAC",
    "JOSE DONIZETI",
    "RED DOOR",
)


def log(message: str) -> None:
    print(str(message), flush=True)


def save_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in headers:
                headers.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers or ["empty"])
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def normalized_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def email_is_suspicious(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if "@" not in text:
        return True
    if text.endswith((".png", ".jpg", ".jpeg", ".svg", ".gif", ".webp")):
        return True
    if text.startswith(("http://", "https://")):
        return True
    return False


def phone_is_generic(value: Any) -> bool:
    phone = normalize_phone(value)
    if not phone:
        return False
    if len(set(phone)) <= 2:
        return True
    if "999999" in phone or "000000" in phone or "123456" in phone:
        return True
    if phone in {"1804330972", "18043309723", "11999999999", "00999999977", "1155555555"}:
        return True
    return False


def org_is_polluted(name: str) -> bool:
    upper = normalized_text(name).upper()
    return any(token in upper for token in POLLUTED_ORG_TOKENS)


def address_is_suspicious(value: Any) -> bool:
    text = normalized_text(value)
    upper = text.upper()
    return (
        "MOSSORO" in upper
        or "FAT: N/A" in upper
        or "VERIFICADO VIA CASA DOS DADOS" in upper
        or "TEL: N/A" in upper
        or "EMAIL: N/A" in upper
    )


def main() -> int:
    people_rows = parse_xlsx(PEOPLE_XLSX)
    org_rows = parse_xlsx(ORGS_XLSX)

    people_generic_phone_rows: List[Dict[str, Any]] = []
    people_suspicious_email_rows: List[Dict[str, Any]] = []
    people_polluted_org_rows: List[Dict[str, Any]] = []
    people_by_org: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for row in people_rows:
        person_name = normalized_text(row.get("Pessoa - Nome"))
        org_name = normalized_text(row.get("Pessoa - Organização"))
        emails = [
            row.get("Pessoa - E-mail - Trabalho"),
            row.get("Pessoa - E-mail - Residencial"),
            row.get("Pessoa - E-mail - Outros"),
        ]
        phones = [
            row.get("Pessoa - Telefone - Trabalho"),
            row.get("Pessoa - Telefone - Residencial"),
            row.get("Pessoa - Telefone - Celular"),
            row.get("Pessoa - Telefone - Outros"),
        ]
        people_by_org[org_name].append(row)

        if any(phone_is_generic(value) for value in phones):
            people_generic_phone_rows.append(
                {
                    "person_name": person_name,
                    "org_name": org_name,
                    "telefone_trabalho": row.get("Pessoa - Telefone - Trabalho"),
                    "telefone_residencial": row.get("Pessoa - Telefone - Residencial"),
                    "telefone_celular": row.get("Pessoa - Telefone - Celular"),
                    "telefone_outros": row.get("Pessoa - Telefone - Outros"),
                }
            )
        if any(email_is_suspicious(value) for value in emails):
            people_suspicious_email_rows.append(
                {
                    "person_name": person_name,
                    "org_name": org_name,
                    "email_trabalho": row.get("Pessoa - E-mail - Trabalho"),
                    "email_residencial": row.get("Pessoa - E-mail - Residencial"),
                    "email_outros": row.get("Pessoa - E-mail - Outros"),
                }
            )
        if org_is_polluted(org_name):
            people_polluted_org_rows.append(
                {
                    "person_name": person_name,
                    "org_name": org_name,
                    "open_deals": row.get("Pessoa - Negócios em aberto"),
                    "closed_deals": row.get("Pessoa - Negócios fechados"),
                    "phone": row.get("Pessoa - Telefone - Trabalho") or row.get("Pessoa - Telefone - Celular"),
                    "email": row.get("Pessoa - E-mail - Trabalho") or row.get("Pessoa - E-mail - Outros"),
                }
            )

    org_suspicious_address_rows: List[Dict[str, Any]] = []
    polluted_org_summary: List[Dict[str, Any]] = []
    for row in org_rows:
        org_name = normalized_text(row.get("Organização - Nome"))
        address = normalized_text(row.get("Organização - Endereço"))
        if address_is_suspicious(address):
            org_suspicious_address_rows.append(
                {
                    "org_name": org_name,
                    "address": address,
                    "people_count": row.get("Organização - Pessoas"),
                    "open_deals": row.get("Organização - Negócios em aberto"),
                    "closed_deals": row.get("Organização - Negócios fechados"),
                }
            )
        if org_is_polluted(org_name):
            polluted_org_summary.append(
                {
                    "org_name": org_name,
                    "people_count_export": row.get("Organização - Pessoas"),
                    "people_count_people_export": len(people_by_org.get(org_name, [])),
                    "open_deals": row.get("Organização - Negócios em aberto"),
                    "closed_deals": row.get("Organização - Negócios fechados"),
                    "address": address,
                }
            )

    top_orgs_by_people = [
        {"org_name": org_name, "people_count": count}
        for org_name, count in Counter({k: len(v) for k, v in people_by_org.items()}).items()
    ]
    top_orgs_by_people.sort(key=lambda item: (-int(item["people_count"]), item["org_name"]))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    save_csv(OUT_DIR / "people_generic_phones.csv", people_generic_phone_rows)
    save_csv(OUT_DIR / "people_suspicious_emails.csv", people_suspicious_email_rows)
    save_csv(OUT_DIR / "people_polluted_orgs.csv", people_polluted_org_rows)
    save_csv(OUT_DIR / "org_suspicious_addresses.csv", org_suspicious_address_rows)
    save_csv(OUT_DIR / "polluted_org_summary.csv", polluted_org_summary)
    save_csv(OUT_DIR / "top_orgs_by_people.csv", top_orgs_by_people[:200])

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "people_rows": len(people_rows),
        "org_rows": len(org_rows),
        "people_generic_phone_rows": len(people_generic_phone_rows),
        "people_suspicious_email_rows": len(people_suspicious_email_rows),
        "people_polluted_org_rows": len(people_polluted_org_rows),
        "org_suspicious_address_rows": len(org_suspicious_address_rows),
        "polluted_org_summary_rows": len(polluted_org_summary),
        "top_polluted_orgs": polluted_org_summary[:20],
    }
    (OUT_DIR / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    log("[PEOPLE_ORG_AUDIT] " + " ".join(f"{k}={v}" for k, v in summary.items() if not isinstance(v, list)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
