from __future__ import annotations

import csv
import json
import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set
from urllib.parse import parse_qs, unquote, urlparse

import requests

from enrich_missing_cnpj_from_xlsx import BLOCKED_GENERIC_PHONES, best_phone, normalize_cnpj, normalize_name, normalize_phone
from offline_final_sheet_enrichment import dynamic_write_xlsx


INPUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_casa_focus_enriched.csv")
OUTPUT_CSV = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_public_phone_hunt.csv")
OUTPUT_XLSX = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\deals_public_phone_hunt.xlsx")
SUMMARY_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\logs\deals_public_phone_hunt_summary.json")
CACHE_JSON = Path(r"C:\Users\Asus\legacy\bot_sdr_ai\data\public_phone_hunt_cache.json")

TIMEOUT_SEC = 15
DELAY_SEC = 0.7
PHONE_RE = re.compile(r"(?<!\d)(?:\+?55\s*)?(?:\(?\d{2}\)?\s*)?\d{4,5}[-\s]?\d{4}(?!\d)")


def read_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    headers: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in headers:
                headers.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def read_cache() -> Dict[str, Any]:
    if not CACHE_JSON.exists():
        return {}
    try:
        return json.loads(CACHE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache: Dict[str, Any]) -> None:
    CACHE_JSON.parent.mkdir(parents=True, exist_ok=True)
    CACHE_JSON.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def session_get(url: str, **kwargs: Any) -> requests.Response:
    headers = kwargs.pop("headers", {})
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    }
    default_headers.update(headers)
    return requests.get(url, headers=default_headers, timeout=TIMEOUT_SEC, **kwargs)


def ddg_html(query: str, cache: Dict[str, Any]) -> str:
    key = f"ddg::{query}"
    if isinstance(cache.get(key), str):
        return str(cache[key])
    html = ""
    try:
        resp = session_get("https://html.duckduckgo.com/html/", params={"q": query})
        html = resp.text if resp.status_code == 200 else ""
    except Exception as exc:
        cache[f"erro::{key}"] = str(exc)[:200]
    cache[key] = html
    return html


def google_html(query: str, cache: Dict[str, Any]) -> str:
    key = f"google::{query}"
    if isinstance(cache.get(key), str):
        return str(cache[key])
    html = ""
    try:
        resp = session_get("https://www.google.com/search", params={"q": query, "hl": "pt-BR", "num": "5"})
        html = resp.text if resp.status_code == 200 else ""
    except Exception as exc:
        cache[f"erro::{key}"] = str(exc)[:200]
    cache[key] = html
    return html


def extract_urls(html: str) -> List[str]:
    urls: List[str] = []
    seen: Set[str] = set()
    for raw in re.findall(r'href="([^"]+)"', html or ""):
        candidate = raw
        if raw.startswith("/l/?"):
            parsed = urlparse(raw)
            candidate = parse_qs(parsed.query).get("uddg", [""])[0]
        elif raw.startswith("/url?"):
            parsed = urlparse(raw)
            candidate = parse_qs(parsed.query).get("q", [""])[0]
        candidate = unquote(candidate)
        if not candidate.startswith("http"):
            continue
        host = urlparse(candidate).netloc.lower()
        if any(bad in host for bad in ("duckduckgo.com", "google.com")):
            continue
        if candidate not in seen:
            seen.add(candidate)
            urls.append(candidate)
    return urls


def extract_phones(text: str) -> List[str]:
    found: List[str] = []
    seen: Set[str] = set()
    for match in PHONE_RE.findall(text or ""):
        phone = normalize_phone(match)
        if phone and phone not in BLOCKED_GENERIC_PHONES and phone not in seen:
            seen.add(phone)
            found.append(phone)
    return found


def reject_phone(phone: str, cnpj: str = "") -> bool:
    normalized = normalize_phone(phone)
    if not normalized:
        return True
    if normalized in BLOCKED_GENERIC_PHONES:
        return True
    if len(set(normalized)) <= 2:
        return True
    if normalized[2:] in {"99999999", "999999999", "00000000", "000000000", "12345678", "123456789"}:
        return True
    if cnpj and normalized in cnpj:
        return True
    if cnpj and len(normalized) >= 8 and any(normalized == cnpj[i : i + len(normalized)] for i in range(0, len(cnpj) - len(normalized) + 1)):
        return True
    return False


def fetch_page(url: str, cache: Dict[str, Any]) -> str:
    key = f"page::{url}"
    if isinstance(cache.get(key), str):
        return str(cache[key])
    text = ""
    try:
        resp = session_get(url)
        text = resp.text if resp.status_code == 200 else ""
    except Exception as exc:
        cache[f"erro::{key}"] = str(exc)[:200]
    cache[key] = text[:300000]
    return str(cache[key])


def email_domain_candidates(row: Dict[str, Any]) -> List[str]:
    domains: List[str] = []
    for raw in str(row.get("casa_emails") or row.get("email") or "").split("|"):
        email = str(raw or "").strip().lower()
        if "@" not in email:
            continue
        domain = email.split("@", 1)[1].strip().lower()
        if domain in {"gmail.com", "hotmail.com", "outlook.com", "yahoo.com.br", "yahoo.com"}:
            continue
        if domain and domain not in domains:
            domains.append(domain)
    return domains


def company_queries(row: Dict[str, Any]) -> List[str]:
    names = []
    for key in ("casa_razao_social", "casa_nome_fantasia", "nome_final"):
        value = str(row.get(key) or "").strip()
        if value and value not in names:
            names.append(value)
    cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
    queries: List[str] = []
    for name in names[:2]:
        queries.append(f'"{name}" telefone')
        queries.append(f'"{name}" contato')
        queries.append(f'"{name}" site:.br telefone')
        queries.append(f'"{name}" whatsapp')
        queries.append(f'site:google.com/maps "{name}" telefone')
    if cnpj:
        queries.append(f'"{cnpj}" telefone')
        queries.append(f'"{cnpj}" contato')
    deduped: List[str] = []
    for item in queries:
        if item not in deduped:
            deduped.append(item)
    return deduped[:6]


def hunt_row(row: Dict[str, Any], cache: Dict[str, Any]) -> Dict[str, Any]:
    current = dict(row)
    cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
    candidates: List[str] = []
    sources: List[str] = []
    for domain in email_domain_candidates(row):
        for url in [f"https://{domain}", f"http://{domain}", f"https://www.{domain}", f"https://{domain}/contato", f"https://{domain}/contact"]:
            page = fetch_page(url, cache)
            if not page:
                continue
            for phone in extract_phones(page):
                if not reject_phone(phone, cnpj) and phone not in candidates:
                    candidates.append(phone)
                    sources.append(f"domain:{url}")
            time.sleep(0.2)
    for query in company_queries(row):
        for engine_name, engine in (("ddg", ddg_html), ("google", google_html)):
            html = engine(query, cache)
            for phone in extract_phones(html):
                if not reject_phone(phone, cnpj) and phone not in candidates:
                    candidates.append(phone)
                    sources.append(f"{engine_name}:snippet:{query}")
            urls = extract_urls(html)[:4]
            for url in urls:
                page = fetch_page(url, cache)
                for phone in extract_phones(page):
                    if not reject_phone(phone, cnpj) and phone not in candidates:
                        candidates.append(phone)
                        sources.append(f"{engine_name}:page:{url}")
                time.sleep(0.2)
            time.sleep(DELAY_SEC)
    current["public_phone_candidates"] = " | ".join(candidates)
    current["public_phone_sources"] = " | ".join(sources[:20])
    return current


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=str(INPUT_CSV))
    parser.add_argument("--output-csv", default=str(OUTPUT_CSV))
    parser.add_argument("--output-xlsx", default=str(OUTPUT_XLSX))
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    cache = read_cache()
    rows = read_csv(Path(args.input))
    target_rows = [
        dict(row)
        for row in rows
        if normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
        and (row.get("acao") or "").upper() != "ARCHIVE"
        and not normalize_phone(row.get("telefone_principal_sugerido") or row.get("telefone"))
    ]
    if args.limit:
        target_rows = target_rows[: max(1, int(args.limit))]

    output: List[Dict[str, Any]] = []
    for index, row in enumerate(target_rows, start=1):
        output.append(hunt_row(row, cache))
        save_cache(cache)
        if index % 10 == 0:
            print(f"[PUBLIC_PHONE_HUNT_PROGRESS] {index}/{len(target_rows)}", flush=True)

    phone_to_companies: Dict[str, Set[str]] = defaultdict(set)
    for row in output:
        cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
        split = [normalize_phone(item) for item in str(row.get("public_phone_candidates") or "").split("|")]
        for phone in split:
            if phone and not reject_phone(phone, cnpj):
                phone_to_companies[phone].add(cnpj)

    unique_phone_owner = {phone: next(iter(cnpjs)) for phone, cnpjs in phone_to_companies.items() if len(cnpjs) == 1}
    found_rows = 0
    for row in output:
        cnpj = normalize_cnpj(row.get("cnpj_enriquecido") or row.get("cnpj"))
        unique_candidates = [
            phone
            for phone in [normalize_phone(item) for item in str(row.get("public_phone_candidates") or "").split("|")]
            if phone and not reject_phone(phone, cnpj) and unique_phone_owner.get(phone) == cnpj
        ]
        row["public_phone_unique"] = best_phone(unique_candidates)
        row["public_phone_unique_count"] = str(len(unique_candidates))
        if row["public_phone_unique"]:
            found_rows += 1

    dynamic_write_xlsx(output, Path(args.output_xlsx))
    write_csv(output, Path(args.output_csv))
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_target_rows": len(target_rows),
        "rows_with_unique_public_phone": found_rows,
        "unique_public_phones": len(unique_phone_owner),
    }
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
