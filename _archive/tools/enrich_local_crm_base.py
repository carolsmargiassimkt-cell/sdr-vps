from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.local_crm_cache import LocalCRMCache
from services.casa_dos_dados_client import CasaDosDadosClient, CasaDosDadosError

DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "local_crm_base.json"
CHECKPOINT_EVERY = 10
REQUEST_DELAY_SEC = 0.8


def merge_lists(*values: List[str], normalizer=None, keep=None) -> List[str]:
    output: List[str] = []
    seen = set()
    for items in values:
        for item in items or []:
            clean = str(item or "").strip()
            if normalizer is not None:
                clean = normalizer(clean)
            if keep is not None and not keep(clean):
                continue
            if not clean or clean in seen:
                continue
            seen.add(clean)
            output.append(clean)
    return output


def should_enrich(record: Dict[str, Any], *, force: bool) -> bool:
    if force:
        return bool(LocalCRMCache.normalize_cnpj(record.get("cnpj")))
    if not LocalCRMCache.normalize_cnpj(record.get("cnpj")):
        return False
    if not (record.get("phones") or record.get("emails") or record.get("decision_makers") or record.get("websites")):
        return True
    return not str(record.get("enriched_source") or "").strip()


def enrich_record(record: Dict[str, Any], client: CasaDosDadosClient) -> Dict[str, Any]:
    cnpj = LocalCRMCache.normalize_cnpj(record.get("cnpj"))
    result = client.fetch_by_cnpj(cnpj)
    merged = dict(record)
    patch = result.as_record_patch()
    merged["empresa"] = merged.get("empresa") or patch.get("empresa") or result.company_name
    merged["nome"] = merged.get("nome") or patch.get("nome") or result.trade_name or result.company_name
    merged["telefone"] = merged.get("telefone") or patch.get("telefone") or ""
    merged["email"] = merged.get("email") or patch.get("email") or ""
    merged["phones"] = merge_lists(
        merged.get("phones") or [],
        patch.get("phones") or [],
        normalizer=LocalCRMCache.normalize_phone,
        keep=lambda value: bool(value),
    )
    merged["emails"] = merge_lists(
        merged.get("emails") or [],
        patch.get("emails") or [],
        normalizer=LocalCRMCache.normalize_email,
        keep=lambda value: bool(value) and "@" in value and not value.startswith("{"),
    )
    merged["decision_makers"] = merge_lists(merged.get("decision_makers") or [], patch.get("decision_makers") or [])
    merged["websites"] = merge_lists(
        merged.get("websites") or [],
        patch.get("websites") or [],
        normalizer=lambda value: value.strip().upper(),
        keep=lambda value: bool(value) and not value.startswith("{"),
    )
    merged["aliases"] = merge_lists(merged.get("aliases") or [], [result.trade_name, result.company_name])
    merged["enriched_source"] = patch.get("enriched_source") or "casa_dos_dados"
    merged["enriched_at"] = datetime.now(timezone.utc).isoformat()
    merged["raw_enrichment"] = patch.get("raw_enrichment") or {}
    if not merged.get("telefone") and merged["phones"]:
        merged["telefone"] = merged["phones"][0]
    if not merged.get("email") and merged["emails"]:
        merged["email"] = merged["emails"][0]
    return merged


def main(argv: List[str] | None = None) -> int:
    argv = list(argv or sys.argv[1:])
    force = "--force" in argv
    limit = 0
    if "--limit" in argv:
        idx = argv.index("--limit")
        if idx + 1 < len(argv):
            limit = max(0, int(argv[idx + 1]))

    cache = LocalCRMCache(DEFAULT_OUTPUT)
    payload = cache.load()
    records = [dict(item) for item in payload.get("records", []) if isinstance(item, dict)]
    client = CasaDosDadosClient()

    total = 0
    enriched = 0
    failed = 0
    for idx, record in enumerate(records):
        if limit and total >= limit:
            break
        if not should_enrich(record, force=force):
            continue
        total += 1
        try:
            records[idx] = enrich_record(record, client)
            enriched += 1
            print(f"[CASA_DOS_DADOS_OK] cnpj={records[idx].get('cnpj')} empresa={records[idx].get('empresa')}")
        except CasaDosDadosError as exc:
            failed += 1
            print(f"[CASA_DOS_DADOS_FAIL] cnpj={record.get('cnpj')} erro={exc}")
        if (enriched + failed) % CHECKPOINT_EVERY == 0:
            payload["records"] = records
            payload.setdefault("meta", {})
            payload["meta"]["updated_at"] = datetime.now(timezone.utc).isoformat()
            cache.save(payload)
        time.sleep(REQUEST_DELAY_SEC)

    payload["records"] = records
    payload.setdefault("meta", {})
    payload["meta"]["updated_at"] = datetime.now(timezone.utc).isoformat()
    payload["meta"]["casa_dos_dados_enriched_at"] = datetime.now(timezone.utc).isoformat()
    cache.save(payload)
    print(f"[CASA_DOS_DADOS_RESUMO] tentativas={total} enriquecidos={enriched} falhas={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
