from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List

from utils.safe_json import get_json_store

from services.lead_quality_filter import LeadQualityFilter


class Bot2Pipeline:
    MAX_COMPANIES_PER_CYCLE = 500
    MAX_WORKERS = 5
    PAGE_LIMIT = 100
    MIN_QUEUE_DELAY_SEC = 2.0
    MAX_QUEUE_DELAY_SEC = 5.0

    def __init__(
        self,
        *,
        project_root: Path,
        casa_client,
        pipedrive_client,
        logger,
        max_items: int = 20,
        pipeline_filter: str = "",
    ) -> None:
        self.project_root = Path(project_root)
        self.casa_client = casa_client
        self.pipedrive_client = pipedrive_client
        self.logger = logger
        self.max_items = min(self.MAX_COMPANIES_PER_CYCLE, max(1, int(max_items or self.MAX_COMPANIES_PER_CYCLE)))
        self.pipeline_filter = str(pipeline_filter or "").strip().lower()
        self.runtime_dir = self.project_root / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.processed_cache_file = self.runtime_dir / "bot2_processed_deals.json"
        self._processed_store = get_json_store(self.processed_cache_file, default_factory=dict)
        self._cache_lock = Lock()
        self._run_lock = Lock()
        self._processed_cache = self._load_processed_cache()
        self._inflight_deals: set[int] = set()
        self.quality = LeadQualityFilter(
            logger=logger,
            casa_client=casa_client,
            pipedrive_client=pipedrive_client,
        )

    def run(self) -> Dict[str, int]:
        metrics = {"enriched": 0, "skipped": 0, "errors": 0}
        candidates = self._collect_candidates()
        if not candidates:
            return metrics

        max_workers = min(self.MAX_WORKERS, max(1, len(candidates)))
        for offset in range(0, len(candidates), max_workers):
            batch = candidates[offset : offset + max_workers]
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self._process_deal, deal) for deal in batch]
                for future in as_completed(futures):
                    result = future.result()
                    status = str(result.get("status", "errors")).strip().lower()
                    if status == "enriched":
                        metrics["enriched"] += 1
                    elif status == "skipped":
                        metrics["skipped"] += 1
                    else:
                        metrics["errors"] += 1
            if offset + max_workers < len(candidates):
                time.sleep(self._batch_delay_sec(remaining=len(candidates) - (offset + max_workers)))
        return metrics

    def _collect_candidates(self) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        seen_ids = set()
        for deal in self.pipedrive_client.get_deals(limit=self.PAGE_LIMIT):
            if not isinstance(deal, dict):
                continue
            deal_id = int(deal.get("id", 0) or 0)
            if not deal_id or deal_id in seen_ids or self._is_already_processed(deal_id):
                continue
            if self.pipeline_filter:
                pipeline_id = str(deal.get("pipeline_id", "")).strip().lower()
                pipeline_name = str(deal.get("pipeline_name", "")).strip().lower()
                if self.pipeline_filter not in {pipeline_id, pipeline_name} and self.pipeline_filter not in pipeline_name:
                    continue
            seen_ids.add(deal_id)
            candidates.append(deal)
            if len(candidates) >= self.max_items:
                break
        return candidates

    def _process_deal(self, deal: Dict[str, Any]) -> Dict[str, Any]:
        deal_id = int(deal.get("id", 0) or 0)
        if not deal_id or not self._claim_deal(deal_id):
            return {"status": "skipped", "deal_id": deal_id}

        org_id = self.pipedrive_client._extract_id(deal.get("org_id"))
        person_id = self.pipedrive_client._extract_id(deal.get("person_id"))
        cnpj = ""
        try:
            if not org_id:
                return {"status": "skipped", "deal_id": deal_id}

            organization = self.pipedrive_client.get_organization(org_id)
            person = self.pipedrive_client.get_person(person_id) if person_id else {}
            if self._has_enrichment_data(organization, person):
                self._mark_processed(deal_id, "already_enriched")
                return {"status": "skipped", "deal_id": deal_id}

            cnpj = self.pipedrive_client.extract_cnpj(organization)
            if not cnpj:
                return {"status": "skipped", "deal_id": deal_id}

            company = self.quality.enrich_from_casa_dados(cnpj)
            if not company:
                return {"status": "errors", "deal_id": deal_id}

            quality = self.quality.apply(company, verify_whatsapp=True)
            lead = quality.get("lead", {}) or {}
            lead.setdefault("fonte", "casa_dos_dados")
            tags = list(quality.get("tags") or [])
            phone_state = quality.get("phone", {}) or {}

            if not phone_state.get("valid"):
                self.logger.info(f"telefone invalido | bot2 | cnpj={cnpj}")
                return {"status": "skipped", "deal_id": deal_id}

            if phone_state.get("is_landline"):
                self.logger.info(f"telefone fixo descartado | bot2 | cnpj={cnpj}")
                return {"status": "skipped", "deal_id": deal_id}

            org_update = {
                "address": lead.get("endereco", ""),
                **self.pipedrive_client.build_org_fields(lead.get("cnpj", ""), lead.get("cnae", "")),
                **self.pipedrive_client.build_org_enrichment_fields(lead),
            }
            self.pipedrive_client.update_organization(org_id, org_update)

            if person_id:
                self.pipedrive_client.update_person(
                    person_id,
                    {
                        "name": lead.get("decisor") or lead.get("empresa", ""),
                        "phone": lead.get("telefone", ""),
                        "email": lead.get("email", ""),
                        **self.pipedrive_client.build_person_enrichment_fields(lead),
                    },
                )
            self.pipedrive_client.update_deal(
                deal_id,
                {
                    "status_bot": "bot2_enriched",
                },
            )

            for tag in ["bot2_enriched", "deal_enriquecido_por_casa_dos_dados", *tags]:
                self.pipedrive_client.add_tag("deal", deal_id, tag)
            self.pipedrive_client.add_note(
                content=self.pipedrive_client.build_company_note(lead, mode="enrich"),
                person_id=person_id,
                deal_id=deal_id,
            )
            self._mark_processed(deal_id, "enriched")
            self.logger.info(f"lead enriquecido | bot2 | deal_id={deal_id} | cnpj={cnpj}")
            return {"status": "enriched", "deal_id": deal_id}
        except Exception as exc:
            self.logger.error(f"pipeline bot2 erro | deal_id={deal_id} | cnpj={cnpj} | {exc}")
            return {"status": "errors", "deal_id": deal_id}
        finally:
            with self._run_lock:
                self._inflight_deals.discard(deal_id)

    def _claim_deal(self, deal_id: int) -> bool:
        with self._run_lock:
            if deal_id in self._inflight_deals or self._is_already_processed(deal_id):
                return False
            self._inflight_deals.add(deal_id)
            return True

    def _is_already_processed(self, deal_id: int) -> bool:
        if not deal_id:
            return False
        with self._cache_lock:
            return str(deal_id) in self._processed_cache

    def _mark_processed(self, deal_id: int, result: str) -> None:
        if not deal_id:
            return
        with self._cache_lock:
            self._processed_cache[str(deal_id)] = {"result": str(result or "").strip(), "updated_at": time.time()}
            self._processed_store.replace(self._processed_cache, flush=True)

    def _load_processed_cache(self) -> Dict[str, Dict[str, Any]]:
        payload = self._processed_store.read() if self.processed_cache_file.exists() else {}
        return payload if isinstance(payload, dict) else {}

    def _batch_delay_sec(self, *, remaining: int) -> float:
        backlog = max(0, int(remaining or 0))
        workers = min(self.MAX_WORKERS, max(1, backlog))
        delay = self.MIN_QUEUE_DELAY_SEC + (workers - 1) * 0.5
        return max(self.MIN_QUEUE_DELAY_SEC, min(self.MAX_QUEUE_DELAY_SEC, delay))

    @staticmethod
    def _has_enrichment_data(organization: Dict, person: Dict) -> bool:
        org_phone = str(organization.get("phone") or "").strip()
        person_phone = str(person.get("phone") or "").strip()
        org_email = str(organization.get("email") or "").strip()
        person_email = str(person.get("email") or "").strip()
        return bool((org_phone or person_phone) and (org_email or person_email))
