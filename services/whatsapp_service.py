
# === SAFE GLOBAL DELAY PATCH ===

# === END PATCH ===

import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

import requests

from core.automation_freeze import is_automation_freeze_active


class WhatsAppService:
    SEND_TIMEOUT_SEC = 40
    PENDING_TTL_SECONDS = 30 * 60
    TEST_WHITELIST = {"5535920002020", "35920002020", "5511998804191", "11998804191"}
    CONFIRMED_OUTBOUND_SOURCES = {"sync", "outbound_sync", "webhook_sync", "manual_sync"}

    def __init__(self, *args, **kwargs):
        self.base_dir = Path(__file__).resolve().parents[1]
        self.data_dir = self.base_dir / "data"
        self.logs_dir = self.base_dir / "logs"
        self.outbound_mode = str(os.getenv("WHATSAPP_OUTBOUND_MODE", "auto")).strip().lower()
        self.base_urls = {
            "WA1": "http://127.0.0.1:3000",
        }
        self.sent_file = str(self.base_dir / "sent.json")
        self.sent_lock_file = str(self.base_dir / "sent.json.lock")
        self.invalid_file = str(self.base_dir / "invalidos.json")
        self.history_file = str(self.logs_dir / "whatsapp_message_history.json")
        self.manual_blocklist_file = str(self.logs_dir / "whatsapp_manual_blocklist.json")
        self.channel_map_file = str(self.data_dir / "whatsapp_channel_map.json")
        self.validation_cache_file = str(self.data_dir / "whatsapp_validation_cache.json")
        self.after_hours_state_file = str(self.data_dir / "after_hours_state.json")
        self.after_hours_lock_file = str(self.data_dir / "after_hours_state.lock")
        self.last_send_state = ""

    def is_outbound_manual_mode(self):
        return self.outbound_mode not in {"automatico", "automatic", "auto"}

    def normalize_phone(self, phone):
        digits = re.sub(r"\D+", "", str(phone or ""))
        if digits.startswith("55") and len(digits) > 11:
            digits = digits[2:]
        return digits

    def is_test_whitelist_phone(self, phone):
        normalized = self.normalize_phone(phone)
        if not normalized:
            return False
        variants = self.phone_variants(normalized)
        return bool(variants & set(self.TEST_WHITELIST))

    def phone_variants(self, phone):
        num = self.normalize_phone(phone)
        variants = {num}
        if len(num) == 11 and num[2] == "9":
            variants.add(num[:2] + num[3:])
        if len(num) == 10:
            variants.add(num[:2] + "9" + num[2:])
        return {item for item in variants if item}

    def preferred_phone_variants(self, phone):
        num = self.normalize_phone(phone)
        variants = []
        if len(num) == 10:
            variants.append(num[:2] + "9" + num[2:])
        if num:
            variants.append(num)
        seen = set()
        ordered = []
        for item in variants:
            if item and item not in seen:
                seen.add(item)
                ordered.append(item)
        return ordered

    def is_valid_phone(self, phone):
        num = self.normalize_phone(phone)
        if len(num) not in {10, 11}:
            return False
        if len(set(num)) == 1:
            return False
        if num[:2] == "00":
            return False
        return True

    def _acquire_lock(self, timeout=10, lock_file=None):
        target_lock_file = str(lock_file or self.sent_lock_file)
        started_at = time.time()
        while time.time() - started_at < timeout:
            try:
                return os.open(target_lock_file, os.O_CREAT | os.O_EXCL | os.O_RDWR)
            except FileExistsError:
                time.sleep(0.05)
        raise TimeoutError(target_lock_file)

    def _release_lock(self, fd, lock_file=None):
        target_lock_file = str(lock_file or self.sent_lock_file)
        try:
            os.close(fd)
        except Exception:
            pass
        try:
            os.remove(target_lock_file)
        except Exception:
            pass

    def _load_json(self, path, fallback):
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    if isinstance(loaded, type(fallback)):
                        return loaded
            except Exception:
                pass
        return fallback

    def _save_json(self, path, payload):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2 if isinstance(payload, (list, dict)) else None)

    def _manual_blocklist_numbers(self):
        payload = self._load_json(self.manual_blocklist_file, [])
        blocked = set()
        if not isinstance(payload, list):
            return blocked
        for item in payload:
            if not isinstance(item, dict):
                continue
            for candidate in (item.get("telefone"), item.get("phone"), item.get("number")):
                normalized = self.normalize_phone(candidate)
                if not normalized:
                    continue
                blocked.update(self.phone_variants(normalized))
        return blocked

    def is_phone_in_manual_blocklist(self, phone):
        normalized = self.normalize_phone(phone)
        if not normalized or self.is_test_whitelist_phone(normalized):
            return False
        variants = self.phone_variants(normalized)
        return bool(variants & self._manual_blocklist_numbers())

    def _load_channel_map(self):
        payload = self._load_json(self.channel_map_file, {"phones": {}, "deals": {}})
        if not isinstance(payload, dict):
            payload = {"phones": {}, "deals": {}}
        payload.setdefault("phones", {})
        payload.setdefault("deals", {})
        return payload

    def _save_channel_map(self, payload):
        self._save_json(self.channel_map_file, payload)

    def _load_validation_cache(self):
        payload = self._load_json(self.validation_cache_file, {})
        return payload if isinstance(payload, dict) else {}

    def _save_validation_cache(self, payload):
        self._save_json(self.validation_cache_file, payload if isinstance(payload, dict) else {})

    def _get_cached_validation(self, phone, max_age_seconds=24 * 60 * 60):
        normalized = self.normalize_phone(phone)
        if not normalized:
            return None
        cache = self._load_validation_cache()
        entry = cache.get(normalized)
        if not isinstance(entry, dict):
            return None
        checked_at = str(entry.get("checked_at") or "").strip()
        if not checked_at:
            return None
        try:
            age_seconds = (datetime.now() - datetime.fromisoformat(checked_at)).total_seconds()
        except Exception:
            return None
        if age_seconds > max(1, int(max_age_seconds or 0)):
            return None
        return bool(entry.get("exists"))

    def _cache_validation(self, phone, exists):
        normalized = self.normalize_phone(phone)
        if not normalized:
            return
        cache = self._load_validation_cache()
        cache[normalized] = {
            "exists": bool(exists),
            "checked_at": datetime.now().isoformat(),
        }
        self._save_validation_cache(cache)

    def _load_after_hours_state_unlocked(self):
        payload = self._load_json(
            self.after_hours_state_file,
            {"notices": {}, "pending": {}, "last_resume_date": ""},
        )
        if not isinstance(payload, dict):
            payload = {"notices": {}, "pending": {}, "last_resume_date": ""}
        payload.setdefault("notices", {})
        payload.setdefault("pending", {})
        payload.setdefault("last_resume_date", "")
        if not isinstance(payload.get("notices"), dict):
            payload["notices"] = {}
        if not isinstance(payload.get("pending"), dict):
            payload["pending"] = {}
        return payload

    def _with_after_hours_state(self, mutator):
        fd = None
        try:
            fd = self._acquire_lock(lock_file=self.after_hours_lock_file)
            payload = self._load_after_hours_state_unlocked()
            result = mutator(payload)
            self._save_json(self.after_hours_state_file, payload)
            return result
        finally:
            if fd is not None:
                self._release_lock(fd, lock_file=self.after_hours_lock_file)

    def was_after_hours_notice_sent_today(self, phone, day_str=None):
        normalized = self.normalize_phone(phone)
        target_day = str(day_str or datetime.now().strftime("%Y-%m-%d"))
        if not normalized:
            return False

        def _reader(payload):
            return bool(((payload.get("notices") or {}).get(target_day) or {}).get(normalized))

        return bool(self._with_after_hours_state(_reader))

    def mark_after_hours_notice_sent(self, phone, day_str=None):
        normalized = self.normalize_phone(phone)
        target_day = str(day_str or datetime.now().strftime("%Y-%m-%d"))
        if not normalized:
            return False

        def _writer(payload):
            notices = payload.setdefault("notices", {})
            day_bucket = notices.setdefault(target_day, {})
            already = bool(day_bucket.get(normalized))
            day_bucket[normalized] = datetime.now().isoformat()
            return not already

        return bool(self._with_after_hours_state(_writer))

    def upsert_after_hours_pending(self, phone, *, message="", msg_id="", timestamp="", source=""):
        normalized = self.normalize_phone(phone)
        if not normalized:
            return False

        def _writer(payload):
            pending = payload.setdefault("pending", {})
            entry = dict(pending.get(normalized) or {})
            message_ids = list(entry.get("message_ids") or [])
            normalized_msg_id = str(msg_id or "").strip()
            if normalized_msg_id and normalized_msg_id not in message_ids:
                message_ids.append(normalized_msg_id)
            entry.update(
                {
                    "phone": normalized,
                    "message": str(message or "").strip(),
                    "last_message_at": str(timestamp or "").strip() or datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                    "source": str(source or "").strip() or "after_hours",
                    "status": "aguardando_horario",
                    "message_ids": message_ids[-50:],
                    "pending_count": max(int(entry.get("pending_count") or 0) + 1, len(message_ids), 1),
                }
            )
            pending[normalized] = entry
            return True

        return bool(self._with_after_hours_state(_writer))

    def list_after_hours_pending(self):
        def _reader(payload):
            items = []
            for phone, raw in (payload.get("pending") or {}).items():
                entry = dict(raw or {})
                entry["phone"] = self.normalize_phone(entry.get("phone") or phone)
                if entry["phone"]:
                    items.append(entry)
            items.sort(key=lambda item: str(item.get("updated_at") or ""))
            return items

        return list(self._with_after_hours_state(_reader) or [])

    def clear_after_hours_pending(self, phone):
        normalized = self.normalize_phone(phone)
        if not normalized:
            return False

        def _writer(payload):
            pending = payload.setdefault("pending", {})
            return pending.pop(normalized, None) is not None

        return bool(self._with_after_hours_state(_writer))

    def get_after_hours_resume_date(self):
        def _reader(payload):
            return str(payload.get("last_resume_date") or "").strip()

        return str(self._with_after_hours_state(_reader) or "").strip()

    def mark_after_hours_resumed_today(self, day_str=None):
        target_day = str(day_str or datetime.now().strftime("%Y-%m-%d"))

        def _writer(payload):
            payload["last_resume_date"] = target_day
            return True

        return bool(self._with_after_hours_state(_writer))

    def _status_for_base_url(self, base_url):
        try:
            r = requests.get(f"{base_url}/status", timeout=1)
            if r.status_code != 200:
                return {"connected": False, "mode": "offline", "needs_qr": False, "session_invalid": False}
            body = r.json() if r.content else {}
            if isinstance(body, dict):
                return {
                    "connected": bool(body.get("connected")),
                    "mode": str(body.get("mode") or "offline"),
                    "needs_qr": bool(body.get("needs_qr")),
                    "session_invalid": bool(body.get("session_invalid")),
                    "stable": bool(body.get("stable")),
                    **body,
                }
            return {"connected": False, "mode": "offline", "needs_qr": False, "session_invalid": False}
        except Exception:
            return {"connected": False, "mode": "offline", "needs_qr": False, "session_invalid": False}

    def _healthy_channels(self):
        healthy = []
        for channel_name, base_url in self.base_urls.items():
            status = self._status_for_base_url(base_url)
            if bool(status.get("connected")):
                healthy.append((channel_name, base_url, status))
        return healthy

    def heartbeat(self):
        statuses = {}
        for channel_name, base_url in self.base_urls.items():
            status = self._status_for_base_url(base_url)
            statuses[channel_name] = status
            if bool(status.get("connected")):
                logging.info(f"[WA_HEALTH_OK] canal={channel_name}")
        if any(bool(item.get("connected")) for item in statuses.values()):
            logging.info("[WA_ESTAVEL]")
        else:
            logging.warning("[WA_OFFLINE]")
        return statuses

    def _resolve_channel(self, phone=None, deal_id=None):
        payload = self._load_channel_map()
        normalized = self.normalize_phone(phone)
        if normalized:
            preferred = str((payload.get("phones") or {}).get(normalized) or "").strip()
            if preferred in self.base_urls:
                preferred_status = self._status_for_base_url(self.base_urls[preferred])
                if bool(preferred_status.get("connected")):
                    return preferred, self.base_urls[preferred]
        if int(deal_id or 0) > 0:
            preferred = str((payload.get("deals") or {}).get(str(int(deal_id))) or "").strip()
            if preferred in self.base_urls:
                preferred_status = self._status_for_base_url(self.base_urls[preferred])
                if bool(preferred_status.get("connected")):
                    return preferred, self.base_urls[preferred]
        healthy = self._healthy_channels()
        if not healthy:
            return "WA1", self.base_urls["WA1"]
        return healthy[0][0], healthy[0][1]

    def remember_channel(self, phone, deal_id=None, channel_name="WA1"):
        payload = self._load_channel_map()
        normalized = self.normalize_phone(phone)
        if normalized:
            payload.setdefault("phones", {})[normalized] = "WA1"
        if int(deal_id or 0) > 0:
            payload.setdefault("deals", {})[str(int(deal_id))] = "WA1"
        self._save_channel_map(payload)

    def has_deal_send_record(self, deal_id):
        normalized_id = int(deal_id or 0)
        if normalized_id <= 0:
            return False
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            all_deals = set(str(item) for item in data.get("DEAL_ALL", []))
            pending_deals = set(str(item) for item in (data.get("DEAL_PENDING") or {}).keys())
            return str(normalized_id) in all_deals or str(normalized_id) in pending_deals
        except Exception:
            return False
        finally:
            if fd is not None:
                self._release_lock(fd)

    def reserve_deal_send(self, deal_id):
        normalized_id = int(deal_id or 0)
        if normalized_id <= 0:
            return False
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            data.setdefault("DEAL_ALL", [])
            data.setdefault("DEAL_PENDING", {})
            if str(normalized_id) in set(str(item) for item in data.get("DEAL_ALL", [])):
                return False
            if str(normalized_id) in set(str(item) for item in (data.get("DEAL_PENDING") or {}).keys()):
                return False
            data["DEAL_PENDING"][str(normalized_id)] = datetime.now().isoformat()
            self._save_json(self.sent_file, data)
            return True
        except Exception:
            return False
        finally:
            if fd is not None:
                self._release_lock(fd)

    def release_deal_send(self, deal_id):
        normalized_id = int(deal_id or 0)
        if normalized_id <= 0:
            return
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            data.setdefault("DEAL_PENDING", {}).pop(str(normalized_id), None)
            self._save_json(self.sent_file, data)
        except Exception:
            pass
        finally:
            if fd is not None:
                self._release_lock(fd)

    def mark_deal_sent(self, deal_id):
        normalized_id = int(deal_id or 0)
        if normalized_id <= 0:
            return
        today = datetime.now().strftime("%Y-%m-%d")
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            data.setdefault("DEAL_PENDING", {}).pop(str(normalized_id), None)
            data.setdefault("DEAL_ALL", [])
            deal_today_key = f"DEAL_{today}"
            data.setdefault(deal_today_key, [])
            if str(normalized_id) not in [str(item) for item in data["DEAL_ALL"]]:
                data["DEAL_ALL"].append(str(normalized_id))
            if str(normalized_id) not in [str(item) for item in data[deal_today_key]]:
                data[deal_today_key].append(str(normalized_id))
            self._save_json(self.sent_file, data)
        except Exception:
            pass
        finally:
            if fd is not None:
                self._release_lock(fd)

    def _load_history(self):
        payload = self._load_json(self.history_file, {})
        return payload if isinstance(payload, dict) else {}

    def _append_history_entry(self, phone, direction, message, step=0, source=""):
        normalized = self.normalize_phone(phone)
        if not normalized:
            return
        history = self._load_history()
        items = history.get(normalized, [])
        if not isinstance(items, list):
            items = []
        items.append(
            {
                "direction": str(direction or "").strip().lower(),
                "message": str(message or "").strip(),
                "step": int(step or 0),
                "created_at": datetime.now().isoformat(),
                "source": str(source or "").strip().lower(),
            }
        )
        history[normalized] = items[-30:]
        self._save_json(self.history_file, history)

    def _is_duplicate_recent_text(self, phone, text, within_seconds=120):
        normalized = self.normalize_phone(phone)
        target = str(text or "").strip()
        if not normalized or not target:
            return False
        history = self._load_history().get(normalized, [])
        now = datetime.now()
        for item in reversed(history):
            if str(item.get("direction") or "").strip().lower() != "out":
                continue
            if str(item.get("message") or "").strip() != target:
                continue
            created_at = str(item.get("created_at") or "").strip()
            if not created_at:
                return True
            try:
                created = datetime.fromisoformat(created_at)
            except Exception:
                return True
            if (now - created).total_seconds() <= within_seconds:
                return True
            break
        return False

    @classmethod
    def _history_entry_is_confirmed(cls, item):
        source = str((item or {}).get("source") or "").strip().lower()
        if source in cls.CONFIRMED_OUTBOUND_SOURCES:
            return True
        return bool((item or {}).get("sync_confirmed"))

    def wait_for_outbound_sync(self, phone, text, timeout_seconds=12, poll_seconds=0.5):
        normalized = self.normalize_phone(phone)
        target = str(text or "").strip()
        if not normalized or not target:
            return False
        deadline = time.time() + max(float(timeout_seconds or 0), 0.5)
        while time.time() < deadline:
            history = self._load_history().get(normalized, [])
            now = datetime.now()
            for item in reversed(history):
                if str(item.get("direction") or "").strip().lower() != "out":
                    continue
                if str(item.get("message") or "").strip() != target:
                    continue
                if not self._history_entry_is_confirmed(item):
                    continue
                created_at = str(item.get("created_at") or "").strip()
                if not created_at:
                    return True
                try:
                    created = datetime.fromisoformat(created_at)
                except Exception:
                    return True
                if (now - created).total_seconds() <= max(float(timeout_seconds or 0), 30.0):
                    return True
            time.sleep(max(float(poll_seconds or 0), 0.1))
        return False

    def _legacy_numbers(self, data):
        results = set()
        if not isinstance(data, dict):
            return results
        for key, values in data.items():
            if key in {"ALL", "PENDING"} or not isinstance(values, list):
                continue
            for raw in values:
                token = str(raw or "")
                candidates = [token.split("_")[-1] if "_" in token else token]
                digit_groups = re.findall(r"\d{10,11}", token)
                if digit_groups:
                    candidates.extend(digit_groups)
                for phone in candidates:
                        normalized = self.normalize_phone(phone)
                        if normalized:
                            results.update(self.phone_variants(normalized))
        return results

    def _dated_sent_numbers(self, data):
        results = set()
        if not isinstance(data, dict):
            return results
        for key, values in data.items():
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(key or "").strip()):
                continue
            if not isinstance(values, list):
                continue
            for raw in values:
                normalized = self.normalize_phone(raw)
                if self.is_valid_phone(normalized):
                    results.add(normalized)
        return results

    def _history_outbound_numbers(self):
        results = set()
        history = self._load_history()
        if not isinstance(history, dict):
            return results
        for phone, items in history.items():
            normalized = self.normalize_phone(phone)
            if not self.is_valid_phone(normalized):
                continue
            if not isinstance(items, list):
                continue
            if any(str(item.get("direction") or "").strip().lower() == "out" for item in items if isinstance(item, dict)):
                results.add(normalized)
        return results

    @staticmethod
    def _parse_iso_datetime(raw):
        try:
            token = str(raw or "").strip()
            return datetime.fromisoformat(token) if token else None
        except Exception:
            return None

    def _clean_pending_phone_map(self, pending):
        cleaned = {}
        if not isinstance(pending, dict):
            return cleaned
        now = datetime.now()
        for raw_phone, stamp in list(pending.items()):
            normalized = self.normalize_phone(raw_phone)
            created_at = self._parse_iso_datetime(stamp)
            if not self.is_valid_phone(normalized) or created_at is None:
                continue
            if (now - created_at).total_seconds() > self.PENDING_TTL_SECONDS:
                continue
            cleaned[normalized] = stamp
        return cleaned

    def _clean_pending_deal_map(self, pending):
        cleaned = {}
        if not isinstance(pending, dict):
            return cleaned
        now = datetime.now()
        for raw_deal_id, stamp in list(pending.items()):
            token = str(raw_deal_id or "").strip()
            created_at = self._parse_iso_datetime(stamp)
            if not token.isdigit() or int(token) <= 0 or created_at is None:
                continue
            if (now - created_at).total_seconds() > self.PENDING_TTL_SECONDS:
                continue
            cleaned[token] = stamp
        return cleaned

    def _clean_daily_phone_buckets(self, data):
        if not isinstance(data, dict):
            return
        for key, values in list(data.items()):
            key_str = str(key or "").strip()
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", key_str):
                continue
            if not isinstance(values, list):
                data[key_str] = []
                continue
            unique_phones = []
            seen_phones = set()
            for raw in values:
                normalized = self.normalize_phone(raw)
                if not self.is_valid_phone(normalized) or normalized in seen_phones:
                    continue
                seen_phones.add(normalized)
                unique_phones.append(normalized)
            data[key_str] = unique_phones

    def _reconcile_daily_deal_phone_keys(self, data):
        if not isinstance(data, dict):
            return
        for key, values in list(data.items()):
            key_str = str(key or "").strip()
            match = re.fullmatch(r"DEAL_PHONE_(\d{4}-\d{2}-\d{2})", key_str)
            if not match:
                continue
            day_str = match.group(1)
            successful_deals = {
                str(item).strip()
                for item in list(data.get(f"DEAL_{day_str}") or [])
                if str(item).strip()
            }
            successful_phones = {
                self.normalize_phone(item)
                for item in list(data.get(day_str) or [])
            }
            successful_phones = {
                phone for phone in successful_phones if self.is_valid_phone(phone)
            }
            cleaned_keys = []
            seen_keys = set()
            if not isinstance(values, list):
                data[key_str] = cleaned_keys
                continue
            for raw in values:
                token = str(raw or "").strip()
                match_token = re.fullmatch(r"\d{4}-\d{2}-\d{2}:(\d+):(\d+)", token)
                if not match_token:
                    continue
                deal_id, phone = match_token.groups()
                normalized_phone = self.normalize_phone(phone)
                normalized_token = f"{day_str}:{deal_id}:{normalized_phone}"
                if (
                    deal_id not in successful_deals
                    or normalized_phone not in successful_phones
                    or normalized_token in seen_keys
                ):
                    continue
                seen_keys.add(normalized_token)
                cleaned_keys.append(normalized_token)
            data[key_str] = cleaned_keys

    def _load_sent_data_unlocked(self):
        data = self._load_json(self.sent_file, {"ALL": [], "PENDING": {}})
        if not isinstance(data, dict):
            data = {"ALL": [], "PENDING": {}}
        data.setdefault("ALL", [])
        data.setdefault("PENDING", {})
        data.setdefault("DEAL_ALL", [])
        data.setdefault("DEAL_PENDING", {})
        self._clean_daily_phone_buckets(data)
        normalized_all = []
        seen = set()
        trusted_numbers = set()
        trusted_numbers.update(self._dated_sent_numbers(data))
        trusted_numbers.update(self._history_outbound_numbers())
        for item in trusted_numbers:
            normalized = self.normalize_phone(item)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            normalized_all.append(normalized)
        data["ALL"] = normalized_all

        for key, values in list(data.items()):
            if not str(key).startswith("DEAL_"):
                continue
            if key in {"DEAL_ALL", "DEAL_PENDING"} or str(key).startswith("DEAL_PHONE_"):
                continue
            if not isinstance(values, list):
                continue
            unique_ids = []
            seen_ids = set()
            for raw in values:
                token = str(raw or "").strip()
                if not token or token in seen_ids:
                    continue
                seen_ids.add(token)
                unique_ids.append(token)
            data[key] = unique_ids

        data["PENDING"] = self._clean_pending_phone_map(data.get("PENDING"))
        data["DEAL_PENDING"] = self._clean_pending_deal_map(data.get("DEAL_PENDING"))
        self._reconcile_daily_deal_phone_keys(data)

        return data

    def _load_invalid_numbers_unlocked(self):
        payload = self._load_json(self.invalid_file, [])
        numbers = set()
        for item in payload:
            if not isinstance(item, dict):
                continue
            numbers.update(self.phone_variants(item.get("phone")))
        return numbers, payload

    def _is_blocked_unlocked(self, data, invalid_numbers, phone):
        if self.is_test_whitelist_phone(phone):
            return False
        variants = self.phone_variants(phone)
        all_numbers = set()
        for item in data.get("ALL", []):
            all_numbers.update(self.phone_variants(item))
        pending_numbers = set()
        for item in (data.get("PENDING") or {}).keys():
            pending_numbers.update(self.phone_variants(item))
        return bool(
            variants & all_numbers
            or variants & pending_numbers
        )

    def can_send(self, phone):
        if not self.is_valid_phone(phone):
            return False
        if self.is_test_whitelist_phone(phone):
            return True
        if self.is_phone_in_manual_blocklist(phone):
            return False
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            invalid_numbers, _ = self._load_invalid_numbers_unlocked()
            return not self._is_blocked_unlocked(data, invalid_numbers, phone)
        except Exception:
            return False
        finally:
            if fd is not None:
                self._release_lock(fd)

    def has_any_send_record(self, phone):
        normalized = self.normalize_phone(phone)
        if not normalized:
            return False
        if self.is_test_whitelist_phone(normalized):
            return False
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            variants = self.phone_variants(normalized)
            all_numbers = set()
            for item in data.get("ALL", []):
                all_numbers.update(self.phone_variants(item))
            pending_numbers = set()
            for item in (data.get("PENDING") or {}).keys():
                pending_numbers.update(self.phone_variants(item))
            return bool(variants & all_numbers or variants & pending_numbers)
        except Exception:
            return False
        finally:
            if fd is not None:
                self._release_lock(fd)

    def can_send_followup(self, phone):
        if not self.is_valid_phone(phone):
            return False
        if self.is_test_whitelist_phone(phone):
            return True
        if self.is_phone_in_manual_blocklist(phone):
            return False
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            variants = self.phone_variants(phone)
            pending_numbers = set()
            for item in (data.get("PENDING") or {}).keys():
                pending_numbers.update(self.phone_variants(item))
            return not bool(variants & pending_numbers)
        except Exception:
            return False
        finally:
            if fd is not None:
                self._release_lock(fd)

    def reserve_send(self, phone):
        if not self.is_valid_phone(phone):
            self.mark_invalid(phone, "invalid_phone")
            return False
        if self.is_test_whitelist_phone(phone):
            return True
        if self.is_phone_in_manual_blocklist(phone):
            return False
        normalized = self.normalize_phone(phone)
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            invalid_numbers, _ = self._load_invalid_numbers_unlocked()
            if self._is_blocked_unlocked(data, invalid_numbers, normalized):
                return False
            pending = data.setdefault("PENDING", {})
            pending[normalized] = datetime.now().isoformat()
            self._save_json(self.sent_file, data)
            return True
        except Exception:
            return False
        finally:
            if fd is not None:
                self._release_lock(fd)

    def reserve_followup_send(self, phone):
        if not self.is_valid_phone(phone):
            self.mark_invalid(phone, "invalid_phone")
            return False
        if self.is_test_whitelist_phone(phone):
            return True
        if self.is_phone_in_manual_blocklist(phone):
            return False
        normalized = self.normalize_phone(phone)
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            variants = self.phone_variants(normalized)
            pending_numbers = set()
            for item in (data.get("PENDING") or {}).keys():
                pending_numbers.update(self.phone_variants(item))
            if variants & pending_numbers:
                return False
            pending = data.setdefault("PENDING", {})
            pending[normalized] = datetime.now().isoformat()
            self._save_json(self.sent_file, data)
            return True
        except Exception:
            return False
        finally:
            if fd is not None:
                self._release_lock(fd)

    def release_send_slot(self, phone):
        normalized = self.normalize_phone(phone)
        if self.is_test_whitelist_phone(normalized):
            return
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            pending = data.setdefault("PENDING", {})
            pending.pop(normalized, None)
            self._save_json(self.sent_file, data)
        except Exception:
            pass
        finally:
            if fd is not None:
                self._release_lock(fd)

    def mark_sent(self, phone):
        normalized = self.normalize_phone(phone)
        if not normalized:
            return
        if self.is_test_whitelist_phone(normalized):
            return
        today = datetime.now().strftime("%Y-%m-%d")
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            pending = data.setdefault("PENDING", {})
            pending.pop(normalized, None)
            data.setdefault("ALL", [])
            data.setdefault(today, [])
            if normalized not in data["ALL"]:
                data["ALL"].append(normalized)
            if normalized not in data[today]:
                data[today].append(normalized)
            self._save_json(self.sent_file, data)
        except Exception:
            pass
        finally:
            if fd is not None:
                self._release_lock(fd)

    def mark_invalid(self, phone, reason="invalid_phone"):
        normalized = self.normalize_phone(phone)
        if not normalized:
            return
        if self.is_test_whitelist_phone(normalized):
            return
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            data.setdefault("PENDING", {}).pop(normalized, None)
            self._save_json(self.sent_file, data)
            invalid_numbers, payload = self._load_invalid_numbers_unlocked()
            if normalized not in invalid_numbers:
                payload.append({
                    "phone": normalized,
                    "reason": reason,
                    "created_at": datetime.now().isoformat()
                })
                self._save_json(self.invalid_file, payload)
        except Exception:
            pass
        finally:
            if fd is not None:
                self._release_lock(fd)

    def healthcheck(self):
        try:
            statuses = self.heartbeat()
            return any(bool((body or {}).get("connected")) for body in statuses.values())
        except Exception as e:
            logging.error(f"[BAILEYS_OFFLINE] {e}")
            return False

    def status_payload(self):
        statuses = self.heartbeat()
        for channel_name, body in statuses.items():
            if bool((body or {}).get("connected")):
                payload = dict(body or {})
                payload["channel"] = channel_name
                return payload
        if statuses:
            preferred_name, preferred_status = next(iter(statuses.items()))
            payload = dict(preferred_status or {})
            payload["channel"] = preferred_name
            payload.setdefault("connected", False)
            payload.setdefault("mode", "offline")
            payload.setdefault("needs_qr", False)
            payload.setdefault("session_invalid", False)
            return payload
        return {"connected": False, "mode": "offline", "needs_qr": False, "session_invalid": False}

    def validate_whatsapp(self, phone):
        normalized = self.normalize_phone(phone)
        if not self.is_valid_phone(normalized):
            return False
        if self.is_test_whitelist_phone(normalized):
            return True
        for candidate in self.preferred_phone_variants(normalized):
            try:
                _channel_name, base_url = self._resolve_channel(phone=candidate)
                r = requests.post(
                    f"{base_url}/validate",
                    json={"number": f"55{candidate}"},
                    timeout=20,
                )
                if r.status_code != 200:
                    continue
                try:
                    body = r.json()
                except Exception:
                    body = {}
                exists = bool((body or {}).get("exists"))
                self._cache_validation(candidate, exists)
                if candidate != normalized:
                    self._cache_validation(normalized, exists)
                if exists:
                    return True
            except Exception as e:
                logging.error(f"[ERRO_VALIDACAO_WHATSAPP] telefone={candidate} erro={e}")
        return False

    def validate_whatsapp_cached(self, phone, max_age_seconds=24 * 60 * 60):
        normalized = self.normalize_phone(phone)
        if not self.is_valid_phone(normalized):
            self.mark_invalid(phone, "invalid_phone")
            return False
        if self.is_test_whitelist_phone(normalized):
            return True
        cached = self._get_cached_validation(normalized, max_age_seconds=max_age_seconds)
        if cached is not None:
            return bool(cached)
        exists = self.validate_whatsapp(normalized)
        if not exists:
            self.mark_invalid(normalized, "invalid_phone_outbound")
        return bool(exists)

    def send_message(

        self,
        phone,
        text,
        cadence_step=1,
        allow_non_cellular=True,
        deal_id=None,
        count_towards_daily_limit=True,
        bypass_manual_blocklist=False,
    ):
        normalized = self.normalize_phone(phone)
        if is_automation_freeze_active(service="whatsapp"):
            self.last_send_state = "global_freeze"
            logging.warning(f"[GLOBAL_FREEZE_SEND_BLOCK] telefone={normalized or phone}")
            return False
        if self.is_outbound_manual_mode() and not self.is_test_whitelist_phone(normalized):
            self.last_send_state = "manual_mode"
            logging.warning(f"[MANUAL_MODE_SEND_BLOCK] telefone={normalized or phone}")
            return False
        if not self.is_valid_phone(normalized):
            self.last_send_state = "invalid"
            return False
        clean_text = str(text or "").strip()
        if not clean_text:
            self.last_send_state = "invalid"
            return False
        if not bool(bypass_manual_blocklist) and self.is_phone_in_manual_blocklist(normalized):
            self.last_send_state = "manual_blocklist"
            logging.warning(f"[MANUAL_BLOCKLIST_SEND_BLOCK] telefone={normalized}")
            return False
        if not self.is_test_whitelist_phone(normalized) and self._is_duplicate_recent_text(normalized, clean_text, within_seconds=120):
            self.last_send_state = "duplicate_blocked"
            logging.warning(f"[DUPLICIDADE_BLOQUEADA_TEXTO] {normalized}")
            return False

        self.last_send_state = "failed"
        channel_name, base_url = self._resolve_channel(phone=normalized, deal_id=deal_id)
        status_payload = self._status_for_base_url(base_url)
        if bool(status_payload.get("needs_qr")) or bool(status_payload.get("session_invalid")):
            self.last_send_state = "session_blocked"
            logging.error(
                f"[WA_BLOQUEADO_SEM_SESSAO] canal={channel_name} telefone={normalized} "
                f"needs_qr={bool(status_payload.get('needs_qr'))} session_invalid={bool(status_payload.get('session_invalid'))}"
            )
            logging.warning("[WA_OFFLINE] resposta não enviada")
            return False
        if not bool(status_payload.get("connected")):
            self.last_send_state = "offline_precheck"
            logging.warning(f"[WA_STATUS_OFFLINE_PRECHECK] canal={channel_name} telefone={normalized} tentando_envio_mesmo_assim")

        candidates = self.preferred_phone_variants(normalized)
        for candidate in candidates:
            payload = {
                "number": f"55{candidate}",
                "text": clean_text,
                "count_towards_daily_limit": bool(count_towards_daily_limit),
            }
            jid = f"55{candidate}@s.whatsapp.net"
            for attempt in range(2):
                try:
                    logging.info(f"[WHATSAPP_ENVIO] canal=WA1 telefone={candidate} tentativa={attempt + 1}")
                    logging.info(f"[ENVIO_TENTANDO] {jid} tentativa={attempt + 1}")
                    r = requests.post(f"{base_url}/send", json=payload, timeout=self.SEND_TIMEOUT_SEC)
                    logging.info(f"[ENVIO] canal=WA1 telefone={candidate} status={r.status_code} tentativa={attempt + 1}")
                    if r.status_code == 200:
                        try:
                            body = r.json()
                        except Exception:
                            body = {}
                        status = str((body or {}).get("status") or "").strip().lower()
                        if status == "invalid":
                            self.last_send_state = "invalid"
                            logging.warning(f"[ENVIO_FALHOU] {jid} motivo=invalid")
                            logging.warning(f"[SEND_RESULT] telefone={candidate} state=invalid http_status=200")
                            break
                        if status == "sent" and str((body or {}).get("message_id") or "").strip():
                            self.last_send_state = "sent"
                            self._append_history_entry(
                                candidate,
                                "out",
                                clean_text,
                                step=cadence_step,
                                source="send_api",
                            )
                            self.remember_channel(candidate, deal_id=deal_id, channel_name="WA1")
                            logging.info(f"[ENVIO] canal=WA1 phone={candidate}")
                            logging.info(f"[ENVIO_OK_REAL] {jid}")
                            logging.info(
                                f"[SEND_RESULT] telefone={candidate} state=sent http_status=200 "
                                f"message_id={str((body or {}).get('message_id') or '').strip()}"
                            )
                            return True
                        if status:
                            self.last_send_state = status
                    elif r.status_code == 503:
                        self.last_send_state = "offline"
                    elif r.status_code == 429:
                        self.last_send_state = "warmup_limit"
                    logging.warning(f"[ENVIO_FALHOU] {jid} status_http={r.status_code}")
                except requests.Timeout:
                    self.last_send_state = "timeout"
                    logging.error(f"[WHATSAPP_TIMEOUT] telefone={candidate} tentativa={attempt + 1}")
                except Exception as e:
                    logging.error(f"[ERRO_ENVIO] telefone={candidate} tentativa={attempt + 1} erro={e}")
                time.sleep(1)

        logging.warning(f"[FALHA_ENVIO] {normalized}")
        logging.warning(f"[SEND_RESULT] telefone={normalized} state={self.last_send_state}")
        return False
