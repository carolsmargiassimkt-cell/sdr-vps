import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

import requests


class WhatsAppService:
    SEND_TIMEOUT_SEC = 40

    def __init__(self, *args, **kwargs):
        self.base_dir = Path(__file__).resolve().parents[1]
        self.data_dir = self.base_dir / "data"
        self.logs_dir = self.base_dir / "logs"
        self.base_urls = {
            "WA1": "http://127.0.0.1:3000",
            "WA2": "http://127.0.0.1:3001",
        }
        self.sent_file = str(self.base_dir / "sent.json")
        self.sent_lock_file = str(self.base_dir / "sent.json.lock")
        self.invalid_file = str(self.base_dir / "invalidos.json")
        self.history_file = str(self.logs_dir / "whatsapp_message_history.json")
        self.channel_map_file = str(self.data_dir / "whatsapp_channel_map.json")
        self.after_hours_state_file = str(self.data_dir / "after_hours_state.json")
        self.after_hours_lock_file = str(self.data_dir / "after_hours_state.lock")
        self.last_send_state = ""

    def normalize_phone(self, phone):
        digits = re.sub(r"\D+", "", str(phone or ""))
        if digits.startswith("55") and len(digits) > 11:
            digits = digits[2:]
        return digits

    def phone_variants(self, phone):
        num = self.normalize_phone(phone)
        variants = {num}
        if len(num) == 11 and num[2] == "9":
            variants.add(num[:2] + num[3:])
        if len(num) == 10:
            variants.add(num[:2] + "9" + num[2:])
        return {item for item in variants if item}

    def is_valid_phone(self, phone):
        num = self.normalize_phone(phone)
        if len(num) not in {10, 11}:
            return False
        if len(set(num)) == 1:
            return False
        if num[:2] == "00":
            return False
        if len(num) == 11 and num[2] != "9":
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
            json.dump(payload, f, indent=2 if isinstance(payload, list) else None)

    def _load_channel_map(self):
        payload = self._load_json(self.channel_map_file, {"phones": {}, "deals": {}})
        if not isinstance(payload, dict):
            payload = {"phones": {}, "deals": {}}
        payload.setdefault("phones", {})
        payload.setdefault("deals", {})
        return payload

    def _save_channel_map(self, payload):
        self._save_json(self.channel_map_file, payload)

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
        if len(healthy) == 1:
            return healthy[0][0], healthy[0][1]
        target = "WA1" if int(deal_id or 0) % 2 == 0 else "WA2"
        for channel_name, base_url, _status in healthy:
            if channel_name == target:
                return channel_name, base_url
        return healthy[0][0], healthy[0][1]

    def remember_channel(self, phone, deal_id=None, channel_name="WA1"):
        payload = self._load_channel_map()
        normalized = self.normalize_phone(phone)
        if normalized:
            payload.setdefault("phones", {})[normalized] = str(channel_name or "WA1")
        if int(deal_id or 0) > 0:
            payload.setdefault("deals", {})[str(int(deal_id))] = str(channel_name or "WA1")
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
                phone = token.split("_")[-1] if "_" in token else token
                normalized = self.normalize_phone(phone)
                if normalized:
                    results.update(self.phone_variants(normalized))
        return results

    def _load_sent_data_unlocked(self):
        data = self._load_json(self.sent_file, {"ALL": [], "PENDING": {}})
        if not isinstance(data, dict):
            data = {"ALL": [], "PENDING": {}}
        data.setdefault("ALL", [])
        data.setdefault("PENDING", {})
        data.setdefault("DEAL_ALL", [])
        data.setdefault("DEAL_PENDING", {})
        normalized_all = []
        seen = set()
        for item in list(data.get("ALL", [])) + list(self._legacy_numbers(data)):
            normalized = self.normalize_phone(item)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            normalized_all.append(normalized)
        data["ALL"] = normalized_all
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
            or variants & invalid_numbers
        )

    def can_send(self, phone):
        if not self.is_valid_phone(phone):
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
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            invalid_numbers, _ = self._load_invalid_numbers_unlocked()
            variants = self.phone_variants(phone)
            pending_numbers = set()
            for item in (data.get("PENDING") or {}).keys():
                pending_numbers.update(self.phone_variants(item))
            return not bool(variants & pending_numbers or variants & invalid_numbers)
        except Exception:
            return False
        finally:
            if fd is not None:
                self._release_lock(fd)

    def reserve_send(self, phone):
        if not self.is_valid_phone(phone):
            self.mark_invalid(phone, "invalid_phone")
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
        normalized = self.normalize_phone(phone)
        fd = None
        try:
            fd = self._acquire_lock()
            data = self._load_sent_data_unlocked()
            invalid_numbers, _ = self._load_invalid_numbers_unlocked()
            variants = self.phone_variants(normalized)
            pending_numbers = set()
            for item in (data.get("PENDING") or {}).keys():
                pending_numbers.update(self.phone_variants(item))
            if variants & invalid_numbers or variants & pending_numbers:
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
        try:
            _channel_name, base_url = self._resolve_channel(phone=normalized)
            r = requests.post(
                f"{base_url}/validate",
                json={"number": f"55{normalized}"},
                timeout=20,
            )
            if r.status_code != 200:
                return False
            try:
                body = r.json()
            except Exception:
                body = {}
            return bool((body or {}).get("exists"))
        except Exception as e:
            logging.error(f"[ERRO_VALIDACAO_WHATSAPP] telefone={normalized} erro={e}")
            return False

    def send_message(self, phone, text, cadence_step=1, allow_non_cellular=True, deal_id=None):
        normalized = self.normalize_phone(phone)
        if not self.is_valid_phone(normalized):
            self.last_send_state = "invalid"
            return False
        clean_text = str(text or "").strip()
        if not clean_text:
            self.last_send_state = "invalid"
            return False
        if self._is_duplicate_recent_text(normalized, clean_text, within_seconds=120):
            self.last_send_state = "duplicate_blocked"
            logging.warning(f"[DUPLICIDADE_BLOQUEADA_TEXTO] {normalized}")
            return False

        self.last_send_state = "failed"
        payload = {"number": f"55{normalized}", "text": clean_text}
        jid = f"55{normalized}@s.whatsapp.net"
        channel_name, base_url = self._resolve_channel(phone=normalized, deal_id=deal_id)
        status_payload = self._status_for_base_url(base_url)
        if bool(status_payload.get("needs_qr")) or bool(status_payload.get("session_invalid")):
            self.last_send_state = "session_blocked"
            logging.error(
                f"[WA_BLOQUEADO_SEM_SESSAO] canal={channel_name} telefone={normalized} "
                f"needs_qr={bool(status_payload.get('needs_qr'))} session_invalid={bool(status_payload.get('session_invalid'))}"
            )
            return False
        if not bool(status_payload.get("connected")):
            self.last_send_state = "offline"
            logging.warning(f"[WA_OFFLINE] canal={channel_name} telefone={normalized}")
            return False

        for attempt in range(2):
            try:
                logging.info(f"[WHATSAPP_ENVIO] canal={channel_name} telefone={normalized} tentativa={attempt + 1}")
                logging.info(f"[ENVIO_TENTANDO] {jid} tentativa={attempt + 1}")
                r = requests.post(f"{base_url}/send", json=payload, timeout=self.SEND_TIMEOUT_SEC)
                logging.info(f"[ENVIO] telefone={normalized} status={r.status_code} tentativa={attempt + 1}")
                if r.status_code == 200:
                    try:
                        body = r.json()
                    except Exception:
                        body = {}
                    status = str((body or {}).get("status") or "").strip().lower()
                    if status == "invalid":
                        self.last_send_state = "invalid"
                        logging.warning(f"[ENVIO_FALHOU] {jid} motivo=invalid")
                        return False
                    if status == "sent" and str((body or {}).get("message_id") or "").strip():
                        self.last_send_state = "sent"
                        self.remember_channel(normalized, deal_id=deal_id, channel_name=channel_name)
                        logging.info(f"[WHATSAPP_OK] telefone={normalized}")
                        logging.info(f"[ENVIO_OK_REAL] {jid}")
                        return True
                logging.warning(f"[ENVIO_FALHOU] {jid} status_http={r.status_code}")
            except requests.Timeout:
                self.last_send_state = "timeout"
                logging.error(f"[WHATSAPP_TIMEOUT] telefone={normalized} tentativa={attempt + 1}")
                return False
            except Exception as e:
                logging.error(f"[ERRO_ENVIO] telefone={normalized} tentativa={attempt + 1} erro={e}")
            time.sleep(1)

        logging.warning(f"[FALHA_ENVIO] {normalized}")
        return False
