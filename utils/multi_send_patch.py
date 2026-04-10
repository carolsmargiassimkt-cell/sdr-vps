def _extract_all_phones(self, lead):
    phones = []

    base = [
        lead.get("telefone"),
        lead.get("phone"),
        lead.get("celular"),
    ]

    extra = lead.get("phones") or lead.get("contatos") or []

    for p in base:
        if p:
            phones.append(str(p))

    for item in extra:
        if isinstance(item, dict):
            val = item.get("phone") or item.get("telefone")
            if val:
                phones.append(str(val))
        elif isinstance(item, str):
            phones.append(item)

    # remove duplicados e vazios
    phones = list(set([p.strip() for p in phones if p]))

    return phones


def _send_to_all_numbers(self, lead, message):
    phones = self._extract_all_phones(lead)

    if not phones:
        self._emit("[SEM_TELEFONE]")
        return False

    for phone in phones:
        try:
            self._emit(f"[TENTANDO_ENVIO] telefone={phone}")

            # NÃO bloqueia fixo → tenta sempre
            if len(phone) < 10:
                self._emit(f"[SKIP_INVALIDO] telefone={phone}")
                continue

            sent = self.send_whatsapp_message(phone, message)

            if sent:
                self._emit(f"[ENVIADO_OK] telefone={phone}")
                return True

        except Exception as e:
            self._emit(f"[ERRO_ENVIO] telefone={phone} erro={e}")

    self._emit("[FALHA_TOTAL_ENVIO]")
    return False
