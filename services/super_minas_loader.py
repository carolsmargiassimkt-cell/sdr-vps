def _fetch_super_minas_targets(self, limit=None):
    try:
        targets = []

        # Fonte principal (ajusta se necessário)
        if hasattr(self, "super_minas_source") and self.super_minas_source:
            targets = self.super_minas_source
        else:
            # fallback padrão (arquivo local)
            import json, os
            path = os.path.join("data", "super_minas_leads.json")
            if os.path.exists(path):
                with open(path, encoding="utf-8") as f:
                    targets = json.load(f)

        if not isinstance(targets, list):
            print("[ERRO_LEADS_FORMATO_INVALIDO]")
            return []

        # normalização mínima (evita quebrar worker)
        normalized = []
        for t in targets:
            if not isinstance(t, dict):
                continue

            phone = t.get("phone") or t.get("telefone") or t.get("whatsapp")
            if not phone:
                continue

            normalized.append({
                "phone": str(phone),
                "name": t.get("name") or t.get("nome") or "",
                "company": t.get("company") or t.get("empresa") or "",
                "raw": t
            })

        # aplicar limite se existir
        if limit:
            normalized = normalized[:int(limit)]

        print(f"[LEADS_CARREGADOS] {len(normalized)} encontrados")
        return normalized

    except Exception as e:
        print(f"[ERRO_CARREGAR_LEADS] {e}")
        return []
