    def _update_tag(self, deal_id, new_tag):
        r = requests.get(
            f"{self.base_url}/deals/{deal_id}",
            params={"api_token": self.api_token}
        )

        data = r.json().get("data") or {}
        current = str(data.get("label") or "")

        tags = [x.strip() for x in current.split(",") if x.strip()]

        # ?? GARANTE SUPER_MINAS
        if "175" not in tags and "162" not in tags:
            tags.append("175")

        # ?? REMOVE CAD ANTIGO
        cad_tags = ["176","177","178","179","180"]
        tags = [t for t in tags if t not in cad_tags]

        # ? ADICIONA NOVO CAD
        tags.append(str(new_tag))

        new_labels = ",".join(tags)

        requests.put(
            f"{self.base_url}/deals/{deal_id}",
            params={"api_token": self.api_token},
            json={"label": new_labels}
        )

        print(f"[TAG_UPDATE] {deal_id} ? {new_labels}")
