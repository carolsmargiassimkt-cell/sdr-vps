    def _update_tag(self, deal_id, new_tag):
        r = requests.get(
            f"{self.base_url}/deals/{deal_id}",
            params={"api_token": self.api_token}
        )

        data = r.json().get("data") or {}
        current = str(data.get("label") or "")

        tags = [x.strip() for x in current.split(",") if x.strip()]

        # garante SUPER_MINAS
        if "175" not in tags and "162" not in tags:
            tags.append("175")

        # adiciona nova tag
        if str(new_tag) not in tags:
            tags.append(str(new_tag))

        new_labels = ",".join(tags)

        requests.put(
            f"{self.base_url}/deals/{deal_id}",
            params={"api_token": self.api_token},
            json={"label": new_labels}
        )

        print(f"[TAG_SAFE] {deal_id} ? {new_labels}")
