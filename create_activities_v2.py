
import json
import time
from crm.pipedrive_client import PipedriveClient

def create_super_minas_tasks_v2():
    print("[INFO] Criando 50 atividades Super Minas (Labels 176, 177 + Tag SUPER_MINAS)...")
    crm = PipedriveClient()

    # Busca deals abertos (limit 2000 para cobrir bem a base)
    deals = crm.get_deals(status="open", limit=2000)
    print(f"[INFO] {len(deals)} deals encontrados.")

    count = 0
    for deal in deals:
        if count >= 50:
            break

        deal_id = deal.get("id")
        
        # Pega labels como string ou lista
        raw_labels = deal.get("label")
        if isinstance(raw_labels, list):
            labels = [str(l) for l in raw_labels]
        else:
            labels = str(raw_labels or "").split(",")

        # Verifica Labels 176, 177, 175, 162
        is_sm_label = any(l in ["176", "177", "175", "162"] for l in labels)
        
        # Também verifica se tem a tag "SUPER_MINAS" em algum campo de texto ou se o título contém
        title = str(deal.get("title") or "").upper()
        is_sm_tag = "SUPER MINAS" in title or "SUPERMINAS" in title or "SUPER_MINAS" in title

        if is_sm_label or is_sm_tag:
            # Verifica se já tem atividade hoje para evitar duplicidade manual
            if crm.has_open_activity_today(deal_id=deal_id):
                 print(f"[SKIP] Deal {deal_id} já tem atividade.")
                 continue

            print(f"[ATIVIDADE] Criando para Deal {deal_id} (Labels: {labels})...")
            ok = crm.create_activity(
                subject="Abordagem Super Minas - Automatica",
                type="call",
                deal_id=deal_id,
                due_date=time.strftime("%Y-%m-%d"),
                due_time="11:30",
                note="Atividade gerada via script de reparo SDR (Filtro Super Minas Expandido)."
            )
            if ok:
                count += 1
                print(f"[OK] {count}/50 atividades criadas.")
            time.sleep(0.3)

    print(f"[FIM] Total de atividades criadas: {count}")

if __name__ == "__main__":
    create_super_minas_tasks_v2()
