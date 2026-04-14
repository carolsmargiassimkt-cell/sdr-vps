
import json
import time
from crm.pipedrive_client import PipedriveClient

def create_super_minas_tasks_aggressive():
    print("[INFO] Criando 50 atividades Super Minas (MODO AGRESSIVO)...")
    crm = PipedriveClient()
    
    # Busca 100 deals abertos no pipeline 2 (Pipeline Super Minas)
    deals = crm.get_deals(status="open", pipeline_id=2, limit=100)
    
    count = 0
    for deal in deals:
        if count >= 50:
            break
            
        deal_id = deal.get("id")
        title = str(deal.get("title") or "").upper()
        
        # Filtro: Contém Super Minas no título ou labels Super Minas
        labels = str(deal.get("label") or "").split(",")
        is_sm = any(l in ["175", "162"] for l in labels) or "SUPER MINAS" in title or "SUPERMINAS" in title
        
        if is_sm:
            print(f"[ATIVIDADE] Criando para Deal {deal_id} ({title})...")
            ok = crm.create_activity(
                subject="Abordagem Super Minas - Automatica",
                type="call",
                deal_id=deal_id,
                due_date=time.strftime("%Y-%m-%d"),
                due_time="10:00",
                note="Atividade gerada via script de reparo SDR."
            )
            if ok:
                count += 1
                print(f"[OK] {count}/50 atividades criadas.")
            time.sleep(0.5)

    print(f"[FIM] Total de atividades criadas: {count}")

if __name__ == "__main__":
    create_super_minas_tasks_aggressive()
