
import json
import time
from crm.pipedrive_client import PipedriveClient

def create_super_minas_tasks():
    print("[INFO] Iniciando criacao de 50 atividades Super Minas...")
    
    # O token ja foi atualizado no config via ferramenta anterior
    crm = PipedriveClient()
    
    # Busca deals com a label SUPER MINAS ou similar
    # Aqui vamos buscar os deals abertos no pipeline 2
    deals = crm.get_deals(status="open", pipeline_id=2, limit=100)
    
    count = 0
    for deal in deals:
        if count >= 50:
            break
            
        deal_id = deal.get("id")
        # Verifica se e Super Minas (labels 175 ou 162 conforme supervisor.py)
        labels = str(deal.get("label") or "").split(",")
        is_super_minas = any(l in ["175", "162"] for l in labels)
        
        if is_super_minas:
            print(f"[ATIVIDADE] Criando para Deal {deal_id}...")
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
            time.sleep(0.5) # Evitar rate limit

    print(f"[FIM] Total de atividades criadas: {count}")

if __name__ == "__main__":
    create_super_minas_tasks()
