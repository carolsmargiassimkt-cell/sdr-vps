
import time
from crm.pipedrive_client import PipedriveClient

def create_super_minas_tasks():
    print("[INFO] Buscando deals Super Minas para criacao de atividade unica...")
    crm = PipedriveClient()
    
    # Busca deals abertos no pipeline 2
    deals = crm.get_deals(status="open", pipeline_id=2, limit=50)
    
    count = 0
    for deal in deals:
        deal_id = int(deal.get("id") or 0)
        if deal_id <= 0:
            continue
            
        # Verifica se e Super Minas (labels 175, 162, 176, 177 conforme supervisor.py)
        labels = str(deal.get("label") or "").split(",")
        is_super_minas = any(l.strip() in ["175", "162", "176", "177", "193"] for l in labels)
        
        if is_super_minas:
            # VERIFICA SE JA EXISTE ATIVIDADE HOJE PARA ESTE DEAL
            subject = "Abordagem Super Minas - Automatica"
            if crm.has_open_activity_today(deal_id=deal_id, activity_type="call", subject=subject):
                print(f"[SKIP] Deal {deal_id} ja possui atividade para hoje.")
                continue

            print(f"[ATIVIDADE] Criando para Deal {deal_id}...")
            ok = crm.create_activity(
                subject=subject,
                type="call",
                deal_id=deal_id,
                due_date=time.strftime("%Y-%m-%d"),
                due_time="10:00",
                note="Atividade gerada via script de reparo SDR (Otimizada)."
            )
            if ok:
                count += 1
                print(f"[OK] Atividade criada para Deal {deal_id}.")
                # LIMITA A 1 ATIVIDADE CONFORME SOLICITADO
                print("[INFO] Limite de 1 atividade relevante atingido.")
                break
            time.sleep(0.5)

    print(f"[FIM] Processo concluido. Total de atividades criadas: {count}")

if __name__ == "__main__":
    create_super_minas_tasks()
