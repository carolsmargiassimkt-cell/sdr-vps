import time
from services.autopilot_service import AutopilotService

service = AutopilotService()

idle_cycles = 0

while True:
    print("[LOOP_INICIO]")

    has_work = False

    try:
        # 🔥 roda autopilot real
        processed = service.run_once()

        if processed:
            has_work = True

    except Exception as e:
        print(f"[WORKER_ERRO] {e}")

    if not has_work:
        idle_cycles += 1
        time.sleep(min(5, idle_cycles))
    else:
        idle_cycles = 0
        time.sleep(0.5)
