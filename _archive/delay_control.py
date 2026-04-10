import time
import random
from datetime import datetime

# 🔥 controle de ritmo diário
INTERVALO_BASE = 18 * 60  # 18 minutos

def delay_inteligente():
    variacao = random.uniform(-300, 300)  # -5 a +5 min
    delay = INTERVALO_BASE + variacao

    agora = datetime.now().strftime("%H:%M:%S")
    print(f"[DELAY INTELIGENTE] {int(delay)}s | {agora}")

    time.sleep(delay)
