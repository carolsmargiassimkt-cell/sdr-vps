#!/usr/bin/env bash
LOG="/root/.pm2/logs/handler-out.log"
echo "=== POSSÍVEIS LEADS QUENTES HOJE ==="
grep "$(date +%d/%b/%Y)" "$LOG" 2>/dev/null \
| grep -Ei "INTENT_DETECTED|interesse|positivo|agendar|reuni|valor|preço|preco|pode|sim|vamos|me chama|conversando|RESPOSTA_ENVIADA" \
| tail -120
