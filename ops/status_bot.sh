#!/usr/bin/env bash
cd /root/sdr-vps || exit 1

echo "=== PM2 ==="
pm2 list

echo
echo "=== WHATSAPP STATUS ==="
curl -s http://127.0.0.1:3000/status || true

echo
echo "=== API STATUS ==="
curl -s http://127.0.0.1:8001/ || true

echo
echo "=== CONTADOR ==="
cat data/whatsapp_daily_counter.json 2>/dev/null || echo "sem contador"

echo
echo "=== COOLDOWN ==="
cat data/whatsapp_global_cooldown.json 2>/dev/null || echo "sem cooldown"

echo
echo "=== SESSION GUARD ==="
cat data/whatsapp_session_guard.json 2>/dev/null || echo "sem session guard"

echo
echo "=== ÚLTIMOS ALERTAS ==="
grep -Ei "Traceback|ERRO|FALHOU|SESSION_GUARD|RATE_LIMIT|BLOQUEADO|COOLDOWN|INTENT_DETECTED|RESPOSTA_ENVIADA" \
  /root/.pm2/logs/handler-out.log /root/.pm2/logs/supervisor-out.log 2>/dev/null | tail -80
