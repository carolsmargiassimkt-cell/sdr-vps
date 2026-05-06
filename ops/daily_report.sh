#!/usr/bin/env bash
cd /root/sdr-vps || exit 1
OUT="reports/report_$(date +%F).txt"

{
echo "RELATÓRIO SDR - $(date)"
echo
echo "=== PM2 ==="
pm2 jlist | python3 - <<'PY'
import sys,json
try:
    data=json.load(sys.stdin)
    for p in data:
        print(f"{p.get('name')}: {p.get('pm2_env',{}).get('status')}")
except Exception:
    pass
PY

echo
echo "=== CONTADOR ==="
cat data/whatsapp_daily_counter.json 2>/dev/null || true

echo
echo "=== INTENTS ==="
grep -h "INTENT_DETECTED" /root/.pm2/logs/handler-out.log 2>/dev/null | tail -200 | sed -E 's/.*intent=([^ ]+).*/\1/' | sort | uniq -c | sort -nr

echo
echo "=== ENVIOS OK ==="
grep -h "ENVIO_OK_REAL" /root/.pm2/logs/*.log 2>/dev/null | tail -100 | wc -l

echo
echo "=== BLOQUEIOS ==="
grep -h "BLOCKLIST_ADICIONADO\|AUTO_REPLY_BLOQUEADO_TAG_CRM\|INBOUND_OLD_SESSION_IGNORED" /root/.pm2/logs/handler-out.log 2>/dev/null | tail -100

echo
echo "=== ERROS ==="
grep -hEi "Traceback|ERRO|FALHOU|RATE_LIMIT|429" /root/.pm2/logs/*.log 2>/dev/null | tail -80
} > "$OUT"

echo "RELATORIO_GERADO: $OUT"
cat "$OUT"

echo
echo "=== HOT LEADS / RETOMADA ==="
./ops/hot_leads_queue.sh 60 48
