#!/usr/bin/env bash
cd /root/sdr-vps || exit 1

TODAY="$(TZ=America/Sao_Paulo date +%F)"
DOW="$(TZ=America/Sao_Paulo date +%u)"

if [ "$DOW" -gt 5 ]; then
  echo "[CRON_SKIP] fim_de_semana date=$TODAY"
  exit 0
fi

if grep -qx "$TODAY" /root/sdr-vps/config/feriados_br_2026.txt; then
  echo "[CRON_SKIP] feriado_br date=$TODAY"
  exit 0
fi

./ops/start_supervisor.sh
