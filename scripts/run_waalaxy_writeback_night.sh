#!/usr/bin/env bash
set -euo pipefail

cd /root/sdr-vps
set -a
. /root/sdr-vps/.env
set +a
mkdir -p logs runtime

LOCK="runtime/waalaxy_writeback.lock"

if [ -f "$LOCK" ]; then
  echo "[WAALAXY_WRITEBACK] lock ativo, saindo"
  exit 0
fi

touch "$LOCK"
trap 'rm -f "$LOCK"' EXIT

echo "[WAALAXY_WRITEBACK] inicio $(date)"

for i in $(seq 1 30); do
  echo "[WAALAXY_WRITEBACK] ciclo=$i $(date)"
  python3 scripts/apply_waalaxy_writeback.py --apply --limit 1 || true
  sleep 300
done

echo "[WAALAXY_WRITEBACK] fim $(date)"
