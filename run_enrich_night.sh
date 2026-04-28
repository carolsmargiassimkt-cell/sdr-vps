#!/usr/bin/env bash
set -euo pipefail

cd /root/sdr-vps

export API_SLEEP=0.8

/usr/bin/python3 -u /root/sdr-vps/enrich_final_oportunidados_safe.py \
  >> /root/sdr-vps/runtime/enrich_final_oportunidados_safe_$(date +\%F).log 2>&1
