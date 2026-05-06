#!/usr/bin/env bash
cd /root/sdr-vps || exit 1
./ops/hot_leads_queue.sh "${1:-60}" "${2:-48}" > "reports/hot_leads_$(date +%F_%H%M).txt"
echo "OK reports/hot_leads_$(date +%F_%H%M).txt"
