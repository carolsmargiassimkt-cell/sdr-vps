#!/usr/bin/env bash
set -euo pipefail
cd /root/sdr-vps
exec /root/pyfix/bin/python3 -u /root/sdr-vps/supervisor.py
