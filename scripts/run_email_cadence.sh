#!/bin/bash
cd /root/sdr-vps || exit 1
export PYTHONPATH=/root/sdr-vps
set -a
. /root/sdr-vps/.env
set +a
/usr/bin/python3 /root/sdr-vps/email_cadence/engine.py
