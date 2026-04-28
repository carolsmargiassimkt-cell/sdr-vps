#!/usr/bin/env bash
set -euo pipefail

export PM2_HOME=/root/.pm2
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

/usr/local/bin/pm2 stop super-minas-writeback || true
/usr/local/bin/pm2 stop pipeline2-nightly-writeback || true
