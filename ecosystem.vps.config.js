module.exports = {
  apps: [
    {
      name: "api",
      cwd: "/root/sdr-vps",
      script: "/root/sdr-vps/venv/bin/python3",
      args: "-m uvicorn app.main:app --host 0.0.0.0 --port 8001",
      interpreter: "none",
      autorestart: true,
      env: {
        PYTHONUNBUFFERED: "1"
      }
    },
    {
      name: "handler",
      cwd: "/root/sdr-vps",
      script: "/root/sdr-vps/venv/bin/python3",
      args: "/root/sdr-vps/inbox_handler.py",
      interpreter: "none",
      autorestart: true,
      env: {
        PORT: "5001",
        PYTHONUNBUFFERED: "1",
        WHATSAPP_OUTBOUND_MODE: "auto"
      }
    },
    {
      name: "supervisor",
      cwd: "/root/sdr-vps",
      script: "/root/sdr-vps/venv/bin/python3",
      args: "/root/sdr-vps/supervisor.py",
      interpreter: "none",
      autorestart: true,
      env: {
        WHATSAPP_OUTBOUND_MODE: "manual",
        WHATSAPP_DAILY_LIMIT: "70",
        PYTHONUNBUFFERED: "1"
      }
    },
    {
      name: "whatsapp",
      cwd: "/root/sdr-vps",
      script: "/root/sdr-vps/central_whatsapp.mjs",
      interpreter: "node",
      autorestart: true,
      env: {
        WHATSAPP_OUTBOUND_MODE: "auto",
        WHATSAPP_DAILY_LIMIT: "70"
      }
    }
  ]
};
