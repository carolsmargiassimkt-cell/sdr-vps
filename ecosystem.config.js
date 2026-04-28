module.exports = {
  apps: [
    {
      name: "sdr-bot",
      script: "/root/sdr-vps/venv/bin/python3",
      args: "supervisor.py",
      cwd: "/root/sdr-vps",
      env: {
        WHATSAPP_DAILY_LIMIT: "70",
      },
    },
    {
      name: "whatsapp",
      script: "central_whatsapp.mjs",
      cwd: "/root/sdr-vps",
      env: {
        WHATSAPP_DAILY_LIMIT: "70",
      },
    },
    {
      name: "api",
      script: "/root/sdr-vps/venv/bin/python3",
      args: "-m uvicorn app.main:app --host 0.0.0.0 --port 8001",
      cwd: "/root/sdr-vps",
    }
  ]
};
