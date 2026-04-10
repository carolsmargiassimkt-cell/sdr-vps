module.exports = {
  apps: [
    {
      name: "vps-api",
      cwd: "C:\\Users\\Asus\\legacy\\bot_sdr_ai",
      script: "C:\\Users\\Asus\\legacy\\bot_sdr_ai\\start_vps_api.bat",
      interpreter: "none",
      autorestart: true,
      instances: 1,
      exec_mode: "fork"
    },
    {
      name: "sdr-bot",
      cwd: "C:\\Users\\Asus\\legacy\\bot_sdr_ai",
      script: "C:\\Users\\Asus\\Python311\\python.exe",
      args: "supervisor.py --all",
      interpreter: "none",
      autorestart: true,
      instances: 1,
      exec_mode: "fork"
    },
    {
      name: "whatsapp",
      cwd: "C:\\Users\\Asus\\legacy\\bot_sdr_ai",
      script: "central_whatsapp.mjs",
      interpreter: "node",
      autorestart: true,
      instances: 1,
      exec_mode: "fork"
    }
  ]
};
