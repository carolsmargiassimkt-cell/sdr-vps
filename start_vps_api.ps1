$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

& "$scriptDir\kill_port_8001.ps1"
& "C:\Users\Asus\Python311\python.exe" -m uvicorn app.main:app --host 127.0.0.1 --port 8001
