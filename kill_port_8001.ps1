$port = 8001

$netstatLines = netstat -ano -p TCP | Select-String -Pattern "LISTENING"
$pids = @()

foreach ($line in $netstatLines) {
    $text = ($line.ToString() -replace "\s+", " ").Trim()
    if ($text -match "^(TCP)\s+(\S+):$port\s+\S+\s+LISTENING\s+(\d+)$") {
        $pid = [int]$matches[3]
        if ($pid -gt 0 -and -not $pids.Contains($pid)) {
            $pids += $pid
        }
    }
}

if ($pids.Count -eq 0) {
    Write-Host "[KILL_PORT] Porta livre"
    exit 0
}

foreach ($pid in $pids) {
    Write-Host "[KILL_PORT] Matando PID $pid"
    taskkill /F /PID $pid | Out-Null
}

exit 0
