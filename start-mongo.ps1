# Starts the portable MongoDB for BOTMAXIMUS (no admin/service required).
# Data lives in .\data\db, logs in .\data\mongod.log.
$root = $PSScriptRoot
$dbPath = Join-Path $root "data\db"
$logPath = Join-Path $root "data\mongod.log"
New-Item -ItemType Directory -Force -Path $dbPath | Out-Null

$mongod = Get-ChildItem -Path (Join-Path $root "mongodb") -Recurse -Filter mongod.exe | Select-Object -First 1
if (-not $mongod) { Write-Error "mongod.exe not found under $root\mongodb"; exit 1 }

& $mongod.FullName --dbpath $dbPath --logpath $logPath --bind_ip 127.0.0.1 --port 27017
