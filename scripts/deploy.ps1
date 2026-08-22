$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not (Test-Path '.env')) {
    throw "Missing $root\.env - copy .env.example and fill it in"
}

function Assert-Exit([string]$Name) {
    if ($LASTEXITCODE -ne 0) {
        throw "$Name failed with exit code $LASTEXITCODE"
    }
}

$deadline = (Get-Date).AddMinutes(3)
$osType = $null
do {
    $osType = docker info --format '{{.OSType}}' 2>$null
    if ($LASTEXITCODE -eq 0 -and $osType) { break }
    Start-Sleep -Seconds 3
} while ((Get-Date) -lt $deadline)

if ($LASTEXITCODE -ne 0 -or -not $osType) {
    throw 'Docker is not running. Start Docker Desktop and wait until the engine is ready.'
}
if ($osType.Trim() -ne 'linux') {
    throw "Docker is in '$($osType.Trim())' mode; switch to Linux containers (the image is Debian)."
}

docker compose up -d --build --remove-orphans --wait --wait-timeout 120
Assert-Exit 'docker compose up'

Start-Sleep -Seconds 8

$containerId = (docker compose ps -q edgepc | Select-Object -Last 1)
if (-not $containerId) {
    docker compose logs --tail 200
    throw 'edgepc container was not created'
}

$status = (docker inspect -f '{{.State.Status}}' $containerId).Trim()
if ($status -ne 'running') {
    docker compose logs --tail 200
    throw "edgepc is $status, expected running"
}

docker compose ps
docker image prune -f
