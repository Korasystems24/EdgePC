# One-time setup on the Windows bunker (no public IP required).
#
# Prerequisites: Git, Docker Desktop in Linux-container mode, .env next to compose.
# Run elevated PowerShell (Run as administrator):
#
#   cd <clone>
#   .\scripts\install-github-runner.ps1 -Token <registration-token>
#
# Token: repo → Settings → Actions → Runners → New self-hosted runner (Windows).
# Optional: -RunnerDir C:\actions-runner  -RunnerName bunker-1  -LogonPassword '...'

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Token,

    [string]$RunnerDir = 'C:\actions-runner',

    [string]$RunnerName = $env:COMPUTERNAME,

    [string]$RunnerLabels = 'edgepc',

    [string]$LogonPassword
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]$identity
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Run this script in an elevated PowerShell (Run as administrator).'
}

foreach ($cmd in @('git', 'docker')) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        throw "Missing command: $cmd"
    }
}

docker compose version | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw 'docker compose plugin is required'
}

$osType = (docker info --format '{{.OSType}}' 2>$null)
if ($LASTEXITCODE -ne 0) {
    throw 'Docker is not running. Start Docker Desktop (Linux containers) first.'
}
if ($osType.Trim() -ne 'linux') {
    throw "Docker is in '$($osType.Trim())' mode; switch to Linux containers."
}

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$origin = (git -C $root remote get-url origin).Trim().TrimEnd('.git')
if ($origin -notmatch 'github.com[:/]([^/]+)/([^/]+)$') {
    throw "Cannot parse GitHub remote: $origin"
}
$repoUrl = "https://github.com/$($Matches[1])/$($Matches[2])"

$arch = switch ($env:PROCESSOR_ARCHITECTURE) {
    'AMD64' { 'win-x64' }
    'ARM64' { 'win-arm64' }
    default { throw "Unsupported architecture: $env:PROCESSOR_ARCHITECTURE" }
}

New-Item -ItemType Directory -Force -Path $RunnerDir | Out-Null
Set-Location $RunnerDir

if (-not (Test-Path '.\config.cmd')) {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    $release = Invoke-RestMethod -Uri 'https://api.github.com/repos/actions/runner/releases/latest' `
        -Headers @{ 'User-Agent' = 'EdgePC-runner-setup' }
    $version = $release.tag_name.TrimStart('v')
    $zip = "actions-runner-$arch-$version.zip"
    Write-Host "Downloading GitHub runner $version ($arch)"
    Invoke-WebRequest -UseBasicParsing `
        -Uri "https://github.com/actions/runner/releases/download/v$version/$zip" `
        -OutFile $zip
    Expand-Archive -Path $zip -DestinationPath . -Force
    Remove-Item $zip
}

if (Test-Path '.\svc.cmd') {
    & .\svc.cmd stop | Out-Null
}

$account = "$env:USERDOMAIN\$env:USERNAME"
if (-not $LogonPassword) {
    $secure = Read-Host "Password for $account (runner Windows service)" -AsSecureString
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    try {
        $LogonPassword = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    }
    finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
}

$configArgs = @(
    '--unattended', '--replace',
    '--url', $repoUrl,
    '--token', $Token,
    '--name', $RunnerName,
    '--labels', $RunnerLabels,
    '--work', '_work',
    '--runasservice',
    '--windowslogonaccount', $account,
    '--windowslogonpassword', $LogonPassword
)
& .\config.cmd @configArgs
if ($LASTEXITCODE -ne 0) {
    throw "config.cmd failed with exit $LASTEXITCODE"
}

$edgepcDir = $root.Replace('\', '/')
$envLines = @()
if (Test-Path '.\.env') {
    $envLines = @(Get-Content '.\.env' | Where-Object { $_ -notmatch '^(EDGEPC_DIR|PATH)=' })
}
$envLines += "EDGEPC_DIR=$edgepcDir"
$envLines += "PATH=$($env:PATH)"
$utf8 = New-Object System.Text.UTF8Encoding $false
[IO.File]::WriteAllLines((Join-Path $RunnerDir '.env'), $envLines, $utf8)

& .\svc.cmd start
& .\svc.cmd status

Write-Host ""
Write-Host "Runner $RunnerName is listening for jobs."
Write-Host "EDGEPC_DIR=$edgepcDir"
Write-Host "Merge .github/workflows/deploy.yml to main, then a push will rebuild here."
Write-Host "Docker Desktop must start at boot (same Windows user as this service)."
