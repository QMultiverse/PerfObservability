<#
.SYNOPSIS
  Common tasks, so nobody has to remember the flags.

.EXAMPLE
  .\scripts\dev.ps1 check          # ruff, mypy and pytest
  .\scripts\dev.ps1 proto          # regenerate the gRPC stubs
  .\scripts\dev.ps1 up             # start the local stack
  .\scripts\dev.ps1 send pacs.008  # deliver one payment into it
#>
param(
    [Parameter(Position = 0)][string]$Task = "help",
    [Parameter(Position = 1, ValueFromRemainingArguments = $true)][string[]]$Rest
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Root ".venv\Scripts\python.exe"
# compose.yaml sits at the repository root, so no -f is needed.
$Compose = @("compose")

if (-not (Test-Path $Python)) { $Python = "python" }

function Invoke-Step($Name, $ScriptBlock) {
    Write-Host "==> $Name" -ForegroundColor Cyan
    & $ScriptBlock
    if ($LASTEXITCODE -ne 0) { throw "$Name failed with exit code $LASTEXITCODE" }
}

switch ($Task) {
    "install" {
        Invoke-Step "install" { & $Python -m pip install -e ".[dev]" }
        Invoke-Step "proto"   { & $Python (Join-Path $Root "scripts\gen_proto.py") }
    }
    "proto"   { & $Python (Join-Path $Root "scripts\gen_proto.py") }
    "samples" { & $Python (Join-Path $Root "scripts\make_samples.py") }
    "topics"  { & $Python (Join-Path $Root "scripts\create_topics.py") --list }
    "lint"    { Invoke-Step "ruff" { & $Python -m ruff check $Root } }
    "format"  { & $Python -m ruff format $Root }
    "types"   { & $Python -m mypy hub ess libs }
    "test"    { & $Python -m pytest @Rest }
    "check" {
        Invoke-Step "stubs" { & $Python (Join-Path $Root "scripts\gen_proto.py") --check }
        Invoke-Step "ruff"  { & $Python -m ruff check $Root }
        Invoke-Step "mypy"  { & $Python -m mypy hub ess libs }
        Invoke-Step "tests" { & $Python -m pytest -q }
        Write-Host "all checks passed" -ForegroundColor Green
    }
    "up"      { docker @Compose up -d --build }
    "down"    { Write-Host "this wipes all local logs and payment data" -ForegroundColor Yellow; docker @Compose down }
    "stop"    { docker @Compose stop }
    "logs"    { docker @Compose logs -f @Rest }
    "ps"      { docker @Compose ps }
    "send" {
        $type = if ($Rest) { $Rest[0] } else { "pacs.008" }
        docker @Compose exec ess python -m ess.cli send --type $type
    }
    "cases"   { docker @Compose exec ess python -m ess.cli case run-all }
    default {
        Write-Host "tasks: install, proto, samples, topics, lint, format, types, test, check,"
        Write-Host "       up, down, stop, logs, ps, send, cases"
    }
}
