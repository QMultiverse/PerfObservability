<#
.SYNOPSIS
  Send one payment into the running stack and follow it through Kafka,
  the status API, PostgreSQL and Elasticsearch.

.DESCRIPTION
  Does in one command what the walkthrough does step by step. Needs the local
  stack up:  docker compose up -d

.EXAMPLE
  .\scripts\trace-payment.ps1
  .\scripts\trace-payment.ps1 -Type MT103
  .\scripts\trace-payment.ps1 -Type pacs.008 -RunId MY-RUN -Topic hub.pay.canonical
#>
param(
    [string]$Type = "pacs.008",
    [string]$RunId = "TRACE",
    # Which topic to dump the payment's record from.
    [string]$Topic = "hub.in.mx.raw",
    [switch]$SkipElastic
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
# compose.yaml sits at the repository root, so no -f is needed.
$Compose = @("compose")

function Heading($text) { Write-Host "`n=== $text ===" -ForegroundColor Cyan }

# ---------------------------------------------------------------- 1. send
Heading "1. send a $Type into the Hub"
$sent = docker @Compose exec -T ess python -m ess.cli send --type $Type --run-id $RunId
$sent | ForEach-Object { Write-Host "  $_" }
$uetr = [string]($sent | Select-Object -First 1).Trim()
if (-not $uetr) { throw "no UETR came back; is the stack up?" }

# ------------------------------------------------------------- 2. status
Heading "2. where the Hub thinks it is"
$deadline = (Get-Date).AddSeconds(30)
do {
    Start-Sleep -Milliseconds 400
    try { $status = Invoke-RestMethod "http://localhost:8080/payments/$uetr" } catch { $status = $null }
} while ((-not $status -or $status.state -notin @("COMPLETED", "REJECTED", "BLOCKED")) -and (Get-Date) -lt $deadline)

if (-not $status) { throw "the status API has nothing for $uetr" }
Write-Host ("  {0}  {1}  {2}  ({3:N0} ms end to end)" -f `
    $status.uetr, $status.format, $status.state, ($status.end_to_end_s * 1000))
Write-Host "`n  journey:"
foreach ($h in $status.history) {
    Write-Host ("    {0,-12} {1,-22} {2}" -f $h.state, $h.service, $h.reason)
}

# -------------------------------------------------------------- 3. Kafka
Heading "3. the record on $Topic"
# The console consumer exits non-zero when --timeout-ms expires with no further
# messages, which is exactly how we stop it. In Windows PowerShell that stderr
# becomes a terminating NativeCommandError, so relax the preference here.
$lines = @()
& {
    $ErrorActionPreference = "Continue"
    $lines = docker exec hub-kafka /opt/kafka/bin/kafka-console-consumer.sh `
        --bootstrap-server localhost:9092 --topic $Topic --from-beginning --timeout-ms 12000 `
        --property print.key=true --property print.partition=true --property print.offset=true `
        --property key.separator=" | " 2>$null
    $script:kafkaLines = $lines
}
$match = $script:kafkaLines | Select-String -SimpleMatch $uetr | Select-Object -First 1
if ($match) {
    Write-Host "  $($match.Line.Substring(0, [Math]::Min(120, $match.Line.Length)))"
    Write-Host "  (the value is Protobuf, so it reads as binary with the XML legible inside)"
} else {
    Write-Host "  nothing found on $Topic for this UETR" -ForegroundColor Yellow
}

Heading "4. which topics this payment touched"
& {
    $ErrorActionPreference = "Continue"
    $script:offsets = docker exec hub-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092
}
$script:offsets | Where-Object { $_ -match '^hub\.' -and $_ -notmatch ':0$' } | Sort-Object |
    ForEach-Object { Write-Host "    $_" }

# ----------------------------------------------------------- 5. Postgres
Heading "5. the system of record"
& {
    $ErrorActionPreference = "Continue"
    $script:rows = docker exec -e PGPASSWORD=hub hub-postgres psql -U hub -d payments -t -c `
        "SELECT state, service, reason FROM payment_audit WHERE uetr = '$uetr' ORDER BY emitted_ns;"
}
$script:rows | Where-Object { $_.Trim() } | ForEach-Object { Write-Host "  $($_.Trim())" }

# ------------------------------------------------------- 6. Elasticsearch
if (-not $SkipElastic) {
    Heading "6. Elasticsearch"
    # The index template sets refresh_interval: 10s, so a fresh payment is not
    # searchable the instant it completes.
    Write-Host "  waiting for the index to refresh..."
    Start-Sleep -Seconds 12
    $body = "{`"query`":{`"term`":{`"payment.uetr`":`"$uetr`"}}}"
    $count = (Invoke-RestMethod -Method Post -ContentType "application/json" `
        -Uri "http://localhost:9200/logs-payments-*/_count" -Body $body).count
    Write-Host "  $count events for this payment"

    $body = "{`"query`":{`"term`":{`"payment.uetr`":`"$uetr`"}},`"sort`":[{`"@timestamp`":`"asc`"}],`"size`":200}"
    $hits = (Invoke-RestMethod -Method Post -ContentType "application/json" `
        -Uri "http://localhost:9200/logs-payments-*/_search" -Body $body).hits.hits._source
    foreach ($h in $hits) {
        Write-Host ("    {0,-20} {1,-20} {2}" -f $h.service.name, $h.event.action, $h.message)
    }
}

# ------------------------------------------------------------- 7. Kibana
Heading "7. the same thing in Kibana"
$h = @{ "kbn-xsrf" = "true"; "Content-Type" = "application/json" }
try {
    $views = Invoke-RestMethod -Uri "http://localhost:5601/api/data_views" -Headers $h
    $dv = ($views.data_view | Where-Object { $_.title -eq "logs-payments-*" } | Select-Object -First 1).id
} catch { $dv = $null }
if (-not $dv) {
    $body = '{"data_view":{"title":"logs-payments-*","name":"Payment tracking","timeFieldName":"@timestamp"}}'
    $dv = (Invoke-RestMethod -Method Post -Uri "http://localhost:5601/api/data_views/data_view" -Headers $h -Body $body).data_view.id
    Write-Host "  created the 'logs-payments-*' data view"
}
$q = [uri]::EscapeDataString("payment.uetr : `"$uetr`"")
$url = "http://localhost:5601/app/discover#/?_g=(time:(from:now-1h,to:now))&_a=(index:'$dv',query:(language:kuery,query:'$q'),sort:!(!('@timestamp',asc)),columns:!(service.name,event.action,message))"
Write-Host "  $url" -ForegroundColor Green
Write-Host "`nUETR: $uetr`n"
