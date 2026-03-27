param(
  [Parameter(Mandatory = $true)][string]$TdpId,
  [Parameter(Mandatory = $true)][string]$UserName,
  [Parameter(Mandatory = $true)][string]$UserPassword,
  [Parameter(Mandatory = $true)][string]$TargetDatabase,
  [string]$SourceDir = 'E:\PFE2026\data_synth\raw'
)
$ErrorActionPreference = 'Stop'
$tbuild = 'E:\Program Files\Teradata\Client\20.00\bin\tbuild.exe'
if (-not (Test-Path $tbuild)) {
  throw "tbuild.exe introuvable: $tbuild"
}
$baseDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$jobsDir = Join-Path $baseDir 'jobs'
$cfgFile = Join-Path $baseDir 'twbcfg_local.ini'
$logDir = Join-Path $baseDir 'logs'
$ckptDir = Join-Path $baseDir 'checkpoint'
if (-not (Test-Path $cfgFile)) {
  throw "Configuration TPT introuvable: $cfgFile"
}
New-Item -ItemType Directory -Force -Path $logDir, $ckptDir | Out-Null
$tables = @('customers', 'invoices', 'payments', 'plans', 'subscribers', 'usage_events')
foreach ($n in $tables) {
  $job = Join-Path $jobsDir ("load_{0}.tpt" -f $n)
  $csv = Join-Path $SourceDir ("{0}.csv" -f $n)
  if (-not (Test-Path $job)) { throw "Job introuvable: $job" }
  if (-not (Test-Path $csv)) { throw "CSV introuvable: $csv" }
  $vars = "TdpId='$TdpId',UserName='$UserName',UserPassword='$UserPassword',TargetDatabase='$TargetDatabase',SourceFile='$csv'"
  Write-Host "`n=== Chargement $n ===" -ForegroundColor Cyan
  & $tbuild -I $cfgFile -f $job -u $vars -L $logDir -r $ckptDir -j ("load_{0}" -f $n)
  if ($LASTEXITCODE -ne 0) {
    throw "Echec TPT sur $n (code=$LASTEXITCODE)"
  }
}
Write-Host "`nTous les chargements CSV sont termines." -ForegroundColor Green
