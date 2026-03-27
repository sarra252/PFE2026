$ErrorActionPreference = 'Stop'
$baseDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$configPath = Join-Path $baseDir 'tpt.local.config.ps1'
$loaderPath = Join-Path $baseDir 'run_all_tpt_simple.ps1'
if (-not (Test-Path $configPath)) {
  throw "Config introuvable: $configPath"
}
if (-not (Test-Path $loaderPath)) {
  throw "Script introuvable: $loaderPath"
}
$cfg = & $configPath
foreach ($k in @('TdpId','UserName','UserPassword','TargetDatabase','SourceDir')) {
  if (-not $cfg.ContainsKey($k) -or [string]::IsNullOrWhiteSpace($cfg[$k])) {
    throw "Parametre manquant dans tpt.local.config.ps1: $k"
  }
}
& $loaderPath `
  -TdpId $cfg['TdpId'] `
  -UserPassword $cfg['UserPassword'] `
  -UserName $cfg['UserName'] `
  -TargetDatabase $cfg['TargetDatabase'] `
  -SourceDir $cfg['SourceDir']
