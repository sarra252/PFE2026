param(
    [string]$RemoteName = "teamstorage",
    [string]$RemoteUrl = ""
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command dvc -ErrorAction SilentlyContinue)) {
    throw "DVC n'est pas installe. Installe-le d'abord avec: pip install dvc"
}

if (-not (Test-Path ".dvc")) {
    dvc init
}

dvc add data_synth/raw
dvc add data_synth/metadata

if ($RemoteUrl) {
    $existingRemotes = dvc remote list
    if ($existingRemotes -notmatch "^$RemoteName\s") {
        dvc remote add $RemoteName $RemoteUrl
    }
    dvc remote default $RemoteName
}

Write-Host "DVC est pret."
Write-Host "Fichiers generes:"
Write-Host "- data_synth/raw.dvc"
Write-Host "- data_synth/metadata.dvc"
Write-Host ""
Write-Host "Etapes suivantes:"
Write-Host "1. git add .dvc .dvcignore data_synth/raw.dvc data_synth/metadata.dvc"
Write-Host "2. git commit -m 'Setup DVC tracking'"
if ($RemoteUrl) {
    Write-Host "3. dvc push"
}
