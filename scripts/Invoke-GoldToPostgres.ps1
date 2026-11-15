param(
    [Parameter(Mandatory = $true)]
    [string]$GoldPath,
    [int]$BatchSize = 500
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
if ($dockerCommand) {
    $dockerExe = $dockerCommand.Source
} else {
    $dockerExe = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'
    if (-not (Test-Path -LiteralPath $dockerExe)) {
        throw 'Docker CLI not found. Add Docker Desktop resources/bin to PATH.'
    }
}

Push-Location $projectRoot
try {
    & $dockerExe compose up -d postgres spark-master spark-worker-1 spark-worker-2
    if ($LASTEXITCODE -ne 0) { throw "Impossible de démarrer les services ($LASTEXITCODE)." }
    & $dockerExe compose run --rm warehouse-loader python3 -m src.warehouse.load_gold `
        --gold-path $GoldPath --batch-size $BatchSize
    if ($LASTEXITCODE -ne 0) { throw "Chargement Gold vers PostgreSQL échoué ($LASTEXITCODE)." }
} finally {
    Pop-Location
}
