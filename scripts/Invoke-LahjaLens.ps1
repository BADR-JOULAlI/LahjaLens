param(
    [switch]$RunInference,
    [string]$GoldPath = 'hdfs://namenode:9000/data/lahjalens/gold/hespress/e2_inference/20261115T194649Z_17df8866',
    [int]$BatchSize = 500
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    if ($RunInference) {
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\Invoke-SparkNotebook.ps1 `
            -Notebook 'notebooks/08_silver_e2_gold_inference.ipynb'
        if ($LASTEXITCODE -ne 0) { throw "Inférence Spark échouée ($LASTEXITCODE)." }
        $manifest = Get-ChildItem -Path .\data\processed\hespress\e2_inference `
            -Filter run_manifest.json -Recurse -File |
            Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if (-not $manifest) { throw 'Manifeste d''inférence introuvable.' }
        $GoldPath = (Get-Content -LiteralPath $manifest.FullName -Raw | ConvertFrom-Json).gold_path
    }

    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\Invoke-GoldToPostgres.ps1 `
        -GoldPath $GoldPath -BatchSize $BatchSize
    if ($LASTEXITCODE -ne 0) { throw "Chargement PostgreSQL échoué ($LASTEXITCODE)." }

    $dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
    $dockerExe = if ($dockerCommand) { $dockerCommand.Source } `
        else { Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe' }
    & $dockerExe compose up -d dashboard
    if ($LASTEXITCODE -ne 0) { throw "Dashboard non démarré ($LASTEXITCODE)." }
    Write-Output 'LahjaLens disponible sur http://localhost:8501'
} finally {
    Pop-Location
}
