param(
    [string]$Notebook = 'notebooks/05_sentiment_script_aware.ipynb',
    [switch]$AuditOnly
)

# Execute notebook cells on the Docker Spark cluster without exporting .py.
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$notebookPath = (Resolve-Path -LiteralPath (Join-Path $projectRoot $Notebook)).Path
$notebookRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot 'notebooks'))
if (-not $notebookPath.StartsWith($notebookRoot + [IO.Path]::DirectorySeparatorChar,
        [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Choose a notebook inside the project notebooks directory.'
}
$relativeNotebook = $notebookPath.Substring($notebookRoot.Length + 1).Replace('\', '/')
$containerNotebook = '/opt/lahjalens/notebooks/' + $relativeNotebook
$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
if ($dockerCommand) {
    $dockerExe = $dockerCommand.Source
} else {
    $dockerExe = Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'
    if (-not (Test-Path -LiteralPath $dockerExe)) {
        throw 'Docker CLI not found. Add Docker Desktop resources/bin to PATH.'
    }
}
$mode = if ($AuditOnly) { 'audit' } else { 'full' }
$bootstrap = @'
import json
import os
import sys
from pathlib import Path

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--master spark://spark-master:7077 --driver-memory 1g --executor-memory 1g '
    '--conf spark.cores.max=4 --conf spark.executor.cores=2 '
    '--conf spark.executorEnv.PYTHONPATH=/tmp/lahjalens-python '
    '--conf spark.hadoop.dfs.replication=2 pyspark-shell'
)
notebook_path = Path(sys.argv[1])
notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
scope = {"__name__": "__main__"}
try:
    for i, cell in enumerate(notebook["cells"]):
        if cell["cell_type"] != "code":
            continue
        if sys.argv[2] == "audit" and cell.get("id") not in ("config", "audit", "features"):
            continue
        print("NOTEBOOK_CELL", i, cell.get("id", ""), flush=True)
        source = "".join(cell["source"])
        exec(compile(source, str(notebook_path) + ":cell_" + str(i), "exec"), scope)
finally:
    if "spark" in scope:
        scope["spark"].stop()
'@

$previousEncoding = $OutputEncoding
Push-Location $projectRoot
try {
    $OutputEncoding = New-Object System.Text.UTF8Encoding $false
    # PySpark starts its gateway through spark-submit. Notebook code is read
    # on stdin; no temporary Python script or notebook server is needed.
    $bootstrap | & $dockerExe compose exec -T `
        --env PYTHONPATH=/tmp/lahjalens-python:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip `
        --env SPARK_HOME=/opt/spark `
        spark-master python3 - $containerNotebook $mode
    if ($LASTEXITCODE -ne 0) {
        throw "Notebook failed (exit $LASTEXITCODE). Check Docker services and NumPy availability."
    }
} finally {
    $OutputEncoding = $previousEncoding
    Pop-Location
}
