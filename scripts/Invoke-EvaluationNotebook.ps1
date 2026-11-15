param(
    [string]$PythonExe = ''
)

# Evaluation only. This runner never executes historical training notebooks.
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$notebookPath = Join-Path $projectRoot 'notebooks\06_sentiment_robust_evaluation.ipynb'
if (-not (Test-Path -LiteralPath $notebookPath)) {
    throw 'Notebook 06 is missing.'
}
if (-not $PythonExe) {
    $PythonExe = Join-Path $projectRoot '.venv\Scripts\python.exe'
}
if (-not (Test-Path -LiteralPath $PythonExe)) {
    $resolvedPython = Get-Command $PythonExe -ErrorAction SilentlyContinue
    if (-not $resolvedPython) {
        throw 'Choose an existing Python interpreter with NumPy and Matplotlib using -PythonExe.'
    }
    $PythonExe = $resolvedPython.Source
}
$PythonExe = (Resolve-Path -LiteralPath $PythonExe).Path

$bootstrap = @'
import ast
import base64
import contextlib
import hashlib
import io
import json
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
path = Path(sys.argv[1]).resolve()
if path.name != "06_sentiment_robust_evaluation.ipynb":
    raise ValueError("Only notebook 06 is allowed.")
os.chdir(path.parent.parent)
notebook = json.loads(path.read_text(encoding="utf-8"))
for cell in notebook["cells"]:
    if cell["cell_type"] == "code":
        tree = ast.parse("".join(cell["source"]))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                name = getattr(node.func, "attr", getattr(node.func, "id", ""))
                if name in ("fit", "fit_transform", "train", "overwrite"):
                    raise ValueError("Training/overwrite call forbidden in evaluation: " + name)

class Tee(io.TextIOBase):
    def __init__(self, destination, capture):
        self.destination, self.capture = destination, capture
    def write(self, value):
        self.destination.write(value)
        self.capture.write(value)
        return len(value)
    def flush(self):
        self.destination.flush()

scope = {"__name__": "__main__"}
executed = 0
status = "failed"
try:
    for index, cell in enumerate(notebook["cells"]):
        if cell["cell_type"] != "code":
            continue
        executed += 1
        cell["execution_count"] = executed
        cell["outputs"] = []
        output = io.StringIO()
        errors = io.StringIO()
        images_before = len(scope.get("NOTEBOOK_IMAGES", []))
        print("EVALUATION_CELL", executed, cell.get("id", index), flush=True)
        try:
            with contextlib.redirect_stdout(Tee(sys.stdout, output)), contextlib.redirect_stderr(Tee(sys.stderr, errors)):
                exec(compile("".join(cell["source"]), str(path) + ":cell_" + str(index), "exec"), scope)
        except Exception as exc:
            cell["outputs"].append({"output_type": "error", "ename": type(exc).__name__,
                                    "evalue": str(exc), "traceback": traceback.format_exc().splitlines()})
            raise
        finally:
            for stream, buffer in (("stdout", output), ("stderr", errors)):
                if buffer.getvalue():
                    cell["outputs"].append({"output_type": "stream", "name": stream,
                                            "text": buffer.getvalue().splitlines(keepends=True)})
            for image_path in scope.get("NOTEBOOK_IMAGES", [])[images_before:]:
                image_path = Path(image_path)
                cell["outputs"].append({"output_type": "display_data", "metadata": {},
                    "data": {"image/png": base64.b64encode(image_path.read_bytes()).decode("ascii"),
                             "text/plain": [image_path.name]}})
    status = "completed"
finally:
    output_dir = scope.get("OUT_DIR")
    if output_dir is not None and Path(output_dir).is_dir():
        executed_path = Path(output_dir) / "06_sentiment_robust_evaluation.executed.ipynb"
        with executed_path.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(notebook, handle, ensure_ascii=False, indent=1, allow_nan=False)
            handle.write("\n")
        record = {"status": status, "code_cells_executed": executed,
                  "source_notebook_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                  "executed_notebook_sha256": hashlib.sha256(executed_path.read_bytes()).hexdigest(),
                  "finished_at": datetime.now(timezone.utc).isoformat()}
        with (Path(output_dir) / "runner_execution.json").open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(record, handle, indent=2)
            handle.write("\n")
        print("EXECUTED_NOTEBOOK", executed_path, flush=True)
'@

$previousEncoding = $OutputEncoding
Push-Location $projectRoot
try {
    $OutputEncoding = New-Object System.Text.UTF8Encoding $false
    $bootstrap | & $PythonExe -u -B - $notebookPath
    if ($LASTEXITCODE -ne 0) {
        throw "Evaluation failed (exit $LASTEXITCODE). Partial outputs were preserved; no model was trained."
    }
} finally {
    $OutputEncoding = $previousEncoding
    Pop-Location
}
