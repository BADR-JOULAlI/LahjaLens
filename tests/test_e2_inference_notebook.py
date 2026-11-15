"""Static contract checks for the notebook-only E2 inference job."""

import ast
import json
from pathlib import Path


NOTEBOOK = Path(__file__).parents[1] / "notebooks" / "08_silver_e2_gold_inference.ipynb"


def _sources() -> str:
    notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    code = ["".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"]
    for source in code:
        ast.parse(source)
    return "\n".join(code)


def test_notebook_is_valid_python_and_json() -> None:
    assert NOTEBOOK.exists()
    _sources()


def test_inference_loads_e2_without_training_or_overwrite() -> None:
    source = _sources()
    assert "PipelineModel.load" in source
    assert ".fit(" not in source
    assert "LogisticRegression(" not in source
    assert "overwrite" not in source.lower()
    assert "mode('errorifexists')" in source


def test_inference_contract_preserves_labels_and_gold_fields() -> None:
    source = _sources()
    assert "LABELS=['negative','neutral','positive']" in source
    for field in (
        "event_id",
        "comment_text",
        "sentiment",
        "model_version",
        "preprocessing_version",
        "predicted_at",
        "prob_negative",
        "prob_neutral",
        "prob_positive",
    ):
        assert field in source
    assert "probabilities_calibrated" in source
