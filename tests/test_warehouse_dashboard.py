from __future__ import annotations

import ast
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_warehouse_schema_contains_idempotent_contract() -> None:
    sql = (ROOT / "src/warehouse/schema.sql").read_text(encoding="utf-8")
    loader = (ROOT / "src/warehouse/load_gold.py").read_text(encoding="utf-8")
    for fragment in (
        "CREATE TABLE IF NOT EXISTS lahjalens.inference_runs",
        "CREATE TABLE IF NOT EXISTS lahjalens.comments",
        "CREATE TABLE IF NOT EXISTS lahjalens.sentiment_predictions",
        "CREATE OR REPLACE VIEW lahjalens.v_latest_predictions",
        "CREATE OR REPLACE VIEW lahjalens.v_data_quality",
    ):
        assert fragment in sql
    assert "ON CONFLICT (event_id, inference_run_id, model_version)" in loader
    assert "DROP TABLE" not in sql.upper()


def test_warehouse_loader_and_dashboard_parse_without_execution() -> None:
    for relative in ("src/warehouse/load_gold.py", "apps/streamlit_app.py"):
        ast.parse((ROOT / relative).read_text(encoding="utf-8"))


def test_gold_validation_rejects_invalid_probability() -> None:
    from src.warehouse.load_gold import validate_rows

    row = {
        "event_id": "e1",
        "comment_text": "commentaire",
        "sentiment": "neutral",
        "prediction": 1,
        "prob_negative": 0.2,
        "prob_neutral": 0.9,
        "prob_positive": 0.2,
    }
    with pytest.raises(ValueError, match="Somme des probabilités"):
        validate_rows([row])


def test_gold_validation_accepts_contract_row() -> None:
    from src.warehouse.load_gold import validate_rows

    validate_rows([
        {
            "event_id": "e1",
            "comment_text": "commentaire",
            "sentiment": "neutral",
            "prediction": 1,
            "prob_negative": 0.2,
            "prob_neutral": 0.6,
            "prob_positive": 0.2,
        }
    ])
