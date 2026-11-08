"""Tests unitaires pour les indicateurs Spark SQL (Silver vers Gold)."""

import os
import sys
import pytest

# Aligner le worker et le driver Spark sur le mǦme interprǸteur Python (3.11)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.processing.silver_to_gold import (
    compute_kpi_by_language,
    compute_kpi_by_source,
    compute_kpi_daily_trends,
    compute_kpi_text_stats,
    compute_kpi_summary,
)


@pytest.fixture(scope="session")
def spark():
    """Initialise une session PySpark locale lǸgre pour les tests."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("Test-Silver-To-Gold")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def setup_silver_view(spark):
    """PrǸpare un jeu de donnǸes de test reprǸsentatif de la couche Silver."""
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("comment_text", StringType(), True),
        StructField("source", StringType(), True),
        StructField("language", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("article_date_raw", StringType(), True),
        StructField("replay_time", StringType(), True),
        StructField("ingestion_time", StringType(), True),
    ])

    sample_data = [
        (
            "id-1",
            "Hadchi zwin bzaf tbarkellah",
            "hespress/societe",
            "darija",
            "2026-11-01T10:00:00Z",
            None,
            "2026-11-01T12:00:00Z",
            "2026-11-01T12:01:00Z",
        ),
        (
            "id-2",
            "Bravo l'equipe nationale",
            "hespress/sport",
            "francais",
            "2026-11-01T11:00:00Z",
            None,
            "2026-11-01T12:00:00Z",
            "2026-11-01T12:01:00Z",
        ),
        (
            "id-3",
            "مقال ممتاز ومفيد جدا شكرا لكم",
            "hespress/politique",
            "arabe",
            "2026-11-02T09:30:00Z",
            None,
            "2026-11-02T10:00:00Z",
            "2026-11-02T10:01:00Z",
        ),
        (
            "id-4",
            "Salam cv merci",
            "hespress/sport",
            "darija",
            None,
            None,
            "2026-11-02T11:00:00Z",
            "2026-11-02T11:01:00Z",
        ),
    ]

    df = spark.createDataFrame(sample_data, schema=schema)
    df.createOrReplaceTempView("silver_comments")


def test_kpi_by_language(spark):
    df = compute_kpi_by_language(spark)
    rows = df.collect()

    assert len(rows) == 3
    # darija has 2 comments (50%), arabe 1 (25%), francais 1 (25%)
    darija_row = next(r for r in rows if r["language"] == "darija")
    assert darija_row["total_comments"] == 2
    assert darija_row["percentage"] == 50.0


def test_kpi_by_source(spark):
    df = compute_kpi_by_source(spark)
    rows = df.collect()

    assert len(rows) == 3
    # hespress/sport has 2 comments
    sport_row = next(r for r in rows if r["source"] == "hespress/sport")
    assert sport_row["total_comments"] == 2
    assert sport_row["percentage"] == 50.0


def test_kpi_daily_trends(spark):
    df = compute_kpi_daily_trends(spark)
    rows = df.collect()

    assert len(rows) >= 2
    # Verify columns exist and are populated
    for row in rows:
        assert row["comment_date"] is not None
        assert row["total_comments"] >= 1


def test_kpi_text_stats(spark):
    df = compute_kpi_text_stats(spark)
    rows = df.collect()

    assert len(rows) == 3
    for row in rows:
        assert row["avg_char_length"] > 0
        assert row["min_char_length"] > 0
        assert row["max_char_length"] >= row["min_char_length"]
        assert row["avg_word_count"] > 0


def test_kpi_summary(spark):
    df = compute_kpi_summary(spark)
    row = df.collect()[0]

    assert row["total_silver_comments"] == 4
    assert row["total_sources"] == 3
    assert row["total_languages"] == 3
