"""Charge une sortie Gold Parquet versionnée dans PostgreSQL.

Le DataFrame Gold est lu par Spark depuis HDFS. Les lignes sont ensuite envoyées
au pilote par lots contrôlés pour un premier MVP, puis écrites dans PostgreSQL
dans une transaction avec des upserts idempotents. Le modèle n'est jamais
réentraîné et la sortie HDFS reste en lecture seule.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from dotenv import load_dotenv


REQUIRED_COLUMNS = {
    "event_id", "comment_text", "source", "article_id", "article_topic",
    "sentiment", "prediction", "prob_negative", "prob_neutral",
    "prob_positive", "model_version", "preprocessing_version",
    "predicted_at", "inference_run_id",
}
ALLOWED_SENTIMENTS = {"negative", "neutral", "positive"}


@dataclass(frozen=True)
class WarehouseConfig:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    spark_master: str
    batch_size: int

    @classmethod
    def from_environment(cls) -> "WarehouseConfig":
        load_dotenv()
        return cls(
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5434")),
            postgres_db=os.getenv("POSTGRES_DB", "lahjalens_db"),
            postgres_user=os.getenv("POSTGRES_USER", "lahja_user"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "change_me"),
            spark_master=os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
            batch_size=int(os.getenv("WAREHOUSE_BATCH_SIZE", "500")),
        )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-path", required=True, help="Chemin Parquet Gold HDFS")
    parser.add_argument(
        "--schema-path",
        default=str(Path(__file__).with_name("schema.sql")),
        help="Fichier SQL d'initialisation idempotente",
    )
    parser.add_argument("--batch-size", type=int, default=None)
    return parser.parse_args(argv)


def _require_columns(columns: Iterable[str]) -> None:
    missing = REQUIRED_COLUMNS.difference(columns)
    if missing:
        raise ValueError(f"Colonnes Gold manquantes : {', '.join(sorted(missing))}")


def _as_float(value: Any, field: str, event_id: str) -> float:
    if value is None:
        raise ValueError(f"{field} null pour {event_id}")
    result = float(value)
    if not 0.0 <= result <= 1.0:
        raise ValueError(f"{field} hors intervalle pour {event_id}: {result}")
    return result


def validate_rows(rows: Sequence[Mapping[str, Any]]) -> None:
    """Valide un lot avant son écriture, sans modifier les données source."""

    seen: set[str] = set()
    for row in rows:
        event_id = str(row.get("event_id") or "").strip()
        if not event_id:
            raise ValueError("event_id absent dans Gold")
        if event_id in seen:
            raise ValueError(f"event_id dupliqué dans Gold : {event_id}")
        seen.add(event_id)
        if not str(row.get("comment_text") or "").strip():
            raise ValueError(f"comment_text vide pour {event_id}")
        if str(row.get("sentiment") or "") not in ALLOWED_SENTIMENTS:
            raise ValueError(f"sentiment invalide pour {event_id}")
        prediction = int(row.get("prediction"))
        if prediction not in (0, 1, 2):
            raise ValueError(f"prediction invalide pour {event_id}: {prediction}")
        _as_float(row.get("prob_negative"), "prob_negative", event_id)
        _as_float(row.get("prob_neutral"), "prob_neutral", event_id)
        _as_float(row.get("prob_positive"), "prob_positive", event_id)
        probability_sum = sum(
            _as_float(row.get(field), field, event_id)
            for field in ("prob_negative", "prob_neutral", "prob_positive")
        )
        if abs(probability_sum - 1.0) > 1e-5:
            raise ValueError(f"Somme des probabilités invalide pour {event_id}")


def _row_value(row: Mapping[str, Any], key: str) -> Any:
    value = row.get(key)
    return value.isoformat() if hasattr(value, "isoformat") else value


def _connect(config: WarehouseConfig):
    import psycopg2

    return psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )


def _execute_schema(connection: Any, schema_path: str) -> None:
    sql = Path(schema_path).read_text(encoding="utf-8")
    with connection.cursor() as cursor:
        cursor.execute(sql)


def _mark_run(
    connection: Any,
    *,
    run_id: str,
    model_version: str,
    preprocessing_version: str,
    source: str,
    gold_path: str,
    started_at: datetime,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO lahjalens.inference_runs
                (run_id, model_version, preprocessing_version, source, gold_path,
                 status, labels_available, started_at)
            VALUES (%s, %s, %s, %s, %s, 'running', FALSE, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                model_version = EXCLUDED.model_version,
                preprocessing_version = EXCLUDED.preprocessing_version,
                source = EXCLUDED.source,
                gold_path = EXCLUDED.gold_path,
                status = 'running',
                labels_available = FALSE,
                started_at = EXCLUDED.started_at,
                completed_at = NULL,
                last_error = NULL
            """,
            (run_id, model_version, preprocessing_version, source, gold_path, started_at),
        )


def _write_batch(connection: Any, rows: Sequence[Mapping[str, Any]]) -> None:
    from psycopg2.extras import execute_values

    articles = {}
    comments = []
    predictions = []
    for row in rows:
        article_id = row.get("article_id")
        if article_id:
            articles[str(article_id)] = (
                str(article_id), row.get("article_topic"), row.get("article_title"),
                row.get("article_date_raw"),
            )
        event_id = str(row["event_id"])
        comments.append(
            (
                event_id, event_id, row.get("article_id"), row.get("source"),
                row.get("article_topic"), row.get("comment_text"), row.get("source_file"),
                row.get("source_record"), row.get("article_title"), row.get("article_date_raw"),
                _row_value(row, "event_time"), row.get("language"), row.get("source_version"),
                row.get("label_status"),
            )
        )
        predictions.append(
            (
                event_id, event_id, row["inference_run_id"], row["sentiment"],
                int(row["prediction"]), float(row["prob_negative"]), float(row["prob_neutral"]),
                float(row["prob_positive"]), bool(row.get("probabilities_calibrated", False)),
                row["model_version"], row["preprocessing_version"], row.get("model_label_order", "negative,neutral,positive"),
                row["predicted_at"],
            )
        )
    with connection.cursor() as cursor:
        if articles:
            execute_values(
                cursor,
                """
                INSERT INTO lahjalens.articles
                    (article_id, article_topic, article_title, article_date_raw)
                VALUES %s
                ON CONFLICT (article_id) DO UPDATE SET
                    article_topic = EXCLUDED.article_topic,
                    article_title = EXCLUDED.article_title,
                    article_date_raw = EXCLUDED.article_date_raw,
                    updated_at = CURRENT_TIMESTAMP
                """,
                list(articles.values()),
            )
        execute_values(
            cursor,
            """
            INSERT INTO lahjalens.comments
                (comment_id, event_id, article_id, source, article_topic, comment_text,
                 source_file, source_record, article_title, article_date_raw, event_time,
                 language, source_version, label_status)
            VALUES %s
            ON CONFLICT (comment_id) DO UPDATE SET
                event_id = EXCLUDED.event_id,
                article_id = EXCLUDED.article_id,
                source = EXCLUDED.source,
                article_topic = EXCLUDED.article_topic,
                comment_text = EXCLUDED.comment_text,
                source_file = EXCLUDED.source_file,
                source_record = EXCLUDED.source_record,
                article_title = EXCLUDED.article_title,
                article_date_raw = EXCLUDED.article_date_raw,
                event_time = EXCLUDED.event_time,
                language = EXCLUDED.language,
                source_version = EXCLUDED.source_version,
                label_status = EXCLUDED.label_status,
                updated_at = CURRENT_TIMESTAMP
            """,
            comments,
        )
        execute_values(
            cursor,
            """
            INSERT INTO lahjalens.sentiment_predictions
                (event_id, comment_id, inference_run_id, predicted_sentiment,
                 prediction_index, prob_negative, prob_neutral, prob_positive,
                 probabilities_calibrated, model_version, preprocessing_version,
                 model_label_order, predicted_at)
            VALUES %s
            ON CONFLICT (event_id, inference_run_id, model_version) DO UPDATE SET
                comment_id = EXCLUDED.comment_id,
                predicted_sentiment = EXCLUDED.predicted_sentiment,
                prediction_index = EXCLUDED.prediction_index,
                prob_negative = EXCLUDED.prob_negative,
                prob_neutral = EXCLUDED.prob_neutral,
                prob_positive = EXCLUDED.prob_positive,
                probabilities_calibrated = EXCLUDED.probabilities_calibrated,
                preprocessing_version = EXCLUDED.preprocessing_version,
                model_label_order = EXCLUDED.model_label_order,
                predicted_at = EXCLUDED.predicted_at,
                loaded_at = CURRENT_TIMESTAMP
            """,
            predictions,
        )


def _finish_run(connection: Any, run_id: str, row_count: int) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE lahjalens.inference_runs
            SET row_count = %s, status = 'completed', completed_at = CURRENT_TIMESTAMP,
                last_error = NULL
            WHERE run_id = %s
            """,
            (row_count, run_id),
        )


def _fail_run(connection: Any, run_id: str, error: Exception) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE lahjalens.inference_runs
            SET status = 'failed', completed_at = CURRENT_TIMESTAMP, last_error = %s
            WHERE run_id = %s
            """,
            (f"{type(error).__name__}: {error}"[:4000], run_id),
        )


def load_gold(gold_path: str, schema_path: str, config: WarehouseConfig) -> dict[str, Any]:
    """Exécute un chargement Gold contrôlé et retourne un résumé vérifiable."""

    from pyspark.sql import SparkSession

    if config.batch_size < 1:
        raise ValueError("batch_size doit être positif")
    driver_host = os.getenv(
        "SPARK_DRIVER_HOST",
        "localhost" if "localhost" in config.spark_master else "warehouse-loader",
    )
    spark = (
        SparkSession.builder.appName("LahjaLens-Gold-To-PostgreSQL")
        .master(config.spark_master)
        .config("spark.driver.host", driver_host)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    connection = None
    run_id: str | None = None
    started_at = datetime.now(timezone.utc)
    try:
        gold = spark.read.parquet(gold_path)
        _require_columns(gold.columns)
        row_count = gold.count()
        if row_count == 0:
            raise ValueError("Gold est vide")
        duplicate_count = gold.groupBy("event_id", "inference_run_id", "model_version").count().where("count > 1").count()
        if duplicate_count:
            raise ValueError(f"Doublons Gold détectés : {duplicate_count}")
        first = gold.limit(1).collect()[0].asDict(recursive=True)
        run_id = str(first["inference_run_id"])
        model_version = str(first["model_version"])
        preprocessing_version = str(first["preprocessing_version"])
        source = str(first.get("source") or "unknown")
        metadata = gold.select("inference_run_id", "model_version", "preprocessing_version").distinct().limit(2).collect()
        if len(metadata) != 1:
            raise ValueError("Gold doit contenir un seul run, modèle et preprocessing")
        connection = _connect(config)
        _execute_schema(connection, schema_path)
        _mark_run(
            connection, run_id=run_id, model_version=model_version,
            preprocessing_version=preprocessing_version, source=source,
            gold_path=gold_path, started_at=started_at,
        )
        connection.commit()

        optional_columns = {
            "source_file", "source_record", "article_title", "article_date_raw",
            "event_time", "language", "source_version", "label_status",
            "probabilities_calibrated", "model_label_order",
        }.intersection(gold.columns)
        selected = gold.select(*sorted(REQUIRED_COLUMNS | optional_columns)).toLocalIterator()
        batch: list[Mapping[str, Any]] = []
        processed = 0
        for row in selected:
            batch.append(row.asDict(recursive=True))
            if len(batch) >= config.batch_size:
                validate_rows(batch)
                _write_batch(connection, batch)
                processed += len(batch)
                batch = []
        if batch:
            validate_rows(batch)
            _write_batch(connection, batch)
            processed += len(batch)
        if processed != row_count:
            raise ValueError(f"Lignes traitées incohérentes : {processed} != {row_count}")
        _finish_run(connection, run_id, processed)
        connection.commit()
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM lahjalens.sentiment_predictions WHERE inference_run_id = %s",
                (run_id,),
            )
            persisted_count = int(cursor.fetchone()[0])
        if persisted_count != row_count:
            raise ValueError(f"Lignes PostgreSQL incohérentes : {persisted_count} != {row_count}")
        return {
            "status": "completed", "run_id": run_id, "gold_path": gold_path,
            "gold_count": row_count, "persisted_predictions": persisted_count,
            "batch_size": config.batch_size,
        }
    except Exception as error:
        if connection is not None:
            connection.rollback()
            if run_id is not None:
                try:
                    _fail_run(connection, run_id, error)
                    connection.commit()
                except Exception:
                    connection.rollback()
        raise
    finally:
        if connection is not None:
            connection.close()
        spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    config = WarehouseConfig.from_environment()
    if args.batch_size is not None:
        config = WarehouseConfig(**{**config.__dict__, "batch_size": args.batch_size})
    result = load_gold(args.gold_path, args.schema_path, config)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
