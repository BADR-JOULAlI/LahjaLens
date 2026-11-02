
"""Producteur Kafka pour les commentaires Hespress de LahjaLens."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Permet l'exécution directe : python src/ingestion/producer.py
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.schemas.events import CommentEvent, EventValidationError, Language


DEFAULT_CSV = (
    PROJECT_ROOT / "data" / "processed" / "hespress" / "comments_sample.csv"
)

DEFAULT_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "comments.raw")

logger = logging.getLogger("lahjalens.producer")


def current_utc_time() -> datetime:
    """Retourne l'heure actuelle en UTC."""

    return datetime.now(timezone.utc)


def csv_value_to_optional_text(value: Any) -> str | None:
    """Convertit une valeur CSV vide ou NaN en None."""

    if value is None:
        return None

    normalized = str(value).strip()

    if not normalized or normalized.lower() in {"nan", "none", "null"}:
        return None

    return normalized


def row_to_event(row: dict[str, Any]) -> CommentEvent:
    """Transforme une ligne Hespress en événement normalisé."""

    language_value = csv_value_to_optional_text(row.get("language"))

    if language_value is None:
        language_value = Language.UNKNOWN.value

    payload = {
        "event_id": row.get("sample_id"),
        "comment_text": row.get("comment_text"),
        "source": row.get("source"),
        "language": language_value,
        "event_time": csv_value_to_optional_text(row.get("event_time")),
        "article_date_raw": csv_value_to_optional_text(
            row.get("article_date_raw")
        ),
        "replay_time": current_utc_time().isoformat().replace("+00:00", "Z"),
        "ingestion_time": None,
    }

    return CommentEvent.from_mapping(payload)


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Crée un producteur Kafka configuré pour JSON."""

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )


def publish_csv(
    csv_path: Path,
    bootstrap_servers: str,
    topic: str,
    limit: int | None = None,
) -> int:
    """Publie les événements CSV dans Kafka."""

    if not csv_path.is_file():
        raise FileNotFoundError(f"Fichier CSV introuvable : {csv_path}")

    producer = create_producer(bootstrap_servers)
    published_count = 0

    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)

            required_columns = {
                "sample_id",
                "source",
                "comment_text",
                "language",
                "event_time",
                "article_date_raw",
            }

            missing_columns = required_columns - set(reader.fieldnames or [])

            if missing_columns:
                raise ValueError(
                    f"Colonnes CSV manquantes : {sorted(missing_columns)}"
                )

            for row_number, row in enumerate(reader, start=1):
                if limit is not None and published_count >= limit:
                    break

                try:
                    event = row_to_event(row)
                    event_mapping = event.to_mapping()

                    future = producer.send(
                        topic,
                        key=event.event_id,
                        value=event_mapping,
                    )

                    metadata = future.get(timeout=30)

                    published_count += 1

                    logger.info(
                        "Événement publié : id=%s partition=%s offset=%s",
                        event.event_id,
                        metadata.partition,
                        metadata.offset,
                    )

                except (EventValidationError, KafkaError, ValueError) as error:
                    logger.error(
                        "Ligne CSV %s ignorée : %s",
                        row_number,
                        error,
                    )
                    raise

        producer.flush()
        logger.info(
            "Publication terminée : %s événement(s).",
            published_count,
        )

        return published_count

    finally:
        producer.close()


def main() -> None:
    """Point d'entrée du producteur."""

    parser = argparse.ArgumentParser(
        description="Publier les commentaires Hespress dans Kafka."
    )

    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help="Chemin du fichier CSV.",
    )

    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Topic Kafka cible.",
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help="Adresse du broker Kafka.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Nombre maximum d'événements à publier.",
    )

    args = parser.parse_args()

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit doit être strictement positif.")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    try:
        count = publish_csv(
            csv_path=args.csv,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            limit=args.limit,
        )
        logger.info("Total publié : %s", count)

    except (FileNotFoundError, ValueError, KafkaError) as error:
        logger.error("Échec du producteur : %s", error)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
