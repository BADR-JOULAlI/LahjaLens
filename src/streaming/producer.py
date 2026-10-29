"""Replay de commentaires Hespress préparés vers Kafka.

Depuis la racine : python -m src.streaming.producer --dry-run
Envoi réel : python -m src.streaming.producer --limit 100 --delay 0.2
Le mode dry-run utilise uniquement la bibliothèque standard et le contrat local.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sys
import time
from collections import Counter, deque
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, Sequence

from src.schemas.events import CommentEvent, EventValidationError


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = ROOT / "data/processed/hespress/comments_sample.csv"
CATEGORIES = ("politique", "economie", "societe", "sport", "art-et-culture")
REQUIRED_COLUMNS = {
    "sample_id", "source", "comment_text", "language", "event_time",
    "article_id", "article_title", "article_topic", "article_date_raw",
}


class SendFuture(Protocol):
    def get(self, timeout: float) -> Any: ...


class Producer(Protocol):
    def send(self, topic: str, *, key: bytes, value: bytes) -> SendFuture: ...
    def close(self, timeout: float) -> None: ...


class DeliveryError(RuntimeError):
    """Nombre de confirmations reçues et identifiant de l'envoi en échec."""

    def __init__(self, confirmed: int, event_id: str, error: Exception):
        self.confirmed = confirmed
        self.event_id = event_id
        super().__init__(
            f"Envoi interrompu pour {event_id} : {type(error).__name__}: {error}. "
            f"{confirmed} message(s) confirmé(s). L'envoi en échec peut avoir "
            "atteint Kafka : vérifier le topic avant de relancer."
        )


def load_events(
    path: Path, limit: int = 100, categories: Sequence[str] = CATEGORIES,
) -> list[CommentEvent]:
    """Valide tout le CSV, puis sélectionne un lot alternant les catégories.

    Ce lecteur attend l'échantillon préparé (avec le contexte déjà joint).
    Il ne réinterprète pas les colonnes des dix CSV originaux.
    """

    if limit < 1 or not categories or any(c not in CATEGORIES for c in categories):
        raise ValueError("Limite positive et catégories Hespress connues requises.")
    queues: dict[str, deque[CommentEvent]] = {c: deque() for c in dict.fromkeys(categories)}
    ids: set[str] = set()
    ingestion_time = datetime.now(timezone.utc).isoformat()
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, strict=True)
        fields = reader.fieldnames or []
        if len(fields) != len(set(fields)) or not REQUIRED_COLUMNS.issubset(fields):
            raise ValueError(f"Colonnes invalides. Champs requis : {', '.join(sorted(REQUIRED_COLUMNS))}")
        for record, row in enumerate(reader, 1):
            try:
                if None in row or any(value is None for value in row.values()):
                    raise ValueError("Colonnes manquantes ou excédentaires.")
                if row["source"] != "hespress":
                    raise ValueError("Ce lecteur attend source=hespress.")
                if row["article_topic"] not in CATEGORIES:
                    raise ValueError("Catégorie Hespress inconnue.")
                # Ne jamais transformer la date de l'article en date du commentaire.
                # Les labels éventuellement saisis dans le CSV ne sont pas publiés.
                event = CommentEvent.from_mapping({
                    "event_id": row["sample_id"],
                    "comment_text": row["comment_text"],
                    "source": row["source"], "language": row["language"],
                    "event_time": row["event_time"].strip() or None,
                    "ingestion_time": ingestion_time,
                    "article_id": row["article_id"],
                    "article_title": row["article_title"],
                    "article_topic": row["article_topic"],
                    "article_date_raw": row["article_date_raw"],
                })
                if event.event_id in ids:
                    raise ValueError(f"Identifiant dupliqué : {event.event_id}")
                ids.add(event.event_id)
                if event.article_topic in queues:
                    queues[event.article_topic].append(event)
            except (EventValidationError, ValueError) as error:
                raise ValueError(f"{path.name}, enregistrement CSV {record} : {error}") from error

    events = []
    while len(events) < limit and any(queues.values()):
        for queue in queues.values():
            if queue and len(events) < limit:
                events.append(queue.popleft())
    if not events:
        raise ValueError("Aucun commentaire pour les catégories demandées.")
    return events


def create_producer(bootstrap_servers: str) -> Producer:
    """Charge le client Kafka uniquement pour un envoi réel."""

    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
    except ImportError as error:
        raise RuntimeError(
            "Client Kafka absent. Dans votre environnement Python, installer "
            "kafka-python-ng==2.2.3, déjà compatible avec requirements.txt."
        ) from error
    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            client_id="lahjalens-hespress-replay", acks="all", retries=3,
            max_in_flight_requests_per_connection=1,
            api_version_auto_timeout_ms=10000,
            request_timeout_ms=10000, max_block_ms=10000,
        )
    except KafkaError as error:
        raise RuntimeError(
            f"Connexion Kafka impossible ({type(error).__name__}: {error}). "
            "Vérifier Docker Desktop, docker compose up -d kafka et le broker."
        ) from error


def publish_events(
    producer: Producer, events: Sequence[CommentEvent], topic: str,
    *, delay: float = 0, timeout: float = 30,
) -> int:
    """Un message compte comme envoyé après confirmation du broker."""

    confirmed = 0
    for event in events:
        replayed = replace(event, replay_time=datetime.now(timezone.utc))
        value = json.dumps(replayed.to_mapping(), ensure_ascii=False).encode("utf-8")
        try:
            metadata = producer.send(
                topic, key=event.event_id.encode("utf-8"), value=value,
            ).get(timeout=timeout)
        except Exception as error:
            # Périmètre limité à l'appel réseau. L'erreur est exposée, sans faux succès.
            raise DeliveryError(confirmed, event.event_id, error) from error
        confirmed += 1
        print(
            f"CONFIRME {confirmed}/{len(events)} {event.event_id} "
            f"topic={metadata.topic} partition={metadata.partition} offset={metadata.offset}",
            flush=True,
        )
        if delay and confirmed < len(events):
            time.sleep(delay)
    return confirmed


def positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("La valeur doit être positive.")
    return number


def nonnegative_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("Le délai doit être un nombre fini positif ou nul.")
    return number


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="CSV préparé avec le contexte article.")
    parser.add_argument("--limit", type=positive_int, default=100, help="Maximum de messages (défaut : 100).")
    parser.add_argument("--category", choices=CATEGORIES, action="append", help="Filtre répétable ; toutes les catégories par défaut.")
    parser.add_argument("--dry-run", action="store_true", help="Valider et afficher un exemple, sans connexion Kafka.")
    parser.add_argument("--delay", type=nonnegative_float, default=0, help="Secondes entre deux messages confirmés.")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_COMMENTS_RAW", "comments.raw"))
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", args.topic) or args.topic in (".", ".."):
        parser.error("Nom de topic Kafka invalide.")
    servers = [server.strip() for server in args.bootstrap_servers.split(",")]
    if any(not server for server in servers):
        parser.error("Un ou plusieurs brokers Kafka sont vides.")
    producer = None
    try:
        events = load_events(args.input, args.limit, args.category or CATEGORIES)
        counts = dict(Counter(event.article_topic for event in events))
        print(json.dumps({"mode": "dry-run" if args.dry_run else "replay", "selected": len(events), "categories": counts, "topic": args.topic}))
        if args.dry_run:
            print(json.dumps(events[0].to_mapping(), ensure_ascii=False, indent=2))
            print("VALIDATION OK : aucun message envoyé à Kafka.")
            return 0
        producer = create_producer(",".join(servers))
        confirmed = publish_events(producer, events, args.topic, delay=args.delay)
        print(json.dumps({"status": "confirmed", "confirmed": confirmed, "topic": args.topic}))
        return 0
    except (OSError, ValueError, csv.Error, RuntimeError) as error:
        print(f"ECHEC : {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Replay interrompu. Vérifier les messages déjà confirmés avant de relancer.", file=sys.stderr)
        return 130
    finally:
        if producer is not None:
            try:
                producer.close(timeout=5)
            except Exception as error:
                print(f"Fermeture Kafka : {type(error).__name__}: {error}", file=sys.stderr)


if __name__ == "__main__":
    # Les terminaux Windows redirigés peuvent utiliser cp1252 par défaut.
    # Kafka reçoit déjà de l'UTF-8 ; la prévisualisation doit aussi accepter la Darija.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    raise SystemExit(main())
