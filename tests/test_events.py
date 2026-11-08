
import pytest

from src.schemas.events import (
    CommentEvent,
    EventValidationError,
    Language,
)


def hespress_event():
    """Exemple d'événement Hespress sans date individuelle."""

    return {
        "event_id": "hespress:v1:politique:38357",
        "comment_text": "Exemple de commentaire",
        "source": "hespress",
        "language": "inconnu",
        "event_time": None,
        "article_date_raw": "date originale de l'article",
        "replay_time": "2026-11-01T21:00:00Z",
        "ingestion_time": None,
    }


def test_hespress_event_accepts_unknown_event_time():
    event = CommentEvent.from_mapping(hespress_event())

    assert event.event_time is None
    assert event.ingestion_time is None
    assert event.language == Language.UNKNOWN
    assert event.source == "hespress"


def test_replay_time_is_required():
    payload = hespress_event()
    payload.pop("replay_time")

    with pytest.raises(EventValidationError):
        CommentEvent.from_mapping(payload)


def test_replay_time_requires_timezone():
    payload = hespress_event()
    payload["replay_time"] = "2026-11-01T21:00:00"

    with pytest.raises(EventValidationError):
        CommentEvent.from_mapping(payload)


def test_invalid_event_time_is_rejected():
    payload = hespress_event()
    payload["event_time"] = "not-a-date"

    with pytest.raises(EventValidationError):
        CommentEvent.from_mapping(payload)


def test_event_serialization_round_trip():
    original = hespress_event()

    event = CommentEvent.from_mapping(original)
    serialized = event.to_mapping()
    restored = CommentEvent.from_mapping(serialized)

    assert restored == event
    assert serialized["event_time"] is None
    assert serialized["ingestion_time"] is None
    assert serialized["replay_time"] == "2026-11-01T21:00:00Z"


def test_ingestion_time_accepts_valid_datetime():
    payload = hespress_event()
    payload["ingestion_time"] = "2026-11-01T21:05:00+01:00"

    event = CommentEvent.from_mapping(payload)

    assert event.ingestion_time.isoformat() == "2026-11-01T20:05:00+00:00"


def test_invalid_language_is_rejected():
    payload = hespress_event()
    payload["language"] = "espagnol"

    with pytest.raises(EventValidationError):
        CommentEvent.from_mapping(payload)
