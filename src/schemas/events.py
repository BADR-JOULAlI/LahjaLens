"""Contrats et validation des événements de commentaires LahjaLens.

Ce module ne dépend que de la bibliothèque standard Python afin de rester
utilisable avant l'installation des dépendances Spark et Kafka.

Les dates inconnues valent None (null en JSON). La date d'un article et l'heure
du replay ne remplacent jamais la date de publication du commentaire.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Mapping


class Language(StrEnum):
    """Langues admises par le contrat d'événement."""

    DARIJA = "darija"
    ARABIC = "arabe"
    FRENCH = "français"
    MIXED = "mixte"
    UNKNOWN = "inconnu"


class Sentiment(StrEnum):
    """Labels de sentiment admis pour un corpus annoté."""

    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    POSITIVE = "positive"


class EventValidationError(ValueError):
    """Erreur levée lorsqu'un événement ne respecte pas son contrat."""


def _require_text(payload: Mapping[str, Any], field_name: str) -> str:
    """Retourne un champ texte obligatoire après validation."""

    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise EventValidationError(
            f"Le champ '{field_name}' doit être une chaîne non vide."
        )
    return value.strip()


def _optional_text(
    payload: Mapping[str, Any], field_name: str, *, preserve_whitespace: bool = False
) -> str | None:
    """Accepte un champ absent/null, ou une chaîne non vide."""

    if payload.get(field_name) is None:
        return None
    normalized = _require_text(payload, field_name)
    return payload[field_name] if preserve_whitespace else normalized


def _validate_datetime(value: Any, field_name: str) -> None:
    """Exige une date avec un fuseau donnant un décalage UTC effectif."""

    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise EventValidationError(
            f"Le champ '{field_name}' doit être un datetime avec fuseau."
        )


def _parse_datetime(value: Any, field_name: str) -> datetime:
    """Convertit une date ISO-8601 en datetime UTC."""

    if not isinstance(value, str) or not value.strip():
        raise EventValidationError(
            f"Le champ '{field_name}' doit être une date ISO-8601."
        )

    normalized_value = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized_value)
    except ValueError as error:
        raise EventValidationError(
            f"Le champ '{field_name}' contient une date ISO-8601 invalide."
        ) from error

    _validate_datetime(parsed, field_name)
    return parsed.astimezone(timezone.utc)


def _parse_optional_datetime(value: Any, field_name: str) -> datetime | None:
    """La valeur null indique une date réellement inconnue ou non applicable."""

    return None if value is None else _parse_datetime(value, field_name)


def _datetime_to_json(value: datetime | None) -> str | None:
    """Sérialise une date connue en UTC, avec le suffixe Z."""

    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_language(value: Any) -> Language:
    """Convertit une valeur en langue du contrat."""

    if not isinstance(value, str):
        raise EventValidationError("Le champ 'language' doit être une chaîne.")

    try:
        return Language(value.strip().lower())
    except ValueError as error:
        allowed_values = ", ".join(language.value for language in Language)
        raise EventValidationError(
            f"Langue invalide : {value!r}. Valeurs admises : {allowed_values}."
        ) from error


@dataclass(frozen=True, slots=True)
class CommentEvent:
    """Commentaire et contexte source, sans prédiction ni label inventé.

    event_time est la date du commentaire lorsqu'elle est connue. Les champs
    article_* décrivent uniquement l'article associé. article_date_raw conserve
    une date source non encore interprétée ; article_published_at ne se remplit
    qu'après conversion fiable en une date avec fuseau.

    ingestion_time est obligatoire. replay_time est facultatif, à renseigner
    lors d'un replay historique. Tous les datetimes se sérialisent en UTC.
    """

    event_id: str
    comment_text: str
    source: str
    language: Language
    event_time: datetime | None
    ingestion_time: datetime
    article_id: str | None = None
    article_title: str | None = None
    article_topic: str | None = None
    article_date_raw: str | None = None
    article_published_at: datetime | None = None
    replay_time: datetime | None = None

    def __post_init__(self) -> None:
        """Vérifie les invariants de l'événement après sa construction."""

        for field_name in ("event_id", "comment_text", "source"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise EventValidationError(
                    f"Le champ '{field_name}' doit être une chaîne non vide."
                )

        if not isinstance(self.language, Language):
            raise EventValidationError("Le champ 'language' est invalide.")

        _validate_datetime(self.ingestion_time, "ingestion_time")

        for field_name in ("event_time", "article_published_at", "replay_time"):
            value = getattr(self, field_name)
            if value is not None:
                _validate_datetime(value, field_name)

        for field_name in (
            "article_id", "article_title", "article_topic", "article_date_raw"
        ):
            value = getattr(self, field_name)
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise EventValidationError(
                    f"Le champ '{field_name}' doit être une chaîne non vide ou null."
                )

        if self.article_id is None and any(
            value is not None
            for value in (
                self.article_title, self.article_topic,
                self.article_date_raw, self.article_published_at,
            )
        ):
            raise EventValidationError(
                "Le champ 'article_id' est obligatoire pour rattacher "
                "les métadonnées à leur article."
            )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> CommentEvent:
        """Construit un événement à partir d'un mapping JSON-compatible."""

        if not isinstance(payload, Mapping):
            raise EventValidationError("L'événement doit être un objet JSON.")

        return cls(
            event_id=_require_text(payload, "event_id"),
            comment_text=_require_text(payload, "comment_text"),
            source=_require_text(payload, "source"),
            language=_parse_language(payload.get("language")),
            event_time=_parse_optional_datetime(
                payload.get("event_time"), "event_time"
            ),
            ingestion_time=_parse_datetime(
                payload.get("ingestion_time"), "ingestion_time"
            ),
            article_id=_optional_text(payload, "article_id"),
            article_title=_optional_text(payload, "article_title"),
            article_topic=_optional_text(payload, "article_topic"),
            article_date_raw=_optional_text(
                payload, "article_date_raw", preserve_whitespace=True
            ),
            article_published_at=_parse_optional_datetime(
                payload.get("article_published_at"), "article_published_at"
            ),
            replay_time=_parse_optional_datetime(
                payload.get("replay_time"), "replay_time"
            ),
        )

    def to_mapping(self) -> dict[str, str | None]:
        """Sérialise l'événement dans le format du contrat JSON."""

        return {
            "event_id": self.event_id,
            "comment_text": self.comment_text,
            "source": self.source,
            "language": self.language.value,
            "event_time": _datetime_to_json(self.event_time),
            "ingestion_time": _datetime_to_json(self.ingestion_time),
            "article_id": self.article_id,
            "article_title": self.article_title,
            "article_topic": self.article_topic,
            "article_date_raw": self.article_date_raw,
            "article_published_at": _datetime_to_json(self.article_published_at),
            "replay_time": _datetime_to_json(self.replay_time),
        }


@dataclass(frozen=True, slots=True)
class LabeledComment:
    """Commentaire annoté utilisé pour l'entraînement du modèle."""

    comment_text: str
    sentiment: Sentiment
    label_source: str
    language: Language

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> LabeledComment:
        """Construit un commentaire annoté à partir d'un mapping."""

        if not isinstance(payload, Mapping):
            raise EventValidationError("Le commentaire annoté doit être un objet.")

        raw_sentiment = payload.get("sentiment")
        if not isinstance(raw_sentiment, str):
            raise EventValidationError(
                "Le champ 'sentiment' doit être une chaîne."
            )

        try:
            sentiment = Sentiment(raw_sentiment.strip().lower())
        except ValueError as error:
            allowed_values = ", ".join(label.value for label in Sentiment)
            raise EventValidationError(
                f"Sentiment invalide : {raw_sentiment!r}. "
                f"Valeurs admises : {allowed_values}."
            ) from error

        return cls(
            comment_text=_require_text(payload, "comment_text"),
            sentiment=sentiment,
            label_source=_require_text(payload, "label_source"),
            language=_parse_language(payload.get("language")),
        )

    def to_mapping(self) -> dict[str, str]:
        """Sérialise le commentaire annoté dans le contrat JSON."""

        return {
            "comment_text": self.comment_text,
            "sentiment": self.sentiment.value,
            "label_source": self.label_source,
            "language": self.language.value,
        }
