
"""Contrats et validation des événements de commentaires LahjaLens.

Ce module ne dépend que de la bibliothèque standard Python.
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


def _parse_datetime(value: Any, field_name: str) -> datetime:
    """Convertit une date ISO-8601 avec fuseau en datetime UTC."""

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

    if parsed.tzinfo is None:
        raise EventValidationError(
            f"Le champ '{field_name}' doit inclure un fuseau horaire."
        )

    return parsed.astimezone(timezone.utc)


def _parse_optional_datetime(
    value: Any, field_name: str
) -> datetime | None:
    """Convertit une date facultative, ou retourne None si elle est absente."""

    if value is None or (isinstance(value, str) and not value.strip()):
        return None

    return _parse_datetime(value, field_name)


def _parse_optional_text(
    value: Any, field_name: str
) -> str | None:
    """Valide un texte facultatif sans interpréter son contenu."""

    if value is None:
        return None

    if not isinstance(value, str):
        raise EventValidationError(
            f"Le champ '{field_name}' doit être une chaîne ou null."
        )

    return value.strip() or None


def _parse_language(value: Any) -> Language:
    """Convertit une valeur en langue du contrat."""

    if not isinstance(value, str):
        raise EventValidationError(
            "Le champ 'language' doit être une chaîne."
        )

    try:
        return Language(value.strip().lower())
    except ValueError as error:
        allowed_values = ", ".join(
            language.value for language in Language
        )
        raise EventValidationError(
            f"Langue invalide : {value!r}. "
            f"Valeurs admises : {allowed_values}."
        ) from error


def _format_datetime(value: datetime | None) -> str | None:
    """Sérialise une date en UTC au format ISO-8601."""

    if value is None:
        return None

    return value.astimezone(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


@dataclass(frozen=True, slots=True)
class CommentEvent:
    """Événement normalisé représentant un commentaire collecté."""

    event_id: str
    comment_text: str
    source: str
    language: Language
    replay_time: datetime
    event_time: datetime | None = None
    article_date_raw: str | None = None
    ingestion_time: datetime | None = None

    def __post_init__(self) -> None:
        """Vérifie les invariants de l'événement."""

        for field_name in ("event_id", "comment_text", "source"):
            value = getattr(self, field_name)

            if not isinstance(value, str) or not value.strip():
                raise EventValidationError(
                    f"Le champ '{field_name}' doit être une chaîne non vide."
                )

        if not isinstance(self.language, Language):
            raise EventValidationError("Le champ 'language' est invalide.")

        # replay_time est obligatoire.
        # event_time et ingestion_time sont facultatifs.
        for field_name in (
            "replay_time",
            "event_time",
            "ingestion_time",
        ):
            value = getattr(self, field_name)

            if value is None and field_name != "replay_time":
                continue

            if not isinstance(value, datetime) or value.tzinfo is None:
                raise EventValidationError(
                    f"Le champ '{field_name}' doit être un datetime "
                    "avec fuseau horaire."
                )

        if self.article_date_raw is not None:
            if (
                not isinstance(self.article_date_raw, str)
                or not self.article_date_raw.strip()
            ):
                raise EventValidationError(
                    "Le champ 'article_date_raw' doit être "
                    "un texte non vide ou None."
                )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> CommentEvent:
        """Construit un événement à partir d'un mapping JSON-compatible."""

        if not isinstance(payload, Mapping):
            raise EventValidationError(
                "L'événement doit être un objet JSON."
            )

        return cls(
            event_id=_require_text(payload, "event_id"),
            comment_text=_require_text(payload, "comment_text"),
            source=_require_text(payload, "source"),
            language=_parse_language(payload.get("language")),
            replay_time=_parse_datetime(
                payload.get("replay_time"), "replay_time"
            ),
            event_time=_parse_optional_datetime(
                payload.get("event_time"), "event_time"
            ),
            article_date_raw=_parse_optional_text(
                payload.get("article_date_raw"), "article_date_raw"
            ),
            ingestion_time=_parse_optional_datetime(
                payload.get("ingestion_time"), "ingestion_time"
            ),
        )

    def to_mapping(self) -> dict[str, str | None]:
        """Sérialise l'événement dans le contrat JSON."""

        return {
            "event_id": self.event_id,
            "comment_text": self.comment_text,
            "source": self.source,
            "language": self.language.value,
            "event_time": _format_datetime(self.event_time),
            "article_date_raw": self.article_date_raw,
            "replay_time": _format_datetime(self.replay_time),
            "ingestion_time": _format_datetime(self.ingestion_time),
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
            raise EventValidationError(
                "Le commentaire annoté doit être un objet."
            )

        raw_sentiment = payload.get("sentiment")

        if not isinstance(raw_sentiment, str):
            raise EventValidationError(
                "Le champ 'sentiment' doit être une chaîne."
            )

        try:
            sentiment = Sentiment(raw_sentiment.strip().lower())
        except ValueError as error:
            allowed_values = ", ".join(
                label.value for label in Sentiment
            )
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
        """Sérialise le commentaire annoté."""

        return {
            "comment_text": self.comment_text,
            "sentiment": self.sentiment.value,
            "label_source": self.label_source,
            "language": self.language.value,
        }
