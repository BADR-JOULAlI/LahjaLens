"""Règles métier du contrat, notamment les dates inconnues du corpus Hespress."""

import json
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone, tzinfo

from src.schemas.events import (
    CommentEvent,
    EventValidationError,
    LabeledComment,
    Language,
)


class CommentEventTests(unittest.TestCase):
    def setUp(self):
        self.payload = {
            "event_id": "test-001",
            "comment_text": "هاد المنتوج زوين بزاف",
            "source": "manual-test",
            "language": "darija",
            "event_time": "2026-10-28T10:00:00Z",
            "ingestion_time": "2026-10-28T10:01:00Z",
        }

    def event(self, **changes):
        return CommentEvent.from_mapping({**self.payload, **changes})

    def test_previous_six_field_payload_remains_valid(self):
        output = self.event().to_mapping()
        for field, value in self.payload.items():
            self.assertEqual(output[field], value)
        self.assertIsNone(output["article_id"])
        self.assertIsNone(output["replay_time"])

    def test_previous_positional_constructor_remains_valid(self):
        when = datetime(2026, 10, 28, tzinfo=timezone.utc)
        event = CommentEvent("id", "texte", "source", Language.UNKNOWN, when, when)
        self.assertEqual(event.event_time, when)

    def test_missing_or_null_comment_date_is_unknown(self):
        missing = dict(self.payload)
        del missing["event_time"]
        for payload in (missing, {**self.payload, "event_time": None}):
            with self.subTest(payload=payload):
                event = CommentEvent.from_mapping(payload)
                self.assertIsNone(event.event_time)
                self.assertIsNone(event.to_mapping()["event_time"])

    def test_hespress_raw_date_does_not_become_comment_date(self):
        raw_date = "السبت 03 أكتوبر 2020 - 07:50"
        event = self.event(
            event_time=None, source="hespress", language="inconnu",
            article_id="article-1", article_topic="sport",
            article_title="عنوان المقال", article_date_raw=raw_date,
        )
        self.assertEqual(event.article_date_raw, raw_date)
        self.assertIsNone(event.event_time)
        self.assertIsNone(event.article_published_at)
        self.assertEqual(event.language, Language.UNKNOWN)
        self.assertNotIn("theme", event.to_mapping())
        self.assertNotIn("sentiment", event.to_mapping())

    def test_article_and_replay_dates_never_replace_missing_event_time(self):
        event = self.event(
            event_time=None, article_id="article-1",
            article_published_at="2020-10-03T06:50:00Z",
            replay_time="2026-10-28T11:00:00Z",
        )
        output = event.to_mapping()
        self.assertIsNone(output["event_time"])
        self.assertEqual(output["article_published_at"], "2020-10-03T06:50:00Z")
        self.assertEqual(output["ingestion_time"], self.payload["ingestion_time"])
        self.assertEqual(output["replay_time"], "2026-10-28T11:00:00Z")

    def test_unicode_json_roundtrip_keeps_context_and_unknown_dates(self):
        event = self.event(
            event_time=None, article_id="article-1", article_topic="politique",
            article_title="عنوان المقال", article_date_raw="تاريخ المقال الأصلي",
        )
        encoded = json.dumps(event.to_mapping(), ensure_ascii=False)
        self.assertIn('"event_time": null', encoded)
        self.assertEqual(CommentEvent.from_mapping(json.loads(encoded)), event)

    def test_raw_article_date_is_preserved_exactly(self):
        raw = "  السبت 03 أكتوبر 2020 - 07:50\n"
        event = self.event(article_id="article-1", article_date_raw=raw)
        self.assertEqual(event.to_mapping()["article_date_raw"], raw)

    def test_all_json_dates_are_normalized_to_utc(self):
        output = self.event(
            article_id="article-1", event_time="2026-10-28T11:00:00+01:00",
            ingestion_time="2026-10-28T12:01:00+02:00",
            article_published_at="2020-10-03T07:50:00+01:00",
            replay_time="2026-10-28T13:00:00+02:00",
        ).to_mapping()
        self.assertEqual(output["event_time"], "2026-10-28T10:00:00Z")
        self.assertEqual(output["ingestion_time"], "2026-10-28T10:01:00Z")
        self.assertEqual(output["article_published_at"], "2020-10-03T06:50:00Z")
        self.assertEqual(output["replay_time"], "2026-10-28T11:00:00Z")

    def test_direct_constructor_dates_are_also_serialized_in_utc(self):
        when = datetime(2026, 10, 28, 11, tzinfo=timezone(timedelta(hours=1)))
        event = replace(self.event(), event_time=when, ingestion_time=when)
        self.assertEqual(event.to_mapping()["event_time"], "2026-10-28T10:00:00Z")
        self.assertEqual(event.to_mapping()["ingestion_time"], "2026-10-28T10:00:00Z")

    def test_ingestion_time_is_required(self):
        missing = dict(self.payload)
        del missing["ingestion_time"]
        with self.assertRaisesRegex(EventValidationError, "ingestion_time"):
            CommentEvent.from_mapping(missing)
        for value in (None, "", 0):
            with self.subTest(value=value):
                with self.assertRaisesRegex(EventValidationError, "ingestion_time"):
                    self.event(ingestion_time=value)

    def test_present_dates_must_be_iso_with_a_timezone(self):
        for field in ("event_time", "ingestion_time", "article_published_at", "replay_time"):
            for value in ("", "pas une date", "2026-10-28T10:00:00", "2026-02-30T10:00:00Z", 123):
                with self.subTest(field=field, value=value):
                    with self.assertRaisesRegex(EventValidationError, field):
                        self.event(article_id="article-1", **{field: value})

    def test_direct_constructor_rejects_naive_dates(self):
        for field in ("event_time", "ingestion_time", "article_published_at", "replay_time"):
            with self.subTest(field=field):
                with self.assertRaisesRegex(EventValidationError, field):
                    replace(self.event(), article_id="article-1", **{field: datetime(2026, 10, 28)})

    def test_tzinfo_without_effective_offset_is_rejected(self):
        class NoOffset(tzinfo):
            def utcoffset(self, dt):
                return None

        with self.assertRaisesRegex(EventValidationError, "event_time"):
            replace(self.event(), event_time=datetime(2026, 10, 28, tzinfo=NoOffset()))

    def test_article_context_requires_article_id(self):
        for field, value in (
            ("article_title", "titre"), ("article_topic", "sport"),
            ("article_date_raw", "date source"),
            ("article_published_at", "2020-10-03T06:50:00Z"),
        ):
            with self.subTest(field=field):
                with self.assertRaisesRegex(EventValidationError, "article_id"):
                    self.event(**{field: value})

    def test_article_fields_are_nonempty_strings_or_null(self):
        for field in ("article_id", "article_title", "article_topic", "article_date_raw"):
            for value in ("", "  ", 42, []):
                with self.subTest(field=field, value=value):
                    payload = {**self.payload, "article_id": "article-1", field: value}
                    with self.assertRaisesRegex(EventValidationError, field):
                        CommentEvent.from_mapping(payload)
                    with self.assertRaisesRegex(EventValidationError, field):
                        replace(self.event(), **{field: value})

    def test_required_text_fields_are_still_validated(self):
        for field in ("event_id", "comment_text", "source"):
            for value in (None, "", "   ", 123):
                with self.subTest(field=field, value=value):
                    with self.assertRaisesRegex(EventValidationError, field):
                        self.event(**{field: value})

    def test_invalid_language_and_nonobject_payload_are_rejected(self):
        with self.assertRaises(EventValidationError):
            self.event(language="language-inconnue")
        for value in (None, [], "{}"):
            with self.subTest(value=value):
                with self.assertRaises(EventValidationError):
                    CommentEvent.from_mapping(value)


class LabeledCommentTests(unittest.TestCase):
    def test_existing_labeled_comment_contract_is_unchanged(self):
        for sentiment in ("positive", "neutral", "negative"):
            with self.subTest(sentiment=sentiment):
                payload = {
                    "comment_text": "تعليق", "sentiment": sentiment,
                    "label_source": "human", "language": "darija",
                }
                self.assertEqual(LabeledComment.from_mapping(payload).to_mapping(), payload)


if __name__ == "__main__":
    unittest.main()
