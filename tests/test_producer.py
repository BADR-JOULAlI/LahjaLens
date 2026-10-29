"""Lecture Hespress et garanties de publication, sans broker pour ces tests."""

import contextlib
import csv
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.schemas.events import CommentEvent
from src.streaming import producer as replay


class ProducerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "comments.csv"

    def row(self, number=1, category="politique", **changes):
        return {
            "sample_id": f"hespress:v1:{category}:{number}", "source": "hespress",
            "comment_text": 'تعليق فيه, فاصلة\nوفيه "علامات"', "language": "inconnu",
            "event_time": "", "article_id": f"article-{number}",
            "article_title": "عنوان المقال", "article_topic": category,
            "article_date_raw": "  السبت 03 أكتوبر 2020 - 07:50\n",
            "sentiment": "negative", "theme_label": "exemple-annotation",
            **changes,
        }

    def save(self, rows):
        with self.path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    def events(self):
        self.save([self.row(1), self.row(2)])
        return replay.load_events(self.path)

    def test_round_robin_balances_grouped_input(self):
        self.save([self.row(i, category) for category in replay.CATEGORIES for i in range(25)])
        events = replay.load_events(self.path, limit=100)
        self.assertEqual(len(events), 100)
        for category in replay.CATEGORIES:
            self.assertEqual(sum(e.article_topic == category for e in events), 20)
        self.assertEqual([e.article_topic for e in events[:5]], list(replay.CATEGORIES))

    def test_ids_are_stable_and_labels_are_not_leaked(self):
        self.save([self.row()])
        first = replay.load_events(self.path)[0]
        second = replay.load_events(self.path)[0]
        self.assertEqual(first.event_id, second.event_id)
        self.assertIsNone(first.event_time)
        self.assertIsNone(first.replay_time)
        self.assertEqual(first.article_date_raw, self.row()["article_date_raw"])
        self.assertNotIn("sentiment", first.to_mapping())
        self.assertNotIn("theme_label", first.to_mapping())

    def test_category_filter_and_exhausted_categories(self):
        self.save([self.row(), self.row(2, "sport"), self.row(3, "sport")])
        events = replay.load_events(self.path, limit=10, categories=["sport"])
        self.assertEqual(len(events), 2)
        self.assertTrue(all(event.article_topic == "sport" for event in events))

    def test_input_is_validated_beyond_the_selected_limit(self):
        self.save([self.row(), self.row(2, comment_text="")])
        with self.assertRaisesRegex(ValueError, "enregistrement CSV 2"):
            replay.load_events(self.path, limit=1)

    def test_duplicate_ids_are_rejected(self):
        self.save([self.row(), self.row(comment_text="autre texte")])
        with self.assertRaisesRegex(ValueError, "Identifiant dupliqué"):
            replay.load_events(self.path)

    def test_wrong_source_category_and_date_are_rejected(self):
        for changes in ({"source": "facebook"}, {"article_topic": "autre"}, {"event_time": "2020-10-03"}):
            with self.subTest(changes=changes):
                self.save([self.row(**changes)])
                with self.assertRaises(ValueError):
                    replay.load_events(self.path)

    def test_missing_or_duplicate_headers_are_rejected(self):
        for header in ("comment_text\ntexte\n", "sample_id,sample_id\na,b\n"):
            with self.subTest(header=header):
                self.path.write_text(header, encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "Colonnes invalides"):
                    replay.load_events(self.path)

    def test_short_csv_record_is_rejected(self):
        self.save([self.row()])
        with self.path.open("a", encoding="utf-8", newline="") as handle:
            handle.write("missing,columns\n")
        with self.assertRaisesRegex(ValueError, "Colonnes manquantes"):
            replay.load_events(self.path)

    def test_empty_category_selection_is_rejected(self):
        self.save([self.row()])
        with self.assertRaisesRegex(ValueError, "Aucun commentaire"):
            replay.load_events(self.path, categories=["sport"])

    def test_dry_run_never_creates_a_kafka_client(self):
        self.save([self.row()])
        output = io.StringIO()
        with patch.object(replay, "create_producer") as factory, contextlib.redirect_stdout(output):
            result = replay.main(["--input", str(self.path), "--dry-run"])
        self.assertEqual(result, 0)
        factory.assert_not_called()
        self.assertIn("aucun message envoyé", output.getvalue())
        self.assertIn('"event_time": null', output.getvalue())

    def test_cli_preview_supports_arabic_with_windows_legacy_encoding(self):
        self.save([self.row()])
        result = subprocess.run(
            [sys.executable, "-m", "src.streaming.producer", "--dry-run", "--input", str(self.path)],
            cwd=replay.ROOT, env={**os.environ, "PYTHONIOENCODING": "cp1252"},
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode("utf-8"))
        self.assertIn("تعليق", result.stdout.decode("utf-8"))

    def test_acknowledgements_keys_and_utf8_payload(self):
        events = self.events()
        client = Mock()
        client.send.return_value.get.return_value = SimpleNamespace(topic="comments.raw", partition=0, offset=12)
        with contextlib.redirect_stdout(io.StringIO()), patch.object(replay.time, "sleep") as sleep:
            confirmed = replay.publish_events(client, events, "comments.raw", delay=0.2)
        self.assertEqual(confirmed, 2)
        sleep.assert_called_once_with(0.2)
        self.assertEqual(client.send.return_value.get.call_count, 2)
        for call, event in zip(client.send.call_args_list, events):
            self.assertEqual(call.kwargs["key"], event.event_id.encode("utf-8"))
            output = json.loads(call.kwargs["value"].decode("utf-8"))
            parsed = CommentEvent.from_mapping(output)
            self.assertEqual(parsed.event_id, event.event_id)
            self.assertIsNotNone(parsed.replay_time)
            self.assertIsNone(parsed.event_time)
            self.assertIsNone(event.replay_time)

    def test_failure_reports_only_confirmed_messages_and_stops(self):
        events = self.events()
        client = Mock()
        ok = Mock()
        ok.get.return_value = SimpleNamespace(topic="comments.raw", partition=0, offset=0)
        failed = Mock()
        failed.get.side_effect = TimeoutError("broker timeout")
        client.send.side_effect = [ok, failed]
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(replay.DeliveryError) as raised:
                replay.publish_events(client, events, "comments.raw")
        self.assertEqual(raised.exception.confirmed, 1)
        self.assertEqual(raised.exception.event_id, events[1].event_id)
        self.assertIn("peut avoir atteint Kafka", str(raised.exception))
        self.assertEqual(client.send.call_count, 2)

    def test_cli_fails_and_closes_client_when_send_is_not_confirmed(self):
        self.events()
        client = Mock()
        client.send.return_value.get.side_effect = TimeoutError("unavailable")
        with patch.object(replay, "create_producer", return_value=client):
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                result = replay.main(["--input", str(self.path)])
        self.assertEqual(result, 1)
        client.close.assert_called_once_with(timeout=5)

    def test_invalid_arguments_fail_before_loading_or_connecting(self):
        for args in (["--limit", "0"], ["--delay", "nan"], ["--delay", "-1"], ["--topic", ".."], ["--bootstrap-servers", ""]):
            with self.subTest(args=args), patch.object(replay, "create_producer") as factory:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    replay.main(args)
                factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
