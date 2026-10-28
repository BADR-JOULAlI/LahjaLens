"""Contrôles ciblés : originaux, CSV multilignes, jointure et échantillon."""

import csv
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from scripts import prepare_hespress as prep


def save_csv(path, fields, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def story(identifier="article-1", category="sport"):
    return {"": "0", "id": identifier, "title": "عنوان المقال", "date": "السبت 03 أكتوبر 2020 - 07:50", "author": "source", "story": "texte", "topic": category}


def comment(text="تعليق", identifier="article-1", category="sport"):
    return {"": "0", "postId": identifier, "comment": text, "score": "-3", "topic": category}


class PrepareHespressTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def fixture(self, category="sport", comments=None, stories=None):
        save_csv(self.root / f"comments_{category}.csv", prep.COMMENT_FIELDS, comments if comments is not None else [comment(category=category)])
        save_csv(self.root / f"stories_{category}.csv", prep.STORY_FIELDS, stories if stories is not None else [story(category=category)])

    def test_multiline_arabic_is_one_csv_record(self):
        text = 'هادي جملة, فيها فاصلة\nوجملة ثانية "بين علامات"'
        self.fixture(comments=[comment(text)])
        rows = prep.read_rows(self.root / "comments_sport.csv", prep.COMMENT_FIELDS)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["comment"], text)

    def test_malformed_record_is_rejected(self):
        path = self.root / "bad.csv"
        path.write_text(",postId,comment,score,topic\n0,article,text,0,sport,unexpected\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Colonnes manquantes ou excédentaires"):
            prep.read_rows(path, prep.COMMENT_FIELDS)

    def test_wrong_schema_is_rejected(self):
        self.fixture()
        with self.assertRaisesRegex(ValueError, "Schéma inattendu"):
            prep.read_rows(self.root / "comments_sport.csv", prep.STORY_FIELDS)

    def test_unmatched_empty_and_duplicate_comments_are_reported(self):
        self.fixture(comments=[comment("unique"), comment("unique"), comment(""), comment("orphelin", "absent")])
        stats, sample = prep.audit_category(self.root, "sport", 42, 200, set())
        self.assertEqual(stats["comments"], 4)
        self.assertEqual(stats["unlinked_comments"], 1)
        self.assertEqual(stats["empty_comments"], 1)
        self.assertEqual(stats["duplicate_text_rows_after_first"], 1)
        self.assertEqual(len(sample), 1)
        self.assertEqual(sample[0]["event_time"], "")
        self.assertEqual(sample[0]["sentiment"], "")
        self.assertEqual(sample[0]["theme_label"], "")
        self.assertEqual(sample[0]["article_topic"], "sport")

    def test_duplicate_article_ids_are_rejected(self):
        self.fixture(stories=[story(), story()])
        with self.assertRaisesRegex(ValueError, "jointure ambiguë"):
            prep.audit_category(self.root, "sport", 42, 10, set())

    def test_sample_is_reproducible_and_respects_previous_categories(self):
        self.fixture(comments=[comment(f"texte {i}") for i in range(50)])
        stats, first = prep.audit_category(self.root, "sport", 42, 5, {"texte 0"})
        _, second = prep.audit_category(self.root, "sport", 42, 5, {"texte 0"})
        self.assertEqual(first, second)
        self.assertEqual(stats["sample_eligible_unique_texts"], 49)
        self.assertEqual(len(first), 5)
        self.assertNotIn("texte 0", [row["comment_text"] for row in first])

    def test_zip_with_unexpected_filename_is_rejected(self):
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w") as archive:
            archive.writestr("../unexpected.csv", "test")
        with self.assertRaisesRegex(ValueError, "Contenu ZIP inattendu"):
            prep.extract_csv(payload.getvalue(), "comments_sport.csv")

    def test_existing_unrecognized_original_is_never_replaced(self):
        target = self.root / "comments_politique.csv"
        original = b"my existing file"
        target.write_bytes(original)
        metadata = json.dumps({"currentVersionNumber": 1, "licenseName": "CC0: Public Domain"}).encode()
        with patch.object(prep, "fetch_bytes", return_value=metadata):
            with self.assertRaisesRegex(ValueError, "Aucun écrasement"):
                prep.download(self.root)
        self.assertEqual(target.read_bytes(), original)

    def test_annotated_sample_is_never_replaced(self):
        manifest = {"files": {}, "license_declared_by_publisher": "CC0: Public Domain"}
        for category in prep.CATEGORIES:
            self.fixture(category, comments=[comment(f"texte {category}", category=category)])
            for prefix in ("comments", "stories"):
                name = f"{prefix}_{category}.csv"
                data = (self.root / name).read_bytes()
                manifest["files"][name] = {"sha256": prep.sha256(data), "bytes": len(data)}
        sample_path = self.root / "data" / "processed" / "hespress" / "comments_sample.csv"
        sample_path.parent.mkdir(parents=True)
        row = dict.fromkeys(prep.SAMPLE_FIELDS, "")
        row["sentiment"] = "positive"
        save_csv(sample_path, prep.SAMPLE_FIELDS, [row])
        original = sample_path.read_bytes()
        with self.assertRaisesRegex(ValueError, "contient des annotations"):
            prep.audit(self.root, self.root, manifest, 42, 1)
        self.assertEqual(sample_path.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
