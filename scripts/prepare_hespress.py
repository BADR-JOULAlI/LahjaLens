"""Télécharge et audite le corpus Hespress public, sans dépendance externe.

Usage depuis la racine : python scripts/prepare_hespress.py
Réutiliser les originaux locaux : python scripts/prepare_hespress.py --audit-only
Compatible Python 3.11+. Aucun service Docker ni traitement Spark n'est requis.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import random
import re
import sys
import time
import urllib.error
import urllib.request
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATASET = "tariqmassaoudi/hespress"
SOURCE = f"https://www.kaggle.com/datasets/{DATASET}"
API = "https://www.kaggle.com/api/v1/datasets"
VERSION = 1
CATEGORIES = ("politique", "economie", "societe", "sport", "art-et-culture")
COMMENT_FIELDS = ["", "postId", "comment", "score", "topic"]
STORY_FIELDS = ["", "id", "title", "date", "author", "story", "topic"]
SAMPLE_FIELDS = [
    "sample_id", "source", "source_file", "source_record", "article_id",
    "comment_text", "article_topic", "article_title", "article_date_raw",
    "event_time", "language", "sentiment", "theme_label",
    "sentiment_annotator", "theme_annotator",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def fetch_bytes(url: str) -> bytes:
    """Lecture HTTPS bornée ; les échecs réseau sont visibles et réessayés."""
    limit = 80 * 1024 * 1024
    request = urllib.request.Request(url, headers={"User-Agent": "LahjaLens-dataset-preparation/0.1"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                data = response.read(limit + 1)
            if len(data) > limit:
                raise ValueError(f"Réponse supérieure à 80 Mio : {url}")
            return data
        except (urllib.error.URLError, TimeoutError) as error:
            if isinstance(error, urllib.error.HTTPError) and error.code not in (429, 500, 502, 503, 504):
                raise
            if attempt == 2:
                raise
            print(f"Nouvelle tentative réseau ({attempt + 1}/2) : {url}", flush=True)
            time.sleep(attempt + 1)
    raise RuntimeError("Téléchargement interrompu.")


def extract_csv(payload: bytes, filename: str) -> bytes:
    """N'extrait que le fichier attendu, en mémoire, sans extraction de chemins."""
    if payload.startswith(b"PK"):
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            names = [item for item in archive.namelist() if not item.endswith("/")]
            if names != [filename]:
                raise ValueError(f"Contenu ZIP inattendu pour {filename} : {names}")
            if archive.getinfo(filename).file_size > 80 * 1024 * 1024:
                raise ValueError(f"CSV trop volumineux : {filename}")
            return archive.read(filename)
    if payload.lstrip().startswith((b"<!DOCTYPE", b"<html")):
        raise ValueError(f"Page HTML reçue au lieu de {filename}.")
    return payload


def read_rows(path: Path, expected_fields: list[str]) -> list[dict[str, str]]:
    """Le parseur CSV respecte les guillemets et les textes multilignes."""
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, strict=True)
        if reader.fieldnames != expected_fields:
            raise ValueError(f"Schéma inattendu dans {path.name} : {reader.fieldnames}")
        rows = []
        for record, row in enumerate(reader, 1):
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"Colonnes manquantes ou excédentaires : {path.name}, enregistrement {record}")
            rows.append(row)
    return rows


def download(raw_dir: Path) -> dict[str, Any]:
    """Conserve les CSV originaux ; refuse de remplacer un fichier modifié."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = raw_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("dataset") != DATASET or manifest.get("version") != VERSION:
            raise ValueError("Le manifeste local ne correspond pas au corpus/version attendu.")
    else:
        manifest = {"dataset": DATASET, "version": VERSION, "source_url": SOURCE, "files": {}}

    metadata = json.loads(fetch_bytes(f"{API}/view/{DATASET}"))
    if metadata.get("currentVersionNumber") != VERSION:
        raise ValueError("La version publiée a changé. Vérifier la provenance avant de télécharger.")
    if metadata.get("licenseName") != "CC0: Public Domain":
        raise ValueError("La licence déclarée a changé. Vérifier la fiche source.")
    manifest["license_declared_by_publisher"] = metadata["licenseName"]
    manifest["metadata_checked_at"] = utc_now()
    # Chaque consultation est conservée séparément, sans écraser un instantané antérieur.
    snapshot_name = "source_metadata_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + ".json"
    write_json(raw_dir / snapshot_name, metadata)
    manifest["metadata_file"] = snapshot_name

    for category in CATEGORIES:
        for prefix in ("comments", "stories"):
            filename = f"{prefix}_{category}.csv"
            target = raw_dir / filename
            if target.exists():
                previous = manifest["files"].get(filename)
                if previous is None or sha256(target.read_bytes()) != previous["sha256"]:
                    raise ValueError(f"Fichier existant non reconnu ou modifié : {target}. Aucun écrasement.")
                print(f"Déjà présent et vérifié : {filename}", flush=True)
                continue
            url = f"{API}/download/{DATASET}/{filename}?datasetVersionNumber={VERSION}"
            print(f"Téléchargement : {filename}", flush=True)
            original = extract_csv(fetch_bytes(url), filename)
            expected = COMMENT_FIELDS if prefix == "comments" else STORY_FIELDS
            # Vérifier au moins l'en-tête avant d'enregistrer une réponse distante.
            header = next(csv.reader(io.StringIO(original.decode("utf-8-sig"), newline="")), None)
            if header != expected:
                raise ValueError(f"En-tête reçu invalide pour {filename} : {header}")
            with target.open("xb") as handle:
                handle.write(original)
            manifest["files"][filename] = {
                "source_url": url, "downloaded_at": utc_now(),
                "bytes": len(original), "sha256": sha256(original),
            }
            write_json(manifest_path, manifest)
    write_json(manifest_path, manifest)
    return manifest


def audit_category(
    raw_dir: Path, category: str, seed: int, sample_count: int,
    selected_texts: set[str],
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    comments = read_rows(raw_dir / f"comments_{category}.csv", COMMENT_FIELDS)
    stories = read_rows(raw_dir / f"stories_{category}.csv", STORY_FIELDS)
    story_ids = Counter(row["id"] for row in stories)
    if story_ids.get("", 0) or any(count > 1 for count in story_ids.values()):
        raise ValueError(f"Identifiants d'articles vides ou dupliqués dans {category} : jointure ambiguë.")
    by_id = {row["id"]: row for row in stories}
    text_counts = Counter(row["comment"].strip() for row in comments)
    pair_counts = Counter((row["postId"], row["comment"].strip()) for row in comments)
    linked = sum(row["postId"] in by_id for row in comments)
    invalid_scores = sum(not re.fullmatch(r"[+-]?\d+", row["score"].strip()) for row in comments)
    stats = {
        "category": category, "comments": len(comments), "articles": len(stories),
        "linked_comments": linked, "unlinked_comments": len(comments) - linked,
        "join_rate_percent": round(100 * linked / len(comments), 4) if comments else 0,
        "empty_comments": sum(not row["comment"].strip() for row in comments),
        "empty_post_ids": sum(not row["postId"].strip() for row in comments),
        "empty_article_titles": sum(not row["title"].strip() for row in stories),
        "empty_article_dates": sum(not row["date"].strip() for row in stories),
        "comment_topic_mismatches": sum(row["topic"] != category for row in comments),
        "article_topic_mismatches": sum(row["topic"] != category for row in stories),
        "invalid_vote_scores": invalid_scores,
        "duplicate_text_rows_after_first": sum(n - 1 for text, n in text_counts.items() if text),
        "duplicate_article_text_rows_after_first": sum(n - 1 for n in pair_counts.values()),
        "comment_timestamps_available": False,
        "article_date_examples": [row["date"] for row in stories[:3]],
    }
    # Réservoir déterministe, limité en taille, avec unicité des textes de l'échantillon.
    rng = random.Random(f"{seed}:{category}")
    seen: set[str] = set()
    sample: list[dict[str, str]] = []
    eligible = 0
    for record, row in enumerate(comments, 1):
        text = row["comment"].strip()
        article = by_id.get(row["postId"])
        if not text or article is None or row["topic"] != category or article["topic"] != category:
            continue
        if text in seen or text in selected_texts:
            continue
        seen.add(text)
        eligible += 1
        output = {
            "sample_id": f"hespress:v1:{category}:{record}",
            "source": "hespress", "source_file": f"comments_{category}.csv",
            "source_record": str(record), "article_id": row["postId"],
            "comment_text": row["comment"], "article_topic": category,
            "article_title": article["title"], "article_date_raw": article["date"],
            "event_time": "", "language": "inconnu", "sentiment": "", "theme_label": "",
            "sentiment_annotator": "", "theme_annotator": "",
        }
        if len(sample) < sample_count:
            sample.append(output)
        else:
            index = rng.randrange(eligible)
            if index < sample_count:
                sample[index] = output
    selected_texts.update(row["comment_text"].strip() for row in sample)
    stats["sample_eligible_unique_texts"] = eligible
    stats["sample_rows"] = len(sample)
    return stats, sample


def audit(root: Path, raw_dir: Path, manifest: dict[str, Any], seed: int, sample_count: int) -> dict[str, Any]:
    for category in CATEGORIES:
        for prefix in ("comments", "stories"):
            name = f"{prefix}_{category}.csv"
            info = manifest["files"].get(name)
            if info is None or sha256((raw_dir / name).read_bytes()) != info["sha256"]:
                raise ValueError(f"Intégrité du fichier non vérifiée : {name}")
    categories = []
    sample = []
    selected_texts: set[str] = set()
    for category in CATEGORIES:
        stats, rows = audit_category(raw_dir, category, seed, sample_count, selected_texts)
        categories.append(stats)
        sample.extend(rows)
        print(f"{category}: {stats['comments']} commentaires, {stats['articles']} articles, {stats['join_rate_percent']} % reliés", flush=True)

    processed = root / "data" / "processed" / "hespress"
    processed.mkdir(parents=True, exist_ok=True)
    sample_path = processed / "comments_sample.csv"
    # Protéger toute annotation saisie par un membre de l'équipe.
    if sample_path.exists():
        existing = read_rows(sample_path, SAMPLE_FIELDS)
        if any(row[key].strip() for row in existing for key in ("sentiment", "theme_label", "sentiment_annotator", "theme_annotator")):
            raise ValueError("L'échantillon contient des annotations : aucun écrasement. Copier le travail dans data/annotated avant de relancer.")
    with sample_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SAMPLE_FIELDS)
        writer.writeheader()
        writer.writerows(sample)

    result = {
        "audited_at": utc_now(), "dataset": DATASET, "version": VERSION,
        "license_declared_by_publisher": manifest["license_declared_by_publisher"],
        "categories": categories, "total_comments": sum(s["comments"] for s in categories),
        "total_articles": sum(s["articles"] for s in categories),
        "sample": {"seed": seed, "requested_per_category": sample_count, "rows": len(sample), "sha256": sha256(sample_path.read_bytes())},
        "files": manifest["files"],
    }
    write_json(processed / "audit.json", result)
    lines = [
        "# Audit Hespress — cinq catégories", "",
        "Rapport généré par `python scripts/prepare_hespress.py`.", "",
        f"Date UTC : {result['audited_at']}", "",
        f"Source : [{DATASET}]({SOURCE}), version {VERSION}. Licence déclarée : CC0.", "",
        "Les dix CSV originaux sont conservés dans `data/raw/hespress/v1/`.", "",
        "| Catégorie | Commentaires | Articles | Reliés | Sans article | Textes vides | Doublons texte* |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for s in categories:
        lines.append(f"| {s['category']} | {s['comments']} | {s['articles']} | {s['linked_comments']} | {s['unlinked_comments']} | {s['empty_comments']} | {s['duplicate_text_rows_after_first']} |")
    lines += [
        "", f"Total : **{result['total_comments']} commentaires et {result['total_articles']} articles**.", "",
        "*Occurrences après la première, par catégorie, après retrait des espaces de début/fin. Les originaux ne sont pas dédupliqués.", "",
        "## Échantillon pour le travail d'annotation", "",
        f"`data/processed/hespress/comments_sample.csv` : {len(sample)} commentaires, cible {sample_count} par catégorie, graine {seed}.", "",
        f"SHA-256 de l'échantillon : `{result['sample']['sha256']}`.", "",
        "Seuls des commentaires non vides, reliés à un article de la bonne catégorie et au texte distinct dans l'échantillon sont sélectionnés. Le choix est reproductible sur les mêmes originaux et la même version du script.", "",
        "Les champs `sentiment`, `theme_label` et les annotateurs restent vides. `language=inconnu` indique l'absence d'annotation linguistique. L'échantillon est une préparation d'annotation, pas un jeu d'entraînement déjà labellisé.", "",
        "## Contrôles et limites", "",
        "- En-têtes et nombre de colonnes contrôlés sur tous les enregistrements CSV, y compris les textes multilignes.",
        "- Intégrité des fichiers vérifiée avec SHA-256 avant l'audit.",
        "- Les identifiants d'articles doivent être non vides et uniques dans chaque catégorie.",
        "- `postId` des commentaires est joint à `id` des articles de la même catégorie.",
        "- `article_topic` est la catégorie éditoriale, pas un thème du commentaire validé manuellement.",
        "- `score` mesure des votes de lecteurs, pas le sentiment.",
        "- `event_time` reste vide : aucune date de commentaire n'est fournie. `article_date_raw` conserve la date de l'article, sans conversion ou fuseau inventé.",
        "- Aucun appel Kafka, traitement Spark, import HDFS ou entraînement ML n'est effectué ici.",
        "- Les sources ne sont pas représentées comme des données Facebook/Instagram.",
        "- L'audit décrit la structure et la provenance. Il ne certifie pas les langues, thèmes ou sentiments.", "",
        "| Catégorie | Thèmes commentaires incohérents | Thèmes articles incohérents | Scores non entiers | Dates articles vides |",
        "|---|---:|---:|---:|---:|",
    ]
    for s in categories:
        lines.append(f"| {s['category']} | {s['comment_topic_mismatches']} | {s['article_topic_mismatches']} | {s['invalid_vote_scores']} | {s['empty_article_dates']} |")
    lines += ["", "## Empreintes des originaux", "", "| Fichier | Octets | SHA-256 |", "|---|---:|---|"]
    for name, info in sorted(manifest["files"].items()):
        lines.append(f"| {name} | {info['bytes']} | `{info['sha256']}` |")
    report = root / "docs" / "datasets" / "HESPRESS_AUDIT.md"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-only", action="store_true", help="Ne pas accéder au réseau, vérifier les CSV déjà téléchargés.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--sample-per-category", type=int, default=200)
    args = parser.parse_args()
    if args.sample_per_category < 1:
        parser.error("--sample-per-category doit être positif.")
    raw_dir = ROOT / "data" / "raw" / "hespress" / "v1"
    try:
        manifest = json.loads((raw_dir / "manifest.json").read_text(encoding="utf-8")) if args.audit_only else download(raw_dir)
        if manifest.get("dataset") != DATASET or manifest.get("version") != VERSION:
            raise ValueError("Manifeste incompatible avec la version attendue.")
        result = audit(ROOT, raw_dir, manifest, args.seed, args.sample_per_category)
    except (OSError, ValueError, csv.Error, zipfile.BadZipFile) as error:
        print(f"Échec : {error}", file=sys.stderr)
        return 1
    print(f"Terminé : {result['total_comments']} commentaires, {result['total_articles']} articles, {result['sample']['rows']} lignes dans l'échantillon.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
