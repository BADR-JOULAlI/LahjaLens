# Audit Hespress — cinq catégories

Rapport généré par `python scripts/prepare_hespress.py`.

Date UTC : 2026-10-28T21:36:24.362139Z

Source : [tariqmassaoudi/hespress](https://www.kaggle.com/datasets/tariqmassaoudi/hespress), version 1. Licence déclarée : CC0.

Les dix CSV originaux sont conservés dans `data/raw/hespress/v1/`.

| Catégorie | Commentaires | Articles | Reliés | Sans article | Textes vides | Doublons texte* |
|---|---:|---:|---:|---:|---:|---:|
| politique | 45857 | 1000 | 45857 | 0 | 0 | 642 |
| economie | 28076 | 1000 | 28076 | 0 | 0 | 388 |
| societe | 46555 | 1000 | 46555 | 0 | 0 | 356 |
| sport | 8110 | 1000 | 8110 | 0 | 0 | 24 |
| art-et-culture | 10568 | 1000 | 10568 | 0 | 0 | 52 |

Total : **139166 commentaires et 5000 articles**.

*Occurrences après la première, par catégorie, après retrait des espaces de début/fin. Les originaux ne sont pas dédupliqués.

## Échantillon pour le travail d'annotation

`data/processed/hespress/comments_sample.csv` : 1000 commentaires, cible 200 par catégorie, graine 42.

SHA-256 de l'échantillon : `9e620d9d11ccc1bbb4040ed368060b1d8c3b73ec4e3c45f8710bc2a627b6e809`.

Seuls des commentaires non vides, reliés à un article de la bonne catégorie et au texte distinct dans l'échantillon sont sélectionnés. Le choix est reproductible sur les mêmes originaux et la même version du script.

Les champs `sentiment`, `theme_label` et les annotateurs restent vides. `language=inconnu` indique l'absence d'annotation linguistique. L'échantillon est une préparation d'annotation, pas un jeu d'entraînement déjà labellisé.

## Contrôles et limites

- En-têtes et nombre de colonnes contrôlés sur tous les enregistrements CSV, y compris les textes multilignes.
- Intégrité des fichiers vérifiée avec SHA-256 avant l'audit.
- Les identifiants d'articles doivent être non vides et uniques dans chaque catégorie.
- `postId` des commentaires est joint à `id` des articles de la même catégorie.
- `article_topic` est la catégorie éditoriale, pas un thème du commentaire validé manuellement.
- `score` mesure des votes de lecteurs, pas le sentiment.
- `event_time` reste vide : aucune date de commentaire n'est fournie. `article_date_raw` conserve la date de l'article, sans conversion ou fuseau inventé.
- Aucun appel Kafka, traitement Spark, import HDFS ou entraînement ML n'est effectué ici.
- Les sources ne sont pas représentées comme des données Facebook/Instagram.
- L'audit décrit la structure et la provenance. Il ne certifie pas les langues, thèmes ou sentiments.

| Catégorie | Thèmes commentaires incohérents | Thèmes articles incohérents | Scores non entiers | Dates articles vides |
|---|---:|---:|---:|---:|
| politique | 0 | 0 | 0 | 0 |
| economie | 0 | 0 | 0 | 0 |
| societe | 0 | 0 | 0 | 0 |
| sport | 0 | 0 | 0 | 0 |
| art-et-culture | 0 | 0 | 0 | 0 |

## Empreintes des originaux

| Fichier | Octets | SHA-256 |
|---|---:|---|
| comments_art-et-culture.csv | 5060169 | `1b049886aac3e020b03b391c27f8f59e1b8532c9996c7a875ee84524cd1fb216` |
| comments_economie.csv | 14221861 | `543b337848972da9a42844c3c3aa68eaae7cd55a3bca580861a87f8e26c891e7` |
| comments_politique.csv | 23761923 | `48d2792aeab4e08f1e268788b0b23c5775cd5f39e3b85c60952776adfcaf8b30` |
| comments_societe.csv | 24473129 | `b483c4efe2f0e1fc507e21aacc5dec2c572032ec93af7854de36462ab7f123a1` |
| comments_sport.csv | 3602117 | `3787b524fb51a25af1e577ba98cff2eb708b833afee2bab84e0e1d1780a9f04d` |
| stories_art-et-culture.csv | 5113797 | `f9a439ebc0cffb4044f4061b8e491cffede7e18992d92e9b024f202bf8f8813f` |
| stories_economie.csv | 4189145 | `e9403d979e5ec3946a833e4b2cfdc0ef283a59e0d217280147b0776de1b4adbc` |
| stories_politique.csv | 4179258 | `63f20cb3225e4fed8aa435e2309645bb40f4571290a0902894454e34c62858e7` |
| stories_societe.csv | 4064549 | `4438dd3df756b3b59ac42981a48e7dc568d05ec81ab0342749ac528a8348015e` |
| stories_sport.csv | 2750758 | `447837105b3f7573e52dc020da87f650c2209ee0a40f74496edaa8215d9f5448` |
