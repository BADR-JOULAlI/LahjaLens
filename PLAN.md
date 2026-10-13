# Plan de création — LahjaLens

## Objectif

Construire progressivement une plateforme de Big Data Analytics capable d’ingérer, transformer, analyser et restituer des commentaires marocains en darija, arabe et français.

Le développement suit une règle stricte : un seul fichier est créé ou modifié à chaque étape. Après chaque étape, le résultat est testé et validé avant de continuer avec l’instruction `suivant`.

## Architecture cible

![Architecture LahjaLens](docs/architecture.png)

Le flux principal est :

```text
Sources → Batch / Streaming → HDFS Bronze → Silver → Gold
                                      ↓
                              Apache Spark
                         ↙        ↓        ↘
                  Spark MLlib  MLflow  PostgreSQL
                                             ↓
                                  Streamlit + Plotly
```

## Ordre général de création

### Phase 0 — Configuration et documentation

Objectif : préparer un projet reproductible et documenté sans réinstaller les services déjà disponibles.

| Étape | Fichier | Résultat attendu |
|---|---|---|
| 01 | `AGENTS.md` ou conventions équivalentes | Règles de développement et de validation |
| 02 | `README.md` | Présentation du projet et de la stack |
| 03 | `PLAN.md` | Plan d’exécution et critères d’acceptation |
| 04 | `pyproject.toml` | Métadonnées et dépendances Python |
| 05 | `.gitignore` | Exclusion des secrets, caches et artefacts locaux |
| 06 | `.env.example` | Variables nécessaires, sans secret réel |
| 07 | `docker-compose.yml` | Services Kafka et PostgreSQL du MVP |
| 08 | `config/settings.yaml` | Configuration centralisée |
| 09 | `docs/data-contracts.md` | Contrats JSON et Parquet |
| 10 | `docs/runbook.md` | Commandes de démarrage, test et arrêt |

Critère d’acceptation : l’environnement, les dépendances, les conventions et les commandes prévues sont documentés sans prétendre qu’un service a été vérifié s’il ne l’a pas été.

### Phase 1 — Data Lake et ingestion batch

Objectif : stocker réellement les données historiques dans HDFS, puis les relire avec Spark.

| Étape | Fichier / composant | Résultat attendu |
|---|---|---|
| 11 | `src/schemas/events.py` | Schémas d’événements et validations |
| 12 | `src/schemas/parquet.py` | Schéma Parquet documenté |
| 13 | `src/storage/hdfs_paths.py` | Chemins Bronze, Silver et Gold |
| 14 | `src/storage/hdfs_client.py` | Accès HDFS contrôlé |
| 15 | `src/ingestion/read_source.py` | Lecture d’une source autorisée |
| 16 | `src/ingestion/batch_ingest.py` | Écriture des données brutes en Bronze |
| 17 | `src/spark/read_bronze.py` | Lecture Bronze avec Spark |
| 18 | `src/spark/build_silver.py` | Nettoyage et enrichissement vers Silver |

Critère d’acceptation : un jeu de données réel ou explicitement autorisé est écrit dans HDFS, relu par Spark et vérifié sans données inventées.

### Phase 2 — Machine Learning et MLOps

Objectif : entraîner et évaluer une classification de sentiment sur des exemples disposant de labels fiables.

| Étape | Fichier / composant | Résultat attendu |
|---|---|---|
| 19 | `src/ml/text_cleaning.py` | Nettoyage multilingue contrôlé |
| 20 | `src/ml/prepare_training.py` | Jeu d’entraînement documenté |
| 21 | `src/ml/split_data.py` | Séparation entraînement / validation / test |
| 22 | `src/ml/train_sentiment.py` | Modèle Spark MLlib entraîné |
| 23 | `src/ml/evaluate_sentiment.py` | Métriques réellement calculées |
| 24 | `src/ml/mlflow_tracking.py` | Paramètres, métriques et artefacts MLflow |
| 25 | `src/ml/predict_sentiment.py` | Inférence reproductible |

Critère d’acceptation : aucune fuite de données, labels vérifiés, métriques accompagnées de leur contexte et modèle sauvegardé seulement si l’entraînement a réussi.

### Phase 3 — Analytics et Business Intelligence

Objectif : produire les agrégations métier et les rendre accessibles dans PostgreSQL et Streamlit.

| Étape | Fichier / composant | Résultat attendu |
|---|---|---|
| 26 | `src/analytics/spark_sql.py` | Requêtes analytiques Spark SQL |
| 27 | `src/analytics/kpi_queries.py` | KPI et tendances documentés |
| 28 | `src/analytics/themes.py` | Analyse par thème et source |
| 29 | `src/serving/postgres_schema.sql` | Schéma de restitution PostgreSQL |
| 30 | `src/serving/publish_postgres.py` | Publication des agrégations |
| 31 | `dashboard/app.py` | Dashboard Streamlit + Plotly |

Critère d’acceptation : les KPI du dashboard proviennent de données calculées et publiées, avec une distinction claire entre absence de données et valeur nulle.

### Phase 4 — Streaming Kafka

Objectif : intégrer de véritables événements Kafka ou un replay explicitement identifié.

| Étape | Fichier / composant | Résultat attendu |
|---|---|---|
| 32 | `src/streaming/kafka_config.py` | Configuration producteur / consommateur |
| 33 | `src/streaming/producer.py` | Production d’événements autorisés |
| 34 | `src/streaming/spark_stream.py` | Structured Streaming avec le connecteur officiel |
| 35 | `src/streaming/checkpointing.py` | Checkpoints et reprise contrôlée |

Critère d’acceptation : les événements sont traçables, le checkpoint est configuré et le pipeline ne présente pas de replay synthétique comme une donnée réelle.

### Phase 5 — Validation, automatisation et benchmarks

Objectif : vérifier la robustesse du projet avant extension.

| Étape | Fichier / composant | Résultat attendu |
|---|---|---|
| 36 | `tests/test_schemas.py` | Tests des contrats |
| 37 | `tests/test_ingestion.py` | Tests d’ingestion |
| 38 | `tests/test_ml_pipeline.py` | Tests du pipeline ML |
| 39 | `tests/test_streaming.py` | Tests Kafka / streaming |
| 40 | `scripts/validate_project.py` | Validation globale et rapport |

Critère d’acceptation : les tests unitaires et d’intégration sont distingués, les dépendances externes sont signalées et aucun succès n’est simulé.

## Méthode de travail pour chaque étape

1. Lire les exigences de l’étape et les contrats concernés.
2. Vérifier que les prérequis de l’étape précédente sont satisfaits.
3. Créer ou modifier un seul fichier.
4. Ajouter les validations minimales nécessaires au fichier.
5. Exécuter les tests disponibles dans l’environnement.
6. Documenter les commandes, résultats, limites et hypothèses.
7. Attendre `suivant` avant toute étape supplémentaire.

## Règles techniques importantes

- Ne pas réinstaller Hadoop : HDFS 3.4.2 est un prérequis existant.
- Utiliser Python 3.11, Java 11 et PySpark 3.5.7.
- Utiliser `spark-sql-kafka-0-10_2.12` pour l’intégration Kafka avec Spark.
- Réserver le GPU RTX 5070 aux extensions Transformer ; le MVP repose sur Spark MLlib.
- Ne jamais inscrire de secret dans Git.
- Ne jamais inventer un dataset, un label, une métrique ou un résultat.
- Utiliser MSDA pour le sentiment uniquement après vérification de ses labels.
- Garder AtlasIA comme tâche distincte potentielle de détection de la darija.

## État actuel

- Documentation de démarrage : disponible dans `README.md`.
- Infographie : disponible dans `docs/architecture.png`.
- Plan d’exécution : ce fichier.
- Composants data, ML, analytics et streaming : à implémenter progressivement.
- Spécification maître : `DARIJAPULSE_CODEX_MASTER_SPEC.md`, à ajouter à la racine avant les étapes qui en dépendent.
