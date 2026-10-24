# Plan de réalisation — LahjaLens

## 1. Objectif

Construire en 7 semaines un pipeline Big Data reproductible qui :

- publie des événements de commentaires dans Kafka ;
- archive le flux utile dans HDFS ;
- calcule des analyses historiques avec Spark ;
- entraîne un modèle Spark ML ;
- prédit les nouveaux événements Kafka en streaming ;
- stocke les prédictions et indicateurs dans PostgreSQL ;
- présente les résultats dans Streamlit + Plotly ;
- fonctionne sur un cluster distribué simulé avec Docker.

Le projet est réalisé par quatre étudiants. L’intégration, les tests, la documentation et la démonstration sont des responsabilités collectives.

## 2. Architecture obligatoire

    Producteur Python / replay CSV
                │
                ▼
             Kafka
                │
                ▼
       Spark Structured Streaming
                │
        ┌───────┴────────┐
        ▼                ▼
   Archivage HDFS     Prédictions
        │                │
        ▼                ▼
    Spark batch      PostgreSQL
        │                │
        └───────┬────────┘
                ▼
        Streamlit + Plotly

Cluster Docker minimal :

    spark-master
    spark-worker-1
    spark-worker-2
    namenode
    datanode-1
    datanode-2
    kafka
    postgres
    mlflow

Contraintes :

- tous les traitements Spark utilisent le cluster ;
- HDFS utilise deux DataNodes ;
- la réplication HDFS est configurée à 2 ;
- les volumes Docker sont persistants et distincts ;
- Kafka est la source de Spark Structured Streaming ;
- le modèle sauvegardé et ses transformations sont chargés au démarrage du streaming.

## 3. Stack fixée

- Python 3.11 ;
- Java 11 si compatible avec l’environnement Spark retenu ;
- PySpark 3.5.7 ;
- Apache Kafka 3.9.x en mode KRaft ;
- HDFS 3.4.2 ;
- Spark MLlib ;
- MLflow ;
- PostgreSQL ;
- Streamlit + Plotly ;
- Docker Compose ;
- connecteur Spark Kafka officiel spark-sql-kafka-0-10_2.12.

## 4. Organisation des dossiers

    /ingestion       Producteur et publication Kafka
    /batch           Lecture HDFS, Spark SQL et analyses historiques
    /streaming       Structured Streaming et inférence
    /ml              Entraînement, évaluation et tuning
    /dashboard       Streamlit + Plotly
    /infra            Docker Compose, Dockerfiles et configurations
    /docs             Architecture, contrats et runbooks
    /tests            Tests unitaires et intégration
    README.md
    .env.example
    requirements.txt ou pyproject.toml

Les dépendances, variables d’environnement et fichiers Docker sont préparés par les membres responsables de l’infrastructure.

## 5. Répartition entre quatre étudiants

### Étudiant 1 — Infrastructure et cluster

Responsabilités :

- Docker Compose ;
- Spark Master et deux Workers ;
- NameNode et deux DataNodes ;
- réplication HDFS à 2 ;
- volumes persistants ;
- Kafka, PostgreSQL et MLflow ;
- versions fixées et configuration sans secrets ;
- vérification du démarrage complet.

Livrables principaux :

    infra/docker-compose.yml
    infra/Dockerfile.spark
    infra/Dockerfile.hdfs
    .env.example
    requirements.txt ou pyproject.toml
    docs/runbook.md

### Étudiant 2 — Ingestion, Kafka et archivage HDFS

Responsabilités :

- collecte ou import de la source autorisée ;
- documentation de la provenance et de la licence ;
- validation du schéma d’événement ;
- producteur Kafka ;
- replay contrôlé d’un CSV ou fichier historique ;
- archivage du flux utile dans HDFS ;
- vérification Bronze et réplication.

Livrables principaux :

    ingestion/
    batch/
    src/schemas/
    tests/test_ingestion.py
    docs/data-contracts.md

### Étudiant 3 — Analyse batch et Machine Learning

Responsabilités :

- lecture de l’historique HDFS ;
- nettoyage et préparation ;
- indicateurs Spark SQL ;
- séparation entraînement/test ;
- baseline ;
- comparaison de trois techniques ML ;
- tuning d’une technique sélectionnée ;
- métriques et interprétation ;
- sauvegarde du pipeline ML dans MLflow.

Livrables principaux :

    batch/
    ml/
    tests/test_ml.py
    docs/ml-report.md

### Étudiant 4 — Streaming, restitution et dashboard

Responsabilités :

- chargement du modèle sauvegardé au démarrage ;
- prédiction sur les nouveaux événements Kafka ;
- checkpoints et reprise ;
- écritures PostgreSQL ;
- dashboard Streamlit + Plotly ;
- vérification du délai observé et de l’actualisation.

Livrables principaux :

    streaming/
    dashboard/
    tests/test_streaming.py
    tests/test_dashboard.py

Chaque étudiant doit comprendre l’architecture complète et contribuer aux intégrations hebdomadaires.

## 6. Contrats de données

Événement Kafka provisoire :

    {
      "event_id": "string",
      "comment_text": "string",
      "source": "string",
      "event_time": "ISO-8601 datetime",
      "language": "darija|arabe|français|mixte|inconnu"
    }

Le contrat final doit documenter :

- provenance et conditions d’utilisation ;
- volume ;
- sens d’un événement ;
- identifiant ;
- horodatage ;
- variables disponibles ;
- cible de prédiction.

Aucune variable indisponible au moment de la prédiction ne peut être utilisée comme variable explicative.

## 7. Exigences Machine Learning

Une seule tâche ML est retenue : la classification de sentiments.

Le pipeline doit :

1. définir la cible et l’unité de prédiction ;
2. séparer entraînement et test ;
3. ajuster les transformations apprises sur l’entraînement uniquement ;
4. établir une référence simple ;
5. tester trois techniques ML différentes ;
6. comparer leurs résultats ;
7. sélectionner une technique ;
8. régler plusieurs paramètres de cette technique ;
9. sauvegarder le pipeline complet ;
10. charger ce pipeline dans le job streaming.

Métriques obligatoires :

- matrice de confusion ;
- précision ;
- rappel ;
- F1-score ;
- interprétation des faux positifs et faux négatifs.

Les résultats doivent être réellement calculés et accompagnés de leurs limites.

## 8. Étapes de développement

### Phase 0 — Contrats et préparation

Fichiers :

    docs/data-contracts.md
    docs/architecture.md
    README.md

But :

- valider le sujet ;
- documenter le dataset ;
- fixer le contrat Kafka ;
- confirmer les versions ;
- confirmer les ressources.

### Phase 1 — Infrastructure Docker

Fichiers :

    infra/docker-compose.yml
    infra/Dockerfile.spark
    infra/Dockerfile.hdfs
    .env.example
    requirements.txt ou pyproject.toml

But :

- démarrer le cluster complet ;
- vérifier Spark Master et Workers ;
- vérifier NameNode et DataNodes ;
- configurer HDFS avec réplication 2 ;
- démarrer Kafka, PostgreSQL et MLflow.

Critère : le cluster est opérationnel et les services répondent.

### Phase 2 — Ingestion et archivage

Fichiers :

    ingestion/producer.py
    ingestion/replay.py
    batch/archive_to_hdfs.py
    src/schemas/events.py

But :

- publier des événements dans Kafka ;
- rejouer un CSV autorisé ;
- consommer le flux ;
- archiver le flux utile dans HDFS ;
- produire un historique Parquet ou CSV.

Critère : les événements sont visibles dans Kafka et l’historique est lisible dans HDFS.

### Phase 3 — Analyse batch

Fichiers :

    batch/read_hdfs.py
    batch/spark_sql.py
    batch/build_indicators.py

But :

- relire l’historique avec le cluster Spark ;
- calculer les premiers indicateurs ;
- préparer les données pour le ML ;
- produire la couche analytique Gold.

Critère : démonstration Kafka → Spark → HDFS → analyse batch.

### Phase 4 — Machine Learning

Fichiers :

    ml/prepare_data.py
    ml/train_models.py
    ml/evaluate.py
    ml/tune.py
    ml/register_model.py

But :

- préparer le corpus annoté ;
- comparer trois techniques ;
- évaluer avec les métriques obligatoires ;
- effectuer le tuning ;
- versionner le pipeline dans MLflow.

Critère : modèle évalué, comparé, réglé, sauvegardé et prêt pour l’inférence.

### Phase 5 — Inférence streaming et restitution

Fichiers :

    streaming/predict_stream.py
    streaming/checkpointing.py
    serving/postgres_schema.sql
    serving/write_predictions.py
    dashboard/app.py

But :

- charger le modèle au démarrage ;
- prédire les nouveaux événements Kafka ;
- conserver les checkpoints ;
- écrire les prédictions et indicateurs ;
- afficher les résultats dans le dashboard.

Critère : démonstration complète du producteur jusqu’à la visualisation.

### Phase 6 — Validation finale

Fichiers :

    tests/test_contracts.py
    tests/test_ingestion.py
    tests/test_batch.py
    tests/test_ml.py
    tests/test_streaming.py
    docs/runbook.md
    docs/report-contributions.md

But :

- vérifier la reprise du streaming ;
- relancer les requêtes ;
- vérifier la reproductibilité ;
- mesurer le délai observé ;
- documenter les limites ;
- préparer le rapport et la vidéo.

Critère : dépôt reproductible et livrables finalisés.

## 9. Planning sur sept semaines

| Semaine | Travaux prioritaires | Jalon attendu |
|---|---|---|
| 1 | Sujet, données, Docker, HDFS, Spark, Kafka et dépôt | Cluster opérationnel et événements publiés dans Kafka |
| 2 | Consommation Kafka, archivage HDFS et premiers indicateurs | Démonstration Kafka → Spark → HDFS → batch |
| 3 | Préparation, entraînement, évaluation et sauvegarde ML | Modèle évalué, comparé et versionné |
| 4 | Tuning, inférence streaming, PostgreSQL et dashboard | Démonstration producteur → prédiction → visualisation |
| 5 | Tests de reprise, checkpoints et robustesse | Streaming relançable et données persistantes |
| 6 | Documentation, rapport, interprétation et corrections | Dépôt reproductible et rapport presque final |
| 7 | Vidéo, répétition et livraison | Présentation complète et livrables déposés |

Un point d’intégration collectif doit être organisé au moins une fois par semaine.

## 10. Livrables finaux

Le dépôt GitHub doit contenir :

- code source commenté ;
- Docker Compose et Dockerfiles ;
- configurations et versions fixées ;
- .env.example sans secrets ;
- README de lancement ;
- constitution de l’historique ;
- entraînement et comparaison ML ;
- tuning ;
- inférence streaming ;
- PostgreSQL ;
- dashboard ;
- tests ;
- rapport professionnel de 20 pages maximum ;
- tableau des contributions des quatre étudiants.

La vidéo doit durer environ 10 minutes et présenter le cas métier, l’architecture, le démarrage du cluster, les résultats ML, la comparaison, le tuning et le flux actif jusqu’au dashboard.

## 11. Critères de réussite

Le projet est réussi si :

- le cluster Docker démarre sur une machine propre ;
- Spark utilise le Master et les deux Workers ;
- HDFS utilise un NameNode et deux DataNodes ;
- la réplication HDFS est égale à 2 ;
- Kafka publie les événements ;
- Spark Structured Streaming lit Kafka ;
- le modèle est chargé au démarrage ;
- les nouveaux événements sont prédits ;
- les résultats sont écrits en base ;
- les métriques et le tuning sont interprétés ;
- le dépôt permet de reproduire le scénario ;
- les contributions des quatre étudiants sont documentées.

