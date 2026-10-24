# LahjaLens — Spécification technique du projet

> Statut : spécification interne provisoire. Aucun cahier des charges officiel n’ayant été fourni, ce document formalise les décisions actuelles du projet.

## 1. Objectif

LahjaLens analyse les sentiments de commentaires marocains en darija, arabe et français.

La plateforme doit importer des commentaires autorisés, traiter un corpus historique en batch, stocker les données dans HDFS, rejouer le corpus historique avec Kafka, traiter le flux avec Spark Structured Streaming, entraîner un modèle Spark MLlib, suivre les expériences avec MLflow, publier les résultats dans PostgreSQL et les afficher avec Streamlit et Plotly.

## 2. Architecture de référence

    Sources autorisées
            │
            ├── Corpus historique CSV / JSONL / Parquet
            └── Corpus annoté avec labels vérifiés
                            │
                            ▼
                 Ingestion batch Python / Spark
                            │
                            ▼
                       HDFS Bronze
                            │
                            ├── Replay Python → Kafka
                            │                         │
                            │                         ▼
                            │                Spark Structured Streaming
                            ▼
                   Nettoyage avec PySpark
                            │
                       HDFS Silver
                            │
                   Spark SQL et agrégations
                            │
                        HDFS Gold
                       /         \
              Spark MLlib       PostgreSQL
                   │                 │
                 MLflow       Streamlit + Plotly

Kafka sert au replay historique et à la démonstration du streaming. Il ne représente pas nécessairement une collecte instantanée.

## 3. Stack technique

- Ubuntu 24.04 sous WSL2 ;
- Python 3.11 et Java 11 ;
- HDFS 3.4.2 existant ;
- PySpark 3.5.7 en mode local ;
- Apache Kafka 3.9.x en mode KRaft ;
- spark-sql-kafka-0-10_2.12 ;
- Spark MLlib, MLflow, PostgreSQL ;
- Streamlit, Plotly et Docker Compose.

Hadoop ne doit pas être réinstallé. Le GPU est réservé aux extensions Transformer et n’est pas requis pour le MVP.

## 4. Sources de données

Sources acceptées :

- datasets publics dont la licence est documentée ;
- fichiers CSV, JSONL ou Parquet ;
- exports obtenus avec autorisation ;
- replay de données historiques collectées légalement.

Aucun scraping non autorisé, dataset inventé ou résultat simulé ne doit être utilisé.

MSDA peut servir à la classification à trois sentiments après vérification de ses labels. AtlasIA pourra servir plus tard à une tâche distincte de détection de la darija.

## 5. Contrat d’événement commentaire

    {
      "event_id": "string",
      "comment_text": "string",
      "source": "string",
      "language": "darija|arabe|français|mixte|inconnu",
      "event_time": "ISO-8601 datetime",
      "ingestion_time": "ISO-8601 datetime"
    }

Règles : event_id est obligatoire et stable ; comment_text est obligatoire et non vide ; source indique la provenance réelle ; event_time est la date du commentaire ; ingestion_time est la date d’entrée dans la plateforme. Les données invalides sont signalées et non supprimées silencieusement.

## 6. Contrat du corpus annoté

    {
      "comment_text": "string",
      "sentiment": "negative|neutral|positive",
      "label_source": "string",
      "language": "darija|arabe|français|mixte|inconnu"
    }

Le modèle est entraîné uniquement sur des labels fiables et documentés.

## 7. Couches du Data Lake

    Bronze/comments/    Données brutes et traçables
    Silver/comments/    Données nettoyées et enrichies
    Gold/sentiment/     Prédictions et agrégations analytiques

Bronze reste proche de la source. Silver contient les données nettoyées. Gold contient les données prêtes pour l’analyse. Une transformation ne doit pas écraser Bronze. Les chemins HDFS définitifs seront centralisés dans la configuration.

## 8. Kafka et replay

Kafka fonctionne en mode KRaft avec Docker Compose. Le topic provisoire est comments.raw.

Le producteur Python lit un corpus historique autorisé, convertit chaque ligne vers le contrat d’événement, publie les événements et permet un délai de replay configurable.

Spark Structured Streaming lit comments.raw, valide le JSON, traite les événements conformes, isole les erreurs et utilise un checkpoint. Le replay doit toujours être identifié comme historique.

## 9. Machine Learning et restitution

Pipeline MLlib : lecture du corpus annoté, nettoyage, séparation entraînement/validation/test, TF-IDF, entraînement, métriques et suivi MLflow.

PostgreSQL reçoit les données Gold et les prédictions. Le dashboard présente les volumes, sentiments, tendances, sources, langues et thèmes si cette information existe.

## 10. Ordre de développement

1. src/schemas/events.py
2. src/schemas/parquet.py
3. src/storage/hdfs_paths.py
4. src/storage/hdfs_client.py
5. src/ingestion/read_source.py
6. src/ingestion/batch_ingest.py
7. src/spark/read_bronze.py
8. src/spark/build_silver.py
9. src/spark/build_gold.py
10. tests/test_data_lake.py
11. src/streaming/kafka_config.py
12. src/streaming/producer.py
13. src/streaming/spark_stream.py
14. src/ml/
15. src/serving/
16. dashboard/
17. tests d’intégration

Un seul fichier est créé ou modifié par étape. Chaque étape est testée et expliquée avant de continuer.

## 11. Critères d’acceptation du Data Lake

- source documentée et autorisée ;
- contrat respecté ;
- données réellement écrites dans Bronze ;
- lecture Bronze possible avec Spark ;
- données Silver nettoyées ;
- agrégations Gold vérifiables ;
- erreurs traçables ;
- aucun résultat inventé ;
- tests exécutés ou limites documentées.

## 12. Ressources et décisions à confirmer

Le projet doit rester compatible avec 32 Go de RAM, un Core Ultra 7, une RTX 5070 de 8 Go, Spark local et HDFS sous WSL2.

Restent à confirmer : chemins HDFS définitifs, dataset final, règles de déduplication, modèle ML, stratégie de thèmes, paramètres Kafka et critères d’évaluation du professeur.

