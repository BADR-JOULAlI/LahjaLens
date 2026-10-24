# Organisation du travail — LahjaLens

## 1. Objectif

Ce document explique comment trois personnes vont développer LahjaLens en parallèle tout en gardant une architecture cohérente.

LahjaLens analyse des commentaires marocains en darija, arabe et français. L’architecture comprend :

    Corpus historique → Batch Python / Spark → HDFS Bronze
             │
             └→ Replay Python → Kafka → Spark Structured Streaming
                                                  │
    Bronze → Nettoyage Spark → Silver → Spark SQL → Gold
                                      │                 │
    Corpus annoté → Spark MLlib ──────┘                 ▼
                    │                      PostgreSQL → Streamlit + Plotly
                    └→ MLflow

Le développement peut être parallèle, mais les contrats de données et les interfaces doivent être validés avant l’intégration.

## 2. Règle du travail parallèle

Les membres peuvent travailler en même temps sur une même grande fonctionnalité, par exemple la collecte et le Machine Learning, à condition de :

- ne pas modifier le même fichier en même temps ;
- travailler sur des branches Git séparées ;
- utiliser les mêmes contrats de données ;
- documenter les hypothèses et les données temporaires ;
- tester chaque composant indépendamment ;
- intégrer les branches uniquement après revue.

Le développement est parallèle, mais l’intégration est contrôlée.

## 3. Répartition des responsabilités

### Personne 1 — Data Collection et Data Lake

Responsabilité : faire entrer les données dans la plateforme et les organiser dans HDFS.

Travail prévu :

- identifier les datasets et sources autorisés ;
- vérifier les formats CSV, JSONL et Parquet ;
- documenter la provenance des données ;
- créer l’ingestion batch Python/Spark ;
- écrire les données brutes dans HDFS Bronze ;
- nettoyer et dédupliquer les données vers Silver ;
- créer les agrégations nécessaires vers Gold ;
- vérifier que les données peuvent être relues par Spark.

Dossiers principaux :

    src/schemas/
    src/ingestion/
    src/storage/
    src/spark/

Branche recommandée : feature/data-lake

Livrables :

- schéma des événements ;
- schéma du corpus annoté ;
- lecteur de sources ;
- ingestion Bronze ;
- transformation Silver ;
- documentation des chemins HDFS ;
- tests de validation des données.

### Personne 2 — Kafka et Spark Structured Streaming

Responsabilité : intégrer le replay historique et le traitement streaming.

Travail prévu :

- configurer Kafka en mode KRaft avec Docker Compose ;
- créer le topic comments.raw ;
- développer le producteur de replay Python ;
- publier les événements avec le contrat commun ;
- lire les événements avec Spark Structured Streaming ;
- gérer les checkpoints ;
- traiter les événements invalides ;
- vérifier la reprise après interruption.

Dossiers principaux :

    docker-compose.yml
    src/streaming/
    tests/test_streaming.py

Branche recommandée : feature/kafka-streaming

La personne 2 peut commencer avec un petit fichier d’événements conforme au contrat. Ce fichier est uniquement un support de test et ne doit pas être présenté comme une vraie collecte.

Livrables :

- configuration Kafka ;
- topic documenté ;
- producteur de replay ;
- job Spark Streaming ;
- configuration des checkpoints ;
- tests du producteur et du consommateur.

### Personne 3 — Machine Learning, MLflow et restitution

Responsabilité : entraîner le modèle de sentiment et rendre les résultats exploitables.

Travail prévu :

- préparer le corpus annoté ;
- vérifier les labels de sentiment ;
- nettoyer les textes en darija, arabe et français ;
- séparer entraînement, validation et test ;
- entraîner un modèle Spark MLlib ;
- calculer les métriques ;
- enregistrer les expériences dans MLflow ;
- publier les prédictions dans PostgreSQL ;
- construire le dashboard Streamlit + Plotly.

Dossiers principaux :

    src/ml/
    src/analytics/
    src/serving/
    dashboard/
    tests/test_ml_pipeline.py

Branche recommandée : feature/ml-dashboard

Livrables :

- préparation du corpus annoté ;
- pipeline de nettoyage ;
- entraînement Spark MLlib ;
- évaluation ;
- suivi MLflow ;
- schéma PostgreSQL ;
- requêtes KPI ;
- dashboard.

## 4. Contrats communs à valider en premier

### Événement de commentaire

    {
      "event_id": "string",
      "comment_text": "string",
      "source": "string",
      "language": "darija|arabe|français|mixte|inconnu",
      "event_time": "ISO-8601 datetime",
      "ingestion_time": "ISO-8601 datetime"
    }

### Corpus annoté

    {
      "comment_text": "string",
      "sentiment": "negative|neutral|positive",
      "label_source": "string",
      "language": "darija|arabe|français|mixte|inconnu"
    }

### Règles des contrats

- event_id doit identifier un événement de manière stable ;
- comment_text ne doit pas être remplacé par une valeur inventée ;
- source doit indiquer l’origine réelle ou le nom du dataset ;
- event_time et ingestion_time doivent être distingués ;
- les labels doivent être vérifiés avant l’entraînement ;
- toute donnée temporaire doit être marquée comme test ;
- toute modification du contrat doit être annoncée à l’équipe.

## 5. Ce qui peut être fait en parallèle

### Data Collection et Machine Learning

Ces travaux peuvent commencer en même temps.

La personne Data prépare le schéma Silver et l’ingestion. La personne ML utilise le corpus annoté disponible ou un petit échantillon conforme au même schéma.

La personne ML peut commencer le nettoyage, TF-IDF, Naive Bayes, régression logistique, évaluation et suivi MLflow sans attendre toute la collecte.

L’intégration finale attend toutefois la validation du schéma Silver et du chemin HDFS réel.

### Data Collection et Kafka

Ces travaux peuvent aussi commencer en parallèle.

La personne Kafka utilise un événement JSON conforme au contrat. Quand l’ingestion réelle est prête, le producteur de replay lit les données depuis le chemin validé au lieu du fichier de test.

### ML et Dashboard

Le dashboard peut commencer avec des tables PostgreSQL minimales et clairement identifiées comme données de développement. Les requêtes définitives sont intégrées après validation des colonnes Gold et des prédictions ML.

## 6. Dépendances à valider

| Dépendance | Validation nécessaire |
|---|---|
| Kafka vers Spark Streaming | Contrat JSON et topic validés |
| Bronze vers Silver | Schéma et règles de nettoyage validés |
| Silver vers ML | Corpus annoté et labels vérifiés |
| Gold vers PostgreSQL | Colonnes et agrégations validées |
| PostgreSQL vers Dashboard | Tables et KPI disponibles |
| Tests globaux | Branches fusionnées et services accessibles |

## 7. Organisation Git

Chaque membre récupère le projet et crée sa branche :

    git clone https://github.com/BADR-JOULAlI/LahjaLens.git
    cd LahjaLens
    git checkout master
    git pull origin master
    git checkout -b feature/data-lake

Branches prévues :

- feature/data-lake
- feature/kafka-streaming
- feature/ml-dashboard

Cycle de travail :

    git pull origin master
    git status
    git add fichiers-concernes
    git commit -m "type: description courte"
    git push -u origin feature/nom-de-branche

Exemples de commits :

    feat: add Bronze batch ingestion
    feat: add Kafka replay producer
    feat: train sentiment classifier
    test: validate event schema
    docs: document HDFS paths

Avant une Pull Request, chaque membre doit :

1. mettre sa branche à jour ;
2. exécuter les tests concernés ;
3. vérifier qu’aucun secret n’est présent ;
4. expliquer les hypothèses et les limites ;
5. décrire les fichiers modifiés ;
6. demander une revue à un autre membre.

## 8. Ordre recommandé d’intégration

### Étape A — Contrats

Valider les schémas JSON, Parquet, noms de champs, topic Kafka et labels.

### Étape B — Data Lake

Fusionner l’ingestion et vérifier l’écriture Bronze, la transformation Silver et la lecture Spark.

### Étape C — Kafka

Fusionner le producteur de replay et Spark Structured Streaming. Vérifier le format des événements.

### Étape D — Machine Learning

Fusionner le pipeline ML, entraîner sur le corpus annoté et enregistrer uniquement les métriques obtenues.

### Étape E — Restitution

Fusionner PostgreSQL et Streamlit après validation des données Gold et des prédictions.

### Étape F — Validation finale

Exécuter les tests unitaires, les tests d’intégration et le parcours complet :

    Source → Bronze → Silver → Gold → ML → PostgreSQL → Dashboard
                             ↘ Kafka → Spark Streaming ↗

## 9. Synchronisation de l’équipe

À chaque jalon, une synchronisation courte doit présenter :

- ce qui est terminé ;
- ce qui est en cours ;
- les fichiers modifiés ;
- les contrats modifiés ;
- les blocages ;
- les tests réussis ou échoués.

Une modification de schéma, chemin HDFS, topic Kafka ou table PostgreSQL doit être annoncée immédiatement.

## 10. Critères de fin d’une tâche

Une tâche est terminée uniquement si :

- le code est présent dans la branche prévue ;
- les tests adaptés ont été exécutés ;
- les erreurs importantes sont corrigées ou documentées ;
- les données utilisées sont identifiées ;
- aucune valeur n’a été inventée ;
- la documentation est mise à jour si nécessaire ;
- une Pull Request peut être revue.

## 11. Résumé pratique

    1. Valider les contrats
    2. Créer trois branches
    3. Développer Data Lake, Kafka et ML en parallèle
    4. Tester chaque branche séparément
    5. Revoir les Pull Requests
    6. Fusionner dans l’ordre des dépendances
    7. Exécuter les tests d’intégration
    8. Valider le pipeline complet

Le principe essentiel est de travailler en parallèle sur des composants séparés, avec des interfaces communes et une intégration progressive.

