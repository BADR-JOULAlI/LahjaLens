# TP — Data Collection et Data Lake LahjaLens

## Objectif pédagogique

Ce TP consiste à construire progressivement la partie Data Engineering de LahjaLens.

À la fin de cette partie, nous saurons :

- identifier une source de commentaires autorisée ;
- vérifier et documenter sa structure ;
- définir un contrat de données stable ;
- ingérer les fichiers historiques ;
- stocker les données brutes dans HDFS Bronze ;
- nettoyer les données vers Silver ;
- produire des agrégations Gold ;
- relire les données avec Spark ;
- préparer les données pour Kafka, le Machine Learning et le dashboard.

Le but n’est pas seulement de produire du code. Chaque fichier doit aider à comprendre une responsabilité précise du pipeline.

## Architecture étudiée

    Source CSV / JSONL / Parquet
                │
                ▼
        Validation du schéma
                │
                ▼
          HDFS Bronze
        Données originales
                │
                ▼
       Nettoyage avec Spark
                │
                ▼
          HDFS Silver
      Données propres et enrichies
                │
                ▼
       Spark SQL / agrégations
                │
                ▼
           HDFS Gold
      Données prêtes pour l’analyse
                │
        ┌───────┴────────┐
        ▼                ▼
   Spark MLlib       PostgreSQL
  Classification     Restitution
                           │
                           ▼
                    Streamlit Dashboard

Kafka sera ajouté dans la partie streaming pour rejouer le corpus historique et alimenter Spark Structured Streaming.

## Règle de travail

Nous créons ou modifions un seul fichier à la fois.

Pour chaque fichier :

1. expliquer son rôle ;
2. écrire une première version minimale ;
3. exécuter les tests adaptés ;
4. examiner le résultat ;
5. corriger les problèmes ;
6. attendre l’instruction « suivant ».

Aucun résultat ne doit être inventé. Une donnée d’exemple doit être clairement marquée comme donnée de test.

## Préparation nécessaire

Avant les fichiers dépendant des contrats, il faut placer à la racine :

    DARIJAPULSE_CODEX_MASTER_SPEC.md

Ce document est la référence pour les noms de champs, les chemins HDFS et les critères de validation. Tant qu’il est absent, nous pouvons préparer la structure pédagogique, mais nous ne devons pas inventer les contrats définitifs.

## Fichiers à créer

### Étape 1 — Schémas des données

Fichier :

    src/schemas/events.py

But :

Définir la structure minimale d’un commentaire et les règles de validation.

Contenu prévu :

- champs obligatoires ;
- types de données ;
- valeurs autorisées pour la langue ;
- représentation du sentiment lorsqu’il existe ;
- validation des identifiants et des dates ;
- erreurs explicites pour les données invalides.

Ce que l’on apprend :

- pourquoi un pipeline a besoin d’un contrat ;
- comment éviter que CSV, Kafka, Spark et PostgreSQL utilisent des formats différents ;
- différence entre un événement brut et un commentaire annoté.

Test attendu :

- un événement valide est accepté ;
- un événement sans texte est rejeté ;
- une langue inconnue est signalée ;
- une date incorrecte est rejetée.

### Étape 2 — Schéma Parquet

Fichier :

    src/schemas/parquet.py

But :

Définir les colonnes et les types utilisés pour les fichiers Parquet du Data Lake.

Contenu prévu :

- nom des colonnes ;
- types Spark ;
- champs obligatoires et optionnels ;
- compatibilité avec Bronze, Silver et Gold.

Ce que l’on apprend :

- pourquoi Parquet est adapté à l’analyse ;
- différence entre schéma JSON et schéma tabulaire ;
- rôle des types dans la qualité des données.

Test attendu :

- le schéma contient les colonnes obligatoires ;
- les types sont compatibles avec Spark ;
- les colonnes inattendues sont détectées ou documentées.

### Étape 3 — Chemins HDFS

Fichier :

    src/storage/hdfs_paths.py

But :

Centraliser les chemins du Data Lake.

Structure prévue :

    /data/lahjalens/bronze/comments/
    /data/lahjalens/silver/comments/
    /data/lahjalens/gold/sentiment/

Ce que l’on apprend :

- séparation entre données brutes, propres et analytiques ;
- importance de chemins reproductibles ;
- différence entre un chemin local et un chemin HDFS.

Test attendu :

- chaque couche possède un chemin distinct ;
- aucun chemin n’est construit de manière incohérente ;
- les chemins sont configurables sans modifier le code métier.

Les chemins définitifs devront être confirmés par la spécification maître.

### Étape 4 — Client HDFS

Fichier :

    src/storage/hdfs_client.py

But :

Fournir une interface contrôlée pour vérifier les répertoires et écrire ou lire des fichiers HDFS.

Contenu prévu :

- vérification de la connexion ;
- création contrôlée des répertoires ;
- vérification de l’existence d’un chemin ;
- fonctions de lecture et d’écriture ;
- messages d’erreur exploitables.

Ce que l’on apprend :

- différence entre écrire localement et écrire dans HDFS ;
- pourquoi l’accès au stockage doit être isolé du pipeline ;
- comment distinguer une erreur de code d’une absence de service.

Test attendu :

- le client détecte si HDFS est disponible ;
- un chemin valide est reconnu ;
- une erreur de connexion n’est pas transformée en faux succès.

### Étape 5 — Lecture d’une source

Fichier :

    src/ingestion/read_source.py

But :

Lire un fichier autorisé et le convertir vers une structure interne commune.

Sources possibles :

- CSV ;
- JSONL ;
- Parquet ;
- dataset public documenté ;
- export fourni avec autorisation.

Ce que l’on apprend :

- différence entre collecte et transformation ;
- gestion de plusieurs formats ;
- conservation de la provenance ;
- importance de l’encodage UTF-8 pour l’arabe et la darija.

Test attendu :

- le format est détecté correctement ;
- les colonnes sont vérifiées ;
- les lignes invalides sont signalées ;
- la source est conservée dans les métadonnées.

### Étape 6 — Ingestion batch vers Bronze

Fichier :

    src/ingestion/batch_ingest.py

But :

Copier les données originales vers HDFS Bronze sans perdre leur traçabilité.

Contenu prévu :

- appel au lecteur de source ;
- ajout des métadonnées d’ingestion ;
- validation minimale ;
- écriture en Parquet ;
- partitionnement si prévu par le contrat ;
- rapport du nombre de lignes traitées.

Ce que l’on apprend :

- Bronze doit rester proche de la donnée originale ;
- l’ingestion ne doit pas supprimer silencieusement des informations ;
- une ingestion réussie doit être vérifiable dans HDFS.

Test attendu :

- les données sont écrites dans le bon chemin ;
- le nombre de lignes est vérifiable ;
- les erreurs sont rapportées ;
- une deuxième ingestion ne crée pas de doublons non documentés.

### Étape 7 — Lecture Bronze avec Spark

Fichier :

    src/spark/read_bronze.py

But :

Relire les données Bronze avec Spark et vérifier leur schéma réel.

Ce que l’on apprend :

- différence entre une écriture réussie et une lecture exploitable ;
- inférence et définition explicite de schéma ;
- inspection des colonnes et du nombre de lignes ;
- préparation aux traitements distribués.

Test attendu :

- Spark lit le chemin HDFS ;
- le schéma réel est affiché ou contrôlé ;
- les colonnes obligatoires existent ;
- les données ne sont pas vides sans explication.

### Étape 8 — Transformation Bronze vers Silver

Fichier :

    src/spark/build_silver.py

But :

Nettoyer et enrichir les données sans modifier Bronze.

Transformations possibles :

- suppression des lignes sans texte ;
- normalisation des espaces ;
- déduplication selon le contrat ;
- normalisation des dates ;
- conservation de la langue ;
- ajout de colonnes de qualité ;
- écriture dans Silver.

Ce que l’on apprend :

- Bronze est immuable ;
- Silver contient des données propres et exploitables ;
- chaque transformation doit être explicite et testable ;
- un nettoyage ne doit pas détruire le texte original.

Test attendu :

- Bronze reste inchangé ;
- les doublons sont traités selon une règle documentée ;
- les lignes invalides sont comptées ;
- Silver respecte le schéma attendu.

### Étape 9 — Requêtes analytiques et Gold

Fichier :

    src/spark/build_gold.py

But :

Créer les agrégations utiles pour le Machine Learning et le dashboard.

Exemples d’agrégations :

- volume de commentaires par source ;
- répartition des sentiments ;
- évolution par date ;
- distribution par langue ;
- résultats par thème lorsque le thème existe.

Ce que l’on apprend :

- différence entre données détaillées et données agrégées ;
- rôle de Gold dans la restitution ;
- préparation d’une table analytique stable.

Test attendu :

- les agrégations sont reproductibles ;
- les résultats sont reliés à une période et une source ;
- les valeurs nulles et l’absence de données sont distinguées.

### Étape 10 — Validation du Data Lake

Fichier :

    tests/test_data_lake.py

But :

Vérifier le parcours complet de la collecte au stockage.

Tests prévus :

- validation des schémas ;
- lecture d’un fichier de test ;
- écriture contrôlée ;
- vérification Bronze, Silver et Gold ;
- détection des colonnes obligatoires ;
- test de données vides ou invalides.

Ce que l’on apprend :

- différence entre test unitaire et test d’intégration ;
- comment prouver qu’un pipeline fonctionne ;
- pourquoi les tests doivent vérifier les données et pas seulement l’absence d’exception.

## Dépendances entre les étapes

    events.py
         │
         ├── parquet.py
         └── read_source.py
                  │
            batch_ingest.py
                  │
             hdfs_client.py
                  │
            read_bronze.py
                  │
            build_silver.py
                  │
            build_gold.py
                  │
           test_data_lake.py

Les étapes 1 à 2 définissent les contrats. Les étapes 3 à 6 connectent les sources au stockage. Les étapes 7 à 9 transforment les données. L’étape 10 valide l’ensemble.

## Travail parallèle possible

Pendant que la personne Data Lake développe les schémas et l’ingestion :

- la personne Kafka peut préparer le producteur de replay ;
- la personne ML peut préparer le nettoyage du corpus annoté ;
- la personne Dashboard peut documenter les KPI attendus.

Mais tous doivent utiliser le même contrat et identifier clairement les données de test.

## Critères d’acceptation de la partie Data Lake

Cette partie est terminée lorsque :

- la source utilisée est identifiée et autorisée ;
- les données sont réellement écrites dans HDFS Bronze ;
- Spark peut relire Bronze ;
- Silver contient des données nettoyées ;
- Gold contient des agrégations vérifiables ;
- les schémas sont documentés ;
- les tests passent ou leurs limites sont expliquées ;
- aucune donnée ou métrique n’a été inventée.

## Première étape pratique

La prochaine action sera de créer uniquement :

    src/schemas/events.py

Avant cette création, il faut vérifier la présence de DARIJAPULSE_CODEX_MASTER_SPEC.md. S’il est absent, nous créerons une version pédagogique minimale uniquement si elle ne contredit aucun contrat officiel, et nous la marquerons comme provisoire.

