# LahjaLens — Data Warehouse et Dashboard

## Objectif

Cette étape complète le MVP après l’inférence E2 :

```text
HDFS Silver → Spark MLlib E2 → HDFS Gold → PostgreSQL → Streamlit + Plotly
```

Les prédictions Hespress restent des sorties de modèle. Comme l’échantillon
Hespress n’a pas de labels humains vérifiés, le dashboard n’affiche aucune
Accuracy, Macro-F1, précision ou rappel sur Hespress.

## Composants ajoutés

| Fichier | Rôle |
|---|---|
| `src/warehouse/schema.sql` | Schéma PostgreSQL idempotent, index et vues analytiques |
| `src/warehouse/load_gold.py` | Lecture Gold Parquet par Spark et upsert transactionnel PostgreSQL |
| `apps/streamlit_app.py` | Dashboard Overview, Sentiment, Explorer, Monitoring et Data Quality |
| `Dockerfile.warehouse` | Image du chargeur avec Spark et psycopg2 |
| `Dockerfile.dashboard` | Image Streamlit légère |
| `requirements-dashboard.txt` | Dépendances dédiées au dashboard |
| `scripts/Invoke-GoldToPostgres.ps1` | Chargement contrôlé d’un chemin Gold |
| `scripts/Invoke-LahjaLens.ps1` | Orchestration inférence optionnelle → PostgreSQL → dashboard |
| `tests/test_warehouse_dashboard.py` | Tests de contrat, validation et syntaxe |
| `docker-compose.yml` | Services `warehouse-loader` et `dashboard` ajoutés au Compose existant |

## Modèle PostgreSQL

Le schéma `lahjalens` contient :

- `inference_runs` : provenance, modèle, preprocessing, statut et volume du run ;
- `articles` : contexte article sans confondre sa date avec celle du commentaire ;
- `comments` : identifiants, texte original et métadonnées source ;
- `sentiment_predictions` : prédiction, scores, version du modèle et run ;
- `v_latest_predictions` : dernière prédiction connue par commentaire ;
- `v_overview`, `v_sentiment_distribution`, `v_topic_sentiment`,
  `v_model_runs`, `v_data_quality` : vues consommées par le dashboard.

La clé primaire des prédictions est `(event_id, inference_run_id, model_version)`.
Une relance du même Gold effectue donc un upsert et ne crée pas de doublon.
Les données Gold sont uniquement lues par Spark ; le chargeur ne modifie pas HDFS.

## Commandes de démonstration

Depuis la racine du dépôt :

### 1. Démarrer les services

```powershell
docker compose up -d postgres spark-master spark-worker-1 spark-worker-2
```

Pour démarrer également le dashboard :

```powershell
docker compose up -d dashboard
```

### 2. Utiliser le Gold déjà validé

```powershell
.\scripts\Invoke-GoldToPostgres.ps1 `
  -GoldPath 'hdfs://namenode:9000/data/lahjalens/gold/hespress/e2_inference/20261115T194649Z_17df8866'
```

Le chargeur exécute Spark dans le service `warehouse-loader`, avec PostgreSQL
accessible sur `postgres:5432` depuis le réseau Docker.

### 3. Exécuter la chaîne complète sur un nouveau run

```powershell
.\scripts\Invoke-LahjaLens.ps1 -RunInference
```

Cette commande lance le notebook 08, récupère le chemin Gold du manifeste,
charge ce chemin dans PostgreSQL puis démarre le dashboard.

### 4. Ouvrir l’interface

```text
http://localhost:8501
```

Les connexions depuis le PC utilisent PostgreSQL sur `localhost:5434` ; le
dashboard et le chargeur utilisent `postgres:5432` à l’intérieur de Docker.

## Pages du dashboard

- **Overview** : volumes globaux, répartition des sentiments prédits et volumes
  par catégorie Hespress ;
- **Sentiment Analysis** : comparaison catégorie × sentiment et filtres ;
- **Data Explorer** : recherche, pagination, scores et export de la page filtrée ;
- **Model Monitoring** : runs, versions, statuts et volumes chargés ;
- **Data Quality** : texte ou article manquant, probabilités invalides, volumes
  et runs en échec.

Les filtres portent sur la catégorie d’article, la source, le sentiment prédit
et la version du modèle. La catégorie Hespress est explicitement affichée comme
contexte de l’article, pas comme thème NLP automatiquement extrait du commentaire.

## Vérifications à exécuter

Tests locaux :

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_warehouse_dashboard.py tests\test_e2_inference_notebook.py -q
```

Validation de la configuration Compose :

```powershell
docker compose config
docker compose ps
```

Après chargement, les contrôles PostgreSQL attendus sont :

```powershell
docker compose exec -T postgres psql -U lahja_user -d lahjalens_db -c `
  "SELECT * FROM lahjalens.v_overview;"
docker compose exec -T postgres psql -U lahja_user -d lahjalens_db -c `
  "SELECT * FROM lahjalens.v_data_quality ORDER BY check_name;"
```

Le nombre de prédictions PostgreSQL doit être égal au nombre de lignes Gold du
run chargé. Une seconde exécution avec le même Gold doit conserver ce nombre.

## État et limites

- L’inférence E2 Silver→Gold a été exécutée et validée sur 1 000 commentaires.
- Le schéma, le chargeur et le dashboard sont présents et testés statiquement.
- L’exécution PostgreSQL/dashboard doit être vérifiée lorsque Docker Desktop est
  accessible dans le terminal courant.
- Aucun score de classification Hespress ne sera affiché avant annotation humaine.
- Les tendances temporelles ne sont pas affichées à partir de `replay_time` ; la
  date d’article reste une métadonnée de l’article.
- Kafka reste disponible dans l’architecture, mais n’est pas ajouté à cette
  première chaîne batch, conformément à la priorité du MVP.
