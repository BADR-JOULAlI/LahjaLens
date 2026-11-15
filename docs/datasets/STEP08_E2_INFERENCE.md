# LahjaLens — Étape 08 : inférence E2 Silver → Gold

## Statut

Exécution d’intégration terminée le 15 novembre 2026 avec le cluster Spark Docker. Aucun entraînement n’a été lancé et aucun modèle ou corpus historique n’a été écrasé.

Le push demandé avant cette étape a déjà été effectué sur `feature/sentiment-results` avec le commit `2978793` (`feat: add MAC multi-dataset experiment`). Les livrables de l’étape 08 restent locaux jusqu’à une demande explicite de push.

## Architecture réellement exécutée

```text
Hespress CSV local (1 000 commentaires)
        │
        ▼
Spark cluster Docker : spark-master + spark-worker-1 + spark-worker-2
        │
        ├── Silver HDFS : préparation et contrat de données
        │
        ├── PipelineModel E2 chargé depuis HDFS
        │       AfriSenti + MAC, sans réentraînement
        │
        └── Gold HDFS : prédictions, scores et traçabilité
```

Application Spark observée : `app-20261115194650-0008`.

- Master : `spark://spark-master:7077`
- Spark : `3.5.7`
- Workers : `2` workers actifs, `4` cœurs au total
- Réplication HDFS configurée : `2`

## Artefacts utilisés

### Modèle E2

```text
hdfs://namenode:9000/data/lahjalens/models/sentiment/experiments/mac_multidataset/20261115T190123Z_9cb0026f/e2
```

Version déclarée : `e2-afrisenti-mac-20261115T190123Z_9cb0026f`.

Le `PipelineModel` contient notamment les transformations de nettoyage, vectorisation, TF-IDF, assemblage et la régression logistique finale. L’ordre des classes vérifié dans la sortie est :

```text
negative, neutral, positive
```

Le preprocessing de l’inférence est référencé comme `shared_clean_v1_nb05`. Les probabilités sont conservées comme scores de sortie du modèle et ne sont pas déclarées calibrées.

### Entrée

```text
/opt/lahjalens/data/processed/hespress/comments_sample.csv
```

Cet échantillon contient 1 000 commentaires Hespress répartis sur les catégories d’articles. Les labels de sentiment humains ne sont pas disponibles ; il ne s’agit donc pas d’un benchmark annoté.

## Exécution validée

Run : `20261115T194649Z_17df8866`

```text
Silver : hdfs://namenode:9000/data/lahjalens/silver/hespress/e2_inference/20261115T194649Z_17df8866
Gold   : hdfs://namenode:9000/data/lahjalens/gold/hespress/e2_inference/20261115T194649Z_17df8866
```

Résultats observés :

| Contrôle | Résultat |
|---|---:|
| Entrées Hespress | 1 000 |
| Lignes Silver | 1 000 |
| Lignes Gold | 1 000 |
| Identifiants d’entrée uniques | 1 000 |
| Identifiants Gold uniques | 1 000 |
| Divergences de reproductibilité | 0 |
| Valeurs de prédiction observées | 0, 1, 2 |
| Anomalies de probabilités | 0 |
| Identifiants d’article manquants | 0 |
| Date artificielle utilisée pour les tendances | Non |

Distribution descriptive des prédictions :

| Sentiment prédit | Nombre |
|---|---:|
| negative | 508 |
| neutral | 308 |
| positive | 184 |

Cette distribution décrit uniquement les sorties du modèle E2 sur Hespress. Sans annotations humaines, aucune Accuracy, Macro-F1, précision ou rappel ne doit être calculé.

Répartition des articles : `art-et-culture`, `economie`, `politique`, `societe` et `sport` contiennent chacun 200 commentaires. La source est `hespress` pour les 1 000 lignes.

## Schéma Gold

La sortie conserve l’identifiant, le texte, les métadonnées Hespress et la traçabilité de l’inférence. Elle contient notamment :

```text
event_id, comment_text, source, article_id, article_title,
article_topic, sentiment, prediction, model_version,
preprocessing_version, predicted_at,
prob_negative, prob_neutral, prob_positive,
probabilities_calibrated, model_label_order,
inference_run_id, silver_path, gold_path
```

Chaque exécution écrit dans un chemin versionné avec le mode `errorifexists`; les anciennes sorties Gold ne sont pas écrasées.

## Fichiers livrés

- `notebooks/08_silver_e2_gold_inference.ipynb` : audit, préparation Silver, chargement E2, inférence, contrôles, analytics Spark SQL et rapport.
- `tests/test_e2_inference_notebook.py` : contrôles statiques empêchant notamment le réentraînement et `overwrite()`.
- `docs/datasets/STEP08_E2_INFERENCE.md` : ce rapport technique.

Les artefacts détaillés de l’exécution sont générés localement sous :

```text
data/processed/hespress/e2_inference/20261115T194649Z_17df8866/
```

Ils comprennent le manifeste, l’audit modèle, les contrôles d’intégration, la reproductibilité et les tableaux JSON Spark SQL.

## Commandes de reproduction

Vérification statique :

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_e2_inference_notebook.py -q
```

Audit sans écriture :

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\Invoke-SparkNotebook.ps1 `
  -Notebook 'notebooks/08_silver_e2_gold_inference.ipynb' -AuditOnly
```

Exécution complète sur un nouveau run versionné :

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\Invoke-SparkNotebook.ps1 `
  -Notebook 'notebooks/08_silver_e2_gold_inference.ipynb'
```

## Préparation PostgreSQL

L’étape suivante peut lire le Gold Parquet versionné et charger une vue de restitution dans PostgreSQL. Le chargement devra conserver `model_version`, `inference_run_id` et `predicted_at`, et ne devra pas transformer les prédictions Hespress non annotées en métriques de performance.

## Limites et suite recommandée

1. Annoter manuellement un sous-ensemble Hespress avec deux annotateurs et arbitrage.
2. Séparer les commentaires par article et familles de quasi-doublons avant de constituer un benchmark.
3. Évaluer E2 sur ce benchmark réservé avant de conclure sur sa qualité hors domaine.
4. Ajouter ensuite la restitution PostgreSQL/Streamlit et les contrôles de fraîcheur des runs.
