# Exécuter l'évaluation robuste du sentiment

Le [notebook 06](../../notebooks/06_sentiment_robust_evaluation.ipynb) compare les
prédictions existantes des modèles 03, 04 et 05. Il n'entraîne aucun modèle.

## Prérequis

- Un Python avec **NumPy** et **Matplotlib ≥ 3.7**. Le notebook utilise des bibliothèques
  d'analyse, sans importer Spark ML pour entraîner.
- Les CSV préparés AfriSenti v1, les manifestes bruts et préparés.
- Le dossier `data/processed/afrisenti/diagnostics/20261115T120646Z_7cae2241/`,
  avec les trois CSV de prédictions test, `report.json`, `inner_split.csv`,
  `selection_locked.json` et `selection_protocol.json`.
- Les notebooks 01 à 05, lus uniquement comme sources.
- Facultatif : Docker/HDFS pour lire les métadonnées des modèles historiques.

Le notebook s'arrête si les données requises manquent ou si les identifiants,
labels et empreintes sont incohérents. Une indisponibilité HDFS est signalée ;
elle n'est pas remplacée par une inférence locale.

Les dépendances du projet ne sont ni modifiées ni réinstallées automatiquement.
Pour l'exécution observée de cette étape, utiliser les versions enregistrées dans
le manifeste du run, plutôt que supposer que l'environnement correspond à celui
de l'ancien entraînement Spark.

## Exécution

Depuis la racine du dépôt :

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\Invoke-EvaluationNotebook.ps1
```

Autre interpréteur déjà installé :

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\Invoke-EvaluationNotebook.ps1 -PythonExe "C:\chemin\python.exe"
```

Dans VS Code/Jupyter, ouvrir le notebook 06 et exécuter toutes les cellules avec
un kernel ayant accès aux fichiers locaux. `CHECK_HDFS=False` désactive
explicitement les consultations HDFS. Ne pas utiliser le runner d'entraînement
`Invoke-SparkNotebook.ps1` pour cette évaluation Matplotlib locale.

## Sorties

Chaque exécution crée un répertoire unique :

`data/processed/afrisenti/evaluations/<date_UTC>_<identifiant>/`

- `comparison_report.md` : rapport comparatif complet.
- `evaluation.json` : métriques et différences appariées.
- `reproducibility_checks.json` : constats de provenance et limites.
- `run_manifest.json` : versions, entrées/sorties et empreintes.
- `tables/` : CSV des métriques, matrices, erreurs, transitions de prédiction
  et gains/pertes par classe.
- `figures/` : six figures PNG, SVG, PDF et un PDF regroupé.
- `private_review/` : textes, panel à annoter et huit cas illustratifs commentés.
  Les notes de lecture sont exploratoires, sans annotation humaine indépendante ;
  ces fichiers restent locaux.
- Avec le runner : notebook exécuté et `runner_execution.json`.

Aucun artefact existant n'est écrasé. Pour réexécuter une cellule d'export,
recommencer depuis la configuration afin de créer un nouveau run. Une erreur
conserve son dossier partiel pour diagnostic.

## Interprétation

Les scores du test AfriSenti restent exploratoires. Le bootstrap apparié
ne corrige pas l'exposition antérieure au test. Le proxy Arabizi numérique
n'est pas une annotation linguistique ; les groupes latin et Arabizi ne sont
pas synonymes. Les groupes petits ou sans certaines classes sont signalés.

Le [protocole Hespress](HESPRESS_BENCHMARK_PROTOCOL.md) décrit la constitution
d'une nouvelle évaluation indépendante. Les commentaires non annotés existants
ne sont pas considérés comme une vérité terrain.

Le notebook ne recalcule pas les probabilités, la calibration, une nouvelle
inférence Spark ou un entraînement. Il ne crée pas de run MLflow et ne fait
aucun commit/push Git.
