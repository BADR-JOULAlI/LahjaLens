# AfriSenti : pipeline de préparation du sentiment

## Décision

AfriSenti devient le corpus principal pour entraîner la classification du
sentiment en darija. Il fournit les labels `negative`, `neutral` et `positive`,
avec les splits officiels `train`, `dev` et `test`. Sa licence déclarée est
CC BY 4.0.

Hespress reste le corpus de contexte et de thèmes (`politique`, `economie`,
`societe`, `sport`, `art-et-culture`). Sa rubrique d'article ne remplace pas
un label de sentiment.

## Notebook de préparation

Ouvrir [`01_prepare_afrisenti.ipynb`](../../notebooks/01_prepare_afrisenti.ipynb)
avec Jupyter ou VS Code, puis exécuter les cellules dans l'ordre.

Le notebook :

1. télécharge les TSV officiels dans `data/raw/afrisenti/v1/` ;
2. vérifie les colonnes `tweet` et `label` ;
3. refuse les labels inconnus et les fichiers vides ;
4. conserve les empreintes SHA-256 dans `manifest.json` ;
5. applique une normalisation prudente Unicode NFKC et espaces ;
6. retire les doublons et les recouvrements train/dev ;
7. écrit les fichiers propres dans `data/processed/afrisenti/v1/` ;
8. produit `audit.json` avec les proportions finales.

Pour un nouvel audit hors réseau, commenter la cellule de téléchargement et
exécuter les cellules d'extraction, de nettoyage et d'écriture avec les TSV
déjà présents.

## Sorties produites

Chaque fichier propre contient :

| Champ | Rôle |
|---|---|
| `record_id` | identifiant stable du commentaire et du split |
| `comment_text` | texte nettoyé, sans suppression des mots de Darija |
| `sentiment` | label lisible |
| `label` | code ML : `0`, `1`, `2` |
| `source_dataset` | `afrisenti` |
| `source_split` | `train`, `dev` ou `test` |
| `language` | `darija` |
| `cleaning_version` | version de la transformation |

Le fichier `audit.json` est la preuve de la préparation. Les données restent
locales et ignorées par Git tant que l'équipe n'a pas décidé comment les
redistribuer conformément à la licence.

## Notebooks d'entraînement Spark ML

Après le démarrage du cluster Docker, ouvrir
[`02_train_sentiment_spark.ipynb`](../../notebooks/02_train_sentiment_spark.ipynb)
avec un kernel PySpark attaché au cluster. Le notebook utilise
`spark://spark-master:7077` comme master Spark.

Les notebooks 02 à 05 ont été exécutés sur le cluster Docker. Le premier modèle
est écrit dans HDFS sous
`hdfs://namenode:9000/data/lahjalens/models/afrisenti_v1` et ses métriques dans le
sous-dossier `metrics`. Les expériences suivantes conservent leurs modèles
dans des répertoires datés distincts.

Les expériences améliorées sont dans
[`03_sentiment_experiments.ipynb`](../../notebooks/03_sentiment_experiments.ipynb).
Elles comparent les n-grammes de mots, les n-grammes de caractères et la
combinaison des deux avec le macro-F1.

Le notebook [04](../../notebooks/04_sentiment_tfidf_normalized.ipynb) vérifie
également la lecture des CSV et compare des représentations TF-IDF normalisées.
Son meilleur score dev masquait un rappel nul des négatifs en écriture latine.

Le point d'entrée recommandé pour le diagnostic et la nouvelle baseline est
[`05_sentiment_script_aware.ipynb`](../../notebooks/05_sentiment_script_aware.ipynb).
Il ajoute un nettoyage enregistré dans le modèle, une validation interne
stratifiée par sentiment et écriture, et des métriques séparées par écriture.
Les fichiers source restent inchangés ; les transformations sont appliquées
aux caractéristiques du modèle.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\Invoke-SparkNotebook.ps1 -Notebook notebooks/05_sentiment_script_aware.ipynb
```

Le runner lit directement les cellules, sans créer de fichier `.py`. Chaque
exécution écrit un nouveau rapport sous `data/processed/afrisenti/diagnostics/`
et un modèle sous `hdfs://namenode:9000/data/lahjalens/models/afrisenti_script_v3/`.
Pour exécuter uniquement la préparation du diagnostic et les contrôles : ajouter
`-AuditOnly`. Ce mode ne produit pas de modèle entraîné.

Lors de l'exécution du 15 novembre, les conteneurs utilisaient Spark 3.5.7,
Python 3.8.10 et NumPy 1.24.4 installé dans `/tmp/lahjalens-python` sur le master
et les deux workers. Le runner configure ce chemin. Cette installation NumPy
est temporaire : après recréation des conteneurs, elle doit être rétablie ou
intégrée à l'image par la personne responsable des dépendances. Les CSV doivent
être accessibles sous `/opt/lahjalens/data/processed/afrisenti/v1`, et Spark doit
pouvoir créer ses modèles dans `/data/lahjalens/models` sur HDFS.

Les résultats, limites et prochaines étapes sont détaillés dans le
[diagnostic du sentiment](SENTIMENT_DIAGNOSTIC.md). Sur les mêmes 2 961 lignes de
test, le macro-F1 est de **56,99 %** pour le notebook 05, contre **55,61 %** pour
le modèle 03 et **51,40 %** pour le modèle 04. Le gain face au modèle 03 reste
incertain et la généralisation à Hespress n'a pas encore été validée.

## Résultat du premier audit

| Split | Lignes propres | Negative | Neutral | Positive |
|---|---:|---:|---:|---:|
| train | 5 563 | 1 657 | 2 161 | 1 745 |
| dev | 492 | 138 | 189 | 165 |
| test | 2 961 | 850 | 967 | 1 144 |

Vingt doublons internes ont été retirés du train et deux textes qui recoupaient
le train ont été retirés du dev. Le test n'est pas rééchantillonné.

## Limites

- AfriSenti est adapté au sentiment, pas à la classification des thèmes
  Hespress.
- Les textes viennent de réseaux sociaux et peuvent différer du style des
  commentaires Hespress.
- Le nettoyage ne traduit pas l'Arabizi et ne supprime pas automatiquement la
  négation, les emojis ou la ponctuation.
- Les performances doivent être mesurées séparément sur AfriSenti et sur un
  échantillon Hespress annoté par l'équipe.
