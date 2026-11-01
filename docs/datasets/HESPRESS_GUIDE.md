# Premier TP : récupérer et comprendre les données Hespress

## Résultat de cette étape

Les fichiers des cinq catégories sont téléchargés et contrôlés : **139 166 commentaires et 5 000 articles**. Tous les commentaires ont une correspondance avec un article de leur catégorie.

| Catégorie | Commentaires | Articles |
|---|---:|---:|
| Politique | 45 857 | 1 000 |
| Économie | 28 076 | 1 000 |
| Société | 46 555 | 1 000 |
| Sport | 8 110 | 1 000 |
| Art et culture | 10 568 | 1 000 |

Source : [Hespress, publié par Tariq Massaoudi sur Kaggle](https://www.kaggle.com/datasets/tariqmassaoudi/hespress), version 1. La fiche déclare une licence CC0. Le manifeste local conserve les URL, les dates de récupération et les empreintes SHA-256.

## 1. Quels fichiers avons-nous créés ?

```text
scripts/prepare_hespress.py                Téléchargement, audit et échantillonnage
tests/test_prepare_hespress.py             Tests des contrôles et protections
docs/datasets/HESPRESS_AUDIT.md            Rapport des vérifications réelles
data/raw/hespress/v1/
    comments_politique.csv                 Commentaires de chaque catégorie
    stories_politique.csv                  Articles correspondants
    comments_economie.csv
    stories_economie.csv
    comments_societe.csv
    stories_societe.csv
    comments_sport.csv
    stories_sport.csv
    comments_art-et-culture.csv
    stories_art-et-culture.csv
    manifest.json                         Provenance et SHA-256 des dix CSV
    source_metadata_<date_UTC>.json        Copie des métadonnées Kaggle
data/processed/hespress/
    audit.json                            Résultats détaillés lisibles par un programme
    comments_sample.csv                   1 000 commentaires pour préparer l'annotation
```

Les dix CSV originaux, leur manifeste, l'instantané des métadonnées de téléchargement et l'échantillon initial de 1 000 commentaires sont partagés dans GitHub sur la branche `feature/docker-compose`. Les originaux représentent environ 91 Mo. Les annotations individuelles dans `data/annotated/`, les autres datasets et les sorties générées comme `audit.json` restent exclus de Git.

La licence CC0 déclarée par le déposant du [dataset source](https://www.kaggle.com/datasets/tariqmassaoudi/hespress) concerne ce corpus. Le manifeste et l'instantané des métadonnées conservent cette déclaration et l'attribution à Tariq Massaoudi. Les fichiers sont redistribués sans modifier les originaux. `.gitattributes` désactive la conversion automatique des fins de ligne pour ces CSV afin de préserver les empreintes SHA-256 sur tous les systèmes.

Il s'agit encore de fichiers sur le PC. L'importation HDFS sera une étape distincte.

## 2. Récupérer les données partagées

Pour un camarade qui n'a pas encore le dépôt :

```powershell
git clone --branch feature/docker-compose https://github.com/BADR-JOULAlI/LahjaLens.git
cd LahjaLens
python scripts/prepare_hespress.py --audit-only
```

La commande `git clone` récupère le code et les données partagées. Aucun compte Kaggle n'est nécessaire pour utiliser cette copie. La commande d'audit vérifie les dix empreintes, relit les CSV et reproduit l'échantillon avec la graine 42.

Si le dépôt est déjà présent, mettre à jour la branche `feature/docker-compose` avant de lancer l'audit. Conserver ou valider ses modifications locales avant de changer de branche.

### Télécharger à nouveau depuis Kaggle si nécessaire

Dans PowerShell, depuis la racine du projet :

```powershell
python scripts/prepare_hespress.py
```

Le script utilise uniquement la bibliothèque standard et fonctionne avec Python 3.11 ou plus récent. Il ne nécessite ni pandas, ni Spark, ni Docker, ni installation de dépendances pour cette préparation locale. Un nouveau téléchargement crée un instantané des métadonnées et actualise le manifeste. Pour travailler avec les données déjà partagées, privilégier `--audit-only`.

Il récupère dix fichiers du dataset existant sur Kaggle. Il ne collecte rien directement sur le site Hespress. À la seconde exécution, il vérifie les fichiers présents par leur empreinte et les réutilise. Un original modifié ou non reconnu provoque une erreur explicite.

Pour refaire seulement l'audit, sans accès réseau :

```powershell
python scripts/prepare_hespress.py --audit-only
```

Pour exécuter les tests :

```powershell
python -m unittest discover -s tests -p test_prepare_hespress.py -v
```

## 3. Comprendre la relation commentaire–article

Un fichier `comments_*.csv` contient :

| Champ | Signification |
|---|---|
| colonne sans nom | Ancien index d'export, à conserver dans l'original |
| `postId` | Identifiant de l'article auquel répond le commentaire |
| `comment` | Texte du commentaire |
| `score` | Votes de lecteurs, pas le sentiment |
| `topic` | Rubrique éditoriale de l'article |

Un fichier `stories_*.csv` contient : `id`, `title`, `date`, `author`, `story`, `topic`, avec un ancien index sans nom.

La jointure utilise `comments.postId = stories.id`. Elle permet de lire le titre et la rubrique de l'article lorsqu'un commentaire seul manque de contexte.

La date disponible est celle de l'article. Nous conservons sa valeur originale dans `article_date_raw`. Aucune date de commentaire ni aucun fuseau horaire n'est inventé.

## 4. Examiner l'échantillon

L'échantillon contient **200 commentaires par catégorie**, avec une graine de tirage fixée à 42. Les textes sont distincts dans l'échantillon, après retrait des espaces au début et à la fin pour la comparaison. Ce n'est pas un échantillon représentatif des proportions du corpus : les catégories sont volontairement équilibrées.

```powershell
Import-Csv data\processed\hespress\comments_sample.csv |
    Select-Object -First 5 sample_id, comment_text, article_topic, article_title
```

Vérifier la répartition :

```powershell
Import-Csv data\processed\hespress\comments_sample.csv |
    Group-Object article_topic |
    Select-Object Name, Count
```

La source de chaque ligne reste traçable avec `source_file`, `source_record` et `article_id`. `source_record` désigne le numéro d'enregistrement CSV, hors en-tête ; ce n'est pas nécessairement une ligne physique, car un commentaire peut contenir des retours à la ligne.

## 5. Pourquoi les labels restent-ils vides ?

- `article_topic` décrit le contexte éditorial. Il ne prouve pas le sujet réellement exprimé dans chaque commentaire.
- `theme_label` sera le thème vérifié par l'équipe.
- `sentiment` sera le sentiment annoté : `positive`, `neutral` ou `negative` lorsqu'il est identifiable.
- `language` vaut `inconnu` en attendant une vérification linguistique.
- `event_time` reste vide parce que le corpus ne fournit pas la date du commentaire.

Une annotation incertaine doit être discutée et documentée. L'absence d'opinion claire et l'impossibilité de comprendre un texte ne doivent pas être confondues.

Pour commencer une annotation, travailler sur des copies distinctes dans `data/annotated/`, en gardant le même `sample_id`. Deux personnes peuvent annoter les mêmes exemples indépendamment ; la troisième aide à résoudre les désaccords. Ne pas modifier les originaux.

Le script refuse de remplacer un échantillon dont les colonnes d'annotation ont été remplies. Garder les copies annotées séparément évite aussi de mélanger le tirage et les décisions humaines.

## Étape suivante

Examiner ensemble quelques commentaires, définir les règles d'annotation des thèmes et du sentiment, puis annoter un petit lot commun. Le contrat Kafka devra ensuite prévoir explicitement les dates de commentaires inconnues avant l'importation de ce corpus.

Les contrôles détaillés et les empreintes des dix fichiers figurent dans [HESPRESS_AUDIT.md](HESPRESS_AUDIT.md).
