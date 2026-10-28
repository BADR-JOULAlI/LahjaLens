# Choix des données pour LahjaLens

Recherche effectuée le 28 octobre 2026. Ce document propose un choix de départ pour l'équipe de trois personnes. Les statistiques annoncées par les auteurs et les vérifications effectuées sur les fichiers sont distinguées ci-dessous.

## 1. Décision proposée

Commencer par le corpus **Hespress de Tariq Massaoudi** pour l'ingestion, les catégories thématiques et la découverte de sujets. Sa fiche Kaggle déclare une licence CC0 et plus de 300 000 commentaires. Un fichier de commentaires et un fichier d'articles ont été effectivement lus pendant la recherche.

Retenir **MAC comme candidat pour entraîner le sentiment**, sous réserve de clarifier les conditions d'utilisation : aucun fichier de licence explicite n'apparaissait à la racine du dépôt inspecté. Prévoir en parallèle un petit jeu de commentaires Hespress annoté par l'équipe, pour évaluer les prédictions sur le domaine cible.

Retenir **DarijaMachaair comme piste prioritaire pour Facebook et Instagram**. La publication décrit des données adaptées, mais la page consultée indique encore un accès sur demande auprès des auteurs. Aucun fichier de ce corpus n'a été obtenu ou inspecté. Il ne doit pas conditionner le démarrage du projet.

Le premier livrable sera un audit d'importation et un échantillon traçable. Le développement des modèles viendra après cet audit.

## 2. Hespress : corpus principal pour les thèmes

- Source : <https://www.kaggle.com/datasets/tariqmassaoudi/hespress>
- Métadonnées vérifiées : <https://www.kaggle.com/api/v1/datasets/view/tariqmassaoudi/hespress>
- Liste des fichiers : <https://www.kaggle.com/api/v1/datasets/list/tariqmassaoudi/hespress>
- Licence déclarée par le déposant : **CC0: Public Domain**.
- Volume annoncé : plus de 10 000 articles et plus de 300 000 commentaires, non recomptés sur le corpus complet.
- Catégories disponibles : politique, économie, société, sport, art et culture, régions, faits divers, médias, orbites, Marocains du monde et tamazight.

### Vérification des fichiers

`comments_sport.csv` a été lu intégralement en mémoire : **8 110 enregistrements**, sans colonnes manquantes ou excédentaires selon le contrôle structurel CSV effectué. Cela ne constitue pas une validation complète de la qualité des textes.

Colonnes :

```text
index sans nom, postId, comment, score, topic
```

`stories_sport.csv` a également été inspecté : **1 000 articles**.

Colonnes :

```text
index sans nom, id, title, date, author, story, topic
```

Les champs `postId` et `id` permettent de préparer une jointure commentaire–article. Le taux de correspondance devra être mesuré à l'importation.

### Ce que nous pouvons construire

- Un flux de commentaires historiques rejoués dans Kafka.
- Une ingestion HDFS Bronze, puis des transformations Spark Silver et Gold.
- Des statistiques de volume par catégorie.
- Une découverte de sous-sujets à l'intérieur des catégories.
- Une association entre sujets et sentiments prédits, après évaluation du modèle.

### Limites à conserver dans les résultats

- `score` représente des votes de lecteurs, pas le sentiment du commentaire.
- `topic` correspond à la rubrique de l'article : c'est une étiquette de contexte, pas une annotation humaine du sujet exact de chaque commentaire.
- Une catégorie d'article peut servir de supervision faible. Un échantillon annoté manuellement reste nécessaire pour évaluer la classification des commentaires.
- Aucune date individuelle de commentaire n'apparaît dans le fichier inspecté. La date de l'article doit rester `article_published_at`, jamais une fausse date de commentaire.
- Une analyse regroupant les réactions par date d'article devra l'annoncer explicitement. Elle ne mesure pas le rythme réel de publication des commentaires.
- Il s'agit d'un corpus historique : il ne décrit pas les tendances actuelles de Facebook ou Instagram.
- Pour évaluer un modèle sur les commentaires, séparer les articles entre entraînement et test afin d'éviter que des réactions au même article apparaissent des deux côtés.

## 3. MAC : candidat pour le sentiment

- Dépôt des auteurs : <https://github.com/LeMGarouani/MAC>
- Fichier inspecté : <https://github.com/LeMGarouani/MAC/blob/main/MAC%20corpus.csv>
- Nombre de lignes lues : **18 087**.
- Colonnes : `tweets`, `type`, `class`.

Répartition réelle du champ `type` :

| Label | Nombre |
|---|---:|
| positive | 9 897 |
| neutral | 4 039 |
| negative | 3 508 |
| mixed | 643 |
| Total | 18 087 |

Le corpus n'a donc pas exactement trois classes. Pour l'expérience à trois sentiments, conserver **17 444 lignes avant nettoyage et déduplication**, et isoler explicitement les 643 lignes `mixed`. Ne pas les convertir automatiquement en `neutral`.

Le champ `class` distingue `standard` (12 611 lignes), `dialectal` (5 464) et une variante orthographique `diatectal` (12). Tout MAC ne doit pas être présenté comme exclusivement de la Darija. La normalisation de cette variante et le périmètre linguistique doivent être documentés.

Le fichier ne fournit ni date ni thème. Son usage concerne l'apprentissage et l'évaluation du sentiment, pas la mesure de tendances temporelles. Aucun fichier de licence explicite n'a été trouvé dans la liste des fichiers à la racine ; clarifier ce point avant de l'adopter définitivement ou de redistribuer ses données.

## 4. Autres pistes examinées

| Corpus | Apport possible | Limite pour notre démarrage |
|---|---|---|
| MYC | Commentaires YouTube en écritures arabe et latine, sentiment binaire | Le fichier nettoyé inspecté contient `sentence` et `polarity`, sans dates ni thèmes. Licence explicite non trouvée à la racine. |
| ElecMorocco2016 | 10 254 commentaires Facebook sur les élections marocaines de 2016 selon les auteurs | Un seul domaine politique ne permet pas d'apprendre à distinguer tous les thèmes. Licence explicite non trouvée à la racine ; fichier XLSX non audité. |
| MSDA | La publication décrit des annotations de sentiment et de thèmes, avec plusieurs dialectes dont le marocain | Le lien historique de téléchargement n'était pas exploitable pendant la recherche. Les volumes des différentes tâches ne doivent pas être confondus ni présentés comme exclusivement marocains. |
| Hespress COVID-19 | Fichier contenant notamment `date`, `comment`, `language` | Domaine COVID restreint, qualité des dates à auditer et licence non trouvée. Ne pas fusionner automatiquement avec le corpus Hespress principal. |
| DarijaMachaair | Plus de 117 870 commentaires annoncés, provenant de Facebook, YouTube, Instagram et TikTok ; sentiments, émotions et contextes | Accès sur demande selon la publication. Dates, thèmes, licence de réutilisation et métadonnées par plateforme restent à vérifier dans les fichiers. |
| Arabic news and public opinion dataset from YouTube | Plus de deux millions de commentaires et réponses annoncés, avec métadonnées temporelles et licence CC BY 4.0 | Corpus arabe général. La part réellement marocaine/Darija et les fichiers n'ont pas été vérifiés. Piste secondaire pour les tendances. |

MYC : le CSV nettoyé est encodé en UTF-16. Le fichier brut présente un emballage CSV particulier avec des lignes entre guillemets. Un importateur spécifique et des contrôles de lignes seront nécessaires. Les fichiers brut et nettoyé représentent des versions du même corpus et ne doivent pas être additionnés comme des sources indépendantes.

DarijaBench réutilise notamment des sous-ensembles de MAC, MYC et MSDA. Il ne doit pas servir de test indépendant après entraînement sur leurs lignes communes. Sa fiche précise aussi que les sous-ensembles de sentiment ne disposent pas d'une licence fournie.

## 5. Conditions nécessaires pour les tendances

Trois questions correspondent à trois mesures différentes :

1. **Sujets les plus présents** : compter les commentaires par sujet dans le corpus, sans exiger de date.
2. **Évolution historique** : comparer des périodes à partir de dates réelles, en précisant s'il s'agit de dates de commentaires ou de dates d'articles.
3. **Tendances actuelles Facebook/Instagram** : obtenir un flux récent autorisé avec plateforme, identifiants de publication et horodatages réels.

L'heure du replay Kafka sert à la démonstration technique. Elle ne remplace pas la date originale et ne prouve aucune tendance sociale réelle.

Les statistiques décriront le corpus observé. Leur volume ne permet pas, à lui seul, de conclure sur l'ensemble des utilisateurs marocains ou sur toute une plateforme.

## 6. Première étape de travail

Commencer par les catégories `politique`, `economie`, `societe`, `sport`, `art-et-culture`.

1. Récupérer les fichiers de commentaires et d'articles de ces catégories en conservant les originaux.
2. Enregistrer leur URL, version, date de récupération et empreinte SHA-256.
3. Contrôler l'encodage, les colonnes, les valeurs manquantes et les doublons.
4. Mesurer la jointure `postId` vers `id` et vérifier les catégories.
5. Produire un échantillon reproductible, cible de 1 000 commentaires, réparti entre les catégories et les articles.
6. Faire annoter un sous-ensemble par deux membres, puis résoudre les désaccords avec le troisième. Distinguer sujet du commentaire, contexte de l'article et sentiment.
7. Préparer ensuite la conversion en événements Kafka et l'archivage HDFS.

Le contrat actuel rend `event_time` obligatoire. Avant d'importer un corpus sans date de commentaire, prévoir explicitement une valeur inconnue et un champ indiquant l'origine du temps utilisé. Conserver séparément `article_published_at`, `ingestion_time` et `replay_time`. Cette évolution du contrat n'est pas implémentée par le présent document.

### Répartition immédiate entre trois personnes

| Personne | Travail en parallèle | Livrable |
|---|---|---|
| 1 | Hespress : politique et économie, audit des commentaires/articles | Fiche source et contrôles de qualité |
| 2 | Hespress : société, sport et culture, jointure et qualité des catégories | Échantillon traçable et bilan des métadonnées |
| 3 | MAC : labels, langues et conditions d'utilisation ; préparer une demande d'accès à DarijaMachaair | Fiche sentiment et état de l'accès aux données sociales |

L'annotation, la validation du contrat et l'intégration sont communes aux trois personnes. Aucun message aux auteurs n'a été envoyé pendant cette recherche.

## 7. Sources complémentaires

- MYC, dépôt des auteurs : <https://github.com/MouadJb/MYC>
- MYC, publication : <https://arxiv.org/abs/2303.15987>
- ElecMorocco2016, dépôt des auteurs : <https://github.com/sentiprojects/ElecMorocco2016>
- MSDA, publication et statistiques par tâche : <https://arxiv.org/html/2102.11000v1>
- Hespress COVID-19, dépôt distinct : <https://github.com/HankarM88/Hespress_COVID-19_Dataset>
- DarijaMachaair, publication et conditions d'accès : <https://link.springer.com/article/10.1007/s10579-026-09945-5>
- Corpus arabe YouTube, fiche et licence : <https://data.mendeley.com/datasets/3mnjw5hjkh/1>
- DarijaBench, provenance et limites de licence : <https://huggingface.co/datasets/MBZUAI-Paris/DarijaBench/blob/main/README.md>

Pendant la recherche initiale, les fichiers ont seulement été inspectés en mémoire. Après validation du choix, les dix CSV des cinq catégories Hespress ont été téléchargés le 28 octobre 2026 : **139 166 commentaires et 5 000 articles**, avec 100 % de correspondances commentaire–article dans chaque catégorie. Un échantillon de 1 000 commentaires sans labels inventés a été préparé.

Voir [le guide de récupération et de lecture](HESPRESS_GUIDE.md) et [le rapport d'audit](HESPRESS_AUDIT.md). Aucun modèle n'a été entraîné et aucun résultat de performance n'est revendiqué.

Les dix originaux Hespress et leur provenance sont désormais destinés au partage dans le dépôt GitHub, avec l'échantillon initial. Les autres datasets et les copies d'annotation individuelles restent locaux. Les CSV partagés conservent leurs octets grâce aux règles `.gitattributes`, ce qui permet de contrôler les empreintes après récupération sur un autre système.
