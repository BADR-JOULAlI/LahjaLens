# Diagnostic du sentiment — 15 novembre 2026

## Décision et résultat

**Conserver AfriSenti `ary` et corriger la méthodologie d’entraînement et d’évaluation.**
Le déséquilibre global de 70 % ne décrit pas les fichiers AfriSenti actuellement utilisés.
Changer de corpus uniquement pour obtenir un meilleur score ne traiterait pas les
difficultés mesurées sur les textes en écriture latine.

Le [notebook 05](../../notebooks/05_sentiment_script_aware.ipynb) a été exécuté sur
le cluster Spark Docker. Il corrige une partie du biais, mais le modèle reste
une baseline expérimentale. Le gain face au meilleur ancien modèle est modeste
et n’est pas clairement établi par notre intervalle bootstrap.

Les chiffres ci-dessous viennent des fichiers locaux et des prédictions réelles.
Le [rapport JSON partageable](results/sentiment_20261115.json) contient les
empreintes des données, les paramètres, les matrices de confusion et les résultats
des huit candidats. Il ne contient aucun texte de commentaire. Les historiques
complets de l’objectif sont conservés dans le rapport local.

## Ce que l’audit a établi

### 1. Le corpus est assez équilibré globalement, mais pas conditionnellement à l’écriture

Le train contient 5 563 commentaires : 1 657 négatifs (29,8 %), 2 161 neutres
(38,8 %) et 1 745 positifs (31,4 %).

| Groupe du train | Negative | Neutral | Positive |
|---|---:|---:|---:|
| Écriture arabe dominante | 1 143 | 970 | 1 173 |
| Écriture latine dominante | 514 | 1 191 | 572 |

Un label globalement fréquent peut donc être rare dans une écriture. Pour les
textes latins, la part de neutres passe de **52,3 % dans le train à 29,8 % dans le
test**. Un modèle qui associe trop facilement cette écriture à « neutral » se
trompe lorsque cette association change.

L’écriture est ici déterminée par le nombre de caractères arabes et latins.
Cette heuristique ne prouve pas qu’un texte est en Darija : le groupe latin
inclut potentiellement du français, de l’Arabizi et des mélanges de langues.

### 2. Le test diffère du train et de dev

| Indicateur | Train | Dev | Test |
|---|---:|---:|---:|
| Nombre de commentaires | 5 563 | 492 | 2 961 |
| Écriture latine dominante | 40,9 % | 43,3 % | 52,5 % |
| Commentaires contenant une mention `@` | 0 | 0 | 1 246 (42,1 %) |

Ces différences observées indiquent un décalage de distribution. Elles ne
prouvent ni que le test est incorrect, ni que les auteurs ont mal annoté les
textes. Le papier AfriSenti décrit effectivement un corpus marocain à deux
écritures ; notre audit mesure la répartition des fichiers utilisés ici.
[Source : publication AfriSenti](https://aclanthology.org/2023.emnlp-main.862/).

### 3. Le score global masquait un échec local

Le modèle 04 avait un macro-F1 dev de **75,35 %**, mais :

- il reconnaissait **0 des 34 négatifs latins** de dev ;
- il reconnaissait seulement **4 des 531 négatifs latins** du test ;
- sur le test latin, son macro-F1 était de **32,02 %**.

Le macro-F1 global équilibre les trois sentiments. Il ne garantit pas une
performance équilibrée entre les écritures.

### 4. Ajouter des itérations ne suffit pas

Un témoin reprend les caractères 3–5, la normalisation L2, la pondération par
classe, la régularisation `0.001` et `standardization=False` du modèle 04.
Sur le nouveau partage interne, il converge après **237 itérations**, mais garde
un rappel nul des négatifs latins. L’arrêt à 80 itérations n’expliquait donc pas,
à lui seul, cette catégorie ignorée.

Le réglage de la régularisation dépend de l’échelle des caractéristiques.
Normaliser les documents en L2 et standardiser les variables dans la régression
logistique sont deux opérations distinctes.
[Documentation Spark 3.5.7](https://spark.apache.org/docs/3.5.7/api/python/reference/api/pyspark.ml.classification.LogisticRegression.html).

### 5. Un défaut de lecture CSV avait également été corrigé

Les anciens lecteurs acceptaient 5 562 lignes du train et 2 960 du test.
La gestion des guillemets était incompatible avec les CSV exportés par Python.

Les notebooks utilisent désormais `quote='"'`, `escape='"'`, `multiLine=True`
et une lecture stricte. Les notebooks 04 et 05 comparent chaque ligne lue par
Spark à la référence Python : **5 563 / 492 / 2 961 lignes**. Ce défaut portait sur
deux lignes et n’expliquait pas l’écart de performance entre les écritures.

## Ce qui a été changé et testé

1. **Nettoyage identique en entraînement et en prédiction** : retrait des mentions
   et URL, normalisation prudente de l’alif et des diacritiques, réduction des
   lettres répétées, espaces. Les chiffres Arabizi, négations et emojis sont
   conservés. Le texte source reste intact sur disque.
2. **Validation interne déterministe** : 4 453 commentaires pour apprendre les
   candidats, 1 110 pour les sélectionner, avec stratification par sentiment et
   écriture. Aucun recouvrement de texte normalisé n’a été trouvé entre les
   splits. Le vocabulaire et l’IDF sont appris uniquement sur la partie fit.
3. **Huit candidats prédéfinis** : caractères seuls ou mots + caractères,
   pondération par classe ou par couple écriture–sentiment, réglages de
   régularisation et de standardisation.
4. **Sélection sur la moyenne des macro-F1 par écriture**, uniquement sur la
   validation interne. Un candidat ignorant complètement un couple observé
   écriture–sentiment est inéligible.
5. **Réentraînement sur le train officiel complet**, puis évaluation dev/test
   après verrouillage du choix. Le modèle a convergé en 54 itérations.
6. **Relecture des anciens modèles** : comparaison sur les mêmes 2 961 lignes de
   test, contrôle du rechargement HDFS et de la conservation des données.

Le candidat sélectionné est `clean_wordchar_class` : unigrammes/bigrammes de
mots + caractères 3–5, TF-IDF, normalisation L2, régression logistique multinomiale,
`regParam=0.1`, `standardization=True`, pondération par classe.

**La pondération par écriture a été testée, mais n’a pas remporté la sélection.**
Elle reste une expérience comparée ; le modèle retenu ne l’utilise pas.
Les transformations de nettoyage et de création des caractéristiques sont
enregistrées dans le PipelineModel.

## Résultats comparables

Tous les modèles sont évalués sur les mêmes fichiers. Le modèle 03 historique
avait été appris avec une ligne de train en moins ; son score test ci-dessous
est recalculé sur le fichier complet.

| Modèle | Macro-F1 dev | Macro-F1 test | Accuracy test |
|---|---:|---:|---:|
| Ancien meilleur — notebook 03 | 69,64 % | 55,61 % | 56,57 % |
| Dernier essai — notebook 04 | 75,35 % | 51,40 % | 54,10 % |
| Nouvelle méthode — notebook 05 | 73,04 % | **56,99 %** | **58,02 %** |

| Indicateur sur le test | Notebook 03 | Notebook 04 | Notebook 05 |
|---|---:|---:|---:|
| Macro-F1, écriture arabe | 65,91 % | 69,12 % | 67,47 % |
| Macro-F1, écriture latine | 46,52 % | 32,02 % | 48,25 % |
| Rappel des négatifs latins | 30,32 % | 0,75 % | 27,31 % |

Le nouveau modèle améliore le macro-F1 moyen, mais **ne gagne pas sur tous les
indicateurs** : le rappel négatif latin reste inférieur à celui du modèle 03,
et le macro-F1 arabe reste inférieur à celui du modèle 04.

Bootstrap apparié exploratoire, 2 000 tirages, seed 20261115 :

- gain face au modèle 04 : **+5,59 points**, intervalle percentile 95 % :
  **[+3,80 ; +7,27] points** ;
- gain face au modèle 03 : **+1,38 point**, intervalle :
  **[−0,16 ; +2,90] points**. Cet intervalle contient zéro.

Ces intervalles décrivent la variabilité sur ce test ; ils ne corrigent pas le
biais d’avoir consulté ce benchmark pendant plusieurs itérations du projet.

## Ce qui reste réellement problématique

Le nouveau modèle atteint **100 % sur son train**, contre 76,17 % de macro-F1
sur la validation interne et 56,99 % sur le test. Il mémorise donc très bien les
exemples d’apprentissage et généralise encore insuffisamment. Il reste aussi
faible sur les négatifs latins : 145 reconnus sur 531.

Notre décision est de garder ce modèle comme **nouvelle baseline expérimentale**,
sans remplacer automatiquement un modèle en production. Les modèles précédents
sont conservés. Le projet n’a pas encore une qualité démontrée pour Hespress,
Facebook ou Instagram.

La prochaine collecte doit répondre au défaut observé :

1. Préparer un lot indépendant de commentaires de la source cible, séparé par
   article/publication et par provenance. Il servira uniquement à l’évaluation.
2. Faire annoter ce lot par les trois membres, avec double annotation et
   arbitrage. Ne pas déduire les sentiments des votes ou des catégories d’articles.
3. Constituer séparément un lot d’entraînement avec davantage de négatifs et de
   positifs en Arabizi, ainsi que des exemples ambigus, puis mesurer l’accord.
4. Comparer ensuite une représentation préentraînée adaptée à la Darija/Arabizi
   et une baseline plus régularisée, avec sélection sur validation et une seule
   évaluation finale sur le nouveau lot réservé.

Changer de dataset devient pertinent si un corpus réutilisable apporte ces
exemples manquants et une provenance documentée. Remplacer AfriSenti par un corpus
arbitraire, ou mélanger son test au train, rendrait les chiffres moins interprétables.

## Où retrouver l’exécution

- Run : `20261115T120646Z_7cae2241`.
- Application : `app-20261115120648-0006`.
- Rapport complet local :
  `data/processed/afrisenti/diagnostics/20261115T120646Z_7cae2241/report.json`.
- Affectation fit/validation : `inner_split.csv` dans le même dossier.
- Choix verrouillé avant dev/test : `selection_locked.json`.
- Prédictions : `new_dev_predictions.csv`, `new_test_predictions.csv` et celles
  des deux références, dans ce même dossier.
- Modèle HDFS :
  `hdfs://namenode:9000/data/lahjalens/models/afrisenti_script_v3/20261115T120646Z_7cae2241/model`.
- Métriques et prédictions HDFS : sous-dossiers `metrics` et `predictions` du run.

Les notebooks sont le code source. Aucun module Python d’entraînement n’a été ajouté.

Vérifications finales : rechargement sans différence de prédiction, inférence
sur cinq textes sans colonne de label ni de poids, deux workers Spark actifs,
HDFS `HEALTHY`, réplication moyenne de 2 et aucun bloc sous-répliqué. Les empreintes
des trois CSV sont identiques à celles précédant l'entraînement.
