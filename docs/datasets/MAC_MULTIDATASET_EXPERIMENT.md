# Audit MAC et expérimentation multi-datasets

## Exécution de référence

- Notebook : `notebooks/07_mac_multidataset_experiment.ipynb`
- Run : `20261115T190123Z_9cb0026f`
- Application Spark : `app-20261115190128-0005`
- Cluster : `spark://spark-master:7077`, Spark 3.5.7, réplication HDFS 2
- Source MAC : commit `9fa9d89dcb0d485f12c81cd09a6681b8dae2b828`
- SHA-256 du fichier source : `094e624dfdd65e4d3522251a66183ee6eb089c5a55593e8739c48588fd109f61`

Les sorties détaillées sont générées dans `data/processed/mac_multidataset/20261115T190123Z_9cb0026f/` et les modèles dans HDFS :

```text
hdfs://namenode:9000/data/lahjalens/models/sentiment/experiments/mac_multidataset/20261115T190123Z_9cb0026f/e0
hdfs://namenode:9000/data/lahjalens/models/sentiment/experiments/mac_multidataset/20261115T190123Z_9cb0026f/e1
hdfs://namenode:9000/data/lahjalens/models/sentiment/experiments/mac_multidataset/20261115T190123Z_9cb0026f/e2
```

## Audit du corpus MAC

Le corpus contient 18 087 lignes et les champs `tweets`, `type`, `class`.

- Textes vides : 12.
- Labels bruts : 9 897 `positive`, 4 039 `neutral`, 3 508 `negative`, 643 `mixed`.
- Doublons exacts : 5 453 lignes ; doublons normalisés : 5 698 lignes dans 4 110 groupes.
- Groupes à labels contradictoires : 58, soit 391 lignes exclues de l’apprentissage.
- `mixed` est exclu comme sentiment ambigu ; il n’est jamais transformé en `neutral`.
- Textes uniques à label unique après nettoyage logique : 11 893.
- Recouvrement normalisé avec AfriSenti train/dev/test : 0.
- Écriture détectée : 18 074 arabe, 1 mixte, 12 autres.
- Proxy Arabizi MAC : 0 ligne détectée ; ce proxy est heuristique et ne constitue pas une annotation linguistique.

Le dépôt officiel ne contient pas de fichier `LICENSE` au commit audité. Le README décrit le corpus comme open/free, mais cela ne suffit pas à autoriser la redistribution brute. Le fichier MAC brut reste donc local et n’est pas publié dans le dépôt LahjaLens.

## Expériences

Les trois expériences utilisent le même preprocessing que NB05 : suppression des URL/mentions pour la représentation modèle, normalisation arabe et répétitions, TF-IDF mots unigrammes/bigrammes et caractères 3–5, normalisation L2, régression logistique multinomiale et poids de classes.

| Expérience | Entraînement |
|---|---|
| E0 | AfriSenti uniquement, 5 563 lignes |
| E1 | MAC uniquement, 9 558 lignes |
| E2 | AfriSenti + MAC, 15 121 lignes |

Partition MAC déterministe : train 9 558, dev 1 163, test 1 172. Le test AfriSenti reste le même test réservé de 2 961 identifiants pour les trois modèles.

## Résultats observés

| Évaluation | Modèle | Accuracy | Macro-F1 | F1 négatif | Rappel négatif |
|---|---:|---:|---:|---:|---:|
| AfriSenti test, n=2 961 | E0 | 58,02 % | 56,99 % | 44,40 % | 42,71 % |
| AfriSenti test, n=2 961 | E1 | 43,94 % | 38,99 % | 21,75 % | 15,76 % |
| AfriSenti test, n=2 961 | E2 | 58,26 % | 57,15 % | 46,33 % | 42,35 % |
| MAC test, n=1 172 | E0 | 60,75 % | 50,69 % | 53,61 % | 74,81 % |
| MAC test, n=1 172 | E1 | 74,66 % | 71,39 % | 69,58 % | 67,83 % |
| MAC test, n=1 172 | E2 | 75,17 % | 71,59 % | 70,54 % | 72,87 % |

Bootstrap apparié, 2 000 rééchantillonnages, graine `20261115` :

- AfriSenti, E2 − E0 : +0,16 point de Macro-F1, IC95 % [-1,16 ; +1,56]. Le gain global n’est pas démontré.
- AfriSenti, E1 − E0 : −18,00 points, IC95 % [-20,24 ; -15,61]. MAC seul ne généralise pas à AfriSenti.
- MAC, E2 − E0 : +20,90 points, IC95 % [+17,42 ; +24,26].
- MAC, E2 − E1 : +0,19 point, IC95 % [-1,86 ; +2,33]. E2 n’améliore pas significativement E1 sur son propre domaine.

Sur AfriSenti, E2 augmente le rappel négatif Latin de 27,51 % à 30,93 % et le rappel négatif du proxy Arabizi de 28,01 % à 31,29 %, mais son Macro-F1 Latin baisse de 48,02 % à 47,01 %. Le groupe mixte ne compte que 75 exemples et doit rester interprété avec prudence. MAC ne contient pas de groupe Latin/Arabizi exploitable.

Les distributions de prédictions, erreurs fréquentes, matrices de confusion et intervalles par groupe sont dans `evaluation/results.json`. Les visualisations sont :

- `plots/macro_f1_comparison.svg`
- `plots/confusion_afrisenti_test.svg`
- `plots/confusion_mac_test.svg`

## Recommandation expérimentale

Conserver **E2 comme référence expérimentale multi-datasets**, avec **E0 comme baseline AfriSenti** et **E1 comme baseline MAC spécialisée**. E2 est le seul modèle qui reste au niveau E0 sur AfriSenti tout en gagnant fortement sur MAC, mais il ne faut pas le présenter comme une amélioration universelle : le gain AfriSenti est compatible avec zéro et le gain sur MAC mesure surtout l’adaptation au domaine MAC.

La prochaine amélioration prioritaire n’est pas d’ajouter davantage de MAC brut : il faut constituer un corpus Latin/Arabizi réellement représentatif et annoté, puis valider sur Hespress avec une séparation par article et familles de quasi-doublons.

## Protocole Hespress indépendant

Les commentaires Hespress non annotés ne sont pas un benchmark de sentiment. Le protocole proposé est :

1. Échantillonner environ 1 200 commentaires par article, catégorie et période, sans utiliser les prédictions pour choisir les exemples.
2. Calculer une clé normalisée, un regroupement de quasi-doublons et une famille d’article ; aucune famille ne doit être partagée entre annotation de développement et test final.
3. Sur-échantillonner les écritures Latin, Arabizi proxy et mixtes. Viser au minimum 200 exemples Latin/Arabizi et 100 exemples mixtes dans la réserve annotée, avec les trois sentiments représentés dans chaque groupe autant que possible.
4. Faire annoter chaque commentaire indépendamment par deux annotateurs avec `negative`, `neutral`, `positive` et un indicateur séparé `mixed/ambiguous`. `mixed` ne doit pas être forcé vers `neutral`.
5. Mesurer l’accord inter-annotateurs par classe et macro-F1 d’accord ; arbitrer les désaccords avec un troisième avis ou une règle documentée.
6. Figer une partie finale, par exemple 20 % des familles, avant toute sélection de modèle. Les labels finaux ne doivent servir qu’à l’évaluation finale.
7. Publier uniquement les statistiques, identifiants anonymisés et artefacts autorisés ; ne pas publier le texte brut sans vérifier les droits de Hespress.

Ce benchmark devra rapporter l’accuracy, le Macro-F1, les métriques par classe, la matrice de confusion, les résultats par écriture et la performance sur Arabizi/mixte avec intervalles de confiance.

## MLflow et reproductibilité

Les runs E0, E1 et E2 ont été enregistrés dans l’expérience `LahjaLens-MAC-multidataset`. Les chemins HDFS sont versionnés par `RUN_ID` et le notebook refuse `overwrite` sur un chemin existant. Les anciennes sorties de développement restent distinctes et ne sont pas utilisées comme résultats finaux.
