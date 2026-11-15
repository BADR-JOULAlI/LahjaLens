# Benchmark indépendant Hespress — protocole proposé

## Statut et objectif

**Protocole à mettre en œuvre, pas benchmark déjà annoté.** L'audit précédent
a observé 1 000 commentaires dans l'échantillon de travail, sans label de
sentiment, avec seulement 51 textes latins et 29 mixtes selon une heuristique.
Le notebook 06 actualise ces effectifs sans attribuer de sentiment.

L'objectif est de mesurer le transfert d'un modèle figé vers les commentaires
Hespress et, séparément, ses limites en Arabizi et dans les textes mixtes.
Les votes `score`, les rubriques `topic` et les prédictions ne sont jamais des labels.

## 1. Définir les lots avant l'annotation finale

Cibles initiales proposées, ajustables avant l'ouverture des labels finaux :

| Lot | Cible | Sélection | Usage |
|---|---:|---|---|
| Développement | 300 commentaires | Catégories et écritures variées | Guide d'annotation, analyse d'erreurs, choix des modèles |
| Final représentatif | 600 commentaires | Échantillonnage reflétant le corpus cible | Estimation de la performance sur Hespress |
| Final défi | 300 commentaires au départ | Enrichissement Arabizi vérifié et mixte | Diagnostic conditionnel, jamais présenté comme représentatif |

Le nombre de commentaires ne garantit pas le support de chaque classe.
Pour le lot défi, fixer avant l'annotation les cibles suivantes :

- **Arabizi vérifié : au moins 200 commentaires**, dont 100 négatifs, 50 neutres
  et 50 positifs.
- **Mixte arabe/latin vérifié : au moins 200 commentaires**, avec au moins
  50 exemples de chacune des trois classes.
- Au moins 30 composantes article/doublon distinctes par groupe, afin de ne pas
  confondre un effectif élevé de commentaires avec un grand nombre de cas indépendants.

Les groupes peuvent se recouper : publier leur intersection. Atteindre ces cibles
peut nécessiter plus de 300 commentaires dans le lot défi et une annotation
nettement plus large. Avec 100 négatifs, l'incertitude d'un rappel reste de l'ordre
de plusieurs points à une dizaine de points, davantage en présence de dépendances.
Ces cibles sont des seuils de planification, pas une garantie de puissance.
Si elles sont inaccessibles dans le corpus, étendre la collecte sur de nouveaux
articles ou déclarer le groupe insuffisamment évalué ; ne pas fabriquer de textes
pour compléter un test. Ce sont des objectifs, pas des effectifs acquis.

Une éventuelle sélection par classe du lot défi appartient uniquement au
gestionnaire du benchmark, après annotation, suivant une règle préenregistrée.
Elle ne doit utiliser ni les prédictions ni les erreurs d'un modèle. Les quotas
et probabilités de sélection sont publiés. Ses résultats restent distincts de
ceux du lot représentatif ; ne pas fusionner leurs scores sans pondération justifiée.

## 2. Séparer par articles et familles de doublons

1. Conserver l'identifiant stable du commentaire, son article, sa catégorie et
   la provenance du fichier.
2. Calculer une empreinte textuelle sur une copie normalisée, sans modifier le brut.
3. Construire les familles de doublons exacts et de quasi-doublons vérifiés.
4. Relier les commentaires par **article commun OU famille de doublons commune**.
   Affecter chaque composante connexe entière à un seul lot.
5. Contrôler les recouvrements avec AfriSenti train/dev/test et avec tout corpus
   déjà utilisé pour développer le modèle.
6. Effectuer l'affectation avec une graine fixe et enregistrer le manifeste des
   groupes, les règles, les exclusions et les empreintes.

Limiter le nombre de commentaires par article pour éviter qu'une publication
domine l'évaluation. Si une très grande composante empêche les quotas, documenter
son traitement au lieu de la couper entre apprentissage et benchmark.

Le lot final représentatif doit garder les proportions de la population cible,
et non les 200 commentaires par catégorie de l'ancien échantillon de travail.
Les quotas d'écriture du lot défi sont contrôlés humainement : latin ≠ Arabizi.

## 3. Guide d'annotation

Chaque annotateur renseigne indépendamment :

| Champ | Valeurs ou rôle |
|---|---|
| `sentiment` | negative, neutral, positive |
| `language_verified` | darija, arabe, français, autre, mixte, indéterminé |
| `writing_system_verified` | arabe, latin, mixte, autre |
| `arabizi_verified` | oui, non, incertain |
| `needs_context` | Le texte seul permet-il de décider ? |
| `ambiguity_reason` | ironie, cible ambiguë, sentiment mixte, texte incompréhensible… |
| `annotation_note` | Justification courte |
| `annotator_id`, `guide_version` | Traçabilité |

Annoter le sentiment exprimé dans le commentaire, pas le sentiment de
l'annotateur envers son opinion. Définir dans le pilote le traitement des
insultes, de l'ironie, des citations et des sentiments contradictoires.

Ne pas convertir automatiquement « difficile à comprendre » ou « manque de
contexte » en neutral. Prévoir un statut d'arbitrage `unresolved`, distinct
des trois classes ML. Publier la couverture et les exclusions préétablies,
plutôt que faire disparaître discrètement les cas difficiles.

Pour l'évaluation principale du modèle actuel, les annotateurs voient le
commentaire seul, comme le modèle. Une annotation nécessitant le contexte
d'article peut être étudiée séparément, sans mélanger les deux tâches.

Masquer les votes, le modèle, ses prédictions et, pour cette tâche, la rubrique
éditoriale. Le texte original demeure accessible ; les auteurs ne sont pas
profilés et les informations personnelles ne sont pas exportées publiquement.

## 4. Organisation pour trois membres

- Deux membres annotent indépendamment, sans voir les décisions de l'autre.
- Le troisième arbitre à partir du guide et conserve les désaccords initiaux.
- Les rôles peuvent tourner sur le **développement**.
- Une personne gère les manifests et l'accès aux labels du benchmark final.

Réaliser un pilote de 50 à 100 commentaires **du développement**. Examiner les
désaccords, réviser le guide, puis figer sa version avant l'annotation finale.
Rapporter accord brut, matrice des désaccords et une mesure corrigée du hasard
(kappa/Cohen pour deux annotateurs ou alpha de Krippendorff), par groupe lorsque
les effectifs le permettent.

**Avec une équipe de trois personnes qui participe aussi au ML :** sélectionner
les modèles et geler leur code, hyperparamètres et artefacts **avant** que ces
membres annotent ou consultent les labels finaux. Cela évite de prétendre qu'un
benchmark est indépendant alors que ses annotations ont guidé le développement.

## 5. Fichiers et accès proposés

Ces fichiers sont à créer ultérieurement, dans un espace local ou privé :

- `group_manifest.csv` : identifiant, article, famille, composante, lot, graine.
- `development_annotations.csv` : labels utilisés pour le développement.
- `final_annotation_A.csv`, `final_annotation_B.csv` : annotations aveugles.
- `final_adjudication.csv` : décision finale et motif d'arbitrage.
- `benchmark_manifest.json` : version, règles, empreintes, effectifs et exclusions.

Ne pas publier automatiquement les textes, les annotations individuelles ou
les fichiers contenant des informations sensibles. Les chiffres agrégés et les
règles de constitution peuvent être partagés séparément.

## 6. Évaluation finale et critères de décision

Avant de révéler les labels finaux, figer :

- les modèles candidats et leurs empreintes ;
- les métriques principales et sous-groupes ;
- les règles de traitement des cas non résolus ;
- la méthode statistique et les critères d'acceptation.

Mesures : Macro-F1, précision/rappel/F1 par classe, matrices, couverture,
rappel négatif en **Arabizi vérifié**, résultats des textes mixtes, intervalles
de confiance. Effectuer un bootstrap apparié **par composante article/doublon**,
et non seulement par ligne, pour respecter les dépendances.

Présenter séparément le lot représentatif et le lot défi. Une amélioration
globale ne suffit pas si une régression importante affecte un groupe prioritaire.
Les marges de non-régression doivent être choisies sur les besoins du projet
avant la consultation du benchmark.

Après cette évaluation, tout réglage inspiré des résultats transforme ce lot en
donnée de développement pour les versions suivantes : réserver alors une nouvelle
cohorte finale. Aucun score de benchmark ne sera annoncé avant les annotations.
