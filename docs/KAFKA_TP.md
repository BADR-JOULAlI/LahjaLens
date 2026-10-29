# TP Kafka : publier les premiers commentaires Hespress

## Objectif

Lire 100 commentaires réels de l'échantillon préparé et les publier dans le topic `comments.raw`. Le lot alterne les catégories : 20 commentaires pour politique, économie, société, sport et art et culture lorsque chaque catégorie dispose de suffisamment de lignes.

Le programme est `src/streaming/producer.py`. Il s'exécute depuis la racine avec `python -m src.streaming.producer`.

## Ce que fait le producteur

1. Il lit `data/processed/hespress/comments_sample.csv` avec un parseur CSV qui comprend les textes multilignes et l'UTF-8.
2. Il valide le fichier entier, les métadonnées et l'unicité des identifiants avant toute connexion Kafka. Une ligne invalide arrête l'opération avec son numéro d'enregistrement.
3. Il sélectionne le lot demandé en alternant les catégories disponibles.
4. Il conserve `sample_id` comme `event_id`, stable entre deux exécutions.
5. Il convertit les dates de commentaire vides en `null` et ajoute une heure réelle d'ingestion UTC.
6. Pour chaque envoi réel, il ajoute `replay_time`, sérialise le JSON en UTF-8 et utilise `event_id` comme clé Kafka.
7. Il attend la réponse du broker avant d'afficher `CONFIRME`, avec la partition et l'offset.

Un **topic** regroupe les messages d'une même catégorie technique. Une **partition** est un journal ordonné du topic. Un **offset** est la position d'un message dans cette partition. Le topic de ce TP a une partition sur notre broker de développement.

`article_topic` reste la rubrique de l'article. Le producteur n'en fait pas un thème prédit et ne publie pas les colonnes d'annotation `sentiment` et `theme_label`.

## 1. Vérifier le lot sans Kafka

Python 3.11 ou plus récent suffit pour cette commande, sans installation du client Kafka :

```powershell
python -m src.streaming.producer --dry-run --limit 100
```

Le programme affiche la répartition, un événement exemple et :

```text
VALIDATION OK : aucun message envoyé à Kafka.
```

Le champ `replay_time` vaut encore `null`, car aucun envoi n'a eu lieu. Le texte et le contexte proviennent du dataset. Les heures affichées décrivent l'exécution du programme et ne représentent pas des dates de publication historiques.

## 2. Préparer le client Python pour un envoi réel

Activer l'environnement Python géré par l'équipe. La dépendance `kafka-python-ng` figure déjà dans `requirements.txt` et `pyproject.toml`.

Si seul ce client manque dans l'environnement actif, cette installation ciblée suffit pour le TP :

```powershell
python -m pip install kafka-python-ng==2.2.3
```

Vérifier :

```powershell
python -c "import kafka; print(kafka.__version__)"
```

Pour ceux qui utilisent `uv`, une exécution isolée est également possible :

```powershell
uv run --no-project --python 3.11 --with kafka-python-ng==2.2.3 python -m src.streaming.producer --dry-run
```

Les commandes suivantes utilisent `python` dans l'environnement actif. Avec `uv`, remplacer ce préfixe par `uv run --no-project --python 3.11 --with kafka-python-ng==2.2.3 python` pour les envois.

## 3. Démarrer Kafka

Ouvrir Docker Desktop, attendre qu'il soit prêt, puis :

```powershell
docker compose up -d kafka
docker compose ps kafka
```

Créer le topic s'il n'existe pas encore :

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic comments.raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Vérifier sa description :

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --describe --topic comments.raw --bootstrap-server localhost:9092
```

La réplication Kafka vaut 1 car ce TP utilise un seul broker. La réplication HDFS à 2 concerne un autre service et ses deux DataNodes.

## 4. Terminal 1 : observer les messages

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic comments.raw --property print.key=true
```

Laisser le consommateur démarrer quelques secondes avant de lancer le producteur. Cette commande attend les nouveaux messages. Pour voir également l'historique, ajouter `--from-beginning` : d'anciens messages de tests manuels peuvent alors apparaître.

## 5. Terminal 2 : envoyer les 100 commentaires

```powershell
python -m src.streaming.producer --limit 100 --delay 0.2
```

`--delay 0.2` espace les messages confirmés de 0,2 seconde pour rendre le replay visible. Le programme affiche les confirmations individuelles, puis un bilan comme :

```json
{"status": "confirmed", "confirmed": 100, "topic": "comments.raw"}
```

Ce bilan n'est produit qu'après les confirmations. Le consommateur affiche la clé, puis le JSON contenant le commentaire et son contexte. `Ctrl+C` arrête le consommateur.

## Options utiles

Publier seulement des commentaires sportifs :

```powershell
python -m src.streaming.producer --category sport --limit 10 --delay 0.5
```

Valider deux catégories :

```powershell
python -m src.streaming.producer --category politique --category economie --limit 20 --dry-run
```

Utiliser un autre CSV préparé ou un autre broker :

```powershell
python -m src.streaming.producer --input data/processed/hespress/comments_sample.csv --bootstrap-servers localhost:9092 --topic comments.raw --dry-run
```

Le fichier d'entrée attendu est l'échantillon préparé, contenant déjà la jointure vers les articles. Les dix CSV originaux ne possèdent pas ce même schéma. Si la limite dépasse le nombre de commentaires disponibles, le programme annonce le nombre réellement sélectionné.

Les valeurs par défaut peuvent être fournies par les variables d'environnement du processus `KAFKA_BOOTSTRAP_SERVERS` et `KAFKA_TOPIC_COMMENTS_RAW`. Le producteur ne charge pas automatiquement un fichier `.env`. Les options de commande priment sur ces variables.

## Rejouer et gérer les erreurs

Un second lancement publie à nouveau les commentaires, avec les mêmes identifiants et de nouvelles heures d'exécution. Une clé stable permet de reconnaître un commentaire ; elle ne supprime pas automatiquement les doublons dans Kafka. La déduplication sera traitée explicitement dans le pipeline.

Le client attend `acks=all` et permet des réessais réseau limités. Cette configuration ne garantit pas une absence de doublons. En cas d'erreur ou de délai dépassé, le programme indique le nombre de messages confirmés et l'identifiant de l'envoi concerné. Un message sans confirmation peut malgré tout avoir atteint le broker : vérifier les offsets et les identifiants avant de relancer.

La version testée du client est `kafka-python-ng==2.2.3`. La documentation de développement d'autres versions peut proposer des options supplémentaires. Voir [la distribution et les exemples des mainteneurs](https://pypi.org/project/kafka-python-ng/) pour l'utilisation de `send(...).get(...)` et du JSON UTF-8.

## Tests et étape suivante

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

Les tests couvrent notamment le mode sans connexion, l'équilibrage des catégories, les identifiants dupliqués, les dates inconnues, la préservation de l'arabe et le comptage des confirmations en cas d'échec. Les tests simulant un client Kafka sont distincts d'une vérification réelle sur le broker.

Vérifications effectuées le 29 octobre 2026 :

- 42 tests réussis sous Python 3.11, dont les tests précédents des contrats et du téléchargement.
- Mode `--dry-run --limit 100` réussi sur le fichier partagé : 20 commentaires dans chacune des cinq catégories.
- Chargement du client `kafka-python-ng==2.2.3` vérifié dans un environnement `uv` isolé.
- La première tentative de connexion a échoué avec `NoBrokersAvailable`. Docker Desktop plantait sur `initializing Inference manager`, lié à un ancien socket `dockerInference` inaccessible.
- Après sauvegarde et recréation des dossiers de communication, Docker Engine 29.6.2 répond de nouveau et le conteneur Kafka est `Up`. Le topic `comments.raw` a conservé son identifiant et possède une partition avec son broker dans l'ISR.
- Connexion Python au broker vérifiée avec le producteur du projet : récupération des métadonnées de `comments.raw` et de la partition 0 réussie, sans publication de commentaires.
- L'envoi et la relecture des 100 commentaires sont laissés au test manuel dans les deux terminaux décrit plus haut. Aucun lot de commentaires n'a été envoyé lors du contrôle de connexion.

### Incident de démarrage Docker résolu le 29 octobre 2026

Le journal Docker montrait un échec sur `%LOCALAPPDATA%\Docker\run\dockerInference`. Le même problème et un contournement par renommage du dossier parent sont décrits dans [le signalement Docker #554](https://github.com/docker/desktop-feedback/issues/554).

Docker Desktop étant complètement arrêté, les dossiers `%LOCALAPPDATA%\Docker\run` et `%LOCALAPPDATA%\docker-secrets-engine` ont été sauvegardés par renommage, puis recréés. Les sauvegardes conservées sont :

```text
%LOCALAPPDATA%\Docker\run.backup-20261029-lahjalens
%LOCALAPPDATA%\docker-secrets-engine.backup-20261029-lahjalens
```

Les images, les conteneurs, les volumes et les réglages Docker ont été conservés. Seul le service Kafka du projet a été démarré pour ce TP. Les sauvegardes sont restées en place et aucune commande de nettoyage de données Docker n'a été exécutée. Cette intervention répond au journal observé ; elle ne doit pas être répétée pendant que Docker fonctionne.

La prochaine étape sera un consommateur Spark Structured Streaming pour archiver les événements dans HDFS Bronze avec un checkpoint. Il faudra alors configurer les listeners Kafka pour permettre aussi l'accès depuis les conteneurs Spark : l'adresse `localhost:9092` annoncée actuellement convient au producteur lancé sur le PC.
