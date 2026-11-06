# UrgenceNet : pipeline Big Data pour un réseau hospitalier

Projet Écosystème Big Data 2, EMSI 5e année IIR, 2026-2027.
Analyse des passages aux urgences et des transferts entre établissements avec
Spark, Kafka, MinIO, Delta Lake, GraphFrames et GraphX. Données entièrement synthétiques.

## Architecture

```
producer.py -> Kafka (topic urgences, 3 partitions) -> Spark Structured Streaming
-> MinIO (bucket lake : Bronze -> Silver -> Gold) -> GraphFrames / GraphX -> Parquet
```

## Versions

| Composant | Version |
|---|---|
| Apache Spark | 3.5.3 (Scala 2.12, Java 17, Python 3.10) |
| Kafka | 3.7.0 (mode KRaft) |
| Delta Lake | 3.2.0 |
| Hadoop S3A / AWS SDK | 3.3.4 / 1.12.262 |
| MinIO | quay.io/minio/minio:latest |
| GraphFrames | 0.8.4-spark3.5-s_2.12 |
| GraphX | livré avec Spark 3.5.3 (aucun paquet à ajouter) |
| Scala (spark-shell) | 2.12 |

## Prérequis

Docker Desktop, 8 Go de RAM minimum. Cluster Spark : 1 Master et 2 Workers
(2 cœurs et 2 Go chacun, soit 4 cœurs au total).

## Lancement

```powershell
docker compose build spark-master      # une seule fois (construit l'image)
docker compose up -d
docker compose ps -a
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic urgences --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092
```

Le bucket `lake` est créé automatiquement par le service `minio-init`.
Ne pas utiliser `docker compose up --build` : les 4 services Spark partagent la même image,
et Docker Desktop échoue en la construisant 4 fois en parallèle.

## Génération des données

```powershell
docker compose exec jupyter python3 /workspace/data/gen_facilities.py
docker compose exec jupyter python3 /workspace/data/producer.py --mode file --n 50000
docker compose exec jupyter python3 -c "from minio import Minio; c=Minio('minio:9000','minioadmin','minioadmin123',secure=False); c.fput_object('lake','bronze/history/urgences_history.jsonl','/workspace/data/urgences_history.jsonl'); c.fput_object('lake','bronze/reference/facilities.json','/workspace/data/facilities.json')"
```

Flux Kafka (Parties 4 et 5) :

```powershell
docker compose exec jupyter python3 /workspace/data/producer.py --mode kafka --rate 20
```

- `producer.py` et `gen_facilities.py` sont des reconstitutions conformes au sujet.
- Le temps simulé va 20 fois plus vite que le temps réel (`--speedup 20`, option `--speedup 1` pour le temps réel).
- Le générateur injecte aussi des cas inhabituels pour la Partie 6 (ping-pong A->B->A->B->A,
  chaînes de 4 transferts, transferts inter-groupes vers un non-CHU).
  Options `--anomaly-rate 0 --violation-rate 0` pour les désactiver.

## Exécution des parties (ordre)

| Notebook / fichier | Contenu |
|---|---|
| `notebooks/01_cluster.ipynb` | Partie 1 : cluster, SparkSession |
| `notebooks/02_rdd.ipynb` | Partie 2 : RDD |
| `notebooks/03_dataframes.ipynb` | Partie 3 : DataFrames, SQL, UDF, Silver historique |
| `notebooks/04_streaming.ipynb` | Partie 4 : Structured Streaming, Bronze, watermark |
| `notebooks/05_pipeline.ipynb` | Partie 5 : Silver streaming, fenêtres, Delta Gold, reprise |
| `notebooks/06_graphes.ipynb` | Partie 6 : GraphFrames, anomalies, métriques, visualisation |
| `scala/graphx_analysis.scala` | Partie 6 : GraphX (Scala) |

Règle importante : un seul notebook Spark actif à la fois. Chaque application prend les
4 cœurs du cluster. Faire `spark.stop()` en fin de notebook, sinon le suivant reste en attente
(`Initial job has not accepted any resources`).

Pour la Partie 5, repartir d'un topic vide avant de lancer le producteur (le watermark
serait faussé par d'anciens messages) :

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic urgences --bootstrap-server kafka:9092
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic urgences --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092
```

## Partie 6 : GraphFrames (PySpark)

Le jar `graphframes-0.8.4-spark3.5-s_2.12` est chargé par la session du notebook 06.
Valider avec un `GraphFrame(vertices, edges).inDegrees`.
`connectedComponents` exige un répertoire de checkpoint :

```python
spark.sparkContext.setCheckpointDir("s3a://lake/checkpoints/graphframes/")
```

Limite observée : l'algorithme par défaut de `connectedComponents` a échoué avec
`OutOfMemoryError` (driver). On utilise `connectedComponents(algorithm="graphx")`.

## Partie 6 : GraphX (Scala)

Le notebook 06 exporte d'abord les tables du graphe en Parquet
(`gold/graph_analysis/input/vertices`, `edges`, `transfer_weights`).
Fermer la session PySpark (`spark.stop()`), puis lancer :

```powershell
docker compose exec jupyter /opt/spark/bin/spark-shell `
  --master spark://spark-master:7077 `
  --driver-memory 1g --executor-memory 1g --total-executor-cores 4 `
  --conf spark.driver.host=jupyter `
  --conf spark.driver.bindAddress=0.0.0.0 `
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
  --conf spark.hadoop.fs.s3a.access.key=minioadmin `
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 `
  --conf spark.hadoop.fs.s3a.path.style.access=true `
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false `
  -i /workspace/scala/graphx_analysis.scala
```

Le script construit un `Graph(verticesRDD, edgesRDD)` pondéré (40 établissements, 325 paires,
poids = nombre de transferts), puis calcule par groupe hospitalier `degrees`,
`connectedComponents` et `staticPageRank(20)`. Résultat écrit dans
`gold/graph_analysis/graphx/metrics` (partitionné par `hospital_group`).
Ne pas interrompre le calcul avec Ctrl+C : les variables du shell sont perdues.

## Résultats de la Partie 6

| Contrôle | Valeur |
|---|---|
| Transferts | 1 185 (aucune destination inconnue, aucun auto-transfert) |
| Transferts inter-groupes | 138 (137 vers un CHU, 1 violation : P-04788, F-012 -> F-026) |
| Composantes (GraphX et GraphFrames) | 1 par groupe (Atlantique 20, Nord 10, Sud 10) |
| Degrés GraphX contre GraphFrames | identiques sur le même sous-graphe (écart total = 2 x 138 transferts inter-groupes) |

Règles d'anomalies (exports sous `gold/graph_analysis/anomalies/`, séparés par `hospital_group`) :

| Règle | Seuil | Résultat |
|---|---|---|
| R1 : transferts par patient | >= 4 | 79 patients |
| R2 : boucle A->B->A | présence | 142 transferts, 57 patients |
| R3 : inter-groupe vers un non-CHU | présence | 1 cas |
| R4 : part des transferts reçus du groupe | < 0,8 | 3 établissements (F-031, F-021, F-011) |

La centralité (PageRank) décrit la structure du réseau, pas un dysfonctionnement.

## Organisation du lac (bucket `lake`)

```
bronze/history/            urgences_history.jsonl
bronze/reference/          facilities.json
bronze/urgences_raw/       JSON bruts lus dans Kafka
silver/urgences_history/   Parquet batch, partitionné par city (Partie 3)
silver/urgences_clean/     Parquet streaming, partitionné par city et date (Partie 5)
gold/kpi_facility_10min/   Delta Lake : KPI par fenêtre de 10 minutes
gold/graph_analysis/       Parquet : entrées, résultats GraphFrames/GraphX, anomalies, métriques
checkpoints/               checkpoints Structured Streaming
```

## Interfaces

| Service | URL |
|---|---|
| Jupyter | http://localhost:8888 |
| Spark Master | http://localhost:8080 |
| Spark UI (application active) | http://localhost:4040 |
| Console MinIO | http://localhost:9001 (minioadmin / minioadmin123) |

## Arrêt

```powershell
docker compose down        # conserve le volume MinIO
```

## Notes

- Le client `mc` n'est pas dans l'image (téléchargement retiré par MinIO) : le bucket est
  créé avec le paquet Python `minio`.
- Le sink `console` écrit dans les logs du conteneur : `docker compose logs -f jupyter`.
- Les checkpoints des requêtes de streaming sont sur MinIO (un checkpoint local `/tmp`
  provoque des erreurs d'état quand une tâche change de Worker).
- Temps mesurés : RDD 5,98 s, DataFrame 3,57 s (Parties 2 et 3) ; UDF 8,83 s, `when/otherwise` 4,54 s ;
  GraphX 134,6 s pour 3 groupes (`staticPageRank(20)`, 2 partitions).