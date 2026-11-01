from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify-Kafka-HDFS")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

path = (
    "hdfs://namenode:9000/"
    "data/lahjalens/bronze/comments/kafka_stream"
)

df = spark.read.parquet(path)

print("\n========== SCHEMA ==========")
df.printSchema()

print("\n========== NOMBRE DE LIGNES ==========")
print("Total :", df.count())
print("\n========== IDENTIFIANTS DUPLIQUES ==========")

duplicates = (
    df.groupBy("event_id")
    .count()
    .filter("count > 1")
)

duplicates.show(truncate=False)

print("Identifiants distincts :", df.select("event_id").distinct().count())

print("\n========== EVENEMENTS ==========")
df.select(
    "event_id",
    "source",
    "language",
    "comment_text",
    "replay_time",
    "ingestion_time"
).show(truncate=80, vertical=True)

print("\n========== VERIFICATION SILVER ==========")

silver_path = (
    "hdfs://namenode:9000/"
    "data/lahjalens/silver/comments/deduplicated"
)

silver_df = spark.read.parquet(silver_path)

print("Nombre de lignes Silver :", silver_df.count())

print(
    "Nombre d'identifiants distincts :",
    silver_df.select("event_id").distinct().count()
)

print("\n========== DOUBLONS SILVER ==========")

silver_duplicates = (
    silver_df.groupBy("event_id")
    .count()
    .filter("count > 1")
)

silver_duplicates.show(truncate=False)

print("\n========== DONNEES SILVER ==========")

silver_df.select(
    "event_id",
    "source",
    "language",
    "replay_time"
).show(truncate=False)

spark.stop()
