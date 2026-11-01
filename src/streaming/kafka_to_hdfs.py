
"""Consommation Kafka avec Spark Structured Streaming et stockage HDFS."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    date_format,
    from_json,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)
import os


# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "comments.raw")

HDFS_OUTPUT_PATH = os.getenv(
    "HDFS_OUTPUT_PATH",
    "hdfs://namenode:9000/data/lahjalens/bronze/comments/kafka_stream",
)

HDFS_CHECKPOINT_PATH = os.getenv(
    "HDFS_CHECKPOINT_PATH",
    "hdfs://namenode:9000/data/lahjalens/bronze/checkpoints/kafka_to_hdfs",
)


def main():
    spark = (
        SparkSession.builder
        .appName("LahjaLens-Kafka-To-HDFS")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Schéma conforme au contrat CommentEvent.
    event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("comment_text", StringType(), True),
        StructField("source", StringType(), True),
        StructField("language", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("article_date_raw", StringType(), True),
        StructField("replay_time", StringType(), True),
        StructField("ingestion_time", StringType(), True),
    ])

    # Lecture du flux Kafka.
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Conversion du JSON Kafka en colonnes structurées.
    parsed_stream = (
        kafka_stream
        .select(
            from_json(
                col("value").cast("string"),
                event_schema,
            ).alias("event")
        )
        .select("event.*")
    )

    # Conservation des événements respectant les champs obligatoires.
    valid_stream = parsed_stream.filter(
        col("event_id").isNotNull()
        & (col("event_id") != "")
        & col("comment_text").isNotNull()
        & (col("comment_text") != "")
        & col("source").isNotNull()
        & col("language").isNotNull()
        & col("replay_time").isNotNull()
    )

    # ingestion_time correspond à l'heure réelle de traitement
    # si le producteur ne l'a pas renseignée.
    bronze_stream = valid_stream.withColumn(
        "ingestion_time",
        coalesce(
            col("ingestion_time"),
            date_format(
                current_timestamp(),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            ),
        ),
    )

    # Écriture continue au format Parquet dans HDFS.
    query = (
        bronze_stream.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", HDFS_OUTPUT_PATH)
        .option("checkpointLocation", HDFS_CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("LahjaLens : consommateur Kafka demarre.")
    print(f"Topic Kafka : {KAFKA_TOPIC}")
    print(f"Destination HDFS : {HDFS_OUTPUT_PATH}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
