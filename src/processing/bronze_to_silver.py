"""Nettoyage des commentaires Bronze vers Silver."""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number


BRONZE_PATH = (
    "hdfs://namenode:9000/"
    "data/lahjalens/bronze/comments/kafka_stream"
)

SILVER_PATH = (
    "hdfs://namenode:9000/"
    "data/lahjalens/silver/comments/deduplicated"
)


def main():
    spark = (
        SparkSession.builder
        .appName("LahjaLens-Bronze-To-Silver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    bronze_df = spark.read.parquet(BRONZE_PATH)

    print("Nombre de lignes Bronze :", bronze_df.count())

    window = (
        Window.partitionBy("event_id")
        .orderBy(col("replay_time").desc())
    )

    silver_df = (
        bronze_df
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    print("Nombre de lignes Silver :", silver_df.count())

    (
        silver_df.write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )

    print("Écriture Silver terminée :", SILVER_PATH)

    spark.stop()


if __name__ == "__main__":
    main()