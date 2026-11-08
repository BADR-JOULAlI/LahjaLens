"""VǸrification et affichage des indicateurs de la couche Gold."""

import os
from pyspark.sql import SparkSession


GOLD_BASE_PATH = os.getenv(
    "GOLD_BASE_PATH",
    "hdfs://namenode:9000/data/lahjalens/gold",
)


def main():
    spark = (
        SparkSession.builder
        .appName("Verify-LahjaLens-Gold")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kpi_paths = {
        "1. RǸpartition par Langue": f"{GOLD_BASE_PATH}/kpi_by_language",
        "2. RǸpartition par Source": f"{GOLD_BASE_PATH}/kpi_by_source",
        "3. Tendances Quotidiennes": f"{GOLD_BASE_PATH}/kpi_daily_trends",
        "4. Statistiques Textuelles": f"{GOLD_BASE_PATH}/kpi_text_stats",
        "5. Synthse Globale": f"{GOLD_BASE_PATH}/kpi_summary",
    }

    print("==================================================")
    print("      V%RIFICATION DE LA COUCHE HDFS GOLD         ")
    print("==================================================")

    for label, path in kpi_paths.items():
        print(f"\n>>> {label} ({path})")
        try:
            df = spark.read.parquet(path)
            print(f"SchǸma :")
            df.printSchema()
            print(f"Nombre de lignes : {df.count()}")
            print(f"DonnǸes :")
            df.show(truncate=False)
        except Exception as error:
            print(f"[ERREUR] Impossible de lire {path} : {error}")

    spark.stop()


if __name__ == "__main__":
    main()
