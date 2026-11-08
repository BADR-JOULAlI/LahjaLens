"""Calcul des indicateurs analytiques Spark SQL depuis Silver vers Gold."""

import os
import sys

# Aligner le worker et le driver Spark sur le mme interprteur Python
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, DataFrame


SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "hdfs://namenode:9000/data/lahjalens/silver/comments/deduplicated",
)

GOLD_BASE_PATH = os.getenv(
    "GOLD_BASE_PATH",
    "hdfs://namenode:9000/data/lahjalens/gold",
)

# Configuration PostgreSQL pour restitution optionnelle
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "lahjalens_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "lahja_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me")


def get_spark_session(app_name: str = "LahjaLens-Silver-To-Gold") -> SparkSession:
    """Initialise ou rǸcupre une session Spark configurǸe en UTC."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def compute_kpi_by_language(spark: SparkSession) -> DataFrame:
    """Indicateur 1 : RǸpartition des commentaires par langue."""
    query = """
        SELECT
            language,
            COUNT(*) AS total_comments,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
        FROM silver_comments
        GROUP BY language
        ORDER BY total_comments DESC
    """
    return spark.sql(query)


def compute_kpi_by_source(spark: SparkSession) -> DataFrame:
    """Indicateur 2 : Volume des commentaires par source / rubrique thǸmatique."""
    query = """
        SELECT
            source,
            COUNT(*) AS total_comments,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
        FROM silver_comments
        GROUP BY source
        ORDER BY total_comments DESC
    """
    return spark.sql(query)


def compute_kpi_daily_trends(spark: SparkSession) -> DataFrame:
    """Indicateur 3 : %volutions temporelles quotidiennes par source."""
    query = """
        SELECT
            COALESCE(
                TO_DATE(event_time),
                TO_DATE(replay_time),
                TO_DATE(ingestion_time)
            ) AS comment_date,
            source,
            COUNT(*) AS total_comments
        FROM silver_comments
        GROUP BY 1, 2
        ORDER BY comment_date DESC, total_comments DESC
    """
    return spark.sql(query)


def compute_kpi_text_stats(spark: SparkSession) -> DataFrame:
    """Indicateur 4 : Statistiques textuelles pour prǸparation ML et qualitǸ."""
    query = """
        SELECT
            language,
            COUNT(*) AS total_comments,
            ROUND(AVG(LENGTH(comment_text)), 1) AS avg_char_length,
            MIN(LENGTH(comment_text)) AS min_char_length,
            MAX(LENGTH(comment_text)) AS max_char_length,
            ROUND(AVG(SIZE(SPLIT(TRIM(comment_text), '\\\\s+'))), 1) AS avg_word_count
        FROM silver_comments
        GROUP BY language
        ORDER BY total_comments DESC
    """
    return spark.sql(query)


def compute_kpi_summary(spark: SparkSession) -> DataFrame:
    """Indicateur 5 : Synthse globale de la santǸ de la couche Silver."""
    query = """
        SELECT
            COUNT(*) AS total_silver_comments,
            COUNT(DISTINCT source) AS total_sources,
            COUNT(DISTINCT language) AS total_languages,
            MIN(COALESCE(event_time, replay_time)) AS oldest_event,
            MAX(COALESCE(event_time, replay_time)) AS newest_event
        FROM silver_comments
    """
    return spark.sql(query)


def save_to_hdfs_gold(df: DataFrame, destination_path: str) -> None:
    """Sauvegarde un DataFrame d'indicateurs dans HDFS Gold en Parquet."""
    (
        df.write
        .mode("overwrite")
        .parquet(destination_path)
    )
    print(f"[Gold HDFS] %crit avec succs : {destination_path}")


def export_to_postgres(df: DataFrame, table_name: str) -> None:
    """Exporte un DataFrame vers PostgreSQL pour exploitation dans Streamlit."""
    try:
        from sqlalchemy import create_engine

        db_uri = (
            f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
        engine = create_engine(db_uri)

        # Conversion sǸcurisǸe en pandas pour Ǹcriture directe SQLAlchemy
        pandas_df = df.toPandas()
        pandas_df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False,
        )
        print(f"[PostgreSQL] Table '{table_name}' mise  jour ({len(pandas_df)} lignes).")
    except Exception as error:
        print(f"[PostgreSQL] Export ignorǸ pour '{table_name}' : {error}")


def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"=== Lecture de la couche Silver depuis : {SILVER_PATH} ===")
    silver_df = spark.read.parquet(SILVER_PATH)
    silver_count = silver_df.count()
    print(f"Nombre total de commentaires Silver : {silver_count}")

    if silver_count == 0:
        print("[Attention] La couche Silver est vide. ArrǦt du traitement.")
        spark.stop()
        return

    # CrǸation de la vue temporaire pour requǦtes SQL
    silver_df.createOrReplaceTempView("silver_comments")

    indicators = {
        "kpi_by_language": (
            compute_kpi_by_language(spark),
            f"{GOLD_BASE_PATH}/kpi_by_language",
            "gold_kpi_by_language",
        ),
        "kpi_by_source": (
            compute_kpi_by_source(spark),
            f"{GOLD_BASE_PATH}/kpi_by_source",
            "gold_kpi_by_source",
        ),
        "kpi_daily_trends": (
            compute_kpi_daily_trends(spark),
            f"{GOLD_BASE_PATH}/kpi_daily_trends",
            "gold_kpi_daily_trends",
        ),
        "kpi_text_stats": (
            compute_kpi_text_stats(spark),
            f"{GOLD_BASE_PATH}/kpi_text_stats",
            "gold_kpi_text_stats",
        ),
        "kpi_summary": (
            compute_kpi_summary(spark),
            f"{GOLD_BASE_PATH}/kpi_summary",
            "gold_kpi_summary",
        ),
    }

    print("\n========== AFFICHAGE ET SAUVEGARDE DES INDICATEURS GOLD ==========\n")

    enable_pg = os.getenv("ENABLE_POSTGRES_EXPORT", "false").lower() in ("true", "1", "yes")

    for name, (kpi_df, hdfs_path, pg_table) in indicators.items():
        print(f"\n--- {name.upper()} ---")
        kpi_df.show(truncate=False)

        # 1. Sauvegarde dans HDFS Gold
        save_to_hdfs_gold(kpi_df, hdfs_path)

        # 2. Export optionnel vers PostgreSQL
        if enable_pg:
            export_to_postgres(kpi_df, pg_table)

    print("\n=== Traitement Silver -> Gold terminǸ avec succs. ===")
    spark.stop()


if __name__ == "__main__":
    main()
