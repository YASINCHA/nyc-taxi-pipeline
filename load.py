# ==============================================================================
# load.py — Chargement PySpark : processed/ → PostgreSQL
#
# CORRECTIONS APPLIQUÉES :
#   1. Credentials lus depuis os.environ (plus de hardcode)
#   2. Paramètres year/month via argparse (dates dynamiques depuis le DAG)
#   3. Pattern staging + swap atomique (évite la perte de données sur overwrite)
#   4. Logging structuré (remplace les print())
#   5. sys.exit(1) sur exception (Airflow détecte le code de retour)
#   6. Validation du DataFrame avant l'écriture (colonne attendues présentes ?)
#   7. Affichage du volume traité et des statistiques de chargement
# ==============================================================================

import argparse
import logging
import os
import sys

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, sum as _sum, count as _count

# ─────────────────────────────────────────────────────────────────────────────
# Configuration du logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Credentials lus depuis l'environnement
# CORRECTION : plus de credentials codés en dur
# ─────────────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
BUCKET_NAME = os.environ.get("MINIO_BUCKET", "nytaxi")

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "nyc_taxi_dw")
PG_USER = os.environ.get("POSTGRES_USER")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

# Colonnes minimales requises dans le DataFrame source
REQUIRED_COLUMNS = {
    "tpep_pickup_datetime",
    "total_amount",
    "trip_distance",
    "passenger_count",
}


def validate_env() -> None:
    """Vérifie que toutes les variables d'environnement critiques sont définies."""
    missing = []
    for var in ["MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "POSTGRES_USER", "POSTGRES_PASSWORD"]:
        if not os.environ.get(var):
            missing.append(var)
    if missing:
        log.error("Variables d'environnement manquantes : %s", ", ".join(missing))
        sys.exit(1)


def create_spark_session(app_name: str = "NYC_Taxi_Load_To_Postgres") -> SparkSession:
    """Crée et retourne une SparkSession configurée pour MinIO + PostgreSQL."""
    return (
    SparkSession.builder
    .appName(app_name)
    .master("spark://spark:7077")       # ← mode cluster
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.postgresql:postgresql:42.6.0",
    )
    .config("fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("fs.s3a.endpoint.region", "us-east-1")
    .config("fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enabled", "false")
    .config(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
    )


def validate_schema(df: DataFrame) -> None:
    """Vérifie que les colonnes requises sont présentes dans le DataFrame."""
    df_columns = set(df.columns)
    missing_cols = REQUIRED_COLUMNS - df_columns
    if missing_cols:
        log.error(
            "Colonnes manquantes dans les données source : %s",
            ", ".join(missing_cols),
        )
        sys.exit(1)


def get_jdbc_url() -> str:
    return f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"


def get_jdbc_properties() -> dict:
    return {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver",
    }


def atomic_swap_tables(target_table: str, staging_table: str) -> None:
    old_table = f"{target_table}_old"
    log.info("Swap atomique : %s → %s (via %s)", staging_table, target_table, old_table)

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {old_table} CASCADE")
            cur.execute(f"ALTER TABLE IF EXISTS {target_table} RENAME TO {old_table}")
            cur.execute(f"ALTER TABLE {staging_table} RENAME TO {target_table}")
            cur.execute(f"DROP TABLE IF EXISTS {old_table} CASCADE")
            # Recréer la vue analytique si c'est la table Gold
            if target_table == "daily_taxi_summary":
                cur.execute("""
                    CREATE OR REPLACE VIEW v_weekly_performance AS
                    SELECT
                        TO_CHAR(trip_date, 'Day')           AS day_of_week,
                        EXTRACT(DOW FROM trip_date)         AS dow_number,
                        ROUND(AVG(revenue)::NUMERIC, 2)     AS avg_daily_revenue,
                        ROUND(AVG(trip_count)::NUMERIC, 0)  AS avg_daily_trips,
                        COUNT(*)                            AS data_points
                    FROM daily_taxi_summary
                    GROUP BY TO_CHAR(trip_date, 'Day'), EXTRACT(DOW FROM trip_date)
                    ORDER BY dow_number
                """)
        conn.commit()
        log.info("Swap atomique réussi pour '%s'.", target_table)
    except Exception as e:
        conn.rollback()
        log.error("Erreur lors du swap atomique pour '%s' : %s", target_table, e)
        raise
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Chargement PySpark des données NYC Taxi vers PostgreSQL."
    )
    parser.add_argument("--year", required=True, type=int, help="Année (ex: 2026)")
    parser.add_argument("--month", required=True, type=str, help="Mois format 2 chiffres (ex: 01)")
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    args = parse_args()
    year = args.year
    month = args.month.zfill(2)

    log.info("=== Démarrage du chargement : year=%d, month=%s ===", year, month)

    validate_env()

    input_path = f"s3a://{BUCKET_NAME}/processed/yellow_taxi/year={year}/month={month}/"
    jdbc_url = get_jdbc_url()
    jdbc_props = get_jdbc_properties()

    spark = create_spark_session()
    log.info("Session Spark configurée et démarrée.")

    try:
        # ── 1. Lecture des données nettoyées depuis processed/ ────────────────
        log.info("Lecture des données depuis : %s", input_path)
        df = spark.read.parquet(input_path)

        row_count = df.count()
        log.info("Lignes lues depuis la zone processed : %d", row_count)

        if row_count == 0:
            log.error(
                "La zone processed/ est vide pour year=%d month=%s. "
                "L'étape transform a-t-elle bien été exécutée ?",
                year, month,
            )
            sys.exit(1)

        # ── 2. Validation du schéma ───────────────────────────────────────────
        validate_schema(df)

        # ── 3. Chargement Silver : données détaillées ─────────────────────────
        # CORRECTION : écriture vers une table staging, puis swap atomique
        # Au lieu de : df.write.mode("overwrite").jdbc(table="final_taxi_data")
        log.info("Écriture vers la table staging 'final_taxi_data_staging'...")
        df.write.mode("overwrite").jdbc(
            url=jdbc_url,
            table="final_taxi_data_staging",
            properties=jdbc_props,
        )
        log.info("Écriture staging Silver terminée (%d lignes).", row_count)

        atomic_swap_tables("final_taxi_data", "final_taxi_data_staging")
        log.info("Table Silver 'final_taxi_data' mise à jour avec succès.")

        # ── 4. Agrégation Gold : résumé quotidien ─────────────────────────────
        log.info("Calcul du résumé quotidien (Gold)...")
        df_summary = (
            df.withColumn("trip_date", to_date(col("tpep_pickup_datetime")))
            .groupBy("trip_date")
            .agg(
                _sum("total_amount").alias("revenue"),
                _sum("trip_distance").alias("total_distance"),
                _count("*").alias("trip_count"),  # Ajout du nombre de courses
            )
        )

        summary_count = df_summary.count()
        log.info("Résumé quotidien calculé : %d jours couverts.", summary_count)

        # ── 5. Chargement Gold avec swap atomique ─────────────────────────────
        log.info("Écriture vers la table staging 'daily_taxi_summary_staging'...")
        df_summary.write.mode("overwrite").jdbc(
            url=jdbc_url,
            table="daily_taxi_summary_staging",
            properties=jdbc_props,
        )

        atomic_swap_tables("daily_taxi_summary", "daily_taxi_summary_staging")
        log.info("Table Gold 'daily_taxi_summary' mise à jour avec succès.")

        log.info(
            "=== Chargement terminé avec succès : %d lignes Silver, %d jours Gold ===",
            row_count,
            summary_count,
        )

    except Exception as e:
        log.error("Erreur critique lors du chargement : %s", e, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
