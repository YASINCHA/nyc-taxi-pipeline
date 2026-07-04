# ==============================================================================
# transform.py — Transformation PySpark : raw/ → processed/
#
# AMÉLIORATIONS :
#   1. Credentials lus depuis os.environ
#   2. Paramètres year/month via argparse
#   3. Logging structuré
#   4. Mode cluster Spark (spark://spark:7077)
#   5. Contrôles qualité avancés (6 assertions)
#   6. Monitoring : écriture des métriques dans pipeline_metrics (PostgreSQL)
#   7. Idempotence : partitionOverwriteMode=dynamic (écrase uniquement le mois traité)
# ==============================================================================

import argparse
import logging
import os
import sys
import time

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, year as spark_year, month as spark_month

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Credentials
# ─────────────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
ACCESS_KEY     = os.environ.get("MINIO_ACCESS_KEY")
SECRET_KEY     = os.environ.get("MINIO_SECRET_KEY")
BUCKET_NAME    = os.environ.get("MINIO_BUCKET", "nytaxi")

PG_HOST     = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT     = os.environ.get("POSTGRES_PORT", "5432")
PG_DB       = os.environ.get("POSTGRES_DB", "nyc_taxi_dw")
PG_USER     = os.environ.get("POSTGRES_USER")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

REJECTION_THRESHOLD  = 0.30
DATE_ERROR_THRESHOLD = 0.05
TIME_ERROR_THRESHOLD = 0.02


# ─────────────────────────────────────────────────────────────────────────────
# Monitoring — écriture dans pipeline_metrics
# ─────────────────────────────────────────────────────────────────────────────
def write_metrics(year: int, month: int, total_rows: int, valid_rows: int,
                  rejected_rows: int, duration_seconds: float,
                  status: str, error_message: str = None) -> None:
    """Enregistre les métriques du run dans la table pipeline_metrics."""
    rejection_rate = round(rejected_rows / total_rows * 100, 2) if total_rows > 0 else 0.0
    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD,
        )
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_metrics
                    (script_name, year, month, total_rows, valid_rows,
                     rejected_rows, rejection_rate_pct, duration_seconds,
                     status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                "transform.py", year, month, total_rows, valid_rows,
                rejected_rows, rejection_rate, round(duration_seconds, 2),
                status, error_message,
            ))
        conn.commit()
        log.info("Métriques enregistrées dans pipeline_metrics (status=%s, durée=%.2fs).",
                 status, duration_seconds)
    except Exception as e:
        log.warning("Impossible d'écrire les métriques : %s", e)
    finally:
        if conn:
            conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession avec idempotence (partitionOverwriteMode=dynamic)
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session(app_name: str = "NYC_Taxi_Transform") -> SparkSession:
    """
    Crée une SparkSession en mode cluster avec :
    - Connexion MinIO via s3a
    - partitionOverwriteMode=dynamic pour l'idempotence :
      Spark écrase uniquement la partition year/month traitée,
      pas l'intégralité du Data Lake.
    """
    if not ACCESS_KEY or not SECRET_KEY:
        log.error("Variables MINIO_ACCESS_KEY / MINIO_SECRET_KEY manquantes !")
        sys.exit(1)

    return (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark:7077")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        # ── Connexion MinIO ──────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # ── Idempotence : écrase uniquement la partition traitée ─────────
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.writeLegacyFormat", "false")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Filtres qualité de base
# ─────────────────────────────────────────────────────────────────────────────
def apply_quality_filters(df: DataFrame) -> DataFrame:
    """
    Applique les règles de qualité sur le DataFrame brut.
    Centralise toutes les règles pour faciliter les évolutions futures.
    """
    return df.filter(
        (col("trip_distance") > 0)
        & (col("total_amount") > 0)
        & (col("tpep_pickup_datetime").isNotNull())
        & (col("tpep_dropoff_datetime").isNotNull())
        & (col("passenger_count") > 0)
        & (col("total_amount") < 10_000)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Contrôles qualité avancés
# ─────────────────────────────────────────────────────────────────────────────
def run_quality_checks(df: DataFrame, year: int, month: int) -> None:
    """
    6 assertions de qualité sur le DataFrame nettoyé.
    Lève une ValueError si une règle critique est violée.
    """
    log.info("=== Démarrage des contrôles qualité ===")
    errors = []
    row_count = df.count()

    # 1. Volume non vide
    if row_count == 0:
        raise ValueError("CRITIQUE : DataFrame vide après filtrage.")
    log.info("Contrôle 1/6 — Volume : %d lignes ✅", row_count)

    # 2. Dates cohérentes avec year/month
    date_check = df.filter(
        (spark_year(col("tpep_pickup_datetime")) != year) |
        (spark_month(col("tpep_pickup_datetime")) != month)
    ).count()
    if date_check > 0:
        pct = date_check / row_count
        if pct > DATE_ERROR_THRESHOLD:
            errors.append(
                f"QUALITÉ : {date_check} courses ({pct*100:.2f}%) dates incohérentes "
                f"(seuil : {DATE_ERROR_THRESHOLD*100:.0f}%)."
            )
        else:
            log.warning("Contrôle 2/6 — Dates incohérentes : %d (%.2f%%) — sous le seuil ⚠️",
                        date_check, pct * 100)
    else:
        log.info("Contrôle 2/6 — Dates cohérentes ✅")

    # 3. Montants non négatifs
    neg_amounts = df.filter(col("total_amount") < 0).count()
    if neg_amounts > 0:
        errors.append(f"QUALITÉ : {neg_amounts} courses avec montant négatif.")
    else:
        log.info("Contrôle 3/6 — Montants positifs ✅")

    # 4. Distances non négatives
    neg_dist = df.filter(col("trip_distance") < 0).count()
    if neg_dist > 0:
        errors.append(f"QUALITÉ : {neg_dist} courses avec distance négative.")
    else:
        log.info("Contrôle 4/6 — Distances positives ✅")

    # 5. Colonnes obligatoires non nulles
    required_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime",
                     "total_amount", "trip_distance"]
    null_errors = []
    for col_name in required_cols:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            null_errors.append(f"'{col_name}' : {null_count} nulls")
    if null_errors:
        errors.append("QUALITÉ : Valeurs nulles — " + ", ".join(null_errors))
    else:
        log.info("Contrôle 5/6 — Colonnes obligatoires non nulles ✅")

    # 6. Pickup strictement avant Dropoff
    invalid_times = df.filter(
        col("tpep_pickup_datetime") >= col("tpep_dropoff_datetime")
    ).count()
    if invalid_times > 0:
        pct = invalid_times / row_count
        if pct > TIME_ERROR_THRESHOLD:
            errors.append(
                f"QUALITÉ : {invalid_times} courses ({pct*100:.2f}%) "
                f"pickup >= dropoff (seuil : {TIME_ERROR_THRESHOLD*100:.0f}%)."
            )
        else:
            log.warning("Contrôle 6/6 — Pickup >= dropoff : %d (%.2f%%) — sous le seuil ⚠️",
                        invalid_times, pct * 100)
    else:
        log.info("Contrôle 6/6 — Pickup < dropoff ✅")

    # Résultat final
    if errors:
        log.error("=== ÉCHEC des contrôles qualité : %d erreur(s) ===", len(errors))
        for err in errors:
            log.error("  ✗ %s", err)
        raise ValueError(
            f"{len(errors)} contrôle(s) qualité échoué(s).\n" + "\n".join(errors)
        )

    log.info("=== Tous les contrôles qualité sont passés ✅ ===")


# ─────────────────────────────────────────────────────────────────────────────
# Arguments CLI
# ─────────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transformation PySpark des données NYC Taxi (raw → processed)."
    )
    parser.add_argument("--year",  required=True, type=int, help="Année (ex: 2026)")
    parser.add_argument("--month", required=True, type=str, help="Mois 2 chiffres (ex: 01)")
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    args  = parse_args()
    year  = args.year
    month = args.month.zfill(2)

    log.info("=== Démarrage de la transformation : year=%d, month=%s ===", year, month)

    # Chemin d'entrée : partition spécifique du mois brut
    input_path = f"s3a://{BUCKET_NAME}/raw/yellow_taxi/year={year}/month={month}/"

    # Chemin de sortie : racine de la zone processed/ (partitionnée dynamiquement)
    # IDEMPOTENCE : Spark écrase uniquement year={year}/month={month}/
    # grâce à partitionOverwriteMode=dynamic — les autres mois ne sont pas touchés
    output_base = f"s3a://{BUCKET_NAME}/processed/yellow_taxi/"

    spark      = create_spark_session()
    start_time = time.time()
    total_rows = 0
    valid_rows = 0

    try:
        # ── 1. Lecture des données brutes ─────────────────────────────────
        log.info("Lecture depuis : %s", input_path)
        df         = spark.read.parquet(input_path)
        total_rows = df.count()
        log.info("Lignes brutes : %d", total_rows)

        if total_rows == 0:
            log.error("Fichier source vide. Vérifiez l'étape d'ingestion.")
            write_metrics(year, int(month), 0, 0, 0,
                          time.time() - start_time, "FAILED", "Fichier source vide.")
            sys.exit(1)

        # ── 2. Filtres qualité de base ────────────────────────────────────
        df_cleaned    = apply_quality_filters(df)
        valid_rows    = df_cleaned.count()
        rejected_rows = total_rows - valid_rows
        rejection_rate = rejected_rows / total_rows

        log.info("Nettoyage : %d valides / %d rejetées (%.1f%%)",
                 valid_rows, rejected_rows, rejection_rate * 100)

        if rejection_rate > REJECTION_THRESHOLD:
            log.warning("Taux de rejet élevé : %.1f%% > %.0f%%",
                        rejection_rate * 100, REJECTION_THRESHOLD * 100)

        # ── 3. Contrôles qualité avancés ──────────────────────────────────
        run_quality_checks(df_cleaned, year, int(month))

        # ── 4. Ajout des colonnes de partition ────────────────────────────
        # Nécessaire pour que Spark sache dans quelle partition écrire
        df_partitioned = df_cleaned \
            .withColumn("year",  lit(year)) \
            .withColumn("month", lit(int(month)))

        # ── 5. Sauvegarde avec partition dynamique (idempotent) ───────────
        log.info(
            "Sauvegarde vers : %s (partition year=%d/month=%d)",
            output_base, year, int(month)
        )
        df_partitioned.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(output_base)

        duration = time.time() - start_time

        # ── 6. Métriques de monitoring ────────────────────────────────────
        write_metrics(year, int(month), total_rows, valid_rows,
                      rejected_rows, duration, "SUCCESS")

        log.info("=== Transformation terminée avec succès en %.2fs ===", duration)

    except Exception as e:
        duration = time.time() - start_time
        write_metrics(year, int(month), total_rows, valid_rows,
                      total_rows - valid_rows, duration, "FAILED", str(e))
        log.error("Erreur critique : %s", e, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()