# ==============================================================================
# spark_streaming_consumer.py — Consommateur Spark Structured Streaming
#
# Lit les courses NYC Taxi depuis Kafka en temps réel
# Agrège par fenêtre de 1 minute et écrit dans PostgreSQL
# ==============================================================================

import os
import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, sum as _sum,
    count as _count, avg, round as _round
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType
)

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
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC           = "nyc_taxi_rides"
MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT", "minio:9000")
ACCESS_KEY      = os.environ.get("MINIO_ACCESS_KEY", "admin")
SECRET_KEY      = os.environ.get("MINIO_SECRET_KEY", "supersecretpassword123")

PG_HOST     = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT     = os.environ.get("POSTGRES_PORT", "5432")
PG_DB       = os.environ.get("POSTGRES_DB", "nyc_taxi_dw")
PG_USER     = os.environ.get("POSTGRES_USER", "data_engineer")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "password123")

# ─────────────────────────────────────────────────────────────────────────────
# Schéma des messages Kafka
# ─────────────────────────────────────────────────────────────────────────────
RIDE_SCHEMA = StructType([
    StructField("vendor_id",          IntegerType(), True),
    StructField("pickup_datetime",    StringType(),  True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id",IntegerType(), True),
    StructField("passenger_count",    IntegerType(), True),
    StructField("trip_distance",      DoubleType(),  True),
    StructField("fare_amount",        DoubleType(),  True),
    StructField("tip_amount",         DoubleType(),  True),
    StructField("tolls_amount",       DoubleType(),  True),
    StructField("total_amount",       DoubleType(),  True),
    StructField("payment_type",       IntegerType(), True),
])


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("NYC_Taxi_Streaming")
        .master("spark://spark:7077")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.6.0",
        )
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_streaming_checkpoint")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Écriture vers PostgreSQL (foreach batch)
# ─────────────────────────────────────────────────────────────────────────────
def write_to_postgres(batch_df, batch_id):
    """Écrit chaque micro-batch dans la table streaming_metrics."""
    if batch_df.count() == 0:
        return

    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    jdbc_props = {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    batch_df.write.mode("append").jdbc(
        url=jdbc_url,
        table="streaming_metrics",
        properties=jdbc_props,
    )
    log.info("Batch %d écrit dans streaming_metrics (%d lignes).",
             batch_id, batch_df.count())


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=== Démarrage du Spark Structured Streaming ===")
    log.info("Topic Kafka : %s | Bootstrap : %s", TOPIC, KAFKA_BOOTSTRAP)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Lecture depuis Kafka ───────────────────────────────────────────────
    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # ── 2. Parsing JSON ───────────────────────────────────────────────────────
    df_parsed = (
        df_raw
        .select(from_json(col("value").cast("string"), RIDE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("pickup_datetime", col("pickup_datetime").cast(TimestampType()))
        .filter(col("total_amount") > 0)
        .filter(col("trip_distance") > 0)
    )

    # ── 3. Agrégation par fenêtre de 1 minute ────────────────────────────────
    df_aggregated = (
        df_parsed
        .withWatermark("pickup_datetime", "2 minutes")
        .groupBy(window(col("pickup_datetime"), "1 minute"))
        .agg(
            _count("*").alias("trip_count"),
            _round(_sum("total_amount"), 2).alias("total_revenue"),
            _round(avg("total_amount"), 2).alias("avg_revenue"),
            _round(avg("trip_distance"), 2).alias("avg_distance"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("trip_count"),
            col("total_revenue"),
            col("avg_revenue"),
            col("avg_distance"),
        )
    )

    # ── 4. Écriture vers PostgreSQL ───────────────────────────────────────────
    query = (
        df_aggregated
        .writeStream
        .outputMode("append")
        .foreachBatch(write_to_postgres)
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("=== Streaming démarré — en attente de messages Kafka ===")
    log.info("Agrégation toutes les 30 secondes → table streaming_metrics")

    query.awaitTermination()
