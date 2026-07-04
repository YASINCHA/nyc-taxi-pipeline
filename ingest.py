# ==============================================================================
# ingest.py — Ingestion Parquet vers MinIO (Data Lake)
#
# CORRECTIONS APPLIQUÉES :
#   1. Credentials lus depuis os.environ (plus de hardcode)
#   2. Paramètres year/month via argparse (dates dynamiques depuis le DAG)
#   3. Vérification que le fichier local existe avant l'upload
#   4. Vérification/création du bucket si absent (évite un crash au 1er run)
#   5. Logging structuré avec le module logging (remplace les print())
#   6. sys.exit(1) sur erreur critique (Airflow détecte le code de retour)
# ==============================================================================

import argparse
import logging
import os
import sys

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

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
# Configuration MinIO — lue depuis les variables d'environnement
# CORRECTION : plus de credentials codés en dur dans le code source
# ─────────────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
BUCKET_NAME = os.environ.get("MINIO_BUCKET", "nytaxi")

# Répertoire où se trouvent les fichiers Parquet bruts
DATA_DIR = os.environ.get("DATA_DIR", "/opt/apps")


def get_s3_client():
    """Initialise et retourne le client boto3 configuré pour MinIO."""
    if not ACCESS_KEY or not SECRET_KEY:
        log.error("Variables d'environnement MINIO_ACCESS_KEY / MINIO_SECRET_KEY manquantes !")
        sys.exit(1)

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket_exists(s3_client, bucket: str) -> None:
    """Crée le bucket s'il n'existe pas encore."""
    try:
        s3_client.head_bucket(Bucket=bucket)
        log.info("Bucket '%s' déjà existant.", bucket)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("404", "NoSuchBucket"):
            log.info("Bucket '%s' absent, création en cours...", bucket)
            s3_client.create_bucket(Bucket=bucket)
            log.info("Bucket '%s' créé avec succès.", bucket)
        else:
            log.error("Erreur inattendue lors de la vérification du bucket : %s", e)
            sys.exit(1)


def upload_file_to_lake(s3_client, local_path: str, target_lake_path: str) -> None:
    """Upload un fichier local vers MinIO et lève une exception en cas d'échec."""
    # CORRECTION : vérification que le fichier source existe
    if not os.path.isfile(local_path):
        log.error("Fichier source introuvable : '%s'", local_path)
        log.error("Vérifiez que le fichier Parquet est bien présent dans %s", DATA_DIR)
        sys.exit(1)

    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    log.info("Démarrage de l'upload : %s (%.2f Mo)...", local_path, file_size_mb)

    try:
        s3_client.upload_file(local_path, BUCKET_NAME, target_lake_path)
        log.info(
            "Upload réussi → s3://%s/%s (%.2f Mo transférés)",
            BUCKET_NAME,
            target_lake_path,
            file_size_mb,
        )
    except ClientError as e:
        log.error("Échec de l'upload vers MinIO : %s", e)
        sys.exit(1)


def parse_args() -> argparse.Namespace:
    """Parse les arguments CLI fournis par le DAG Airflow."""
    parser = argparse.ArgumentParser(
        description="Ingestion mensuelle des données NYC Taxi vers MinIO."
    )
    parser.add_argument(
        "--year",
        required=True,
        type=int,
        help="Année de la partition (ex: 2026)",
    )
    parser.add_argument(
        "--month",
        required=True,
        type=str,
        help="Mois de la partition, format 2 chiffres (ex: 01, 12)",
    )
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    args = parse_args()
    year = args.year
    month = args.month.zfill(2)  # Garantit le format "01", "02", ... "12"

    log.info("=== Démarrage de l'ingestion : year=%d, month=%s ===", year, month)

    # Construction des chemins dynamiques
    filename = f"yellow_tripdata_{year}-{month}.parquet"
    local_file = os.path.join(DATA_DIR, filename)
    lake_path = f"raw/yellow_taxi/year={year}/month={month}/{filename}"

    # Exécution
    s3 = get_s3_client()
    ensure_bucket_exists(s3, BUCKET_NAME)
    upload_file_to_lake(s3, local_file, lake_path)

    log.info("=== Ingestion terminée avec succès ===")
