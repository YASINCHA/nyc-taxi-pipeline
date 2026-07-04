# ─────────────────────────────────────────────────────────────────────────────
# Backfill — Stratégie de ré-ingestion de l'historique
# ─────────────────────────────────────────────────────────────────────────────
# Pour lancer un backfill sur plusieurs mois :
#
#   docker exec local_orchestrator_airflow airflow dags backfill \
#       nyc_taxi_pipeline_automation \
#       --start-date 2026-01-01 \
#       --end-date 2026-06-01 \
#       --max-active-runs 1
#
# IMPORTANT :
#   - Le pipeline est idempotent (partitionOverwriteMode=dynamic)
#     → relancer sur un mois déjà traité ne crée pas de doublons
#   - max-active-runs=1 évite de saturer le cluster Spark
#   - Chaque mois nécessite un fichier Parquet source dans /opt/apps/
#
# Estimation : ~3 minutes par mois × 12 mois = ~36 minutes pour 1 an complet
# ─────────────────────────────────────────────────────────────────────────────
# ==============================================================================
# dags/nyc_taxi_pipeline.py — DAG Airflow complet
#
# PIPELINE COMPLET :
#   pipeline_start
#       → ingest_raw_data          (Python — upload MinIO)
#       → spark_transform          (Spark cluster — nettoyage + qualité)
#       → spark_transform_and_load (Spark cluster — chargement Silver PostgreSQL)
#       → dbt_run                  (dbt — modélisation Gold)
#       → dbt_test                 (dbt — tests de schéma Gold)
#       → pipeline_end
#
# AMÉLIORATIONS :
#   1. Tâches dbt_run et dbt_test intégrées après le chargement Spark
#   2. on_failure_callback sur toutes les tâches
#   3. Dates dynamiques via Jinja (year/month passés à chaque script)
#   4. Timeout par tâche (évite les jobs bloqués)
#   5. 2 retries avec délai de 10 minutes
# ==============================================================================

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Callback d'alerte sur échec
# ─────────────────────────────────────────────────────────────────────────────
def notify_failure(context):
    """Callback exécuté automatiquement si une tâche échoue."""
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id  = context["run_id"]
    log_url = context["task_instance"].log_url
    log.error(
        "ÉCHEC détecté — DAG: %s | Task: %s | Run: %s | Logs: %s",
        dag_id, task_id, run_id, log_url,
    )
    # Pour activer l'email, configurer un SMTP dans Airflow
    # et décommenter :
    # from airflow.utils.email import send_email
    # send_email(
    #     to="data_engineer@example.com",
    #     subject=f"[AIRFLOW FAILURE] {dag_id}.{task_id}",
    #     html_content=f"Task {task_id} a échoué.<br>Logs: {log_url}"
    # )


# ─────────────────────────────────────────────────────────────────────────────
# Helper : commande Docker exec pour Spark
# ─────────────────────────────────────────────────────────────────────────────
def build_docker_exec_cmd(spark_cmd: str, log_file: str = "/tmp/spark_output.log") -> str:
    """Construit la commande curl pour exécuter un job dans le conteneur Spark."""
    payload_create = json.dumps({
        "AttachStdout": True,
        "AttachStderr": True,
        "User": "0",
        "Cmd": ["sh", "-c", spark_cmd]
    })
    return (
        f'exec_id=$(curl --silent --unix-socket /var/run/docker.sock '
        f'-X POST -H "Content-Type: application/json" '
        f"-d '{payload_create}' "
        f'http://localhost/containers/local_spark_engine/exec '
        f'| grep -o \'"Id":"[^"]*\' | cut -d\'"\' -f4) && '
        f'curl --unix-socket /var/run/docker.sock '
        f'-X POST -H "Content-Type: application/json" '
        f'-d \'{{"Detach":false,"Tty":false}}\' '
        f'http://localhost/exec/$exec_id/start > {log_file} 2>&1 ; '
        f'cat {log_file}'
    )


# ─────────────────────────────────────────────────────────────────────────────
# Helper : commande Docker exec pour dbt
# ─────────────────────────────────────────────────────────────────────────────
def build_dbt_exec_cmd(dbt_cmd: str, log_file: str = "/tmp/dbt_output.log") -> str:
    """Construit la commande curl pour exécuter une commande dbt dans le conteneur dbt."""
    full_cmd = (
        f"cd /dbt/nyc_taxi && "
        f"DBT_PROFILES_DIR=/dbt/nyc_taxi {dbt_cmd}"
    )
    payload_create = json.dumps({
        "AttachStdout": True,
        "AttachStderr": True,
        "User": "0",
        "Cmd": ["sh", "-c", full_cmd]
    })
    return (
        f'exec_id=$(curl --silent --unix-socket /var/run/docker.sock '
        f'-X POST -H "Content-Type: application/json" '
        f"-d '{payload_create}' "
        f'http://localhost/containers/local_dbt/exec '
        f'| grep -o \'"Id":"[^"]*\' | cut -d\'"\' -f4) && '
        f'curl --unix-socket /var/run/docker.sock '
        f'-X POST -H "Content-Type: application/json" '
        f'-d \'{{"Detach":false,"Tty":false}}\' '
        f'http://localhost/exec/$exec_id/start > {log_file} 2>&1 ; '
        f'cat {log_file}'
    )


# ─────────────────────────────────────────────────────────────────────────────
# Configuration par défaut
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email": ["data_engineer@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": notify_failure,
}

# Packages Spark communs
SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.postgresql:postgresql:42.6.0"
)


# ─────────────────────────────────────────────────────────────────────────────
# Définition du DAG
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="nyc_taxi_pipeline_automation",
    default_args=DEFAULT_ARGS,
    description="Pipeline ETL complet : MinIO → Spark → PostgreSQL → dbt Gold",
    schedule_interval="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["nyc_taxi", "etl", "spark", "dbt", "minio", "postgres"],
    doc_md="""
## NYC Taxi ETL Pipeline

Pipeline mensuel complet :
1. **Ingest** — Upload Parquet brut vers MinIO (zone raw/)
2. **Transform** — Spark nettoie les données (6 contrôles qualité) → processed/
3. **Load** — Spark charge Silver dans PostgreSQL (swap atomique)
4. **dbt run** — Modélisation Gold (daily_summary + weekly_performance)
5. **dbt test** — Validation schéma Gold (6 tests unique + not_null)

### Stack
MinIO · Spark 3.5.1 (cluster) · PostgreSQL 15 · dbt 1.8.0 · Airflow 2.9
""",
) as dag:

    # ── Marqueur de début ─────────────────────────────────────────────────────
    start = EmptyOperator(task_id="pipeline_start")

    # ── Tâche 1 : Ingestion MinIO ─────────────────────────────────────────────
    task_ingestion = BashOperator(
        task_id="ingest_raw_data",
        bash_command=(
            "python3 /opt/apps/ingest.py "
            "--year {{ execution_date.year }} "
            "--month {{ execution_date.strftime('%m') }}"
        ),
        doc_md="Upload du fichier Parquet mensuel vers MinIO (zone raw/).",
    )

    # ── Tâche 2 : Transformation Spark (raw → processed) ─────────────────────
    spark_transform_cmd = (
        f"/opt/spark/bin/spark-submit "
        f"--master spark://spark:7077 "
        f"--packages {SPARK_PACKAGES} "
        "/opt/spark/apps/transform.py "
        "--year {{ execution_date.year }} "
        "--month {{ execution_date.strftime('%m') }}"
    )

    task_transform = BashOperator(
        task_id="spark_transform",
        bash_command=build_docker_exec_cmd(spark_transform_cmd, "/tmp/spark_transform.log"),
        output_encoding="latin-1",
        doc_md="Nettoyage Spark (6 contrôles qualité) — raw/ → processed/ (idempotent).",
    )

    # ── Tâche 3 : Chargement Spark Silver (processed → PostgreSQL) ───────────
    spark_load_cmd = (
        f"/opt/spark/bin/spark-submit "
        f"--master spark://spark:7077 "
        f"--packages {SPARK_PACKAGES} "
        "/opt/spark/apps/load.py "
        "--year {{ execution_date.year }} "
        "--month {{ execution_date.strftime('%m') }}"
    )

    task_load = BashOperator(
        task_id="spark_transform_and_load",
        bash_command=build_docker_exec_cmd(spark_load_cmd, "/tmp/spark_load.log"),
        output_encoding="latin-1",
        doc_md="Chargement Silver PostgreSQL via swap atomique (zero-downtime).",
    )

    # ── Tâche 4 : dbt run — Modélisation Gold ────────────────────────────────
    # NOUVEAU : dbt matérialise les modèles Gold après chaque chargement Spark
    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=build_dbt_exec_cmd(
            "dbt run --select staging.stg_silver_taxi gold",
            "/tmp/dbt_run.log"
        ),
        output_encoding="latin-1",
        doc_md="dbt run — Matérialise stg_silver_taxi + daily_taxi_summary + weekly_performance.",
    )

    # ── Tâche 5 : dbt test — Validation schéma Gold ───────────────────────────
    # NOUVEAU : valide que les modèles Gold sont corrects après chaque run
    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=build_dbt_exec_cmd(
            "dbt test --select gold",
            "/tmp/dbt_test.log"
        ),
        output_encoding="latin-1",
        doc_md="dbt test — 6 tests de schéma (unique + not_null) sur les modèles Gold.",
    )

    # ── Marqueur de fin ───────────────────────────────────────────────────────
    end = EmptyOperator(task_id="pipeline_end")

    # ── Ordre d'exécution ─────────────────────────────────────────────────────
    # AVANT : start → ingest → transform → load → end
    # APRÈS  : start → ingest → transform → load → dbt_run → dbt_test → end
    (
        start
        >> task_ingestion
        >> task_transform
        >> task_load
        >> task_dbt_run
        >> task_dbt_test
        >> end
    )