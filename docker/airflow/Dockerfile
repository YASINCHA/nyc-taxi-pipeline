# ==============================================================================
# docker/airflow/Dockerfile
#
# Image Airflow custom avec les dépendances Python préinstallées.
# CORRECTION : boto3 n'est plus installé dans le BashOperator à chaque run.
# ==============================================================================

FROM apache/airflow:2.9.1

USER root

# Outils système nécessaires pour interagir avec Docker socket
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Dépendances Python du pipeline
RUN pip install --no-cache-dir \
    boto3==1.34.0 \
    psycopg2-binary==2.9.9 \
    apache-airflow-providers-docker==3.9.0
