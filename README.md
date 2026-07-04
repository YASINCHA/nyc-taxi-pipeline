# 🚕 NYC Taxi ETL Pipeline — Big Data Stack Engineering Grade

[![CI Tests](https://github.com/YASINCHA/nyc-taxi-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/YASINCHA/nyc-taxi-pipeline/actions)
[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange)](https://spark.apache.org)
[![Airflow](https://img.shields.io/badge/Airflow-2.9.1-green)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.8.0-red)](https://www.getdbt.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://docker.com)

---

## 📋 Description

Pipeline de données **Big Data complet et production-ready** qui ingère, transforme, valide, modélise et visualise les données officielles de courses de taxi new-yorkais (NYC TLC) sur une infrastructure entièrement conteneurisée sous Docker.

Ce projet démontre les compétences d'un **Data Engineer Senior** : architecture distribuée scalable, modélisation analytique versionnée avec dbt, idempotence garantie, data quality à deux niveaux, monitoring temps réel, et tests automatisés.

---

## 🏗️ Architecture

```
Parquet TLC (3.7M lignes brutes)
         ↓
    Apache Airflow 2.9
    (orchestration mensuelle + alerting on_failure_callback)
         ↓
    MinIO (Data Lake S3)
    (raw/ et processed/ partitionnés year=/month=/)
         ↓
    Spark Master + 2 Workers
    (cluster distribué, écriture idempotente)
         ↓
    PostgreSQL 15 (Data Warehouse)
    (Silver + pipeline_metrics)
         ↓
    dbt 1.8 (modélisation Gold)
    (5 modèles, 1 macro, 18 tests)
         ↓
    Metabase (Dashboard analytique)
    (5 graphiques + monitoring)
```

---

## 🛠️ Stack technique

| Composant | Technologie | Rôle |
|---|---|---|
| Orchestration | Apache Airflow 2.9.1 | Pipeline scheduling + alerting |
| Traitement | Apache Spark 3.5.1 (cluster) | ETL distribué |
| Data Lake | MinIO (S3-compatible) | Stockage Parquet partitionné |
| Data Warehouse | PostgreSQL 15 | Silver + Gold + Metrics |
| Modélisation | dbt-postgres 1.8.0 | SQL versionné + tests |
| Visualisation | Metabase | Dashboards analytiques |
| Infrastructure | Docker Compose | 10 services conteneurisés |
| Tests | pytest + dbt tests | 17 + 18 = 35 tests automatisés |

---

## 📊 Pipeline de données — 7 tâches Airflow

```
pipeline_start
    → ingest_raw_data          (Python/boto3 — upload MinIO)
    → spark_transform          (Spark — nettoyage + 6 contrôles qualité)
    → spark_transform_and_load (Spark — chargement Silver PostgreSQL)
    → dbt_run                  (dbt — modélisation Gold)
    → dbt_test                 (dbt — 18 tests de schéma)
    → pipeline_end
```

---

## ✅ Garanties d'ingénierie

| Garantie | Implémentation |
|---|---|
| **Idempotence** | `partitionOverwriteMode=dynamic` — réexécution sans duplication |
| **Zero-downtime** | Swap atomique PostgreSQL (`ALTER TABLE RENAME`) |
| **Layered Data Quality** | Spark (6 assertions) + dbt Source (5 tests) + dbt Gold (10 tests) |
| **Observabilité** | `pipeline_metrics` + `on_failure_callback` Airflow |
| **Lignage des données** | Graphe dbt automatique |
| **Scalabilité** | Cluster Spark 2 Workers (4 cores, 4 GiB RAM) |
| **Backfill** | `airflow dags backfill` — historique sans duplication |
| **CI/CD** | GitHub Actions — pytest automatique à chaque push |

---

## 📁 Structure du projet

```
nyc_taxi_pipeline/
├── .github/
│   └── workflows/
│       └── tests.yml           ← CI/CD GitHub Actions
├── dags/
│   └── nyc_taxi_pipeline.py    ← DAG Airflow (7 tâches)
├── dbt/
│   └── nyc_taxi/
│       ├── macros/
│       │   └── revenue_per_mile.sql   ← Macro dbt réutilisable
│       └── models/
│           ├── staging/
│           │   ├── sources.yml
│           │   └── stg_silver_taxi.sql
│           └── gold/
│               ├── schema.yml
│               ├── daily_taxi_summary.sql
│               ├── weekly_performance.sql
│               ├── top_pickup_zones.sql
│               └── trip_efficiency.sql
├── docker/
│   └── airflow/
│       └── Dockerfile          ← Image Airflow custom
├── sql/
│   └── init_schema.sql         ← Schéma PostgreSQL
├── tests/
│   └── test_transform.py       ← 17 tests unitaires pytest
├── docker-compose.yaml         ← Stack complète (10 services)
├── ingest.py                   ← Ingestion MinIO
├── transform.py                ← Transformation Spark + qualité
├── load.py                     ← Chargement PostgreSQL
├── .env.example                ← Template secrets
└── .gitignore
```

---

## 🚀 Démarrage rapide

### Prérequis
- Docker Desktop 4.x+
- 8 Go de RAM minimum alloués à Docker
- Git

### Installation

```bash
# 1. Cloner le repo
git clone https://github.com/YASINCHA/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline

# 2. Configurer les secrets
cp .env.example .env
# Éditer .env avec vos valeurs

# 3. Générer la clé Fernet Airflow
docker run --rm apache/airflow:2.9.1 python -c \
  "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Coller la valeur dans .env → AIRFLOW_FERNET_KEY=...

# 4. Construire et démarrer la stack
docker compose build airflow
docker compose up -d

# 5. Créer l'utilisateur Airflow
docker exec local_orchestrator_airflow airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname Admin \
  --role Admin --email admin@example.com
```

### Accès aux interfaces

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8085 | admin / admin |
| Spark Master UI | http://localhost:8080 | — |
| Spark Worker 1 | http://localhost:8081 | — |
| Spark Worker 2 | http://localhost:8082 | — |
| MinIO Console | http://localhost:9001 | admin / supersecretpassword123 |
| Metabase | http://localhost:3000 | Compte créé au setup |
| PostgreSQL | localhost:5432 | data_engineer / password123 |

### Télécharger les données

```bash
# Source officielle NYC TLC
# https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# Télécharger : yellow_tripdata_YYYY-MM.parquet
# Placer dans le répertoire racine du projet
```

### Lancer le pipeline

```bash
# Via Airflow UI : http://localhost:8085
# Ou via CLI :
docker exec local_orchestrator_airflow airflow dags trigger \
  nyc_taxi_pipeline_automation \
  --exec-date 2026-01-01T00:00:00+00:00
```

### Backfill historique

```bash
docker exec local_orchestrator_airflow airflow dags backfill \
  nyc_taxi_pipeline_automation \
  --start-date 2026-01-01 \
  --end-date 2026-06-01
```

---

## 🧪 Tests

### Tests unitaires (pytest)

```bash
# Lancer les 17 tests unitaires dans le conteneur Spark
docker exec \
  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  local_spark_engine \
  python3 -m pytest /opt/spark/apps/tests/test_transform.py -v
```

### Tests dbt (18 tests de schéma)

```bash
# Tests source Silver (5 tests)
docker exec -e DBT_PROFILES_DIR=/dbt/nyc_taxi -w /dbt/nyc_taxi \
  local_dbt dbt test --select source:public.final_taxi_data

# Tests modèles Gold (13 tests)
docker exec -e DBT_PROFILES_DIR=/dbt/nyc_taxi -w /dbt/nyc_taxi \
  local_dbt dbt test --select gold
```

---

## 📈 Résultats mesurés

| Métrique | Valeur |
|---|---|
| Lignes brutes traitées | 3 724 889 |
| Lignes Silver chargées | 2 552 307 |
| Taux de rejet | 31.48% |
| Durée pipeline complet | ~3 minutes |
| Modèles dbt | 5 (staging + 4 Gold) |
| Macro dbt | 1 (`revenue_per_mile`) |
| Tests automatisés | 35 (17 pytest + 18 dbt) |
| Services Docker | 10 |

---

## 🗄️ Modèles dbt — Lignage des données

```
final_taxi_data (Silver — chargée par Spark)
        ↓
stg_silver_taxi          (staging — vue de lecture)
        ↓
daily_taxi_summary        (Gold — résumé quotidien)
        ↓
weekly_performance        (Gold — performance hebdomadaire)
top_pickup_zones          (Gold — Top 20 zones rentables)
trip_efficiency           (Gold — revenu/mile via macro)
```

---

## 📊 Data Dictionary

### `final_taxi_data` (Silver)
| Colonne | Type | Description |
|---|---|---|
| `tpep_pickup_datetime` | TIMESTAMP | Date/heure de prise en charge |
| `tpep_dropoff_datetime` | TIMESTAMP | Date/heure de dépose |
| `passenger_count` | BIGINT | Nombre de passagers |
| `trip_distance` | DOUBLE | Distance en miles |
| `total_amount` | DOUBLE | Montant total en dollars |
| `tip_amount` | DOUBLE | Pourboire en dollars |

### `daily_taxi_summary` (Gold)
| Colonne | Type | Description |
|---|---|---|
| `trip_date` | DATE | Date de la journée |
| `trip_count` | BIGINT | Nombre de courses |
| `revenue` | NUMERIC | Revenu total |
| `total_distance` | NUMERIC | Distance totale |
| `avg_revenue_per_trip` | NUMERIC | Revenu moyen par course |

### `pipeline_metrics` (Monitoring)
| Colonne | Type | Description |
|---|---|---|
| `run_date` | TIMESTAMP | Date d'exécution |
| `total_rows` | BIGINT | Lignes brutes traitées |
| `valid_rows` | BIGINT | Lignes valides chargées |
| `rejection_rate_pct` | NUMERIC | Taux de rejet en % |
| `duration_seconds` | NUMERIC | Durée d'exécution |
| `status` | VARCHAR | SUCCESS / FAILED |

---

## 🔮 Évolutions prévues

- **Apache Kafka** — streaming temps réel des courses
- **Great Expectations** — data contracts formels
- **RAG / LangChain** — chatbot Q&A sur les données NYC Taxi
- **MLflow** — prédiction de revenus (régression)
- **Terraform** — Infrastructure as Code pour déploiement cloud

---

## 👤 Auteur

**Yassin Lajmi** — Data Engineer
- GitHub : [@YASINCHA](https://github.com/YASINCHA)
- Email : lajmiyassin280@gmail.com

---

*Stack locale équivalente à : AWS S3 + EMR + MWAA + RDS + dbt Cloud + Tableau*
