# 🚕 NYC Taxi Data Platform — Big Data & AI Engineering Grade

[![CI Tests](https://github.com/YASINCHA/nyc-taxi-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/YASINCHA/nyc-taxi-pipeline/actions)
[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange)](https://spark.apache.org)
[![Airflow](https://img.shields.io/badge/Airflow-2.9.1-green)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.8.0-red)](https://www.getdbt.com)
[![Kafka](https://img.shields.io/badge/Kafka-7.5.0-black)](https://kafka.apache.org)
[![MLflow](https://img.shields.io/badge/MLflow-2.x-blueviolet)](https://mlflow.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109-teal)](https://fastapi.tiangolo.com)
[![LangChain](https://img.shields.io/badge/LangChain-0.3-green)](https://langchain.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://docker.com)

---

## 📋 Description

Plateforme de données **Big Data complète, production-ready et Engineering Grade** qui ingère, transforme, valide, modélise, visualise et analyse en langage naturel les données officielles de courses de taxi new-yorkais (NYC TLC) sur une infrastructure entièrement conteneurisée sous **14 services Docker**.

Ce projet démontre les compétences d'un **Senior Data & AI Engineer** :

| Compétence | Implémentation |
|---|---|
| 🏗️ **Architecture distribuée scalable** | Cluster Spark 2 Workers (4 cores, 4 GiB RAM) |
| 🔄 **Streaming temps réel** | Apache Kafka + Spark Structured Streaming |
| 🤖 **IA générative / RAG** | LangChain + Ollama (llama3.2) + Streamlit |
| 📈 **MLOps** | MLflow tracking + 3 modèles scikit-learn (R²=0.9791) |
| 🧪 **Data Quality à 2 niveaux** | Spark (6 assertions) + dbt (18 tests de schéma) |
| 🔁 **Idempotence garantie** | `partitionOverwriteMode=dynamic` + swap atomique PostgreSQL |
| 📡 **API REST production** | FastAPI avec 8 endpoints |
| 📊 **Visualisation** | Metabase — 6 dashboards |
| 📦 **Infrastructure as Code** | Docker Compose — 14 services |

---

## 🏗️ Architecture — 14 Services Docker

```
                    ┌──────────────────────────────────────────────┐
                    │             Apache Airflow 2.9.1              │
                    │         (Orchestrateur — 8 tâches)             │
                    │    pipeline_start → ingest → transform →      │
                    │    load → dbt_run → dbt_test → model_train →  │
                    │    pipeline_end                                │
                    └──────────────┬───────────────────────────────┘
                                   │
                    ┌──────────────▼───────────────────────────────┐
                    │              MinIO (Data Lake S3)             │
                    │   raw/ (Parquet brut)  ───→  processed/       │
                    │         year=/month=/           year=/month=/  │
                    └──────────────┬───────────────────────────────┘
                                   │
                    ┌──────────────▼───────────────────────────────┐
                    │       Apache Spark 3.5.1 (Cluster)            │
                    │   Master :8080  │  Worker 1 :8081             │
                    │                 │  Worker 2 :8082             │
                    ├──────────────────────────────────────────────┤
                    │  Transform — 6 contrôles qualité + monitoring │
                    │  Streaming — Fenêtres 1 min → streaming_metrics│
                    └──────────────┬───────────────────────────────┘
                                   │
                    ┌──────────────▼───────────────────────────────┐
                    │          PostgreSQL 15 (Data Warehouse)        │
                    │  Silver  │  Gold (dbt)  │  Metrics  │  ML      │
                    │  final_taxi_data                              │
                    │  daily_taxi_summary, weekly_performance,       │
                    │  top_pickup_zones, trip_efficiency,            │
                    │  pipeline_metrics, streaming_metrics,          │
                    │  revenue_predictions                           │
                    └──────┬───────────┬──────────┬────────────────┘
                           │           │          │
              ┌────────────┴────┐ ┌────┴─────┐ ┌─┴─────────────┐
              │   dbt 1.8.0    │ │ Metabase │ │    FastAPI     │
              │  5 modèles     │ │ :3000    │ │ 8 endpoints    │
              │  1 macro       │ │Dashboard │ │ :8000/docs     │
              │  18 tests      │ │          │ │                │
              └────────────────┘ └──────────┘ └────────────────┘
                           │
              ┌────────────┴─────────────────────────────┐
              │        Kafka Streaming Stack              │
              │  Zookeeper :2181                          │
              │    ↓                                      │
              │  Kafka :9092                              │
              │    ↓ (topic: nyc_taxi_rides)              │
              │  Spark Structured Streaming               │
              │  → streaming_metrics (PostgreSQL)          │
              └───────────────────────────────────────────┘
                           │
              ┌────────────┴──────────────┐
              │     MLOps Stack           │
              │  train_model.py            │
              │    ↓                       │
              │  MLflow :5000              │
              │  → 3 modèles trackés       │
              │  → revenue_predictions     │
              └───────────────────────────┘
                           │
              ┌────────────┴──────────────┐
              │     RAG Chatbot Stack     │
              │  LangChain 0.3            │
              │  Ollama (llama3.2)        │
              │  Streamlit :8501          │
              │  → Questions en NL        │
              └───────────────────────────┘
```

---

## 🛠️ Stack technique complète

| Composant | Technologie | Version | Rôle |
|---|---|---|---|
| **Orchestration** | Apache Airflow | 2.9.1 | Pipeline scheduling + alerting + backfill |
| **Traitement batch** | Apache Spark (cluster) | 3.5.1 | ETL distribué — 3 nœuds (1 master + 2 workers) |
| **Streaming** | Apache Kafka | 7.5.0 | Message broker temps réel |
| **Streaming** | Spark Structured Streaming | 3.5.1 | Fenêtres 1 minute → PostgreSQL |
| **Data Lake** | MinIO (S3-compatible) | latest | Stockage Parquet partitionné (raw/ + processed/) |
| **Data Warehouse** | PostgreSQL | 15 | Silver + Gold + Metrics + ML |
| **Modélisation** | dbt-postgres | 1.8.0 | SQL versionné + 18 tests + 1 macro |
| **MLOps** | MLflow | 2.x | Tracking expériences + model registry |
| **Machine Learning** | scikit-learn | 1.4+ | LinearRegression, RandomForest, GradientBoosting |
| **RAG / Chatbot** | LangChain + Ollama | 0.3 / llama3.2 | Q&A en langage naturel sur les données |
| **RAG UI** | Streamlit | 1.28+ | Interface web du chatbot |
| **API REST** | FastAPI | 0.109+ | 8 endpoints — documentation Swagger |
| **Visualisation** | Metabase | latest | 6 dashboards connectés |
| **CI/CD** | GitHub Actions | — | Tests pytest automatiques à chaque push |
| **Infrastructure** | Docker Compose | — | 14 services conteneurisés |

---

## 📊 Pipeline de données — 8 tâches Airflow

```
pipeline_start
    → ingest_raw_data             (Python/boto3 — upload Parquet vers MinIO raw/)
    → spark_transform             (Spark cluster — 6 contrôles qualité → processed/)
    → spark_transform_and_load    (Spark cluster — chargement Silver PostgreSQL + swap atomique)
    → dbt_run                     (dbt — 5 modèles Gold : staging + summary + weekly + zones + efficiency)
    → dbt_test                    (dbt — 18 tests de schéma unique/not_null)
    → model_training              (MLflow — entraînement mensuel, 3 modèles, meilleur R² tracké)
    → pipeline_end
```

> **Nouveau** : La tâche `model_training` réentraîne automatiquement le modèle de prédiction de revenus chaque mois après le succès des tests dbt.

---

## ✅ Garanties d'ingénierie

| Garantie | Implémentation |
|---|---|
| **🔁 Idempotence** | `partitionOverwriteMode=dynamic` — réexécution sans duplication |
| **🔄 Zero-downtime** | Swap atomique PostgreSQL (`ALTER TABLE RENAME CASCADE`) |
| **🧪 Data Quality (2 niveaux)** | Spark (6 assertions) + dbt Source (5 tests) + dbt Gold (13 tests) |
| **📊 Observabilité** | `pipeline_metrics` + `streaming_metrics` + `on_failure_callback` Airflow |
| **📈 Lignage des données** | Graphe dbt automatique — traçabilité complète |
| **⚡ Scalabilité** | Cluster Spark 2 Workers (4 cores, 4 GiB RAM) |
| **📅 Backfill** | `airflow dags backfill` — historique sans duplication |
| **🔔 Alerting** | `on_failure_callback` + email configurable |
| **🤖 IA temps réel** | RAG Chatbot + Streaming Kafka + MLflow predictions |
| **✅ CI/CD** | GitHub Actions — pytest automatique à chaque push |

---

## 📁 Structure du projet

```
nyc_taxi_pipeline/
├── .github/
│   └── workflows/
│       └── tests.yml                      ← CI/CD GitHub Actions
├── dags/
│   └── nyc_taxi_pipeline.py               ← DAG Airflow (8 tâches)
├── dbt/
│   ├── chatbot_nyc_taxi.py                ← RAG Chatbot LangChain + Streamlit
│   ├── profiles.yml
│   └── nyc_taxi/
│       ├── dbt_project.yml
│       ├── macros/
│       │   └── revenue_per_mile.sql        ← Macro dbt réutilisable
│       └── models/
│           ├── staging/
│           │   ├── sources.yml             ← Source + 5 tests
│           │   └── stg_silver_taxi.sql
│           └── gold/
│               ├── schema.yml              ← 13 tests Gold
│               ├── daily_taxi_summary.sql
│               ├── weekly_performance.sql
│               ├── top_pickup_zones.sql
│               └── trip_efficiency.sql
├── docker/
│   └── airflow/
│       └── Dockerfile                      ← Image Airflow custom
├── sql/
│   └── init_schema.sql                     ← Schéma PostgreSQL complet
├── tests/
│   ├── __init__.py
│   └── test_transform.py                   ← 17 tests unitaires pytest
├── docker-compose.yaml                     ← Stack complète (14 services)
├── ingest.py                               ← Ingestion MinIO (boto3)
├── transform.py                            ← Transformation Spark + 6 contrôles qualité
├── load.py                                 ← Chargement PostgreSQL (swap atomique)
├── api.py                                  ← FastAPI REST (8 endpoints)
├── train_model.py                          ← MLOps — entraînement MLflow
├── kafka_producer.py                       ← Producteur Kafka (2 courses/sec)
├── spark_streaming_consumer.py             ← Spark Structured Streaming
├── .env.example                            ← Template secrets
└── .gitignore
```

---

## 🚀 Démarrage rapide

### Prérequis

- Docker Desktop 4.x+ (Windows, macOS, Linux)
- 12 Go de RAM minimum alloués à Docker
- Git
- **Optionnel** : Ollama (pour le chatbot RAG)

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

# 4. Télécharger les données NYC TLC
# Source : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# Télécharger : yellow_tripdata_YYYY-MM.parquet
# Placer dans : D:\nyc_taxi_pipeline1\ (monté dans /opt/apps/)

# 5. Construire et démarrer la stack complète
docker compose build airflow
docker compose up -d

# 6. Créer l'utilisateur Airflow
docker exec local_orchestrator_airflow airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname Admin \
  --role Admin --email admin@example.com

# 7. (Optionnel) Installer Ollama pour le chatbot
# https://ollama.com/download
ollama pull llama3.2
```

---

## 📡 Accès aux interfaces

| Service | URL | Port | Auth |
|---|---|---|---|
| **Airflow UI** | http://localhost:8085 | 8085 | admin / admin |
| **Spark Master** | http://localhost:8080 | 8080 | — |
| **Spark Worker 1** | http://localhost:8081 | 8081 | — |
| **Spark Worker 2** | http://localhost:8082 | 8082 | — |
| **MinIO Console** | http://localhost:9001 | 9001 | admin / supersecretpassword123 |
| **Metabase** | http://localhost:3000 | 3000 | Compte créé au setup |
| **MLflow UI** | http://localhost:5000 | 5000 | — |
| **FastAPI Docs** | http://localhost:8000/docs | 8000 | — |
| **FastAPI Redoc** | http://localhost:8000/redoc | 8000 | — |
| **Streamlit RAG** | http://localhost:8501 | 8501 | — |
| **PostgreSQL** | localhost:5432 | 5432 | data_engineer / password123 |

---

## 🔄 Streaming temps réel (Kafka + Spark)

### Architecture streaming

```
kafka_producer.py                 spark_streaming_consumer.py
      │                                    │
      │  2 courses/sec                     │  Spark Structured Streaming
      │  topic: nyc_taxi_rides             │  Fenêtres de 1 minute
      ▼                                    ▼
┌──────────┐    ┌──────────────────┐    ┌────────────────────┐
│ Zookeeper│───▶│ Kafka :9092      │───▶│ Spark Structured   │
│ :2181    │    │ 3 partitions     │    │ Streaming (cluster)│
└──────────┘    └──────────────────┘    └─────────┬──────────┘
                                                   │
                                                   ▼
                                        ┌────────────────────┐
                                        │ PostgreSQL          │
                                        │ streaming_metrics   │
                                        └────────────────────┘
```

### Lancer le streaming

```bash
# Terminal 1 : Démarrer le producteur Kafka (2 courses/seconde)
docker exec -u 0 local_orchestrator_airflow \
  python3 -u /opt/apps/kafka_producer.py

# Terminal 2 : Démarrer le consommateur Spark Streaming
docker exec local_spark_engine /opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262,\
org.postgresql:postgresql:42.6.0 \
  /opt/spark/apps/spark_streaming_consumer.py
```

### Voir les données streaming via l'API

```bash
curl http://localhost:8000/streaming/latest?limit=5
```

---

## 🤖 Chatbot RAG (LangChain + Ollama + Streamlit)

Le chatbot permet de poser des questions en **langage naturel** sur les données NYC Taxi.

### Stack du chatbot

- **LangChain 0.3** — orchestration LLM + prompt templating
- **Ollama** — serveur LLM local avec `llama3.2` (3B paramètres)
- **PostgreSQL** — contexte de données enrichi (métriques en temps réel)
- **Streamlit** — interface web réactive

### Questions suggérées

- _"Quel jour de la semaine génère le plus de revenus ?"_
- _"Combien de courses ont été effectuées au total ?"_
- _"Quelle est la zone de pickup la plus rentable ?"_
- _"Quel est le revenu moyen par course ?"_
- _"Quelles sont les données de streaming temps réel ?"_
- _"Compare les revenus du lundi vs vendredi"_

### Lancer le chatbot

```bash
# 1. Assurez-vous qu'Ollama tourne sur votre machine hôte
ollama serve

# 2. Tirez le modèle
ollama pull llama3.2

# 3. Lancez Streamlit dans le conteneur dbt
docker exec local_dbt python -m streamlit run \
  /dbt/chatbot_nyc_taxi.py \
  --server.address 0.0.0.0 --server.port 8501

# 4. Ouvrez http://localhost:8501
```

> **Note** : Le chatbot accède à Ollama via `host.docker.internal:11434` — assurez-vous qu'Ollama écoute sur `0.0.0.0` (pas seulement localhost).

---

## 📈 MLOps — MLflow + Prédiction de revenus

### Modèles entraînés

| Modèle | R² | MAE |
|---|---|---|
| **LinearRegression** 🏆 | **0.9791** | Meilleur |
| GradientBoosting | 0.9512 | Intermédiaire |
| RandomForest | 0.9228 | Standard |

### Features utilisées

- `trip_count` — nombre de courses quotidien
- `total_distance` — distance totale parcourue
- `avg_distance_per_trip` — distance moyenne par course
- `day_of_week` — jour de la semaine (0-6)
- `month` — mois (1-12)
- `day_of_month` — jour du mois
- `is_weekend` — indicateur week-end

### Entraînement manuel

```bash
docker exec -e MLFLOW_TRACKING_URI=http://localhost:5000 \
  local_mlflow python3 /opt/apps/train_model.py
```

### Entraînement automatique (Airflow)

La tâche `model_training` s'exécute automatiquement chaque mois après le succès du pipeline ETL.

### Voir les résultats

- **MLflow UI** : http://localhost:5000
- **API Prédiction** : `http://localhost:8000/predict/revenue?trip_count=500&total_distance=1200&day_of_week=4`
- **API Docs** : http://localhost:8000/docs

---

## 🌐 FastAPI — 8 Endpoints REST

L'API REST expose l'ensemble des données et prédictions de la plateforme.

| Méthode | Endpoint | Description | Source |
|---|---|---|---|
| `GET` | `/` | Health check + statut des services connectés | — |
| `GET` | `/metrics/daily` | Résumé quotidien (daily_taxi_summary) | dbt Gold |
| `GET` | `/metrics/weekly` | Performance par jour de la semaine | dbt Gold |
| `GET` | `/metrics/top-zones` | Top 20 zones de pickup les plus rentables | dbt Gold |
| `GET` | `/predict/revenue` | Prédiction de revenu (MLflow LinearRegression) | ML + historique |
| `GET` | `/pipeline/status` | Statut des derniers runs (pipeline_metrics) | Monitoring |
| `GET` | `/streaming/latest` | Dernières fenêtres de streaming Kafka | Streaming |
| `GET` | `/silver/stats` | Statistiques globales Silver | PostgreSQL |

```bash
# Exemples de requêtes
curl http://localhost:8000/
curl http://localhost:8000/metrics/daily?limit=5
curl "http://localhost:8000/predict/revenue?trip_count=500&total_distance=1200&day_of_week=4"
curl http://localhost:8000/pipeline/status
curl http://localhost:8000/streaming/latest
curl http://localhost:8000/silver/stats
```

Documentation interactive : [http://localhost:8000/docs](http://localhost:8000/docs)

---

## 🧪 Tests

### Tests unitaires (pytest) — 17 tests

```bash
docker exec \
  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  local_spark_engine \
  python3 -m pytest /opt/spark/apps/tests/test_transform.py -v
```

### Tests dbt — 18 tests de schéma

```bash
# Tests source Silver (5 tests)
docker exec -e DBT_PROFILES_DIR=/dbt/nyc_taxi -w /dbt/nyc_taxi \
  local_dbt dbt test --select source:public.final_taxi_data

# Tests modèles Gold (13 tests)
docker exec -e DBT_PROFILES_DIR=/dbt/nyc_taxi -w /dbt/nyc_taxi \
  local_dbt dbt test --select gold

# Tous les tests
docker exec -e DBT_PROFILES_DIR=/dbt/nyc_taxi -w /dbt/nyc_taxi \
  local_dbt dbt test
```

### Couverture totale

| Type | Nombre | Outil |
|---|---|---|
| Tests unitaires | 17 | pytest |
| Tests dbt source | 5 | dbt test |
| Tests dbt Gold | 13 | dbt test |
| **Total** | **35** | Automatisé |

---

## 📈 Résultats mesurés

| Métrique | Valeur |
|---|---|
| Lignes brutes traitées | **3 724 889** |
| Lignes Silver chargées | **2 552 307** |
| Taux de rejet | **31.48%** |
| Durée pipeline complet | **~3 minutes** |
| Modèles dbt | **5** (staging + 4 Gold) |
| Macro dbt | **1** (`revenue_per_mile`) |
| Tests automatisés | **35** (17 pytest + 18 dbt) |
| Modèles ML trackés | **3** (Meilleur R²=0.9791) |
| Services Docker | **14** |
| Endpoints API | **8** |
| Streaming temps réel | **2 courses Kafka/seconde** |
| Fenêtres streaming | **1 minute** |

---

## 🗄️ Data Dictionary

### `final_taxi_data` (Silver — données individuelles)

| Colonne | Type | Description |
|---|---|---|
| `vendor_id` | INTEGER | Identifiant du fournisseur (1=Creative Mobile, 2=VeriFone) |
| `tpep_pickup_datetime` | TIMESTAMP | Date/heure de prise en charge |
| `tpep_dropoff_datetime` | TIMESTAMP | Date/heure de dépose |
| `passenger_count` | INTEGER | Nombre de passagers |
| `trip_distance` | DOUBLE PRECISION | Distance en miles |
| `ratecode_id` | INTEGER | Code tarifaire (1=Standard, 2=JFK, 3=Newark, etc.) |
| `store_and_fwd_flag` | VARCHAR(1) | Stocké puis transmis (Y/N) |
| `pu_location_id` | INTEGER | Zone de prise en charge (TLC Location ID) |
| `do_location_id` | INTEGER | Zone de dépose (TLC Location ID) |
| `payment_type` | INTEGER | Type de paiement (1=Carte, 2=Espèces, 3=Gratuit, 4=Contesté) |
| `fare_amount` | DOUBLE PRECISION | Tarif de base (hors suppléments) |
| `extra` | DOUBLE PRECISION | Suppléments divers |
| `mta_tax` | DOUBLE PRECISION | Taxe MTA ($0.50) |
| `tip_amount` | DOUBLE PRECISION | Pourboire |
| `tolls_amount` | DOUBLE PRECISION | Péages |
| `improvement_surcharge` | DOUBLE PRECISION | Supplément d'amélioration ($0.30) |
| `total_amount` | DOUBLE PRECISION | Montant total (incluant tous les suppléments) |
| `congestion_surcharge` | DOUBLE PRECISION | Supplément congestion ($2.50) |

### `daily_taxi_summary` (Gold — résumé quotidien)

| Colonne | Type | Description |
|---|---|---|
| `trip_date` | DATE | Date de la journée |
| `trip_count` | BIGINT | Nombre de courses |
| `revenue` | NUMERIC | Revenu total ($) |
| `total_distance` | NUMERIC | Distance totale (miles) |
| `avg_revenue_per_trip` | NUMERIC | Revenu moyen par course ($) |
| `avg_distance_per_trip` | NUMERIC | Distance moyenne par course (miles) |
| `avg_passengers` | NUMERIC | Nombre moyen de passagers |

### `weekly_performance` (Gold — performance hebdomadaire)

| Colonne | Type | Description |
|---|---|---|
| `day_of_week` | VARCHAR | Nom du jour (Lundi, Mardi, ...) |
| `dow_number` | INTEGER | Numéro du jour (0=Dimanche, 6=Samedi) |
| `avg_daily_revenue` | NUMERIC | Revenu moyen pour ce jour ($) |
| `avg_daily_trips` | NUMERIC | Nombre moyen de courses pour ce jour |
| `avg_daily_distance` | NUMERIC | Distance moyenne parcourue ce jour (miles) |
| `data_points` | INTEGER | Nombre de semaines de données |

### `top_pickup_zones` (Gold — zones rentables)

| Colonne | Type | Description |
|---|---|---|
| `pickup_location_id` | INTEGER | Identifiant TLC de la zone de pickup |
| `trip_count` | BIGINT | Nombre total de courses depuis cette zone |
| `total_revenue` | NUMERIC | Revenu total généré ($) |
| `avg_revenue_per_trip` | NUMERIC | Revenu moyen par course ($) |
| `avg_distance_miles` | NUMERIC | Distance moyenne par course (miles) |
| `avg_passengers` | NUMERIC | Nombre moyen de passagers |
| `total_tips` | NUMERIC | Total des pourboires ($) |
| `avg_tip_per_trip` | NUMERIC | Pourboire moyen par course ($) |

### `trip_efficiency` (Gold — efficacité)

| Colonne | Type | Description |
|---|---|---|
| `trip_date` | DATE | Date |
| `trip_count` | BIGINT | Nombre de courses |
| `revenue` | NUMERIC | Revenu total ($) |
| `total_distance` | NUMERIC | Distance totale (miles) |
| `avg_revenue_per_mile` | NUMERIC | Revenu moyen par mile (via macro dbt `revenue_per_mile`) |
| `avg_revenue_per_trip` | NUMERIC | Revenu moyen par course ($) |
| `avg_distance_per_trip` | NUMERIC | Distance moyenne par course (miles) |

### `pipeline_metrics` (Monitoring — exécutions du pipeline)

| Colonne | Type | Description |
|---|---|---|
| `id` | SERIAL PK | Identifiant unique du run |
| `run_date` | TIMESTAMP | Date d'exécution |
| `script_name` | VARCHAR(50) | Nom du script exécuté |
| `year` | INTEGER | Année traitée |
| `month` | INTEGER | Mois traité |
| `total_rows` | BIGINT | Lignes brutes |
| `valid_rows` | BIGINT | Lignes valides chargées |
| `rejected_rows` | BIGINT | Lignes rejetées |
| `rejection_rate_pct` | NUMERIC(5,2) | Taux de rejet (%) |
| `duration_seconds` | NUMERIC(10,2) | Durée du run |
| `status` | VARCHAR(20) | SUCCESS / FAILED |
| `error_message` | TEXT | Message d'erreur (si échec) |

### `streaming_metrics` (Temps réel — fenêtres Kafka)

| Colonne | Type | Description |
|---|---|---|
| `window_start` | TIMESTAMP | Début de la fenêtre de 1 minute |
| `window_end` | TIMESTAMP | Fin de la fenêtre |
| `trip_count` | INTEGER | Nombre de courses dans la fenêtre |
| `total_revenue` | NUMERIC | Revenu total dans la fenêtre ($) |
| `avg_revenue` | NUMERIC | Revenu moyen par course ($) |
| `avg_distance` | NUMERIC | Distance moyenne par course (miles) |

### `revenue_predictions` (ML — prédictions)

| Colonne | Type | Description |
|---|---|---|
| `trip_date` | DATE | Date prédite |
| `revenue` | NUMERIC | Revenu réel ($) |
| `predicted_revenue` | NUMERIC | Revenu prédit par le modèle ($) |
| `prediction_error` | NUMERIC | Erreur de prédiction ($) |
| `error_pct` | NUMERIC | Erreur relative (%) |

---

## 🗄️ Modèles dbt — Lignage des données

```
final_taxi_data (Silver — chargée par Spark depuis processed/)
        │
        ▼
stg_silver_taxi (Staging — vue de lecture avec 5 tests source)
        │
        ├──────────────────────────────────────┐
        ▼                                       ▼
daily_taxi_summary (Gold — résumé quotidien)   │
        │                                       │
        ├──────────────────────────────────────┐│
        ▼               ▼               ▼      ▼▼
weekly_performance   top_pickup_zones   trip_efficiency
(Gold)               (Gold)             (Gold — via macro
                                        revenue_per_mile)
```

---

## 🔮 Évolutions à venir

- [ ] **Agentic AI** — Agent autonome qui surveille `pipeline_metrics` et réagit intelligemment aux anomalies (LangChain + Ollama)
- [ ] **Great Expectations** — Data contracts formels avec GE
- [ ] **Terraform** — Infrastructure as Code pour déploiement cloud (AWS EKS + RDS + MSK)
- [ ] **Apache Iceberg** — Format de table pour le Data Lake (ACID transactions)
- [ ] **Streamlit Dashboard** — Dashboard temps réel des métriques streaming
- [ ] **Prometheus + Grafana** — Monitoring infrastructure

---

## 💻 Commandes utiles

### Gestion de la stack Docker

```bash
# Démarrer la stack
docker compose up -d

# Arrêter la stack
docker compose down

# Voir les logs
docker compose logs -f

# Redémarrer un service
docker compose restart spark
```

### Pipeline ETL

```bash
# Déclencher le pipeline complet
docker exec local_orchestrator_airflow airflow dags trigger \
  nyc_taxi_pipeline_automation \
  --exec-date 2026-01-01T00:00:00+00:00

# Backfill historique (plusieurs mois)
docker exec local_orchestrator_airflow airflow dags backfill \
  nyc_taxi_pipeline_automation \
  --start-date 2026-01-01 \
  --end-date 2026-06-01 \
  --max-active-runs 1
```

### Streaming

```bash
# Producteur Kafka (2 courses/seconde)
docker exec -u 0 local_orchestrator_airflow \
  python3 -u /opt/apps/kafka_producer.py

# Consommateur Spark Streaming
docker exec local_spark_engine /opt/spark/bin/spark-submit \
  --master spark://spark:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262,\
org.postgresql:postgresql:42.6.0 \
  /opt/spark/apps/spark_streaming_consumer.py
```

### Machine Learning

```bash
# Entraînement manuel
docker exec -e MLFLOW_TRACKING_URI=http://localhost:5000 \
  local_mlflow python3 /opt/apps/train_model.py

# Voir les résultats MLflow
# http://localhost:5000
```

### Chatbot RAG

```bash
# Lancer le chatbot
docker exec local_dbt python -m streamlit run \
  /dbt/chatbot_nyc_taxi.py \
  --server.address 0.0.0.0 --server.port 8501

# Ouvrir : http://localhost:8501
```

### Tests

```bash
# Tests pytest (17 tests)
docker exec \
  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  local_spark_engine \
  python3 -m pytest /opt/spark/apps/tests/test_transform.py -v

# Tests dbt (18 tests)
docker exec -e DBT_PROFILES_DIR=/dbt/nyc_taxi -w /dbt/nyc_taxi \
  local_dbt dbt test

# Tests dbt — Gold uniquement
docker exec -e DBT_PROFILES_DIR=/dbt/nyc_taxi -w /dbt/nyc_taxi \
  local_dbt dbt test --select gold
```

### API

```bash
# Health check
curl http://localhost:8000/

# Métriques quotidiennes
curl http://localhost:8000/metrics/daily?limit=7

# Prédiction de revenu
curl "http://localhost:8000/predict/revenue?trip_count=500&total_distance=1200&day_of_week=4"

# Données streaming
curl http://localhost:8000/streaming/latest

# Documentation Swagger
# http://localhost:8000/docs
```

### Git

```bash
cd D:\nyc_taxi_pipeline1
git add .
git commit -m "feat: mise à jour de la plateforme NYC Taxi"
git push origin main
```

---

## 📊 Équivalence Cloud

| Service local | Équivalent AWS |
|---|---|
| MinIO | Amazon S3 |
| Spark cluster | Amazon EMR |
| Airflow | Amazon MWAA |
| PostgreSQL | Amazon RDS |
| dbt | dbt Cloud |
| Metabase | Amazon QuickSight / Tableau |
| Kafka | Amazon MSK |
| MLflow | Amazon SageMaker |
| FastAPI | AWS Lambda + API Gateway |
| Streamlit | Streamlit Community Cloud |

---

## 👤 Auteur

**Yassin Lajmi** — Senior Data & AI Engineer

- 🐙 GitHub : [@YASINCHA](https://github.com/YASINCHA)
- 📧 Email : lajmiyassin280@gmail.com
- 🌍 Tunisie — Remote Europe

> *"Stack locale équivalente à : AWS S3 + EMR + MWAA + RDS + dbt Cloud + MSK + SageMaker + QuickSight"*

---

<div align="center">
  <strong>⭐ Si ce projet vous a été utile, n'hésitez pas à laisser une star sur GitHub !</strong>
</div>

