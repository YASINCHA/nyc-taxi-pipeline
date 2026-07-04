-- ==============================================================================
-- sql/init_schema.sql — Initialisation du schéma PostgreSQL
--
-- Exécuté automatiquement par PostgreSQL au premier démarrage du conteneur.
-- Crée les tables cibles pour le pipeline NYC Taxi.
-- ==============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- Table Silver : données de courses individuelles
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS final_taxi_data (
    vendor_id           INTEGER,
    tpep_pickup_datetime  TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count     INTEGER,
    trip_distance       DOUBLE PRECISION,
    ratecode_id         INTEGER,
    store_and_fwd_flag  VARCHAR(1),
    pu_location_id      INTEGER,
    do_location_id      INTEGER,
    payment_type        INTEGER,
    fare_amount         DOUBLE PRECISION,
    extra               DOUBLE PRECISION,
    mta_tax             DOUBLE PRECISION,
    tip_amount          DOUBLE PRECISION,
    tolls_amount        DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount        DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION
);

-- Table de staging pour le swap atomique (même schéma)
CREATE TABLE IF NOT EXISTS final_taxi_data_staging (LIKE final_taxi_data INCLUDING ALL);

-- Index pour améliorer les performances des requêtes analytiques
CREATE INDEX IF NOT EXISTS idx_final_taxi_pickup_datetime
    ON final_taxi_data (tpep_pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_final_taxi_pu_location
    ON final_taxi_data (pu_location_id);


-- ─────────────────────────────────────────────────────────────────────────────
-- Table Gold : résumé quotidien agrégé
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_taxi_summary (
    trip_date       DATE        NOT NULL,
    revenue         DOUBLE PRECISION,
    total_distance  DOUBLE PRECISION,
    trip_count      BIGINT,

    CONSTRAINT pk_daily_taxi_summary PRIMARY KEY (trip_date)
);

-- Table de staging pour le swap atomique
CREATE TABLE IF NOT EXISTS daily_taxi_summary_staging (LIKE daily_taxi_summary INCLUDING ALL);

-- ─────────────────────────────────────────────────────────────────────────────
-- Vue analytique : performances par jour de la semaine
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_weekly_performance AS
SELECT
    TO_CHAR(trip_date, 'Day')           AS day_of_week,
    EXTRACT(DOW FROM trip_date)         AS dow_number,
    ROUND(AVG(revenue)::NUMERIC, 2)     AS avg_daily_revenue,
    ROUND(AVG(trip_count)::NUMERIC, 0)  AS avg_daily_trips,
    COUNT(*)                            AS data_points
FROM daily_taxi_summary
GROUP BY TO_CHAR(trip_date, 'Day'), EXTRACT(DOW FROM trip_date)
ORDER BY dow_number;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table de monitoring : métriques de chaque run du pipeline
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id                  SERIAL PRIMARY KEY,
    run_date            TIMESTAMP DEFAULT NOW(),
    script_name         VARCHAR(50),
    year                INTEGER,
    month               INTEGER,
    total_rows          BIGINT,
    valid_rows          BIGINT,
    rejected_rows       BIGINT,
    rejection_rate_pct  NUMERIC(5,2),
    duration_seconds    NUMERIC(10,2),
    status              VARCHAR(20),
    error_message       TEXT
);