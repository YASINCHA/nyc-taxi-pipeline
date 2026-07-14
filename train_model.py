# ==============================================================================
# train_model.py — MLOps : Entraînement du modèle de prédiction de revenus
#
# Prédit le revenu quotidien total basé sur :
#   - jour de la semaine
#   - nombre de courses
#   - distance totale
#
# Tracked avec MLflow : paramètres, métriques, modèle
# Lancement : python3 /opt/apps/train_model.py
# ==============================================================================

import os
import logging
import psycopg2
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler

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
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
PG_URL = os.environ.get(
    "PG_URL",
    "postgresql://data_engineer:password123@postgres/nyc_taxi_dw"
)
MLFLOW_URL = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT_NAME = "nyc_taxi_revenue_prediction"


# ─────────────────────────────────────────────────────────────────────────────
# Chargement des données depuis PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────
def load_training_data() -> pd.DataFrame:
    """Charge les données Gold depuis PostgreSQL pour l'entraînement."""
    log.info("Chargement des données depuis PostgreSQL...")

    engine = create_engine(PG_URL)
    query = """
        SELECT
            trip_date,
            trip_count,
            revenue,
            total_distance,
            avg_revenue_per_trip,
            avg_distance_per_trip,
            EXTRACT(DOW FROM trip_date)    AS day_of_week,
            EXTRACT(MONTH FROM trip_date)  AS month,
            EXTRACT(DAY FROM trip_date)    AS day_of_month,
            CASE
                WHEN EXTRACT(DOW FROM trip_date) IN (0, 6) THEN 1
                ELSE 0
            END AS is_weekend
        FROM daily_taxi_summary
        WHERE revenue > 0
        ORDER BY trip_date
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    log.info("Données chargées : %d lignes", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Feature Engineering
# ─────────────────────────────────────────────────────────────────────────────
def prepare_features(df: pd.DataFrame):
    """Prépare les features et la target pour l'entraînement."""
    features = [
        "trip_count",
        "total_distance",
        "avg_distance_per_trip",
        "day_of_week",
        "month",
        "day_of_month",
        "is_weekend",
    ]
    target = "revenue"

    X = df[features]
    y = df[target]

    log.info("Features : %s", features)
    log.info("Target   : %s", target)
    log.info("Shape    : X=%s, y=%s", X.shape, y.shape)

    return X, y, features


# ─────────────────────────────────────────────────────────────────────────────
# Entraînement avec MLflow tracking
# ─────────────────────────────────────────────────────────────────────────────
def train_and_track(X, y, features):
    """Entraîne plusieurs modèles et track les résultats dans MLflow."""
    mlflow.set_tracking_uri(MLFLOW_URL)
    mlflow.set_experiment(EXPERIMENT_NAME)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    models = {
        "LinearRegression": LinearRegression(),
        "RandomForest": RandomForestRegressor(
            n_estimators=100, max_depth=5, random_state=42
        ),
        "GradientBoosting": GradientBoostingRegressor(
            n_estimators=100, learning_rate=0.1, max_depth=3, random_state=42
        ),
    }

    best_model = None
    best_r2 = -999
    best_run_id = None

    for model_name, model in models.items():
        log.info("Entraînement : %s", model_name)

        with mlflow.start_run(run_name=model_name):
            # Entraînement
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)

            # Métriques
            mae  = mean_absolute_error(y_test, y_pred)
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            r2   = r2_score(y_test, y_pred)
            cv_scores = cross_val_score(model, X, y, cv=3, scoring="r2")

            log.info(
                "%s → MAE=%.2f | RMSE=%.2f | R²=%.4f | CV R²=%.4f",
                model_name, mae, rmse, r2, cv_scores.mean()
            )

            # Log MLflow
            mlflow.log_param("model_type", model_name)
            mlflow.log_param("features", str(features))
            mlflow.log_param("train_size", len(X_train))
            mlflow.log_param("test_size", len(X_test))

            mlflow.log_metric("mae",     round(mae, 2))
            mlflow.log_metric("rmse",    round(rmse, 2))
            mlflow.log_metric("r2",      round(r2, 4))
            mlflow.log_metric("cv_r2",   round(cv_scores.mean(), 4))

            # Log du modèle
            mlflow.sklearn.log_model(model, artifact_path="model")

            # Meilleur modèle
            if r2 > best_r2:
                best_r2    = r2
                best_model = model
                best_run_id = mlflow.active_run().info.run_id

    log.info("Meilleur modèle : R²=%.4f | run_id=%s", best_r2, best_run_id)
    return best_model, best_run_id, best_r2


# ─────────────────────────────────────────────────────────────────────────────
# Sauvegarde des prédictions dans PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────
def save_predictions(model, df: pd.DataFrame, X: pd.DataFrame) -> None:
    """Génère et sauvegarde les prédictions dans PostgreSQL."""
    log.info("Génération des prédictions...")
    engine = create_engine(PG_URL)

    df_pred = df.copy()
    df_pred["predicted_revenue"] = model.predict(X).round(2)
    df_pred["prediction_error"]  = (df_pred["revenue"] - df_pred["predicted_revenue"]).round(2)
    df_pred["error_pct"]         = (
        (df_pred["prediction_error"] / df_pred["revenue"]) * 100
    ).round(2)

    df_output = df_pred[[
        "trip_date", "revenue", "predicted_revenue",
        "prediction_error", "error_pct"
    ]]

    with engine.connect() as conn:
        df_output.to_sql(
            "revenue_predictions",
            conn,
            if_exists="replace",
            index=False,
        )

    log.info("Prédictions sauvegardées dans 'revenue_predictions' (%d lignes).", len(df_output))


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=== Démarrage de l'entraînement MLOps ===")

    # 1. Charger les données
    df = load_training_data()

    if len(df) < 5:
        log.error("Pas assez de données pour entraîner le modèle (minimum 5 lignes).")
        exit(1)

    # 2. Préparer les features
    X, y, features = prepare_features(df)

    # 3. Entraîner + tracker avec MLflow
    best_model, run_id, r2 = train_and_track(X, y, features)

    # 4. Sauvegarder les prédictions
    save_predictions(best_model, df, X)

    log.info("=== Entraînement terminé | Meilleur R²=%.4f ===", r2)
    log.info("Voir les résultats sur : http://localhost:5000")
