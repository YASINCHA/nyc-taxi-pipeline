# ==============================================================================
# tests/test_transform.py — Tests unitaires pour transform.py
#
# Ces tests vérifient le comportement des fonctions de transformation
# sans avoir besoin de MinIO, Spark cluster, ou PostgreSQL.
# Ils utilisent une SparkSession locale en mémoire.
#
# Lancement :
#   docker exec local_spark_engine python3 -m pytest /opt/spark/apps/tests/ -v
# ==============================================================================

import sys
import os
import pytest

# Ajout du répertoire parent pour importer transform.py
sys.path.insert(0, "/opt/spark/apps")

# Variables d'environnement minimales pour importer transform.py
os.environ.setdefault("MINIO_ACCESS_KEY", "test_key")
os.environ.setdefault("MINIO_SECRET_KEY", "test_secret")
os.environ.setdefault("POSTGRES_USER",    "test_user")
os.environ.setdefault("POSTGRES_PASSWORD","test_pass")
os.environ.setdefault("POSTGRES_DB",      "test_db")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, TimestampType
)
from datetime import datetime

from transform import apply_quality_filters, run_quality_checks


# ─────────────────────────────────────────────────────────────────────────────
# Fixture SparkSession locale (partagée entre tous les tests)
# ─────────────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def spark():
    """SparkSession locale pour les tests — pas de cluster nécessaire."""
    session = (
        SparkSession.builder
        .appName("NYC_Taxi_Tests")
        .master("local[1]")           # Mode local, 1 thread
        .config("spark.ui.enabled", "false")      # Désactive l'UI Spark pendant les tests
        .config("spark.sql.shuffle.partitions", "2")  # Réduit pour les tests
        .getOrCreate()
    )
    yield session
    session.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Schéma de test (simplifié par rapport au schéma réel TLC)
# ─────────────────────────────────────────────────────────────────────────────
def make_schema():
    return StructType([
        StructField("tpep_pickup_datetime",  TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count",       LongType(),      True),
        StructField("trip_distance",         DoubleType(),    True),
        StructField("total_amount",          DoubleType(),    True),
    ])


def make_row(pickup="2026-01-15 08:00:00", dropoff="2026-01-15 08:30:00",
             passengers=2, distance=5.0, amount=15.0):
    """Crée une ligne valide par défaut, paramétrable pour les cas d'erreur."""
    fmt = "%Y-%m-%d %H:%M:%S"
    return (
        datetime.strptime(pickup, fmt),
        datetime.strptime(dropoff, fmt),
        passengers,
        distance,
        amount,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Tests — apply_quality_filters()
# ─────────────────────────────────────────────────────────────────────────────
class TestApplyQualityFilters:

    def test_ligne_valide_conservee(self, spark):
        """Une ligne valide doit passer tous les filtres."""
        df = spark.createDataFrame([make_row()], schema=make_schema())
        result = apply_quality_filters(df)
        assert result.count() == 1

    def test_distance_zero_rejetee(self, spark):
        """Une course avec distance = 0 doit être rejetée."""
        df = spark.createDataFrame([make_row(distance=0.0)], schema=make_schema())
        result = apply_quality_filters(df)
        assert result.count() == 0

    def test_distance_negative_rejetee(self, spark):
        """Une course avec distance négative doit être rejetée."""
        df = spark.createDataFrame([make_row(distance=-1.0)], schema=make_schema())
        result = apply_quality_filters(df)
        assert result.count() == 0

    def test_montant_zero_rejete(self, spark):
        """Une course avec montant = 0 doit être rejetée."""
        df = spark.createDataFrame([make_row(amount=0.0)], schema=make_schema())
        result = apply_quality_filters(df)
        assert result.count() == 0

    def test_montant_negatif_rejete(self, spark):
        """Une course avec montant négatif doit être rejetée."""
        df = spark.createDataFrame([make_row(amount=-5.0)], schema=make_schema())
        result = apply_quality_filters(df)
        assert result.count() == 0

    def test_montant_aberrant_rejete(self, spark):
        """Une course avec montant > 10 000 doit être rejetée."""
        df = spark.createDataFrame([make_row(amount=15000.0)], schema=make_schema())
        result = apply_quality_filters(df)
        assert result.count() == 0

    def test_passagers_zero_rejetes(self, spark):
        """Une course avec 0 passager doit être rejetée."""
        df = spark.createDataFrame([make_row(passengers=0)], schema=make_schema())
        result = apply_quality_filters(df)
        assert result.count() == 0

    def test_passagers_negatifs_rejetes(self, spark):
        """Une course avec passagers négatifs doit être rejetée."""
        df = spark.createDataFrame([make_row(passengers=-1)], schema=make_schema())
        result = apply_quality_filters(df)
        assert result.count() == 0

    def test_plusieurs_lignes_filtrage_partiel(self, spark):
        """Sur 3 lignes, seules les valides doivent être conservées."""
        rows = [
            make_row(),                    # valide
            make_row(distance=0.0),        # rejetée
            make_row(amount=-10.0),        # rejetée
        ]
        df = spark.createDataFrame(rows, schema=make_schema())
        result = apply_quality_filters(df)
        assert result.count() == 1

    def test_dataframe_vide(self, spark):
        """Un DataFrame vide doit rester vide après filtrage."""
        df = spark.createDataFrame([], schema=make_schema())
        result = apply_quality_filters(df)
        assert result.count() == 0

    def test_taux_rejet_coherent(self, spark):
        """Le taux de rejet doit être calculable et cohérent."""
        rows = [make_row() for _ in range(7)] + \
               [make_row(distance=0.0) for _ in range(3)]
        df = spark.createDataFrame(rows, schema=make_schema())
        result = apply_quality_filters(df)
        assert result.count() == 7
        rejection_rate = (10 - 7) / 10
        assert abs(rejection_rate - 0.30) < 0.01


# ─────────────────────────────────────────────────────────────────────────────
# Tests — run_quality_checks()
# ─────────────────────────────────────────────────────────────────────────────
class TestRunQualityChecks:

    def test_donnees_valides_passent(self, spark):
        """Des données valides doivent passer tous les contrôles sans exception."""
        rows = [make_row() for _ in range(10)]
        df = spark.createDataFrame(rows, schema=make_schema())
        # Ne doit pas lever d'exception
        run_quality_checks(df, year=2026, month=1)

    def test_dataframe_vide_leve_exception(self, spark):
        """Un DataFrame vide doit lever une ValueError."""
        df = spark.createDataFrame([], schema=make_schema())
        with pytest.raises(ValueError, match="vide"):
            run_quality_checks(df, year=2026, month=1)

    def test_dates_incoherentes_bloquantes(self, spark):
        """Plus de 5% de dates incohérentes doit lever une ValueError."""
        # 6 lignes avec mauvais mois (juillet au lieu de janvier) = 60% > seuil 5%
        bad_rows = [make_row(pickup="2026-07-15 08:00:00",
                             dropoff="2026-07-15 08:30:00") for _ in range(6)]
        good_rows = [make_row() for _ in range(4)]
        df = spark.createDataFrame(bad_rows + good_rows, schema=make_schema())
        with pytest.raises(ValueError):
            run_quality_checks(df, year=2026, month=1)

    def test_dates_incoherentes_sous_seuil_passent(self, spark):
        """Moins de 5% de dates incohérentes doit générer un WARNING sans exception."""
        # 1 ligne avec mauvais mois sur 100 = 1% < seuil 5%
        bad_rows  = [make_row(pickup="2026-07-15 08:00:00",
                              dropoff="2026-07-15 08:30:00")]
        good_rows = [make_row() for _ in range(99)]
        df = spark.createDataFrame(bad_rows + good_rows, schema=make_schema())
        # Ne doit pas lever d'exception
        run_quality_checks(df, year=2026, month=1)

    def test_pickup_apres_dropoff_bloquant(self, spark):
        """Plus de 2% de pickup >= dropoff doit lever une ValueError."""
        # 5 lignes invalides sur 100 = 5% > seuil 2%
        bad_rows  = [make_row(pickup="2026-01-15 09:00:00",
                              dropoff="2026-01-15 08:00:00") for _ in range(5)]
        good_rows = [make_row() for _ in range(95)]
        df = spark.createDataFrame(bad_rows + good_rows, schema=make_schema())
        with pytest.raises(ValueError):
            run_quality_checks(df, year=2026, month=1)

    def test_pickup_apres_dropoff_sous_seuil_passe(self, spark):
        """Moins de 2% de pickup >= dropoff doit générer un WARNING sans exception."""
        # 1 ligne invalide sur 100 = 1% < seuil 2%
        bad_rows  = [make_row(pickup="2026-01-15 09:00:00",
                              dropoff="2026-01-15 08:00:00")]
        good_rows = [make_row() for _ in range(99)]
        df = spark.createDataFrame(bad_rows + good_rows, schema=make_schema())
        # Ne doit pas lever d'exception
        run_quality_checks(df, year=2026, month=1)
