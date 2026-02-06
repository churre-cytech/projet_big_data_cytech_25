"""Constantes de configuration pour le service de prediction ML.

Notes
-----
Centralise toutes les constantes du projet.
Les identifiants MinIO correspondent au docker-compose.yml du projet.
"""

import os

# -- Configuration MinIO / S3 --
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "nyc-clean")
PARQUET_KEY = os.getenv("PARQUET_KEY", "yellow_tripdata_2025-05_clean.parquet")

# -- Configuration des features --
# Seules les colonnes utilisables comme features (sans leakage).
FEATURE_COLUMNS = [
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "VendorID",
    "RatecodeID",
    "payment_type",
    "store_and_fwd_flag",
]

# Colonne source pour les features temporelles
DATETIME_COLUMN = "tpep_pickup_datetime"

# Features temporelles derivees (creees dans feature_engineering.py)
TIME_FEATURES = [
    "pickup_hour",
    "pickup_dayofweek",
    "pickup_is_weekend",
]

# Toutes les features du modele (brutes + derivees)
ALL_FEATURES = FEATURE_COLUMNS + TIME_FEATURES

# Variable cible
TARGET_COLUMN = "total_amount"

# Colonnes INTERDITES comme features (data leakage)
# total_amount = somme de toutes ces colonnes, les utiliser = tricher
LEAKAGE_COLUMNS = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
]

# -- Configuration du modele --
MODEL_DIR = os.getenv("MODEL_DIR", "models")
MODEL_FILENAME = os.getenv("MODEL_FILENAME", "lgbm_total_amount.joblib")
RANDOM_STATE = 42
TEST_SIZE = 0.2
