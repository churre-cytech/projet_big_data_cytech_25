"""Pipeline d'inference pour la prediction de tarif taxi NYC.

Charge un modele entraine et fait des predictions.
Deux modes : batch (parquet complet) ou single (une course via CLI).

Usage
-----
    uv run python -m src.predict
    uv run python -m src.predict --single --distance 5.2 \
        --pu-location 138 --do-location 236
"""

import os
import argparse
import logging

import joblib
import pandas as pd
import numpy as np

from src.config import (
    MODEL_DIR,
    MODEL_FILENAME,
    ALL_FEATURES,
)
from src.data_loader import load_clean_data
from src.feature_engineering import prepare_features

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_model():
    """Charge le modele entraine depuis le disque.

    Returns
    -------
    lightgbm.LGBMRegressor
        Le modele entraine.

    Raises
    ------
    FileNotFoundError
        Si le fichier modele n'existe pas. Lancer train.py d'abord.
    """
    model_path = os.path.join(MODEL_DIR, MODEL_FILENAME)
    if not os.path.exists(model_path):
        raise FileNotFoundError(
            f"Modele introuvable : {model_path}. Lancez train.py d'abord."
        )
    return joblib.load(model_path)


def predict_batch(model, df):
    """Fait des predictions sur un DataFrame complet.

    Parameters
    ----------
    model : lightgbm.LGBMRegressor
        Le modele entraine.
    df : pandas.DataFrame
        DataFrame brut (meme schema que les donnees d'entrainement).

    Returns
    -------
    numpy.ndarray
        Valeurs predites de total_amount.
    """
    X, y_true = prepare_features(df)
    y_pred = model.predict(X)

    rmse = np.sqrt(np.mean((y_true - y_pred) ** 2))
    logger.info(f"RMSE sur le batch : {rmse:.4f}")
    logger.info(
        f"Predictions : min={y_pred.min():.2f}, "
        f"max={y_pred.max():.2f}, mean={y_pred.mean():.2f}"
    )

    return y_pred


def predict_single(model, trip_data):
    """Predit total_amount pour une seule course.

    Parameters
    ----------
    model : lightgbm.LGBMRegressor
        Le modele entraine.
    trip_data : dict
        Dictionnaire avec les cles correspondant a ALL_FEATURES.

    Returns
    -------
    float
        total_amount predit.
    """
    df = pd.DataFrame([trip_data])
    prediction = model.predict(df[ALL_FEATURES])
    return float(prediction[0])


def main():
    """Execute le pipeline d'inference."""
    parser = argparse.ArgumentParser(
        description="Prediction du tarif taxi NYC"
    )
    parser.add_argument(
        "--single", action="store_true",
        help="Mode prediction unique (une seule course)"
    )
    parser.add_argument("--distance", type=float, default=3.0)
    parser.add_argument("--pu-location", type=int, default=161)
    parser.add_argument("--do-location", type=int, default=237)
    parser.add_argument("--passengers", type=int, default=1)
    parser.add_argument("--vendor", type=int, default=2)
    parser.add_argument("--ratecode", type=int, default=1)
    parser.add_argument("--payment", type=int, default=1)
    parser.add_argument("--hour", type=int, default=12)
    parser.add_argument("--dayofweek", type=int, default=2)
    args = parser.parse_args()

    model = load_model()

    if args.single:
        trip = {
            "trip_distance": args.distance,
            "PULocationID": args.pu_location,
            "DOLocationID": args.do_location,
            "passenger_count": args.passengers,
            "VendorID": args.vendor,
            "RatecodeID": args.ratecode,
            "payment_type": args.payment,
            "store_and_fwd_flag": 0,
            "pickup_hour": args.hour,
            "pickup_dayofweek": args.dayofweek,
            "pickup_is_weekend": 1 if args.dayofweek >= 5 else 0,
        }
        prediction = predict_single(model, trip)
        logger.info(f"Total amount predit : ${prediction:.2f}")
    else:
        logger.info("Chargement des donnees batch depuis MinIO...")
        df = load_clean_data()
        predict_batch(model, df)


if __name__ == "__main__":
    main()
