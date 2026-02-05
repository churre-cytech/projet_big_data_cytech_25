"""Pipeline d'entrainement du modele de prediction de tarif taxi NYC.

Charge les donnees depuis MinIO, cree les features, entraine un
modele LightGBM, evalue les metriques et sauvegarde le modele.

Usage
-----
    uv run python -m src.train
"""

import os
import logging

import joblib
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    mean_squared_error,
    mean_absolute_error,
    r2_score,
)
import lightgbm as lgb

from src.data_loader import load_clean_data
from src.feature_engineering import prepare_features
from src.config import (
    MODEL_DIR,
    MODEL_FILENAME,
    RANDOM_STATE,
    TEST_SIZE,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def train_model(X_train, y_train, categorical_features=None):
    """Entraine un modele LightGBM Regressor.

    Parameters
    ----------
    X_train : pandas.DataFrame
        Matrice de features d'entrainement.
    y_train : pandas.Series
        Valeurs cibles d'entrainement.
    categorical_features : list of str, optional
        Colonnes a traiter comme categorielles par LightGBM.

    Returns
    -------
    lightgbm.LGBMRegressor
        Le modele entraine.
    """
    model = lgb.LGBMRegressor(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=8,
        num_leaves=63,
        min_child_samples=50,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.1,
        reg_lambda=0.1,
        random_state=RANDOM_STATE,
        n_jobs=-1,
        verbose=-1,
    )

    cat_features = categorical_features or [
        "PULocationID", "DOLocationID", "VendorID",
        "RatecodeID", "payment_type",
    ]

    model.fit(
        X_train, y_train,
        categorical_feature=cat_features,
    )
    return model


def evaluate_model(model, X_test, y_test):
    """Evalue le modele et affiche les metriques.

    Parameters
    ----------
    model : lightgbm.LGBMRegressor
        Le modele entraine.
    X_test : pandas.DataFrame
        Matrice de features de test.
    y_test : pandas.Series
        Valeurs cibles de test.

    Returns
    -------
    dict
        Dictionnaire avec RMSE, MAE et R2.
    """
    y_pred = model.predict(X_test)

    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    metrics = {"RMSE": rmse, "MAE": mae, "R2": r2}

    logger.info("=== Evaluation du modele ===")
    logger.info(f"RMSE : {rmse:.4f}  (objectif : < 10.0)")
    logger.info(f"MAE  : {mae:.4f}")
    logger.info(f"R2   : {r2:.4f}")

    if rmse < 10.0:
        logger.info("OK : le RMSE est en dessous du seuil de 10.")
    else:
        logger.warning(
            "ATTENTION : le RMSE depasse le seuil de 10."
        )

    return metrics


def main():
    """Execute le pipeline complet d'entrainement."""
    # 1. Charger les donnees
    logger.info("Chargement des donnees depuis MinIO...")
    df = load_clean_data()
    logger.info(f"Donnees chargees : {len(df):,} lignes.")

    # 2. Feature engineering
    logger.info("Creation des features...")
    X, y = prepare_features(df)
    logger.info(f"Matrice de features : {X.shape}")
    logger.info(f"Features utilisees : {list(X.columns)}")

    # 3. Split train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )
    logger.info(
        f"Train : {len(X_train):,} lignes, Test : {len(X_test):,} lignes"
    )

    # 4. Entrainement
    logger.info("Entrainement du modele LightGBM...")
    model = train_model(X_train, y_train)

    # 5. Evaluation
    metrics = evaluate_model(model, X_test, y_test)

    # 6. Sauvegarde du modele
    os.makedirs(MODEL_DIR, exist_ok=True)
    model_path = os.path.join(MODEL_DIR, MODEL_FILENAME)
    joblib.dump(model, model_path)
    logger.info(f"Modele sauvegarde dans {model_path}")

    return metrics


if __name__ == "__main__":
    main()
