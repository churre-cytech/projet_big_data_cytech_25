"""Feature engineering pour la prediction du tarif taxi NYC.

Transforme les donnees brutes nettoyees en features pour le modele.
Extrait les features temporelles et encode les variables categorielles.

Notes
-----
Les colonnes composantes de total_amount (fare_amount, extra, mta_tax,
tip_amount, tolls_amount, etc.) ne sont JAMAIS utilisees comme features.
Les utiliser serait du data leakage car total_amount = leur somme.
"""

import pandas as pd

from src.config import (
    DATETIME_COLUMN,
    TARGET_COLUMN,
    LEAKAGE_COLUMNS,
    ALL_FEATURES,
)


def validate_no_leakage(df, features):
    """Verifie qu'aucune colonne de leakage n'est dans les features.

    Parameters
    ----------
    df : pandas.DataFrame
        Le DataFrame d'entree.
    features : list of str
        Liste des noms de colonnes features a valider.

    Raises
    ------
    ValueError
        Si une colonne de leakage est trouvee dans les features.
    """
    leaked = set(features) & set(LEAKAGE_COLUMNS)
    if leaked:
        raise ValueError(
            f"Data leakage detecte. Colonnes interdites : {leaked}"
        )


def add_time_features(df):
    """Extrait les features temporelles du datetime de pickup.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame contenant la colonne datetime de pickup.

    Returns
    -------
    pandas.DataFrame
        DataFrame avec les colonnes ajoutees : pickup_hour,
        pickup_dayofweek, pickup_is_weekend.
    """
    dt = pd.to_datetime(df[DATETIME_COLUMN])
    df = df.copy()
    df["pickup_hour"] = dt.dt.hour
    df["pickup_dayofweek"] = dt.dt.dayofweek  # 0=Lundi, 6=Dimanche
    df["pickup_is_weekend"] = (dt.dt.dayofweek >= 5).astype(int)
    return df


def encode_store_and_fwd(df):
    """Convertit store_and_fwd_flag de Y/N vers 1/0.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame contenant la colonne store_and_fwd_flag.

    Returns
    -------
    pandas.DataFrame
        DataFrame avec store_and_fwd_flag en entier (1=Y, 0=N).
    """
    df = df.copy()
    df["store_and_fwd_flag"] = (
        df["store_and_fwd_flag"]
        .map({"Y": 1, "N": 0})
        .fillna(0)
        .astype(int)
    )
    return df


def prepare_features(df):
    """Pipeline complet de feature engineering.

    Applique toutes les transformations et retourne X et y.

    Parameters
    ----------
    df : pandas.DataFrame
        Le DataFrame brut nettoye charge depuis MinIO.

    Returns
    -------
    X : pandas.DataFrame
        Matrice de features avec les colonnes de ALL_FEATURES.
    y : pandas.Series
        Variable cible (total_amount).
    """
    validate_no_leakage(df, ALL_FEATURES)

    df = add_time_features(df)
    df = encode_store_and_fwd(df)

    X = df[ALL_FEATURES].copy()
    y = df[TARGET_COLUMN].copy()

    return X, y
