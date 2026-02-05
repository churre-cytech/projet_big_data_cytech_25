"""Tests unitaires pour le pipeline de feature engineering."""

import pandas as pd
import numpy as np
import pytest

from src.feature_engineering import (
    add_time_features,
    encode_store_and_fwd,
    prepare_features,
    validate_no_leakage,
)
from src.config import LEAKAGE_COLUMNS, ALL_FEATURES


class TestTimeFeatures:
    """Teste l'extraction des features temporelles."""

    def test_pickup_hour_entre_0_et_23(self, sample_valid_df):
        """Verifie que pickup_hour est entre 0 et 23."""
        df = add_time_features(sample_valid_df)
        assert df["pickup_hour"].between(0, 23).all()

    def test_pickup_dayofweek_entre_0_et_6(self, sample_valid_df):
        """Verifie que pickup_dayofweek est entre 0 et 6."""
        df = add_time_features(sample_valid_df)
        assert df["pickup_dayofweek"].between(0, 6).all()

    def test_is_weekend_binaire(self, sample_valid_df):
        """Verifie que pickup_is_weekend vaut 0 ou 1."""
        df = add_time_features(sample_valid_df)
        assert set(df["pickup_is_weekend"].unique()).issubset({0, 1})


class TestEncoding:
    """Teste l'encodage des variables categorielles."""

    def test_store_and_fwd_devient_int(self, sample_valid_df):
        """Verifie que store_and_fwd_flag est converti en 0/1."""
        df = encode_store_and_fwd(sample_valid_df)
        assert df["store_and_fwd_flag"].dtype in [np.int64, np.int32, int]
        assert set(df["store_and_fwd_flag"].unique()).issubset({0, 1})


class TestLeakagePrevention:
    """Teste la protection contre le data leakage."""

    def test_colonnes_leakage_detectees(self):
        """Verifie que les colonnes de leakage levent une erreur."""
        bad_features = ["trip_distance", "fare_amount"]
        with pytest.raises(ValueError, match="leakage"):
            validate_no_leakage(pd.DataFrame(), bad_features)

    def test_features_safe_passent(self):
        """Verifie que ALL_FEATURES ne contient aucune colonne de leakage."""
        leaked = set(ALL_FEATURES) & set(LEAKAGE_COLUMNS)
        assert len(leaked) == 0, f"Leakage detecte : {leaked}"

    def test_prepare_features_dimensions(self, sample_valid_df):
        """Verifie les dimensions de sortie de prepare_features."""
        X, y = prepare_features(sample_valid_df)
        assert X.shape[0] == len(sample_valid_df)
        assert X.shape[1] == len(ALL_FEATURES)
        assert len(y) == len(sample_valid_df)
