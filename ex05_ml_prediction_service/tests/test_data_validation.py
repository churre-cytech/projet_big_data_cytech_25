"""Tests unitaires de validation des donnees d'entree.

Verifie le schema et la qualite des donnees d'entree,
aussi bien pour l'entrainement que pour l'inference.
"""

from src.config import (
    FEATURE_COLUMNS,
    TARGET_COLUMN,
)


class TestDataSchema:
    """Teste que les donnees ont le bon schema."""

    def test_colonnes_requises_presentes(self, sample_valid_df):
        """Verifie que toutes les colonnes requises existent."""
        required = FEATURE_COLUMNS + [TARGET_COLUMN, "tpep_pickup_datetime"]
        for col in required:
            assert col in sample_valid_df.columns, (
                f"Colonne manquante : {col}"
            )

    def test_pas_de_null_dans_features(self, sample_valid_df):
        """Verifie l'absence de valeurs nulles dans les features."""
        for col in FEATURE_COLUMNS:
            assert sample_valid_df[col].notna().all(), (
                f"Valeurs nulles dans la colonne : {col}"
            )

    def test_target_pas_null(self, sample_valid_df):
        """Verifie que total_amount n'a pas de valeurs nulles."""
        assert sample_valid_df[TARGET_COLUMN].notna().all()

    def test_target_non_negatif(self, sample_valid_df):
        """Verifie que total_amount est positif ou nul."""
        assert (sample_valid_df[TARGET_COLUMN] >= 0).all()

    def test_trip_distance_positif(self, sample_valid_df):
        """Verifie que trip_distance est strictement positif."""
        assert (sample_valid_df["trip_distance"] > 0).all()

    def test_passenger_count_valide(self, sample_valid_df):
        """Verifie que passenger_count est au moins 1."""
        assert (sample_valid_df["passenger_count"] >= 1).all()

    def test_vendor_id_valide(self, sample_valid_df):
        """Verifie que VendorID est dans les valeurs attendues."""
        valid_vendors = {1, 2, 6, 7}
        assert set(sample_valid_df["VendorID"].unique()).issubset(
            valid_vendors
        )

    def test_ratecode_id_valide(self, sample_valid_df):
        """Verifie que RatecodeID est dans les valeurs attendues."""
        valid_ratecodes = {1, 2, 3, 4, 5, 6}
        assert set(sample_valid_df["RatecodeID"].unique()).issubset(
            valid_ratecodes
        )

    def test_store_and_fwd_valide(self, sample_valid_df):
        """Verifie que store_and_fwd_flag est Y ou N."""
        assert set(sample_valid_df["store_and_fwd_flag"].unique()).issubset(
            {"Y", "N"}
        )

    def test_location_ids_positifs(self, sample_valid_df):
        """Verifie que les location IDs sont des entiers positifs."""
        assert (sample_valid_df["PULocationID"] > 0).all()
        assert (sample_valid_df["DOLocationID"] > 0).all()
