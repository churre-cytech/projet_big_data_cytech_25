"""Fixtures pytest pour les tests du service ML."""

import pandas as pd
import numpy as np
import pytest


@pytest.fixture
def sample_valid_df():
    """Cree un petit DataFrame valide simulant les donnees taxi nettoyees."""
    np.random.seed(42)
    n = 100
    return pd.DataFrame({
        "VendorID": np.random.choice([1, 2, 6, 7], n),
        "tpep_pickup_datetime": pd.date_range(
            "2025-05-01 08:00", periods=n, freq="15min"
        ),
        "tpep_dropoff_datetime": pd.date_range(
            "2025-05-01 08:20", periods=n, freq="15min"
        ),
        "passenger_count": np.random.randint(1, 5, n).astype(float),
        "trip_distance": np.random.uniform(0.5, 20.0, n),
        "RatecodeID": np.random.choice([1, 2, 3, 4, 5, 6], n),
        "store_and_fwd_flag": np.random.choice(["Y", "N"], n),
        "PULocationID": np.random.randint(1, 264, n),
        "DOLocationID": np.random.randint(1, 264, n),
        "payment_type": np.random.choice([0, 1, 2, 3, 4, 5, 6], n),
        "fare_amount": np.random.uniform(5, 100, n),
        "extra": np.random.uniform(0, 5, n),
        "mta_tax": np.full(n, 0.5),
        "tip_amount": np.random.uniform(0, 20, n),
        "tolls_amount": np.random.uniform(0, 10, n),
        "improvement_surcharge": np.full(n, 0.3),
        "congestion_surcharge": np.random.choice([0, 2.5], n),
        "Airport_fee": np.random.choice([0, 1.75], n),
        "cbd_congestion_fee": np.random.choice([0, 2.5], n),
        "total_amount": np.random.uniform(10, 150, n),
    })
