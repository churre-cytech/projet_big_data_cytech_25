"""Chargement des donnees NYC taxi nettoyees depuis MinIO.

Notes
-----
Le fichier parquet dans MinIO a ete produit par Spark (ex02).
Spark ecrit un dossier contenant plusieurs fichiers part-*.parquet.
Ce module gere ce format en listant et concatenant les fichiers.
"""

import io

import boto3
import pandas as pd

from src.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
    PARQUET_KEY,
)


def get_s3_client():
    """Cree un client boto3 S3 configure pour MinIO.

    Returns
    -------
    botocore.client.S3
        Client S3 pointe vers le MinIO local.
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def load_clean_data():
    """Charge le parquet nettoye depuis MinIO dans un DataFrame.

    Spark ecrit le parquet comme un dossier de fichiers part-*.parquet.
    Cette fonction liste tous les fichiers .parquet dans le dossier
    et les concatene en un seul DataFrame.

    Returns
    -------
    pandas.DataFrame
        Les donnees de courses de taxi nettoyees.

    Raises
    ------
    FileNotFoundError
        Si aucun fichier parquet n'est trouve dans le bucket.
    """
    client = get_s3_client()

    # Lister tous les fichiers part-*.parquet dans le dossier
    response = client.list_objects_v2(
        Bucket=MINIO_BUCKET,
        Prefix=PARQUET_KEY,
    )

    parts = []
    for obj_meta in response.get("Contents", []):
        key = obj_meta["Key"]
        if key.endswith(".parquet"):
            obj = client.get_object(Bucket=MINIO_BUCKET, Key=key)
            part_df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            parts.append(part_df)

    if not parts:
        raise FileNotFoundError(
            f"Aucun fichier parquet dans {MINIO_BUCKET}/{PARQUET_KEY}"
        )

    return pd.concat(parts, ignore_index=True)
