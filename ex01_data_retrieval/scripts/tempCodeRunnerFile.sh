#!/bin/bash
set -e

URL_PARQUET_FILE="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-05.parquet"
URL_MINIO_BACKEND="http://localhost:9000"

mc alias set local "$URL_MINIO_BACKEND" minio minio123
mc mb -p local/nyc-raw
curl -L "$URL_PARQUET_FILE" | mc pipe local/nyc-raw/yellow_tripdata_2025-05.parquet
