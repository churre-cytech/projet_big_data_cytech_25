import pandas as pd
import s3fs

def main():
    print("Hello from ex05-ml-prediction-service!")

    fs = s3fs.S3FileSystem(
        key="minio",
        secret="minio123",
        client_kwargs={"endpoint_url": "http://localhost:9000"},
    )

    df = pd.read_parquet("nyc-clean/yellow_tripdata_2025-05_clean.parquet", filesystem=fs)
    print(df.count())
    print(len(df))

if __name__ == "__main__":
    main()
