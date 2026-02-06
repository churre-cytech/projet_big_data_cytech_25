from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


MINIO_ENV = {
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ACCESS_KEY": "minio",
    "MINIO_SECRET_KEY": "minio123",
}

PG_ENV = {
    "PG_HOST": "postgres",
    "PG_PORT": "5432",
    "PG_DATABASE": "bigdata_dwh",
    "PG_USER": "bigdata",
    "PG_PASSWORD": "bigdata123",
}

with DAG(
    dag_id="tp_bigdata_pipeline",
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
    tags=["bigdata", "tp"],
) as dag:
    wait_services = BashOperator(
        task_id="wait_services",
        bash_command="""
        until curl -sf http://minio:9000/minio/health/live > /dev/null; do sleep 2; done
        until pg_isready -h postgres -U bigdata -d bigdata_dwh > /dev/null; do sleep 2; done
        """,
    )

    ex03_init_schema = BashOperator(
        task_id="ex03_init_schema",
        env={"PGPASSWORD": PG_ENV["PG_PASSWORD"]},
        append_env=True,
        bash_command="psql -h postgres -U bigdata -d bigdata_dwh -v ON_ERROR_STOP=1 -f /workspace/ex03_sql_table_creation/creation.sql",
    )

    ex03_seed_dimensions = BashOperator(
        task_id="ex03_seed_dimensions",
        env={"PGPASSWORD": PG_ENV["PG_PASSWORD"]},
        append_env=True,
        bash_command="psql -h postgres -U bigdata -d bigdata_dwh -v ON_ERROR_STOP=1 -f /workspace/ex03_sql_table_creation/insertion.sql",
    )

    ex01_retrieval = BashOperator(
        task_id="ex01_retrieval",
        env=MINIO_ENV,
        append_env=True,
        bash_command="""
            set -euo pipefail
            test -f /workspace/ex01_data_retrieval/target/scala-2.13/ex01-retrieval-assembly.jar
            java --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
            -jar /workspace/ex01_data_retrieval/target/scala-2.13/ex01-retrieval-assembly.jar
        """,
    )

    ex02_ingestion = BashOperator(
        task_id="ex02_ingestion",
        env={
            **MINIO_ENV,
            **PG_ENV,
            "TAXI_ZONE_LOOKUP_PATH": "/workspace/ex02_data_ingestion/src/main/resources/taxi_zone_lookup.csv",
        },
        append_env=True,
        bash_command="""
            set -euo pipefail
            test -f /workspace/ex02_data_ingestion/target/scala-2.13/ex02-ingestion-assembly.jar
            java --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
            --add-exports=java.base/sun.util.calendar=ALL-UNNAMED \
            -jar /workspace/ex02_data_ingestion/target/scala-2.13/ex02-ingestion-assembly.jar
        """,
    )

    ex05_training = BashOperator(
        task_id="ex05_training",
        env={
            **MINIO_ENV,
            "MINIO_BUCKET": "nyc-clean",
            "PARQUET_KEY": "yellow_tripdata_2025-05_clean.parquet",
            "MODEL_DIR": "/tmp/models",
        },
        append_env=True,
        bash_command="cd /workspace/ex05_ml_prediction_service && python -m src.train",
    )

    wait_services >> ex03_init_schema >> ex03_seed_dimensions >> ex01_retrieval >> ex02_ingestion >> ex05_training
