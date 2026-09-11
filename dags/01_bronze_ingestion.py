import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from airflow.models import Variable

BASE_PATH = Variable.get("DATA_LAKE_PATH", default_var="/opt/airflow/data")


# Extrai CSV e salva como Parquet
def load_bronze():
    spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

    # Caminhos de origem e destino
    source_path = os.path.join(BASE_PATH, "df_fraud_credit.csv")
    target_path = os.path.join(BASE_PATH, "bronze", "fraud_data")

    # Lê arquivo bruto
    df = spark.read.csv(source_path, header=True, inferSchema=True)

    # Grava na camada Bronze
    df.write.mode("overwrite").parquet(target_path)

    spark.stop()


def dq_check_bronze():
    spark = SparkSession.builder.appName("BronzeDQ").getOrCreate()
    target_path = os.path.join(BASE_PATH, "bronze", "fraud_data")

    df = spark.read.parquet(target_path)

    # Regra 1: O arquivo não pode estar vazio
    row_count = df.count()
    if row_count == 0:
        raise ValueError("DQ Fail: A camada Bronze esta vazia. Nenhum dado lido.")

    # Regra 2: Validacao de Contrato (Colunas criticas devem existir)
    expected_columns = [
        "timestamp",
        "receiving_address",
        "amount",
        "transaction_type",
        "location_region",
        "risk_score",
    ]

    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"DQ Fail: Colunas ausentes no schema - {missing_cols}")

    print(f"Data Quality Bronze OK. Volume: {row_count} registros validados.")
    spark.stop()


# Define a DAG
with DAG(
    dag_id="01_bronze_ingestion",
    start_date=datetime(2026, 9, 10),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "ingestion", "data-quality"],
) as dag:

    ingest_task = PythonOperator(
        task_id="load_csv_to_bronze", python_callable=load_bronze
    )

    dq_task = PythonOperator(task_id="dq_check_bronze", python_callable=dq_check_bronze)

    # Ordem de execução
    ingest_task >> dq_task
