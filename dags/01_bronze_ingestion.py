import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from airflow.models import Variable

# ==========================================
# PARÂMETROS E REGRAS DE NEGÓCIO CENTRALIZADAS
# ==========================================
BASE_PATH = Variable.get("DATA_LAKE_PATH", default_var="/opt/airflow/data")
SOURCE_CSV_NAME = "df_fraud_credit.csv"
BRONZE_TARGET_DIR = "fraud_data"
EXPECTED_COLUMNS = [
    "timestamp",
    "receiving_address",
    "amount",
    "transaction_type",
    "location_region",
    "risk_score",
]


def load_bronze(source_path_override=None, target_path_override=None, **kwargs):
    spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

    source_path = source_path_override or os.path.join(BASE_PATH, SOURCE_CSV_NAME)
    target_path = target_path_override or os.path.join(
        BASE_PATH, "bronze", BRONZE_TARGET_DIR
    )

    df = spark.read.csv(source_path, header=True, inferSchema=True)

    df.write.mode("overwrite").parquet(target_path)
    spark.stop()


def dq_check_bronze():
    spark = SparkSession.builder.appName("BronzeDQ").getOrCreate()
    target_path = os.path.join(BASE_PATH, "bronze", BRONZE_TARGET_DIR)

    df = spark.read.parquet(target_path)

    if df.isEmpty():
        spark.stop()
        raise ValueError("DQ Fail: A camada Bronze está vazia. Nenhum dado lido.")

    # Validação de Contrato
    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        spark.stop()
        raise ValueError(f"DQ Fail: Colunas ausentes no schema - {missing_cols}")

    print("Data Quality Bronze OK. Estrutura de colunas e dados validados com sucesso.")
    spark.stop()


with DAG(
    dag_id="01_bronze_ingestion",
    start_date=datetime(2026, 9, 10),
    schedule=None,
    catchup=False,
    tags=["bronze", "ingestion", "data-quality"],
) as dag:

    ingest_task = PythonOperator(
        task_id="load_csv_to_bronze", python_callable=load_bronze
    )

    dq_task = PythonOperator(task_id="dq_check_bronze", python_callable=dq_check_bronze)

    ingest_task >> dq_task
