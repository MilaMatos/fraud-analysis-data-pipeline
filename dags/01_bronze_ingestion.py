import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from airflow.models import Variable

# Extrai CSV e salva como Parquet
def load_bronze():
    spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()
    
    # Caminhos de origem e destino
    BASE_PATH = Variable.get("DATA_LAKE_PATH", default_var="/opt/airflow/data")
    source_path = os.path.join(BASE_PATH, "df_fraud_credit.csv")
    target_path = os.path.join(BASE_PATH, "bronze", "fraud_data")
    
    # Lê arquivo bruto
    df = spark.read.csv(source_path, header=True, inferSchema=True)
    
    # Grava na camada Bronze
    df.write.mode("overwrite").parquet(target_path)
    
    spark.stop()

# Define a DAG
with DAG(
    dag_id="01_bronze_ingestion",
    start_date=datetime(2026, 9, 10),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "ingestion"]
) as dag:

    ingest_task = PythonOperator(
        task_id="load_csv_to_bronze",
        python_callable=load_bronze
    )