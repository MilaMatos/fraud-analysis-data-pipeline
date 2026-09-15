import os
from pyspark.sql import SparkSession
from airflow.models import Variable

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

def process_bronze_ingestion(source_path_override=None, target_path_override=None, **kwargs):
    spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

    source_path = source_path_override or os.path.join(BASE_PATH, SOURCE_CSV_NAME)
    target_path = target_path_override or os.path.join(BASE_PATH, "bronze", BRONZE_TARGET_DIR)

    df = spark.read.csv(source_path, header=True, inferSchema=True)

    # Validacoes de Data Quality
    if df.isEmpty():
        spark.stop()
        raise ValueError("DQ Fail: O arquivo CSV de origem está vazio.")

    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        spark.stop()
        raise ValueError(f"DQ Fail: Colunas ausentes no schema - {missing_cols}")

    df.write.mode("overwrite").parquet(target_path)
    spark.stop()