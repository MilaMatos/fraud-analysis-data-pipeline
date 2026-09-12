import importlib.util
from pathlib import Path

from pyspark.sql import SparkSession

# ==========================================
# CONFIGURAÇÕES DO TESTE
# ==========================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = PROJECT_ROOT / "dags" / "modules" / "bronze_ingestion.py"


def load_dag_module():
    spec = importlib.util.spec_from_file_location(
        "bronze_dag",
        DAG_PATH,
    )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def create_spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("BronzeIntegrationTest")
        .getOrCreate()
    )


def test_bronze_ingestion(tmp_path):
    bronze_dag = load_dag_module()

    csv_path = PROJECT_ROOT / "tests" / "data" / "mock_fraud_data.csv"

    bronze_path = tmp_path / "bronze" / "fraud_data"

    bronze_dag.process_bronze_ingestion(
        source_path_override=str(csv_path),
        target_path_override=str(bronze_path),
    )

    spark = create_spark()

    try:
        df = spark.read.parquet(str(bronze_path))

        assert df.count() == 8

        expected_columns = {
            "timestamp",
            "receiving_address",
            "amount",
            "transaction_type",
            "location_region",
            "risk_score",
        }

        assert set(df.columns) == expected_columns

    finally:
        spark.stop()
