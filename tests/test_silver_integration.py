import importlib.util
import json
from pathlib import Path
import pytest
from pyspark.sql import SparkSession

# ==========================================
# CONFIGURAÇÕES DO TESTE
# ==========================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_DAG_PATH = PROJECT_ROOT / "dags" / "01_bronze_ingestion.py"
SILVER_DAG_PATH = PROJECT_ROOT / "dags" / "02_silver_transform.py"


def load_module(module_name, path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def create_spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("SilverIntegrationTest")
        .getOrCreate()
    )


def test_bronze_to_silver_pipeline(tmp_path):
    bronze_dag = load_module("bronze_dag", BRONZE_DAG_PATH)
    silver_dag = load_module("silver_dag", SILVER_DAG_PATH)

    csv_path = PROJECT_ROOT / "tests" / "data" / "mock_fraud_data.csv"
    bronze_path = tmp_path / "bronze" / "fraud_data"
    silver_path = tmp_path / "silver" / "fraud_data_clean"
    quarantine_path = tmp_path / "silver" / "quarantine"
    report_path = tmp_path / "silver" / "dq_report_silver.json"

    # Execução Ingestão Bronze
    bronze_dag.load_bronze(
        source_path_override=str(csv_path),
        target_path_override=str(bronze_path),
    )

    spark = create_spark()
    try:
        bronze_df = spark.read.parquet(str(bronze_path))
        assert bronze_df.count() == 5
    finally:
        spark.stop()

    # Execução Transformação Silver e DQ
    result = silver_dag.process_silver_and_dq(
        input_path_override=str(bronze_path),
        silver_path_override=str(silver_path),
        quarantine_path_override=str(quarantine_path),
        json_report_path_override=str(report_path),
    )

    spark = create_spark()
    try:
        silver_df = spark.read.parquet(str(silver_path))
        quarantine_df = spark.read.parquet(str(quarantine_path))

        assert silver_df.count() == 1
        assert quarantine_df.count() == 7
    finally:
        spark.stop()

    # Validação de Métricas e Relatório
    assert result["conformity_pct"] == 12.5

    with open(report_path, "r") as file:
        report = json.load(file)

    assert report["metrics"]["total_records"] == 8
    assert report["metrics"]["total_errors"] == 7
    assert report["metrics"]["error_rate_pct"] == 87.5
    assert report["metrics"]["conformity_rate_pct"] == 12.5

    assert report["anomalies"]["missing_values"] == 3
    assert report["anomalies"]["invalid_values"] == 4

    assert report["column_quality"]["amount"]["invalid"] == 1
    assert report["column_quality"]["amount"]["null"] == 1
    assert report["column_quality"]["risk_score"]["invalid"] == 1
    assert report["column_quality"]["risk_score"]["null"] == 1


def test_circuit_breaker_blocks_low_conformity():
    silver_dag = load_module("silver_dag", SILVER_DAG_PATH)

    class FakeTaskInstance:
        def xcom_pull(self, task_ids):
            return {"conformity_pct": 12.5}

    with pytest.raises(ValueError, match="Circuit Breaker acionado"):
        silver_dag.evaluate_circuit_breaker(FakeTaskInstance())
