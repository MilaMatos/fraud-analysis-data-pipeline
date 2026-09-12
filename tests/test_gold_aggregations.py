import importlib.util
from pathlib import Path
import pytest
from pyspark.sql import SparkSession

# ==========================================
# CONFIGURAÇÕES DO TESTE
# ==========================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_DAG_PATH = PROJECT_ROOT / "dags" / "03_gold_aggregations.py"


def load_module(module_name, path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def create_spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("GoldIntegrationTest")
        .getOrCreate()
    )


@pytest.fixture
def mock_silver_data(tmp_path):
    # Cria um dataframe mockado simulando a saída limpa da Silver
    spark = create_spark()
    data = [
        ("South America", "purchase", 100.50, 15.5),
        ("Europe", "transfer", 200.00, 20.0),
        ("Europe", "transfer", 300.00, 40.0),
        ("Asia", "sale", 150.00, 10.0),
    ]
    columns = ["location_region", "transaction_type", "amount", "risk_score"]

    silver_path = tmp_path / "silver" / "fraud_data_clean"
    df = spark.createDataFrame(data, columns)
    df.write.mode("overwrite").parquet(str(silver_path))

    spark.stop()
    return str(silver_path)


def test_gold_aggregations_and_dq(tmp_path, mock_silver_data):
    gold_dag = load_module("gold_dag", GOLD_DAG_PATH)

    region_path = tmp_path / "gold" / "region_metrics"
    risk_path = tmp_path / "gold" / "risk_analysis"

    # Executa agregação 1
    gold_dag.aggregate_region_metrics(
        silver_path_override=mock_silver_data, gold_path_override=str(region_path)
    )

    # Executa agregação 2
    gold_dag.aggregate_risk_analysis(
        silver_path_override=mock_silver_data, gold_path_override=str(risk_path)
    )

    spark = create_spark()
    try:
        df_region = spark.read.parquet(str(region_path))
        df_risk = spark.read.parquet(str(risk_path))

        # Validação Matemática: Region (Europe tem 2 transações, totalizando 500)
        europe_data = df_region.filter(df_region.location_region == "Europe").collect()[
            0
        ]
        assert europe_data["total_transactions"] == 2
        assert europe_data["total_amount_transacted"] == 500.0

        # Validação Matemática: Risk (Transfer tem 2 transações, média de risco 30.0)
        transfer_data = df_risk.filter(
            df_risk.transaction_type == "transfer"
        ).collect()[0]
        assert transfer_data["total_transactions"] == 2
        assert transfer_data["avg_risk_score"] == 30.0

    finally:
        spark.stop()
