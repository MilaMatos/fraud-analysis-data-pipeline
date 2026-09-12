import importlib.util
from pathlib import Path
from datetime import datetime
import pytest
from pyspark.sql import SparkSession

# ==========================================
# CONFIGURAÇÕES DO TESTE
# ==========================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_DAG_PATH = PROJECT_ROOT / "dags" / "modules" / "gold_aggregations.py"


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
    spark = create_spark()

    data = [
        # Massa para teste de Media de Risco (Europe = 30.0 media)
        ("Europe", "transfer", 200.00, 20.0, datetime(2026, 1, 1, 10, 0), "addr1"),
        ("Europe", "transfer", 300.00, 40.0, datetime(2026, 1, 1, 11, 0), "addr2"),
        (
            "South America",
            "purchase",
            100.50,
            15.5,
            datetime(2026, 1, 1, 12, 0),
            "addr3",
        ),
        # Massa para teste de Vendas (Top 3 recentes)
        # addr4 tem duas compras. Apenas a mais recente (500.00) deve ser considerada
        ("Asia", "sale", 150.00, 10.0, datetime(2026, 1, 1, 8, 0), "addr4"),
        ("Asia", "sale", 500.00, 15.0, datetime(2026, 1, 2, 8, 0), "addr4"),
        # addr5 (maior valor, deve ser top 1)
        ("North America", "sale", 1000.00, 10.0, datetime(2026, 1, 3, 9, 0), "addr5"),
        # addr6 (valor medio, deve entrar no top 3)
        ("Africa", "sale", 250.00, 50.0, datetime(2026, 1, 3, 10, 0), "addr6"),
        # addr7 (menor valor de sale, deve ficar de fora do top 3)
        ("Europe", "sale", 50.00, 5.0, datetime(2026, 1, 4, 11, 0), "addr7"),
    ]

    columns = [
        "location_region",
        "transaction_type",
        "amount",
        "risk_score",
        "timestamp",
        "receiving_address",
    ]

    silver_path = tmp_path / "silver" / "fraud_data_clean"
    df = spark.createDataFrame(data, columns)
    df.write.mode("overwrite").parquet(str(silver_path))

    spark.stop()
    return str(silver_path)


def test_gold_aggregations_and_business_rules(tmp_path, mock_silver_data):
    gold_module = load_module("gold_module", GOLD_DAG_PATH)

    region_path = tmp_path / "gold" / "region_risk"
    sales_path = tmp_path / "gold" / "top_sales"

    # Executa agregação 1 (Risco)
    gold_module.aggregate_region_risk(
        silver_path_override=mock_silver_data, gold_path_override=str(region_path)
    )

    # Executa agregação 2 (Vendas)
    gold_module.get_top_sales(
        silver_path_override=mock_silver_data, gold_path_override=str(sales_path)
    )

    spark = create_spark()
    try:
        df_region = spark.read.parquet(str(region_path)).toPandas()
        df_sales = spark.read.parquet(str(sales_path)).toPandas()

        # ---------------------------------------------------------
        # Validacao 1: aggregate_region_risk
        # ---------------------------------------------------------
        # Verifica se o primeiro da lista ordenada é Africa (50.0) ou Europe (Media de 20 e 40 = 30.0, mas 5.0 do addr7 reduz)
        # Calculo manual da base: Europe (20+40+5)/3 = 21.67, Africa = 50.0
        top_risk_region = df_region.iloc[0]["location_region"]
        assert top_risk_region == "Africa"  # 50.0 é a maior media

        europe_data = df_region[df_region["location_region"] == "Europe"].iloc[0]
        assert europe_data["avg_risk_score"] == 21.67

        # ---------------------------------------------------------
        # Validacao 2: get_top_sales
        # ---------------------------------------------------------
        # Garante que temos exatamente 3 registros
        assert len(df_sales) == 3

        # Garante a ordem correta (maior amount primeiro)
        assert df_sales.iloc[0]["receiving_address"] == "addr5"  # 1000.00
        assert df_sales.iloc[1]["receiving_address"] == "addr4"  # 500.00
        assert df_sales.iloc[2]["receiving_address"] == "addr6"  # 250.00

        # Garante que o addr4 contabilizou a transacao mais recente de 500, e ignorou a antiga de 150
        assert df_sales.iloc[1]["amount"] == 500.00

        # Garante que o addr7 (50.00) ficou de fora do limite de 3
        assert "addr7" not in df_sales["receiving_address"].values

    finally:
        spark.stop()
