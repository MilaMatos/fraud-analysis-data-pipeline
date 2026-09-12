import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg as _avg, round as _round, row_number
from airflow.models import Variable

BASE_PATH = Variable.get("DATA_LAKE_PATH", default_var="/opt/airflow/data")
SILVER_SOURCE = "silver/fraud_data_clean"


def aggregate_region_risk(silver_path_override=None, gold_path_override=None, **kwargs):
    spark = SparkSession.builder.appName("GoldRegionRisk").getOrCreate()
    source_path = silver_path_override or os.path.join(BASE_PATH, SILVER_SOURCE)
    target_path = gold_path_override or os.path.join(BASE_PATH, "gold/region_risk")

    df = spark.read.parquet(source_path)

    # Media de risco por regiao ordenada de forma decrescente
    df_agg = (
        df.groupBy("location_region")
        .agg(_round(_avg("risk_score"), 2).alias("avg_risk_score"))
        .orderBy(col("avg_risk_score").desc())
    )

    df_agg.write.mode("overwrite").parquet(target_path)
    spark.stop()


def get_top_sales(silver_path_override=None, gold_path_override=None, **kwargs):
    spark = SparkSession.builder.appName("GoldTopSales").getOrCreate()
    source_path = silver_path_override or os.path.join(BASE_PATH, SILVER_SOURCE)
    target_path = gold_path_override or os.path.join(BASE_PATH, "gold/top_sales")

    df = spark.read.parquet(source_path)

    # 1. Filtra apenas transacoes do tipo sale
    df_sales = df.filter(col("transaction_type") == "sale")

    # 2. Janela para pegar apenas a transacao mais recente por receiving_address
    window_spec = Window.partitionBy("receiving_address").orderBy(
        col("timestamp").desc()
    )

    df_recent = df_sales.withColumn("rn", row_number().over(window_spec)).filter(
        col("rn") == 1
    )

    # 3. Pega os 3 maiores amounts dessa selecao
    df_top3 = (
        df_recent.orderBy(col("amount").desc())
        .limit(3)
        .select("receiving_address", "amount", "timestamp")
    )

    df_top3.write.mode("overwrite").parquet(target_path)
    spark.stop()
