import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum as _sum,
    count as _count,
    avg as _avg,
    round as _round,
)
from airflow.models import Variable

# Parametros
BASE_PATH = Variable.get("DATA_LAKE_PATH", default_var="/opt/airflow/data")
SILVER_SOURCE = "silver/fraud_data_clean"
GOLD_REGION_TARGET = "gold/region_metrics"
GOLD_RISK_TARGET = "gold/risk_analysis"


def aggregate_region_metrics(
    silver_path_override=None, gold_path_override=None, **kwargs
):
    spark = SparkSession.builder.appName("GoldRegionMetrics").getOrCreate()
    source_path = silver_path_override or os.path.join(BASE_PATH, SILVER_SOURCE)
    target_path = gold_path_override or os.path.join(BASE_PATH, GOLD_REGION_TARGET)

    df = spark.read.parquet(source_path)

    # Agrega metricas por regiao
    df_agg = df.groupBy("location_region").agg(
        _count("*").alias("total_transactions"),
        _round(_sum("amount"), 2).alias("total_amount_transacted"),
    )

    df_agg.write.mode("overwrite").parquet(target_path)
    spark.stop()


def aggregate_risk_analysis(
    silver_path_override=None, gold_path_override=None, **kwargs
):
    spark = SparkSession.builder.appName("GoldRiskAnalysis").getOrCreate()
    source_path = silver_path_override or os.path.join(BASE_PATH, SILVER_SOURCE)
    target_path = gold_path_override or os.path.join(BASE_PATH, GOLD_RISK_TARGET)

    df = spark.read.parquet(source_path)

    # Agrega metricas por tipo de transacao
    df_agg = df.groupBy("transaction_type").agg(
        _count("*").alias("total_transactions"),
        _round(_avg("risk_score"), 2).alias("avg_risk_score"),
    )

    df_agg.write.mode("overwrite").parquet(target_path)
    spark.stop()


with DAG(
    dag_id="03_gold_aggregations",
    start_date=datetime(2026, 9, 10),
    schedule=None,
    catchup=False,
    tags=["gold", "aggregations", "data-mart"],
) as dag:

    task_region = PythonOperator(
        task_id="aggregate_region_metrics", python_callable=aggregate_region_metrics
    )

    task_risk = PythonOperator(
        task_id="aggregate_risk_analysis", python_callable=aggregate_risk_analysis
    )

    # Tasks paralelas e independentes
    [task_region, task_risk]
