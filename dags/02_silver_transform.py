import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when, sum as _sum
from airflow.models import Variable

BASE_PATH = Variable.get("DATA_LAKE_PATH", default_var="/opt/airflow/data")


def process_silver_and_dq(**kwargs):
    spark = SparkSession.builder.appName("SilverPipeline").getOrCreate()

    # Caminhos
    bronze_path = os.path.join(BASE_PATH, "bronze", "fraud_data")
    silver_path = os.path.join(BASE_PATH, "silver", "fraud_data_clean")
    quarantine_path = os.path.join(BASE_PATH, "silver", "quarantine")
    json_report_path = os.path.join(BASE_PATH, "silver", "dq_report_silver.json")

    df = spark.read.parquet(bronze_path)
    total_rows = df.count()

    # Schema Enforcement
    df = (
        df.withColumn("amount", col("amount").cast("float"))
        .withColumn("risk_score", col("risk_score").cast("float"))
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
    )

    # Regras de Negocio
    valid_types = ["transfer", "purchase", "sale", "phishing", "scam"]
    valid_regions = ["Europe", "South America", "Asia", "Africa", "North America"]

    cond_amount = col("amount") >= 0
    cond_risk = col("risk_score").between(0.0, 100.0)
    cond_time = col("timestamp") <= current_timestamp()
    cond_type = col("transaction_type").isin(valid_types)
    cond_region = col("location_region").isin(valid_regions)

    # Roteamento (DLQ)
    full_condition = cond_amount & cond_risk & cond_time & cond_type & cond_region
    df_valid = df.filter(full_condition)
    df_quarantine = df.filter(~full_condition)

    valid_count = df_valid.count()
    error_count = df_quarantine.count()

    # Valores Faltantes (Completude individual)
    null_exprs = [
        _sum(
            when(
                col(c).isNull() | (col(c).cast("string").rlike("(?i)^none$|nan")), 1
            ).otherwise(0)
        ).alias(c)
        for c in df.columns
    ]
    null_counts = df.agg(*null_exprs).collect()[0].asDict()
    total_missing = sum(null_counts.values())

    # Valores Inconsistentes e Incorretos
    anomaly_metrics = (
        df.select(
            _sum(when(~cond_amount | ~cond_risk | ~cond_time, 1).otherwise(0)).alias(
                "inconsistent_values"
            ),
            _sum(when(~cond_type | ~cond_region, 1).otherwise(0)).alias(
                "incorrect_values"
            ),
        )
        .collect()[0]
        .asDict()
    )

    completeness = {}
    for c, null_qtd in null_counts.items():
        completeness[c] = {
            "null_count": null_qtd,
            "completeness_pct": (
                round(((total_rows - null_qtd) / total_rows) * 100, 2)
                if total_rows > 0
                else 0.0
            ),
        }

    # Metricas
    error_rate = round((error_count / total_rows) * 100, 2) if total_rows > 0 else 0.0
    conformity_rate = (
        round((valid_count / total_rows) * 100, 2) if total_rows > 0 else 0.0
    )

    # Estrutura JSON
    dq_report = {
        "execution_date": datetime.now().isoformat(),
        "metrics": {
            "total_records": total_rows,
            "total_errors": error_count,
            "error_rate_pct": error_rate,
            "conformity_rate_pct": conformity_rate,
        },
        "anomalies": {
            "missing_values": total_missing,
            "inconsistent_values": anomaly_metrics["inconsistent_values"],
            "incorrect_values": anomaly_metrics["incorrect_values"],
        },
        "column_completeness": completeness,
    }

    # Grava Arquivos
    os.makedirs(os.path.dirname(json_report_path), exist_ok=True)
    with open(json_report_path, "w") as f:
        json.dump(dq_report, f, indent=4)

    df_valid.write.mode("overwrite").parquet(silver_path)
    df_quarantine.write.mode("overwrite").parquet(quarantine_path)

    spark.stop()
    return {"conformity_pct": conformity_rate}


def evaluate_circuit_breaker(ti):
    # Recupera metricas e avalia qualidade
    metrics = ti.xcom_pull(task_ids="process_silver_and_dq")
    conformity = metrics.get("conformity_pct", 0)

    if conformity < 80.0:
        raise ValueError(
            f"Circuit Breaker acionado: Conformidade {conformity}% (Meta: 80%)."
        )

    print(f"Qualidade aprovada: Conformidade de {conformity}%.")


with DAG(
    dag_id="02_silver_transform",
    start_date=datetime(2026, 9, 10),
    schedule_interval=None,
    catchup=False,
    tags=["silver", "transformation", "data-quality"],
) as dag:

    process_task = PythonOperator(
        task_id="process_silver_and_dq", python_callable=process_silver_and_dq
    )

    circuit_breaker_task = PythonOperator(
        task_id="evaluate_circuit_breaker", python_callable=evaluate_circuit_breaker
    )

    process_task >> circuit_breaker_task
