import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    when,
    sum as _sum,
    array,
    concat_ws,
)
from airflow.models import Variable

# ==========================================
# PARÂMETROS E REGRAS DE NEGÓCIO CENTRALIZADAS
# ==========================================
BASE_PATH = Variable.get("DATA_LAKE_PATH", default_var="/opt/airflow/data")
CIRCUIT_BREAKER_THRESHOLD_PCT = 80.0
VALID_TRANSACTION_TYPES = ["transfer", "purchase", "sale", "phishing", "scam"]
VALID_LOCATION_REGIONS = ["Europe", "South America", "Asia", "Africa", "North America"]


def process_silver_and_dq(
    input_path_override=None,
    silver_path_override=None,
    quarantine_path_override=None,
    json_report_path_override=None,
    **kwargs,
):
    spark = SparkSession.builder.appName("SilverPipeline").getOrCreate()

    bronze_path = input_path_override or os.path.join(BASE_PATH, "bronze", "fraud_data")
    silver_path = silver_path_override or os.path.join(
        BASE_PATH, "silver", "fraud_data_clean"
    )
    quarantine_path = quarantine_path_override or os.path.join(
        BASE_PATH, "silver", "quarantine"
    )
    json_report_path = json_report_path_override or os.path.join(
        BASE_PATH, "silver", "dq_report_silver.json"
    )

    df = spark.read.parquet(bronze_path)
    total_rows = df.count()

    # Schema Enforcement
    df = (
        df.withColumn("amount", col("amount").cast("float"))
        .withColumn("risk_score", col("risk_score").cast("float"))
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
    )

    # Condições de validação
    cond_amount = col("amount").isNotNull() & (col("amount") >= 0)
    cond_risk = col("risk_score").isNotNull() & col("risk_score").between(0.0, 100.0)
    cond_time = col("timestamp").isNotNull() & (col("timestamp") <= current_timestamp())
    cond_type = col("transaction_type").isNotNull() & col("transaction_type").isin(
        VALID_TRANSACTION_TYPES
    )
    cond_region = col("location_region").isNotNull() & col("location_region").isin(
        VALID_LOCATION_REGIONS
    )

    # Roteamento (DLQ) e Mapeamento de Erros
    full_condition = cond_amount & cond_risk & cond_time & cond_type & cond_region
    df_valid = df.filter(full_condition)

    df_quarantine = df.filter(~full_condition)
    df_quarantine = df_quarantine.withColumn(
        "rejection_reason",
        concat_ws(
            " | ",
            array(
                when(col("amount").isNull(), "Amount Nulo"),
                when(
                    col("amount").isNotNull() & (col("amount") < 0), "Amount Negativo"
                ),
                when(col("risk_score").isNull(), "Risk Score Nulo"),
                when(
                    col("risk_score").isNotNull()
                    & ~col("risk_score").between(0.0, 100.0),
                    "Risk Score Invalido",
                ),
                when(col("timestamp").isNull(), "Timestamp Nulo"),
                when(
                    col("timestamp").isNotNull()
                    & (col("timestamp") > current_timestamp()),
                    "Timestamp Futuro",
                ),
                when(col("transaction_type").isNull(), "Tipo Transacao Nulo"),
                when(
                    col("transaction_type").isNotNull()
                    & ~col("transaction_type").isin(VALID_TRANSACTION_TYPES),
                    "Tipo Transacao Invalido",
                ),
                when(col("location_region").isNull(), "Regiao Nula"),
                when(
                    col("location_region").isNotNull()
                    & ~col("location_region").isin(VALID_LOCATION_REGIONS),
                    "Regiao Invalida",
                ),
            ),
        ),
    )

    valid_count = df_valid.count()
    error_count = df_quarantine.count()

    # Métricas de qualidade por coluna
    col_metrics = (
        df.select(
            _sum(when(col("amount").isNull(), 1).otherwise(0)).alias("amount_null"),
            _sum(when(col("amount").isNotNull() & ~cond_amount, 1).otherwise(0)).alias(
                "amount_invalid"
            ),
            _sum(when(col("risk_score").isNull(), 1).otherwise(0)).alias(
                "risk_score_null"
            ),
            _sum(
                when(col("risk_score").isNotNull() & ~cond_risk, 1).otherwise(0)
            ).alias("risk_score_invalid"),
            _sum(when(col("timestamp").isNull(), 1).otherwise(0)).alias(
                "timestamp_null"
            ),
            _sum(when(col("timestamp").isNotNull() & ~cond_time, 1).otherwise(0)).alias(
                "timestamp_invalid"
            ),
            _sum(when(col("transaction_type").isNull(), 1).otherwise(0)).alias(
                "type_null"
            ),
            _sum(
                when(col("transaction_type").isNotNull() & ~cond_type, 1).otherwise(0)
            ).alias("type_invalid"),
            _sum(when(col("location_region").isNull(), 1).otherwise(0)).alias(
                "region_null"
            ),
            _sum(
                when(col("location_region").isNotNull() & ~cond_region, 1).otherwise(0)
            ).alias("region_invalid"),
        )
        .collect()[0]
        .asDict()
    )

    column_quality = {}
    total_missing = 0
    total_invalid = 0

    col_map = {
        "amount": ("amount_null", "amount_invalid"),
        "risk_score": ("risk_score_null", "risk_score_invalid"),
        "timestamp": ("timestamp_null", "timestamp_invalid"),
        "transaction_type": ("type_null", "type_invalid"),
        "location_region": ("region_null", "region_invalid"),
    }

    # Consolida resultados do relatório
    for column, (null_key, invalid_key) in col_map.items():
        n_null = col_metrics[null_key]
        n_invalid = col_metrics[invalid_key]
        n_valid = total_rows - n_null - n_invalid

        total_missing += n_null
        total_invalid += n_invalid

        column_quality[column] = {
            "valid": n_valid,
            "null": n_null,
            "invalid": n_invalid,
            "completeness_pct": (
                round(((total_rows - n_null) / total_rows) * 100, 2)
                if total_rows > 0
                else 0.0
            ),
        }

    error_rate = round((error_count / total_rows) * 100, 2) if total_rows > 0 else 0.0
    conformity_rate = (
        round((valid_count / total_rows) * 100, 2) if total_rows > 0 else 0.0
    )

    dq_report = {
        "execution_date": datetime.now().isoformat(),
        "metrics": {
            "total_records": total_rows,
            "total_errors": error_count,
            "error_rate_pct": error_rate,
            "conformity_rate_pct": conformity_rate,
        },
        "anomalies": {"missing_values": total_missing, "invalid_values": total_invalid},
        "column_quality": column_quality,
    }

    # Cria diretorio de historico e salva arquivo unico
    reports_dir = os.path.dirname(json_report_path_override) if json_report_path_override else os.path.join(BASE_PATH, "silver", "dq_reports")    
    os.makedirs(reports_dir, exist_ok=True)
    
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"dq_report_{timestamp_str}.json"
    json_report_path = os.path.join(reports_dir, report_filename)

    with open(json_report_path, "w") as f:
        json.dump(dq_report, f, indent=4)

    df_valid.write.mode("overwrite").parquet(silver_path)
    df_quarantine.write.mode("overwrite").parquet(quarantine_path)

    spark.stop()
    return {"conformity_pct": conformity_rate}


def evaluate_circuit_breaker(ti=None, **kwargs):
    metrics = ti.xcom_pull(task_ids="transform_silver")

    if not metrics:
        raise ValueError(
            "Falha ao recuperar as métricas do XCom. Verifique o retorno da task transform_silver."
        )

    conformity = metrics.get("conformity_pct", 0)

    if conformity < CIRCUIT_BREAKER_THRESHOLD_PCT:
        raise ValueError(
            f"Circuit Breaker acionado: Conformidade {conformity}% (Meta: {CIRCUIT_BREAKER_THRESHOLD_PCT}%)."
        )

    print(f"Qualidade aprovada: Conformidade de {conformity}%.")
