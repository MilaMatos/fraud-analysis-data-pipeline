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
    min as _min,
    max as _max,
    avg as _avg
)
from airflow.models import Variable

BASE_PATH = Variable.get("DATA_LAKE_PATH", default_var="/opt/airflow/data")
CIRCUIT_BREAKER_THRESHOLD_PCT = 83

# Domínios validados
VALID_TRANSACTION_TYPES = ["transfer", "purchase", "sale", "phishing", "scam"]
VALID_LOCATION_REGIONS = ["Europe", "South America", "Asia", "Africa", "North America"]
VALID_ANOMALY = ["low_risk", "moderate_risk", "high_risk"]
VALID_AGE_GROUP = ["established", "veteran", "new"]
VALID_PURCHASE_PATTERN = ["focused", "high_value", "random"]


def _cast_columns(df):
    df = (
        df.withColumn("amount", col("amount").cast("float"))
        .withColumn("risk_score", col("risk_score").cast("float"))
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
    )
    
    if "ip_prefix" in df.columns:
        df = df.withColumn("ip_prefix", col("ip_prefix").cast("string"))
    if "login_frequency" in df.columns:
        df = df.withColumn("login_frequency", col("login_frequency").cast("int"))
    if "session_duration" in df.columns:
        df = df.withColumn("session_duration", col("session_duration").cast("int"))
        
    return df


def _get_validation_rules(columns):
    # Colunas estritamente obrigatorias
    condicoes = {
        "amount": col("amount").isNotNull() & (col("amount") >= 0),
        "risk_score": col("risk_score").isNotNull() & col("risk_score").between(0.0, 100.0),
        "timestamp": col("timestamp").isNotNull() & (col("timestamp") <= current_timestamp()),
        "transaction_type": col("transaction_type").isNotNull() & col("transaction_type").isin(VALID_TRANSACTION_TYPES),
        "location_region": col("location_region").isNotNull() & col("location_region").isin(VALID_LOCATION_REGIONS),
        "receiving_address": col("receiving_address").isNotNull() & col("receiving_address").rlike(r"^0x[a-fA-F0-9]+$")
    }

    reasons = [
        when(~condicoes["amount"], "Erro de Amount"),
        when(~condicoes["risk_score"], "Erro de Risk Score"),
        when(~condicoes["timestamp"], "Erro de Timestamp"),
        when(~condicoes["transaction_type"], "Erro de Transaction Type"),
        when(~condicoes["location_region"], "Erro de Regiao"),
        when(~condicoes["receiving_address"], "Erro de Receiving Address")
    ]

    opt_cat_cols = []
    
    # Schema Evolution para colunas opcionais
    if "anomaly" in columns:
        condicoes["anomaly"] = col("anomaly").isNotNull() & col("anomaly").isin(VALID_ANOMALY)
        reasons.append(when(~condicoes["anomaly"], "Erro de Anomaly"))
        opt_cat_cols.append("anomaly")

    if "age_group" in columns:
        condicoes["age_group"] = col("age_group").isNotNull() & col("age_group").isin(VALID_AGE_GROUP)
        reasons.append(when(~condicoes["age_group"], "Erro de Age Group"))
        opt_cat_cols.append("age_group")

    if "purchase_pattern" in columns:
        condicoes["purchase_pattern"] = col("purchase_pattern").isNotNull() & col("purchase_pattern").isin(VALID_PURCHASE_PATTERN)
        reasons.append(when(~condicoes["purchase_pattern"], "Erro de Purchase Pattern"))
        opt_cat_cols.append("purchase_pattern")

    if "ip_prefix" in columns:
        condicoes["ip_prefix"] = col("ip_prefix").isNotNull() & col("ip_prefix").rlike(r"^\d{1,3}\.\d{1,3}$")
        reasons.append(when(~condicoes["ip_prefix"], "Erro de IP Prefix"))

    if "login_frequency" in columns:
        condicoes["login_frequency"] = col("login_frequency").isNotNull() & (col("login_frequency") > 0)
        reasons.append(when(~condicoes["login_frequency"], "Erro de Login Frequency"))

    if "session_duration" in columns:
        condicoes["session_duration"] = col("session_duration").isNotNull() & (col("session_duration") > 0)
        reasons.append(when(~condicoes["session_duration"], "Erro de Session Duration"))

    if "sending_address" in columns:
        condicoes["sending_address"] = col("sending_address").isNull() | col("sending_address").rlike(r"^0x[a-fA-F0-9]+$")
        reasons.append(when(~condicoes["sending_address"], "Erro de Sending Address (Possivel Vazamento PII)"))

    return condicoes, reasons, opt_cat_cols


def _compute_metrics(df, condicoes, total_rows, valid_count, error_count):
    agg_exprs = []
    for c, cond in condicoes.items():
        agg_exprs.append(_sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"{c}_null"))
        agg_exprs.append(_sum(when(col(c).isNotNull() & ~cond, 1).otherwise(0)).alias(f"{c}_invalid"))

    col_metrics = df.select(*agg_exprs).collect()[0].asDict()

    column_quality = {}
    total_missing = total_invalid = 0

    for c in condicoes.keys():
        n_null = col_metrics[f"{c}_null"]
        n_invalid = col_metrics[f"{c}_invalid"]
        n_valid = total_rows - n_null - n_invalid
        
        total_missing += n_null
        total_invalid += n_invalid
        
        column_quality[c] = {
            "valid": n_valid,
            "null": n_null,
            "invalid": n_invalid,
            "completeness_pct": round(((total_rows - n_null) / total_rows) * 100, 2) if total_rows > 0 else 0.0,
            "validity_pct": round((n_valid / total_rows) * 100, 2) if total_rows > 0 else 0.0
        }

    return {
        "metrics": {
            "total_records": total_rows,
            "total_errors": error_count,
            "error_rate_pct": round((error_count / total_rows) * 100, 2) if total_rows > 0 else 0.0,
            "conformity_rate_pct": round((valid_count / total_rows) * 100, 2) if total_rows > 0 else 0.0,
            "circuit_breaker_threshold_pct": CIRCUIT_BREAKER_THRESHOLD_PCT
        },
        "anomalies": {"missing_values": total_missing, "invalid_values": total_invalid},
        "column_quality": column_quality
    }


def _extract_eda(df, opt_cat_cols):
    eda_data = {"categorical": {}, "numeric": {}}
    
    cat_cols = ["transaction_type", "location_region"] + opt_cat_cols
    for c in cat_cols:
        unique_rows = df.select(c).distinct().collect()
        eda_data["categorical"][c] = [str(r[0]) for r in unique_rows if r[0] is not None]
        
    num_stats = df.select(
        _min("amount").alias("amt_min"), _max("amount").alias("amt_max"), _avg("amount").alias("amt_avg"),
        _min("risk_score").alias("rsk_min"), _max("risk_score").alias("rsk_max"), _avg("risk_score").alias("rsk_avg")
    ).collect()[0]
    
    eda_data["numeric"] = {
        "amount": {"min": num_stats["amt_min"], "max": num_stats["amt_max"], "avg": round(num_stats["amt_avg"], 2) if num_stats["amt_avg"] else 0},
        "risk_score": {"min": num_stats["rsk_min"], "max": num_stats["rsk_max"], "avg": round(num_stats["rsk_avg"], 2) if num_stats["rsk_avg"] else 0}
    }
    return eda_data


def _save_report(report_data, override_path):
    reports_dir = os.path.dirname(override_path) if override_path else os.path.join(BASE_PATH, "silver", "dq_reports")
    os.makedirs(reports_dir, exist_ok=True)
    
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_report_path = os.path.join(reports_dir, f"dq_report_{timestamp_str}.json")

    with open(json_report_path, "w") as f:
        json.dump(report_data, f, indent=4)


# Orquestrador da camada Silver
def process_silver_and_dq(input_path_override=None, silver_path_override=None, quarantine_path_override=None, json_report_path_override=None, **kwargs):
    spark = SparkSession.builder.appName("SilverPipeline").getOrCreate()

    bronze_path = input_path_override or os.path.join(BASE_PATH, "bronze", "fraud_data")
    silver_path = silver_path_override or os.path.join(BASE_PATH, "silver", "fraud_data_clean")
    quarantine_path = quarantine_path_override or os.path.join(BASE_PATH, "silver", "quarantine")

    df = spark.read.parquet(bronze_path)
    total_rows = df.count()

    df = _cast_columns(df)
    condicoes, reasons, opt_cat_cols = _get_validation_rules(df.columns)

    # Constroi filtro unificado
    full_condition = None
    for cond in condicoes.values():
        full_condition = cond if full_condition is None else full_condition & cond

    df_valid = df.filter(full_condition)
    df_quarantine = df.filter(~full_condition).withColumn("rejection_reason", concat_ws(" | ", array(*reasons)))

    valid_count = df_valid.count()
    error_count = df_quarantine.count()

    metrics_data = _compute_metrics(df, condicoes, total_rows, valid_count, error_count)
    eda_data = _extract_eda(df, opt_cat_cols)

    dq_report = {
        "execution_date": datetime.now().isoformat(),
        **metrics_data,
        "eda": eda_data
    }

    _save_report(dq_report, json_report_path_override)

    df_valid.write.mode("overwrite").parquet(silver_path)
    df_quarantine.write.mode("overwrite").parquet(quarantine_path)

    spark.stop()
    return {"conformity_pct": metrics_data["metrics"]["conformity_rate_pct"]}


def evaluate_circuit_breaker(ti=None, **kwargs):
    metrics = ti.xcom_pull(task_ids="transform_silver")
    
    if not metrics:
        raise ValueError("Falha ao recuperar as métricas do XCom.")

    conformity = metrics.get("conformity_pct", 0)

    if conformity < CIRCUIT_BREAKER_THRESHOLD_PCT:
        raise ValueError(f"Circuit Breaker acionado: Conformidade {conformity}%.")

    print(f"Qualidade aprovada: Conformidade de {conformity}%.")