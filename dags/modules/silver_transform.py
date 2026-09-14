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
CIRCUIT_BREAKER_THRESHOLD_PCT = 80.0

# Domínios validados
VALID_TRANSACTION_TYPES = ["transfer", "purchase", "sale", "phishing", "scam"]
VALID_LOCATION_REGIONS = ["Europe", "South America", "Asia", "Africa", "North America"]
VALID_ANOMALY = ["low_risk", "moderate_risk", "high_risk"]
VALID_AGE_GROUP = ["established", "veteran", "new"]
VALID_PURCHASE_PATTERN = ["focused", "high_value", "random"]

ALL_EXPECTED_COLUMNS = [
    "timestamp", "sending_address", "receiving_address", "amount", 
    "transaction_type", "location_region", "ip_prefix", 
    "login_frequency", "session_duration", "purchase_pattern", 
    "age_group", "risk_score", "anomaly"
]


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
    # HARD RULES (Bloqueio - Mandam a linha para Quarentena)
    hard_conds = {
        "amount": col("amount").isNotNull() & (col("amount") >= 0),
        "risk_score": col("risk_score").isNotNull() & col("risk_score").between(0.0, 100.0),
        "timestamp": col("timestamp").isNotNull() & (col("timestamp") <= current_timestamp()),
        "transaction_type": col("transaction_type").isNotNull() & col("transaction_type").isin(VALID_TRANSACTION_TYPES),
        "location_region": col("location_region").isNotNull() & col("location_region").isin(VALID_LOCATION_REGIONS),
        "receiving_address": col("receiving_address").isNotNull() & col("receiving_address").rlike(r"^0x[a-fA-F0-9]+$")
    }

    hard_reasons = [
        when(~hard_conds["amount"], "Erro de Amount"),
        when(~hard_conds["risk_score"], "Erro de Risk Score"),
        when(~hard_conds["timestamp"], "Erro de Timestamp"),
        when(~hard_conds["transaction_type"], "Erro de Transaction Type"),
        when(~hard_conds["location_region"], "Erro de Regiao"),
        when(~hard_conds["receiving_address"], "Erro de Receiving Address")
    ]

    # SOFT RULES (Alertas - Não bloqueiam, mas são reportadas)
    soft_conds = {}
    opt_cat_cols = []
    
    if "anomaly" in columns:
        soft_conds["anomaly"] = col("anomaly").isin(VALID_ANOMALY)
        opt_cat_cols.append("anomaly")

    if "age_group" in columns:
        soft_conds["age_group"] = col("age_group").isin(VALID_AGE_GROUP)
        opt_cat_cols.append("age_group")

    if "purchase_pattern" in columns:
        soft_conds["purchase_pattern"] = col("purchase_pattern").isin(VALID_PURCHASE_PATTERN)
        opt_cat_cols.append("purchase_pattern")

    if "ip_prefix" in columns:
        soft_conds["ip_prefix"] = col("ip_prefix").rlike(r"^\d{1,3}\.\d{1,3}$")

    if "login_frequency" in columns:
        soft_conds["login_frequency"] = (col("login_frequency") > 0)

    if "session_duration" in columns:
        soft_conds["session_duration"] = (col("session_duration") > 0)

    if "sending_address" in columns:
        soft_conds["sending_address"] = col("sending_address").rlike(r"^0x[a-fA-F0-9]+$")

    return hard_conds, hard_reasons, soft_conds, opt_cat_cols


def _compute_metrics(df, hard_conds, soft_conds, total_rows, valid_count, error_count, duplicate_count, unmapped_columns):
    agg_exprs = []
    all_conds = {**hard_conds, **soft_conds}
    
    for c, cond in all_conds.items():
        agg_exprs.append(_sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"{c}_null"))
        agg_exprs.append(_sum(when(col(c).isNotNull() & ~cond, 1).otherwise(0)).alias(f"{c}_invalid"))

    col_metrics = df.select(*agg_exprs).collect()[0].asDict()

    column_quality = {}
    total_missing = total_invalid = total_alerts = 0

    # Iteração separando as métricas Hard das Soft para o contador de Alertas
    for c, is_hard in [(k, True) for k in hard_conds.keys()] + [(k, False) for k in soft_conds.keys()]:
        n_null = col_metrics[f"{c}_null"]
        n_invalid = col_metrics[f"{c}_invalid"]
        n_valid = total_rows - n_null - n_invalid
        
        total_missing += n_null
        total_invalid += n_invalid
        
        if not is_hard:
            total_alerts += n_invalid
            
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
            "total_alerts": total_alerts,
            "duplicate_records": duplicate_count,
            "unmapped_columns": unmapped_columns,
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

    df_raw = spark.read.parquet(bronze_path)
    total_incoming = df_raw.count()
    
    # 1. Desduplicação Exata
    df = df_raw.dropDuplicates()
    total_rows = df.count()
    duplicate_count = total_incoming - total_rows
    
    # 2. Detecção de Schema Drift (Colunas novas não catalogadas)
    unmapped_columns = [c for c in df.columns if c not in ALL_EXPECTED_COLUMNS]

    df = _cast_columns(df)
    hard_conds, hard_reasons, soft_conds, opt_cat_cols = _get_validation_rules(df.columns)

    # Constrói o filtro estrito APENAS para colunas obrigatórias
    full_condition = None
    for cond in hard_conds.values():
        full_condition = cond if full_condition is None else full_condition & cond

    df_valid = df.filter(full_condition)
    df_quarantine = df.filter(~full_condition).withColumn("rejection_reason", concat_ws(" | ", array(*hard_reasons)))

    valid_count = df_valid.count()
    error_count = df_quarantine.count()

    metrics_data = _compute_metrics(df, hard_conds, soft_conds, total_rows, valid_count, error_count, duplicate_count, unmapped_columns)
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