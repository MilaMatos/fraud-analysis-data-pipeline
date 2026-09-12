from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from modules.bronze_ingestion import process_bronze_ingestion
from modules.silver_transform import process_silver_and_dq, evaluate_circuit_breaker
from modules.gold_aggregations import aggregate_region_risk, get_top_sales

with DAG(
    dag_id="pipeline_fraud_analysis",
    start_date=datetime(2026, 9, 10),
    schedule=None,
    catchup=False,
    tags=["master", "fraud_analysis"],
) as dag:

    # --- TAREFAS DA BRONZE ---
    task_bronze = PythonOperator(
        task_id="ingest_bronze", python_callable=process_bronze_ingestion
    )

    # --- TAREFAS DA SILVER ---
    task_silver_process = PythonOperator(
        task_id="transform_silver", python_callable=process_silver_and_dq
    )

    task_silver_circuit_breaker = PythonOperator(
        task_id="check_circuit_breaker", python_callable=evaluate_circuit_breaker
    )

    # --- TAREFAS DA GOLD ---
    task_gold_risk = PythonOperator(
        task_id="calc_region_risk", python_callable=aggregate_region_risk
    )

    task_gold_sales = PythonOperator(
        task_id="calc_top_sales", python_callable=get_top_sales
    )

    # --- ORQUESTRAÇÃO VISUAL ---
    (
        task_bronze
        >> task_silver_process
        >> task_silver_circuit_breaker
        >> [task_gold_risk, task_gold_sales]
    )
