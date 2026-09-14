import json
import traceback
from datetime import datetime
import os
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.bronze_ingestion import process_bronze_ingestion
from modules.silver_transform import process_silver_and_dq, evaluate_circuit_breaker
from modules.gold_aggregations import aggregate_region_risk, get_top_sales

def _clear_error_state():
    error_file = '/opt/airflow/data/system_error_state.json'
    archive_dir = '/opt/airflow/data/errors_history'
    
    if os.path.exists(error_file):
        os.makedirs(archive_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_path = os.path.join(archive_dir, f"error_{timestamp}.json")
        shutil.move(error_file, archive_path)

# Callback para registrar erros sistemicos
def on_failure_callback(context):
    exception = context.get('exception')
    task_id = context.get('task_instance').task_id
    
    error_data = {
        "timestamp": datetime.now().isoformat(),
        "task_failed": task_id,
        "error_message": str(exception),
        "traceback": traceback.format_exc() if exception else "No traceback"
    }
    
    # Salva na pasta raiz do data lake para o Streamlit ler
    with open('/opt/airflow/data/system_error_state.json', 'w') as f:
        json.dump(error_data, f)

default_args = {
    'start_date': datetime(2026, 9, 10),
    'on_failure_callback': on_failure_callback
}

with DAG(
    dag_id="pipeline_fraud_analysis",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["master", "fraud_analysis"],
) as dag:

    clear_state_task = PythonOperator(
        task_id="clear_system_error_state", python_callable=_clear_error_state
    )

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
        clear_state_task
        >> task_bronze
        >> task_silver_process
        >> task_silver_circuit_breaker
        >> [task_gold_risk, task_gold_sales]
    )
