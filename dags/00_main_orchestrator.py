from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="00_main_orchestrator",
    start_date=datetime(2026, 9, 10),
    schedule=None,
    catchup=False,
    tags=["orchestration", "master", "pipeline"],
) as dag:

    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze",
        trigger_dag_id="01_bronze_ingestion",
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="02_silver_transform",
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="03_gold_aggregations",
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_bronze >> trigger_silver >> trigger_gold
