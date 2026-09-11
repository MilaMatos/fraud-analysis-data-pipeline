import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(
        dag_folder="dags/",
        include_examples=False,
    )


def test_dags_load_without_errors(dag_bag):
    assert not dag_bag.import_errors, f"Erros ao importar DAGs: {dag_bag.import_errors}"


def test_expected_dags_exist(dag_bag):
    assert "01_bronze_ingestion" in dag_bag.dags
    assert "02_silver_transform" in dag_bag.dags


def test_bronze_dag_structure(dag_bag):
    dag = dag_bag.dags["01_bronze_ingestion"]

    assert "load_csv_to_bronze" in dag.task_ids
    assert "dq_check_bronze" in dag.task_ids

    assert dag.task_dict["load_csv_to_bronze"].downstream_task_ids == {
        "dq_check_bronze"
    }


def test_silver_dag_structure(dag_bag):
    dag = dag_bag.dags["02_silver_transform"]

    assert "process_silver_and_dq" in dag.task_ids
    assert "evaluate_circuit_breaker" in dag.task_ids

    assert dag.task_dict["process_silver_and_dq"].downstream_task_ids == {
        "evaluate_circuit_breaker"
    }
