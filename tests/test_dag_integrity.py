import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(dag_folder="dags/", include_examples=False)


def test_expected_dags_exist(dag_bag):
    assert not dag_bag.import_errors
    assert "pipeline_fraud_analysis" in dag_bag.dags


def test_main_pipeline_structure(dag_bag):
    dag = dag_bag.dags["pipeline_fraud_analysis"]

    expected_tasks = [
        "ingest_bronze",
        "transform_silver",
        "check_circuit_breaker",
        "calc_region_risk",
        "calc_top_sales",
    ]

    for task in expected_tasks:
        assert task in dag.task_ids

    # Valida encadeamento correto (Silver ramificando para as duas da Gold)
    assert dag.task_dict["check_circuit_breaker"].downstream_task_ids == {"calc_region_risk", "calc_top_sales"}