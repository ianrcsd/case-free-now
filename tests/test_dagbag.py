from airflow.models import DagBag


def test_dagbag():
    dag_bag = DagBag(include_examples=False)  # Loads all DAGs in $AIRFLOW_HOME/dags
    assert (
        not dag_bag.import_errors
    )  # Import errors aren't raised but captured to ensure all DAGs are parsed
