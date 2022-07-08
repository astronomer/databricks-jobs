"""Test the validity of all DAGs. This test ensures that all Dags have tags, retries set to two, and no import errors. Feel free to add and remove tests."""

from airflow.models import DagBag


def test_dagbag():
    """
    Validate DAG files using Airflow's DagBag.
    This includes sanity checks e.g. do tasks have required arguments, are DAG ids unique & do DAGs have no cycles.
    """
    dag_bag = DagBag(include_examples=False)
    print(dag_bag)
    assert not dag_bag.import_errors  # Import errors aren't raised but captured to ensure all DAGs are parsed


# Additional project-specific checks can be added here, e.g. to enforce each DAG has a tag:
def test_dag_tags():
    dag_bag = DagBag(include_examples=False)
    exceptions = []
    for dag_id, dag in dag_bag.dags.items():
        error_msg = f"{dag_id} in {dag.fileloc} has no tags"
        try:
            assert dag.tags, error_msg
        except Exception as e:
            exceptions.append(str(e))

    # Create and raise one exception message containing all exceptions for this test
    if exceptions:
        exception_msg = "\n".join(exceptions)
        raise Exception(exception_msg)

# Example test to enforce setting retries on all DAGs
def test_retries_present():
    dag_bag = DagBag(include_examples=False)
    print(dag_bag)
    exceptions = []
    for dag_id, dag in dag_bag.dags.items():
        retries = dag_bag.dags[dag_id].default_args.get('retries', [])
        error_msg = f"{dag_id} in {dag.fileloc} does not have retries not set to 2."
        try:
            assert retries == 2, error_msg
        except Exception as e:
            exceptions.append(str(e))
    # Create and raise one exception message containing all exceptions for this test
    if exceptions:
        exception_msg = "\n".join(exceptions)
        raise Exception(exception_msg)