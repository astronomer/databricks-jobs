import json
import logging

from airflow.decorators import dag
from airflow.exceptions import AirflowFailException
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
from astronomer.providers.http.sensors.http import HttpSensorAsync
from pendulum import datetime


def response_check(response: dict):
    """Custom check for HttpSensorAsync tasks used to monitor Databricks Job tasks

    :param response: Response from Databricks Jobs API for task run
    :type response: str

    :raises AirflowFailException: To fail task if result_state value is not SUCCESS

    :return: Determined based on existence and value of 'result_state' key in response
    :rtype: bool
    """

    logging.info(response)

    # Check if the response contains key 'result_state' and create return value accordingly
    if response:
        if ('result_state' in response['state']) and (response['state']['result_state'] == 'SUCCESS'):
            return True
        elif ('result_state' in response['state']) and (response['state']['result_state'] != 'SUCCESS'):
            raise AirflowFailException(
                f"result_state is {response['state']['result_state']} because {response['state']['state_message']}"
            )
    else:
        return False


def parse_run_info(run_info):
    """Parse the response from the 'get_run_info' task to create necessary XCOM data for downstream tasks

    :param run_info: Response from API call in task to Databricks get job run API
    :type run_info: dict

    :return: Info needed by downstream task group that mirrors databricks job
    :rtype: dict
    """

    # Get latest task attempt info - run_id, run_page_url, attempt_number
    task_attempts = {}
    for t in run_info['tasks']:
        if t['task_key'] not in task_attempts:
            task_attempts[t['task_key']] = {'run_id': t['run_id'], 'run_page_url': t['run_page_url'],
                                            'attempt_number': t['attempt_number']}
        else:
            if t['attempt_number'] > task_attempts[t['task_key']]['attempt_number']:
                task_attempts[t['task_key']] = {
                    'run_id': t['run_id'],
                    'run_page_url': t['run_page_url'],
                    'attempt_number': t['attempt_number']
                }

    latest_repair_id = None
    if "repair_history" in run_info and len(run_info['repair_history']) > 1:
        print(run_info['repair_history'])
        latest_repair_id = run_info['repair_history'][-1]['id']
    return {'tasks': task_attempts, 'latest_repair_id': latest_repair_id}


job_config = {
        "name": "airflow-job-test",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "job-task-1",
                "notebook_task": {
                    "notebook_path": "/Users/faisal@astronomer.io/job-task-1",
                    "source": "WORKSPACE"
                },
                "depends_on": [],
                "job_cluster_key": "workflow-job-cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "job-task-2",
                "depends_on": [
                    {
                        "task_key": "job-task-1"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "/Users/faisal@astronomer.io/job-task-2",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "workflow-job-cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "job-task-3",
                "depends_on": [
                    {
                        "task_key": "job-task-1"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "/Users/faisal@astronomer.io/job-task-3",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "workflow-job-cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "workflow-job-cluster",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "10.4.x-scala2.12",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "aws_attributes": {
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "zone_id": "us-east-1a",
                        "spot_bid_price_percent": 100
                    },
                    "node_type_id": "m5d.large",
                    "driver_node_type_id": "m5d.large",
                    "custom_tags": {
                        "ResourceClass": "SingleNode"
                    },
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": True,
                    "runtime_engine": "STANDARD",
                    "num_workers": 0
                }
            }
        ],
        "format": "MULTI_TASK"
}


@dag(
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example', 'databricks']
)
def databricks_job_create():
    """
    Executing and viewing your Databricks Jobs from Airflow.

    NOTE: This DAG uses the `job_config` dict defined in this file to dynamically create the databricks job and the
    task group that mirrors a databricks job. It does not dynamically create multiple DAGs that mirror multiple
    databricks jobs. Thus, it needs a job_config variable defined at the top level for the Databricks job it is meant to
    create and then mirror.
    """

    create_job = SimpleHttpOperator(
        task_id='create_job',
        method='POST',
        http_conn_id='http_default',
        endpoint='/api/2.1/jobs/create',
        data=json.dumps(job_config),
        response_filter=lambda response: response.json()
    )

    trigger_job = SimpleHttpOperator(
        task_id='trigger_job',
        method='POST',
        http_conn_id='http_default',
        endpoint='/api/2.1/jobs/run-now',
        data=json.dumps({'job_id': "{{ ti.xcom_pull(task_ids='create_job')['job_id'] }}"}),
        response_filter=lambda response: response.json()
    )

    get_run_info = SimpleHttpOperator(
        task_id='get_run_info',
        method='GET',
        http_conn_id='http_default',
        endpoint='/api/2.1/jobs/runs/get',
        data="run_id={{ ti.xcom_pull(task_ids='trigger_job')['run_id'] }}&include_history=true",
        response_filter=lambda response: parse_run_info(response.json())
    )

    with TaskGroup(group_id='Databricks_Job_Tasks') as job_taskgroup:

        # Create Airflow tasks from Databricks Tasks
        db_tasks = {}
        for task_info in job_config['tasks']:
            task_key = task_info['task_key']
            airflow_task = HttpSensorAsync(
                task_id=task_key,
                http_conn_id='http_default',
                method='GET',
                endpoint='/api/2.1/jobs/runs/get',
                request_params={
                    "run_id": f"{{{{ ti.xcom_pull(task_ids='get_run_info')['tasks']['{task_key}']['run_id'] }}}}"},
                response_check=lambda response: response_check(response.json()),
                # exponential_backoff=True
            )
            db_tasks[task_key] = airflow_task

        # Generate task dependencies
        for task_info in job_config['tasks']:
            task_key = task_info['task_key']
            if task_info['depends_on']:
                for upstream in task_info['depends_on']:
                    db_tasks[upstream['task_key']] >> db_tasks[task_key]

    create_job >> trigger_job >> get_run_info >> job_taskgroup


dag = databricks_job_create()
