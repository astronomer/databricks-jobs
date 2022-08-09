import json
import logging

from airflow.decorators import dag
from airflow.exceptions import AirflowFailException
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
from astronomer.providers.http.sensors.http import HttpSensorAsync
from pendulum import datetime

# job_id = "552857564708371"
# job_id = "387060766748255"
job_id = "557501144019716"


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


@dag(
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example', 'databricks']
)
def databricks_job_repair():
    """
    Executing and viewing your Databricks Jobs from Airflow

    NOTE: This DAG uses the task_dependencies.json to dynamically create the task group that mirrors a databricks
    job. It does not dynamically create multiple DAGs that mirror multiple databricks jobs. Thus, it needs a job_id
    variable defined at the top level for the Databricks job it is meant to mirror.
    """

    trigger_job = SimpleHttpOperator(
        task_id='trigger_job',
        method='POST',
        http_conn_id='http_default',
        endpoint='/api/2.1/jobs/run-now',
        data=json.dumps({'job_id': job_id}),
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

        task_dependencies = json.load(open('./include/task_dependencies.json'))

        # Create Airflow tasks from Databricks Tasks
        db_tasks = {}
        for task_key in task_dependencies[job_id].keys():
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
        for task_key, dependencies in task_dependencies[job_id].items():
            if dependencies:
                for upstream in dependencies:
                    db_tasks[upstream] >> db_tasks[task_key]

    trigger_job >> get_run_info >> job_taskgroup


dag = databricks_job_repair()
