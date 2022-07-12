import json
import logging

from airflow.models import XCom
from airflow.utils.session import provide_session
from airflow.utils.task_group import TaskGroup
from pendulum import datetime

from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator
from astronomer.providers.http.sensors.http import HttpSensorAsync
from airflow.exceptions import AirflowFailException
from sensors.DatabricksHttpSensorAsync import DatabricksHttpSensorAsync

# job_id = 552857564708371
# job_id = "387060766748255"
job_id = "557501144019716"


def response_check(response: dict):
    logging.info(response)

    if response:
        if ('result_state' in response['state']) and (response['state']['result_state'] == 'SUCCESS'):
            return True
        elif ('result_state' in response['state']) and (response['state']['result_state'] != 'SUCCESS'):
            raise AirflowFailException(
                f"result_state is {response['state']['result_state']} because {response['state']['state_message']}"
            )
    else:
        return False


@dag(
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example', 'databricks']
)
def databricks_job_operator():
    """
    Executing and viewing your Databricks Jobs from Airflow
    """

    trigger_job = SimpleHttpOperator(
        task_id='trigger_job',
        method='POST',
        http_conn_id='http_default',
        endpoint='/api/2.1/jobs/run-now',
        data=json.dumps({'job_id': job_id}),
        response_filter=lambda response: response.json(),
    )

    get_run_info = SimpleHttpOperator(
        task_id='get_run_info',
        method='GET',
        http_conn_id='http_default',
        endpoint='/api/2.1/jobs/runs/get',
        data="run_id={{ ti.xcom_pull(task_ids='trigger_job')['run_id'] }}",
        response_filter=lambda response: {t['task_key']: {'run_id': t['run_id'], 'run_page_url': t['run_page_url']} for
                                          t in response.json()['tasks']}
    )

    with TaskGroup(group_id='Databricks_Job_Tasks') as job_taskgroup:

        job_info = json.load(open('./include/jobs.json'))

        # TODO: Check for initial delay on sensor start/poke
        # Create Airflow tasks from Databricks Tasks
        db_tasks = {}
        for task_key in job_info[job_id].keys():
            airflow_task = DatabricksHttpSensorAsync(
                task_id=task_key,
                http_conn_id='http_default',
                method='GET',
                endpoint='/api/2.1/jobs/runs/get',
                request_params={
                    "run_id": f"{{{{ ti.xcom_pull(task_ids='get_run_info')['{task_key}']['run_id'] }}}}"},
                response_check=lambda response: response_check(response.json()),
                # initial_delay=15
                # exponential_backoff=True
            )
            db_tasks[task_key] = airflow_task

        # Generate task dependencies
        for task_key, dependencies in job_info[job_id].items():
            if dependencies:
                for upstream in dependencies:
                    db_tasks[upstream] >> db_tasks[task_key]

    trigger_job >> get_run_info >> job_taskgroup


dag = databricks_job_operator()

# TODO: Reruns with repair tracking/integration
