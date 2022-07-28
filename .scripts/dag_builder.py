import json
import os
from pprint import pprint

import requests


def get_job_info(job_id: str, bearer_token):
    url = f"https://dbc-0eb40f15-5780.cloud.databricks.com/api/2.1/jobs/get?job_id={job_id}"

    headers = {
        'Authorization': f'Bearer {bearer_token}'
    }

    response = requests.request("GET", url, headers=headers)

    return json.loads(response.text)['settings']['tasks']


def get_dependencies(job_info):
    dependencies = {}
    for db_task in job_info:
        depends_on = []
        if 'depends_on' in db_task:
            for upstream in db_task['depends_on']:
                depends_on.append(upstream['task_key'])
        dependencies[db_task['task_key']] = depends_on
    return dependencies


def run():
    jobs = json.load(open('include/databricks_jobs.json'))

    job_ids = [v for k, v in jobs.items()]

    depends_on = {}
    for job_id in job_ids:
        job_info = get_job_info(job_id, os.environ['BEARER_TOKEN'])
        dependencies = get_dependencies(job_info)
        depends_on[job_id] = dependencies
    pprint(depends_on)

    with open('include/task_dependencies.json', 'w') as jobs_file:
        jobs_file.write(json.dumps(depends_on))


run()
