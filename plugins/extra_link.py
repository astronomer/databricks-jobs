import json
from urllib.parse import quote, unquote
import requests

from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperatorLink
from airflow.models.xcom import XCom
from airflow.utils.session import provide_session
from airflow.utils.state import State

from astronomer.providers.http.sensors.http import HttpSensorAsync
from sensors.DatabricksHttpSensorAsync import DatabricksHttpSensorAsync

from flask import Blueprint, request, redirect, url_for
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook


class DatabricksRunLink(BaseOperatorLink):
    """
    Link to a specific Databricks task. Is generated with run_page_url info from get_run_info task's XCOM push.
    """

    name = "Databricks Task"

    operators = [HttpSensorAsync, DatabricksHttpSensorAsync]

    def get_link(self, operator, *, ti_key=None):

        # Get run_page_url from XCOM of get_run_info task.
        task_id = ti_key.task_id.split('.')[-1]
        run_page_url = XCom.get_one(
            dag_id=ti_key.dag_id, task_id='get_run_info', run_id=ti_key.run_id
        )['tasks'][task_id]['run_page_url']

        return run_page_url


class DatabricksRepairRunLink(BaseOperatorLink):
    """
    Link to custom DatabricksRepair view. Is populated based on Dag Run and task instance information
    """

    name = "Repair Run"

    operators = [HttpSensorAsync, DatabricksHttpSensorAsync]

    @provide_session
    def get_link(self, operator, *, ti_key, session=None):

        # Get task instance for failed or skipped tasks in the current dag then check if it's failed or not and enable
        # link accordingly.
        task_instance = session.query(TaskInstance).filter(
            TaskInstance.task_id == ti_key.task_id,
            TaskInstance.run_id == ti_key.run_id,
            TaskInstance.dag_id == ti_key.dag_id
        ).one()

        if task_instance.state != State.FAILED:
            return ''


        databricks_run_id = XCom.get_one(
            dag_id=ti_key.dag_id, task_id='trigger_job', run_id=ti_key.run_id
        )['run_id']

        latest_repair_id = XCom.get_one(
            dag_id=ti_key.dag_id, task_id='get_run_info', run_id=ti_key.run_id
        )['latest_repair_id']

        # If the task is failed then generate link accordingly
        return "/databricksrun/repair?dag_id={dag_id}&task_id={task_id}&run_id={run_id}&databricks_run_id={databricks_run_id}&latest_repair_id={latest_repair_id}".format(
            dag_id=ti_key.dag_id,
            task_id=ti_key.task_id,
            run_id=quote(ti_key.run_id),
            databricks_run_id=databricks_run_id,
            latest_repair_id=latest_repair_id
        )


# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "test_plugin",
    __name__,
    template_folder="templates",  # registers airflow/plugins/templates as a Jinja template folder
    static_folder="static",
    static_url_path="/static/test_plugin",
)


# Creating a flask appbuilder BaseView
class DatabricksRun(AppBuilderBaseView):
    default_view = "repair"

    @provide_session
    @expose('/repair', methods=['GET', 'POST'])
    def repair(self, session=None):
        args = request.args
        dag_id = args.get('dag_id')
        run_id = args.get('run_id')
        task_id = args.get('task_id')
        databricks_run_id = args.get('databricks_run_id')
        latest_repair_id = args.get('latest_repair_id')

        print(type(latest_repair_id))

        # Get task instance for task that was source of this request
        task_instance = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.task_id == task_id,
            TaskInstance.run_id == unquote(run_id)
        ).one()

        print(task_instance)

        # Get all other failed or skipped tasks in the same dag and run
        repair_tasks = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == unquote(run_id)
        ).filter(
            (TaskInstance.state == State.FAILED) | (TaskInstance.state == State.UPSTREAM_FAILED)
        ).all()

        print(repair_tasks)

        # Generate list of databricks task keys from all task_ids that could be repaired.
        repair_task_keys = [t.task_id.split('.')[-1] for t in repair_tasks]

        if request.method == 'POST':
            task_keys = request.form.getlist("task_keys")  # databricks task keys selected for repair

            print(task_keys)

            # Build request paramters for Databricks Jobs repair run API
            request_body = {'run_id': databricks_run_id, 'rerun_tasks': task_keys}
            if latest_repair_id != 'None':
                request_body['latest_repair_id'] = latest_repair_id

            # Submit API request
            http_hook = HttpHook(
                method='POST',
                http_conn_id='http_default',
            )

            response = http_hook.run(
                endpoint='/api/2.1/jobs/runs/repair',
                data=json.dumps(request_body),
            )

            # Clear task instances for selected task_keys/task_ids
            task_group = task_id.split('.')[0]
            task_instances = []

            for task_key in task_keys:
                ti = session.query(TaskInstance).filter(
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.run_id == unquote(run_id),
                    TaskInstance.task_id == '{task_group}.{task_id}'.format(
                        task_group=task_group,
                        task_id=task_key
                    )
                ).filter(
                    (TaskInstance.state == State.FAILED) | (TaskInstance.state == State.UPSTREAM_FAILED)
                ).one()

                task_instances.append(ti)

            # all_upstreams = set()
            # for ti in task_instances:
            #     for u in ti.task.upstream_task_ids:
            #         if task_group in u and u.split('.')[-1] in repair_tasks and u.split('.')[-1] not in task_keys:
            #             all_upstreams.add(u)
            #
            # upstream_tis = []
            #
            # for upstream in all_upstreams:
            #     ti = session.query(TaskInstance).filter(
            #         TaskInstance.dag_id == dag_id,
            #         TaskInstance.run_id == unquote(run_id),
            #         TaskInstance.task_id == upstream
            #     ).filter(
            #         (TaskInstance.state == State.FAILED) | (TaskInstance.state == State.UPSTREAM_FAILED)
            #     ).one()
            #
            #     upstream_tis.append(ti)
            #
            #
            # all_tis = upstream_tis + task_instances

            get_run_info_ti = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.task_id == 'get_run_info',
                TaskInstance.run_id == unquote(run_id)
            ).one()

            task_instances.insert(0, get_run_info_ti)

            clear_task_instances(tis=task_instances, session=session)

            return redirect("/dags/{dag_id}/grid".format(dag_id=dag_id))

        return self.render_template(
            "test_plugin/test.html",
            dag_id=dag_id,
            task_id=task_id.split('.')[-1],
            run_id=run_id,
            all_tasks=repair_task_keys,
            databricks_run_id=databricks_run_id
        )


v_appbuilder_nomenu_view = DatabricksRun()
v_appbuilder_nomenu_package = {"view": v_appbuilder_nomenu_view}


# Defining the plugin class
class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "extra_link_plugin"
    operator_extra_links = [
        DatabricksRunLink(),
        DatabricksRepairRunLink()
    ]
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_nomenu_package]

