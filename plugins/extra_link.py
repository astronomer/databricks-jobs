import logging
from typing import Optional

from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperatorLink
from airflow.models.xcom import XCom
from airflow.models.taskinstance import TaskInstance
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from astronomer.providers.http.sensors.http import HttpSensorAsync
from sensors.DatabricksHttpSensorAsync import DatabricksHttpSensorAsync
from airflow.settings import Session
from airflow.utils.context import Context


class DatabricksRunLink(BaseOperatorLink):
    name = "Databricks Task"

    operators = [HttpSensorAsync, DatabricksHttpSensorAsync]

    def get_link(self, operator, *, ti_key=None):

        task_id = ti_key.task_id.split('.')[-1]
        run_page_url = XCom.get_one(
            dag_id=ti_key.dag_id, task_id='get_run_info', run_id=ti_key.run_id
        )[task_id]['run_page_url']

        return run_page_url


# Defining the plugin class
class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "extra_link_plugin"
    operator_extra_links = [
        DatabricksRunLink(),
    ]
