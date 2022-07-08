from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperatorLink
from airflow.models.xcom import XCom
from airflow.models.taskinstance import TaskInstance
from astronomer.providers.http.sensors.http import HttpSensorAsync


class DatabricksLogLink(BaseOperatorLink):
    name = "Databricks Task"

    # Add list of all the operators to which you want to add this OperatorLinks
    # Example: operators = [GCSToS3Operator, GCSToBigQueryOperator]
    operators = [HttpSensorAsync]

# TODO: get run link working
    def get_link(self, operator, *, ti_key):
        # task_id = ti_key.task_id
        # run_page_url = XCom.get_one(task_id='get_run_info')[ti_key.task_id.split('.')[-1]]['run_page_url']
        run_page_url = "https://databricsk.com"
        return run_page_url


# Defining the plugin class
class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "extra_link_plugin"
    operator_extra_links = [
        DatabricksLogLink(),
    ]
