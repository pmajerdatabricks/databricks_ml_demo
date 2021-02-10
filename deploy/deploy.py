# Databricks notebook source
#%pip install databricks_cli

# COMMAND ----------

import os
databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None) 
os.environ['DATABRICKS_HOST'] = 'https://e2-dogfood.staging.cloud.databricks.com/'
os.environ['DATABRICKS_TOKEN'] = databricks_token

# COMMAND ----------

new_cluster_config = """
{
    "spark_version": "7.3.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "aws_attributes": {
      "availability": "ON_DEMAND"
    },
    "num_workers": 2
}
"""
existing_cluster_id = '0923-164208-meows279'
notebook_path = '/Repos/michael.shtelma@databricks.com/ml_demo_msh/test/unittest_model'

# COMMAND ----------

import json
import time

from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import JobsService

config = EnvironmentVariableConfigProvider().get_config()
api_client = _get_api_client(config, command_name="cicdtemplates-")
jobs_service = JobsService(api_client)

notebook_task = {'notebook_path': notebook_path}
#new_cluster = json.loads(new_cluster_config)
res = jobs_service.submit_run(run_name="xxx", existing_cluster_id=existing_cluster_id,  notebook_task=notebook_task, )
run_id = res['run_id']
print(run_id)
while True:
    status = jobs_service.get_run(run_id)
    print(status)
    result_state = status["state"].get("result_state", None)
    if result_state:
        print(result_state)
        assert result_state == "SUCCESS"
    else:
        time.sleep(5)

# COMMAND ----------

# MAGIC %sh ls -la

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

