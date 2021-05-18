# Databricks notebook source
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
# Existing cluster ID where integration test will be executed
existing_cluster_id = '0923-164208-meows279'
# Path to the notebook with the integration test
notebook_path = '/Repos/michael.shtelma@databricks.com/ml_demo_msh/test/unittest_model'

# COMMAND ----------

import json
import time

from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import JobsService

# Let's create Databricks CLI API client to be able to interact with Databricks REST API
config = EnvironmentVariableConfigProvider().get_config()
api_client = _get_api_client(config, command_name="cicdtemplates-")

# Let's update our Repo to the latest git revision using Repos API
res = api_client.perform_query('PATCH','/repos/{repo_id}'.format(repo_id="3814111933528092"), {"branch":"master"})
print(res)

#Let's create a jobs service to be able to start/stop Databricks jobs
jobs_service = JobsService(api_client)

notebook_task = {'notebook_path': notebook_path}
#new_cluster = json.loads(new_cluster_config)

# Submit integration test job to Databricks REST API
res = jobs_service.submit_run(run_name="xxx", existing_cluster_id=existing_cluster_id,  notebook_task=notebook_task, )
run_id = res['run_id']
print(run_id)

#Wait for the job to complete
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


