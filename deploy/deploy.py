# Databricks notebook source
#import os
#os.environ['DATABRICKS_HOST'] = 'https://eastus2.azuredatabricks.net/'
#os.environ['DATABRICKS_TOKEN'] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

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
# Existing cluster ID where integration test will be executed
existing_cluster_id = '0820-100141-forum325'
# Path to the notebook with the integration test
notebook_path = '/test/unittest_model'
repo_path = '/Repos/michael.shtelma@databricks.com/databricks_ml_demo'


repos_path_prefix='/Repos/michael.shtelma@databricks.com/databricks_ml_demo'
git_url = 'https://github.com/mshtelma/databricks_ml_demo'
provider = 'gitHub'
branch = 'main'

# COMMAND ----------

from argparse import ArgumentParser
p = ArgumentParser()
p.add_argument("--branch", required=False, type=str)
namespace = p.parse_known_args()[0]
conf_branch = namespace.branch
if conf_branch is not None:
  branch = conf_branch
  print('Configured branch: ', branch)
else:
  print('No branch was specified. Using main....')


# COMMAND ----------

import json
import time
from datetime import datetime

from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import JobsService, ReposService

# Let's create Databricks CLI API client to be able to interact with Databricks REST API
config = EnvironmentVariableConfigProvider().get_config()
api_client = _get_api_client(config, command_name="cicdtemplates-")

#Let's create Repos Service
repos_service = ReposService(api_client)

# Let's store the path for our new Repo
repo_path = f'{repos_path_prefix}_{branch}_{str(datetime.now().microsecond)}'
print('Checking out the following repo: ', repo_path)

# Let's clone our GitHub Repo in Databricks using Repos API
repo = repos_service.create_repo(url=git_url, provider=provider, path=repo_path)

#Let's checkout the needed branch
repos_service.update_repo(id=repo['id'], tag=branch)

#Let's create a jobs service to be able to start/stop Databricks jobs
jobs_service = JobsService(api_client)

notebook_task = {'notebook_path': repo_path + notebook_path}
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
        break
    else:
        time.sleep(5)

# COMMAND ----------


