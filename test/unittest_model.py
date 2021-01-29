# Databricks notebook source
from sklearn.metrics import roc_auc_score

# COMMAND ----------

# DBTITLE 1,Train New Model on Pull Request
# MAGIC %run ./../jobs/model_trainning_job

# COMMAND ----------

# DBTITLE 1,Test if model fulfils quality requirements on curated data
runs = mlflow.search_runs(experiment_ids=experiment_id)
latest_run_index = runs.start_time.idxmax()
latest_run = runs.loc[0]
artifact_uri = latest_run['artifact_uri']

model = mlflow.sklearn.load_model(artifact_uri + '/model')

# COMMAND ----------

X, Y = get_dummy_test_data()

predictions = model.predict(X)

roc = roc_auc_score(Y, predictions)

assert roc > 0.8