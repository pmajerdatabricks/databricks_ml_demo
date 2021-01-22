# Databricks notebook source
# DBTITLE 1,Train New Model on Pull Request
# MAGIC %run ./../model/MLflow_TrainModel

# COMMAND ----------

# DBTITLE 1,Test if model fulfils quality requirements on curated data
runs = mlflow.search_runs(experiment_ids=experimentID)
latest_run_index = runs.start_time.idxmax()
latest_run = runs.loc[0]
artifact_uri = latest_run['artifact_uri']

model = mlflow.sklearn.load_model(artifact_uri + '/model')

# COMMAND ----------

pdf, predictors, target  get_dummy_test_data()

predictions model.predict(pdf[predictors])

roc = roc_auc_score(pdf[target], predictions)

assert roc > 0.8