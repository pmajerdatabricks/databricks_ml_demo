# Databricks notebook source
# MAGIC %run  ./../utils/config

# COMMAND ----------

# MAGIC %run  ./../utils/utils_functions

# COMMAND ----------

# MAGIC %run ./../data/data_transformations

# COMMAND ----------

# TODD: load right env for model registry param

model_name = "ml-model-demo"
env = 'dev'
experiment_id = setup_mlflow_config(env_experiment_id_dict['dev'])

client = mlflow.tracking.MlflowClient()
production_model = client.get_latest_versions(name = model_name, stages = ["Production"])[0]
DataProvider = LendingClubDataProvider(spark)

# COMMAND ----------

# TODO: change the scoring data to sth else

_, X_test, _, _ = DataProvider.run()

df_predictions = model.predict(X_test)
