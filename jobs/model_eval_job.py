# Databricks notebook source
# MAGIC %run  ./../utils/config

# COMMAND ----------

# MAGIC %run  ./../utils/utils_functions

# COMMAND ----------

# MAGIC %run ./../data/data_transformations

# COMMAND ----------

# MAGIC %run ./../model/evaluation_pipeline

# COMMAND ----------

model_name = "ml-model-demo"
env = 'dev'
experiment_id = setup_mlflow_config(env_experiment_id_dict['dev'])

DataProvider = LendingClubDataProvider(spark)
X_train, X_test, Y_train, Y_test = DataProvider.run()
display(X_train)

# COMMAND ----------

eval_pipeline = LendingClubModelEvaluationPipeline(spark, experiment_id, model_name)

eval_pipeline.run(X_train, X_test, Y_train, Y_test)
