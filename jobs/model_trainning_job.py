# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Pipeline that trains model and deploys it to MLflow
# MAGIC 
# MAGIC This pipeline trains simple model and logs it to MLflow.
# MAGIC 
# MAGIC We are using MLflow tags here to mark our model as "production" and set the version of the model. 
# MAGIC 
# MAGIC The consumers will be able to get the latest model by quering MLflow and determining the latest version

# COMMAND ----------

# MAGIC %run ./../data/data_transformations

# COMMAND ----------

# MAGIC %run ./../model/training_pipeline

# COMMAND ----------

# MAGIC %run  ./../utils/utils_functions

# COMMAND ----------

# MAGIC %run  ./../utils/config

# COMMAND ----------

# DBTITLE 1,get and transform data
model_name = "ml-model-demo"
env = 'dev'
experiment_id = setup_mlflow_config(env_experiment_id_dict['dev'])

DataProvider = LendingClubDataProvider(spark)
X_train, X_test, Y_train, Y_test = DataProvider.run()
# display(X_train)

# COMMAND ----------

# DBTITLE 1,Train a simple LR model
model_pipeline = LendingClubTrainingPipeline(model_name, experiment_id)
model_pipeline.run(X_train, X_test, Y_train, Y_test)
