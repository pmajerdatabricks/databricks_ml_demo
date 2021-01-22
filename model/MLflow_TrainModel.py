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

# MAGIC %run ./../utils/data_transformations

# COMMAND ----------

# MAGIC %run ./model_training_utils

# COMMAND ----------

# DBTITLE 1,Import packages and set up MLflow experiment
model_name = "ml-model-demo"

experimentID, experimentPath = get_experiment_id_path(model_name)
mlflow.set_experiment(experimentPath)

# COMMAND ----------

# DBTITLE 1,get and transform data
df = load_and_transform_data()
display(df)

# COMMAND ----------

# DBTITLE 1,Split the data into training and test sets
pdf, predictors, target = transform_to_pandas(df)
    
X_train, X_test, Y_train, Y_test = train_test_split(pdDf[predictors], pdDf[target], test_size=0.3)

# COMMAND ----------

# DBTITLE 1,Train a simple LR model
lr = LogisticRegression()
lr.fit(X_train, Y_train)
with mlflow.start_run(run_name="Manual deployment") as run:
  eval_and_log_metrics(lr, X_test, Y_test)
  

# COMMAND ----------

max_version = get_max_version(model_name)
max_version

# COMMAND ----------

# DBTITLE 1,Function to deploy model (log model with incremented version)
def deploy_model(model, name):
  max_version = get_max_version(model_name)
  print('Max version as of now: ', max_version)
  with mlflow.start_run(run_name="Manual deployment") as run:
    eval_and_log_metrics(model, X_test, Y_test)
    mlflow.sklearn.log_model(model, "model")
    mlflow.set_tag("model_name", name)
    mlflow.set_tag("prod", "true")
    mlflow.set_tag("version", max_version + 1)
    

  print('Max version after deployment: ', get_max_version(model_name))

# COMMAND ----------

deploy_model(lr, model_name)

# COMMAND ----------

mlflow.end_run()

# COMMAND ----------

