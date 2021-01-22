# Databricks notebook source
import os
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn

from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, accuracy_score, mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split, cross_val_score
from  mlflow.tracking import MlflowClient


# COMMAND ----------

# DBTITLE 1,Method for evaluating and logging metrics
def eval_and_log_metrics(estimator, X, Y):
    predictions = estimator.predict(X)
    
    # Calc metrics
    acc = accuracy_score(Y, predictions)
    roc = roc_auc_score(Y, predictions)
    mse = mean_squared_error(Y, predictions)
    mae = mean_absolute_error(Y, predictions)
    r2 = r2_score(Y_test, predictions)
    
    #Print metrics
    print("  acc: {}".format(acc))
    print("  roc: {}".format(roc))
    print("  mse: {}".format(mse))
    print("  mae: {}".format(mae))
    print("  R2: {}".format(r2))
    
    # Log metrics
    mlflow.log_metric("acc", acc)
    mlflow.log_metric("roc", roc)
    mlflow.log_metric("mse", mse)
    mlflow.log_metric("mae", mae)  
    mlflow.log_metric("r2", r2)  

# COMMAND ----------

def get_max_version(name):
  df = spark.read.format("mlflow-experiment").load(experimentID)
  v = df.where("tags.prod='true' and tags.model_name='{}' ".format(name)).select("tags.version").agg({"version":"max"}).head()[0]
  if v:
    return int(v)
  else:
    return 0

# COMMAND ----------

def get_experiment_id_path(model_name):
  experimentPath = "/Shared/" + model_name

  experimentID = MlflowClient().get_experiment_by_name(experimentPath).experiment_id
  print(experimentID)
  return experimentID, experimentPath