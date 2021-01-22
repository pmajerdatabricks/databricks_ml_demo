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

from pyspark.sql.functions import *


# COMMAND ----------

def load_and_transform_data():

  df = spark.read.format("parquet").load("/databricks-datasets/samples/lending_club/parquet") 

  df = df.select("loan_status", "int_rate", "revol_util", "issue_d", "earliest_cr_line", "emp_length", "verification_status", "total_pymnt", "loan_amnt", "grade", "annual_inc", "dti", "addr_state", "term", "home_ownership", "purpose", "application_type", "delinq_2yrs", "total_acc")

  print("------------------------------------------------------------------------------------------------")
  print("Create bad loan label, this will include charged off, defaulted, and late repayments on loans...")
  df = df.filter(df.loan_status.isin(["Default", "Charged Off", "Fully Paid"]))\
                         .withColumn("bad_loan", (~(df.loan_status == "Fully Paid")).cast("string"))


  print("------------------------------------------------------------------------------------------------")
  print("Turning string interest rate and revoling util columns into numeric columns...")
  df = df.withColumn('int_rate', regexp_replace('int_rate', '%', '').cast('float')) \
                         .withColumn('revol_util', regexp_replace('revol_util', '%', '').cast('float')) \
                         .withColumn('issue_year',  substring(df.issue_d, 5, 4).cast('double') ) \
                         .withColumn('earliest_year', substring(df.earliest_cr_line, 5, 4).cast('double'))
  df = df.withColumn('credit_length_in_years', (df.issue_year - df.earliest_year))


  print("------------------------------------------------------------------------------------------------")
  print("Converting emp_length column into numeric...")
  df = df.withColumn('emp_length', trim(regexp_replace(df.emp_length, "([ ]*+[a-zA-Z].*)|(n/a)", "") ))
  df = df.withColumn('emp_length', trim(regexp_replace(df.emp_length, "< 1", "0") ))
  df = df.withColumn('emp_length', trim(regexp_replace(df.emp_length, "10\\+", "10") ).cast('float'))

  print("------------------------------------------------------------------------------------------------")
  print("Map multiple levels into one factor level for verification_status...")
  df = df.withColumn('verification_status', trim(regexp_replace(df.verification_status, 'Source Verified', 'Verified')))

  print("------------------------------------------------------------------------------------------------")
  print("Calculate the total amount of money earned or lost per loan...")
  df = df.withColumn('net', round( df.total_pymnt - df.loan_amnt, 2))

  return df

# COMMAND ----------

def transform_to_pandas(df):
  predictors = ["term", "home_ownership", "purpose", "addr_state", "verification_status","application_type", "loan_amnt","emp_length", "annual_inc","dti", "delinq_2yrs","revol_util","total_acc", "credit_length_in_years", "int_rate", "net", "issue_year"]
  target = 'bad_loan'

  pdDf = df.toPandas()

  for col in pdDf.columns:
    if pdDf.dtypes[col]=='object':
      pdDf[col] =  pdDf[col].astype('category').cat.codes
    pdDf[col] = pdDf[col].fillna(0)
    
  return pdDf, predictors, target

# COMMAND ----------

def get_dummy_test_data():
  
  df = load_and_transform_data().head(1000)
  pdf, predictors, target = transform_to_pandas(df)
  
  return pdf, predictors, target
  
  