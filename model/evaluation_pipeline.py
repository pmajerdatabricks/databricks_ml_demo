# Databricks notebook source
# MAGIC %run ./../utils/utils_functions

# COMMAND ----------

import time
import mlflow
import numpy as np
import mlflow.sklearn
from mlflow.exceptions import RestException
from mlflow.tracking import MlflowClient
from sklearn.metrics import accuracy_score, roc_auc_score


class LendingClubModelEvaluationPipeline():
    def  __init__(self, spark, experimentID, model_name):
        self.spark = spark
        self.model_name = model_name
        self.experimentID = experimentID

    @timed(logger)
    def run(self, X_train, X_test, Y_train, Y_test):
        mlflow_client = MlflowClient()

        cand_run_ids = self.get_candidate_models()
        best_cand_roc, best_cand_run_id = self.get_best_model(cand_run_ids, X_test, Y_test)
        print('Best ROC (candidate models): ', np.mean(best_cand_roc))

        try:
            versions = mlflow_client.get_latest_versions(self.model_name, stages=['Production'])
            prod_run_ids = [v.run_id for v in versions]
            best_prod_roc, best_prod_run_id = self.get_best_model(prod_run_ids, X_test, Y_test)
        except RestException:
            best_prod_roc = -1
        print('ROC (production models): ', np.mean(best_prod_roc))

        if np.mean(best_cand_roc >= best_prod_roc) > 0.9:
            # deploy new model
            model_version = mlflow.register_model("runs:/" + best_cand_run_id + "/model", self.model_name)
            time.sleep(5)
            mlflow_client.transition_model_version_stage(name=self.model_name, version=model_version.version,
                                                         stage="Production")
            print('Deployed version: ', model_version.version)
        # remove candidate tags
        for run_id in cand_run_ids:
            mlflow_client.set_tag(run_id, 'candidate', 'false')

    @timed(logger)
    def get_best_model(self, run_ids, X, Y):
        best_roc = -1
        best_run_id = None
        for run_id in run_ids:
            roc = self.evaluate_model(run_id, X, Y)
            if np.mean(roc > best_roc) > 0.9:
                best_roc = roc
                best_run_id = run_id
        return best_roc, best_run_id
    
    @timed(logger)
    def get_candidate_models(self):
        spark_df = self.spark.read.format("mlflow-experiment").load(str(self.experimentID))
        pdf = spark_df.where("tags.candidate='true'").select("run_id").toPandas()
        return pdf['run_id'].values

    @timed(logger)
    def evaluate_model(self, run_id, X, Y):
        model = mlflow.sklearn.load_model('runs:/{}/model'.format(run_id))
        predictions = model.predict(X)
        # acc = accuracy_score(Y, predictions)
        n = 100
        sampled_scores = []
        score = 0.5
        rng = np.random.RandomState()
        for i in range(n):
            # sampling with replacement on the prediction indices
            indices = rng.randint(0, len(predictions), len(predictions))
            if len(np.unique(Y.iloc[indices])) < 2:
                sampled_scores.append(score)
                continue
                
            score = roc_auc_score(Y.iloc[indices], predictions[indices])
            sampled_scores.append(score)
        return np.array(sampled_scores)
