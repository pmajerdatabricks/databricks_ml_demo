# Databricks notebook source


# COMMAND ----------

import logging
import sys
import functools
import time
import mlflow

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("azure").setLevel(logging.ERROR)

# COMMAND ----------

def timed(logger, level=None, format='%s: %s ms'):
    if level is None:
        level = logging.INFO

    def decorator(fn):
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            start = time.time()
            result = fn(*args, **kwargs)
            duration = time.time() - start
            logger.log(level, format, fn.__qualname__  + ' took', np.round(duration * 1000,3))
            return result
        return inner

    return decorator

# COMMAND ----------

def setup_mlflow_config(path):
    mlflow.set_experiment(path)
    try:
        experiment_id = mlflow.get_experiment_by_name(
            path
        ).experiment_id
        return experiment_id
    except FileNotFoundError:
        time.sleep(10)
        experiment_id = mlflow.get_experiment_by_name(
            path
        ).experiment_id
        return experiment_id

# COMMAND ----------

# MAGIC %md
# MAGIC Utils
