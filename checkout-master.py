# Databricks notebook source
# Initialize input widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("admin-token", "", "Admin Token")
dbutils.widgets.text("databricks-instance", "", "Databricks Instance")


# COMMAND ----------

# Feel free to directly set the environment variables here, instead of setting them through widgets
import os
os.environ["ADMIN_TOKEN"] = dbutils.widgets.get("admin-token")
os.environ["DATABRICKS_INSTANCE"] = dbutils.widgets.get("databricks-instance")


# COMMAND ----------

# MAGIC %md
# MAGIC Optional check that environment variables are set correctly

# COMMAND ----------

# MAGIC %sh
# MAGIC echo $ADMIN_TOKEN
# MAGIC echo $DATABRICKS_INSTANCE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check that there is no folder named "Projects" in the root directory
# MAGIC You should get a `RESOURCE_DOES_NOT_EXIST` response here. If you don't, rename or delete the "Projects" folder in the root directory and migrate the jobs.

# COMMAND ----------

# MAGIC %sh
# MAGIC curl -n -X POST https://$DATABRICKS_INSTANCE/api/2.0/projects/fetch-and-checkout \
# MAGIC     -H 'Authorization: Bearer '$ADMIN_TOKEN'' \
# MAGIC     -d '{"path": "/Projects/piotr.majer@databricks.com/demo", "branch": "main"}'
