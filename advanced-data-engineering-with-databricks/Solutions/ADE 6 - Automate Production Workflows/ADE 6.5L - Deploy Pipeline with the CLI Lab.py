# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-ec8041ff-7d13-4f98-b940-3803c89bb6d1
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Lab: Deploy Pipeline with the CLI
# MAGIC In this lab, you will use the Databricks CLI to work with DLT pipelines.
# MAGIC
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Programmatically deploy a workload using the Databricks CLI

# COMMAND ----------

# DBTITLE 0,--i18n-cacb5a78-ef75-48af-b3d1-9de4921e7b74
# MAGIC %md
# MAGIC
# MAGIC ## Run the setup
# MAGIC Run the setup script for this lesson by running the cell below. This will ensure that:
# MAGIC * The Databricks CLI is installed
# MAGIC * Authentication is configured
# MAGIC * A pipeline is created

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-06.5L

# COMMAND ----------

# DBTITLE 0,--i18n-344f74ae-b469-4fca-9f1b-f08f57d61cef
# MAGIC %md
# MAGIC ## Run your pipeline
# MAGIC The pipeline you will be working with was generated when you ran the classroom setup script at the top of this notebook. The output from that cell shows, among other things, your pipeline's name. Use the pipeline name to get your pipeline id from the list of pipelines [here](#joblist/pipelines)
# MAGIC  
# MAGIC Open the driver's terminal by clicking View -> Open web terminal in the menu at the top of the page, and run the proper CLI command to start your pipeline.

# COMMAND ----------

# ANSWER
%sh databricks pipelines start-update $DATABRICKS_PIPELINE_ID

# COMMAND ----------

# DBTITLE 0,--i18n-c50189c2-9eb3-11ee-8c90-0242ac120002
# MAGIC %md
# MAGIC
# MAGIC ## Check your work
# MAGIC To check your work, execute the following Python code. This uses the pipelines API to check whether you've successfully triggered an update for your pipeline.

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

pipeline = w.pipelines.get(pipeline_id=os.environ['DATABRICKS_PIPELINE_ID'])

try:
  state = pipeline.latest_updates[0].state.value

  not_done = ["WAITING_FOR_RESOURCES", "INITIALIZING", "SETTING_UP_TABLES", "RUNNING"]
  done = ["COMPLETED", "FAILED", "CANCELED"]

  if state in not_done:
      print(f"Pipeline is running (State: {state})")
      print("Excellent work!!")
  elif state in done:
      print(f"Pipeline is done (State: {state})")
      print("Excellent work!!")
  else:
      print("Something must be wrong. Double-check that you started the pipeline")
except:
  print("Something must be wrong. Double-check that you started the pipeline")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
