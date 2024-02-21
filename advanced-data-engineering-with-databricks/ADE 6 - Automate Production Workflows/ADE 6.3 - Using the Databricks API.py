# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-633e3a73-aea8-41b4-b743-85b5263bc88e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Using the Databricks API
# MAGIC In this lesson, you will learn how to setup, configure, and use the Databricks API.
# MAGIC
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Programmatically run commands through the REST API using `curl` and Python

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-06.3

# COMMAND ----------

# DBTITLE 0,--i18n-bba843f8-9e3b-4927-a0a2-031fe9efc8c2
# MAGIC %md
# MAGIC
# MAGIC ## Make API calls Using `curl`
# MAGIC
# MAGIC Use the following pattern with **`curl`** to issue API calls. Note the following:
# MAGIC * **`cat`**, in combination with a <a href="https://tldp.org/LDP/abs/html/here-docs.html" target="_blank">here document</a>, supplies the JSON payload that specifies the API parameters; in this example, we specify the **`path`** parameter.
# MAGIC * the **`-H`** option, in combination with **`DATABRICKS_TOKEN`** environment variable, specifies the authentication header
# MAGIC * the **`DATABRICKS_HOST`** environment variable supplies the base URL for the [API endpoints](https://docs.databricks.com/api).
# MAGIC * **`json_pp`** formats the JSON response into a readable form.
# MAGIC
# MAGIC This particular invocation lists the contents of the */Users* directory.

# COMMAND ----------

# MAGIC %sh cat << EOF | curl -s -X GET -H "Authorization: Bearer $DATABRICKS_TOKEN" $DATABRICKS_HOST/api/2.0/workspace/list -d @- | json_pp
# MAGIC {
# MAGIC   "path": "/Users"
# MAGIC }
# MAGIC EOF

# COMMAND ----------

# DBTITLE 0,--i18n-f2b6864a-9b72-11ee-8c90-0242ac120002
# MAGIC %md
# MAGIC
# MAGIC The commands are a bit simpler if we don't need to pass in parameters. For example, the following lists all clusters.

# COMMAND ----------

# MAGIC %sh curl -s -X GET -H "Authorization: Bearer $DATABRICKS_TOKEN" $DATABRICKS_HOST/api/2.0/clusters/list | json_pp

# COMMAND ----------

# DBTITLE 0,--i18n-93303b47-dc2b-4f71-8359-c57649d954fb
# MAGIC %md
# MAGIC
# MAGIC ## Make API calls using Python
# MAGIC
# MAGIC You can issue API calls through Python. The easiest way to do this is to install and use the [Databricks SDK](https://docs.databricks.com/en/dev-tools/sdk-python.html). This is already installed for you in this setup.
# MAGIC
# MAGIC In this section, we'll see how to duplicate the two API calls from above using the Python SDK.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# Instantiate a client object. You can specify credentials in this initialization call
# but in this setup we are relying on DATABRICKS_HOST and DATABRICKS_TARGET
# having been set up.

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 0,--i18n-f2b6894c-9b72-11ee-8c90-0242ac120002
# MAGIC %md
# MAGIC
# MAGIC Use the API to list contents of the */Users* folder.

# COMMAND ----------

[ i.path for i in w.workspace.list('/Users') ]

# COMMAND ----------

# DBTITLE 0,--i18n-3212e460-9b7c-11ee-8c90-0242ac120002
# MAGIC %md
# MAGIC
# MAGIC Use the API to list clusters.

# COMMAND ----------

[ (i.cluster_id,i.cluster_name) for i in w.clusters.list() ]

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
