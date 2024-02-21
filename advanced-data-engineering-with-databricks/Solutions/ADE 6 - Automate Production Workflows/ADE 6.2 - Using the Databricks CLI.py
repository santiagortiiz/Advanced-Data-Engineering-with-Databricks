# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-74269711-cd43-4b9c-8908-f06d215ca27d
# MAGIC %md
# MAGIC # Using the Databricks CLI
# MAGIC In this lesson, you will execute commands using the Databricks CLI. We will use the cluster web terminal for this demo.
# MAGIC
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Launch the driver's web terminal to run shell commands on the driver node
# MAGIC * Install the Databricks CLI and configure authentication to a Databricks workspace
# MAGIC * List files or clusters in your workspace to verify successful authentication
# MAGIC * Configure Databricks Secrets

# COMMAND ----------

# DBTITLE 0,--i18n-bccc5e28-657f-4d43-a064-12f310a9b7e0
# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the classroom setup script in the next cell to configure the classroom.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-06.2

# COMMAND ----------

# DBTITLE 0,--i18n-99bcb96f-b69e-4f51-b7d0-3e4f2bd561c2
# MAGIC %md
# MAGIC
# MAGIC ## Setup CLI on cluster
# MAGIC
# MAGIC Install the Databricks CLI using the following cell. Note that this procedure removes any existing version that may already be installed, and installs the newest version of the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html). A legacy version exists that is distributed through **`pip`**, however we recommend following the procedure here to install the newer one.

# COMMAND ----------

# MAGIC %sh rm -f $(which databricks); curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# COMMAND ----------

# DBTITLE 0,--i18n-d0b9eb16-9b01-11ee-b9d1-0242ac120002
# MAGIC %md
# MAGIC
# MAGIC ### Authentication
# MAGIC
# MAGIC Usually, you would have to set up authentication for the CLI. But in this training environment, that's already taken care of if you ran through the accompanying *Generate Tokens* notebook. If you did, credentials will already be loaded into the **`DATABRICKS_HOST`** and **`DATABRICKS_TOKEN`** environment variables. If you did not, run through it now then restart this notebook.
# MAGIC
# MAGIC Verify the credentials by executing the following cell, which displays a lists the contents of in */Users* workspace directory.

# COMMAND ----------

# MAGIC %sh databricks workspace list /Users

# COMMAND ----------

# DBTITLE 0,--i18n-f044424f-6685-4966-a84d-6f98b4b4dcf0
# MAGIC %md
# MAGIC ## Create secrets
# MAGIC Secrets are a key-value pair stored with a Databricks-backed scope. First you must create the scope, then store the secrets in the scope you created. In this case we'll assuming a scope name of *dbacademy*, but you can change that if you wish.

# COMMAND ----------

# MAGIC %sh databricks secrets create-scope dbacademy

# COMMAND ----------

# DBTITLE 0,--i18n-f2b68406-9b72-11ee-8c90-0242ac120002
# MAGIC %md
# MAGIC
# MAGIC Populate the scope with your secrets.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks secrets put-secret dbacademy db_instance --string-value $DATABRICKS_HOST
# MAGIC databricks secrets put-secret dbacademy db_token --string-value $DATABRICKS_TOKEN

# COMMAND ----------

# DBTITLE 0,--i18n-c40d923a-9a1d-11ee-b9d1-0242ac120002
# MAGIC %md
# MAGIC ## Read secrets
# MAGIC
# MAGIC Read the secrets using the Databricks CLI using a command like this.

# COMMAND ----------

# MAGIC %sh databricks secrets get-secret dbacademy db_instance

# COMMAND ----------

# DBTITLE 0,--i18n-f2b68528-9b72-11ee-8c90-0242ac120002
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC From a notetook, use Python code like the following to read a secret into a variable. Note that the values fetched from the scope are never displayed in the notebook (see [Secret redaction](https://docs.databricks.com/security/secrets/redaction.html)).

# COMMAND ----------

instance = dbutils.secrets.get(scope="dbacademy", key="db_instance")
token = dbutils.secrets.get(scope="dbacademy", key="db_token")
instance,token

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
