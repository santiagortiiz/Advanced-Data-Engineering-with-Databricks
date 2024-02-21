# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-bcf2134a-a03a-4e8b-b0a8-a4b1f3d8e400
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC # Propagating Changes with CDF Lab
# MAGIC
# MAGIC We'll be using Change Data Feed to propagate changes to many tables from a single source.
# MAGIC
# MAGIC For this lab, we'll work with the fitness tracker datasets to propagate changes through a Lakehouse with Delta Lake Change Data Feed (CDF).
# MAGIC
# MAGIC Because the **`user_lookup`** table links identifying information between different pipelines, we'll make this the point where changes propagate from.
# MAGIC
# MAGIC
# MAGIC ## Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC - Enable Change Data Feed on a particular table
# MAGIC - Read CDF output with Spark SQL or PySpark
# MAGIC - Refactor ELT code to process CDF output

# COMMAND ----------

# DBTITLE 0,--i18n-1beef264-5e2d-4b3c-9f81-118904a70dd1
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Begin by running the following cell to set up relevant databases and paths.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-03.5L

# COMMAND ----------

# DBTITLE 0,--i18n-e82ee278-dc55-4717-b0e2-8784d7c8f236
# MAGIC %md
# MAGIC
# MAGIC ## Enables CDF for the table
# MAGIC
# MAGIC To enables CDF for the **"user_lookup"** table use ALTER TABLE and set TBLPROPERTIES to activate **`delta.enableChangeDataFeed`**.

# COMMAND ----------

# TODO

# COMMAND ----------

# DBTITLE 0,--i18n-89e09f6b-d915-4012-9066-75b86d4e7f3c
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Read the CDF output from table
# MAGIC
# MAGIC To read the CDF data:
# MAGIC - Set up a streaming read on the **`user_lookup`** table
# MAGIC - Configure the stream to enable reading change data
# MAGIC - Configure the stream to start reading from version 1 of the **`user_lookup`** table

# COMMAND ----------

# TODO

user_lookup_df = (FILL_IN)

# COMMAND ----------

# DBTITLE 0,--i18n-b175df8f-e60f-4fb1-a2cc-b06342e7d012
# MAGIC %md
# MAGIC ### Delete a record from table
# MAGIC
# MAGIC To delete record from table:
# MAGIC - Use **`DELETE`** statement with column_name of table
# MAGIC - Enter actual name of the column and value to delete record

# COMMAND ----------

# TODO

# COMMAND ----------

# DBTITLE 0,--i18n-00623cc7-c2dc-4251-8040-24a0a275e58e
# MAGIC %md
# MAGIC ### Check that the record was deleted from user_lookup
# MAGIC
# MAGIC To check whether record was deleted:
# MAGIC - Use **`SELECT`** statement to get record from table
# MAGIC - Specify the column_name and value to see whether record with specific id exist in table

# COMMAND ----------

# TODO

# COMMAND ----------

# DBTITLE 0,--i18n-6ec11ffc-1a61-4537-92be-0e90c3c6adc9
# MAGIC %md
# MAGIC ### Propagate deletes from multiple tables
# MAGIC
# MAGIC To propagate delete follow these steps:
# MAGIC - Create temporary view of user_lookup table as **`user_lookup_deletes`**
# MAGIC - Select all record in view where **`_change_type`** is **delete**  
# MAGIC - Merge into **`users`** table when **`alt_id`** gets matched
# MAGIC - Similarly, merge into **`user_bins`** table when **`user_id`** gets matched

# COMMAND ----------

# TODO

CREATE OR REPLACE TEMPORARY VIEW FILL_IN 

MERGE INTO users u
USING FILL_IN

MERGE INTO user_bins ub
USING FILL_IN

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
