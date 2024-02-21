# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-3e1803b4-b28d-4097-b82a-750c0e12e2e4
# MAGIC %md
# MAGIC # Processing Records from Delta Change Data Feed
# MAGIC
# MAGIC In this notebook, we'll demonstrate an end-to-end of how you can easily propagate changes through a Lakehouse with Delta Lake Change Data Feed (CDF).
# MAGIC
# MAGIC For this demo, we'll work with a slightly different dataset representing patient information for medical records. Descriptions of the data at various stages follow.
# MAGIC
# MAGIC ### Bronze Table
# MAGIC Here we store all records as consumed. A row represents:
# MAGIC 1. A new patient providing data for the first time
# MAGIC 1. An existing patient confirming that their information is still correct
# MAGIC 1. An existing patient updating some of their information
# MAGIC
# MAGIC The type of action a row represents is not captured.
# MAGIC
# MAGIC ### Silver Table
# MAGIC This is the validated view of our data. Each patient will appear only once in this table. An upsert statement will be used to identify rows that have changed.
# MAGIC
# MAGIC ### Gold Table
# MAGIC For this example, we'll create a simple gold table that captures patients that have a new address.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Enable Change Data Feed on a cluster or for a particular table
# MAGIC - Describe how changes are recorded
# MAGIC - Read CDF output with Spark SQL or PySpark
# MAGIC - Refactor ELT code to process CDF output

# COMMAND ----------

# DBTITLE 0,--i18n-25cbaf37-fdc7-4589-8a3e-bc04b126dc50
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Setup
# MAGIC
# MAGIC The following code defines some paths, a demo database, and clears out previous runs of the demo.
# MAGIC
# MAGIC It also defines another data factory that we'll use to land raw data in our source directory, allowing us to process new records as if they were arriving in production.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-03.4

# COMMAND ----------

# DBTITLE 0,--i18n-dc58a0f4-19c1-4e34-a087-3d1b506a2511
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC Enable CDF using Spark conf setting in a notebook or on a cluster will ensure it's used on all newly created Delta tables in that scope.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)

# COMMAND ----------

# DBTITLE 0,--i18n-7b33f943-32a7-4c92-9f47-8340d3979e29
# MAGIC %md
# MAGIC
# MAGIC ## Ingest Data with Auto Loader
# MAGIC
# MAGIC Here we'll use Auto Loader to ingest data as it arrives.
# MAGIC
# MAGIC The steps below include:
# MAGIC * Declaring the target table
# MAGIC * Creating & starting the stream
# MAGIC * Load some data into our source directory

# COMMAND ----------

# DBTITLE 0,--i18n-24e9f0f6-dc60-44cf-a2d0-68b6c2ebdde9
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Create the bronze table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze
# MAGIC   (mrn BIGINT, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, zip BIGINT, city STRING, state STRING, updated timestamp) 
# MAGIC LOCATION '${DA.paths.working_dir}/bronze'

# COMMAND ----------

# DBTITLE 0,--i18n-1581a709-554f-4fab-8b96-e91f98c64114
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Create and start the stream.
# MAGIC
# MAGIC For this example, we will:
# MAGIC * Use continuous processing as opposed to trigger-once or trigger-available-now
# MAGIC * Specify the schema as opposed to inferring it

# COMMAND ----------

schema = "mrn BIGINT, dob DATE, sex STRING, gender STRING, first_name STRING, last_name STRING, street_address STRING, zip BIGINT, city STRING, state STRING, updated TIMESTAMP"

bronze_query = (spark.readStream
                     .format("cloudFiles")
                     .option("cloudFiles.format", "json")
                     .schema(schema)
                     .load(DA.paths.cdc_stream)
                     .writeStream
                     .format("delta")
                     .outputMode("append")
                     #.trigger(availableNow=True)
                     .trigger(processingTime='5 seconds')
                     .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
                     .table("bronze"))

DA.block_until_stream_is_ready(bronze_query)

# COMMAND ----------

# DBTITLE 0,--i18n-ce0db6b4-5146-489c-9ceb-5acf10a627ef
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Expand the stream monitor above to see the progress of your stream. 
# MAGIC
# MAGIC Use the cell below to land a batch of data; you should see these records processed as a single batch.

# COMMAND ----------

DA.cdc_stream.load()

# COMMAND ----------

# DBTITLE 0,--i18n-e195453d-fb0b-49c5-b57d-b781e7f525de
# MAGIC %md
# MAGIC
# MAGIC ## Create a Target Table
# MAGIC
# MAGIC Here we use **`DEEP CLONE`** to move read-only data from PROD to our DEV environment (where we have full write/delete access).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE silver
# MAGIC DEEP CLONE delta.`${DA.paths.silver_source}`
# MAGIC LOCATION '${DA.paths.user_db}/silver'

# COMMAND ----------

# DBTITLE 0,--i18n-317dc7f9-f89c-40a8-901c-a09a8b8fed82
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Tables that were not created with CDF enabled will not have it turned on by default, but can be altered to capture changes with the following syntax.
# MAGIC
# MAGIC Note that editing properties will version a table. 
# MAGIC
# MAGIC Note: CDC data is **NOT** captured during **`CLONE`** operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED silver

# COMMAND ----------

# DBTITLE 0,--i18n-64655ff1-8c57-4879-9b76-bd1aaf4416eb
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Upsert Data with Delta Lake
# MAGIC
# MAGIC Here we define upsert logic into the silver table using a streaming read against the bronze table, matching on our unique identifier **`mrn`**.
# MAGIC
# MAGIC We specify an additional conditional check to ensure that a field in the data has changed before inserting the new record.

# COMMAND ----------

def upsert_to_delta(microBatchDF, batchId):
    microBatchDF.createOrReplaceTempView("updates")
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO silver s
        USING updates u
        ON s.mrn = u.mrn
        WHEN MATCHED AND s.dob <> u.dob OR
                         s.sex <> u.sex OR
                         s.gender <> u.gender OR
                         s.first_name <> u.first_name OR
                         s.last_name <> u.last_name OR
                         s.street_address <> u.street_address OR
                         s.zip <> u.zip OR
                         s.city <> u.city OR
                         s.state <> u.state OR
                         s.updated <> u.updated
            THEN UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

# COMMAND ----------

query = (spark.readStream
              .table("bronze")
              .writeStream
              .foreachBatch(upsert_to_delta)
              .trigger(availableNow=True)
            #   .trigger(processingTime='5 seconds')
              .start())

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# DBTITLE 0,--i18n-9de94600-c28f-40bf-8d91-9850b3d5fe29
# MAGIC %md
# MAGIC
# MAGIC We have an additional metadata directory nested in our table directory, *_change_data*, where Databricks records change data for **`UPDATE`**, **`DELETE`**, and **`MERGE`** operations. Some operations, such as insert-only operations and full partition deletes, do not generate data in this directory because Databricks can efficiently compute the change data feed directly from the transaction log.
# MAGIC
# MAGIC If you do not see this directory present, it's likely because none of the applicable operations have been performed on the table yet; wait a moment and try again.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.user_db}/silver")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-c6633e4e-72f4-4176-9895-b26c8e508a33
# MAGIC %md
# MAGIC
# MAGIC We can see this directory also contains parquet files.
# MAGIC
# MAGIC **NOTE:** If this operation fails, it's likely because none of the applicable operations that populate the *_change_data* directory have been performed on the table yet; wait a moment and try again.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.user_db}/silver/_change_data")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-2dd98693-9b2e-4a44-99d0-36a8af63db73
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Read the Change Data Feed
# MAGIC
# MAGIC To pick up the recorded CDC data, we add two options:
# MAGIC - **`readChangeData`**
# MAGIC - **`startingVersion`** (can use **`startingTimestamp`** instead)
# MAGIC
# MAGIC Here we'll do a streaming display of just those patients in LA. Note that users with changes have two records present.

# COMMAND ----------

cdc_df = (spark.readStream
               .format("delta")
               .option("readChangeData", True)
               .option("startingVersion", 1)
               .table("silver"))

cdc_la_df = cdc_df.filter("city = 'Los Angeles'")

display(cdc_la_df, streamName = "display_la")
DA.block_until_stream_is_ready("display_la")

# COMMAND ----------

# DBTITLE 0,--i18n-22a0d58f-bffe-481b-8dc5-534e5bb9b32e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC If we land another file in our source directory and wait a few seconds, we'll see that we now have captured CDC changes for multiple **`_commit_version`** (change the sort order of the **`_commit_version`** column in the display above to see this).

# COMMAND ----------

DA.cdc_stream.load()

# COMMAND ----------

# DBTITLE 0,--i18n-7d1b89c8-2595-4998-ae17-4b79f2b94d97
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Gold Table
# MAGIC Our gold table will capture all of those patients that have a new address, and record this information alongside 2 timestamps: the time at which this change was made in our source system (currently labeled **`updated`**) and the time this was processed into our silver table (captured by the **`_commit_timestamp`** generated CDC field).
# MAGIC
# MAGIC Within silver table CDC records:
# MAGIC - check for max **`_commit_version`** for each record
# MAGIC - if new version and address change, insert to gold table
# MAGIC - record **`updated_timestamp`** and **`processed_timestamp`**
# MAGIC
# MAGIC #### Gold Table Schema
# MAGIC | field | type |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | new_street_address | string |
# MAGIC | new_zip | long |
# MAGIC | new_city | string |
# MAGIC | new_state | string |
# MAGIC | old_street_address | string |
# MAGIC | old_zip | long |
# MAGIC | old_city | string |
# MAGIC | old_state | string |
# MAGIC | updated_timestamp | timestamp |
# MAGIC | processed_timestamp | timestamp |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold (mrn BIGINT,
# MAGIC                    new_street_address STRING,
# MAGIC                    new_zip BIGINT,
# MAGIC                    new_city STRING,
# MAGIC                    new_state STRING,
# MAGIC                    old_street_address STRING,
# MAGIC                    old_zip BIGINT,
# MAGIC                    old_city STRING,
# MAGIC                    old_state STRING,
# MAGIC                    updated_timestamp TIMESTAMP,
# MAGIC                    processed_timestamp TIMESTAMP)
# MAGIC USING DELTA
# MAGIC LOCATION '${DA.paths.working_dir}/gold'

# COMMAND ----------

# DBTITLE 0,--i18n-cecf3978-5499-457d-894a-d74a636178aa
# MAGIC %md
# MAGIC
# MAGIC Note that we are using a table that has updates written to it as a streaming source! 
# MAGIC
# MAGIC This is a **huge** value add, and something that historically has required extensive workarounds to process correctly.

# COMMAND ----------

silver_stream_df = (spark.readStream
                         .format("delta")
                         .option("readChangeData", True)
                         .option("startingVersion", 1)
                         .table("silver"))

# COMMAND ----------

# DBTITLE 0,--i18n-0b8e09ff-357d-43f9-8be7-d76f3e374c42
# MAGIC %md
# MAGIC
# MAGIC Our **`_change_type`** field lets us easily distinguish valid and invalid records.
# MAGIC
# MAGIC New valid rows will have the **`update_postimage`** or **`insert`** label.
# MAGIC New invalid rows will have the **`update_preimage`** or **`delete`** label. 
# MAGIC
# MAGIC (**NOTE**: We'll demonstrate logic for propagating deletes a little later)
# MAGIC
# MAGIC In the cell below, we'll define two queries against our streaming source to perform a stream-stream merge on our data.

# COMMAND ----------

from pyspark.sql import functions as F

new_df = (silver_stream_df
         .filter(F.col("_change_type").isin(["update_postimage", "insert"]))
         .selectExpr("mrn",
                     "street_address AS new_street_address",
                     "zip AS new_zip",
                     "city AS new_city",
                     "state AS new_state",
                     "updated AS updated_timestamp",
                     "_commit_timestamp AS processed_timestamp"))
                                                                                         
old_df = (silver_stream_df
         .filter(F.col("_change_type").isin(["update_preimage"]))
         .selectExpr("mrn",
                     "street_address AS old_street_address",
                     "zip AS old_zip",
                     "city AS old_city",
                     "state AS old_state",
                     "_commit_timestamp AS processed_timestamp"))

# COMMAND ----------

# DBTITLE 0,--i18n-0c7699b6-ff6b-4f5d-bbb5-ad82fdb24c85
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Assuming that we have properly deduplicated our data to ensure that only a single record for our **`mrn`** can be processed to our silver table, **`mrn`** and **`_commit_timestamp`** (aliased to **`processed_timestamp`** here) serve as a unique composite key.
# MAGIC
# MAGIC Our join will allow us to match up the current and previous states of our data to track all changes.
# MAGIC
# MAGIC This table could drive further downstream processes, such as triggering confirmation emails or automatic mailings for patients with updated addresses.
# MAGIC
# MAGIC Our CDC data arrives as a stream, so only newly changed data at the silver level will be processed. Therefore, we can write to our gold table in append mode and maintain the grain of our data.

# COMMAND ----------

query = (new_df.withWatermark("processed_timestamp", "3 minutes")
               .join(old_df, ["mrn", "processed_timestamp"], "left")
               .filter("new_street_address <> old_street_address OR old_street_address IS NULL")
               .writeStream
               .outputMode("append")
               #.trigger(availableNow=True)
               .trigger(processingTime="5 seconds")
               .option("checkpointLocation", f"{DA.paths.checkpoints}/gold")
               .table("gold"))

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# DBTITLE 0,--i18n-14dd4498-67c5-4d89-bd83-9818d598a3a7
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Note the number of rows in our gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold

# COMMAND ----------

# DBTITLE 0,--i18n-d9a6673a-c8b3-431e-8ca7-ee434c143796
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC If we land a new raw file and wait a few seconds, we can see that all of our changes have propagated through our pipeline.
# MAGIC
# MAGIC (This assumes you're using **`processingTime`** instead of trigger-once or trigger-available-now processing. Scroll up to the gold table streaming write to wait for a new peak in the processing rate to know your data has arrived.)

# COMMAND ----------

DA.cdc_stream.load()

# COMMAND ----------

# DBTITLE 0,--i18n-3076b88b-41ef-42f3-9e65-54c047d3a03e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC You should be able to see a jump in the number of records in your gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold

# COMMAND ----------

# DBTITLE 0,--i18n-6b568178-c139-4b84-9f85-c7b5f7fea5d6
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Make sure to run the following cell to stop all active streams.

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()
    stream.awaitTermination()

# COMMAND ----------

# DBTITLE 0,--i18n-fc843897-a0dc-403e-9b14-f51824617516
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Propagating Deletes
# MAGIC
# MAGIC While some use cases may require processing deletes alongside updates and inserts, the most important delete requests are those that allow companies to maintain compliance with privacy regulations such as GDPR and CCPA. Most companies have stated SLAs around how long these requests will take to process, but for various reasons, these are often handled in pipelines separate from their core ETL.
# MAGIC
# MAGIC Here, we should see a single user being deleted from our **`silver`** table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM silver WHERE mrn = 14125426

# COMMAND ----------

# DBTITLE 0,--i18n-c0d98848-42cb-4e1e-9593-89b897539c60
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC As expected, when we try to locate this user in our **`silver`** table, we'll get no result.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver WHERE mrn = 14125426

# COMMAND ----------

# DBTITLE 0,--i18n-70ed8a79-e2e7-4ebd-85a5-7a83afc02869
# MAGIC %md
# MAGIC
# MAGIC This change has been captured in our Change Data Feed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes("silver", 3)
# MAGIC WHERE mrn = 14125426
# MAGIC ORDER BY _commit_version

# COMMAND ----------

# DBTITLE 0,--i18n-1a3303ed-e4d4-4c2f-affc-cac3478b1fd3
# MAGIC %md
# MAGIC
# MAGIC Because we have a record of this delete action, we can define logic that propagates deletes to our **`gold`** table.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deletes AS (
# MAGIC   SELECT mrn
# MAGIC   FROM table_changes("silver", 3)
# MAGIC   WHERE _change_type='delete'
# MAGIC )
# MAGIC
# MAGIC MERGE INTO gold g
# MAGIC USING deletes d
# MAGIC ON d.mrn=g.mrn
# MAGIC WHEN MATCHED
# MAGIC   THEN DELETE

# COMMAND ----------

# DBTITLE 0,--i18n-51cf7827-5c2f-4fd2-9afb-c27e2b11c527
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC This drastically simplifies deleting user data, and allows the keys and logic used in your ETL to also be used for propagating delete requests.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold WHERE mrn = 14125426

# COMMAND ----------

# DBTITLE 0,--i18n-a601d7c8-c5c8-4310-8f53-d23069877fa2
# MAGIC %md
# MAGIC
# MAGIC Run the following cell to delete the tables and files associated with this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
