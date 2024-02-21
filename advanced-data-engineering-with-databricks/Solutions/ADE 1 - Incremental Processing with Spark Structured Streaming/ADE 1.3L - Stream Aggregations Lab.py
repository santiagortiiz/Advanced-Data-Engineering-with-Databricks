# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-433999da-5c3d-4858-b6c6-df9d4b5b5750
# MAGIC %md
# MAGIC
# MAGIC # Stream Aggregations Lab
# MAGIC
# MAGIC ### Activity by Traffic
# MAGIC
# MAGIC Process streaming data to display total active users by traffic source.
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Read data stream
# MAGIC 2. Get active users by traffic source
# MAGIC 3. Execute query with display() and plot results
# MAGIC 4. Execute the same streaming query with DataStreamWriter
# MAGIC 5. View results being updated in the query table
# MAGIC 6. List and stop all active streams
# MAGIC
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>

# COMMAND ----------

# DBTITLE 0,--i18n-782b481b-91f4-4599-b75c-13362cbb5cbe
# MAGIC %md
# MAGIC
# MAGIC ### Setup
# MAGIC Run the cells below to generate data and create the **`schema`** string needed for this lab.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01.3L

# COMMAND ----------

# DBTITLE 0,--i18n-0b92987e-b792-40b7-a651-9ce83bf38367
# MAGIC %md
# MAGIC
# MAGIC ### 1. Read data stream
# MAGIC - Set to process 1 file per trigger
# MAGIC - Read from Delta with filepath stored in **`DA.paths.events`**
# MAGIC
# MAGIC Assign the resulting Query to **`df`**.

# COMMAND ----------

# ANSWER
df = (spark.readStream
           .option("maxFilesPerTrigger", 1)
           .format("delta")
           .load(DA.paths.events))

# COMMAND ----------

# DBTITLE 0,--i18n-09723417-e88c-4952-acb0-a50b4b2aaa1b
# MAGIC %md
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_1_1(df)

# COMMAND ----------

# DBTITLE 0,--i18n-b2d1b7a7-5bdd-4775-8ac2-0a45910b4e45
# MAGIC %md
# MAGIC ### 2. Get active users by traffic source
# MAGIC - Set default shuffle partitions to number of cores on your cluster (not required, but runs faster)
# MAGIC - Group by **`traffic_source`**
# MAGIC   - Aggregate the approximate count of distinct users and alias with "active_users"
# MAGIC - Sort by **`traffic_source`**

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col, approx_count_distinct, count

spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

traffic_df = (df
              .groupBy("traffic_source")
              .agg(approx_count_distinct("user_id").alias("active_users"))
              .sort("traffic_source")
             )

# COMMAND ----------

# DBTITLE 0,--i18n-6ff40a67-649e-457a-bcb0-39ea8f28ca5c
# MAGIC %md
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_2_1(traffic_df.schema)

# COMMAND ----------

# DBTITLE 0,--i18n-cf8d69ae-1d80-4b00-a92f-e47a79b40cee
# MAGIC %md
# MAGIC
# MAGIC ### 3. Execute query with display() and plot results
# MAGIC - Execute results for **`traffic_df`** using display()
# MAGIC - Plot the streaming query results as a bar graph

# COMMAND ----------

# ANSWER
display(traffic_df)

# COMMAND ----------

# DBTITLE 0,--i18n-f4b716a0-6f7a-4727-8e13-4a5e56b2bbd0
# MAGIC %md
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**
# MAGIC - You bar chart should plot **`traffic_source`** on the x-axis and **`active_users`** on the y-axis
# MAGIC - The top three traffic sources in descending order should be **`google`**, **`facebook`**, and **`instagram`**.

# COMMAND ----------

# DBTITLE 0,--i18n-4fcb6e43-84bf-4e2d-a477-3dba9a647ee7
# MAGIC %md
# MAGIC ### 4. Execute the same streaming query with DataStreamWriter
# MAGIC - Name the query "active_users_by_traffic"
# MAGIC - Set to "memory" format and "complete" output mode
# MAGIC - Set a trigger interval of 1 second

# COMMAND ----------

# ANSWER
traffic_query = (traffic_df
                 .writeStream
                 .queryName("active_users_by_traffic")
                 .format("memory")
                 .outputMode("complete")
                 .trigger(processingTime="1 second")
                 .start())

DA.block_until_stream_is_ready("active_users_by_traffic")

# COMMAND ----------

# DBTITLE 0,--i18n-d098bb39-ca7d-48b7-853f-ecb5bbca888b
# MAGIC %md
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_4_1(traffic_query)

# COMMAND ----------

# DBTITLE 0,--i18n-8ca7e78b-216b-4384-a515-e57f0f0fa3c3
# MAGIC %md
# MAGIC ### 5. View results being updated in the query table
# MAGIC Run a query in a SQL cell to display the results from the **`active_users_by_traffic`** table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT * FROM active_users_by_traffic

# COMMAND ----------

# DBTITLE 0,--i18n-a03272d0-9ffd-49ff-9754-cee98e22232b
# MAGIC %md
# MAGIC
# MAGIC **5.1: CHECK YOUR WORK**
# MAGIC Your query should eventually result in the following values.
# MAGIC
# MAGIC |traffic_source|active_users|
# MAGIC |---|---|
# MAGIC |direct|438886|
# MAGIC |email|281525|
# MAGIC |facebook|956769|
# MAGIC |google|1781961|
# MAGIC |instagram|530050|
# MAGIC |youtube|253321|

# COMMAND ----------

# DBTITLE 0,--i18n-7a281d50-782a-4f52-afc9-f6f4b1bd4435
# MAGIC %md
# MAGIC ### 6. List and stop all active streams
# MAGIC - Use SparkSession to get list of all active streams
# MAGIC - Iterate over the list and stop each query

# COMMAND ----------

# ANSWER
for s in spark.streams.active:
    print(s.name)
    s.stop()

# COMMAND ----------

# DBTITLE 0,--i18n-82d33668-0827-482d-a973-8f5be6156d92
# MAGIC %md
# MAGIC
# MAGIC **6.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_6_1(traffic_query)

# COMMAND ----------

# DBTITLE 0,--i18n-90682bbd-1b19-4ac9-b115-a287e0337790
# MAGIC %md
# MAGIC
# MAGIC ### Classroom Cleanup
# MAGIC Run the cell below to clean up resources.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
