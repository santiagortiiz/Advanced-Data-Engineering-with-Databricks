# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-d6d37384-055d-4f2e-85c6-7e10efe30a02
# MAGIC %md
# MAGIC
# MAGIC # Streaming Query Lab
# MAGIC ### Coupon Sales
# MAGIC
# MAGIC Process and append streaming data on transactions using coupons.
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Read data stream
# MAGIC 2. Filter for transactions with coupons codes
# MAGIC 3. Write streaming query results to Delta
# MAGIC 4. Monitor streaming query
# MAGIC 5. Stop streaming query
# MAGIC
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01.2L

# COMMAND ----------

# DBTITLE 0,--i18n-3b34e740-2ed7-4f5f-88ad-6fca87aaaa1d
# MAGIC %md
# MAGIC
# MAGIC ### 1. Read data stream
# MAGIC - Set to process 1 file per trigger
# MAGIC - Read from Delta files in the source directory specified by **`DA.paths.sales`**
# MAGIC
# MAGIC Assign the resulting DataFrame to **`df`**.

# COMMAND ----------

# ANSWER
df = (spark
      .readStream
      .option("maxFilesPerTrigger", 1)
      .format("delta")
      .load(DA.paths.sales)
     )

# COMMAND ----------

# DBTITLE 0,--i18n-4cb5da45-01a7-4207-a24e-e23b7d562810
# MAGIC %md
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_1_1(df)

# COMMAND ----------

# DBTITLE 0,--i18n-0f9d73d7-334d-4589-8233-318a49bd036c
# MAGIC %md
# MAGIC ### 2. Filter for transactions with coupon codes
# MAGIC - Explode the **`items`** field in **`df`** with the results replacing the existing **`items`** field
# MAGIC - Filter for records where **`items.coupon`** is not null
# MAGIC
# MAGIC Assign the resulting DataFrame to **`coupon_sales_df`**.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col, explode

coupon_sales_df = (df
                   .withColumn("items", explode(col("items")))
                   .filter(col("items.coupon").isNotNull())
                  )

# COMMAND ----------

# DBTITLE 0,--i18n-23e5b714-2529-4736-b2e4-e1988b9a7dff
# MAGIC %md
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_2_1(coupon_sales_df.schema)

# COMMAND ----------

# DBTITLE 0,--i18n-4f329dea-d9df-44d9-932d-10314b5e4c96
# MAGIC %md
# MAGIC
# MAGIC ### 3. Write streaming query results to Delta
# MAGIC - Configure the streaming query to write Delta format files in "append" mode
# MAGIC - Set the query name to "coupon_sales"
# MAGIC - Set a trigger interval of 1 second
# MAGIC - Set the checkpoint location to **`coupons_checkpoint_path`**
# MAGIC - Set the output path to **`coupons_output_path`**
# MAGIC
# MAGIC Start the streaming query and assign the resulting handle to **`coupon_sales_query`**.

# COMMAND ----------

# ANSWER

coupons_checkpoint_path = f"{DA.paths.checkpoints}/coupon-sales"
coupons_output_path = f"{DA.paths.working_dir}/coupon-sales/output"

coupon_sales_query = (coupon_sales_df
                      .writeStream
                      .outputMode("append")
                      .format("delta")
                      .queryName("coupon_sales")
                      .trigger(processingTime="1 second")
                      .option("checkpointLocation", coupons_checkpoint_path)
                      .start(coupons_output_path))

DA.block_until_stream_is_ready(coupon_sales_query)

# COMMAND ----------

# DBTITLE 0,--i18n-302883bf-6af9-45dd-aa36-27cedfcaa7da
# MAGIC %md
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_3_1(coupon_sales_query)

# COMMAND ----------

# DBTITLE 0,--i18n-de8aeae4-7c03-453d-b0ad-dcc4ba2a5b29
# MAGIC %md
# MAGIC ### 4. Monitor streaming query
# MAGIC - Get the ID of streaming query and store it in **`queryID`**
# MAGIC - Get the status of streaming query and store it in **`queryStatus`**

# COMMAND ----------

# ANSWER
query_id = coupon_sales_query.id
print(query_id)

# COMMAND ----------

# ANSWER
query_status = coupon_sales_query.status
print(query_status)

# COMMAND ----------

# DBTITLE 0,--i18n-b6d83075-3642-4a3a-b0ba-e5aae8b679b0
# MAGIC %md
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_4_1(query_id, query_status)

# COMMAND ----------

# DBTITLE 0,--i18n-f4defcd9-b83d-44ea-966a-a465508699cd
# MAGIC %md
# MAGIC
# MAGIC ### 5. Stop streaming query
# MAGIC - Stop the streaming query

# COMMAND ----------

# ANSWER
coupon_sales_query.stop()
coupon_sales_query.awaitTermination()

# COMMAND ----------

# DBTITLE 0,--i18n-f01e86e8-256a-471f-9cf6-15551538be43
# MAGIC %md
# MAGIC
# MAGIC **5.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_5_1(coupon_sales_query)

# COMMAND ----------

# DBTITLE 0,--i18n-df733595-d0de-4db2-a661-58e2cf53ac44
# MAGIC %md
# MAGIC ### 6. Verify the records were written in Delta format

# COMMAND ----------

# ANSWER
display(spark.read.format("delta").load(coupons_output_path))

# COMMAND ----------

# DBTITLE 0,--i18n-fd47da89-8f50-4d87-875a-080d6dc4e9a8
# MAGIC %md
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
