# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-7cdec4a6-3abf-4813-b9e4-9ef8f027f95d
# MAGIC %md
# MAGIC # Pseudonymized ETL
# MAGIC
# MAGIC We will process user updates from a CDC feed, applying a transformation to add a pseudonymized key to our incremental workloads.
# MAGIC
# MAGIC ##### Objectives
# MAGIC - Apply incremental transformations to store data with pseudonymized keys

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")
lookup_db = spark.conf.get("lookup_db")

# COMMAND ----------

# DBTITLE 0,--i18n-cc3212d6-d107-4ec5-a96f-31ffc207a0e6
# MAGIC %md
# MAGIC
# MAGIC ## Set up bronze table

# COMMAND ----------

@dlt.table
def date_lookup():
    return spark.read.table(f"{lookup_db}.date_lookup").select("date", "week_part")


@dlt.table(
    partition_cols=["topic", "week_part"],
    table_properties={"quality": "bronze"}
)
def bronze():
    return (
      spark.readStream
        .format("cloudFiles")
        .schema("key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG")
        .option("cloudFiles.format", "json")
        .load(f"{source}/daily")
        .join(
          F.broadcast(dlt.read("date_lookup")), 
          F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date"), "left")
    )

        
users_schema = "user_id LONG, update_type STRING, timestamp FLOAT, dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING, address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>"    

@dlt.table(
    table_properties={"quality": "bronze"}
)
def users_bronze():
    return (
        dlt.read_stream("bronze")
          .filter("topic = 'user_info'")
          .select(F.from_json(F.col("value").cast("string"), users_schema).alias("v"))
          .select("v.*")
    )

# COMMAND ----------

# DBTITLE 0,--i18n-c011ab05-f011-4f70-95a1-ec160eb12d5e
# MAGIC %md
# MAGIC
# MAGIC ## Pseudonymize user data in ELT pipeline
# MAGIC
# MAGIC The data in the **`user_info`** topic contains complete row outputs from a Change Data Capture feed. There are three values for **`update_type`** present in the data: **`new`**, **`update`**, and **`delete`**. The **`users`** table will be implemented as a Type 1 table, so only the most recent value matters.

# COMMAND ----------

salt = "BEANS"
     
# Define function to pseudonymize with salted hashing    
def salted_hash(id):
    return F.sha2(F.concat(id, F.lit(salt)), 256)

# COMMAND ----------

rules = {
  "valid_user_id": "user_id IS NOT NULL",
  "valid_operation": "update_type IS NOT NULL"
}

# Pseudonymize user data in ELT pipeline
@dlt.table(
    table_properties={
#         "pipelines.reset.allowed":"false",
        "quality":"bronze"
    }
)
# @dlt.expect_all_or_drop(rules)
def users_cdc_clean():
    return (
        dlt.read_stream("users_bronze")        
            .select(
                # Adding a pseudonymized key to incremental workloads is as simple as adding a transformation.
                salted_hash(F.col("user_id")).alias("alt_id"),
                F.col("timestamp").cast("timestamp").alias("updated"),
                F.to_date("dob", "MM/dd/yyyy").alias("dob"), 
                "sex", "gender", "first_name", "last_name", "address.*", "update_type")
    )    
    
# Process user updates from CDC feed        
dlt.create_streaming_live_table(
    name = "users")

dlt.apply_changes(
    target = "users",
    source = "users_cdc_clean",
    keys = ["alt_id"],
    sequence_by = F.col("updated"),
    apply_as_deletes = F.expr("update_type = 'delete'"),
    except_column_list = ["update_type"]
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
