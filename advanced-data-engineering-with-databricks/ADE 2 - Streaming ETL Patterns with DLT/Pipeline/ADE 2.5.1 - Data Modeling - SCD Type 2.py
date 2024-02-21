# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-e452f682-7fac-409c-bf32-b789eef61b18
# MAGIC %md
# MAGIC
# MAGIC # Data Modeling: SCD Type 2
# MAGIC ## Processing CDC

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

users_schema = "user_id LONG, update_type STRING, timestamp FLOAT, dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING, address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>"    

@dlt.table(
    table_properties={"quality": "bronze"}
)
def users_cdc_bronze():
    return (
        dlt.read_stream("bronze")
          .filter("topic = 'user_info'")
          .select(F.from_json(F.col("value").cast("string"), users_schema).alias("v"))
          .select("v.*")
    )

# COMMAND ----------

rules = {
  "valid_user_id": "user_id IS NOT NULL",
  "valid_operation": "update_type IS NOT NULL"
}

@dlt.table(
    table_properties={"quality":"bronze"}
)
@dlt.expect_all_or_drop(rules)
def users_cdc_clean():
    return (
        dlt.read_stream("users_cdc_bronze")        
            .select(
                # Adding a pseudonymized key to incremental workloads is as simple as adding a transformation.
#                 F.sha2(F.concat(F.col("user_id"), F.lit(salt)), 256).alias("alt_id"),
                F.col("user_id"),              
                F.col("timestamp").cast("timestamp").alias("updated"),
                F.to_date("dob", "MM/dd/yyyy").alias("dob"), 
                "sex", "gender", "first_name", "last_name", "address.*", "update_type")
    )

quarantine_rules = {}
quarantine_rules["invalid_record"] = f"NOT({' AND '.join(rules.values())})"

@dlt.table
@dlt.expect_all_or_drop(quarantine_rules)
def users_cdc_quarantine():
    return (
        dlt.read_stream("users_cdc_bronze")
        )    

# COMMAND ----------

# Process user updates from CDC feed
dlt.create_streaming_live_table(
  name="users_silver", 
  table_properties={"quality": "silver"}
)
dlt.apply_changes(
  target = "users_silver", 
  source = "users_cdc_clean",
  keys = ["user_id"], 
  sequence_by = F.col("updated"),
  apply_as_deletes = F.expr("update_type = 'delete'"),
  except_column_list = ["update_type"]
)


dlt.create_streaming_live_table(
  name="SCD2_users", 
  table_properties={"quality": "silver"}
)
dlt.apply_changes(
  target = "SCD2_users", 
  source = "users_cdc_clean",
  keys = ["user_id"],
  sequence_by = F.col("updated"),
  apply_as_deletes = F.expr("update_type = 'delete'"),
  except_column_list = ["update_type"],
  stored_as_scd_type = "2" #Enable SCD2 and store individual updates
)

# COMMAND ----------

rules = {
  "pk_must_be_unique": "number_of_records_for_key = 1"
}

@dlt.table(
    comment="Check that users table only contains unique user id"
)
@dlt.expect_all_or_fail(rules)
def unique_user_id():
    return spark.sql("""
      SELECT user_id, count(*) AS number_of_records_for_key
      FROM LIVE.users_silver
      GROUP BY user_id
    """)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
