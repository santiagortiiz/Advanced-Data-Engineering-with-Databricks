# Databricks notebook source
# DBTITLE 0,--i18n-8ff65717-989b-4890-b9cc-15c163af753c
# MAGIC %md
# MAGIC
# MAGIC ## Users Silver Updates

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

lookup_db = spark.conf.get("lookup_db")

def get_rules(topic):
    df = spark.read.table(f"{lookup_db}.rules").filter(F.col("topic") == topic)
    rules = {}
    for row in df.collect(): 
        rules[row["name"]] = row["condition"]
    return rules


salt = "BEANS"
   
def salted_hash(id):
    return F.sha2(F.concat(id, F.lit(salt)), 256)   

# COMMAND ----------

# Pseudonymize/clean users cdc data

@dlt.table(table_properties={"quality":"bronze"})
def users_cdc_clean():
    return (
        dlt.read_stream("valid_users_cdc").select(
            salted_hash(F.col("user_id")).alias("alt_id"),
            F.col("timestamp").cast("timestamp").alias("updated"),
            F.to_date("dob", "MM/dd/yyyy").alias("dob"), 
            "sex", "gender", "first_name", "last_name", "address.*", "update_type")
            .withWatermark("updated", "30 seconds")
            .dropDuplicates(["alt_id", "updated"])
    )

# Process user updates from CDC feed
dlt.create_streaming_live_table(
    name="users_silver", 
    table_properties={"quality": "silver"}
)
dlt.apply_changes(
    target = "users_silver",
    source = "users_cdc_clean",
    keys = ["alt_id"],
    sequence_by = F.col("updated"),
    apply_as_deletes = F.expr("update_type = 'delete'"),
    except_column_list = ["update_type"]
)

# SCD Type 2
dlt.create_streaming_live_table(
  name="SCD2_users", 
  table_properties={"quality": "silver"}
)
dlt.apply_changes(
  target = "SCD2_users", 
  source = "users_cdc_clean",
  keys = ["alt_id"],
  sequence_by = F.col("updated"),
  apply_as_deletes = F.expr("update_type = 'delete'"),
  except_column_list = ["update_type"],
  stored_as_scd_type = "2" #Enable SCD2 and store individual updates
)


@dlt.table
def user_lookup():
    return spark.read.table(f"{lookup_db}.user_lookup")

            
# user bins
def age_bins(dob_col):
    age_col = F.floor(F.months_between(F.current_date(), dob_col) / 12).alias("age")
    return (
        F.when((age_col < 18), "under 18")
        .when((age_col >= 18) & (age_col < 25), "18-25")
        .when((age_col >= 25) & (age_col < 35), "25-35")
        .when((age_col >= 35) & (age_col < 45), "35-45")
        .when((age_col >= 45) & (age_col < 55), "45-55")
        .when((age_col >= 55) & (age_col < 65), "55-65")
        .when((age_col >= 65) & (age_col < 75), "65-75")
        .when((age_col >= 75) & (age_col < 85), "75-85")
        .when((age_col >= 85) & (age_col < 95), "85-95")
        .when((age_col >= 95), "95+")
        .otherwise("invalid age")
        .alias("age")
    )

@dlt.table
def user_bins():
    return (
        dlt.read("users_silver")
        .join(dlt.read("user_lookup").select("alt_id", "user_id"), ["alt_id"], "left")
        .select("user_id", age_bins(F.col("dob")), "gender", "city", "state")
    )                

