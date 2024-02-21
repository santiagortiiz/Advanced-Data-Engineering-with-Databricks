# Databricks notebook source
import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 0,--i18n-dfbcb403-6b85-4444-8b42-808ccd8d14e9
# MAGIC %md
# MAGIC ## Validate Bronze Tables with Portable Expectations

# COMMAND ----------

lookup_db = spark.conf.get("lookup_db")

def get_rules(topic):
    df = spark.read.table(f"{lookup_db}.rules").filter(F.col("topic") == topic)
    rules = {}
    for row in df.collect(): 
        rules[row["name"]] = row["condition"]
    return rules

# COMMAND ----------

def create_validated_views(dataset, topic, source_table, valid_view, invalid_view):
    rules = get_rules(topic)
    quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))
    
    @dlt.table(
        name=f"{dataset}_quarantine",
        # temporary=True, 
        partition_cols=["is_quarantined"]
    )
    @dlt.expect_all(rules)
    def create_quarantine():
        return dlt.read_stream(source_table).withColumn("is_quarantined", F.expr(quarantine_rules))
        
    @dlt.view(name=f"{valid_view}")
    def create_valid():
        return dlt.read_stream(f"{dataset}_quarantine").filter("is_quarantined=false")
    
    @dlt.view(name=f"{invalid_view}")
    def create_invalid():
        return dlt.read_stream(f"{dataset}_quarantine").filter("is_quarantined=true")    

# COMMAND ----------

quarantine_tables_config = {
    "bpm": { 
      "rules_tag": "bpm",
      "source": "bpm_bronze",
      "valid_view": "valid_bpm",
      "invalid_view": "invalid_bpm"
    },
    "workouts": { 
      "rules_tag": "workout",
      "source": "workouts_bronze",
      "valid_view": "valid_workouts",
      "invalid_view": "invalid_workouts"
    },
    "users_cdc": { 
      "rules_tag": "user_info",
      "source": "users_cdc_bronze",
      "valid_view": "valid_users_cdc",
      "invalid_view": "invalid_users_cdc"
    }
} 

for dataset, c in quarantine_tables_config.items():
    create_validated_views(dataset, c["rules_tag"], c["source"], c["valid_view"], c["invalid_view"])

