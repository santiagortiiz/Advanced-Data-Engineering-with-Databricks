# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-b5a53c91-1411-491a-bb3d-d07ed979b2b0
# MAGIC %md
# MAGIC # Creating a Pseudonymized PII Lookup Table
# MAGIC
# MAGIC In this lesson we'll create a pseudonymized key for storing potentially sensitive user data.  
# MAGIC Our approach in this notebook is fairly straightforward; some industries may require more elaborate de-identification to guarantee privacy.
# MAGIC
# MAGIC We'll examine design patterns for ensuring PII is stored securely and updated accurately. 
# MAGIC
# MAGIC ##### Objectives
# MAGIC - Describe the purpose of "salting" before hashing
# MAGIC - Apply salted hashing to sensitive data for pseudonymization

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

# DBTITLE 0,--i18n-d423fccb-8f92-44f8-bf1f-a65a0217a257
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Pseudonymize with salted hashing
# MAGIC
# MAGIC Create a function to register this logic to the current database under the name **`salted_hash`**. This will allow this logic to be called by any user with appropriate permissions on this function. 
# MAGIC
# MAGIC Note that it is theoretically possible to link the original key and pseudo-ID if the hash function and the salt are known. Here, we use this method to add a layer of obfuscation; in production, you may wish to have a much more sophisticated hashing method.

# COMMAND ----------

salt = "BEANS"
     
# Define function to pseudonymize with salted hashing    
def salted_hash(id):
    return F.sha2(F.concat(id, F.lit(salt)), 256)

# COMMAND ----------

# DBTITLE 0,--i18n-ff1789ab-419a-479a-bb7c-da46a2122e38
# MAGIC %md
# MAGIC
# MAGIC ## Create pseudonymized user lookup table
# MAGIC
# MAGIC The logic below creates the **`user_lookup`** table. In the next notebook, we'll use this pseudo-ID as the sole link to user PII. By controlling access to the link between our **`alt_id`** and other natural keys, we'll be able to prevent linking PII to other user data throughout our system.
# MAGIC
# MAGIC Use the function above to create the **`alt_id`** to the **`user_id`** from the **`registered_users`** table. Make sure to include all necessary columns for the target **`user_lookup`** table.

# COMMAND ----------

# Ingest data into the registered_users table incrementally with Auto Loader
@dlt.table
def registered_users():
    return (
        spark.readStream
            .format("cloudFiles")
            .schema("device_id LONG, mac_address STRING, registration_timestamp DOUBLE, user_id LONG")
            .option("cloudFiles.format", "json")
            .load(f"{source}/user_reg"))


# Create pseudonymized user lookup table
@dlt.table
def user_lookup():
    return (dlt.read_stream("registered_users")
              .select(
                  salted_hash(F.col("user_id")).alias("alt_id"),
                  "device_id", "mac_address", "user_id")
           )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
