# Databricks notebook source
# MAGIC %run ./Classroom-Setup-06-Common

# COMMAND ----------

LESSON = "generate_tokens"
DA = init_DA(LESSON, reset=False)

@DBAcademyHelper.monkey_patch
def create_credentials_file(self, db_token, db_instance):
    contents = f"""
db_token = "{db_token}"
db_instance = "{db_instance}"
    """
    self.write_to_file(contents, "credentials.py")

None

