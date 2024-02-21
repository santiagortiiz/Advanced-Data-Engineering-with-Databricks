# Databricks notebook source
# MAGIC %run ./Classroom-Setup-01-Common

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_1_1(self, df):
    suite = self.tests.new("1.3L-1.1")

    suite.test_true(lambda: df.isStreaming, description="The query is streaming")
    
    columns = ["device", "ecommerce", "event_name", "event_previous_timestamp", "event_timestamp", "geo", "items", "traffic_source", "user_first_touch_timestamp", "user_id"]
    suite.test_sequence(lambda: df.columns, 
                        expected_value=columns,
                        test_column_order=False,
                        description=f"DataFrame contains all {len(columns)} columns",
                        hint="Found [[ACTUAL_VALUE]]")
    
    suite.display_results()
    assert suite.passed, "One or more tests failed."

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_2_1(self, schema:StructType):
    suite = self.tests.new("1.3L-2.1")
    suite.test_equals(lambda: type(schema), 
                      expected_value=StructType, 
                      description="Schema is of type StructType",
                      hint="Found [[ACTUAL_VALUE]]")
    
    suite.test_length(lambda: schema.fieldNames(), 2, description="Schema contians two fields", hint="Found [[LEN_ACTUAL_VALUE]]: [[ACTUAL_VALUE]]")

    suite.test_schema_field(lambda: schema, "traffic_source", "StringType", None)
    suite.test_schema_field(lambda: schema, "active_users", "LongType", None)
    
    suite.display_results()
    assert suite.passed, "One or more tests failed."


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_4_1(self, query):
    suite = self.tests.new("1.3L-4.1")

    suite.test_true(lambda: query.isActive, description="The query is active")
    suite.test_equals(lambda: query.name, "active_users_by_traffic", description="The query name is \"active_users_by_traffic\".")
    suite.test_equals(lambda: query.lastProgress["sink"]["description"], "MemorySink", description="The format is \"MemorySink\".", hint="Found [[ACTUAL_VALUE]]")
    
    suite.display_results()
    assert suite.passed, "One or more tests failed."

# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_6_1(self, query):
    suite = self.tests.new("1.3L-6.1")

    suite.test_false(lambda: query.isActive, description="The query has been stopped")
    
    suite.display_results()
    assert suite.passed, "One or more tests failed."

