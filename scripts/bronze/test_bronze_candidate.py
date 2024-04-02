# Databricks notebook source
# MAGIC %run ../../tests/generic_tests

# COMMAND ----------

env = "cert"
app_id = "xcloud"
table_name = "bronze_candidate"
trg_col = "candidate_id"

print("Test status for duplicate_test :" + verify_duplicate_records(env, app_id, table_name,trg_col))
print("Test status for null_test :"+ verify_null_records(env, app_id, table_name,trg_col))
# print(insert_query())
# verify_null_records(env, app_id, table_name)

# COMMAND ----------

env = "cert"
app_id = "xcloud"
table_list = "bronze_candidate"
trg_col = "candidate_id"
table_dict: {'bronze_candidate': {'duplicate': ['candidate_id','user'], 'null': ['candidate_id','user']}}
print("Test status for duplicate_test :" + verify_duplicate_records(env, app_id, table_name,trg_col))
print("Test status for null_test :"+ verify_null_records(env, app_id, table_name,trg_col))
# verify_null_records(env, app_id, table_name)

# COMMAND ----------


# Rest of your code
dbutils.notebook.run("/Workspace/saurav/generic_test/duplicate",60)
from datetime import datetime

env = "cert"
app_id = "xcloud"
table_name = "bronze_candidate"
target_col_dict = {"candidate_id": "string"} 
target_columns = ", ".join(target_col_dict.values())
print(target_columns)

test_status = verify_duplicate_records(env, app_id, table_name)

print("Test Status:", test_status)