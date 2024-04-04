# Databricks notebook source
# MAGIC %run ../../tests/generic_tests

# COMMAND ----------

env = dbutils.widgets.get("Environment")
app_id = dbutils.widgets.get("Client")
trg_tb = dbutils.widgets.get("TargetTableName")
# src_tb = dbutils.widget.get("SourceTableName")

# env = "cert"
# app_id = "xcloud"
# trg_tb = "bronze_candidate"

for trg_col in getDuplicateTestTargetColumns(trg_tb):
  verify_duplicate_records(env, app_id, trg_tb,trg_col)

for trg_col in getNullTestTargetColumns(trg_tb):  
  verify_null_records(env, app_id, trg_tb,trg_col)


# COMMAND ----------

env = dbutils.widgets.get("Environment")
app_id = dbutils.widgets.get("Client")
trg_tb = dbutils.widgets.get("TargetTableName")
# src_tb = dbutils.widget.get("SourceTableName")

# env = "cert"
# app_id = "xcloud"
# trg_tb = "bronze_candidate"

# for trg_col in getDuplicateTestTargetColumns(trg_tb):
#   verify_duplicate_records(env, app_id, trg_tb,trg_col)

# for trg_col in getNullTestTargetColumns(trg_tb):  
#   verify_null_records(env, app_id, trg_tb,trg_col)



# COMMAND ----------

# env = dbutils.widgets.get("Environment")
query = f"""
    select distinct app_id from cert.sfx_analytics.bronze_load_config where app_id='xcloud'
"""
print(query)
result_df = spark.sql(query)
appId_list = [row.app_id for row in result_df.collect()]
# print(appId_list)

# print(env = dbutils.widgets.get("env"))

for id in appId_list:
    print(id)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select source_tablename,load_type,target from cert.sfx_analytics.bronze_load_config where app_id = 'xcloud'

# COMMAND ----------

# from datetime import datetime, timedelta
# ten_days_ago = datetime.now() - timedelta(days=10)
# formatted_date = ten_days_ago.strftime('%Y-%m-%d')
# deleteQuery = f"DELETE FROM test_output_table WHERE date_column < '{formatted_date}'"
# spark.sql(deleteQuery)
# print("Data older than 10 days has been successfully deleted.")