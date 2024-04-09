# Databricks notebook source
# MAGIC %run ../../tests/generic_tests

# COMMAND ----------

env = dbutils.widgets.get("Environment")
app_id = dbutils.widgets.get("Client")
tb_list = dbutils.widgets.get("TargetTableName")

if(tb_list.length() == 0):
  trg_table_list = getTargetTableList(app_id)
else:
  trg_table_list = tb_list.split(',')

suiteStartTime = getCurrentTime()

for trg_tb in trg_table_list:
  
  if (trg_tb == 'bronze_candidate'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
    
    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime)

  if(trg_tb == 'bronze_folder'):
    print("Running test for bronze_folder")
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)
    
    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime)

 



