# Databricks notebook source
# MAGIC %run ../../tests/generic_tests

# COMMAND ----------

env = dbutils.widgets.get("Environment")
app_id = dbutils.widgets.get("Client")
tb_list = dbutils.widgets.get("TargetTableName")

# env = "cert"
# app_id = "xcloud"
# tb_list = "bronze_candidate,bronze_folder,bronze_user_logins,bronze_users,bronze_folder_candidates,bronze_user_searches,bronze_folder_candidates_status_log"

if(len(tb_list) == 0):
  trg_table_list = getTargetTableList(app_id)
else:
  trg_table_list = tb_list.split(',')

suiteStartTime = getCurrentTime()

for trg_tb in trg_table_list:
  
  # verifying table BRONZE_CANDIDATE
  if (trg_tb == 'bronze_candidate'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
    
    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime)

  # verifying table BRONZE_FOLDER
  if(trg_tb == 'bronze_folder'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)
    
    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime)

  # verifying table BRONZE_USER_LOGINS
  if(trg_tb == 'bronze_user_logins'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime)

  # verifying table BRONZE_USERS
  if(trg_tb == 'bronze_users'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)
    
    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime)

  # verifying table BRONZE_FOLDER_CANDIDATE
  if(trg_tb == 'bronze_folder_candidates'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime)

  # verifying table BRONZE_USER_SEARCHES
  if(trg_tb == 'bronze_user_searches'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime)

  # verifying table BRONZE_FOLDER_CANDIDATE_STATUS_LOG
  if(trg_tb == 'bronze_folder_candidates_status_log'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime)
    

 



