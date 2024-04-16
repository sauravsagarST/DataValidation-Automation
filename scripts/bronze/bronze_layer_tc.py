# Databricks notebook source
# MAGIC %run ../../tests/generic_tests

# COMMAND ----------

env = dbutils.widgets.get("Environment")
app_id = dbutils.widgets.get("Client")
dboard = dbutils.widgets.get("Dashboard")
trg_table_list = getDashboardTables(dboard)

# env = "prod"
# app_id = "xcloud"
# trg_table_list = getDashboardTables('userinsight')
# trg_table_list = getDashboardTables('talentnetwork')



suiteStartTime = getCurrentTime()

for trg_tb in trg_table_list:
  
  # verifying table BRONZE_CANDIDATE
  if (trg_tb == 'bronze_candidate'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
    
    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'update_date')

  # verifying table BRONZE_FOLDER
  if(trg_tb == 'bronze_folder'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)
    
    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'updated')

  # verifying table BRONZE_USER_LOGINS
  if(trg_tb == 'bronze_user_logins'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'date')

  # verifying table BRONZE_USERS
  if(trg_tb == 'bronze_users'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)
    
    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'updated')

  # verifying table BRONZE_FOLDER_CANDIDATE
  if(trg_tb == 'bronze_folder_candidates'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'updated')

  # verifying table BRONZE_USER_SEARCHES
  if(trg_tb == 'bronze_user_searches'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'search_datetime')

  # verifying table BRONZE_FOLDER_CANDIDATE_STATUS_LOG
  if(trg_tb == 'bronze_folder_candidates_status_log'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'created_at')

  if(trg_tb == 'bronze_work_history'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'updated')

  if(trg_tb == 'bronze_attachment'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'create_date')

  if(trg_tb == 'bronze_communication_types'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'updated_date')

  if(trg_tb == 'bronze_candidate_subscription'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'updated_date')
  
  if(trg_tb == 'bronze_areaofinterest_candidate'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'modified_on')

  if(trg_tb == 'bronze_tags'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'updated')

  if(trg_tb == 'bronze_tags_candidate'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,'modified_on')

 