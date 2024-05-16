# Databricks notebook source
# MAGIC %run ../../tests/generic_tests

# COMMAND ----------

# MAGIC %run ../../tests/cwsDashboard_tests

# COMMAND ----------

# MAGIC %run ../../tests/contactMessagesDashboard_tests

# COMMAND ----------

env = dbutils.widgets.get("Environment")
app_id = dbutils.widgets.get("Client")
dboard = dbutils.widgets.get("Dashboard")
trg_table_list = getDashboardTables(dboard)

# env = "prod"
# app_id = "clevelandclinic"
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

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  # verifying table BRONZE_FOLDER
  if(trg_tb == 'bronze_folder'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)
    
    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  # verifying table BRONZE_USER_LOGINS
  if(trg_tb == 'bronze_user_logins'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

  # verifying table BRONZE_USERS
  if(trg_tb == 'bronze_users'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)
    
    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  # verifying table BRONZE_FOLDER_CANDIDATE
  if(trg_tb == 'bronze_folder_candidates'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  # verifying table BRONZE_USER_SEARCHES
  if(trg_tb == 'bronze_user_searches'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

  # verifying table BRONZE_FOLDER_CANDIDATE_STATUS_LOG
  if(trg_tb == 'bronze_folder_candidates_status_log'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  if(trg_tb == 'bronze_work_history'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  if(trg_tb == 'bronze_attachment'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  if(trg_tb == 'bronze_communication_types'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  if(trg_tb == 'bronze_candidate_subscription'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))
  
  if(trg_tb == 'bronze_areaofinterest_candidate'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  if(trg_tb == 'bronze_tags'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  if(trg_tb == 'bronze_tags_candidate'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    

  if(googleAnalytics_test_flag == "true" and trg_tb == 'bronze_ga_content'):
    googleAnalytics_GaContent_RowCount(env,app_id,trg_tb,suiteStartTime)

  if(googleAnalytics_test_flag == "true" and trg_tb == 'bronze_ga_location'):
    googleAnalytics_GaLocation_RowCount(env,app_id,trg_tb,suiteStartTime)
  
  if(googleAnalytics_test_flag == "true" and trg_tb == 'bronze_ga_sources'):
    googleAnalytics_GaSources_RowCount(env,app_id,trg_tb,suiteStartTime)
  
  if(googleAnalytics_test_flag == "true" and trg_tb == 'bronze_ga_visitors'):
    googleAnalytics_GaVisitors_RowCount(env,app_id,trg_tb,suiteStartTime)

  if(trg_tb == 'bronze_ga4_content'):
    googleAnalytics4_Ga4_Content_RowCount(env,app_id,trg_tb,suiteStartTime)
    
  if(trg_tb == 'bronze_ga4_location'):
    googleAnalytics4_Ga4_Location_RowCount(env,app_id,trg_tb,suiteStartTime)
    
  if(trg_tb == 'bronze_ga4_sources'):
    googleAnalytics4_Ga4_Sources_RowCount(env,app_id,trg_tb,suiteStartTime)

  if(trg_tb == 'bronze_ga4_visitors'):
    googleAnalytics4_Ga4_Visitors_RowCount(env,app_id,trg_tb,suiteStartTime)




  if(trg_tb == 'bronze_sms_campaign_data'):
    new_trg_tb = trg_tb+"_"+app_id
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, new_trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, new_trg_tb,trg_col,suiteStartTime)

    sms_campaign_analytics_RowCount(env,app_id,new_trg_tb,suiteStartTime)

  if(trg_tb == 'bronze_sendgrid_event'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  if(trg_tb == 'bronze_communication_log'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  if(trg_tb == 'bronze_communication_template'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  if(trg_tb == 'bronze_campaign'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  if(trg_tb == 'bronze_campaign_rule'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

  if(trg_tb == 'bronze_jobalert_client_log'):
    for trg_col in getDuplicateTestTargetColumns(trg_tb):
      verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    for trg_col in getNullTestTargetColumns(trg_tb):  
      verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

    Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))





 



