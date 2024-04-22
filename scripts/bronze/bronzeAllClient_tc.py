# Databricks notebook source
# MAGIC %run ../../tests/generic_tests

# COMMAND ----------

env = dbutils.widgets.get("Environment")
groupId = dbutils.widgets.get("group_id")
client_list = getClientListByGroup(groupId,env)

suiteStartTime = getCurrentTime()

for app_id in client_list:    
  trg_table_list = getTargetTableList(app_id,env)

  if(len(trg_table_list) > 0):
    for trg_tb in trg_table_list:

    # TB-1 TEST METHODS CALLING FOR BRONZE_APPLICATION
      if(trg_tb == 'bronze_application'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    # TB-2 TEST METHODS CALLING FOR BRONZE_AREAOFINTEREST_CANDIDATE
      if(trg_tb == 'bronze_areaofinterest_candidate'):
        for trg_col in getDuplicateTestTargetColumns(trg_tb):
          verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        for trg_col in getNullTestTargetColumns(trg_tb):  
          verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-3 TEST METHODS CALLING FOR BRONZE_ASSESSMENT
      if(trg_tb == 'bronze_assessment'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-4 TEST METHODS CALLING FOR BRONZE_ATTACHMENT
      if(trg_tb == 'bronze_attachment'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-5 TEST METHODS CALLING FOR BRONZE_CAMPAIGN
      if(trg_tb == 'bronze_campaign'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-6 TEST METHODS CALLING FOR BRONZE_CAMPAIGN_PERSONA_ACTIVITY
      if(trg_tb == 'bronze_campaign_persona_activity'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-7 TEST METHODS CALLING FOR BRONZE_CAMPAIGN_RULE
      if(trg_tb == 'bronze_campaign_rule'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-8 TEST METHODS CALLING FOR BRONZE_CAMPAIGN_TACTICS
      if(trg_tb == 'bronze_campaign_tactics'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-9 TEST METHODS CALLING FOR BRONZE_CANDIDATE
      if (trg_tb == 'bronze_candidate'):
        for trg_col in getDuplicateTestTargetColumns(trg_tb):
          verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
        for trg_col in getNullTestTargetColumns(trg_tb):  
          verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-10 TEST METHODS CALLING FOR BRONZE_CANDIDATE_DISPOSITION_LIST
      if(trg_tb == 'bronze_candidate_disposition_list'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-11 TEST METHODS CALLING FOR BRONZE_CANDIDATE_STATUS_LIST
      if(trg_tb == 'bronze_candidate_status_list'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-12 TEST METHODS CALLING FOR BRONZE_CANDIDATE_SUBSCRIPTION
      if(trg_tb == 'bronze_candidate_subscription'):
        for trg_col in getDuplicateTestTargetColumns(trg_tb):
          verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        for trg_col in getNullTestTargetColumns(trg_tb):  
          verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-13 TEST METHODS CALLING FOR BRONZE_COMMUNICATION_LOG
      if(trg_tb == 'bronze_communication_log'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-14 TEST METHODS CALLING FOR BRONZE_COMMUNICATION_TEMPLATE
      if(trg_tb == 'bronze_communication_template'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

    #TB-15 TEST METHODS CALLING FOR BRONZE_COMMUNICATION_TYPES
      if(trg_tb == 'bronze_communication_types'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-16 TEST METHODS CALLING FOR BRONZE_EVENT_STORE
      if(trg_tb == 'bronze_event_store'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

    #TB-17 TEST METHODS CALLING FOR BRONZE_EVENT_TYPE
      if(trg_tb == 'bronze_event_type'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-18 TEST METHODS CALLING FOR BRONZE_EVENT_WORKER
      if(trg_tb == 'bronze_event_worker'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

    #TB-19 TEST METHODS CALLING FOR BRONZE_EVENTS
      if(trg_tb == 'bronze_events'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-20 TEST METHODS CALLING FOR BRONZE_FOLDER
      if(trg_tb == 'bronze_folder'):
        for trg_col in getDuplicateTestTargetColumns(trg_tb):
          verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        for trg_col in getNullTestTargetColumns(trg_tb):  
          verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
        Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-21 TEST METHODS CALLING FOR BRONZE_FOLDER_CANDIDATE
      if(trg_tb == 'bronze_folder_candidates'):
        for trg_col in getDuplicateTestTargetColumns(trg_tb):
          verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        for trg_col in getNullTestTargetColumns(trg_tb):  
          verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-22 TEST METHODS CALLING FOR BRONZE_FOLDER_CANDIDATE_STATUS_LOG
      if(trg_tb == 'bronze_folder_candidates_status_log'):
        for trg_col in getDuplicateTestTargetColumns(trg_tb):
          verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        for trg_col in getNullTestTargetColumns(trg_tb):  
          verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-23 TEST METHODS CALLING FOR BRONZE_GLOBAL_STATUS_LOG
      if(trg_tb == 'bronze_global_status_log'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-24 TEST METHODS CALLING FOR BRONZE_GROUPS only have 2 columns
      if(trg_tb == 'bronze_groups'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

    #TB-25 TEST METHODS CALLING FOR BRONZE_HIERARCHY
      if(trg_tb == 'bronze_hierarchy'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

    #TB-26 TEST METHODS CALLING FOR BRONZE_INTEGRATION_EXCEPTION_LOG
      if(trg_tb == 'bronze_integration_exception_log'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-27 TEST METHODS CALLING FOR BRONZE_INTEGRATION_LOG
      if(trg_tb == 'bronze_integration_log'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-28 TEST METHODS CALLING FOR BRONZE_INTERVIEW
      if(trg_tb == 'bronze_interview'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-29 TEST METHODS CALLING FOR BRONZE_JOBALERT_CLIENT_LOG
      if(trg_tb == 'bronze_jobalert_client_log'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)
    
    #TB-30 TEST METHODS CALLING FOR BRONZE_LIST
      if(trg_tb == 'bronze_list'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

    #TB-31 TEST METHODS CALLING FOR BRONZE_REFERRAL
      if(trg_tb == 'bronze_referral'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-32 TEST METHODS CALLING FOR BRONZE_REQUISITION
      if(trg_tb == 'bronze_requisition'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-33 TEST METHODS CALLING FOR BRONZE_SENDGRID_EVENT
      if(trg_tb == 'bronze_sendgrid_event'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-34 TEST METHODS CALLING FOR BRONZE_SPONSOR_INFO
      if(trg_tb == 'bronze_sponsor_info'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-35 TEST METHODS CALLING FOR BRONZE_TACTIC_TYPE
      if(trg_tb == 'bronze_tactic_type'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-36 TEST METHODS CALLING FOR BRONZE_TAGS
      if(trg_tb == 'bronze_tags'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-37 TEST METHODS CALLING FOR BRONZE_TAGS_CANDIDATE
      if(trg_tb == 'bronze_tags_candidate'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-38 TEST METHODS CALLING FOR BRONZE_UNIFIED_CAMPAIGN
      if(trg_tb == 'bronze_unified_campaign'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-39 TEST METHODS CALLING FOR BRONZE_USER_LOGINS
      if(trg_tb == 'bronze_user_logins'):
        for trg_col in getDuplicateTestTargetColumns(trg_tb):
          verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        for trg_col in getNullTestTargetColumns(trg_tb):  
          verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

    #TB-40 TEST METHODS CALLING FOR BRONZE_USER_SEARCHES
      if(trg_tb == 'bronze_user_searches'):
        for trg_col in getDuplicateTestTargetColumns(trg_tb):
          verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        for trg_col in getNullTestTargetColumns(trg_tb):  
          verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

    #TB-41 TEST METHODS CALLING FOR BRONZE_USERS
      if(trg_tb == 'bronze_users'):
        for trg_col in getDuplicateTestTargetColumns(trg_tb):
          verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        for trg_col in getNullTestTargetColumns(trg_tb):  
          verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
        Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

    #TB-42 TEST METHODS CALLING FOR BRONZE_WORK_HISTORY
      if(trg_tb == 'bronze_work_history'):
        for trg_col in getDuplicateTestTargetColumns(trg_tb):
          verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        for trg_col in getNullTestTargetColumns(trg_tb):  
          verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

        Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb)) 