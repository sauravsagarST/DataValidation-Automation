# Databricks notebook source
# MAGIC %run ../../tests/generic_tests

# COMMAND ----------

env = dbutils.widgets.get("Environment")
groupId = dbutils.widgets.get("group_id")
runType = dbutils.widgets.get("RunType")
client_list = getClientListByGroup(groupId,env)

suiteStartTime = getCurrentTime()

for app_id in client_list:  
  clientCount = 0
  clientCount = clientCount + 1
  print(clientCount, ". Running Test Cases for Client: "+ app_id)  
  trg_table_list = getTargetTableList(app_id,env)

  if(len(trg_table_list) > 0):
    for trg_tb in trg_table_list:

    # TB-1 TEST METHODS CALLING FOR BRONZE_APPLICATION
      if(trg_tb == 'bronze_application'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")


    # TB-2 TEST METHODS CALLING FOR BRONZE_AREAOFINTEREST_CANDIDATE
      if(trg_tb == 'bronze_areaofinterest_candidate'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-3 TEST METHODS CALLING FOR BRONZE_ASSESSMENT
      if(trg_tb == 'bronze_assessment'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-4 TEST METHODS CALLING FOR BRONZE_ATTACHMENT
      if(trg_tb == 'bronze_attachment'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-5 TEST METHODS CALLING FOR BRONZE_CAMPAIGN
      if(trg_tb == 'bronze_campaign'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-6 TEST METHODS CALLING FOR BRONZE_CAMPAIGN_PERSONA_ACTIVITY
      if(trg_tb == 'bronze_campaign_persona_activity'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-7 TEST METHODS CALLING FOR BRONZE_CAMPAIGN_RULE
      if(trg_tb == 'bronze_campaign_rule'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-8 TEST METHODS CALLING FOR BRONZE_CAMPAIGN_TACTICS
      if(trg_tb == 'bronze_campaign_tactics'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-9 TEST METHODS CALLING FOR BRONZE_CANDIDATE
      if (trg_tb == 'bronze_candidate'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-10 TEST METHODS CALLING FOR BRONZE_CANDIDATE_DISPOSITION_LIST
      if(trg_tb == 'bronze_candidate_disposition_list'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-11 TEST METHODS CALLING FOR BRONZE_CANDIDATE_STATUS_LIST
      if(trg_tb == 'bronze_candidate_status_list'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-12 TEST METHODS CALLING FOR BRONZE_CANDIDATE_SUBSCRIPTION
      if(trg_tb == 'bronze_candidate_subscription'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-13 TEST METHODS CALLING FOR BRONZE_COMMUNICATION_LOG
      if(trg_tb == 'bronze_communication_log'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-14 TEST METHODS CALLING FOR BRONZE_COMMUNICATION_TEMPLATE
      if(trg_tb == 'bronze_communication_template'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

    #TB-15 TEST METHODS CALLING FOR BRONZE_COMMUNICATION_TYPES
      if(trg_tb == 'bronze_communication_types'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-16 TEST METHODS CALLING FOR BRONZE_EVENT_STORE
      if(trg_tb == 'bronze_event_store'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

    #TB-17 TEST METHODS CALLING FOR BRONZE_EVENT_TYPE
      if(trg_tb == 'bronze_event_type'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-18 TEST METHODS CALLING FOR BRONZE_EVENT_WORKER
      if(trg_tb == 'bronze_event_worker'):
          for trg_col in getDuplicateTestTargetColumns(trg_tb):
            verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
        
          for trg_col in getNullTestTargetColumns(trg_tb):  
            verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

          Verify_full_load_row_count2(env,app_id,trg_tb,suiteStartTime)

    #TB-19 TEST METHODS CALLING FOR BRONZE_EVENTS
      if(trg_tb == 'bronze_events'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-20 TEST METHODS CALLING FOR BRONZE_FOLDER
      if(trg_tb == 'bronze_folder'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-21 TEST METHODS CALLING FOR BRONZE_FOLDER_CANDIDATE
      if(trg_tb == 'bronze_folder_candidates'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-22 TEST METHODS CALLING FOR BRONZE_FOLDER_CANDIDATE_STATUS_LOG
      if(trg_tb == 'bronze_folder_candidates_status_log'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-23 TEST METHODS CALLING FOR BRONZE_GLOBAL_STATUS_LOG
      if(trg_tb == 'bronze_global_status_log'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

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
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-27 TEST METHODS CALLING FOR BRONZE_INTEGRATION_LOG
      if(trg_tb == 'bronze_integration_log'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-28 TEST METHODS CALLING FOR BRONZE_INTERVIEW
      if(trg_tb == 'bronze_interview'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

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
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-32 TEST METHODS CALLING FOR BRONZE_REQUISITION
      if(trg_tb == 'bronze_requisition'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-33 TEST METHODS CALLING FOR BRONZE_SENDGRID_EVENT
      if(trg_tb == 'bronze_sendgrid_event'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-34 TEST METHODS CALLING FOR BRONZE_SPONSOR_INFO
      if(trg_tb == 'bronze_sponsor_info'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-35 TEST METHODS CALLING FOR BRONZE_TACTIC_TYPE
      if(trg_tb == 'bronze_tactic_type'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-36 TEST METHODS CALLING FOR BRONZE_TAGS
      if(trg_tb == 'bronze_tags'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-37 TEST METHODS CALLING FOR BRONZE_TAGS_CANDIDATE
      if(trg_tb == 'bronze_tags_candidate'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

    #TB-38 TEST METHODS CALLING FOR BRONZE_UNIFIED_CAMPAIGN
      if(trg_tb == 'bronze_unified_campaign'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")

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
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")


    #TB-42 TEST METHODS CALLING FOR BRONZE_WORK_HISTORY
      if(trg_tb == 'bronze_work_history'):
        full_table_name = f"`{env}`.`{app_id}`.`{trg_tb}`"
        try:
          spark.table(full_table_name).limit(1).count()

          if(runType == 'incremental'):
            primary_column = getPrimaryColumn(trg_tb)
            date_column = getDateColumn(trg_tb)
            maxincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "max")
            minincrementalValue = getIncrementalValues(env, app_id, trg_tb, primary_column, date_column, "min")

            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_incremental_duplicate_records(env, app_id, trg_tb, trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_incremental_null_records(env, app_id, trg_tb,trg_col, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime)

            Verify_incremental_row_count(env, app_id, trg_tb, primary_column, maxincrementalValue, minincrementalValue, suiteStartTime, date_column)

          if(runType == 'full'):
            for trg_col in getDuplicateTestTargetColumns(trg_tb):
              verify_duplicate_records(env, app_id, trg_tb,trg_col,suiteStartTime)
          
            for trg_col in getNullTestTargetColumns(trg_tb):  
              verify_null_records(env, app_id, trg_tb,trg_col,suiteStartTime)

            Verify_full_load_row_count(env,app_id,trg_tb,suiteStartTime,getDateColumn(trg_tb))

        except Exception as e:
            print(f"Table '{full_table_name}' does not exist.")