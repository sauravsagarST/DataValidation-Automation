# Databricks notebook source
# MAGIC %run ../utils/insert_ts

# COMMAND ----------

# MAGIC %run ../utils/util

# COMMAND ----------

def s3_sms_campaign_analyticsRowCounter(client):
    client_path = "s3://sms-campaign-analytics/"+client+"/"
    json_file_count = 0
    clientDateWiseDirectory = dbutils.fs.ls(client_path)
    for dateDir in clientDateWiseDirectory:
        subDirList = dbutils.fs.ls(dateDir[0])
        for Dir in subDirList:
            json_file_count = len([file for file in Dir if file.name.endswith('.json')])
    return json_file_count

# COMMAND ----------

def sms_campaign_analytics_RowCount(env,clientName,target_table,suiteStartTime):
    full_table_name = f"{env}.sfx_analytics.{target_table}"

    dbricksQuery = f"""select count(*) as row_count from {full_table_name}'
    """
    dbricksResult = spark.sql(dbricksQuery)
    dbricksCount = dbricksResult.collect()[0]['row_count'] 

    s3rowCount = s3_sms_campaign_analyticsRowCounter(clientName)
    source_table = target_table[7:]
    if(dbricksCount == s3rowCount):
        test_status = "PASS"
        print("'sms-campaign-analytics_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status)
    else:
        test_status = "FAIL"
        print("'sms-campaign-analytics_RowCount_test' target table " + target_table + " and source table: " + source_table + ", Test status: " + test_status + ",  Databricks_count(" +str(dbricksCount)+")  S3_count("+str(s3rowCount)+")" )

    # insert_query(suiteStartTime,getCurrentTime(),getTestId(),env,'sms-campaign-analytics_RowCount','',source_table,clientName,target_table,'','{testQuery}',test_status)


    return test_status
