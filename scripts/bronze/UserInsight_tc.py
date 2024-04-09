# Databricks notebook source
# MAGIC %run ../../utils/util

# COMMAND ----------

# env = dbutils.widgets.get("Environment")
# app_id = dbutils.widgets.get("Client")
# tb_list = dbutils.widgets.get("TargetTableName")
# if(tb_list.length() == 0):
#   trg_table_list = getTargetTableList(app_id)
# else:
#   trg_table_list = tb_list.split(',')

env = "cert"
client = "xcloud"
target_table = "bronze_users"


mySql_df = getMySqlData(env,client,target_table)
mySql_df.createOrReplaceTempView("mysql_tempTable")

mySqlQuery = f"""SELECT count(*) as row_count FROM mysql_tempTable
"""
sqlResult = spark.sql(mySqlQuery)
mySqlCount = sqlResult.collect()[0]['row_count']
print(mySqlCount)
