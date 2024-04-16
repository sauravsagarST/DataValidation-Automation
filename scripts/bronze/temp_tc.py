# Databricks notebook source
# MAGIC %run ../../utils/util

# COMMAND ----------


env = "prod"
client = "clevelandclinic"
src_tb = "areaofinterest_candidate"
mySqlHost_prod = "jdbc:mysql://prod-xcloud-aurora-57-new.coc65bxwymxu.us-west-1.rds.amazonaws.com"


dbricksQuery = f"""select count(*) as row_count from prod.clevelandclinic.bronze_areaofinterest_candidate where modified_on < '2024-04-15 00:00:00'
"""
dbricksResult = spark.sql(dbricksQuery)
dbricksCount = dbricksResult.collect()[0]['row_count'] 
print(dbricksCount)
db_host = ""
db_username = "saurav.sagar"
db_password = "Purpl3M()useGl0wing"

mysql_df = spark.read \
            .format("jdbc") \
            .option("url", mySqlHost_prod) \
            .option("dbtable",f"{client}.{src_tb}") \
            .option("user", db_username) \
            .option("password", db_password) \
            .load()
mysql_df.createOrReplaceTempView("mysql_tempTable")

# Run SQL queries against the temporary view
mySqlQuery = f"""SELECT count(*) as row_count FROM mysql_tempTable where where modified_on < '2024-04-15 00:00:00'
"""
sqlResult = spark.sql(mySqlQuery)
mySqlCount = sqlResult.collect()[0]['row_count']
print(mySqlCount)

# comparequery = f"""
# """
