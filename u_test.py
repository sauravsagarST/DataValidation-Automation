# Databricks notebook source
# MAGIC %sql
# MAGIC -- select * from cert.sfx_analytics.bronze_load_config where app_id = 'xcloud' and config_name = 'candidate' limit 10;
# MAGIC
# MAGIC select * from cert.xcloud.bronze_candidate limit 20;
# MAGIC
# MAGIC -- select * from cert.xcloud.bronze_folder limit 20;
# MAGIC
# MAGIC -- select * from cert.xcloud.bronze_folder_candidates limit 20;
# MAGIC
# MAGIC -- select * from cert.xcloud.bronze_users limit 20;
# MAGIC
# MAGIC -- select * from cert.xcloud.bronze_work_history limit 20;
# MAGIC
# MAGIC -- select * from cert.xcloud.bronze_communication_types limit 20;
# MAGIC
# MAGIC -- select * from cert.xcloud.bronze_tags limit 20;
# MAGIC
# MAGIC -- select * from cert.xcloud.bronze_attachment limit 20;
# MAGIC
# MAGIC -- select * from cert.xcloud.bronze_candidate_subscription limit 20;
# MAGIC
# MAGIC -- select * from cert.xcloud.bronze_tags_candidate limit 20;
# MAGIC
# MAGIC -- Select count(*) as cnt  from ( Select count(*) from cert.xcloud.bronze_candidate group by candidate_id having count(*) > 1 );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEST_OUTPUT_TABLE
# MAGIC create database if not exists cert.test_status_db1;
# MAGIC -- DROP DATABASE IF EXISTS test_status_db1 CASCADE;
# MAGIC use cert.cert.test_status_db1;
# MAGIC CREATE TABLE IF NOT EXISTS cert.test_satus_db1.test_output_table (
# MAGIC     execution_time timestamp,
# MAGIC     test_id String,
# MAGIC     environment STRING,
# MAGIC     testname STRING,
# MAGIC     source_database STRING,
# MAGIC     source_tablename STRING,
# MAGIC     target_database String,
# MAGIC     target_tablename String,
# MAGIC     test_status String
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TableName - bronze_candidate
# MAGIC
# MAGIC --  Duplicate records validation
# MAGIC Select (case when cnt = 0 then 'PASS' else 'FAIL' end) as Test_status from ( Select count(*) as cnt  from 
# MAGIC                 (Select count(*) from cert.xcloud.bronze_candidate group by candidate_id having count(*) > 1 ));
# MAGIC
# MAGIC -- INSERT INTO cert.test_satus_db1.test_output_table (
# MAGIC --     CURRENT_TIMESTAMP(),
# MAGIC --     'TC001',
# MAGIC --     'cert',
# MAGIC --     'Duplicate_record_validation',
# MAGIC --     '',
# MAGIC --     '',
# MAGIC --     'xcloud',
# MAGIC --     'bronze_candidate',
# MAGIC --     test_status
# MAGIC -- )
# MAGIC
# MAGIC
# MAGIC --  Null records validation
# MAGIC
# MAGIC                 -- column 'candidate_id'
# MAGIC Select (case when cnt = 0 then 'PASS' else 'FAIL' end) as Test_Status from
# MAGIC                 (Select count(*) as cnt from cert.xcloud.bronze_candidate where candidate_id is null);
# MAGIC
# MAGIC                 -- column 'uname'
# MAGIC Select (case when cnt = 0 then 'PASS' else 'FAIL' end) as Test_Status from
# MAGIC                 (Select count(*) as cnt from cert.xcloud.bronze_candidate where uname is null);
# MAGIC
# MAGIC                -- column 'first_name'
# MAGIC Select (case when cnt = 0 then 'PASS' else 'FAIL' end) as Test_Status from
# MAGIC                 (Select count(*) as cnt from cert.xcloud.bronze_candidate where first_name is null);
# MAGIC
# MAGIC
# MAGIC --  Date format validation
# MAGIC -- Select (case when cnt = 0 then 'PASS' else 'FAIL' end) as Test_Status  from (Select count(*) as cnt from 
# MAGIC --                 (Select to_date(substr(dob,0,10),'YYYY-MM-DD') as new_date  from cert.xcloud.bronze_candidate));
# MAGIC
# MAGIC -- Length validation
# MAGIC -- select case when cnt = 0 then 'PASS' else 'FAIL' end from
# MAGIC --                 (Select count(*) as cnt from cert.xcloud.bronze_candidate where length(column_name>col_length));
# MAGIC
# MAGIC
# MAGIC -- row count validation for table bronze_candidate
# MAGIC Select  case when src_cnt = trg_cnt then 'PASS' else 'FAIL' end as status
# MAGIC                     from  (Select count(*) as src_cnt from cert.xcloud.bronze_candidate ) as src_tb,
# MAGIC                     (Select count(*) as trg_cnt from cert.xcloud.bronze_candidate) as trg_tb;
# MAGIC
# MAGIC -- constant column value validation for table bronze_folder
# MAGIC Select case when cnt = 0 then 'PASS' else 'FAIL' end  from
# MAGIC                     (Select count(*) as cnt from cert.xcloud.bronze_folder where access_type not in ('PUBLIC','TEAMSANDUSERS','HIERARCHY','PRIVATE'));
# MAGIC
# MAGIC -- Valid email for table bronze_users
# MAGIC Select case when cnt = 0 then 'PASS' else 'FAIL' end  from
# MAGIC                     (Select count(*) as cnt from cert.xcloud.bronze_users where email like '%.com');
# MAGIC
# MAGIC select * from cert.xcloud.bronze_users where email not like '%.com';
# MAGIC
# MAGIC -- select * from cert.xcloud.bronze_users

# COMMAND ----------

