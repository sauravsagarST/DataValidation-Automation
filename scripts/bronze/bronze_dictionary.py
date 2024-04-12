# Databricks notebook source
mySqlHost = "jdbc:mysql://cert-xcloud-for-upgrade.ceq12wejj76d.us-east-1.rds.amazonaws.com"


bronze_candidate_dict = {
    'duplicate_tc': ['candidate_id','users_id'],
    'null_tc': ['candidate_id','users_id','uname']
}
bronze_folder_dict = {
    'duplicate_tc': ['folder_id'],
    'null_tc': ['folder_id','users_id','hierarchy_id']
}
bronze_users_dict = {
    'duplicate_tc': ['users_id'],
    'null_tc': ['users_id']
}
bronze_user_logins_dict ={
    'duplicate_tc': ['users_id'],
    'null_tc': ['users_id']
}
bronze_folder_candidates_dict = {
    'duplicate_tc': ['folder_cands_id'],
    'null_tc': ['folder_cands_id','folder_id']
}
bronze_user_searches_dict = {
    'duplicate_tc': ['user_searches_id'],
    'null_tc': ['user_searches_id','users_id']
}
bronze_folder_candidates_status_log_dict = {
    'duplicate_tc': ['folder_cands_status_log_id'],
    'null_tc': ['folder_cands_status_log_id','candidate_id']
}
