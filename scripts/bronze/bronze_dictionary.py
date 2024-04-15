# Databricks notebook source
mySqlHost_cert = "jdbc:mysql://cert-xcloud-for-upgrade.ceq12wejj76d.us-east-1.rds.amazonaws.com"
mySqlHost_prod = "jdbc:mysql://prod-xcloud-aurora-57-new.coc65bxwymxu.us-west-1.rds.amazonaws.com"

dashboard_dict = {
    'userinsight': ['bronze_candidate','bronze_folder','bronze_users','bronze_user_logins','bronze_folder_candidates','bronze_user_searches','bronze_folder_candidates_status_log'],
    'talentnetwork': ['bronze_candidate','bronze_work_hostory','bronze_attachment','bronze_communication_types','bronze_candidate_subscription','bronze_areaofinterest_candidate','bronze_folder','bronze_folder_candidates','bronze_users','bronze_tags','bronze_tags_candidate']
}


bronze_candidate_dict = {
    'duplicate_tc': ['candidate_id'],
    'null_tc': ['candidate_id']
}
bronze_folder_dict = {
    'duplicate_tc': ['folder_id'],
    'null_tc': ['folder_id','users_id']
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
bronze_work_history_dict = {
    'duplicate_tc': ['work_history_id'],
    'null_tc': ['work_history_id']
}
bronze_attachment_dict = {
    'duplicate_tc': ['attachment_id'],
    'null_tc': ['attachment_id']
}
bronze_communication_types_dict = {
    'duplicate_tc': ['communication_types_id'],
    'null_tc': ['communication_types_id']
}
bronze_candidate_subscription_dict = {
    'duplicate_tc': ['candidate_subscription_id'],
    'null_tc': ['candidate_subscription_id']
}
bronze_areaofinterest_candidate_dict = {
    'duplicate_tc': ['areaofinterest_candidate_id'],
    'null_tc': ['areaofinterest_candidate_id']
}
bronze_tags_dict = {
    'duplicate_tc': ['tag_id'],
    'null_tc': ['tag_id']
}
bronze_tags_candidate_dict = {
    'duplicate_tc': ['tags_candidate_id'],
    'null_tc': ['tags_candidate_id']
}
