mySqlHost_cert = "jdbc:mysql://cert-xcloud-for-upgrade.ceq12wejj76d.us-east-1.rds.amazonaws.com"
mySqlHost_prod = "jdbc:mysql://prod-xcloud-aurora-57-new.coc65bxwymxu.us-west-1.rds.amazonaws.com"

googleAnalytics_GaContentPath = "s3://hodes-external-data/google-analytics/archive-clean/ga_content/"
googleAnalytics_GaLocationPath = "s3://hodes-external-data/google-analytics/archive-clean/ga_location/"
googleAnalytics_GaSourcesPath = "s3://hodes-external-data/google-analytics/archive-clean/ga_sources/"
googleAnalytics_GaVisitorsPath = "s3://hodes-external-data/google-analytics/archive-clean/ga_visitors/"

googleAnalytics4_GaContentPath = "s3://hodes-external-data/google-analytics4/archive-clean/ga_content/"
googleAnalytics4_GaLocationPath = "s3://hodes-external-data/google-analytics4/archive-clean/ga_location/"
googleAnalytics4_GaSourcesPath = "s3://hodes-external-data/google-analytics4/archive-clean/ga_sources/"
googleAnalytics4_GaVisitorsPath = "s3://hodes-external-data/google-analytics4/archive-clean/ga_visitors/"

googleAnalytics_test_flag = "false"

dashboard_dict = {
    'userinsight': ['bronze_candidate','bronze_folder','bronze_users','bronze_user_logins','bronze_folder_candidates','bronze_user_searches','bronze_folder_candidates_status_log'],

    'talentnetwork': ['bronze_candidate','bronze_work_hostory','bronze_attachment','bronze_communication_types','bronze_candidate_subscription','bronze_areaofinterest_candidate','bronze_folder','bronze_folder_candidates','bronze_users','bronze_tags','bronze_tags_candidate'],

    'cws': ['bronze_ga_content','bronze_ga_location','bronze_ga_source','bronze_ga_visitors','bronze_ga4_content','bronze_ga4_location','bronze_ga4_source','bronze_ga4_visitors']

}


bronze_application_dict = {
    'duplicate_tc': ['application_id'],
    'null_tc': ['application_id'],
    'date_column': ['update_date'],
    'primary_column' : ['application_id']
}
bronze_areaofinterest_candidate_dict = {
    'duplicate_tc': ['areaofinterest_candidate_id'],
    'null_tc': ['areaofinterest_candidate_id'],
    'date_column': ['created_on'],
    'primary_column' : ['areaofinterest_candidate_id']
}
bronze_assessment_dict = {
    'duplicate_tc': ['assessment_id'],
    'null_tc': ['assessment_id'],
    'date_column': ['create_date'],
    'primary_column' : ['assessment_id']
}
bronze_attachment_dict = {
    'duplicate_tc': ['attachment_id'],
    'null_tc': ['attachment_id'],
    'date_column': ['create_date'],
    'primary_column' : ['attachment_id']
}
bronze_campaign_dict = {
    'duplicate_tc': ['campaign_id'],
    'null_tc': ['campaign_id'],
    'date_column': ['created'],
    'primary_column' : ['campaign_id']
}
bronze_campaign_persona_activity_dict = {
    'duplicate_tc': ['campaign_persona_activity_id'],
    'null_tc': ['campaign_persona_activity_id'],
    'date_column': ['created'],
    'primary_column' : ['campaign_persona_activity_id']
}
bronze_campaign_rule_dict = {
    'duplicate_tc': ['campaign_rule_id'],
    'null_tc': ['campaign_rule_id'],
    'date_column': ['created'],
    'primary_column' : ['campaign_rule_id']
}
bronze_campaign_tactics_dict = {
    'duplicate_tc': ['campaign_tactics_id'],
    'null_tc': ['campaign_tactics_id'],
    'date_column': ['created_date'],
    'primary_column' : ['campaign_tactics_id']
}
bronze_candidate_dict = {
    'duplicate_tc': ['candidate_id'],
    'null_tc': ['candidate_id'],
    'date_column': ['create_date'],
    'primary_column' : ['candidate_id']
}
bronze_candidate_disposition_list_dict = {
    'duplicate_tc': ['candidate_disposition_id'],
    'null_tc': ['candidate_disposition_id'],
    'date_column': ['created_at'],
    'primary_column' : ['candidate_disposition_id']
}
bronze_candidate_status_list_dict = {
    'duplicate_tc': ['candidate_status_id'],
    'null_tc': ['candidate_status_id'],
    'date_column': ['created_at'],
    'primary_column' : ['candidate_status_id']
}
bronze_candidate_subscription_dict = {
    'duplicate_tc': ['candidate_subscription_id'],
    'null_tc': ['candidate_subscription_id'],
    'date_column': ['created_date'],
    'primary_column' : ['candidate_subscription_id']
}
bronze_communication_log_dict = {
    'duplicate_tc': ['communication_log_id'],
    'null_tc': ['communication_log_id'],
    'date_column': ['date'],
    'primary_column' : ['communication_log_id']
}
bronze_communication_template_dict = {
    'duplicate_tc': ['communication_template_id'],
    'null_tc': ['communication_template_id'],
    'primary_column' : ['communication_template_id']
}
bronze_communication_types_dict = {
    'duplicate_tc': ['communication_types_id'],
    'null_tc': ['communication_types_id'],
    'date_column': ['created_date'],
    'primary_column' : ['communication_types_id']
}
bronze_event_store_dict = {
    'duplicate_tc': ['event_store_id'],
    'null_tc': ['event_store_id'],
    'primary_column' : ['event_store_id']
}
bronze_event_type_dict = {
    'duplicate_tc': ['event_type_id'],
    'null_tc': ['event_type_id'],
    'date_column': ['created_date'],
    'primary_column' : ['event_type_id']
}
bronze_event_worker_dict = {
    'duplicate_tc': ['event_worker_id'],
    'null_tc': ['event_worker_id'],
    'primary_column' : ['event_worker_id']
}
bronze_events_dict = {
    'duplicate_tc': ['event_id'],
    'null_tc': ['event_id'],
    'date_column': ['created_date'],
    'primary_column' : ['event_id']
}
bronze_folder_dict = {
    'duplicate_tc': ['folder_id'],
    'null_tc': ['folder_id','users_id'],
    'date_column': ['created'],
    'primary_column' : ['folder_id']
}
bronze_folder_candidates_dict = {
    'duplicate_tc': ['folder_cands_id'],
    'null_tc': ['folder_cands_id','folder_id'],
    'date_column': ['created'],
    'primary_column' : ['folder_cands_id']
}
bronze_folder_candidates_status_log_dict = {
    'duplicate_tc': ['folder_cands_status_log_id'],
    'null_tc': ['folder_cands_status_log_id','candidate_id'],
    'date_column': ['created_at'],
    'primary_column' : ['folder_cands_status_log_id']
}
bronze_global_status_log_dict = {
    'duplicate_tc': ['global_status_log_id'],
    'null_tc': ['global_status_log_id'],
    'date_column': ['created_at'],
    'primary_column' : ['global_status_log_id']
}
bronze_groups_dict = {
    'duplicate_tc': ['groups_id'],
    'null_tc': ['groups_id'],
    'primary_column' : ['groups_id']
}
bronze_hierarchy_dict = {
    'duplicate_tc': ['hierarchy_id'],
    'null_tc': ['hierarchy_id'],
    'primary_column' : ['hierarchy_id']
}
bronze_integration_exception_log_dict = {
    'duplicate_tc': ['exception_id'],
    'null_tc': ['exception_id'],
    'date_column': ['created_date'],
    'primary_column' : ['exception_id']
}
bronze_integration_log_dict = {
    'duplicate_tc': ['log_id'],
    'null_tc': ['log_id'],
    'date_column': ['created_date'],
    'primary_column' : ['log_id']
}
bronze_interview_dict = {
    'duplicate_tc': ['interview_id'],
    'null_tc': ['interview_id'],
    'date_column': ['create_datetime'],
    'primary_column' : ['interview_id']
}
bronze_jobalert_client_log_dict = {
    'duplicate_tc': ['jobalert_client_log_id'],
    'null_tc': ['jobalert_client_log_id'],
    'primary_column' : ['jobalert_client_log_id']
}
bronze_list_dict = {
    'duplicate_tc': ['list_id'],
    'null_tc': ['list_id'],
    'primary_column' : ['list_id']
}
bronze_referral_dict = {
    'duplicate_tc': ['referral_id'],
    'null_tc': ['referral_id'],
    'date_column': ['created'],
    'primary_column' : ['referral_id']
}
bronze_requisition_dict = {
    'duplicate_tc': ['requisition_id'],
    'null_tc': ['requisition_id'],
    'date_column': ['created'],
    'primary_column' : ['requisition_id']
}
bronze_sendgrid_event_dict = {
    'duplicate_tc': ['sendgrid_event_id'],
    'null_tc': ['sendgrid_event_id'],
    'date_column': ['created'],
    'primary_column' : ['sendgrid_event_id']
}
bronze_sponsor_info_dict = {
    'duplicate_tc': ['sponsor_type_id'],
    'null_tc': ['sponsor_type_id'],
    'date_column': ['created'],
    'primary_column' : ['sponsor_type_id']
}
bronze_tactic_type_dict = {
    'duplicate_tc': ['tactic_type_id'],
    'null_tc': ['tactic_type_id'],
    'date_column': ['created_date'],
    'primary_column' : ['tactic_type_id']
}
bronze_tags_dict = {
    'duplicate_tc': ['tag_id'],
    'null_tc': ['tag_id'],
    'date_column': ['created'],
    'primary_column' : ['tag_id']
}
bronze_tags_candidate_dict = {
    'duplicate_tc': ['tags_candidate_id'],
    'null_tc': ['tags_candidate_id'],
    'date_column': ['created_on'],
    'primary_column' : ['tags_candidate_id']
}
bronze_unified_campaign_dict = {
    'duplicate_tc': ['unified_campaign_id'],
    'null_tc': ['unified_campaign_id'],
    'date_column': ['created_date'],
    'primary_column' : ['unified_campaign_id']
}
bronze_user_logins_dict ={
    'duplicate_tc': ['user_logins_id'],
    'null_tc': ['user_logins_id'],
    'primary_column' : ['user_logins_id']
}
bronze_user_searches_dict = {
    'duplicate_tc': ['user_searches_id'],
    'null_tc': ['user_searches_id','users_id'],
    'primary_column' : ['user_searches_id']
}
bronze_users_dict = {
    'duplicate_tc': ['users_id'],
    'null_tc': ['users_id'],
    'date_column': ['created'],
    'primary_column' : ['users_id']
}
bronze_work_history_dict = {
    'duplicate_tc': ['work_history_id'],
    'null_tc': ['work_history_id'],
    'date_column': ['created'],
    'primary_column' : ['work_history_id']
}
