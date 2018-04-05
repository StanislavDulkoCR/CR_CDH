/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  002_Rename_Tables.sql
* Date       :  2018/02/21
* Dev & QA   :  Stanislav Dulko
***************************************************************************/

ALTER TABLE prod.agg_abq_donation                    rename to   agg_abq_donation_nightly                ;
ALTER TABLE prod.agg_abq_response                    rename to   agg_abq_response_nightly                ;
ALTER TABLE prod.agg_abq_summary                     rename to   agg_abq_summary_nightly                 ;
ALTER TABLE prod.agg_account_number                  rename to   agg_account_number_nightly              ;
ALTER TABLE prod.agg_app_session                     rename to   agg_app_session_nightly                 ;
ALTER TABLE prod.agg_books_item                      rename to   agg_books_item_nightly                  ;
ALTER TABLE prod.agg_books_order                     rename to   agg_books_order_nightly                 ;
ALTER TABLE prod.agg_books_summary                   rename to   agg_books_summary_nightly               ;
ALTER TABLE prod.agg_build_and_buy                   rename to   agg_build_and_buy_nightly               ;
ALTER TABLE prod.agg_cga                             rename to   agg_cga_nightly                         ;
ALTER TABLE prod.agg_constituent                     rename to   agg_constituent_nightly                 ;
ALTER TABLE prod.agg_constituent_alert_response      rename to   agg_constituent_alert_response_nightly  ;
ALTER TABLE prod.agg_constituent_donation            rename to   agg_constituent_donation_nightly        ;
ALTER TABLE prod.agg_constituent_interest            rename to   agg_constituent_interest_nightly        ;
ALTER TABLE prod.agg_constituent_survey_response     rename to   agg_constituent_survey_response_nightly ;
ALTER TABLE prod.agg_credit_card                     rename to   agg_credit_card_nightly                 ;
ALTER TABLE prod.agg_digital_item                    rename to   agg_digital_item_nightly                ;
ALTER TABLE prod.agg_digital_login                   rename to   agg_digital_login_nightly               ;
ALTER TABLE prod.agg_digital_order                   rename to   agg_digital_order_nightly               ;
ALTER TABLE prod.agg_digital_summary                 rename to   agg_digital_summary_nightly             ;
ALTER TABLE prod.agg_email                           rename to   agg_email_nightly                       ;
ALTER TABLE prod.agg_email_dev_response_detail       rename to   agg_email_dev_response_detail_nightly   ;
ALTER TABLE prod.agg_email_response_summary          rename to   agg_email_response_summary_nightly      ;
ALTER TABLE prod.agg_ereader_subscription            rename to   agg_ereader_subscription_nightly        ;
ALTER TABLE prod.agg_fundraising_donation            rename to   agg_fundraising_donation_nightly        ;
ALTER TABLE prod.agg_fundraising_name                rename to   agg_fundraising_name_nightly            ;
ALTER TABLE prod.agg_fundraising_summary             rename to   agg_fundraising_summary_nightly         ;
ALTER TABLE prod.agg_individual                      rename to   agg_individual_nightly                  ;
ALTER TABLE prod.agg_individual_device_history       rename to   agg_individual_device_history_nightly   ;
ALTER TABLE prod.agg_individual_device_summary       rename to   agg_individual_device_summary_nightly   ;
ALTER TABLE prod.agg_individual_emails               rename to   agg_individual_emails_nightly           ;
/*ALTER TABLE prod.agg_individual_xographic            rename to   agg_individual_xographic_nightly        ;*/
ALTER TABLE prod.agg_infobase_profile                rename to   agg_infobase_profile_nightly            ;
ALTER TABLE prod.agg_list_coop_hist                  rename to   agg_list_coop_hist_nightly              ;
ALTER TABLE prod.agg_model_score                     rename to   agg_model_score_nightly                 ;
ALTER TABLE prod.agg_name_address                    rename to   agg_name_address_nightly                ;
ALTER TABLE prod.agg_preference                      rename to   agg_preference_nightly                  ;
ALTER TABLE prod.agg_preference_summary              rename to   agg_preference_summary_nightly          ;
ALTER TABLE prod.agg_print_order                     rename to   agg_print_order_nightly                 ;
ALTER TABLE prod.agg_print_summary                   rename to   agg_print_summary_nightly               ;
ALTER TABLE prod.agg_promotion                       rename to   agg_promotion_nightly                   ;
ALTER TABLE prod.agg_promotion_summary               rename to   agg_promotion_summary_nightly           ;
ALTER TABLE prod.agg_prospect_email                  rename to   agg_prospect_email_nightly              ;
ALTER TABLE prod.agg_raw_address                     rename to   agg_raw_address_nightly                 ;
ALTER TABLE prod.agg_response_summary                rename to   agg_response_summary_nightly            ;
ALTER TABLE prod.agg_target_analytics                rename to   agg_target_analytics_nightly            ;
ALTER TABLE prod.agg_web_session                     rename to   agg_web_session_nightly                 ;
