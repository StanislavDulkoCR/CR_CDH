/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  001_Drop_Nightly.sql
* Date       :  2018/02/21
* Dev & QA   :  Stanislav Dulko
***************************************************************************/

DROP TABLE IF EXISTS prod.agg_abq_donation_nightly                ;
DROP TABLE IF EXISTS prod.agg_abq_response_nightly                ;
DROP TABLE IF EXISTS prod.agg_abq_summary_nightly                 ;
DROP TABLE IF EXISTS prod.agg_account_number_nightly              ;
DROP TABLE IF EXISTS prod.agg_app_session_nightly                 ;
DROP TABLE IF EXISTS prod.agg_books_item_nightly                  ;
DROP TABLE IF EXISTS prod.agg_books_order_nightly                 ;
DROP TABLE IF EXISTS prod.agg_books_summary_nightly               ;
DROP TABLE IF EXISTS prod.agg_build_and_buy_nightly               ;
DROP TABLE IF EXISTS prod.agg_cga_nightly                         ;
DROP TABLE IF EXISTS prod.agg_constituent_nightly                 ;
DROP TABLE IF EXISTS prod.agg_constituent_alert_response_nightly  ;
DROP TABLE IF EXISTS prod.agg_constituent_donation_nightly        ;
DROP TABLE IF EXISTS prod.agg_constituent_interest_nightly        ;
DROP TABLE IF EXISTS prod.agg_constituent_survey_response_nightly ;
DROP TABLE IF EXISTS prod.agg_credit_card_nightly                 ;
DROP TABLE IF EXISTS prod.agg_digital_item_nightly                ;
DROP TABLE IF EXISTS prod.agg_digital_login_nightly               ;
DROP TABLE IF EXISTS prod.agg_digital_order_nightly               ;
DROP TABLE IF EXISTS prod.agg_digital_summary_nightly             ;
DROP TABLE IF EXISTS prod.agg_email_nightly                       ;
DROP TABLE IF EXISTS prod.agg_email_dev_response_detail_nightly   ;
DROP TABLE IF EXISTS prod.agg_email_response_summary_nightly      ;
DROP TABLE IF EXISTS prod.agg_ereader_subscription_nightly        ;
DROP TABLE IF EXISTS prod.agg_fundraising_donation_nightly        ;
DROP TABLE IF EXISTS prod.agg_fundraising_name_nightly            ;
DROP TABLE IF EXISTS prod.agg_fundraising_summary_nightly         ;
DROP TABLE IF EXISTS prod.agg_individual_nightly                  ;
DROP TABLE IF EXISTS prod.agg_individual_device_history_nightly   ;
DROP TABLE IF EXISTS prod.agg_individual_device_summary_nightly   ;
DROP TABLE IF EXISTS prod.agg_individual_emails_nightly           ;
/*DROP TABLE IF EXISTS prod.agg_individual_xographic_nightly        ;*/
DROP TABLE IF EXISTS prod.agg_infobase_profile_nightly            ;
DROP TABLE IF EXISTS prod.agg_list_coop_hist_nightly              ;
DROP TABLE IF EXISTS prod.agg_model_score_nightly                 ;
DROP TABLE IF EXISTS prod.agg_name_address_nightly                ;
DROP TABLE IF EXISTS prod.agg_preference_nightly                  ;
DROP TABLE IF EXISTS prod.agg_preference_summary_nightly          ;
DROP TABLE IF EXISTS prod.agg_print_order_nightly                 ;
DROP TABLE IF EXISTS prod.agg_print_summary_nightly               ;
DROP TABLE IF EXISTS prod.agg_promotion_nightly                   ;
DROP TABLE IF EXISTS prod.agg_promotion_summary_nightly           ;
DROP TABLE IF EXISTS prod.agg_prospect_email_nightly              ;
DROP TABLE IF EXISTS prod.agg_raw_address_nightly                 ;
DROP TABLE IF EXISTS prod.agg_response_summary_nightly            ;
DROP TABLE IF EXISTS prod.agg_target_analytics_nightly            ;
DROP TABLE IF EXISTS prod.agg_web_session_nightly                 ;
