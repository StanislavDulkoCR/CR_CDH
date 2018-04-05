/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  003_Drop_Constraints.sql
* Date       :  2018/02/21
* Dev & QA   :  Stanislav Dulko
***************************************************************************/

ALTER TABLE prod.agg_abq_donation_nightly                DROP CONSTRAINT agg_abq_donation_pkey                ;
ALTER TABLE prod.agg_abq_response_nightly                DROP CONSTRAINT agg_abq_response_pkey                ;
ALTER TABLE prod.agg_abq_summary_nightly                 DROP CONSTRAINT agg_abq_summary_pkey1                ;
ALTER TABLE prod.agg_account_number_nightly              DROP CONSTRAINT agg_account_number_pkey			  ;
ALTER TABLE prod.agg_app_session_nightly                 DROP CONSTRAINT agg_app_session_pkey                 ;
ALTER TABLE prod.agg_books_item_nightly                  DROP CONSTRAINT agg_books_item_pkey				  ;
ALTER TABLE prod.agg_books_order_nightly                 DROP CONSTRAINT agg_books_order_pkey                 ;
ALTER TABLE prod.agg_books_summary_nightly               DROP CONSTRAINT agg_books_summary_pkey               ;
ALTER TABLE prod.agg_constituent_alert_response_nightly  DROP CONSTRAINT agg_constituent_alert_response_pkey  ;
ALTER TABLE prod.agg_constituent_donation_nightly        DROP CONSTRAINT agg_constituent_donation_pkey        ;
ALTER TABLE prod.agg_constituent_interest_nightly        DROP CONSTRAINT agg_constituent_interest_pkey        ;
ALTER TABLE prod.agg_constituent_nightly                 DROP CONSTRAINT agg_constituent_pkey                 ;
ALTER TABLE prod.agg_constituent_survey_response_nightly DROP CONSTRAINT agg_constituent_survey_response_pkey ;
ALTER TABLE prod.agg_credit_card_nightly                 DROP CONSTRAINT agg_credit_card_pkey                 ;
ALTER TABLE prod.agg_digital_item_nightly                DROP CONSTRAINT agg_digital_item_pkey                ;
ALTER TABLE prod.agg_digital_login_nightly               DROP CONSTRAINT agg_digital_login_pkey               ;
ALTER TABLE prod.agg_digital_order_nightly               DROP CONSTRAINT agg_digital_order_pkey               ;
ALTER TABLE prod.agg_email_dev_response_detail_nightly   DROP CONSTRAINT agg_email_dev_pkey                   ;
ALTER TABLE prod.agg_email_nightly                       DROP CONSTRAINT agg_email_pkey                       ;
ALTER TABLE prod.agg_email_response_summary_nightly      DROP CONSTRAINT agg_email_response_pkey              ;
ALTER TABLE prod.agg_fundraising_donation_nightly        DROP CONSTRAINT agg_fundraising_donation2_pkey       ;
ALTER TABLE prod.agg_fundraising_name_nightly            DROP CONSTRAINT agg_fundraising_name_pkey            ;
ALTER TABLE prod.agg_individual_device_history_nightly   DROP CONSTRAINT agg_individual_device_history_pkey   ;
ALTER TABLE prod.agg_individual_device_summary_nightly   DROP CONSTRAINT agg_individual_device_summary_pkey   ;
/*ALTER TABLE prod.agg_individual_xographic_nightly        DROP CONSTRAINT agg_individual_xographic_pkey        ;*/
ALTER TABLE prod.agg_infobase_profile_nightly            DROP CONSTRAINT agg_infobase_profile_pkey            ;
ALTER TABLE prod.agg_list_coop_hist_nightly              DROP CONSTRAINT agg_list_coop_pkey                   ;
ALTER TABLE prod.agg_model_score_nightly                 DROP CONSTRAINT agg_model_score_pkey                 ;
ALTER TABLE prod.agg_name_address_nightly                DROP CONSTRAINT agg_name_address_pkey1               ;
ALTER TABLE prod.agg_print_order_nightly                 DROP CONSTRAINT agg_print_order_pkey                 ;
ALTER TABLE prod.agg_promotion_nightly                   DROP CONSTRAINT agg_promotion_pkey_full              ;
ALTER TABLE prod.agg_prospect_email_nightly              DROP CONSTRAINT agg_prospect_email_pkey              ;
ALTER TABLE prod.agg_raw_address_nightly                 DROP CONSTRAINT agg_raw_address2_pkey                ;
ALTER TABLE prod.agg_response_summary_nightly            DROP CONSTRAINT agg_response_summary_pkey            ;
ALTER TABLE prod.agg_target_analytics_nightly            DROP CONSTRAINT agg_target_analytics2_pkey           ;
