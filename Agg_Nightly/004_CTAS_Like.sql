/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  004_CTAS_Like.sql
* Date       :  2018/02/21
* Dev & QA   :  Stanislav Dulko
***************************************************************************/

CREATE TABLE prod.agg_abq_donation                ( LIKE prod.agg_abq_donation_nightly                ) ;
CREATE TABLE prod.agg_abq_response                ( LIKE prod.agg_abq_response_nightly                ) ;
CREATE TABLE prod.agg_abq_summary                 ( LIKE prod.agg_abq_summary_nightly                 ) ;
CREATE TABLE prod.agg_account_number              ( LIKE prod.agg_account_number_nightly              ) ;
CREATE TABLE prod.agg_app_session                 ( LIKE prod.agg_app_session_nightly                 ) ;
CREATE TABLE prod.agg_books_item                  ( LIKE prod.agg_books_item_nightly                  ) ;
CREATE TABLE prod.agg_books_order                 ( LIKE prod.agg_books_order_nightly                 ) ;
CREATE TABLE prod.agg_books_summary               ( LIKE prod.agg_books_summary_nightly               ) ;
CREATE TABLE prod.agg_build_and_buy               ( LIKE prod.agg_build_and_buy_nightly               ) ;
CREATE TABLE prod.agg_cga                         ( LIKE prod.agg_cga_nightly                         ) ;
CREATE TABLE prod.agg_constituent                 ( LIKE prod.agg_constituent_nightly                 ) ;
CREATE TABLE prod.agg_constituent_alert_response  ( LIKE prod.agg_constituent_alert_response_nightly  ) ;
CREATE TABLE prod.agg_constituent_donation        ( LIKE prod.agg_constituent_donation_nightly        ) ;
CREATE TABLE prod.agg_constituent_interest        ( LIKE prod.agg_constituent_interest_nightly        ) ;
CREATE TABLE prod.agg_constituent_survey_response ( LIKE prod.agg_constituent_survey_response_nightly ) ;
CREATE TABLE prod.agg_credit_card                 ( LIKE prod.agg_credit_card_nightly                 ) ;
CREATE TABLE prod.agg_digital_item                ( LIKE prod.agg_digital_item_nightly                ) ;
CREATE TABLE prod.agg_digital_login               ( LIKE prod.agg_digital_login_nightly               ) ;
CREATE TABLE prod.agg_digital_order               ( LIKE prod.agg_digital_order_nightly               ) ;
CREATE TABLE prod.agg_digital_summary             ( LIKE prod.agg_digital_summary_nightly             ) ;
CREATE TABLE prod.agg_email                       ( LIKE prod.agg_email_nightly                       ) ;
CREATE TABLE prod.agg_email_dev_response_detail   ( LIKE prod.agg_email_dev_response_detail_nightly   ) ;
CREATE TABLE prod.agg_email_response_summary      ( LIKE prod.agg_email_response_summary_nightly      ) ;
CREATE TABLE prod.agg_ereader_subscription        ( LIKE prod.agg_ereader_subscription_nightly        ) ;
CREATE TABLE prod.agg_fundraising_donation        ( LIKE prod.agg_fundraising_donation_nightly        ) ;
CREATE TABLE prod.agg_fundraising_name            ( LIKE prod.agg_fundraising_name_nightly            ) ;
CREATE TABLE prod.agg_fundraising_summary         ( LIKE prod.agg_fundraising_summary_nightly         ) ;
CREATE TABLE prod.agg_individual                  ( LIKE prod.agg_individual_nightly                  ) ;
CREATE TABLE prod.agg_individual_device_history   ( LIKE prod.agg_individual_device_history_nightly   ) ;
CREATE TABLE prod.agg_individual_device_summary   ( LIKE prod.agg_individual_device_summary_nightly   ) ;
CREATE TABLE prod.agg_individual_emails           ( LIKE prod.agg_individual_emails_nightly           ) ;
/*CREATE TABLE prod.agg_individual_xographic        ( LIKE prod.agg_individual_xographic_nightly        ) ;*/
CREATE TABLE prod.agg_infobase_profile            ( LIKE prod.agg_infobase_profile_nightly            ) ;
CREATE TABLE prod.agg_list_coop_hist              ( LIKE prod.agg_list_coop_hist_nightly              ) ;
CREATE TABLE prod.agg_model_score                 ( LIKE prod.agg_model_score_nightly                 ) ;
CREATE TABLE prod.agg_name_address                ( LIKE prod.agg_name_address_nightly                ) ;
CREATE TABLE prod.agg_preference                  ( LIKE prod.agg_preference_nightly                  ) ;
CREATE TABLE prod.agg_preference_summary          ( LIKE prod.agg_preference_summary_nightly          ) ;
CREATE TABLE prod.agg_print_order                 ( LIKE prod.agg_print_order_nightly                 ) ;
CREATE TABLE prod.agg_print_summary               ( LIKE prod.agg_print_summary_nightly               ) ;
CREATE TABLE prod.agg_promotion                   ( LIKE prod.agg_promotion_nightly                   ) ;
CREATE TABLE prod.agg_promotion_summary           ( LIKE prod.agg_promotion_summary_nightly           ) ;
CREATE TABLE prod.agg_prospect_email              ( LIKE prod.agg_prospect_email_nightly              ) ;
CREATE TABLE prod.agg_raw_address                 ( LIKE prod.agg_raw_address_nightly                 ) ;
CREATE TABLE prod.agg_response_summary            ( LIKE prod.agg_response_summary_nightly            ) ;
CREATE TABLE prod.agg_target_analytics            ( LIKE prod.agg_target_analytics_nightly            ) ;
CREATE TABLE prod.agg_web_session                 ( LIKE prod.agg_web_session_nightly                 ) ;