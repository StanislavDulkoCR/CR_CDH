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
/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  005_Add_Constraints.sql
* Date       :  2018/02/21
* Dev & QA   :  Stanislav Dulko
***************************************************************************/

ALTER TABLE prod.agg_abq_donation                ADD CONSTRAINT agg_abq_donation_pkey                PRIMARY KEY (individual_id , abq_yr                                                           ) ;
ALTER TABLE prod.agg_abq_response                ADD CONSTRAINT agg_abq_response_pkey                PRIMARY KEY (individual_id , abq_yr              , keycode                                    ) ;
ALTER TABLE prod.agg_abq_summary                 ADD CONSTRAINT agg_abq_summary_pkey1                PRIMARY KEY (individual_id                                                                    ) ;
ALTER TABLE prod.agg_account_number              ADD CONSTRAINT agg_account_number_pkey              PRIMARY KEY (acct_num      , acct_prefix                                                      ) ;
ALTER TABLE prod.agg_app_session                 ADD CONSTRAINT agg_app_session_pkey                 PRIMARY KEY (individual_id , visit_id            , visit_num                                  ) ;
ALTER TABLE prod.agg_books_item                  ADD CONSTRAINT agg_books_item_pkey                  PRIMARY KEY (individual_id , acct_id             , ord_num       , seq_num                    ) ;
ALTER TABLE prod.agg_books_order                 ADD CONSTRAINT agg_books_order_pkey                 PRIMARY KEY (individual_id , acct_id             , ord_num                                    ) ;
ALTER TABLE prod.agg_books_summary               ADD CONSTRAINT agg_books_summary_pkey               PRIMARY KEY (individual_id                                                                    ) ;
ALTER TABLE prod.agg_constituent                 ADD CONSTRAINT agg_constituent_pkey                 PRIMARY KEY (individual_id                                                                    ) ;
ALTER TABLE prod.agg_constituent_alert_response  ADD CONSTRAINT agg_constituent_alert_response_pkey  PRIMARY KEY (individual_id , alert_response_id                                                ) ;
ALTER TABLE prod.agg_constituent_donation        ADD CONSTRAINT agg_constituent_donation_pkey        PRIMARY KEY (individual_id , transaction_id                                                   ) ;
ALTER TABLE prod.agg_constituent_interest        ADD CONSTRAINT agg_constituent_interest_pkey        PRIMARY KEY (individual_id , interest_id                                                      ) ;
ALTER TABLE prod.agg_constituent_survey_response ADD CONSTRAINT agg_constituent_survey_response_pkey PRIMARY KEY (individual_id , survey_id                                                        ) ;
ALTER TABLE prod.agg_credit_card                 ADD CONSTRAINT agg_credit_card_pkey                 PRIMARY KEY (individual_id , cc_source                                                        ) ;
ALTER TABLE prod.agg_digital_item                ADD CONSTRAINT agg_digital_item_pkey                PRIMARY KEY (individual_id , itm_id              , ord_id                                     ) ;
ALTER TABLE prod.agg_digital_login               ADD CONSTRAINT agg_digital_login_pkey               PRIMARY KEY (individual_id , external_key        , login_week                                 ) ;
ALTER TABLE prod.agg_digital_order               ADD CONSTRAINT agg_digital_order_pkey               PRIMARY KEY (individual_id , acct_id             , ord_id                                     ) ;
ALTER TABLE prod.agg_email                       ADD CONSTRAINT agg_email_pkey                       PRIMARY KEY (key_email                                                                        ) ;
ALTER TABLE prod.agg_email_dev_response_detail   ADD CONSTRAINT agg_email_dev_pkey                   PRIMARY KEY (individual_id , device_name         , cell_id                                    ) ;
ALTER TABLE prod.agg_email_response_summary      ADD CONSTRAINT agg_email_response_pkey              PRIMARY KEY (individual_id                                                                    ) ;
ALTER TABLE prod.agg_fundraising_donation        ADD CONSTRAINT agg_fundraising_donation2_pkey       PRIMARY KEY (individual_id , don_dt              , keycode                                    ) ;
ALTER TABLE prod.agg_fundraising_name            ADD CONSTRAINT agg_fundraising_name_pkey            PRIMARY KEY (individual_id                                                                    ) ;
ALTER TABLE prod.agg_individual_device_history   ADD CONSTRAINT agg_individual_device_history_pkey   PRIMARY KEY (individual_id , device_name         , device_type   , operating_system , channel ) ;
ALTER TABLE prod.agg_individual_device_summary   ADD CONSTRAINT agg_individual_device_summary_pkey   PRIMARY KEY (individual_id                                                                    ) ;
/*ALTER TABLE prod.agg_individual_xographic        ADD CONSTRAINT agg_individual_xographic_pkey        PRIMARY KEY (individual_id                                                                    ) ;*/
ALTER TABLE prod.agg_infobase_profile            ADD CONSTRAINT agg_infobase_profile_pkey            PRIMARY KEY (individual_id                                                                    ) ;
ALTER TABLE prod.agg_list_coop_hist              ADD CONSTRAINT agg_list_coop_pkey                   PRIMARY KEY (individual_id , queue_id                                                         ) ;
ALTER TABLE prod.agg_model_score                 ADD CONSTRAINT agg_model_score_pkey                 PRIMARY KEY (individual_id , scr_dt              , mdl_name                                   ) ;
ALTER TABLE prod.agg_name_address                ADD CONSTRAINT agg_name_address_pkey1               PRIMARY KEY (individual_id                                                                    ) ;
ALTER TABLE prod.agg_print_order                 ADD CONSTRAINT agg_print_order_pkey                 PRIMARY KEY (individual_id , acct_id             , ord_id                                     ) ;
ALTER TABLE prod.agg_promotion                   ADD CONSTRAINT agg_promotion_pkey_full              PRIMARY KEY (individual_id , keycode             , bus_unt       , prom_dt                    ) ;
ALTER TABLE prod.agg_prospect_email              ADD CONSTRAINT agg_prospect_email_pkey              PRIMARY KEY (individual_id , email_address       , orig_source                                ) ;
ALTER TABLE prod.agg_raw_address                 ADD CONSTRAINT agg_raw_address2_pkey                PRIMARY KEY (individual_id                                                                    ) ;
ALTER TABLE prod.agg_response_summary            ADD CONSTRAINT agg_response_summary_pkey            PRIMARY KEY (individual_id , bus_unt             , keycode                                    ) ;
ALTER TABLE prod.agg_target_analytics            ADD CONSTRAINT agg_target_analytics2_pkey           PRIMARY KEY (individual_id , scr_dt                                                           ) ;



