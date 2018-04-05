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



