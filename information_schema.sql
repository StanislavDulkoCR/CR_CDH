;
SELECT TOP 300 *
FROM information_schema.columns
WHERE 1=1
and table_schema = 'prod'
and table_name like 'agg%'

and table_name = 'agg_response_summary'

order by 3,5

;
prod.agg_credit_card           |MT_CREDIT_CARD
prod.agg_name_address          |MT_NAME_ADDRESS
prod.agg_print_order           |MT_OFFLINE_ORD
prod.agg_digital_order         |MT_ONLINE_ORD
prod.agg_books_order           |MT_BOOKS_ORD
prod.agg_email_response_summary|MT_EM_RESP_SUMMARY
prod.agg_digital_item          |MT_ONLINE_ITEM
prod.agg_preference            |MT_AUTHORIZATION
prod.agg_account_number        |MT_ACCOUNT_NUMBER
prod.agg_books_summary         |MT_BOOKS_SUMMARY
prod.agg_digital_summary       |MT_ONLINE_SUMMARY
prod.agg_fundraising_name      |MT_FR_NAME
prod.agg_preference_summary    |MT_AUTH_SUMMARY
prod.agg_print_summary         |MT_OFFLINE_SUMMARY
prod.agg_response_summary      |MT_RESP_SUMMARY
prod.agg_fundraising_summary   |MT_FR_SUMMARY
prod.agg_promotion_summary     |MT_PROMO_SUMMARY
prod.agg_individual            |MT_INDIVIDUAL
;


SELECT TOP 100 individual_id, address_id, acx_ef_record
FROM prod.individual_address
WHERE individual_id = 1200688761;
SELECT TOP 100 individual_id, address_id, edit_fail_flg
FROM prod.agg_name_address
WHERE individual_id = 1200688761;