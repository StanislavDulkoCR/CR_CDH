/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  agg_account_number.sql
* Date       :  2018/02/21 12:28
* Dev & QA   :  Stanislav Dulko
***************************************************************************/


drop table if exists prod.agg_account_number;
create table         prod.agg_account_number
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id, hash_account_id)
    AS
WITH cte_dlg AS (
    SELECT
        dlg.hash_account_id
        , dlg.group_id
        , row_number()
          OVER (
              PARTITION BY dlg.hash_account_id
              ORDER BY dlg.source_create_date DESC, dlg.update_date DESC ) AS scd_rnk

    FROM prod.digital_lic_group dlg
    WHERE dlg.delete_date IS NULL

), cte_raw as (

    SELECT
          acc.individual_id               AS individual_id
        , acc.hash_account_id
        , acc.source_name
        , acc.account_subtype_code
        , acc.source_account_id           AS acct_id
        , acc.account_subtype_code        AS acct_prefix
        , acc.source_account_id           AS acct_num
        , acc.first_account_date          AS first_date
        , dad.account_last_login_date     AS acct_lst_logn_dt
        , dad.account_login_cnt           AS acct_logn_cnt
        , cdlg.group_id                   AS group_id
        , upper(dad.external_user_source) AS usr_src_ind
        , pad.purged_ind                  AS purged_ind


    FROM prod.account acc
        left join prod.digital_account_detail dad
            on acc.hash_account_id = dad.hash_account_id
                and acc.source_name = 'PWI'
        left join prod.print_account_detail pad
            on acc.hash_account_id = pad.hash_account_id
                and acc.source_name = 'CDS'
        left join cte_dlg cdlg
            on acc.hash_account_id = cdlg.hash_account_id
                and cdlg.scd_rnk = 1
    where acc.individual_id is not null

)

select
    cte_raw.individual_id
    , cte_raw.hash_account_id
    , cte_raw.source_name
    , cte_raw.account_subtype_code
    , cte_raw.acct_id
    , cte_raw.acct_prefix
    , cte_raw.acct_num
    , cte_raw.first_date
    , cte_raw.acct_lst_logn_dt
    , cte_raw.acct_logn_cnt
    , cte_raw.group_id
    , cte_raw.usr_src_ind
    , cte_raw.purged_ind
    , upper(md5(cte_raw.acct_num||cte_raw.acct_prefix)) as key_account_number /*SD: Upper() because of RPDM implementation of md5()*/
from cte_raw
;

ANALYSE prod.agg_account_number;