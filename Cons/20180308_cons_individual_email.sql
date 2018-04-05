
/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Consolidation
* Module Name:  cons_individual_email.sql
* Date       :  2018/03/06 23:39
* Dev & QA   :  Stanislav Dulko
***************************************************************************/


/***************************************************************************/
--Former_ID check 
/***************************************************************************/

SELECT
    count(*) as former_id_match_cnt

FROM prod.id_xref idx
WHERE exists(SELECT
                 NULL
             FROM prod.individual_email ex0
             WHERE ex0.individual_id = idx.former_id) AND idx.id_type = 'I';

/***************************************************************************/
--Main Body
/***************************************************************************/



drop table if exists    agg.cons_individual_email_temp;
create table            agg.cons_individual_email_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    as
with cte_former_match as (

    select
        idx.current_id
        , idx.former_id

    from prod.id_xref idx
    where exists (select null from prod.individual_email ex0 where ex0.individual_id = idx.former_id)
        and idx.id_type = 'I'
        /*#TODO: RMV*/ and idx.create_feed_instance_id = 'SD-CUSTOM-CONS-TEST-INDEMA'

), cte_precons_upd /*Custom pre-Cons update goes here*/ AS (

    SELECT
        ie.individual_id
        , ie.email_subtype
        , lower(ie.email_address)     AS email_address
        , ie.data_source
        , ie.acx_dma_email_flag
        , ie.acx_valid_email_mailability_flag
        , ie.acx_valid_email_flag
        , ie.valid_flag
        , ie.source_deliverability_indicator
        , ie.source_valid_flag
        , ie.email_client_app
        , nvl(ie.first_date, sysdate) AS first_date
        , ie.eff_date
        , ie.end_date
        , ie.individual_email_match_hash
        , ie.create_date
        , ie.create_feed_instance_id
        , ie.update_date
        , ie.update_feed_instance_id
        , ie.historical_record_indicator
        , cfm.current_id
        , cfm.former_id

FROM prod.individual_email ie
    inner join cte_former_match cfm
        on (ie.individual_id = cfm.current_id or ie.individual_id = cfm.former_id)
WHERE ie.email_address is not null

), cte_cons_eva as (

    SELECT
        upd.*

        , CASE
            WHEN upd.email_subtype = 'DGI'
            THEN date_trunc('day', upd.eff_date) /*SD: only date part of timestamp*/
          ELSE upd.eff_date
          END                                                    AS default_row_01
        , upd.source_deliverability_indicator                    AS default_row_02
        , decode(upd.source_valid_flag, 'N', '2', 'Y', '1', '0') AS default_row_03
        , upd.valid_flag                                         AS default_row_04
        , upd.update_date                                        AS default_row_05

/*
    Keep row with greatest eff_dt
    (only use YYYYMMDD for EMAIL_DIFF 'DGI' else whole timestamp)
        , then least SRC_DELV_IND
        , then DECODE(SRC_VALID_FLAG,'N','2','Y','1','0')
        , then INVALID_FLAG
        and MAINT_DT.
*/

    FROM cte_precons_upd upd



), cte_cons_rnk as (

    select distinct /*SD: must be distinct due window function usage*/

     /*cse.individual_id,cse.email_subtype, email_address, default_row_01, default_row_02, default_row_03, default_row_04, default_row_05,*/

    cse.current_id as individual_id
    , first_value(cse.email_subtype                    ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as email_subtype
    , first_value(cse.email_address                    ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as email_address
    , first_value(cse.data_source                      ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as data_source
    , first_value(cse.acx_dma_email_flag               ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_dma_email_flag
    , first_value(cse.acx_valid_email_mailability_flag ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_valid_email_mailability_flag
    , first_value(cse.acx_valid_email_flag             ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_valid_email_flag
    , first_value(cse.valid_flag                       ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as valid_flag
    , first_value(cse.source_deliverability_indicator  ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as source_deliverability_indicator
    , first_value(cse.source_valid_flag                ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as source_valid_flag
    , first_value(cse.email_client_app                 ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as email_client_app
    , first_value(cse.first_date                       ) over (PARTITION BY cse.current_id, cse.email_subtype order by first_date asc                                                                                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as first_date
    , first_value(cse.eff_date                         ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as eff_date
    , first_value(cse.end_date                         ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as end_date
    , first_value(cse.individual_email_match_hash      ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as individual_email_match_hash
    , first_value(cse.create_date                      ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_date
    , first_value(cse.create_feed_instance_id          ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_feed_instance_id
    , first_value(cse.update_date                      ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_date
    , first_value(cse.update_feed_instance_id          ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_feed_instance_id
    , first_value(cse.historical_record_indicator      ) over (PARTITION BY cse.current_id, cse.email_subtype order by default_row_01 desc, default_row_02 asc, default_row_03 desc, default_row_04 desc, cse.update_date desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as historical_record_indicator


    from cte_cons_eva cse

), cte_fin_union as (

    SELECT
        *, 'Y' as consolidation_indicator
    FROM cte_cons_rnk
    UNION ALL 
    SELECT
        *, cast(null as varchar(4)) as consolidation_indicator
    FROM prod.individual_email noncons
    WHERE NOT exists(SELECT
                       NULL
                   FROM cte_precons_upd cons
                   WHERE noncons.individual_id = cons.individual_id)

)

select *
from cte_fin_union

;

analyze agg.cons_individual_email_temp;



insert into scd.individual_email_cons_hist (individual_id,email_subtype,email_address,data_source,acx_dma_email_flag,acx_valid_email_mailability_flag,acx_valid_email_flag,valid_flag,source_deliverability_indicator,source_valid_flag,email_client_app,first_date,eff_date,end_date,individual_email_match_hash,create_date,create_feed_instance_id,update_date,update_feed_instance_id,historical_record_indicator,current_id,former_id,consolidation_date,consolidation_id
)
with cte_former_match as (

    select
        idx.current_id
        , idx.former_id

    from prod.id_xref idx
    where exists (select null from prod.individual_email ex0 where ex0.individual_id = idx.former_id)
        and idx.id_type = 'I'

)
SELECT
    ie.individual_id
    , ie.email_subtype
    , ie.email_address
    , ie.data_source
    , ie.acx_dma_email_flag
    , ie.acx_valid_email_mailability_flag
    , ie.acx_valid_email_flag
    , ie.valid_flag
    , ie.source_deliverability_indicator
    , ie.source_valid_flag
    , ie.email_client_app
    , ie.first_date
    , ie.eff_date
    , ie.end_date
    , ie.individual_email_match_hash
    , ie.create_date
    , ie.create_feed_instance_id
    , ie.update_date
    , ie.update_feed_instance_id
    , ie.historical_record_indicator
    , cfm.current_id
    , cfm.former_id
    , sysdate                                           as consolidation_date
    , upper(md5(sysdate)) 								as consolidation_id

FROM prod.individual_email ie
    inner join cte_former_match cfm
        on (ie.individual_id = cfm.current_id or ie.individual_id = cfm.former_id)
WHERE ie.email_address is not null
;

CREATE TABLE IF NOT EXISTS prod.individual_email_nightly (LIKE prod.individual_email);

TRUNCATE TABLE prod.individual_email_nightly;
INSERT INTO prod.individual_email_nightly
    SELECT
        *
    FROM prod.individual_email;


TRUNCATE TABLE prod.individual_email;
INSERT INTO prod.individual_email
    SELECT
        individual_id
        , email_subtype
        , email_address
        , data_source
        , acx_dma_email_flag
        , acx_valid_email_mailability_flag
        , acx_valid_email_flag
        , valid_flag
        , source_deliverability_indicator
        , source_valid_flag
        , email_client_app
        , first_date
        , eff_date
        , end_date
        , individual_email_match_hash
        , create_date
        , create_feed_instance_id
        , update_date
        , update_feed_instance_id
        , historical_record_indicator
    FROM agg.cons_individual_email_temp;

ANALYZE prod.individual_email;
