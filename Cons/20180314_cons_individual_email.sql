
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
--Former_ID temp table
/***************************************************************************/



drop table if exists    agg.cons_individual_email_fm_temp;
create table            agg.cons_individual_email_fm_temp
    DISTSTYLE KEY
    DISTKEY (current_id)
    SORTKEY (current_id, former_id)
    as
select
        idx.current_id
        , idx.former_id

    from prod.id_xref idx
    where exists (select null
                  from prod.individual_email ex0
                  where ex0.individual_id = idx.former_id
                  and ex0.email_address is not null)
        and idx.id_type = 'I';

analyze agg.cons_individual_email_fm_temp;

/***************************************************************************/
--Former_ID check
/***************************************************************************/

SELECT  count(*) as former_id_match_cnt
FROM 	agg.cons_individual_email_fm_temp;


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

    select fmt.current_id, fmt.former_id
    from agg.cons_individual_email_fm_temp fmt

), cte_precons_upd /*Custom pre-Cons update goes here*/ AS (

    SELECT
        *

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

--        cse.individual_id||'|'|| lead(cse.individual_id,1)  over (PARTITION BY cse.current_id, cse.email_subtype order by 1)     ,


    cast(cse.current_id as BIGINT) as individual_id
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

    , listagg( distinct cse.individual_id,',') WITHIN GROUP (order by cse.individual_id = cse.current_id desc) over ( partition by cse.current_id, cse.email_subtype) as cons_list

    from cte_cons_eva cse

), cte_fin_union as (

    SELECT
        *
    , case
            when len(cons_list) > 10                        then 'M'   /*Merge 				*/
            when cons_list =  cast(individual_id as BIGINT) then 'C'   /*Current, No change */
            when cons_list <> cast(individual_id as BIGINT) then 'U'   /*Update individual_id , No merge  */
        end cons_status

    FROM cte_cons_rnk

)

select *
from cte_fin_union

;

analyze agg.cons_individual_email_temp;



 CREATE TABLE IF NOT EXISTS audit.cons_individual_email (
     individual_id                    BIGINT      NOT NULL ENCODE DELTA DISTKEY,
     email_subtype                    VARCHAR(64) NOT NULL,
     email_address                    VARCHAR(255),
     data_source                      VARCHAR(55) NOT NULL,
     acx_dma_email_flag               CHAR,
     acx_valid_email_mailability_flag VARCHAR(13),
     acx_valid_email_flag             CHAR,
     valid_flag                       CHAR,
     source_deliverability_indicator  VARCHAR(13),
     source_valid_flag                VARCHAR(10),
     email_client_app                 VARCHAR(55),
     first_date                       TIMESTAMP,
     eff_date                         TIMESTAMP,
     end_date                         TIMESTAMP,
     individual_email_match_hash      VARCHAR(64),
     create_date                      TIMESTAMP   NOT NULL,
     create_feed_instance_id          VARCHAR(64) NOT NULL ENCODE RUNLENGTH,
     update_date                      TIMESTAMP,
     update_feed_instance_id          VARCHAR(64),
     historical_record_indicator      CHAR,
     current_id                       BIGINT,
     former_id                        BIGINT,
     cons_list                        VARCHAR(128),
     cons_status                      CHAR(1),
     cons_state                       CHAR(1),
     cons_date                        TIMESTAMP,
     cons_id                          VARCHAR(32)
 )
     DISTSTYLE KEY
     INTERLEAVED SORTKEY (individual_id, email_subtype, email_address, data_source)
 ;


insert into audit.cons_individual_email
    (  current_id
     , former_id
     , cons_list
     , cons_status
     , cons_state
     , cons_date
     , cons_id
     , individual_id
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

)

with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_individual_email_fm_temp fmt
)

SELECT
      cast(cfm.current_id AS BIGINT) AS current_id
    , cast(cfm.former_id AS BIGINT)  AS former_id
    , cast(NULL AS VARCHAR(128))     AS cons_list
    , cast(NULL AS CHAR(1))          AS cons_status
    , cast('B' AS CHAR(1))           AS cons_state
    , sysdate                        AS consolidation_date
    , upper(md5(sysdate))            AS consolidation_id
    , ie.individual_id
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


FROM prod.individual_email ie
    inner join cte_former_match cfm
        on (ie.individual_id = cfm.current_id or ie.individual_id = cfm.former_id)
WHERE ie.email_address is not null

 UNION ALL

select
      individual_id        AS current_id
    , cast(NULL AS BIGINT) AS former_id
    , cons_list
    , cons_status
    , cast('A' AS CHAR(1)) AS cons_state
    , sysdate              AS consolidation_date
    , upper(md5(sysdate))  AS consolidation_id
    , individual_id
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


FROM agg.cons_individual_email_temp

;


CREATE TABLE IF NOT EXISTS prod.individual_email_nightly (LIKE prod.individual_email);

TRUNCATE TABLE prod.individual_email_nightly;
INSERT INTO prod.individual_email_nightly
    SELECT
        *
    FROM prod.individual_email;



delete from prod.individual_email
 where 1=1
    AND email_address is not NULL
    AND exists (select null from audit.cons_individual_email ex0 where ex0.individual_id = prod.individual_email.individual_id );


 insert into prod.individual_email
 select
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

analyze prod.individual_email;