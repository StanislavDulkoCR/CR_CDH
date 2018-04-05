/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Consolidation
* Module Name:  prod.fundraising_vanity_name.sql
* Date       :  2018/03/21
* Dev        :  Stanislav Dulko
* QA         :  zzzzzzzzzzzzzzzzzzzzzzzzz
***************************************************************************/

/* Legend:
RPL_SCHEMA_NAME     - schema_name, e.g. prod
RPL_TMP_SCHEMA_NAME - temporary_schema name, e.g. agg
RPL_TABLE_NAME      - table_name, e.g. individual_email
RPL_TABLE_PK        - comma separated columns that are defined as PK for consolidation
RPL_FIRST_VALUE     - pre-populated FIRST_VALUE
RPL_COLUMN_LIST     - comma separated columns from target table, all of them
RPL_DDL_COL         - DDL for columns
RPL_DDL_KEY         - DDL for Distkey/Sortkey
*/

/* Debug:

Run only once for QA:

    CREATE TABLE prod.etalon_fundraising_vanity_name as SELECT * FROM prod.fundraising_vanity_name;

Run inbetween runs for QA:

    truncate table prod.fundraising_vanity_name;
    insert into prod.fundraising_vanity_name select * from prod.etalon_fundraising_vanity_name;

    drop table if exists audit.cons_fundraising_vanity_name;

*/

/***************************************************************************/
--Former_ID temp table, finding all nonsurvivor (former_id) individual_id in target table
/***************************************************************************/



drop table if exists    agg.cons_fundraising_vanity_name_fm_temp;
create table            agg.cons_fundraising_vanity_name_fm_temp
    DISTSTYLE KEY
    DISTKEY (current_id)
    SORTKEY (current_id, former_id)
    as
select
        idx.current_id
        , idx.former_id

    from prod.id_xref idx
    where exists (select null
                  from prod.fundraising_vanity_name ex0
                  where idx.former_id = ex0.individual_id

                 )
        and idx.id_type = 'I';

analyze agg.cons_fundraising_vanity_name_fm_temp;

/***************************************************************************/
--Former_ID check, used in automation. If > 0 then Continue, else Stop
/***************************************************************************/

    /* Used in automation only, first step */
    /*
    SELECT  count(*) as former_id_match_cnt
    FROM    agg.cons_fundraising_vanity_name_fm_temp;
    */

/***************************************************************************/
--Main Body, temp table contains final result of consolidation
/***************************************************************************/

drop table if exists    agg.cons_fundraising_vanity_name_temp;
create table            agg.cons_fundraising_vanity_name_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    as
 with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_fundraising_vanity_name_fm_temp fmt

), cte_precons_upd /*Custom pre-Cons update goes here*/ AS (

    SELECT DISTINCT
      individual_id, fundraising_salutation_formal, fundraising_first_name, fundraising_middle_name, fundraising_last_name, fundraising_suffix_name, source_name, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator, vanity_name_match_hash
    , cfm.current_id
FROM prod.fundraising_vanity_name main
    inner join cte_former_match cfm
        /*Picking out only B (Before) rows for pair of survivor+nonsurvivor i.e. current+former*/
        on (main.individual_id = cfm.current_id or main.individual_id = cfm.former_id)

), cte_cons_eva as (

    SELECT
          current_id
        , individual_id, fundraising_salutation_formal, fundraising_first_name, fundraising_middle_name, fundraising_last_name, fundraising_suffix_name, source_name, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator, vanity_name_match_hash

        /*Custom Default Value Rule below. min/max a handled in next CTE as asc/desc respectively*/
        , greatest(upd.create_date, upd.update_date) AS default_row_01
/*        , 1 AS default_row_02
        , 1 AS default_row_03
        , 1 AS default_row_04
        , 1 AS default_row_05*/


    FROM cte_precons_upd upd



), cte_cons_rnk as (

    select distinct /*SD: must be distinct due window function usage*/

    cast(cse.current_id as BIGINT) as individual_id


, /*cse.individual_id, */
  /*Default Value Rule*/ first_value(fundraising_salutation_formal) over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as fundraising_salutation_formal
, /*Default Value Rule*/ first_value(fundraising_first_name)        over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as fundraising_first_name
, /*Default Value Rule*/ first_value(fundraising_middle_name)       over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as fundraising_middle_name
, /*Default Value Rule*/ first_value(fundraising_last_name)         over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as fundraising_last_name
, /*Default Value Rule*/ first_value(fundraising_suffix_name)       over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as fundraising_suffix_name
, /*Default Value Rule*/ first_value(source_name)                   over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as source_name
, /*Default Value Rule*/ first_value(create_date)                   over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_date
, /*Default Value Rule*/ first_value(create_feed_instance_id)       over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_feed_instance_id
, /*Default Value Rule*/ first_value(update_date)                   over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_date
, /*Default Value Rule*/ first_value(update_feed_instance_id)       over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_feed_instance_id
, cast(null as varchar(1)) as historical_record_indicator
, /*Default Value Rule*/ first_value(vanity_name_match_hash)        over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as vanity_name_match_hash



    , listagg( distinct cse.individual_id,',') WITHIN GROUP (order by cse.individual_id = cse.current_id desc) over ( partition by current_id ) as cons_list

    from cte_cons_eva cse

), cte_fin_union as (

    SELECT
        *
    , case
            when len(cons_list) > 10                        then 'M'   /*Merge                            */
            when cons_list =  cast(individual_id as BIGINT) then 'C'   /*Current, No change               */
            when cons_list <> cast(individual_id as BIGINT) then 'U'   /*Update individual_id , No merge  */
        end cons_status

    FROM cte_cons_rnk

)

select *
from cte_fin_union

;

analyze agg.cons_fundraising_vanity_name_temp;



 CREATE TABLE IF NOT EXISTS audit.cons_fundraising_vanity_name (
     /*Table will be made only once*/

     current_id                       BIGINT,
     cons_list                        VARCHAR(128),
     cons_status                      CHAR(1),
     cons_state                       CHAR(1),
     cons_date                        TIMESTAMP,
     cons_id                          VARCHAR(32),
     /*Original table DDL below*/

     individual_id BIGINT NOT NULL  ENCODE delta ,fundraising_salutation_formal VARCHAR(50)   ENCODE lzo ,fundraising_first_name VARCHAR(60)   ENCODE text255 ,fundraising_middle_name VARCHAR(40)   ENCODE lzo ,fundraising_last_name VARCHAR(60)   ENCODE lzo ,fundraising_suffix_name VARCHAR(15)   ENCODE lzo ,source_name VARCHAR(64) NOT NULL  ENCODE lzo ,create_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE lzo ,create_feed_instance_id VARCHAR(64) NOT NULL  ENCODE runlength ,update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE bytedict ,update_feed_instance_id VARCHAR(64)   ENCODE lzo ,historical_record_indicator CHAR(1)   ENCODE lzo ,vanity_name_match_hash VARCHAR(64)   ENCODE lzo

     /*Original Distyle/Sortkey below*/
 )
     DISTSTYLE EVEN INTERLEAVED SORTKEY ( source_name )
 ;


insert into audit.cons_fundraising_vanity_name
    (  current_id
     , cons_list
     , cons_status
     , cons_state
     , cons_date
     , cons_id
     , individual_id, fundraising_salutation_formal, fundraising_first_name, fundraising_middle_name, fundraising_last_name, fundraising_suffix_name, source_name, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator, vanity_name_match_hash

)

with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_fundraising_vanity_name_fm_temp fmt
)

SELECT DISTINCT
      cast(cfm.current_id AS BIGINT) AS current_id
    , cast(NULL AS VARCHAR(128))     AS cons_list
    , cast(NULL AS CHAR(1)     )     AS cons_status
    , cast('B'  AS CHAR(1)     )     AS cons_state
    , sysdate                        AS consolidation_date
    , upper(md5(sysdate))            AS consolidation_id

    /*List of all columns from original table below.*/
    , individual_id, fundraising_salutation_formal, fundraising_first_name, fundraising_middle_name, fundraising_last_name, fundraising_suffix_name, source_name, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator, vanity_name_match_hash

FROM prod.fundraising_vanity_name main
    inner join cte_former_match cfm
        on (main.individual_id = cfm.current_id or main.individual_id = cfm.former_id)

 UNION ALL

select
      individual_id         AS current_id
    , cons_list             AS cons_list
    , cons_status           AS cons_status
    , cast('A'  AS CHAR(1)) AS cons_state
    , sysdate               AS consolidation_date
    , upper(md5(sysdate))   AS consolidation_id

    /*List of all columns from original table below.*/
    , individual_id, fundraising_salutation_formal, fundraising_first_name, fundraising_middle_name, fundraising_last_name, fundraising_suffix_name, source_name, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator, vanity_name_match_hash

FROM agg.cons_fundraising_vanity_name_temp

;


/*CREATE TABLE IF NOT EXISTS prod.fundraising_vanity_name_nightly (LIKE prod.fundraising_vanity_name);

TRUNCATE TABLE prod.fundraising_vanity_name_nightly;
INSERT INTO prod.fundraising_vanity_name_nightly
    SELECT
        *
    FROM prod.fundraising_vanity_name;*/



delete from prod.fundraising_vanity_name
 where 1=1
    AND exists (select null from audit.cons_fundraising_vanity_name ex0 where ex0.individual_id = prod.fundraising_vanity_name.individual_id );


 insert into prod.fundraising_vanity_name
 select

     /*ONLY original columns. Since agg.cons_fundraising_vanity_name_temp table contains extra for audit*/
     individual_id, fundraising_salutation_formal, fundraising_first_name, fundraising_middle_name, fundraising_last_name, fundraising_suffix_name, source_name, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator, vanity_name_match_hash


    FROM agg.cons_fundraising_vanity_name_temp;

analyze prod.fundraising_vanity_name;