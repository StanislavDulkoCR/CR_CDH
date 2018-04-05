/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Consolidation
* Module Name:  prod.impact_device_log.sql
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

    CREATE TABLE prod.etalon_impact_device_log as SELECT * FROM prod.impact_device_log;

Run inbetween runs for QA:

    truncate table prod.impact_device_log;
    insert into prod.impact_device_log select * from prod.etalon_impact_device_log;

    drop table if exists audit.cons_impact_device_log;

*/

/***************************************************************************/
--Former_ID temp table, finding all nonsurvivor (former_id) individual_id in target table
/***************************************************************************/



drop table if exists    agg.cons_impact_device_log_fm_temp;
create table            agg.cons_impact_device_log_fm_temp
    DISTSTYLE KEY
    DISTKEY (current_id)
    SORTKEY (current_id, former_id)
    as
select
        idx.current_id
        , idx.former_id

    from prod.id_xref idx
    where exists (select null
                  from prod.impact_device_log ex0
                  where idx.former_id = ex0.individual_id

                 )
        and idx.id_type = 'I';

analyze agg.cons_impact_device_log_fm_temp;

/***************************************************************************/
--Former_ID check, used in automation. If > 0 then Continue, else Stop
/***************************************************************************/

    /* Used in automation only, first step */
    /*
    SELECT  count(*) as former_id_match_cnt
    FROM    agg.cons_impact_device_log_fm_temp;
    */

/***************************************************************************/
--Main Body, temp table contains final result of consolidation
/***************************************************************************/

drop table if exists    agg.cons_impact_device_log_temp;
create table            agg.cons_impact_device_log_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    as
 with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_impact_device_log_fm_temp fmt

), cte_precons_upd /*Custom pre-Cons update goes here*/ AS (

    SELECT DISTINCT
      individual_id, device_name, event_date, program_id, cell_id, keycode, email_address_recd, device_type, os_name, os_details, os_browser, click_flag, create_date, create_feed_instance_id, update_date, update_feed_instance_id
    , cfm.current_id
FROM prod.impact_device_log main
    inner join cte_former_match cfm
        /*Picking out only B (Before) rows for pair of survivor+nonsurvivor i.e. current+former*/
        on (main.individual_id = cfm.current_id or main.individual_id = cfm.former_id)

), cte_cons_eva as (

    SELECT
          current_id
        , individual_id, device_name, event_date, program_id, cell_id, keycode, email_address_recd, device_type, os_name, os_details, os_browser, click_flag, create_date, create_feed_instance_id, update_date, update_feed_instance_id

        /*Custom Default Value Rule below. min/max a handled in next CTE as asc/desc respectively*/
        , decode(upd.click_flag, 'Y', 1, 0)          AS default_row_01
        , greatest(upd.create_date, upd.update_date) AS default_row_02
/*        , 1 AS default_row_03
        , 1 AS default_row_04
        , 1 AS default_row_05*/


    FROM cte_precons_upd upd



), cte_cons_rnk as (

    select distinct /*SD: must be distinct due window function usage*/

    cast(cse.current_id as BIGINT) as individual_id


    , /*cse.individual_id, */ cse.device_name, cse.event_date, cse.program_id, cse.cell_id
    , /*Default Value Rule*/ first_value(keycode)                 over ( PARTITION BY current_id, device_name, event_date, program_id, cell_id ORDER BY default_row_01 DESC, default_row_02 DESC/*, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as keycode
    , /*Default Value Rule*/ first_value(email_address_recd)      over ( PARTITION BY current_id, device_name, event_date, program_id, cell_id ORDER BY default_row_01 DESC, default_row_02 DESC/*, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as email_address_recd
    , /*Default Value Rule*/ first_value(device_type)             over ( PARTITION BY current_id, device_name, event_date, program_id, cell_id ORDER BY default_row_01 DESC, default_row_02 DESC/*, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as device_type
    , /*Default Value Rule*/ first_value(os_name)                 over ( PARTITION BY current_id, device_name, event_date, program_id, cell_id ORDER BY default_row_01 DESC, default_row_02 DESC/*, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as os_name
    , /*Default Value Rule*/ first_value(os_details)              over ( PARTITION BY current_id, device_name, event_date, program_id, cell_id ORDER BY default_row_01 DESC, default_row_02 DESC/*, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as os_details
    , /*Default Value Rule*/ first_value(os_browser)              over ( PARTITION BY current_id, device_name, event_date, program_id, cell_id ORDER BY default_row_01 DESC, default_row_02 DESC/*, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as os_browser
    , /*Custom*/             first_value(click_flag)              over ( PARTITION BY current_id, device_name, event_date, program_id, cell_id ORDER BY decode(click_flag, 'Y', 1, 0)                                                                                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as click_flag
    , /*Default Value Rule*/ first_value(create_date)             over ( PARTITION BY current_id, device_name, event_date, program_id, cell_id ORDER BY default_row_01 DESC, default_row_02 DESC/*, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_date
    , /*Default Value Rule*/ first_value(create_feed_instance_id) over ( PARTITION BY current_id, device_name, event_date, program_id, cell_id ORDER BY default_row_01 DESC, default_row_02 DESC/*, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_feed_instance_id
    , /*Default Value Rule*/ first_value(update_date)             over ( PARTITION BY current_id, device_name, event_date, program_id, cell_id ORDER BY default_row_01 DESC, default_row_02 DESC/*, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_date
    , /*Default Value Rule*/ first_value(update_feed_instance_id) over ( PARTITION BY current_id, device_name, event_date, program_id, cell_id ORDER BY default_row_01 DESC, default_row_02 DESC/*, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_feed_instance_id


    , listagg( distinct cse.individual_id,',') WITHIN GROUP (order by cse.individual_id = cse.current_id desc) over ( partition by current_id, device_name, event_date, program_id, cell_id ) as cons_list

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

analyze agg.cons_impact_device_log_temp;



 CREATE TABLE IF NOT EXISTS audit.cons_impact_device_log (
     /*Table will be made only once*/

     current_id                       BIGINT,
     cons_list                        VARCHAR(128),
     cons_status                      CHAR(1),
     cons_state                       CHAR(1),
     cons_date                        TIMESTAMP,
     cons_id                          VARCHAR(32),
     /*Original table DDL below*/

     individual_id BIGINT NOT NULL  ENCODE lzo ,device_name VARCHAR(50) NOT NULL  ENCODE lzo ,event_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE lzo ,program_id INTEGER NOT NULL  ENCODE bytedict ,cell_id BIGINT NOT NULL  ENCODE lzo ,keycode VARCHAR(150)   ENCODE lzo ,email_address_recd VARCHAR(255)   ENCODE lzo ,device_type VARCHAR(50)   ENCODE lzo ,os_name VARCHAR(256)   ENCODE lzo ,os_details VARCHAR(256)   ENCODE lzo ,os_browser VARCHAR(256)   ENCODE lzo ,click_flag VARCHAR(30)   ENCODE lzo ,create_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE lzo ,create_feed_instance_id VARCHAR(64) NOT NULL  ENCODE runlength ,update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo ,update_feed_instance_id VARCHAR(64)   ENCODE runlength

     /*Original Distyle/Sortkey below*/
 )
     DISTSTYLE EVEN INTERLEAVED SORTKEY ( individual_id , event_date , device_name , program_id )
 ;


insert into audit.cons_impact_device_log
    (  current_id
     , cons_list
     , cons_status
     , cons_state
     , cons_date
     , cons_id
     , individual_id, device_name, event_date, program_id, cell_id, keycode, email_address_recd, device_type, os_name, os_details, os_browser, click_flag, create_date, create_feed_instance_id, update_date, update_feed_instance_id

)

with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_impact_device_log_fm_temp fmt
)

SELECT DISTINCT
      cast(cfm.current_id AS BIGINT) AS current_id
    , cast(NULL AS VARCHAR(128))     AS cons_list
    , cast(NULL AS CHAR(1)     )     AS cons_status
    , cast('B'  AS CHAR(1)     )     AS cons_state
    , sysdate                        AS consolidation_date
    , upper(md5(sysdate))            AS consolidation_id

    /*List of all columns from original table below.*/
    , individual_id, device_name, event_date, program_id, cell_id, keycode, email_address_recd, device_type, os_name, os_details, os_browser, click_flag, create_date, create_feed_instance_id, update_date, update_feed_instance_id

FROM prod.impact_device_log main
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
    , individual_id, device_name, event_date, program_id, cell_id, keycode, email_address_recd, device_type, os_name, os_details, os_browser, click_flag, create_date, create_feed_instance_id, update_date, update_feed_instance_id

FROM agg.cons_impact_device_log_temp

;


/*CREATE TABLE IF NOT EXISTS prod.impact_device_log_nightly (LIKE prod.impact_device_log);

TRUNCATE TABLE prod.impact_device_log_nightly;
INSERT INTO prod.impact_device_log_nightly
    SELECT
        *
    FROM prod.impact_device_log;*/



delete from prod.impact_device_log
 where 1=1
    AND exists (select null from audit.cons_impact_device_log ex0 where ex0.individual_id = prod.impact_device_log.individual_id );


 insert into prod.impact_device_log
 select

     /*ONLY original columns. Since agg.cons_impact_device_log_temp table contains extra for audit*/
     individual_id, device_name, event_date, program_id, cell_id, keycode, email_address_recd, device_type, os_name, os_details, os_browser, click_flag, create_date, create_feed_instance_id, update_date, update_feed_instance_id


    FROM agg.cons_impact_device_log_temp;

analyze prod.impact_device_log;