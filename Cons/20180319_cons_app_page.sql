/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Consolidation
* Module Name:  prod.app_page.sql
* Date       :  2018/03/19
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

    CREATE TABLE prod.etalon_app_page as SELECT * FROM prod.app_page;

Run inbetween runs for QA:

    truncate table prod.app_page;
    insert into prod.app_page select * from prod.etalon_app_page;

    drop table if exists audit.cons_app_page;

*/

/***************************************************************************/
--Former_ID temp table, finding all nonsurvivor (former_id) individual_id in target table
/***************************************************************************/



drop table if exists    agg.cons_app_page_fm_temp;
create table            agg.cons_app_page_fm_temp
    DISTSTYLE KEY
    DISTKEY (current_id)
    SORTKEY (current_id, former_id)
    as
select
        idx.current_id
        , idx.former_id

    from prod.id_xref idx
    where exists (select null
                  from prod.app_page ex0
                  where idx.former_id = ex0.individual_id

                 )
        and idx.id_type = 'I';

analyze agg.cons_app_page_fm_temp;

/***************************************************************************/
--Former_ID check, used in automation. If > 0 then Continue, else Stop
/***************************************************************************/

    /* Used in automation only, first step */
    /*
    SELECT  count(*) as former_id_match_cnt
    FROM    agg.cons_app_page_fm_temp;
    */

/***************************************************************************/
--Main Body, temp table contains final result of consolidation
/***************************************************************************/

drop table if exists    agg.cons_app_page_temp;
create table            agg.cons_app_page_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    as
 with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_app_page_fm_temp fmt

), cte_precons_upd /*Custom pre-Cons update goes here*/ AS (

    SELECT DISTINCT
      visit_id, visit_num, visit_page_num, event_list_id, page_id, individual_id, app_code, page_name, erights_user_id, hit_time, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator
    , cfm.current_id
FROM prod.app_page main
    inner join cte_former_match cfm
        /*Picking out only B (Before) rows for pair of survivor+nonsurvivor i.e. current+former*/
        on (main.individual_id = cfm.current_id or main.individual_id = cfm.former_id)

), cte_cons_eva as (

    SELECT
          current_id
        , visit_id, visit_num, visit_page_num, event_list_id, page_id, individual_id, app_code, page_name, erights_user_id, hit_time, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator

        /*Custom Default Value Rule below. min/max a handled in next CTE as asc/desc respectively*/
        , greatest(upd.create_date, upd.update_date) AS default_row_01
/*      , 1 AS default_row_02
        , 1 AS default_row_03
        , 1 AS default_row_04
        , 1 AS default_row_05*/


    FROM cte_precons_upd upd



), cte_cons_rnk as (

    select distinct /*SD: must be distinct due window function usage*/

    cast(cse.current_id as BIGINT) as individual_id

    , cse.visit_id, cse.visit_num, cse.visit_page_num, cse.event_list_id, cse.page_id
        /*cse.individual_id, */

    /*SD: erights_user_id order: If erights_id is null, then the date is 5 years less */
    , /*Default Value Rule*/ first_value(app_code)                over ( PARTITION BY visit_id, visit_num, visit_page_num, event_list_id, page_id, current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC, default_row_03 DESC, default_row_04 DESC, default_row_05 DESC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as app_code
    , /*Default Value Rule*/ first_value(page_name)               over ( PARTITION BY visit_id, visit_num, visit_page_num, event_list_id, page_id, current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC, default_row_03 DESC, default_row_04 DESC, default_row_05 DESC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as page_name
    , /*Custom*/             first_value(erights_user_id)         over ( PARTITION BY visit_id, visit_num, visit_page_num, event_list_id, page_id, current_id ORDER BY (case when erights_user_id is not null then default_row_01 else add_months(default_row_01, -60)  end) desc  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as erights_user_id
    , /*Custom*/             first_value(hit_time)                over ( PARTITION BY visit_id, visit_num, visit_page_num, event_list_id, page_id, current_id ORDER BY hit_time asc                                                                                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as hit_time
    , /*Default Value Rule*/ first_value(create_date)             over ( PARTITION BY visit_id, visit_num, visit_page_num, event_list_id, page_id, current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC, default_row_03 DESC, default_row_04 DESC, default_row_05 DESC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_date
    , /*Default Value Rule*/ first_value(create_feed_instance_id) over ( PARTITION BY visit_id, visit_num, visit_page_num, event_list_id, page_id, current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC, default_row_03 DESC, default_row_04 DESC, default_row_05 DESC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_feed_instance_id
    , /*Default Value Rule*/ first_value(update_date)             over ( PARTITION BY visit_id, visit_num, visit_page_num, event_list_id, page_id, current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC, default_row_03 DESC, default_row_04 DESC, default_row_05 DESC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_date
    , /*Default Value Rule*/ first_value(update_feed_instance_id) over ( PARTITION BY visit_id, visit_num, visit_page_num, event_list_id, page_id, current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC, default_row_03 DESC, default_row_04 DESC, default_row_05 DESC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_feed_instance_id
    , cast(null as varchar(1)) as historical_record_indicator



    , listagg( distinct cse.individual_id,',') WITHIN GROUP (order by cse.individual_id = cse.current_id desc) over ( partition by visit_id, visit_num, visit_page_num, event_list_id, page_id, current_id ) as cons_list

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

analyze agg.cons_app_page_temp;



 CREATE TABLE IF NOT EXISTS audit.cons_app_page (
     /*Table will be made only once*/

     current_id                       BIGINT,
     cons_list                        VARCHAR(128),
     cons_status                      CHAR(1),
     cons_state                       CHAR(1),
     cons_date                        TIMESTAMP,
     cons_id                          VARCHAR(32),
     /*Original table DDL below*/

     visit_id VARCHAR(50) NOT NULL  ENCODE lzo ,visit_num VARCHAR(10) NOT NULL  ENCODE bytedict ,visit_page_num INTEGER NOT NULL  ENCODE delta ,event_list_id INTEGER NOT NULL  ENCODE runlength ,page_id INTEGER NOT NULL  ENCODE lzo ,individual_id BIGINT NOT NULL  ENCODE lzo ,app_code VARCHAR(8)   ENCODE lzo ,page_name VARCHAR(1000)   ENCODE lzo ,erights_user_id VARCHAR(45)   ENCODE lzo ,hit_time TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo ,create_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE lzo ,create_feed_instance_id VARCHAR(64) NOT NULL  ENCODE runlength ,update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo ,update_feed_instance_id VARCHAR(64)   ENCODE lzo ,historical_record_indicator CHAR(1)   ENCODE lzo

     /*Original Distyle/Sortkey below*/
 )

 ;


insert into audit.cons_app_page
    (  current_id
     , cons_list
     , cons_status
     , cons_state
     , cons_date
     , cons_id
     , visit_id, visit_num, visit_page_num, event_list_id, page_id, individual_id, app_code, page_name, erights_user_id, hit_time, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator

)

with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_app_page_fm_temp fmt
)

SELECT DISTINCT
      cast(cfm.current_id AS BIGINT) AS current_id
    , cast(NULL AS VARCHAR(128))     AS cons_list
    , cast(NULL AS CHAR(1)     )     AS cons_status
    , cast('B'  AS CHAR(1)     )     AS cons_state
    , sysdate                        AS consolidation_date
    , upper(md5(sysdate))            AS consolidation_id

    /*List of all columns from original table below.*/
    , visit_id, visit_num, visit_page_num, event_list_id, page_id, individual_id, app_code, page_name, erights_user_id, hit_time, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator

FROM prod.app_page main
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
    , visit_id, visit_num, visit_page_num, event_list_id, page_id, individual_id, app_code, page_name, erights_user_id, hit_time, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator

FROM agg.cons_app_page_temp

;


/*CREATE TABLE IF NOT EXISTS prod.app_page_nightly (LIKE prod.app_page);

TRUNCATE TABLE prod.app_page_nightly;
INSERT INTO prod.app_page_nightly
    SELECT
        *
    FROM prod.app_page;*/



delete from prod.app_page
 where 1=1
    AND exists (select null from audit.cons_app_page ex0 where ex0.individual_id = prod.app_page.individual_id );


 insert into prod.app_page
 select

     /*ONLY original columns. Since agg.cons_app_page_temp table contains extra for audit*/
     visit_id, visit_num, visit_page_num, event_list_id, page_id, individual_id, app_code, page_name, erights_user_id, hit_time, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator


    FROM agg.cons_app_page_temp;

analyze prod.app_page;