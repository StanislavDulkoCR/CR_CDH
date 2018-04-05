/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Consolidation
* Module Name:  template.sql
* Date       :  xxxxxxxxxxxxxxxxxxxxxxxxx
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

    CREATE TABLE RPL_SCHEMA_NAME.etalon_RPL_TABLE_NAME as SELECT * FROM RPL_SCHEMA_NAME.RPL_TABLE_NAME;

Run inbetween runs for QA:

    truncate table RPL_SCHEMA_NAME.RPL_TABLE_NAME;
    insert into RPL_SCHEMA_NAME.RPL_TABLE_NAME select * from RPL_SCHEMA_NAME.etalon_RPL_TABLE_NAME;

    drop table if exists audit.cons_RPL_TABLE_NAME;

*/

/***************************************************************************/
--Former_ID temp table, finding all nonsurvivor (former_id) individual_id in target table
/***************************************************************************/



drop table if exists    RPL_TMP_SCHEMA_NAME.cons_RPL_TABLE_NAME_fm_temp;
create table            RPL_TMP_SCHEMA_NAME.cons_RPL_TABLE_NAME_fm_temp
    DISTSTYLE KEY
    DISTKEY (current_id)
    SORTKEY (current_id, former_id)
    as
select
        idx.current_id
        , idx.former_id

    from RPL_SCHEMA_NAME.id_xref idx
    where exists (select null
                  from RPL_SCHEMA_NAME.RPL_TABLE_NAME ex0
                  where idx.former_id = ex0.individual_id

                 )
        and idx.id_type = 'I';

analyze RPL_TMP_SCHEMA_NAME.cons_RPL_TABLE_NAME_fm_temp;

/***************************************************************************/
--Former_ID check, used in automation. If > 0 then Continue, else Stop
/***************************************************************************/

    /* Used in automation only, first step */
    /*
    SELECT  count(*) as former_id_match_cnt
    FROM    RPL_TMP_SCHEMA_NAME.cons_RPL_TABLE_NAME_fm_temp;
    */

/***************************************************************************/
--Main Body, temp table contains final result of consolidation
/***************************************************************************/

drop table if exists    RPL_TMP_SCHEMA_NAME.cons_RPL_TABLE_NAME_temp;
create table            RPL_TMP_SCHEMA_NAME.cons_RPL_TABLE_NAME_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    as
 with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from RPL_TMP_SCHEMA_NAME.cons_RPL_TABLE_NAME_fm_temp fmt

), cte_precons_upd /*Custom pre-Cons update goes here*/ AS (

    SELECT DISTINCT
      RPL_COLUMN_LIST
    , cfm.current_id
FROM RPL_SCHEMA_NAME.RPL_TABLE_NAME main
    inner join cte_former_match cfm 
        /*Picking out only B (Before) rows for pair of survivor+nonsurvivor i.e. current+former*/
        on (main.individual_id = cfm.current_id or main.individual_id = cfm.former_id)

), cte_cons_eva as (

    SELECT
          current_id
        , RPL_COLUMN_LIST

        /*Custom Default Value Rule below. min/max a handled in next CTE as asc/desc respectively*/
        , 1 AS default_row_01
        , 1 AS default_row_02
        , 1 AS default_row_03
        , 1 AS default_row_04
        , 1 AS default_row_05


    FROM cte_precons_upd upd



), cte_cons_rnk as (

    select distinct /*SD: must be distinct due window function usage*/

    cast(cse.current_id as BIGINT) as individual_id

    , RPL_FIRST_VALUE


    , listagg( distinct cse.individual_id,',') WITHIN GROUP (order by cse.individual_id = cse.current_id desc) over ( partition by RPL_TABLE_PK ) as cons_list

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

analyze RPL_TMP_SCHEMA_NAME.cons_RPL_TABLE_NAME_temp;



 CREATE TABLE IF NOT EXISTS audit.cons_RPL_TABLE_NAME (
     /*Table will be made only once*/
     
     current_id                       BIGINT,
     cons_list                        VARCHAR(128),
     cons_status                      CHAR(1),
     cons_state                       CHAR(1),
     cons_date                        TIMESTAMP,
     cons_id                          VARCHAR(32),
     /*Original table DDL below*/  

     RPL_DDL_COL 

     /*Original Distyle/Sortkey below*/
 )
     RPL_DDL_KEY
 ;


insert into audit.cons_RPL_TABLE_NAME
    (  current_id
     , cons_list
     , cons_status
     , cons_state
     , cons_date
     , cons_id
     , RPL_COLUMN_LIST

)

with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from RPL_TMP_SCHEMA_NAME.cons_RPL_TABLE_NAME_fm_temp fmt
)

SELECT DISTINCT
      cast(cfm.current_id AS BIGINT) AS current_id
    , cast(NULL AS VARCHAR(128))     AS cons_list
    , cast(NULL AS CHAR(1)     )     AS cons_status
    , cast('B'  AS CHAR(1)     )     AS cons_state
    , sysdate                        AS consolidation_date
    , upper(md5(sysdate))            AS consolidation_id
    
    /*List of all columns from original table below.*/
    , RPL_COLUMN_LIST

FROM RPL_SCHEMA_NAME.RPL_TABLE_NAME main
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
    , RPL_COLUMN_LIST

FROM RPL_TMP_SCHEMA_NAME.cons_RPL_TABLE_NAME_temp

;


/*CREATE TABLE IF NOT EXISTS RPL_SCHEMA_NAME.RPL_TABLE_NAME_nightly (LIKE RPL_SCHEMA_NAME.RPL_TABLE_NAME);

TRUNCATE TABLE RPL_SCHEMA_NAME.RPL_TABLE_NAME_nightly;
INSERT INTO RPL_SCHEMA_NAME.RPL_TABLE_NAME_nightly
    SELECT
        *
    FROM RPL_SCHEMA_NAME.RPL_TABLE_NAME;*/



delete from RPL_SCHEMA_NAME.RPL_TABLE_NAME
 where 1=1
    AND exists (select null from audit.cons_RPL_TABLE_NAME ex0 where ex0.individual_id = RPL_SCHEMA_NAME.RPL_TABLE_NAME.individual_id );


 insert into RPL_SCHEMA_NAME.RPL_TABLE_NAME
 select
     
     /*ONLY original columns. Since RPL_TMP_SCHEMA_NAME.cons_RPL_TABLE_NAME_temp table contains extra for audit*/
     RPL_COLUMN_LIST


    FROM RPL_TMP_SCHEMA_NAME.cons_RPL_TABLE_NAME_temp;

analyze RPL_SCHEMA_NAME.RPL_TABLE_NAME;