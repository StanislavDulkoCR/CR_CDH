/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Consolidation
* Module Name:  prod.individual.sql
* Date       :  2018/03/22
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

    CREATE TABLE prod.etalon_individual as SELECT * FROM prod.individual;

Run inbetween runs for QA:

    truncate table prod.individual;
    insert into prod.individual select * from prod.etalon_individual;

    drop table if exists audit.cons_individual;

*/

/***************************************************************************/
--Former_ID temp table, finding all nonsurvivor (former_id) individual_id in target table
/***************************************************************************/



drop table if exists    agg.cons_individual_fm_temp;
create table            agg.cons_individual_fm_temp
    DISTSTYLE KEY
    DISTKEY (current_id)
    SORTKEY (current_id, former_id)
    as
select
        idx.current_id
        , idx.former_id

    from prod.id_xref idx
    where exists (select null
                  from prod.individual ex0
                  where idx.former_id = ex0.individual_id

                 )
        and idx.id_type = 'I';

analyze agg.cons_individual_fm_temp;

/***************************************************************************/
--Former_ID check, used in automation. If > 0 then Continue, else Stop
/***************************************************************************/

    /* Used in automation only, first step */
    /*
    SELECT  count(*) as former_id_match_cnt
    FROM    agg.cons_individual_fm_temp;
    */

/***************************************************************************/
--Main Body, temp table contains final result of consolidation
/***************************************************************************/

drop table if exists    agg.cons_individual_temp;
create table            agg.cons_individual_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    as
 with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_individual_fm_temp fmt

), cte_precons_upd /*Custom pre-Cons update goes here*/ AS (

    SELECT DISTINCT
      individual_id, household_id, acx_name_parsing_flag, acx_full_name, acx_prefix_title, acx_first_name, acx_middle_name, acx_lastname_prefix, acx_lastname, acx_original_maiden_name, acx_name_suffix, acx_other_given_name, acx_other_title, acx_prefix_title_code, acx_prof_title, acx_gender, acx_resolved_gender, acx_namecheck_code, acx_namecheck_code2, acx_namecheck_code3, acx_dirty_word_indicator, acx_deceased_data_flag, acx_bankruptcy_data_flag, acx_large_prison_flag, individual_match_hash, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator
    , cfm.current_id
FROM prod.individual main
    inner join cte_former_match cfm
        /*Picking out only B (Before) rows for pair of survivor+nonsurvivor i.e. current+former*/
        on (main.individual_id = cfm.current_id or main.individual_id = cfm.former_id)

), cte_cons_eva as (

    SELECT
          current_id
        , individual_id, household_id, acx_name_parsing_flag, acx_full_name, acx_prefix_title, acx_first_name, acx_middle_name, acx_lastname_prefix, acx_lastname, acx_original_maiden_name, acx_name_suffix, acx_other_given_name, acx_other_title, acx_prefix_title_code, acx_prof_title, acx_gender, acx_resolved_gender, acx_namecheck_code, acx_namecheck_code2, acx_namecheck_code3, acx_dirty_word_indicator, acx_deceased_data_flag, acx_bankruptcy_data_flag, acx_large_prison_flag, individual_match_hash, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator

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
      /*Default Value Rule*/ first_value(household_id)                         over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as household_id
    , /*Default Value Rule*/ first_value(acx_name_parsing_flag)                over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_name_parsing_flag
    , /*Default Value Rule*/ first_value(acx_full_name)                        over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_full_name
    , /*Default Value Rule*/ first_value(acx_prefix_title)                     over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_prefix_title
    , /*Default Value Rule*/ first_value(acx_first_name)                       over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_first_name
    , /*Default Value Rule*/ first_value(acx_middle_name)                      over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_middle_name
    , /*Default Value Rule*/ first_value(acx_lastname_prefix)                  over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_lastname_prefix
    , /*Default Value Rule*/ first_value(acx_lastname)                         over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_lastname
    , /*Default Value Rule*/ first_value(acx_original_maiden_name)             over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_original_maiden_name
    , /*Default Value Rule*/ first_value(acx_name_suffix)                      over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_name_suffix
    , /*Default Value Rule*/ first_value(acx_other_given_name)                 over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_other_given_name
    , /*Default Value Rule*/ first_value(acx_other_title)                      over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_other_title
    , /*Default Value Rule*/ first_value(acx_prefix_title_code)                over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_prefix_title_code
    , /*Default Value Rule*/ first_value(acx_prof_title)                       over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_prof_title
    , /*Default Value Rule*/ first_value(acx_gender)                           over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_gender
    , /*Default Value Rule*/ first_value(acx_resolved_gender)                  over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_resolved_gender
    , /*Default Value Rule*/ first_value(acx_namecheck_code)                   over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_namecheck_code
    , /*Default Value Rule*/ first_value(acx_namecheck_code2)                  over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_namecheck_code2
    , /*Default Value Rule*/ first_value(acx_namecheck_code3)                  over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_namecheck_code3
    , /*Default Value Rule*/ first_value(acx_dirty_word_indicator)             over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_dirty_word_indicator
    , /*Default Value Rule*/ first_value(acx_deceased_data_flag)               over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_deceased_data_flag
    , /*Default Value Rule*/ first_value(acx_bankruptcy_data_flag)             over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_bankruptcy_data_flag
    , /*Default Value Rule*/ first_value(acx_large_prison_flag)                over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_large_prison_flag
    , /*Default Value Rule*/ first_value(individual_match_hash)                over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as individual_match_hash
    , /*Default Value Rule*/ first_value(create_date)                          over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_date
    , /*Default Value Rule*/ first_value(create_feed_instance_id)              over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_feed_instance_id
    , /*Default Value Rule*/ first_value(update_date)                          over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_date
    , /*Default Value Rule*/ first_value(update_feed_instance_id)              over ( PARTITION BY current_id ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_feed_instance_id
    , cast(null as varchar(1)) as historical_record_indicator


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

analyze agg.cons_individual_temp;



 CREATE TABLE IF NOT EXISTS audit.cons_individual (
     /*Table will be made only once*/

     current_id                       BIGINT,
     cons_list                        VARCHAR(128),
     cons_status                      CHAR(1),
     cons_state                       CHAR(1),
     cons_date                        TIMESTAMP,
     cons_id                          VARCHAR(32),
     /*Original table DDL below*/

     individual_id BIGINT NOT NULL ,household_id BIGINT   ENCODE lzo ,acx_name_parsing_flag CHAR(1)   ENCODE lzo ,acx_full_name VARCHAR(100)   ENCODE lzo ,acx_prefix_title VARCHAR(30)   ENCODE lzo ,acx_first_name VARCHAR(30)   ENCODE lzo ,acx_middle_name VARCHAR(30)   ENCODE lzo ,acx_lastname_prefix VARCHAR(30)   ENCODE lzo ,acx_lastname VARCHAR(30)   ENCODE lzo ,acx_original_maiden_name VARCHAR(30)   ENCODE lzo ,acx_name_suffix VARCHAR(20)   ENCODE lzo ,acx_other_given_name VARCHAR(30)   ENCODE lzo ,acx_other_title VARCHAR(50)   ENCODE lzo ,acx_prefix_title_code CHAR(1)   ENCODE lzo ,acx_prof_title VARCHAR(15)   ENCODE lzo ,acx_gender CHAR(1)   ENCODE lzo ,acx_resolved_gender CHAR(1)   ENCODE lzo ,acx_namecheck_code CHAR(3)   ENCODE lzo ,acx_namecheck_code2 CHAR(3)   ENCODE lzo ,acx_namecheck_code3 CHAR(3)   ENCODE lzo ,acx_dirty_word_indicator CHAR(1)   ENCODE lzo ,acx_deceased_data_flag CHAR(1)   ENCODE lzo ,acx_bankruptcy_data_flag CHAR(1)   ENCODE lzo ,acx_large_prison_flag CHAR(1)   ENCODE lzo ,individual_match_hash VARCHAR(64)   ENCODE lzo ,create_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE lzo ,create_feed_instance_id VARCHAR(64) NOT NULL  ENCODE lzo ,update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo ,update_feed_instance_id VARCHAR(64)   ENCODE lzo ,historical_record_indicator CHAR(1)   ENCODE lzo ,PRIMARY KEY (individual_id)

     /*Original Distyle/Sortkey below*/
 )
     DISTSTYLE KEY DISTKEY (individual_id) INTERLEAVED SORTKEY ( individual_id )
 ;


insert into audit.cons_individual
    (  current_id
     , cons_list
     , cons_status
     , cons_state
     , cons_date
     , cons_id
     , individual_id, household_id, acx_name_parsing_flag, acx_full_name, acx_prefix_title, acx_first_name, acx_middle_name, acx_lastname_prefix, acx_lastname, acx_original_maiden_name, acx_name_suffix, acx_other_given_name, acx_other_title, acx_prefix_title_code, acx_prof_title, acx_gender, acx_resolved_gender, acx_namecheck_code, acx_namecheck_code2, acx_namecheck_code3, acx_dirty_word_indicator, acx_deceased_data_flag, acx_bankruptcy_data_flag, acx_large_prison_flag, individual_match_hash, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator

)

with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_individual_fm_temp fmt
)

SELECT DISTINCT
      cast(cfm.current_id AS BIGINT) AS current_id
    , cast(NULL AS VARCHAR(128))     AS cons_list
    , cast(NULL AS CHAR(1)     )     AS cons_status
    , cast('B'  AS CHAR(1)     )     AS cons_state
    , sysdate                        AS consolidation_date
    , upper(md5(sysdate))            AS consolidation_id

    /*List of all columns from original table below.*/
    , individual_id, household_id, acx_name_parsing_flag, acx_full_name, acx_prefix_title, acx_first_name, acx_middle_name, acx_lastname_prefix, acx_lastname, acx_original_maiden_name, acx_name_suffix, acx_other_given_name, acx_other_title, acx_prefix_title_code, acx_prof_title, acx_gender, acx_resolved_gender, acx_namecheck_code, acx_namecheck_code2, acx_namecheck_code3, acx_dirty_word_indicator, acx_deceased_data_flag, acx_bankruptcy_data_flag, acx_large_prison_flag, individual_match_hash, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator

FROM prod.individual main
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
    , individual_id, household_id, acx_name_parsing_flag, acx_full_name, acx_prefix_title, acx_first_name, acx_middle_name, acx_lastname_prefix, acx_lastname, acx_original_maiden_name, acx_name_suffix, acx_other_given_name, acx_other_title, acx_prefix_title_code, acx_prof_title, acx_gender, acx_resolved_gender, acx_namecheck_code, acx_namecheck_code2, acx_namecheck_code3, acx_dirty_word_indicator, acx_deceased_data_flag, acx_bankruptcy_data_flag, acx_large_prison_flag, individual_match_hash, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator

FROM agg.cons_individual_temp

;


/*CREATE TABLE IF NOT EXISTS prod.individual_nightly (LIKE prod.individual);

TRUNCATE TABLE prod.individual_nightly;
INSERT INTO prod.individual_nightly
    SELECT
        *
    FROM prod.individual;*/



delete from prod.individual
 where 1=1
    AND exists (select null from audit.cons_individual ex0 where ex0.individual_id = prod.individual.individual_id );


 insert into prod.individual
 select

     /*ONLY original columns. Since agg.cons_individual_temp table contains extra for audit*/
     individual_id, household_id, acx_name_parsing_flag, acx_full_name, acx_prefix_title, acx_first_name, acx_middle_name, acx_lastname_prefix, acx_lastname, acx_original_maiden_name, acx_name_suffix, acx_other_given_name, acx_other_title, acx_prefix_title_code, acx_prof_title, acx_gender, acx_resolved_gender, acx_namecheck_code, acx_namecheck_code2, acx_namecheck_code3, acx_dirty_word_indicator, acx_deceased_data_flag, acx_bankruptcy_data_flag, acx_large_prison_flag, individual_match_hash, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator


    FROM agg.cons_individual_temp;

analyze prod.individual;