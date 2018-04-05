/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Consolidation
* Module Name:  prod.email_response.sql
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

    CREATE TABLE prod.etalon_email_response as SELECT * FROM prod.email_response;

Run inbetween runs for QA:

    truncate table prod.email_response;
    insert into prod.email_response select * from prod.etalon_email_response;

    drop table if exists audit.cons_email_response;

*/

/***************************************************************************/
--Former_ID temp table, finding all nonsurvivor (former_id) individual_id in target table
/***************************************************************************/



drop table if exists    agg.cons_email_response_fm_temp;
create table            agg.cons_email_response_fm_temp
    DISTSTYLE KEY
    DISTKEY (current_id)
    SORTKEY (current_id, former_id)
    as
select
        idx.current_id
        , idx.former_id

    from prod.id_xref idx
    where exists (select null
                  from prod.email_response ex0
                  where idx.former_id = ex0.individual_id

                 )
        and idx.id_type = 'I';

analyze agg.cons_email_response_fm_temp;

/***************************************************************************/
--Former_ID check, used in automation. If > 0 then Continue, else Stop
/***************************************************************************/

    /* Used in automation only, first step */
    /*
    SELECT  count(*) as former_id_match_cnt
    FROM    agg.cons_email_response_fm_temp;
    */

/***************************************************************************/
--Main Body, temp table contains final result of consolidation
/***************************************************************************/

drop table if exists    agg.cons_email_response_temp;
create table            agg.cons_email_response_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    as
 with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_email_response_fm_temp fmt

), cte_precons_upd /*Custom pre-Cons update goes here*/ AS (

    SELECT DISTINCT
      individual_id, email, member_id, cell_id, catalog_type, legacy_individual_id, campaign_id, campaign_name, opened_flag, bounced_flag, clicked, cell_subject, keycode, list_id, list_desc, campaign_optout, email_finder_num, message_id, create_date, create_feed_instance_id, update_date, update_feed_instance_id
    , cfm.current_id
FROM prod.email_response main
    inner join cte_former_match cfm
        /*Picking out only B (Before) rows for pair of survivor+nonsurvivor i.e. current+former*/
        on (main.individual_id = cfm.current_id or main.individual_id = cfm.former_id)

), cte_cons_eva as (

    SELECT
          current_id
        , individual_id, email, member_id, cell_id, catalog_type, legacy_individual_id, campaign_id, campaign_name, opened_flag, bounced_flag, clicked, cell_subject, keycode, list_id, list_desc, campaign_optout, email_finder_num, message_id, create_date, create_feed_instance_id, update_date, update_feed_instance_id

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


    , /*cse.individual_id, */ cse.email, cse.member_id, cse.cell_id, cse.catalog_type
    , /*Default Value Rule*/ first_value(legacy_individual_id)    over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as legacy_individual_id
    , /*Default Value Rule*/ first_value(campaign_id)             over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as campaign_id
    , /*Default Value Rule*/ first_value(campaign_name)           over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as campaign_name
    , /*Custom*/             first_value(opened_flag)             over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY cse.opened_flag  DESC                                                                                                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as opened_flag
    , /*Custom*/             first_value(bounced_flag)            over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY cse.bounced_flag DESC                                                                                                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as bounced_flag
    , /*Custom*/             first_value(clicked)                 over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY cse.clicked      DESC                                                                                                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as clicked
    , /*Default Value Rule*/ first_value(cell_subject)            over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as cell_subject
    , /*Default Value Rule*/ first_value(keycode)                 over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as keycode
    , /*Default Value Rule*/ first_value(list_id)                 over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as list_id
    , /*Default Value Rule*/ first_value(list_desc)               over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as list_desc
    , /*Default Value Rule*/ first_value(campaign_optout)         over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as campaign_optout
    , /*Default Value Rule*/ first_value(email_finder_num)        over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as email_finder_num
    , /*Default Value Rule*/ first_value(message_id)              over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as message_id
    , /*Default Value Rule*/ first_value(create_date)             over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_date
    , /*Default Value Rule*/ first_value(create_feed_instance_id) over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_feed_instance_id
    , /*Default Value Rule*/ first_value(update_date)             over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_date
    , /*Default Value Rule*/ first_value(update_feed_instance_id) over ( PARTITION BY current_id, email, member_id, cell_id, catalog_type ORDER BY default_row_01 DESC/*, default_row_02 DESC/ASC, default_row_03 DESC/ASC, default_row_04 DESC/ASC, default_row_05 DESC/ASC*/ ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_feed_instance_id


    , listagg( distinct cse.individual_id,',') WITHIN GROUP (order by cse.individual_id = cse.current_id desc) over ( partition by current_id, email, member_id, cell_id, catalog_type ) as cons_list

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

analyze agg.cons_email_response_temp;



 CREATE TABLE IF NOT EXISTS audit.cons_email_response (
     /*Table will be made only once*/

     current_id                       BIGINT,
     cons_list                        VARCHAR(128),
     cons_status                      CHAR(1),
     cons_state                       CHAR(1),
     cons_date                        TIMESTAMP,
     cons_id                          VARCHAR(32),
     /*Original table DDL below*/

     individual_id BIGINT NOT NULL  ENCODE lzo ,email VARCHAR(255) NOT NULL  ENCODE lzo ,member_id NUMERIC(10,0) NOT NULL  ENCODE lzo ,cell_id VARCHAR(10) NOT NULL  ENCODE lzo ,catalog_type CHAR(5) NOT NULL  ENCODE lzo ,legacy_individual_id BIGINT   ENCODE lzo ,campaign_id BIGINT   ENCODE lzo ,campaign_name VARCHAR(250)   ENCODE lzo ,opened_flag NUMERIC(1,0)   ENCODE lzo ,bounced_flag NUMERIC(1,0)   ENCODE lzo ,clicked NUMERIC(1,0)   ENCODE lzo ,cell_subject VARCHAR(256)   ENCODE lzo ,keycode VARCHAR(30)   ENCODE lzo ,list_id INTEGER   ENCODE lzo ,list_desc VARCHAR(256)   ENCODE lzo ,campaign_optout CHAR(1) ,email_finder_num INTEGER ,message_id INTEGER ,create_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE lzo ,create_feed_instance_id VARCHAR(64) NOT NULL  ENCODE runlength ,update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo ,update_feed_instance_id VARCHAR(64)   ENCODE runlength

     /*Original Distyle/Sortkey below*/
 )
     DISTSTYLE KEY DISTKEY (individual_id) INTERLEAVED SORTKEY ( cell_id , catalog_type , campaign_id , individual_id , member_id )
 ;


insert into audit.cons_email_response
    (  current_id
     , cons_list
     , cons_status
     , cons_state
     , cons_date
     , cons_id
     , individual_id, email, member_id, cell_id, catalog_type, legacy_individual_id, campaign_id, campaign_name, opened_flag, bounced_flag, clicked, cell_subject, keycode, list_id, list_desc, campaign_optout, email_finder_num, message_id, create_date, create_feed_instance_id, update_date, update_feed_instance_id

)

with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_email_response_fm_temp fmt
)

SELECT DISTINCT
      cast(cfm.current_id AS BIGINT) AS current_id
    , cast(NULL AS VARCHAR(128))     AS cons_list
    , cast(NULL AS CHAR(1)     )     AS cons_status
    , cast('B'  AS CHAR(1)     )     AS cons_state
    , sysdate                        AS consolidation_date
    , upper(md5(sysdate))            AS consolidation_id

    /*List of all columns from original table below.*/
    , individual_id, email, member_id, cell_id, catalog_type, legacy_individual_id, campaign_id, campaign_name, opened_flag, bounced_flag, clicked, cell_subject, keycode, list_id, list_desc, campaign_optout, email_finder_num, message_id, create_date, create_feed_instance_id, update_date, update_feed_instance_id

FROM prod.email_response main
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
    , individual_id, email, member_id, cell_id, catalog_type, legacy_individual_id, campaign_id, campaign_name, opened_flag, bounced_flag, clicked, cell_subject, keycode, list_id, list_desc, campaign_optout, email_finder_num, message_id, create_date, create_feed_instance_id, update_date, update_feed_instance_id

FROM agg.cons_email_response_temp

;


/*CREATE TABLE IF NOT EXISTS prod.email_response_nightly (LIKE prod.email_response);

TRUNCATE TABLE prod.email_response_nightly;
INSERT INTO prod.email_response_nightly
    SELECT
        *
    FROM prod.email_response;*/



delete from prod.email_response
 where 1=1
    AND exists (select null from audit.cons_email_response ex0 where ex0.individual_id = prod.email_response.individual_id );


 insert into prod.email_response
 select

     /*ONLY original columns. Since agg.cons_email_response_temp table contains extra for audit*/
     individual_id, email, member_id, cell_id, catalog_type, legacy_individual_id, campaign_id, campaign_name, opened_flag, bounced_flag, clicked, cell_subject, keycode, list_id, list_desc, campaign_optout, email_finder_num, message_id, create_date, create_feed_instance_id, update_date, update_feed_instance_id


    FROM agg.cons_email_response_temp;

analyze prod.email_response;
