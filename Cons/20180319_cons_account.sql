/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Consolidation
* Module Name:  prod.account.sql
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

    CREATE TABLE prod.etalon_account as SELECT * FROM prod.account;

Run inbetween runs for QA:

    truncate table prod.account;
    insert into prod.account select * from prod.etalon_account;

    drop table if exists audit.cons_account;

*/

/***************************************************************************/
--Former_ID temp table, finding all nonsurvivor (former_id) individual_id in target table
/***************************************************************************/



drop table if exists    agg.cons_account_fm_temp;
create table            agg.cons_account_fm_temp
    DISTSTYLE KEY
    DISTKEY (current_id)
    SORTKEY (current_id, former_id)
    as
select
        idx.current_id
        , idx.former_id

    from prod.id_xref idx
    where exists (select null
                  from prod.account ex0
                  where idx.former_id = ex0.individual_id

                 )
        and idx.id_type = 'I';

analyze agg.cons_account_fm_temp;

/***************************************************************************/
--Former_ID check, used in automation. If > 0 then Continue, else Stop
/***************************************************************************/

    /* Used in automation only, first step */
    /*
    SELECT  count(*) as former_id_match_cnt
    FROM    agg.cons_account_fm_temp;
    */

/***************************************************************************/
--Main Body, temp table contains final result of consolidation
/***************************************************************************/

drop table if exists    agg.cons_account_temp;
create table            agg.cons_account_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    as
 with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_account_fm_temp fmt

), cte_precons_upd /*Custom pre-Cons update goes here*/ AS (

    SELECT DISTINCT
      hash_account_id, source_account_id, source_name, account_subtype_code, feed_name, individual_id, account_type_code, account_type_desc, first_account_date, name, company, division, department, prefix_name, title, first_name, middle_name, last_name, suffix_name, nickname, prof_suffix, address_1, address_2, city_name, state_province, zip_code, zip4, preferred_city_code, name_type, country_code, email, phone_num, salutation_casual, salutation_formal, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator, account_match_hash
    , cfm.current_id
FROM prod.account main
    inner join cte_former_match cfm
        /*Picking out only B (Before) rows for pair of survivor+nonsurvivor i.e. current+former*/
        on (main.individual_id = cfm.current_id or main.individual_id = cfm.former_id)

), cte_cons_eva as (

    SELECT
          current_id
        , hash_account_id, source_account_id, source_name, account_subtype_code, feed_name, individual_id, account_type_code, account_type_desc, first_account_date, name, company, division, department, prefix_name, title, first_name, middle_name, last_name, suffix_name, nickname, prof_suffix, address_1, address_2, city_name, state_province, zip_code, zip4, preferred_city_code, name_type, country_code, email, phone_num, salutation_casual, salutation_formal, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator, account_match_hash

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

    , cse.hash_account_id

    , /*Default Value Rule*/ first_value(source_account_id)       over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as source_account_id
    , /*Default Value Rule*/ first_value(source_name)             over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as source_name
    , /*Default Value Rule*/ first_value(account_subtype_code)    over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as account_subtype_code
    , /*Default Value Rule*/ first_value(feed_name)               over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as feed_name
    /*, *//*Custom*//*             first_value(individual_id)           over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as individual_id*/
    , /*Default Value Rule*/ first_value(account_type_code)       over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as account_type_code
    , /*Default Value Rule*/ first_value(account_type_desc)       over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as account_type_desc
    , /*Default Value Rule*/ first_value(first_account_date)      over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as first_account_date
    , /*Default Value Rule*/ first_value(name)                    over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as name
    , /*Default Value Rule*/ first_value(company)                 over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as company
    , /*Default Value Rule*/ first_value(division)                over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as division
    , /*Default Value Rule*/ first_value(department)              over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as department
    , /*Default Value Rule*/ first_value(prefix_name)             over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as prefix_name
    , /*Default Value Rule*/ first_value(title)                   over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as title
    , /*Default Value Rule*/ first_value(first_name)              over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as first_name
    , /*Default Value Rule*/ first_value(middle_name)             over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as middle_name
    , /*Default Value Rule*/ first_value(last_name)               over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as last_name
    , /*Default Value Rule*/ first_value(suffix_name)             over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as suffix_name
    , /*Default Value Rule*/ first_value(nickname)                over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as nickname
    , /*Default Value Rule*/ first_value(prof_suffix)             over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as prof_suffix
    , /*Default Value Rule*/ first_value(address_1)               over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as address_1
    , /*Default Value Rule*/ first_value(address_2)               over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as address_2
    , /*Default Value Rule*/ first_value(city_name)               over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as city_name
    , /*Default Value Rule*/ first_value(state_province)          over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as state_province
    , /*Default Value Rule*/ first_value(zip_code)                over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as zip_code
    , /*Default Value Rule*/ first_value(zip4)                    over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as zip4
    , /*Default Value Rule*/ first_value(preferred_city_code)     over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as preferred_city_code
    , /*Default Value Rule*/ first_value(name_type)               over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as name_type
    , /*Default Value Rule*/ first_value(country_code)            over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as country_code
    , /*Default Value Rule*/ first_value(email)                   over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as email
    , /*Default Value Rule*/ first_value(phone_num)               over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as phone_num
    , /*Default Value Rule*/ first_value(salutation_casual)       over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as salutation_casual
    , /*Default Value Rule*/ first_value(salutation_formal)       over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as salutation_formal
    , /*Default Value Rule*/ first_value(create_date)             over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_date
    , /*Default Value Rule*/ first_value(create_feed_instance_id) over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_feed_instance_id
    , /*Default Value Rule*/ first_value(update_date)             over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_date
    , /*Default Value Rule*/ first_value(update_feed_instance_id) over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_feed_instance_id
    , cast(null as varchar(1)) as historical_record_indicator
    , /*Default Value Rule*/ first_value(account_match_hash)      over ( PARTITION BY hash_account_id ORDER BY 1 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as account_match_hash


    , listagg( distinct cse.individual_id,',') WITHIN GROUP (order by cse.individual_id = cse.current_id desc) over ( partition by hash_account_id ) as cons_list

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

analyze agg.cons_account_temp;



 CREATE TABLE IF NOT EXISTS audit.cons_account (
     /*Table will be made only once*/

     current_id                       BIGINT,
     cons_list                        VARCHAR(128),
     cons_status                      CHAR(1),
     cons_state                       CHAR(1),
     cons_date                        TIMESTAMP,
     cons_id                          VARCHAR(32),
     /*Original table DDL below*/

     hash_account_id VARCHAR(64) NOT NULL  ENCODE lzo ,source_account_id VARCHAR(64) NOT NULL  ENCODE lzo ,source_name VARCHAR(64) NOT NULL  ENCODE lzo ,account_subtype_code VARCHAR(20) NOT NULL  ENCODE bytedict ,feed_name VARCHAR(64) NOT NULL  ENCODE lzo ,individual_id BIGINT   ENCODE mostly32 ,account_type_code VARCHAR(20)   ENCODE lzo ,account_type_desc VARCHAR(100) NOT NULL  ENCODE lzo ,first_account_date TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo ,name VARCHAR(100)   ENCODE lzo ,company VARCHAR(100)   ENCODE lzo ,division VARCHAR(10)   ENCODE bytedict ,department VARCHAR(50)   ENCODE lzo ,prefix_name VARCHAR(60)   ENCODE lzo ,title VARCHAR(60)   ENCODE lzo ,first_name VARCHAR(60)   ENCODE lzo ,middle_name VARCHAR(60)   ENCODE lzo ,last_name VARCHAR(60)   ENCODE lzo ,suffix_name VARCHAR(60)   ENCODE lzo ,nickname VARCHAR(60)   ENCODE lzo ,prof_suffix VARCHAR(60)   ENCODE lzo ,address_1 VARCHAR(150)   ENCODE lzo ,address_2 VARCHAR(100)   ENCODE lzo ,city_name VARCHAR(100)   ENCODE text32k ,state_province VARCHAR(20)   ENCODE bytedict ,zip_code VARCHAR(20)   ENCODE lzo ,zip4 VARCHAR(10)   ENCODE lzo ,preferred_city_code VARCHAR(10)   ENCODE lzo ,name_type VARCHAR(10)   ENCODE lzo ,country_code VARCHAR(64)   ENCODE lzo ,email VARCHAR(255)   ENCODE lzo ,phone_num VARCHAR(20)   ENCODE lzo ,salutation_casual VARCHAR(200)   ENCODE lzo ,salutation_formal VARCHAR(200)   ENCODE lzo ,create_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE lzo ,create_feed_instance_id VARCHAR(64) NOT NULL  ENCODE lzo ,update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo ,update_feed_instance_id VARCHAR(64)   ENCODE lzo ,historical_record_indicator CHAR(1)   ENCODE lzo ,account_match_hash VARCHAR(64)   ENCODE lzo

     /*Original Distyle/Sortkey below*/
 )
     DISTSTYLE KEY DISTKEY (hash_account_id) INTERLEAVED SORTKEY ( source_name , account_type_code , account_subtype_code , feed_name , source_account_id , individual_id )
 ;


insert into audit.cons_account
    (  current_id
     , cons_list
     , cons_status
     , cons_state
     , cons_date
     , cons_id
     , hash_account_id, source_account_id, source_name, account_subtype_code, feed_name, individual_id, account_type_code, account_type_desc, first_account_date, name, company, division, department, prefix_name, title, first_name, middle_name, last_name, suffix_name, nickname, prof_suffix, address_1, address_2, city_name, state_province, zip_code, zip4, preferred_city_code, name_type, country_code, email, phone_num, salutation_casual, salutation_formal, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator, account_match_hash

)

with cte_former_match as (

    select fmt.current_id, fmt.former_id
    from agg.cons_account_fm_temp fmt
)

SELECT DISTINCT
      cast(cfm.current_id AS BIGINT) AS current_id
    , cast(NULL AS VARCHAR(128))     AS cons_list
    , cast(NULL AS CHAR(1)     )     AS cons_status
    , cast('B'  AS CHAR(1)     )     AS cons_state
    , sysdate                        AS consolidation_date
    , upper(md5(sysdate))            AS consolidation_id

    /*List of all columns from original table below.*/
    , hash_account_id, source_account_id, source_name, account_subtype_code, feed_name, individual_id, account_type_code, account_type_desc, first_account_date, name, company, division, department, prefix_name, title, first_name, middle_name, last_name, suffix_name, nickname, prof_suffix, address_1, address_2, city_name, state_province, zip_code, zip4, preferred_city_code, name_type, country_code, email, phone_num, salutation_casual, salutation_formal, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator, account_match_hash

FROM prod.account main
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
    , hash_account_id, source_account_id, source_name, account_subtype_code, feed_name, individual_id, account_type_code, account_type_desc, first_account_date, name, company, division, department, prefix_name, title, first_name, middle_name, last_name, suffix_name, nickname, prof_suffix, address_1, address_2, city_name, state_province, zip_code, zip4, preferred_city_code, name_type, country_code, email, phone_num, salutation_casual, salutation_formal, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator, account_match_hash

FROM agg.cons_account_temp

;


/*CREATE TABLE IF NOT EXISTS prod.account_nightly (LIKE prod.account);

TRUNCATE TABLE prod.account_nightly;
INSERT INTO prod.account_nightly
    SELECT
        *
    FROM prod.account;*/



delete from prod.account
 where 1=1
    AND exists (select null from audit.cons_account ex0 where ex0.individual_id = prod.account.individual_id );


 insert into prod.account
 select

     /*ONLY original columns. Since agg.cons_account_temp table contains extra for audit*/
     hash_account_id, source_account_id, source_name, account_subtype_code, feed_name, individual_id, account_type_code, account_type_desc, first_account_date, name, company, division, department, prefix_name, title, first_name, middle_name, last_name, suffix_name, nickname, prof_suffix, address_1, address_2, city_name, state_province, zip_code, zip4, preferred_city_code, name_type, country_code, email, phone_num, salutation_casual, salutation_formal, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator, account_match_hash


    FROM agg.cons_account_temp;

analyze prod.account;
