/***************************************************************************/
--Former_ID check, used in automation. If > 0 then Continue, else Stop
/***************************************************************************/

/* Used in automation only, first step */
/*
SELECT  count(*) as former_id_match_cnt
FROM    agg.cons_individual_email_fm_temp;
*/

/***************************************************************************/
--Main Body, temp table contains final result of consolidation
/***************************************************************************/

DROP TABLE IF EXISTS agg.cons_individual_email_temp
;

CREATE TABLE agg.cons_individual_email_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id) AS WITH cte_former_match AS (

    SELECT
        fmt.current_id
        , fmt.former_id
    FROM agg.cons_individual_email_fm_temp fmt

), cte_precons_upd /*Custom pre-Cons update goes here*/ AS (

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
        , cfm.current_id
    FROM prod.individual_email main INNER JOIN cte_former_match cfm
        /*Picking out only B (Before) rows for pair of survivor+nonsurvivor i.e. current+former*/
            ON (main.individual_id = cfm.current_id OR main.individual_id = cfm.former_id)

), cte_cons_eva AS (

    SELECT
        current_id
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

        /*Custom Default Value Rule below. min/max a handled in next CTE as asc/desc respectively*/
        , 1 AS default_row_01
        , 1 AS default_row_02
        , 1 AS default_row_03
        , 1 AS default_row_04
        , 1 AS default_row_05


    FROM cte_precons_upd upd



), cte_cons_rnk AS (

    SELECT DISTINCT
        /*SD: must be distinct due window function usage*/

          cast(cse.current_id AS BIGINT)               AS individual_id

        , /*cse.individual_id, */ cse.email_subtype
        , /*Default Value Rule*/ first_value(email_address)                    over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as email_address
        , /*Default Value Rule*/ first_value(data_source)                      over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as data_source
        , /*Default Value Rule*/ first_value(acx_dma_email_flag)               over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_dma_email_flag
        , /*Default Value Rule*/ first_value(acx_valid_email_mailability_flag) over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_valid_email_mailability_flag
        , /*Default Value Rule*/ first_value(acx_valid_email_flag)             over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as acx_valid_email_flag
        , /*Default Value Rule*/ first_value(valid_flag)                       over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as valid_flag
        , /*Default Value Rule*/ first_value(source_deliverability_indicator)  over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as source_deliverability_indicator
        , /*Default Value Rule*/ first_value(source_valid_flag)                over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as source_valid_flag
        , /*Default Value Rule*/ first_value(email_client_app)                 over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as email_client_app
        , /*Custom*/             first_value(first_date)                       over ( PARTITION BY current_id, email_subtype ORDER BY cse.first_date ASC                                                                                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as first_date
        , /*Default Value Rule*/ first_value(eff_date)                         over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as eff_date
        , /*Default Value Rule*/ first_value(end_date)                         over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as end_date
        , /*Default Value Rule*/ first_value(individual_email_match_hash)      over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as individual_email_match_hash
        , /*Default Value Rule*/ first_value(create_date)                      over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_date
        , /*Default Value Rule*/ first_value(create_feed_instance_id)          over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as create_feed_instance_id
        , /*Default Value Rule*/ first_value(update_date)                      over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_date
        , /*Default Value Rule*/ first_value(update_feed_instance_id)          over ( PARTITION BY current_id, email_subtype ORDER BY default_row_01 DESC, default_row_02 ASC, default_row_03 ASC, default_row_04 DESC, default_row_05 DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as update_feed_instance_id
        , cast(null as varchar(1)) as historical_record_indicator


        , listagg(DISTINCT cse.individual_id, ',')
          WITHIN GROUP (ORDER BY cse.individual_id = cse.current_id DESC)
          OVER (
              PARTITION BY current_id, email_subtype ) AS cons_list

    FROM cte_cons_eva cse

), cte_fin_union AS (

    SELECT
        *
        , CASE WHEN len(cons_list) > 10
        THEN 'M' /*Merge                            */
          WHEN cons_list = cast(individual_id AS BIGINT)
              THEN 'C' /*Current, No change               */
          WHEN cons_list <> cast(individual_id AS BIGINT)
              THEN 'U' /*Update individual_id , No merge  */
          END cons_status

    FROM cte_cons_rnk

)

SELECT *
FROM cte_fin_union

;

ANALYZE agg.cons_individual_email_temp
;


CREATE TABLE IF NOT EXISTS audit.cons_individual_email (/*Table will be made only once*/

    current_id                       BIGINT,
    former_id                        BIGINT,
    cons_list                        VARCHAR(128),
    cons_status                      CHAR(1),
    cons_state                       CHAR(1),
    cons_date                        TIMESTAMP,
    cons_id                          VARCHAR(32), /*Original table DDL below*/

    individual_id                    BIGINT                      NOT NULL ENCODE DELTA,
    email_subtype                    VARCHAR(64)                 NOT NULL ENCODE LZO,
    email_address                    VARCHAR(255) ENCODE LZO,
    data_source                      VARCHAR(55)                 NOT NULL ENCODE LZO,
    acx_dma_email_flag               CHAR(1) ENCODE LZO,
    acx_valid_email_mailability_flag VARCHAR(13) ENCODE LZO,
    acx_valid_email_flag             CHAR(1) ENCODE LZO,
    valid_flag                       CHAR(1) ENCODE LZO,
    source_deliverability_indicator  VARCHAR(13) ENCODE LZO,
    source_valid_flag                VARCHAR(10) ENCODE LZO,
    email_client_app                 VARCHAR(55) ENCODE LZO,
    first_date                       TIMESTAMP WITHOUT TIME ZONE ENCODE LZO,
    eff_date                         TIMESTAMP WITHOUT TIME ZONE ENCODE LZO,
    end_date                         TIMESTAMP WITHOUT TIME ZONE ENCODE LZO,
    individual_email_match_hash      VARCHAR(64) ENCODE LZO,
    create_date                      TIMESTAMP WITHOUT TIME ZONE NOT NULL ENCODE LZO,
    create_feed_instance_id          VARCHAR(64)                 NOT NULL ENCODE RUNLENGTH,
    update_date                      TIMESTAMP WITHOUT TIME ZONE ENCODE LZO,
    update_feed_instance_id          VARCHAR(64) ENCODE LZO,
    historical_record_indicator      CHAR(1) ENCODE LZO

    /*Original Distyle/Sortkey below*/
)
    DISTSTYLE KEY
    DISTKEY (individual_id)
    INTERLEAVED SORTKEY ( individual_id, email_subtype, email_address, data_source )
;


INSERT INTO audit.cons_individual_email (current_id, former_id, cons_list, cons_status, cons_state, cons_date, cons_id, individual_id, email_subtype, email_address, data_source, acx_dma_email_flag, acx_valid_email_mailability_flag, acx_valid_email_flag, valid_flag, source_deliverability_indicator, source_valid_flag, email_client_app, first_date, eff_date, end_date, individual_email_match_hash, create_date, create_feed_instance_id, update_date, update_feed_instance_id, historical_record_indicator

)

    WITH cte_former_match AS (

        SELECT
            fmt.current_id
            , fmt.former_id
        FROM agg.cons_individual_email_fm_temp fmt
    )

    SELECT
          cast(cfm.current_id AS BIGINT) AS current_id
        , cast(cfm.former_id AS BIGINT)  AS former_id
        , cast(NULL AS VARCHAR(128))     AS cons_list
        , cast(NULL AS CHAR(1))          AS cons_status
        , cast('B' AS CHAR(1))           AS cons_state
        , sysdate                        AS consolidation_date
        , upper(md5(sysdate))            AS consolidation_id

        /*List of all columns from original table below.*/
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

    FROM prod.individual_email main INNER JOIN cte_former_match cfm
            ON (main.individual_id = cfm.current_id OR main.individual_id = cfm.former_id)

    UNION ALL

    SELECT
          individual_id        AS current_id
        , cast(NULL AS BIGINT) AS former_id
        , cons_list            AS cons_list
        , cons_status          AS cons_status
        , cast('A' AS CHAR(1)) AS cons_state
        , sysdate              AS consolidation_date
        , upper(md5(sysdate))  AS consolidation_id

        /*List of all columns from original table below.*/
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


CREATE TABLE IF NOT EXISTS prod.individual_email_nightly (LIKE prod.individual_email
)
;

TRUNCATE TABLE prod.individual_email_nightly
;

INSERT INTO prod.individual_email_nightly
    SELECT *
    FROM prod.individual_email
;


DELETE FROM prod.individual_email
WHERE 1 = 1 AND exists(SELECT NULL
                       FROM audit.cons_individual_email ex0
                       WHERE ex0.individual_id = prod.individual_email.individual_id)
;


INSERT INTO prod.individual_email
    SELECT

        /*ONLY original columns. Since agg.cons_individual_email_temp table contains extra for audit*/
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


    FROM agg.cons_individual_email_temp
;

ANALYZE prod.individual_email
;

