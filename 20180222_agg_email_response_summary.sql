/***************************************************************************
*            (C) Copyright Consumer Reports 2017
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  agg_email_response_summary.sql
* Date       :  2018/02/22
* Dev & QA   :  Stanislav Dulko
***************************************************************************/


insert into prod.agg_email_response_summary
WITH cte_imd_max AS (

    select
        imd_raw.individual_id
        , imd_raw.device_name
        , imd_raw.event_date
        , imd_raw.max_evdt_rnk
    from (
    SELECT
        imd.individual_id
        , imd.device_name
        , imd.event_date
        , row_number()
          OVER (
              PARTITION BY imd.individual_id
              ORDER BY imd.event_date DESC, imd.update_date desc, imd.create_date desc ) max_evdt_rnk
    FROM prod.impact_mobile_device imd) imd_raw
    WHERE max_evdt_rnk = 1

), cte_emr_raw AS (

    SELECT
        emr.individual_id
        , CASE WHEN emr.opened_flag     = 1 THEN emr.campaign_id ELSE NULL END AS open_cnt_raw
        , CASE WHEN emr.clicked         = 1 THEN emr.campaign_id ELSE NULL END AS click_cnt_raw
        , CASE WHEN emr.bounced_flag    = 1 THEN emr.campaign_id ELSE NULL END AS bounce_cnt_raw
        , CASE WHEN emr.opened_flag     = 1 THEN emr.create_date ELSE NULL END AS lst_open_dt_raw
        , CASE WHEN emr.clicked         = 1 THEN emr.create_date ELSE NULL END AS lst_click_dt_raw
        , CASE WHEN emr.bounced_flag    = 1 THEN emr.create_date ELSE NULL END AS lst_bounce_dt_raw
        , CASE WHEN emr.campaign_optout = 0 THEN emr.campaign_id ELSE NULL END AS opt_out_cnt_raw
        , CASE WHEN emr.campaign_optout = 0 THEN emr.create_date ELSE NULL END AS lst_opt_out_dt_raw

    FROM prod.email_response emr

), cte_emr_grp as (

    SELECT
        emrr.individual_id
        , count(DISTINCT emrr.open_cnt_raw)    AS open_cnt
        , count(DISTINCT emrr.click_cnt_raw)   AS click_cnt
        , count(DISTINCT emrr.bounce_cnt_raw)  AS bounce_cnt
        , max(emrr.lst_open_dt_raw)            AS lst_open_dt
        , max(emrr.lst_click_dt_raw)           AS lst_click_dt
        , max(emrr.lst_bounce_dt_raw)          AS lst_bounce_dt
        , count(DISTINCT emrr.opt_out_cnt_raw) AS opt_out_cnt
        , max(emrr.lst_opt_out_dt_raw)         AS lst_opt_out_dt

    FROM cte_emr_raw emrr
    GROUP BY emrr.individual_id

), cte_fin as (

    SELECT
        emrg.individual_id
        , nvl(emrg.open_cnt  ,0)    AS open_cnt
        , nvl(emrg.click_cnt ,0)    AS click_cnt
        , nvl(emrg.bounce_cnt,0)    AS bounce_cnt
        , emrg.lst_open_dt
        , emrg.lst_click_dt
        , emrg.lst_bounce_dt
        , nvl(emrg.opt_out_cnt, 0) AS opt_out_cnt
        , emrg.lst_opt_out_dt
        , imdm.device_name         AS lst_mob_dev
        , imdm.event_date          AS lst_mob_dev_dt

    FROM cte_emr_grp emrg
        LEFT JOIN cte_imd_max imdm
            ON emrg.individual_id = imdm.individual_id
 
)
select
    cte_fin.individual_id
    , cte_fin.open_cnt
    , cte_fin.click_cnt
    , cte_fin.bounce_cnt
    , cte_fin.lst_open_dt
    , cte_fin.lst_click_dt
    , cte_fin.lst_bounce_dt
    , cte_fin.opt_out_cnt
    , cte_fin.lst_opt_out_dt
    , cte_fin.lst_mob_dev
    , cte_fin.lst_mob_dev_dt
from cte_fin
;