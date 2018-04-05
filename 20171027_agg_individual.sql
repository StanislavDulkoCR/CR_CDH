/***************************************************************************
*            (C) Copyright Consumer Reports 2017
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  agg_individual.sql
* Date       :  2017/10/20
* Dev & QA   :  Stanislav Dulko
***************************************************************************/


/***************************************************************************/
-- AGG_INDIVIDUAL - Main temp tables
/***************************************************************************/

SET wlm_query_slot_count = 15;

DROP TABLE IF EXISTS    agg.indiv_ofo_xog_temp;
CREATE TABLE            agg.indiv_ofo_xog_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        xo.individual_id
        , MAX(CASE
              WHEN xo.dc_online_first_dt IS NULL
                   AND xo.dc_last_aps_ord_dt IS NULL
                   AND xo.dc_cdb_last_ord_dt IS NULL
                  THEN NULL
              WHEN NVL(TO_CHAR(TRUNC(xo.dc_corp_first_dt), 'YYYYMMDD'), '00000000') = NVL(TO_CHAR(TRUNC(xo.dc_online_first_dt), 'YYYYMMDD'), '99999999')
                   OR NVL(TO_CHAR(TRUNC(xo.dc_corp_first_dt), 'YYYYMMDD'), '00000000') = NVL(TO_CHAR(TRUNC(xo.dc_last_aps_ord_dt), 'YYYYMMDD'), '99999999')
                   OR NVL(TO_CHAR(TRUNC(xo.dc_corp_first_dt), 'YYYYMMDD'), '00000000') = NVL(TO_CHAR(TRUNC(xo.dc_cdb_last_ord_dt), 'YYYYMMDD'), '99999999')
                  THEN 1
              ELSE 0
              END) AS dc_corp_first_ind
        , MAX(CASE
              WHEN xo.dc_online_first_dt IS NULL
                   AND xo.dc_offline_first_dt IS NULL
                   AND xo.dc_last_aps_ord_dt IS NULL
                   AND xo.dc_cdb_last_ord_dt IS NULL
                  THEN NULL
              WHEN NVL(TO_CHAR(TRUNC(xo.dc_corp_first_dt), 'YYYYMMDD'), '00000000') = NVL(TO_CHAR(TRUNC(xo.dc_online_first_dt), 'YYYYMMDD'), '99999999')
                   OR NVL(TO_CHAR(TRUNC(xo.dc_corp_first_dt), 'YYYYMMDD'), '00000000') = NVL(TO_CHAR(TRUNC(xo.dc_offline_first_dt), 'YYYYMMDD'), '99999999')
                   OR NVL(TO_CHAR(TRUNC(xo.dc_corp_first_dt), 'YYYYMMDD'), '00000000') = NVL(TO_CHAR(TRUNC(xo.dc_last_aps_ord_dt), 'YYYYMMDD'), '99999999')
                   OR NVL(TO_CHAR(TRUNC(xo.dc_corp_first_dt), 'YYYYMMDD'), '00000000') = NVL(TO_CHAR(TRUNC(xo.dc_cdb_last_ord_dt), 'YYYYMMDD'), '99999999')
                  THEN 1
              ELSE 0
              END) AS dc_corp_first_ind2
        , MAX(CASE
              WHEN ofo.ord_dt IS NULL
                  THEN NULL
              WHEN NVL(TO_CHAR(TRUNC(xo.dc_offline_first_dt), 'YYYYMMDD'), '00000000') = NVL(TO_CHAR(TRUNC(ofo.ord_dt), 'YYYYMMDD'), '99999999')
                  THEN 1
              ELSE 0
              END) AS dc_offline_fst_crt_ind
        , MAX(CASE
              WHEN ofo.ord_dt IS NULL
                  THEN NULL
              WHEN NVL(TO_CHAR(TRUNC(xo.dc_corp_first_dt), 'YYYYMMDD'), '00000000') = NVL(TO_CHAR(TRUNC(ofo.ord_dt), 'YYYYMMDD'), '99999999')
                  THEN 1
              ELSE 0
              END) AS dc_corp_fst_crt_ind
    FROM prod.agg_individual_xographic xo
        LEFT JOIN prod.agg_print_order ofo
            ON xo.individual_id = ofo.individual_id
    WHERE 1 = 1
    GROUP BY xo.individual_id;

analyse agg.indiv_ofo_xog_temp;

DROP TABLE IF EXISTS agg.indiv_oli_xog_1_temp;
CREATE TABLE agg.indiv_oli_xog_1_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        xo.individual_id
        , MAX(CASE
              WHEN oli.crt_dt IS NULL
                  THEN NULL
              WHEN NVL(TO_CHAR(TRUNC(xo.dc_last_aps_ord_dt), 'YYYYMMDD'), '00000000') = NVL(TO_CHAR(TRUNC(oli.crt_dt), 'YYYYMMDD'), '99999999')
                  THEN 1
              ELSE 0
              END) AS dc_last_aps_crt_ind
    FROM prod.agg_individual_xographic xo
        LEFT JOIN prod.agg_digital_item oli
            ON xo.individual_id = oli.individual_id
    WHERE 1 = 1
          AND oli.mag_cd IN ('NCBK', 'UCBK', 'NCPR', 'UCPR')
    GROUP BY xo.individual_id;

analyse agg.indiv_oli_xog_1_temp;

DROP TABLE IF EXISTS agg.indiv_oli_xog_2_temp;
CREATE TABLE agg.indiv_oli_xog_2_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        xo.individual_id
        , MAX(CASE
              WHEN oli.crt_dt IS NULL
                  THEN NULL
              WHEN NVL(TO_CHAR(TRUNC(xo.dc_online_first_dt), 'YYYYMMDD'), '00000000') = NVL(TO_CHAR(TRUNC(oli.crt_dt), 'YYYYMMDD'), '99999999')
                  THEN 1
              ELSE 0
              END) AS dc_online_fst_crt_ind
        , MAX(CASE
              WHEN xo.dc_last_aps_ord_dt IS NULL
                  THEN NULL
              WHEN NVL(TO_CHAR(TRUNC(xo.dc_online_first_dt), 'YYYYMMDD'), '00000000') = NVL(TO_CHAR(TRUNC(xo.dc_last_aps_ord_dt), 'YYYYMMDD'), '99999999')
                  THEN 1
              ELSE 0
              END) AS dc_online_fst_aps_ind
    FROM prod.agg_individual_xographic xo
        LEFT JOIN prod.agg_digital_item oli
            ON xo.individual_id = oli.individual_id
    WHERE 1 = 1
    GROUP BY xo.individual_id;

analyse agg.indiv_oli_xog_2_temp;

/*------------------------------------UNTESTED------------------------------------*/
DROP TABLE IF EXISTS    agg.indiv_survey_resp_dt_temp;
CREATE TABLE            agg.indiv_survey_resp_dt_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS WITH cte_surv_cdhicd AS (
        SELECT
            sr.individual_id
            , sr.response_value AS ANSWER_DATE
        FROM prod.survey_response sr
        WHERE sr.answer_type = 'date'
    )
    SELECT
        individual_id
        , max(cast(answer_date as date)) answer_date
        , min(cast(answer_date as date)) min_answer_date
    --## SEE 20171013_agg_individual_survey_resp_pivot.sql
    FROM cte_surv_cdhicd
    WHERE substring(individual_id, 1, 1) = 1
    GROUP BY individual_id
;

analyse agg.indiv_survey_resp_dt_temp;
/*------------------------------------UNTESTED------------------------------------*/

DROP TABLE IF EXISTS    agg.indiv_webpage_temp;
CREATE TABLE            agg.indiv_webpage_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT
            individual_id
            , max(hit_time) hit_time
            , min(hit_time) min_hit_time
        FROM prod.web_page
        WHERE event_list_id = 233
        GROUP BY individual_id;

analyse agg.indiv_webpage_temp;

DROP TABLE IF EXISTS    agg.indiv_webpage_2_temp;
CREATE TABLE            agg.indiv_webpage_2_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT
            individual_id
            , CASE WHEN page.hit_time > sysdate - 365
            THEN 1
              ELSE 0 END AS page_view_within_year
        FROM agg.indiv_webpage_temp page
        WHERE 1 = 1
;

analyse agg.indiv_webpage_2_temp;

DROP TABLE IF EXISTS    agg.indiv_emailfavkey_temp;
CREATE TABLE            agg.indiv_emailfavkey_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        individual_id
        , DENSE_RANK()
          OVER (
              ORDER BY email_id ) email_fav_key
    FROM prod.agg_email ae
    WHERE ae.email_type_cd = 'I';

analyse agg.indiv_emailfavkey_temp;


DROP TABLE IF EXISTS agg.indiv_ibx_temp;
CREATE TABLE agg.indiv_ibx_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        individual_id
        , ROUND(MONTHS_BETWEEN(SYSDATE, TO_DATE(substring(IB8624_DOB_IP_IND_DFLT_1ST_IND, 1, 4) || LPAD(DECODE(substring(ib8624_dob_ip_ind_dflt_1st_ind, 5, 2), '00', CAST(TRUNC(random() * 13 + 1) AS TEXT), substring(IB8624_DOB_IP_IND_DFLT_1ST_IND, 5, 2)), 2, '0') || LPAD(DECODE(substring(ib8624_dob_ip_ind_dflt_1st_ind, 7, 2), '00', CAST(TRUNC(random() * 29 + 1) AS TEXT), substring(ib8624_dob_ip_ind_dflt_1st_ind, 7, 2)), 2, '0'), 'YYYYMMDD')) / 12) AS age_infobase
        , ib8688_gender_input_ind                                                                                                                                                                                                                                                                                                                                                                                                                                   AS gender_infobase
    FROM prod.agg_infobase_profile
    WHERE 1 = 1;

analyse agg.indiv_ibx_temp;

DROP TABLE IF EXISTS    agg.indiv_email_fl_temp;--mt_ind_email_flag_temp
CREATE TABLE            agg.indiv_email_fl_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS WITH cte_pref AS (SELECT
                             individual_id
                             , MAX(CASE WHEN scp_cd LIKE 'NEW%' AND SCP_CD NOT IN ('NEWHHEART', 'NEWHCHILDTEEN', 'NEWHDIABETES', 'NEWHWOMEN', 'NEWHAFTER60') AND auth_flg = 'Y'
            THEN 'Y'
                                   ELSE NULL END) AS NEWS_IND_ACTV_FLG
                         FROM prod.agg_preference
                         WHERE data_source = 'DGI'
                               AND scp_cd IN ('PNL', 'NEWCRAUTO', 'NEWHNBBD', 'NEWCRALERT', 'NEWCRMGHA', 'NEWCRMHANLP', 'NEWWHATS', 'NEWGRNCHC', 'NEWHHEART', 'NEWHCHILDTEEN', 'NEWHDIABETES', 'NEWHWOMEN', 'NEWHAFTER60')
                         GROUP BY individual_id)
    SELECT
        a.individual_id
        , max(CASE WHEN a.NEWS_IND_ACTV_FLG = 'Y' AND e.email_type_cd IN ('N', 'I') AND (nvl(e.valid_flag, 'X') <> 'N' AND nvl(e.src_valid_flag, 'X') <> 'N' AND nvl(e.src_delv_ind, 'X') <> '1')
        THEN 'Y'
              ELSE 'N' END) AS news_ind_actv_flg
    FROM cte_pref a
        LEFT JOIN prod.agg_email e
            ON a.individual_id = e.individual_id
    GROUP BY a.individual_id;

analyse agg.indiv_email_fl_temp;

/***************************************************************************/
-- AGG_INDIVIDUAL - Miscellaneous temp tables
/***************************************************************************/
DROP TABLE IF EXISTS agg.indiv_ind_xref_temp;
CREATE TABLE agg.indiv_ind_xref_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT DISTINCT
          ind.individual_id AS individual_id
        , ind.household_id  AS hh_id
        , ind.household_id  AS osl_hh_id
    FROM PROD.individual ind
    WHERE INDIVIDUAL_ID IS NOT NULL;

analyse agg.indiv_ind_xref_temp;

DROP TABLE IF EXISTS agg.indiv_ext_ref_dt_temp;
CREATE TABLE agg.indiv_ext_ref_dt_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        individual_id
        , MAX(er_fst_dt) AS er_fst_dt
    FROM
        (SELECT
             individual_id
             , account_subtype_code
             , MAX(first_account_date) er_fst_dt
         FROM prod.account acc
         WHERE account_subtype_code IN ('CNS', 'CRM', 'CRH', 'CRE', 'SHM')
         GROUP BY individual_id, account_subtype_code
         HAVING COUNT(*) > 1) accer
    GROUP BY individual_id;

analyse agg.indiv_ext_ref_dt_temp;

DROP TABLE IF EXISTS agg.indiv_cga_temp;
CREATE TABLE agg.indiv_cga_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        individual_id
        , MIN(don_dt)  AS mn_don_dt
        , MAX(don_dt)  AS mx_don_dt
        , COUNT(*)     AS cga_cnt
        , SUM(don_amt) AS cga_don_amt
    FROM prod.agg_cga
    WHERE 1 = 1
    GROUP BY individual_id;

analyse agg.indiv_cga_temp;

DROP TABLE IF EXISTS agg.indiv_preference_temp;
CREATE TABLE agg.indiv_preference_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        individual_id
        , MIN(CASE
              WHEN scp_cd = 'PNL'
                  THEN fst_dt
              ELSE NULL
              END)   AS mn_fst_dt
        , MAX(CASE
              WHEN scp_cd = 'PNL'
                  THEN fst_dt
              ELSE NULL
              END)   AS mx_fst_dt
        , COUNT(CASE
                WHEN scp_cd = 'PNL'
                    THEN 'x'
                ELSE NULL
                END) AS auth_cnt
        , MAX(CASE
              WHEN scp_cd = 'PNL'
                   AND auth_cd = 'EMAIL'
                  THEN auth_flg
              ELSE NULL
              END)   AS mrp_flg
        , COUNT(CASE
                WHEN scp_cd = 'PNL'
                     AND auth_flg = 'Y'
                    THEN 'x'
                ELSE NULL
                END) AS auth_flg_cnt
        , MAX(CASE
              WHEN scp_cd = 'PNL'
                  THEN eff_dt
              ELSE NULL
              END)   AS mx_eff_dt
        , MIN(CASE
              WHEN scp_cd LIKE 'NEW%'
                   AND TO_CHAR(fst_dt, 'YYYYMMDD') != '19000101'
                  THEN fst_dt
              ELSE NULL
              END)   AS mn_news_fst_dt
        , MIN(CASE
              WHEN scp_cd LIKE 'NEW%'
                  THEN fst_dt
              ELSE NULL
              END)   AS mn_newsltr_fst_dt
        , MAX(CASE
              WHEN scp_cd LIKE 'NEW%'
                   AND TO_CHAR(fst_dt, 'YYYYMMDD') != '19000101'
                  THEN fst_dt
              ELSE NULL
              END)   AS mx_news_fst_dt
        , MAX(CASE
              WHEN scp_cd LIKE 'NEW%'
                  THEN fst_dt
              ELSE NULL
              END)   AS mx_newsltr_fst_dt
        , MAX(CASE
              WHEN scp_cd LIKE 'NEW%'
                  THEN eff_dt
              ELSE NULL
              END)   AS mx_news_eff_dt
        , COUNT(CASE
                WHEN scp_cd LIKE 'NEW%'
                    THEN 'x'
                ELSE NULL
                END) AS news_auth_cnt
        , COUNT(CASE
                WHEN scp_cd LIKE 'NEW%'
                     AND auth_flg = 'Y'
                    THEN 'x'
                ELSE NULL
                END) AS news_auth_y_cnt
        , MAX(CASE
              WHEN scp_cd LIKE 'NEW%'
                   AND auth_flg = 'N'
                  THEN eff_dt
              ELSE NULL
              END)   AS mx_news_auth_n_eff_dt
        , MAX(CASE
              WHEN scp_cd = 'NEWCRAUTO'
                   AND auth_flg = 'Y'
                  THEN 'Y'
              WHEN scp_cd = 'NEWCRAUTO'
                   AND auth_flg = 'N'
                  THEN 'N'
              ELSE NULL
              END)   AS news_car_watch_flg
        , MAX(CASE
              WHEN scp_cd = 'NEWHNBBD'
                   AND auth_flg = 'Y'
                  THEN 'Y'
              WHEN scp_cd = 'NEWHNBBD'
                   AND auth_flg = 'N'
                  THEN 'N'
              ELSE NULL
              END)   AS news_best_buy_drug_flg
        , MAX(CASE
              WHEN scp_cd = 'NEWCRALERT'
                   AND auth_flg = 'Y'
                  THEN 'Y'
              WHEN scp_cd = 'NEWCRALERT'
                   AND auth_flg = 'N'
                  THEN 'N'
              ELSE NULL
              END)   AS news_safety_alert_flg
        , MAX(CASE
              WHEN scp_cd = 'NEWCRMHANLP'
                   AND auth_flg = 'Y'
                  THEN 'Y'
              WHEN scp_cd = 'NEWCRMHANLP'
                   AND auth_flg = 'N'
                  THEN 'N'
              ELSE NULL
              END)   AS news_hlth_alert_flg
        , MAX(CASE
              WHEN scp_cd = 'NEWWHATS'
                   AND auth_flg = 'Y'
                  THEN 'Y'
              WHEN scp_cd = 'NEWWHATS'
                   AND auth_flg = 'N'
                  THEN 'N'
              ELSE NULL
              END)   AS news_cro_whats_flg
        , MAX(CASE
              WHEN scp_cd = 'NEWGRNCHC'
                   AND auth_flg = 'Y'
                  THEN 'Y'
              WHEN scp_cd = 'NEWGRNCHC'
                   AND auth_flg = 'N'
                  THEN 'N'
              ELSE NULL
              END)   AS news_green_choice_flg
        , MAX(CASE
              WHEN scp_cd = 'NEWHHEART'
                   AND auth_flg = 'Y'
                  THEN 'Y'
              WHEN scp_cd = 'NEWHHEART'
                   AND auth_flg = 'N'
                  THEN 'N'
              ELSE NULL
              END)   AS news_hlth_heart_flg
        , MAX(CASE
              WHEN scp_cd = 'NEWHCHILDTEEN'
                   AND auth_flg = 'Y'
                  THEN 'Y'
              WHEN scp_cd = 'NEWHCHILDTEEN'
                   AND auth_flg = 'N'
                  THEN 'N'
              ELSE NULL
              END)   AS news_hlth_child_teen_flg
        , MAX(CASE
              WHEN scp_cd = 'NEWHDIABETES'
                   AND auth_flg = 'Y'
                  THEN 'Y'
              WHEN scp_cd = 'NEWHDIABETES'
                   AND auth_flg = 'N'
                  THEN 'N'
              ELSE NULL
              END)   AS news_hlth_diabetes_flg
        , MAX(CASE
              WHEN scp_cd = 'NEWHWOMEN'
                   AND auth_flg = 'Y'
                  THEN 'Y'
              WHEN scp_cd = 'NEWHWOMEN'
                   AND auth_flg = 'N'
                  THEN 'N'
              ELSE NULL
              END)   AS news_hlth_women_flg
        , MAX(CASE
              WHEN scp_cd = 'NEWHAFTER60'
                   AND auth_flg = 'Y'
                  THEN 'Y'
              WHEN scp_cd = 'NEWHAFTER60'
                   AND auth_flg = 'N'
                  THEN 'N'
              ELSE NULL
              END)   AS news_hlth_after_60_flg
    FROM prod.agg_preference
    WHERE data_source = 'DGI'
          AND scp_cd IN ('PNL', 'NEWCRAUTO', 'NEWHNBBD', 'NEWCRALERT', 'NEWCRMGHA', 'NEWCRMHANLP', 'NEWWHATS', 'NEWGRNCHC', 'NEWHHEART', 'NEWHCHILDTEEN', 'NEWHDIABETES', 'NEWHWOMEN', 'NEWHAFTER60')
    GROUP BY individual_id;

analyse agg.indiv_preference_temp;

DROP TABLE IF EXISTS    agg.indiv_off_canc_temp;
CREATE TABLE            agg.indiv_off_canc_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        t1.individual_id
        , SUM(t1.pd_amt) AS ofo_pd_amt
        , MAX(CASE
              WHEN t1.stat_cd = 'B'
                   AND t1.src_cd IN ('CA', 'CC')
                   AND substring(t1.keycode, 1, 4) != 'LIFE'
                  THEN t1.ord_dt
              WHEN t1.stat_cd = 'B'
                   AND t1.src_cd IN ('CA', 'CC')
                   AND t1.entr_typ_cd = 'F'
                  THEN t1.ord_dt
              ELSE NULL
              END)       AS ofo_mx_cc_ord_dt
        , MAX(t1.pmt_dt) AS ofo_mx_pmt_dt
        , MIN(t1.ord_dt) AS ofo_mn_ord_dt
        , COUNT(CASE
                WHEN NVL(t1.set_cd, 'X') IN ('C', 'E')
                    THEN 'x'
                ELSE NULL
                END)     AS ofo_ce_cnt
        , COUNT(CASE
                WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E')
                    THEN 'x'
                ELSE NULL
                END)     AS ofo_xce_cnt
        , MAX(t1.ord_dt) AS ofo_mx_ord_dt
        , MAX(CASE
              WHEN NVL(oca.cancel_reason_code, 'X') NOT IN ('06', '14', '50')
                  THEN t1.canc_dt
              ELSE NULL
              END)       AS ofo_mx_canc_dt
        , MIN(CASE
              WHEN NVL(t1.set_cd, 'X') IN ('C', 'E')
                  THEN NVL(TO_CHAR(t1.ord_dt, 'YYYYMMDD'), '99991231') || '2' || DECODE(t1.mag_cd, 'CNS', 1, 'CRH', 2, 'CRM', 3, 'SHM', 4, 'CRT', 5, 6) ||
                       t1.mag_cd || ' REC'
              ELSE NVL(TO_CHAR(t1.ord_dt, 'YYYYMMDD'), '99991231') || '1' || DECODE(t1.mag_cd, 'CNS', 1, 'CRH', 2, 'CRM', 3, 'SHM', 4, 'CRT', 5, 6) ||
                   t1.mag_cd
              END)       AS ofo_fst_prod_cd
        , SUM(CASE
              WHEN NVL(t1.mag_cd, 'X') != 'CRT'
                  THEN t1.pd_amt
              ELSE NULL
              END)       AS ofo_prod_ltd_pd_amt
        , MIN(CASE
              WHEN NVL(t1.KEYCODE, 'x') NOT IN ('ZFREE', 'VFREE')
                  THEN t1.ord_dt
              END)       AS ofo_prod_fst_ord_dt
        , MAX(CASE
              WHEN NVL(t1.KEYCODE, 'x') NOT IN ('ZFREE', 'VFREE')
                  THEN t1.ord_dt
              END)       AS ofo_prod_lst_ord_dt
        , MAX(CASE
              WHEN t1.canc_rsn_cd IN ('06', '50')
                   AND NVL(t1.mag_cd, 'X') != 'CRT'
                  THEN t1.canc_dt
              ELSE NULL
              END)       AS ofo_prod_lst_canc_bad_dbt_dt
        , COUNT(CASE
                WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E')
                     AND NVL(t1.mag_cd, 'X') != 'CRT'
                     AND NVL(t1.KEYCODE, 'x') NOT IN ('ZFREE', 'VFREE')
                    THEN 'x'
                ELSE NULL
                END)     AS ofo_prod_ord_cnt
        , COUNT(CASE
                WHEN t1.mag_cd = 'CNS'
                     AND t1.set_cd IN ('B', 'D')
                    THEN 1
                END)     AS ofo_cr_prod_cnt
        , COUNT(CASE
                WHEN t1.mag_cd = 'CRH'
                     AND t1.set_cd IN ('B', 'D')
                    THEN 1
                END)     AS ofo_hl_prod_cnt
        , COUNT(CASE
                WHEN t1.mag_cd = 'CRM'
                     AND t1.set_cd IN ('B', 'D')
                    THEN 1
                END)     AS ofo_ma_prod_cnt
        , COUNT(CASE
                WHEN t1.mag_cd = 'SHM'
                     AND t1.set_cd IN ('B', 'D')
                    THEN 1
                END)     AS ofo_shm_prod_cnt
        , COUNT(CASE
                WHEN t1.set_cd IN ('B', 'D')
                     AND NVL(t1.mag_cd, 'X') != 'CRT'
                    THEN 1
                END)     AS ofo_prod_dnr_ord_cnt
        , COUNT(CASE
                WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E')
                     AND t1.mag_cd IN ('CNS', 'CRH', 'CRM', 'SHM')
                     AND ((substring(t1.keycode, 1, 1) IN ('A', 'G', 'X'))
                          OR (substring(t1.keycode, 1, 1) = 'D'
                              AND NVL((decode(substring(t1.keycode, 2, 1), '', NULL, substring(t1.keycode, 2, 1))), 'X') != 'N')
                          OR (substring(t1.keycode, 1, 1) = 'B'
                              AND NVL((decode(substring(t1.keycode, 9, 1), '', NULL, substring(t1.keycode, 9, 1))), 'X') NOT IN ('A', 'B', 'C', 'S'))
                          OR (substring(t1.keycode, 1, 1) = 'U'
                              AND REGEXP_INSTR(NVL((decode(substring(t1.keycode, 9, 1), '', NULL, substring(t1.keycode, 9, 1))), '1'), '[^[:alpha:]]') = 1)
                          OR (substring(t1.keycode, 1, 1) = 'R'
                              AND NVL((decode(substring(t1.keycode, 9, 1), '', NULL, substring(t1.keycode, 9, 1))), 'X') NOT IN ('B', 'C', 'D', 'E', 'F', 'T'))
                          OR (substring(t1.keycode, 1, 1) = 'U'
                              AND LENGTH(t1.keycode) = 7
                              AND REGEXP_INSTR(substring(t1.keycode, 6, 1), '[^[:alpha:]]') = 1))
                    THEN 1
                END)     AS ofo_prod_dm_ord_cnt
        , COUNT(CASE
                WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E')
                     AND t1.mag_cd IN ('CNS', 'CRH', 'CRM', 'SHM')
                     AND ((substring(t1.keycode, 1, 1) = 'E')
                          OR (substring(t1.keycode, 1, 1) = 'D'
                              AND substring(t1.keycode, 2, 1) = 'N')
                          OR (substring(t1.keycode, 1, 1) = 'B'
                              AND substring(t1.keycode, 9, 1) IN ('A', 'B', 'C', 'S'))
                          OR (substring(t1.keycode, 1, 1) = 'U'
                              AND REGEXP_INSTR(substring(t1.keycode, 9, 1), '[[:alpha:]]') = 1)
                          OR (substring(t1.keycode, 1, 1) = 'R'
                              AND substring(t1.keycode, 9, 1) IN ('B', 'C', 'D', 'E', 'F'))
                          OR (substring(t1.keycode, 4, 6) = 'FAILCC')
                          OR (substring(t1.keycode, 1, 1) = 'U'
                              AND LENGTH(t1.keycode) = 7
                              AND substring(t1.keycode, 6, 1) IN ('A', 'B', 'C', 'D', 'E', 'F')))
                    THEN 1
                END)     AS ofo_prod_em_ord_cnt
    FROM prod.agg_print_order t1
        INNER JOIN prod.action_header ah
            ON ah.hash_account_id = t1.hash_account_id
                AND ah.action_id = t1.ord_id
                AND ah.action_type = 'ORDER'
        LEFT JOIN prod.print_cancel oca
            ON t1.hash_account_id = oca.hash_action_id
    WHERE 1 = 1
    GROUP BY t1.individual_id;

analyse agg.indiv_off_canc_temp;

DROP TABLE IF EXISTS agg.indiv_acct_merge_temp;
CREATE TABLE agg.indiv_acct_merge_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        er.individual_id
        , MAX(t1.merge_date) AS mx_merge_dt
    FROM prod.account er, prod.legacy_offline_account_arch t1
    WHERE substring(er.account_subtype_code || '+' || er.source_account_id, 1, 13) = t1.new_account
    GROUP BY er.individual_id;

analyse agg.indiv_acct_merge_temp;

DROP TABLE IF EXISTS agg.indiv_oli_sku_temp;
CREATE TABLE agg.indiv_oli_sku_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
 WITH cte_oli AS
(SELECT
     individual_id
     , tot_amt
     , pmt_dt
     , set_cd
     , stat_cd
     , canc_dt
     , crt_dt
     , mag_cd
     , crd_stat_cd
     , sku_num
     , canc_rsn_cd
     , svc_stat_cd
     , sub_src_cd
     , end_dt
     , sub_rnw_ind
     , CASE
       WHEN substring(NVL(ext_keycd, int_keycd), 1, 2) = 'WF'
           THEN NULL
       WHEN mag_cd = 'CRO'
            AND set_cd = 'A'
            AND MONTHS_BETWEEN(strt_dt, MAX(end_dt)
       OVER (
           PARTITION BY individual_id, mag_cd, set_cd
           ORDER BY strt_dt, NVL(ext_keycd, int_keycd) DESC
           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) > 2
           THEN 1
       END AS cro_break_flg
     , CASE
       WHEN substring(NVL(ext_keycd, int_keycd), 1, 2) = 'WF'
           THEN NULL
       WHEN mag_cd = 'CARP'
            AND set_cd = 'A'
            AND MONTHS_BETWEEN(strt_dt, MAX(end_dt)
       OVER (
           PARTITION BY individual_id, mag_cd, set_cd
           ORDER BY strt_dt, NVL(ext_keycd, int_keycd) DESC
           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) > 2
           THEN 1
       END AS carp_break_flg
     , CASE
       WHEN substring(NVL(ext_keycd, int_keycd), 1, 2) = 'WF'
           THEN NULL
       WHEN mag_cd = 'CRMG'
            AND NVL(set_cd, 'A') = 'A'
            AND MONTHS_BETWEEN(strt_dt, MAX(end_dt)
       OVER (
           PARTITION BY individual_id, mag_cd, NVL(set_cd, 'A')
           ORDER BY strt_dt, NVL(ext_keycd, int_keycd) DESC
           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) > 2
           THEN 1
       END AS crmg_break_flg
 FROM prod.agg_digital_item
), cte_skulkp AS
(SELECT
       sku_num       AS SKU_NUM
     , sub_type_code AS SUB_TYP_CD
     , sku_desc      AS SKU_DES
     , product       AS PRODUCT
     , value         AS VALUE
     , unit_flag     AS UNIT_FLG
     , selection     AS SELECTION
     , comp_flag     AS COMP_FLG
     , value_range   AS AMT
     , update_date   AS MAINT_DT
     , valid_from    AS VALID_FROM
     , valid_to      AS VALID_TO
 FROM prod.sku_lkup
)
SELECT
    t1.individual_id
    , SUM(CASE
          WHEN NVL(t1.sku_num, '1') < '5000000'
              THEN t1.tot_amt
          ELSE NULL
          END)                                      AS oli_tot_amt
    , SUM(CASE
          WHEN t1.sku_num > '5000000'
               AND t1.stat_cd = 'C'
              THEN t1.tot_amt
          ELSE NULL
          END)                                      AS oli_tot_amt2
    , MIN(CASE
          WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E')
              THEN t1.crt_dt
          ELSE NULL
          END)                                      AS olo_prod_fst_ord_dt
    , MAX(CASE
          WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E')
              THEN t1.crt_dt
          ELSE NULL
          END)                                      AS olo_prod_lst_ord_dt
    , MAX(t1.pmt_dt)                                AS oli_mx_pmt_dt
    , MIN(t1.crt_dt)                                AS oli_mn_crt_dt
    , MAX(t1.crt_dt)                                AS oli_mx_crt_dt
    , COUNT(CASE
            WHEN NVL(t1.set_cd, 'X') IN ('C', 'E')
                THEN 'x'
            ELSE NULL
            END)                                    AS oli_ce_cnt
    , COUNT(CASE
            WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E')
                THEN 'x'
            ELSE NULL
            END)                                    AS oli_xce_cnt
    , COUNT(CASE
            WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E')
                 AND t1.sub_src_cd = 'D'
                THEN 'x'
            ELSE NULL
            END)                                    AS oli_xce_d_cnt
    , COUNT(CASE
            WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E')
                 AND t1.sub_src_cd IN ('E', 'G', 'K', 'N', 'U')
                THEN 'x'
            ELSE NULL
            END)                                    AS oli_xce_egknu_cnt
    , MAX(CASE
          WHEN t1.stat_cd = 'R'
              THEN t1.canc_dt
          ELSE NULL
          END)                                      AS oli_mx_canc_dt
    , MIN(CASE
          WHEN NVL(set_cd, 'X') IN ('C', 'E')
              THEN NVL(TO_CHAR(crt_dt, 'YYYYMMDD'), '99991231') || '5' || DECODE(mag_cd, 'CRO', 0, 'CRMG', 1, 'HL', 2, 'NCBK', 3, 'UCBK', 4, 'NCPR', 5, 'UCPR', 6, 'CAPS', 7, 'CARP', 8, 9) || mag_cd || ' REC'
          ELSE NVL(TO_CHAR(crt_dt, 'YYYYMMDD'), '99991231') || '4' || DECODE(mag_cd, 'CRO', 0, 'CRMG', 1, 'HL', 2, 'NCBK', 3, 'UCBK', 4, 'NCPR', 5, 'UCPR', 6, 'CAPS', 7, 'CARP', 8, 9) || mag_cd
          END)                                      AS oli_fst_prod_cd
    , MAX(CASE
          WHEN crd_stat_cd = 'F'
               OR (svc_stat_cd = 'C'
                   AND canc_rsn_cd IN ('50', '06'))
              THEN canc_dt
          ELSE NULL
          END)                                      AS oli_prod_lst_canc_bad_dbt_dt
    , COUNT(DISTINCT
          CASE
          WHEN mag_cd = 'CRO'
               AND set_cd = 'A'
              THEN 1
          END) + COUNT(cro_break_flg) + COUNT(CASE
                                              WHEN mag_cd = 'CRO'
                                                   AND set_cd IN ('B', 'D')
                                                  THEN 1
                                              END)  AS oli_cro_prod_cnt
    , COUNT(DISTINCT
          CASE
          WHEN mag_cd = 'CARP'
               AND set_cd = 'A'
              THEN 1
          END) + COUNT(carp_break_flg) + COUNT(CASE
                                               WHEN mag_cd = 'CARP'
                                                    AND set_cd IN ('B', 'D')
                                                   THEN 1
                                               END) AS oli_carp_prod_cnt
    , COUNT(DISTINCT
          CASE
          WHEN mag_cd = 'CRMG'
               AND NVL(set_cd, 'A') = 'A'
              THEN 1
          END) + COUNT(crmg_break_flg) + COUNT(CASE
                                               WHEN mag_cd = 'CRMG'
                                                    AND set_cd IN ('B', 'D')
                                                   THEN 1
                                               END) AS oli_crmg_prod_cnt
    , COUNT(CASE
            WHEN mag_cd IN ('NCBK', 'UCBK', 'NCPR', 'UCPR')
                THEN 1
            END)                                    AS oli_aps_prod_cnt
    , COUNT(CASE
            WHEN mag_cd = 'CARP'
                THEN 1
            END)                                    AS oli_carp_cnt
    , MAX(CASE
          WHEN mag_cd = 'CRO'
               AND set_cd = 'A'
              THEN 1
          ELSE 0
          END)                                      AS oli_cro_prod_cnta
    , COUNT(CASE
            WHEN set_cd IN ('B', 'D')
                THEN 1
            END)                                    AS oli_prod_dnr_ord_cnt
    , MAX(CASE
          WHEN canc_dt IS NULL
              THEN end_dt
          WHEN canc_dt IS NOT NULL
               AND sub_rnw_ind IN ('Y', 'N')
              THEN end_dt
          END)                                      AS oli_mx_end_dt
FROM cte_oli t1
    LEFT JOIN cte_skulkp sku
        ON t1.sku_num = sku.sku_num
WHERE 1 = 1
GROUP BY t1.individual_id;

analyse agg.indiv_oli_sku_temp;

DROP TABLE IF EXISTS agg.indiv_onl_item_ord_temp;
CREATE TABLE agg.indiv_onl_item_ord_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        ot.individual_id
        , MAX(CASE
              WHEN ot.entr_typ_cd = 'F'
                   AND NVL(it.strt_dt, TO_DATE('00010101', 'YYYYMMDD')) < SYSDATE
                   AND SYSDATE < NVL(it.end_dt, TO_DATE('99991231', 'YYYYMMDD'))
                  THEN ot.crt_dt
              ELSE NULL
              END)       AS olo_mx_crt_dt
        , MIN(CASE
              WHEN NVL(it.crd_stat_cd, 'X') != 'F'
                   AND ot.individual_id = it.individual_id
                   AND ot.hash_account_id = it.hash_account_id
                   AND ot.ord_id = it.ord_id
                  THEN ot.ord_dt
              ELSE NULL
              END)       AS olo_mn_ord_dt
        , MAX(ot.ord_dt) AS olo_mx_ord_dt
        , COUNT(DISTINCT
              CASE
              WHEN NVL(it.set_cd, 'X') NOT IN ('C', 'E')
                   AND ot.hash_account_id = it.hash_account_id
                   AND ot.ord_id = it.ord_id
                  THEN ot.individual_id || ot.hash_account_id || ot.ord_id
              END)       AS olo_prod_ord_cnt
        , COUNT(DISTINCT
              CASE
              WHEN NVL(it.set_cd, 'X') NOT IN ('C', 'E')
                   AND it.sub_src_cd = 'D'
                   AND ot.hash_account_id = it.hash_account_id
                   AND ot.ord_id = it.ord_id
                  THEN ot.individual_id || ot.hash_account_id || ot.ord_id
              END)       AS olo_prod_dm_ord_cnt
        , COUNT(DISTINCT
              CASE
              WHEN NVL(it.set_cd, 'X') NOT IN ('C', 'E')
                   AND it.sub_src_cd IN ('E', 'G', 'K', 'N', 'U')
                   AND ot.hash_account_id = it.hash_account_id
                   AND ot.ord_id = it.ord_id
                  THEN ot.individual_id || ot.hash_account_id || ot.ord_id
              END)       AS olo_prod_em_ord_cnt
    FROM prod.agg_digital_order ot
        LEFT JOIN prod.agg_digital_item it
            ON ot.ord_id = it.ord_id
    WHERE 1 = 1
    GROUP BY ot.individual_id;

analyse agg.indiv_onl_item_ord_temp;

DROP TABLE IF EXISTS agg.indiv_de_expires_temp;
CREATE TABLE agg.indiv_de_expires_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        INDIVIDUAL_ID
        , MIN(NVL(TO_CHAR(EXP_DATE, 'YYYYMMDD'), '99991231') || '3' || DECODE(MAGAZINE_CODE, 'CNS', 1, 'CRH', 2, 3) || MAGAZINE_CODE) AS DE_FST_PROD_CD
        , MIN(EXP_DATE)                                                                                                               AS DE_MIN_EXP_DT
        , MAX(EXP_DATE)                                                                                                               AS DE_MAX_EXP_DT
        , COUNT(*)                                                                                                                    AS DE_CNT
        , COUNT(CASE
                WHEN MAGAZINE_CODE = 'CNS'
                    THEN 1
                END)                                                                                                                  AS DE_CR_PROD_CNT
        , COUNT(CASE
                WHEN MAGAZINE_CODE = 'CRH'
                    THEN 1
                END)                                                                                                                  AS DE_HL_PROD_CNT
    FROM PROD.LEGACY_DEAD_EXPIRES
    GROUP BY INDIVIDUAL_ID;

analyse agg.indiv_de_expires_temp;

DROP TABLE IF EXISTS agg.indiv_books_ord_temp;
CREATE TABLE agg.indiv_books_ord_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        individual_id
        , SUM(pmt_amt)    AS bo_pmt_amt
        , MIN(ord_dt)     AS bo_min_ord_dt
        , MIN(CASE
              WHEN NVL(dnr_recip_cd, 'X') != 'R'
                  THEN ord_dt
              END)        AS bo_prod_fst_ord_dt
        , MAX(ord_dt)     AS bo_max_ord_dt
        , MAX(lst_pmt_dt) AS bo_pmt_dt
        , COUNT(CASE
                WHEN dnr_recip_cd = 'R'
                     OR ord_prem_flg = 'Y'
                    THEN 'x'
                ELSE NULL
                END)      AS bo_ce_cnt
        , COUNT(CASE
                WHEN NVL(dnr_recip_cd, 'X') != 'R'
                     AND NVL(ord_prem_flg, 'X') != 'Y'
                    THEN 'x'
                ELSE NULL
                END)      AS bo_xce_cnt
        , COUNT(CASE
                WHEN dnr_recip_cd = 'D'
                    THEN 'x'
                ELSE NULL
                END)      AS bo_recip_cd_d_cnt
        , COUNT(CASE
                WHEN NVL(dnr_recip_cd, 'X') != 'D'
                    THEN 'x'
                ELSE NULL
                END)      AS bo_recip_cd_not_d_cnt
        , MIN(CASE
              WHEN ord_cdr_flg = 'Y'
                  THEN NVL(TO_CHAR(ord_dt, 'YYYYMMDD'), '99991231') || '90' || 'CDR'
              WHEN ord_cdr_flg = 'N'
                  THEN NVL(TO_CHAR(ord_dt, 'YYYYMMDD'), '99991231') || 'A0' || 'NCDR'
              END)        AS bo_fst_prod_cd
        , MAX(CASE
              WHEN NVL(dnr_recip_cd, 'X') != 'R'
                  THEN ord_dt
              END)        AS bo_prod_lst_ord_dt
        , COUNT(CASE
                WHEN NVL(dnr_recip_cd, 'X') != 'R'
                    THEN 'x'
                END)      AS bo_prod_ord_cnt
        , COUNT(CASE
                WHEN NVL(dnr_recip_cd, 'X') != 'R'
                     AND src_cd = '5'
                    THEN 'x'
                END)      AS bo_prod_dm_ord_cnt
        , COUNT(CASE
                WHEN NVL(dnr_recip_cd, 'X') != 'R'
                     AND src_cd = '11'
                    THEN 'x'
                END)      AS bo_prod_em_ord_cnt
        , COUNT(CASE
                WHEN dnr_recip_cd = 'D'
                    THEN 'x'
                END)      AS bo_prod_dnr_ord_cnt
    FROM prod.agg_books_order
    WHERE 1 = 1
    GROUP BY individual_id;

analyse agg.indiv_books_ord_temp;

DROP TABLE IF EXISTS agg.indiv_misc_agg_temp;
CREATE TABLE agg.indiv_misc_agg_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
 WITH cte_books_item AS
(SELECT
     individual_id
     , MAX(ret_canc_dt) AS bi_ret_canc_dt
     , MAX(CASE
           WHEN ret_canc_rsn_cd IN ('AR', 'AY', 'BG', 'BH', 'BK', 'BM', 'DE', 'DI', 'DJ', 'DW', 'EA')
               THEN ret_canc_dt
           END)         AS bi_prod_lst_canc_bad_dbt_dt
     , COUNT(*)         AS bi_cnt
 FROM prod.agg_books_item
 GROUP BY individual_id
), cte_books_summary AS
( SELECT
      individual_id
      , bk_flg
      , bk_actv_flg
      , bk_ltd_pd_amt bs_prod_ltd_pd_amt
  FROM prod.agg_books_summary
), cte_fr_don AS
(SELECT
     individual_id
     , MAX(don_dt)  AS fr_lst_don_dt
     , MAX(don_amt) AS fr_max_don_amt
 FROM prod.agg_fundraising_donation
 GROUP BY individual_id
), cte_adv_resp AS
(SELECT
     individual_id
     , MIN(lcr.last_response_date) AS mn_last_resp_dt
     , MAX(lcr.last_response_date) AS mx_last_resp_dt
     , COUNT(individual_id)        AS ar_entry_cnt
     , SUM(CASE
           WHEN lcr.last_response_date >= ADD_MONTHS(SYSDATE, -12)
               THEN 1
           ELSE 0
           END)                       cnt_resp_dt_gt_12
 FROM prod.legacy_constituent_response lcr
 GROUP BY individual_id
), cte_fr_pledge AS
( SELECT
        CAST(NULL AS BIGINT) AS individual_id
      , CAST(NULL AS DATE)   AS max_pldg_dt
), cte_fr_lt_pledge AS
( SELECT
        CAST(NULL AS BIGINT) AS individual_id
      , CAST(NULL AS DATE)   AS max_pldg_dt
), cte_abq_don AS
(SELECT
     individual_id
     , TO_DATE(MAX(abq_yr) || '0601', 'YYYYMMDD') max_abq_dt
     , TO_DATE(MIN(abq_yr) || '0601', 'YYYYMMDD') min_abq_dt
 FROM prod.agg_abq_donation
 GROUP BY individual_id
), cte_abqr AS
(SELECT
     individual_id
     , TO_DATE(MIN(abq_yr) || '0601', 'YYYYMMDD') min_abq_mbr_dt
     , TO_DATE(MAX(abq_yr) || '0601', 'YYYYMMDD') max_abq_mbr_dt
 FROM prod.agg_abq_response
 WHERE mbr_flg = 'Y'
 GROUP BY individual_id
)
SELECT
    xr.individual_id
    , xr.hh_id
    , xr.osl_hh_id
    , bi.individual_id               AS bi_individual_id
    , bi.bi_ret_canc_dt              AS bi_bi_ret_canc_dt
    , bi.bi_prod_lst_canc_bad_dbt_dt AS bi_bi_prod_lst_canc_bad_dbt_dt
    , bi.bi_cnt                      AS bi_bi_cnt
    , bs.individual_id               AS bs_individual_id
    , bs.bk_flg                      AS bs_bk_flg
    , bs.bk_actv_flg                 AS bs_bk_actv_flg
    , bs.bs_prod_ltd_pd_amt          AS bs_bs_prod_ltd_pd_amt
    , fr.individual_id               AS fr_individual_id
    , fr.fr_lst_don_dt               AS fr_fr_lst_don_dt
    , fr.fr_max_don_amt              AS fr_fr_max_don_amt
    , ar.individual_id               AS ar_individual_id
    , ar.mn_last_resp_dt             AS ar_mn_last_resp_dt
    , ar.mx_last_resp_dt             AS ar_mx_last_resp_dt
    , ar.ar_entry_cnt                AS ar_ar_entry_cnt
    , ar.cnt_resp_dt_gt_12           AS ar_cnt_resp_dt_gt_12
    , fp.individual_id               AS fp_individual_id
    , fp.max_pldg_dt                 AS fp_max_pldg_dt
    , flp.individual_id              AS flp_individual_id
    , flp.max_pldg_dt                AS flp_max_pldg_dt
    , abqd.individual_id             AS abqd_individual_id
    , abqd.max_abq_dt                AS abqd_max_abq_dt
    , abqd.min_abq_dt                AS abqd_min_abq_dt
    , abqr.individual_id             AS abqr_individual_id
    , abqr.min_abq_mbr_dt            AS abqr_min_abq_mbr_dt
    , abqr.max_abq_mbr_dt            AS abqr_max_abq_mbr_dt
FROM agg.indiv_ind_xref_temp xr
    LEFT JOIN cte_books_item bi
        ON xr.individual_id = bi.individual_id
    LEFT JOIN cte_books_summary bs
        ON xr.individual_id = bs.individual_id
    LEFT JOIN cte_fr_don fr
        ON xr.individual_id = fr.individual_id
    LEFT JOIN cte_adv_resp ar
        ON xr.individual_id = ar.individual_id
    LEFT JOIN cte_fr_pledge fp
        ON xr.individual_id = fp.individual_id
    LEFT JOIN cte_fr_lt_pledge flp
        ON xr.individual_id = flp.individual_id
    LEFT JOIN cte_abq_don abqd
        ON xr.individual_id = abqd.individual_id
    LEFT JOIN cte_abqr abqr
        ON xr.individual_id = abqr.individual_id
WHERE 1 = 1;

analyse agg.indiv_misc_agg_temp;

DROP TABLE IF EXISTS agg.indiv_issue_lkp_acx_temp;
CREATE TABLE agg.indiv_issue_lkp_acx_temp
    DISTSTYLE KEY
    DISTKEY
        (MAG_CD
        )
    SORTKEY
        (PUB_DT
        ) AS
    SELECT
          lkpmag.magazine_code AS MAG_CD
        , lkpmag.iss_num       AS ISS_NUM
        , lkpmag.pub_date      AS PUB_DT
    FROM etl_proc.lookup_magazine lkpmag;

analyse agg.indiv_issue_lkp_acx_temp;


DROP TABLE IF EXISTS agg.indiv_print_lkp_temp;
CREATE TABLE agg.indiv_print_lkp_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        individual_id
        , COUNT(DISTINCT
              CASE
              WHEN mag_cd = 'CNS'
                   AND set_cd = 'A'
                  THEN 1
              END) + COUNT(cr_break_flg)  AS cr_prod_cnt
        , MAX(CASE
              WHEN mag_cd = 'CNS'
                   AND set_cd = 'A'
                  THEN 1
              END)                        AS cr_prod_cnta
        , COUNT(DISTINCT
              CASE
              WHEN mag_cd = 'CRH'
                   AND set_cd = 'A'
                  THEN 1
              END) + COUNT(hl_break_flg)  AS hl_prod_cnt
        , COUNT(DISTINCT
              CASE
              WHEN mag_cd = 'CRM'
                   AND set_cd = 'A'
                  THEN 1
              END) + COUNT(ma_break_flg)  AS ma_prod_cnt
        , COUNT(DISTINCT
              CASE
              WHEN mag_cd = 'SHM'
                   AND set_cd = 'A'
                  THEN 1
              END) + COUNT(shm_break_flg) AS shm_prod_cnt
    FROM
        (SELECT
             oo.individual_id
             , oo.mag_cd
             , oo.set_cd
             , CASE
               WHEN oo.mag_cd = 'CNS'
                    AND oo.set_cd = 'A'
                    AND MONTHS_BETWEEN(strt_lkp.pub_dt, MAX(end_lkp.pub_dt)
               OVER (
                   PARTITION BY oo.individual_id, oo.mag_cd, oo.set_cd
                   ORDER BY
                       strt_lkp.pub_dt
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) > 6
                   THEN 1
               END AS cr_break_flg
             , CASE
               WHEN oo.mag_cd = 'CRH'
                    AND oo.set_cd = 'A'
                    AND MONTHS_BETWEEN(strt_lkp.pub_dt, MAX(end_lkp.pub_dt)
               OVER (
                   PARTITION BY oo.individual_id, oo.mag_cd, oo.set_cd
                   ORDER BY
                       strt_lkp.pub_dt
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) > 6
                   THEN 1
               END AS hl_break_flg
             , CASE
               WHEN oo.mag_cd = 'CRM'
                    AND oo.set_cd = 'A'
                    AND MONTHS_BETWEEN(strt_lkp.pub_dt, MAX(end_lkp.pub_dt)
               OVER (
                   PARTITION BY oo.individual_id, oo.mag_cd, oo.set_cd
                   ORDER BY
                       strt_lkp.pub_dt
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) > 6
                   THEN 1
               END AS ma_break_flg
             , CASE
               WHEN oo.mag_cd = 'SHM'
                    AND oo.set_cd = 'A'
                    AND MONTHS_BETWEEN(strt_lkp.pub_dt, MAX(end_lkp.pub_dt)
               OVER (
                   PARTITION BY oo.individual_id, oo.mag_cd, oo.set_cd
                   ORDER BY
                       strt_lkp.pub_dt
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) > 6
                   THEN 1
               END AS shm_break_flg
         FROM prod.agg_print_order oo
             LEFT JOIN agg.indiv_issue_lkp_acx_temp end_lkp
                 ON oo.mag_cd = end_lkp.mag_cd
                    AND (oo.orig_strt_iss_num + oo.term_mth_cnt) = end_lkp.iss_num
             LEFT JOIN agg.indiv_issue_lkp_acx_temp strt_lkp
                 ON oo.mag_cd = strt_lkp.mag_cd
                    AND oo.orig_strt_iss_num = strt_lkp.iss_num) polkp
    WHERE 1 = 1
    GROUP BY individual_id;

analyse agg.indiv_print_lkp_temp;

DROP TABLE IF EXISTS    agg.indiv_print_crdt_temp;
CREATE TABLE            agg.indiv_print_crdt_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        er.individual_id
        , MIN(eua.cr_create_date) AS min_create_date
    FROM prod.digital_print_association eua
        INNER JOIN prod.account er
            ON eua.cds_target_id = er.source_account_id
               AND eua.assoc_source_desc = 'iPad'
               AND eua.assoc_source_id = 1
               AND eua.enabled = 'Y'
               AND eua.assoc_source_name = 'user or csr flow'
               AND er.source_name = 'CDS'
    WHERE 1 = 1
    GROUP BY er.individual_id;

analyse agg.indiv_print_crdt_temp;

DROP TABLE IF EXISTS    agg.indiv_bnb_temp;
CREATE TABLE            agg.indiv_bnb_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
 WITH cte_bnb AS
(SELECT
     b.individual_id
     , MIN(b.bnb_last_visit_dt) AS                                                                                                                                                                                                                                                                                                               bnb_frst_visitor_dt
     , MAX(b.bnb_last_visit_dt) AS                                                                                                                                                                                                                                                                                                               bnb_last_visitor_dt
     , MIN(CASE
           WHEN b.bnb_status = 'P'
               THEN b.bnb_lead_recvd_dt
           END)                                                                                                                                                                                                                                                                                                                                  bnb_frst_prospect_dt
     , MAX(CASE
           WHEN b.bnb_status = 'P'
               THEN b.bnb_lead_recvd_dt
           END)                                                                                                                                                                                                                                                                                                                                  bnb_last_prospect_dt
     , MIN(b.bnb_lead_sales_dt) AS                                                                                                                                                                                                                                                                                                               bnb_frst_sales_dt
     , MAX(b.bnb_lead_sales_dt) AS                                                                                                                                                                                                                                                                                                               bnb_last_sales_dt
     , COUNT(DISTINCT
           CASE
           WHEN b.bnb_status = 'P'
               THEN b.bnb_trans_id
           END)                                                                                                                                                                                                                                                                                                                                  bnb_tot_prospect_cnt
     , COUNT(DISTINCT
           CASE
           WHEN b.bnb_status = 'S'
               THEN b.bnb_trans_id
           END)                                                                                                                                                                                                                                                                                                                                  bnb_tot_sale_cnt
     , CASE
       WHEN MAX(CASE
                WHEN b.bnb_status = 'S'
                    THEN 1
                END) = 1
           THEN 'S'
       WHEN MAX(CASE
                WHEN b.bnb_status = 'P'
                    THEN 1
                END) = 1
           THEN 'P'
       WHEN MAX(CASE
                WHEN b.bnb_status = 'V'
                    THEN 1
                END) = 1
           THEN 'V'
       ELSE NULL
       END                      AS                                                                                                                                                                                                                                                                                                               bnb_best_status
     , CASE
       WHEN MAX(CASE
                WHEN b.bnb_status = 'S'
                    THEN 1
                END) = 1
           THEN MAX(b.bnb_lead_sales_dt)
       WHEN MAX(CASE
                WHEN b.bnb_status = 'P'
                    THEN 1
                END) = 1
           THEN MAX(b.bnb_lead_recvd_dt)
       WHEN MAX(CASE
                WHEN b.bnb_status = 'V'
                    THEN 1
                END) = 1
           THEN MAX(b.bnb_last_visit_dt)
       ELSE NULL
       END                      AS                                                                                                                                                                                                                                                                                                               bnb_best_status_dt
     , CASE
       WHEN MAX(GREATEST(NVL(b.bnb_lead_recvd_dt, to_date('19000101', 'YYYYMMDD')), NVL(b.bnb_lead_sales_dt, to_date('19000101', 'YYYYMMDD')), NVL(b.bnb_last_visit_dt, to_date('19000101', 'YYYYMMDD')))) = MAX(NVL(b.bnb_lead_sales_dt, to_date('19000101', 'YYYYMMDD')))
           THEN 'S'
       WHEN MAX(GREATEST(NVL(b.bnb_lead_recvd_dt, to_date('19000101', 'YYYYMMDD')), NVL(b.bnb_lead_sales_dt, to_date('19000101', 'YYYYMMDD')), NVL(b.bnb_last_visit_dt, to_date('19000101', 'YYYYMMDD')))) = MAX(NVL(b.bnb_lead_recvd_dt, to_date('19000101', 'YYYYMMDD')))
           THEN 'P'
       WHEN MAX(GREATEST(NVL(b.bnb_lead_recvd_dt, to_date('19000101', 'YYYYMMDD')), NVL(b.bnb_lead_sales_dt, to_date('19000101', 'YYYYMMDD')), NVL(b.bnb_last_visit_dt, to_date('19000101', 'YYYYMMDD')))) = MAX(NVL(b.bnb_last_visit_dt, to_date('19000101', 'YYYYMMDD')))
           THEN 'V'
       ELSE CAST(NULL AS VARCHAR(1))
       END                      AS                                                                                                                                                                                                                                                                                                               bnb_most_recent_status
     , MAX(GREATEST(NVL(b.bnb_lead_recvd_dt, to_date('19000101', 'YYYYMMDD')), NVL(b.bnb_lead_sales_dt, to_date('19000101', 'YYYYMMDD')), NVL(b.bnb_last_visit_dt, to_date('19000101', 'YYYYMMDD'))))                                                                                                                                            bnb_most_recent_dt
     , COUNT(DISTINCT
           CASE
           WHEN b.bnb_status = 'S'
                AND b.bnb_new_used = 'NEW'
               THEN bnb_trans_id
           END)                                                                                                                                                                                                                                                                                                                                  bnb_nbr_new_sales
     , COUNT(DISTINCT
           CASE
           WHEN b.bnb_status = 'S'
                AND b.bnb_new_used = 'USED'
               THEN bnb_trans_id
           END)                                                                                                                                                                                                                                                                                                                                  bnb_nbr_used_sales
     , UPPER(substring(MAX(greatest(NVL(TO_CHAR(bnb_lead_recvd_dt, 'YYYYMMDDHH24MISS'), '19000101000000') || NVL(bnb_trans_type, ''), NVL(TO_CHAR(bnb_lead_sales_dt, 'YYYYMMDDHH24MISS'), '19000101000000') || NVL(bnb_trans_type, ''), NVL(TO_CHAR(bnb_last_visit_dt, 'YYYYMMDDHH24MISS'), '19000101000000') || NVL(bnb_trans_type, ''))), 15)) BNB_MOST_RECENT_TRANS_TYPE
 FROM prod.agg_build_and_buy b
 GROUP BY b.individual_id
)
SELECT
    individual_id
    , bnb_frst_visitor_dt
    , bnb_last_visitor_dt
    , bnb_frst_prospect_dt
    , bnb_last_prospect_dt
    , bnb_frst_sales_dt
    , bnb_last_sales_dt
    , bnb_tot_prospect_cnt
    , bnb_tot_sale_cnt
    , bnb_best_status
    , bnb_best_status_dt
    , bnb_most_recent_status
    , bnb_most_recent_dt
    , bnb_nbr_new_sales
    , bnb_nbr_used_sales
    , DECODE(BNB_MOST_RECENT_TRANS_TYPE, '', NULL, BNB_MOST_RECENT_TRANS_TYPE) AS BNB_MOST_RECENT_TRANS_TYPE
FROM cte_bnb
WHERE 1 = 1;

analyse agg.indiv_bnb_temp;

DROP TABLE IF EXISTS agg.indiv_print_maglkp_temp;
CREATE TABLE agg.indiv_print_maglkp_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        er.individual_id
        , MAX(il.pub_date) AS mx_cds_pub_dt
    FROM prod.account er
        INNER JOIN prod.print_account_detail oa
            ON oa.hash_account_id = er.hash_account_id
        INNER JOIN etl_proc.lookup_magazine il
            ON il.magazine_code = oa.magazine_code
               AND il.iss_num = oa.iss_num
    WHERE er.source_name IN ('CDS')
    GROUP BY er.individual_id;

analyse agg.indiv_print_maglkp_temp;

DROP TABLE IF EXISTS agg.indiv_printsum_pref_temp;
CREATE TABLE agg.indiv_printsum_pref_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
 WITH cte_pr_summ2 AS
(SELECT individual_id
 FROM prod.agg_print_summary
 WHERE ((cr_actv_flg = 'Y'
         AND cr_curr_mbr_dt <= TO_DATE('08/01/2014', 'MM/DD/YYYY'))
        OR (hl_actv_flg = 'Y'
            AND hl_curr_mbr_dt <= TO_DATE('08/01/2014', 'MM/DD/YYYY'))
        OR (ma_actv_flg = 'Y'
            AND ma_curr_mbr_dt <= TO_DATE('08/01/2014', 'MM/DD/YYYY'))
        OR (shm_actv_flg = 'Y'
            AND shm_curr_mbr_dt <= TO_DATE('08/01/2014', 'MM/DD/YYYY')))
 GROUP BY individual_id
), cte_pref_summ AS
( SELECT individual_id
  FROM prod.agg_preference_summary
  WHERE non_prom_list_rent_flg = 'Y'
  GROUP BY individual_id
), cte_pref AS
(SELECT individual_id
 FROM prod.agg_preference
 WHERE ((data_source = 'CCC'
         AND auth_flg = 'N'
         AND auth_cd IN ('SOY', 'MAIL', 'DNM', 'DNS'))
        OR (data_source = 'CU'
            AND auth_flg = 'N'
            AND auth_cd = 'MAIL'))
 GROUP BY individual_id
)
SELECT mos1.individual_id
FROM prod.agg_print_summary mos1
    LEFT JOIN cte_pr_summ2 mos2
        ON mos1.individual_id = mos2.individual_id
           AND mos2.individual_id IS NULL
    LEFT JOIN cte_pref_summ mas
        ON mos1.individual_id = mas.individual_id
    LEFT JOIN cte_pref auth
        ON mos1.individual_id = auth.individual_id
WHERE ((mos1.cr_curr_mbr_dt > TO_DATE('08/01/2014', 'MM/DD/YYYY')
        AND mos1.cr_actv_flg = 'Y')
       OR (mos1.hl_curr_mbr_dt > TO_DATE('08/01/2014', 'MM/DD/YYYY')
           AND mos1.hl_actv_flg = 'Y')
       OR (mos1.ma_curr_mbr_dt > TO_DATE('08/01/2014', 'MM/DD/YYYY')
           AND mos1.ma_actv_flg = 'Y')
       OR (mos1.shm_curr_mbr_dt > TO_DATE('08/01/2014', 'MM/DD/YYYY')
           AND mos1.shm_actv_flg = 'Y'))
      AND mas.individual_id IS NULL
      AND auth.individual_id IS NULL
GROUP BY mos1.individual_id;

analyse agg.indiv_printsum_pref_temp;

DROP TABLE IF EXISTS    agg.indiv_prospect_email_temp;
CREATE TABLE            agg.indiv_prospect_email_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
WITH cte_name_addr_xref AS (SELECT
                                  cast(NULL AS BIGINT) individual_id
                                , cast(NULL AS BIGINT) OSE_FLG
                                , cast(NULL AS BIGINT) total_cnt
                            FROM prod.legacy_name_address_xref)
SELECT
    ix.individual_id
    , MIN(CASE WHEN pem.individual_id IS NULL
    THEN 'C'
          WHEN pem.individual_id IS NOT NULL
               AND ix.customer_typ = 'P'
               AND nar.ose_flg = 1
               AND nar.total_cnt = 1
              THEN 'A'
          WHEN pem.individual_id IS NOT NULL
              THEN 'B'
          ELSE NULL
          END)                              IND_PROSPECT_EML_MTCH_IND
    , MAX(trunc(em.update_date))         AS PROSPECT_EMAIL_MATCH_DT
    , COUNT(DISTINCT (em.email_subtype)) AS PROSPECT_EMAIL_MATCH_CNT
FROM
    prod.agg_individual_xographic ix
    LEFT JOIN prod.agg_prospect_email pem
        ON ix.individual_id = pem.individual_id
    LEFT JOIN prod.individual_email em
        ON ix.individual_id = em.individual_id
    LEFT JOIN cte_name_addr_xref nar
        ON ix.INDIVIDUAL_ID = nar.individual_id
WHERE 1 = 1
      AND substring(EM.email_subtype, 1, 3) = 'OSE'
GROUP BY ix.individual_id
;

analyse agg.indiv_prospect_email_temp;



DROP TABLE IF EXISTS    agg.indiv_misc_agg_2_temp;
CREATE TABLE            agg.indiv_misc_agg_2_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
 WITH cte_app AS
(SELECT
     individual_id
     , MAX(visit_start_time)     max_visit_strt_time
     , MAX(days_since_first_use) max_days_since_fst_use
 FROM prod.app_session
 GROUP BY individual_id
), cte_key AS
( SELECT *
  FROM agg.indiv_emailfavkey_temp
), cte_advint AS
( SELECT
      individual_id
      , COUNT(*) int_cnt
  FROM prod.agg_constituent_interest
  GROUP BY individual_id
), cte_advsur AS
( SELECT
      individual_id
      , COUNT(*) sur_cnt
  FROM prod.agg_constituent_survey_response
  GROUP BY individual_id
), cte_advdon AS
( SELECT
      individual_id
      , COUNT(*) don_cnt
  FROM prod.agg_constituent_donation
  GROUP BY individual_id
), cte_advalrt AS
( SELECT
      individual_id
      , COUNT(*) alrt_cnt
  FROM prod.agg_constituent_alert_response
  GROUP BY individual_id
), cte_eread AS
(SELECT
     individual_id
     , MAX(es.report_start_date) report_start_date
     , MIN(es.report_start_date) min_rpt_start_date
     , COUNT(*)                  ereader_cnt
 FROM prod.agg_ereader_subscription es
 GROUP BY individual_id
)
SELECT
    xr.individual_id
    , hh_id
    , osl_hh_id
    , cte_app.individual_id          AS app_individual_id
    , cte_app.max_visit_strt_time    AS app_max_visit_strt_time
    , cte_app.max_days_since_fst_use AS app_max_days_since_fst_use
    , cte_key.individual_id          AS key_individual_id
    , cte_key.email_fav_key          AS key_email_fav_key
    , cte_advint.individual_id       AS advint_individual_id
    , cte_advint.int_cnt             AS advint_int_cnt
    , cte_advsur.individual_id       AS advsur_individual_id
    , cte_advsur.sur_cnt             AS advsur_sur_cnt
    , cte_advdon.individual_id       AS advdon_individual_id
    , cte_advdon.don_cnt             AS advdon_don_cnt
    , cte_advalrt.individual_id      AS advalrt_individual_id
    , cte_advalrt.alrt_cnt           AS advalrt_alrt_cnt
    , cte_eread.individual_id        AS eread_individual_id
    , cte_eread.report_start_date    AS eread_report_start_date
    , cte_eread.min_rpt_start_date   AS eread_min_rpt_start_date
    , cte_eread.ereader_cnt          AS eread_ereader_cnt
FROM agg.indiv_ind_xref_temp xr
    LEFT JOIN cte_app
        ON xr.individual_id = cte_app.individual_id
    LEFT JOIN cte_key
        ON xr.individual_id = cte_key.individual_id
    LEFT JOIN cte_advint
        ON xr.individual_id = cte_advint.individual_id
    LEFT JOIN cte_advsur
        ON xr.individual_id = cte_advsur.individual_id
    LEFT JOIN cte_advdon
        ON xr.individual_id = cte_advdon.individual_id
    LEFT JOIN cte_advalrt
        ON xr.individual_id = cte_advalrt.individual_id
    LEFT JOIN cte_eread
        ON xr.individual_id = cte_eread.individual_id
WHERE 1 = 1;

analyse agg.indiv_misc_agg_2_temp;


DROP TABLE IF EXISTS    agg.indiv_precalc_1_temp;
CREATE TABLE            agg.indiv_precalc_1_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        xr.individual_id
        , xr.hh_id
        , abq.abq_fst_don_dt      AS abq_abq_fst_don_dt
        , abq.abq_fst_rsp_dt      AS abq_abq_fst_rsp_dt
        , abq.abq_lst_don_amt     AS abq_abq_lst_don_amt
        , abq.abq_lst_don_dt      AS abq_abq_lst_don_dt
        , abq.abq_lst_rsp_dt      AS abq_abq_lst_rsp_dt
        , abq.abq_ltd_don_amt     AS abq_abq_ltd_don_amt
        , abq.abq_ltd_don_cnt     AS abq_abq_ltd_don_cnt
        , abq.abq_ltd_mbr_cnt     AS abq_abq_ltd_mbr_cnt
        , abq.abq_ltd_rsp_cnt     AS abq_abq_ltd_rsp_cnt
        , abq.abq_max_don_amt     AS abq_abq_max_don_amt
        , abq.abq_mbr_flg         AS abq_abq_mbr_flg
        , abq.individual_id       AS abq_individual_id
        , fs.fr_fst_don_dt        AS fs_fr_fst_don_dt
        , fs.fr_lst_don_amt       AS fs_fr_lst_don_amt
        , fs.fr_lst_don_dt        AS fs_fr_lst_don_dt
        , fs.fr_ltd_don_amt       AS fs_fr_ltd_don_amt
        , fs.fr_ltd_don_cnt       AS fs_fr_ltd_don_cnt
        , fs.fr_max_don_amt       AS fs_fr_max_don_amt
        , fs.individual_id        AS fs_individual_id
        , ix.acx_gender_cd        AS ix_acx_gender_cd
        , ix.ad_apl_keycode       AS ix_ad_apl_keycode
        , ix.customer_typ         AS ix_customer_typ
        , ix.dc_cdb_last_ord_dt   AS ix_dc_cdb_last_ord_dt
        , ix.dc_corp_first_dt     AS ix_dc_corp_first_dt
        , ix.dc_fund_first_dt     AS ix_dc_fund_first_dt
        , ix.dc_last_aps_ord_dt   AS ix_dc_last_aps_ord_dt
        , ix.dc_offline_first_dt  AS ix_dc_offline_first_dt
        , ix.dc_online_first_dt   AS ix_dc_online_first_dt
        , ix.individual_id        AS ix_individual_id
        , ofs.cr_actv_flg         AS ofs_cr_actv_flg
        , ofs.cr_flg              AS ofs_cr_flg
        , ofs.cr_fst_dnr_dt       AS ofs_cr_fst_dnr_dt
        , ofs.cr_non_dnr_flg      AS ofs_cr_non_dnr_flg
        , ofs.cr_non_sub_dnr_flg  AS ofs_cr_non_sub_dnr_flg
        , ofs.cr_sub_dnr_flg      AS ofs_cr_sub_dnr_flg
        , ofs.hl_actv_flg         AS ofs_hl_actv_flg
        , ofs.hl_flg              AS ofs_hl_flg
        , ofs.hl_fst_dnr_dt       AS ofs_hl_fst_dnr_dt
        , ofs.hl_non_dnr_flg      AS ofs_hl_non_dnr_flg
        , ofs.hl_non_sub_dnr_flg  AS ofs_hl_non_sub_dnr_flg
        , ofs.hl_sub_dnr_flg      AS ofs_hl_sub_dnr_flg
        , ofs.individual_id       AS ofs_individual_id
        , ofs.ma_actv_flg         AS ofs_ma_actv_flg
        , ofs.ma_flg              AS ofs_ma_flg
        , ofs.ma_fst_dnr_dt       AS ofs_ma_fst_dnr_dt
        , ofs.ma_non_dnr_flg      AS ofs_ma_non_dnr_flg
        , ofs.ma_non_sub_dnr_flg  AS ofs_ma_non_sub_dnr_flg
        , ofs.ma_sub_dnr_flg      AS ofs_ma_sub_dnr_flg
        , ofs.shm_actv_flg        AS ofs_shm_actv_flg
        , ofs.shm_flg             AS ofs_shm_flg
        , ofs.shm_fst_dnr_dt      AS ofs_shm_fst_dnr_dt
        , ofs.shm_non_dnr_flg     AS ofs_shm_non_dnr_flg
        , ofs.shm_non_sub_dnr_flg AS ofs_shm_non_sub_dnr_flg
        , ofs.shm_sub_dnr_flg     AS ofs_shm_sub_dnr_flg
        , ols.carp_actv_flg       AS ols_carp_actv_flg
        , ols.crmg_flg            AS ols_crmg_flg
        , ols.cro_actv_flg        AS ols_cro_actv_flg
        , ols.cro_flg             AS ols_cro_flg
        , ols.cro_fst_dnr_dt      AS ols_cro_fst_dnr_dt
        , ols.cro_non_dnr_flg     AS ols_cro_non_dnr_flg
        , ols.cro_non_sub_dnr_flg AS ols_cro_non_sub_dnr_flg
        , ols.cro_sub_dnr_flg     AS ols_cro_sub_dnr_flg
        , ols.individual_id       AS ols_individual_id
        -- , ols.nc_rpts_flg            as ols_nc_rpts_flg
        -- , ols.nc_rpts_lst_ord_dt     as ols_nc_rpts_lst_ord_dt
        , ols.ncbk_flg            AS ols_ncbk_flg
        , ols.uc_rpts_flg         AS ols_uc_rpts_flg
        -- , ols.uc_rpts_lst_ord_dt     as ols_uc_rpts_lst_ord_dt
        , ols.ucbk_flg            AS ols_ucbk_flg
    FROM agg.indiv_ind_xref_temp xr
        INNER JOIN prod.agg_individual_xographic ix
            ON xr.individual_id = ix.individual_id
        LEFT JOIN prod.agg_abq_summary abq
            ON xr.individual_id = abq.individual_id
        LEFT JOIN prod.agg_fundraising_summary fs
            ON xr.individual_id = fs.individual_id
        LEFT JOIN prod.agg_print_summary ofs
            ON xr.individual_id = ofs.individual_id
        LEFT JOIN prod.agg_digital_summary ols
            ON xr.individual_id = ols.individual_id;

analyse agg.indiv_precalc_1_temp;

DROP TABLE IF EXISTS    agg.indiv_precalc_2_temp;
CREATE TABLE            agg.indiv_precalc_2_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS WITH cte_adv_indiv AS
    ( SELECT DISTINCT individual_id
      FROM prod.account acc
      WHERE EXISTS(SELECT NULL
                   FROM prod.constituent cons
                   WHERE acc.hash_account_id = cons.hash_account_id)
    ), cte_fr_comb_primary AS
    (SELECT
         primary_individual_id
         , MAX(join_eff_date) mx_eff_dt
     FROM prod.fundraising_combination
     GROUP BY primary_individual_id
    ), cte_fr_comb_secondary AS
    ( SELECT DISTINCT secondary_individual_id
      FROM prod.fundraising_combination
    ), cte_name_address AS
    ( SELECT
          individual_id
          , coa_date
      FROM prod.agg_name_address
      WHERE NVL(coa_source, 'XXX') != 'NCA'
    )
    SELECT
        xr.individual_id
        , xr.hh_id
        , adv.individual_id                AS adv_individual_id
        , adv_mt.adv_lst_don_dt            AS adv_mt_adv_lst_don_dt
        , adv_mt.adv_lst_don_amt           AS adv_mt_adv_lst_don_amt
        , adv_mt.adv_ltd_don_amt           AS adv_mt_adv_ltd_don_amt
        , adv_mt.adv_max_don_amt           AS adv_mt_adv_max_don_amt
        , adv_mt.adv_fst_don_dt            AS adv_mt_adv_fst_don_dt
        , adv_mt.adv_ltd_don_cnt           AS adv_mt_adv_ltd_don_cnt
        , adv_mt.first_action_dt           AS adv_mt_first_action_dt
        , adv_mt.individual_id             AS adv_mt_individual_id
        , adv_mt.last_action_dt            AS adv_mt_last_action_dt
        , adv_mt.adv_active_flg            AS adv_mt_adv_active_flg
        , adv_mt.adv_volunteer_flg         AS adv_mt_adv_volunteer_flg
        , adv_mt.adv_ltd_alrt_resp_cnt     AS adv_mt_adv_ltd_alrt_resp_cnt
        , adv_mt.adv_ltd_sur_resp_cnt      AS adv_mt_adv_ltd_sur_resp_cnt
        , bo.bo_ce_cnt                     AS bo_bo_ce_cnt
        , bo.bo_fst_prod_cd                AS bo_bo_fst_prod_cd
        , bo.bo_max_ord_dt                 AS bo_bo_max_ord_dt
        , bo.bo_min_ord_dt                 AS bo_bo_min_ord_dt
        , bo.bo_pmt_amt                    AS bo_bo_pmt_amt
        , bo.bo_pmt_dt                     AS bo_bo_pmt_dt
        , bo.bo_prod_dm_ord_cnt            AS bo_bo_prod_dm_ord_cnt
        , bo.bo_prod_dnr_ord_cnt           AS bo_bo_prod_dnr_ord_cnt
        , bo.bo_prod_em_ord_cnt            AS bo_bo_prod_em_ord_cnt
        , bo.bo_prod_fst_ord_dt            AS bo_bo_prod_fst_ord_dt
        , bo.bo_prod_lst_ord_dt            AS bo_bo_prod_lst_ord_dt
        , bo.bo_prod_ord_cnt               AS bo_bo_prod_ord_cnt
        , bo.bo_recip_cd_d_cnt             AS bo_bo_recip_cd_d_cnt
        , bo.bo_recip_cd_not_d_cnt         AS bo_bo_recip_cd_not_d_cnt
        , bo.bo_xce_cnt                    AS bo_bo_xce_cnt
        , bo.individual_id                 AS bo_individual_id
        , cga.cga_cnt                      AS cga_cga_cnt
        , cga.cga_don_amt                  AS cga_cga_don_amt
        , cga.individual_id                AS cga_individual_id
        , cga.mn_don_dt                    AS cga_mn_don_dt
        , cga.mx_don_dt                    AS cga_mx_don_dt
        , de.de_cnt                        AS de_de_cnt
        , de.de_cr_prod_cnt                AS de_de_cr_prod_cnt
        , de.de_fst_prod_cd                AS de_de_fst_prod_cd
        , de.de_hl_prod_cnt                AS de_de_hl_prod_cnt
        , de.de_max_exp_dt                 AS de_de_max_exp_dt
        , de.de_min_exp_dt                 AS de_de_min_exp_dt
        , de.individual_id                 AS de_individual_id
        , er.er_fst_dt                     AS er_er_fst_dt
        , er.individual_id                 AS er_individual_id
        , frc.mx_eff_dt                    AS frc_mx_eff_dt
        , frc.primary_individual_id        AS frc_prim_indiv_id
        , frc2.secondary_individual_id     AS frc2_secd_indiv_id
        , na.coa_date                      AS na_coa_date
        , na.individual_id                 AS na_individual_id
        , oaa.individual_id                AS oaa_individual_id
        , oaa.mx_merge_dt                  AS oaa_mx_merge_dt
        , ofo.individual_id                AS ofo_individual_id
        , ofo.ofo_ce_cnt                   AS ofo_ofo_ce_cnt
        , ofo.ofo_cr_prod_cnt              AS ofo_ofo_cr_prod_cnt
        , ofo.ofo_fst_prod_cd              AS ofo_ofo_fst_prod_cd
        , ofo.ofo_hl_prod_cnt              AS ofo_ofo_hl_prod_cnt
        , ofo.ofo_ma_prod_cnt              AS ofo_ofo_ma_prod_cnt
        , ofo.ofo_mn_ord_dt                AS ofo_ofo_mn_ord_dt
        , ofo.ofo_mx_canc_dt               AS ofo_ofo_mx_canc_dt
        , ofo.ofo_mx_cc_ord_dt             AS ofo_ofo_mx_cc_ord_dt
        , ofo.ofo_mx_ord_dt                AS ofo_ofo_mx_ord_dt
        , ofo.ofo_mx_pmt_dt                AS ofo_ofo_mx_pmt_dt
        , ofo.ofo_pd_amt                   AS ofo_ofo_pd_amt
        , ofo.ofo_prod_dm_ord_cnt          AS ofo_ofo_prod_dm_ord_cnt
        , ofo.ofo_prod_dnr_ord_cnt         AS ofo_ofo_prod_dnr_ord_cnt
        , ofo.ofo_prod_em_ord_cnt          AS ofo_ofo_prod_em_ord_cnt
        , ofo.ofo_prod_fst_ord_dt          AS ofo_ofo_prod_fst_ord_dt
        , ofo.ofo_prod_lst_canc_bad_dbt_dt AS ofo_ofo_prod_lst_canc_bad_dbt_dt
        , ofo.ofo_prod_lst_ord_dt          AS ofo_ofo_prod_lst_ord_dt
        , ofo.ofo_prod_ltd_pd_amt          AS ofo_ofo_prod_ltd_pd_amt
        , ofo.ofo_prod_ord_cnt             AS ofo_ofo_prod_ord_cnt
        , ofo.ofo_shm_prod_cnt             AS ofo_ofo_shm_prod_cnt
        , ofo.ofo_xce_cnt                  AS ofo_ofo_xce_cnt
        , oli.individual_id                AS oli_individual_id
        , oli.oli_aps_prod_cnt             AS oli_oli_aps_prod_cnt
        , oli.oli_carp_cnt                 AS oli_oli_carp_cnt
        , oli.oli_carp_prod_cnt            AS oli_oli_carp_prod_cnt
        , oli.oli_ce_cnt                   AS oli_oli_ce_cnt
        , oli.oli_crmg_prod_cnt            AS oli_oli_crmg_prod_cnt
        , oli.oli_cro_prod_cnt             AS oli_oli_cro_prod_cnt
        , oli.oli_cro_prod_cnta            AS oli_oli_cro_prod_cnta
        , oli.oli_fst_prod_cd              AS oli_oli_fst_prod_cd
        , oli.oli_mn_crt_dt                AS oli_oli_mn_crt_dt
        , oli.oli_mx_canc_dt               AS oli_oli_mx_canc_dt
        , oli.oli_mx_crt_dt                AS oli_oli_mx_crt_dt
        , oli.oli_mx_end_dt                AS oli_oli_mx_end_dt
        , oli.oli_mx_pmt_dt                AS oli_oli_mx_pmt_dt
        , oli.oli_prod_dnr_ord_cnt         AS oli_oli_prod_dnr_ord_cnt
        , oli.oli_prod_lst_canc_bad_dbt_dt AS oli_oli_prod_lst_canc_bad_dbt_dt
        , oli.oli_tot_amt                  AS oli_oli_tot_amt
        , oli.oli_tot_amt2                 AS oli_oli_tot_amt2
        , oli.oli_xce_cnt                  AS oli_oli_xce_cnt
        , oli.oli_xce_d_cnt                AS oli_oli_xce_d_cnt
        , oli.oli_xce_egknu_cnt            AS oli_oli_xce_egknu_cnt
        , olo.individual_id                AS olo_individual_id
        , olo.olo_mx_crt_dt                AS olo_olo_mx_crt_dt
        , a.auth_cnt                       AS a_auth_cnt
        , a.auth_flg_cnt                   AS a_auth_flg_cnt
        , a.individual_id                  AS a_individual_id
        , a.mn_fst_dt                      AS a_mn_fst_dt
        , a.mn_newsltr_fst_dt              AS a_mn_newsltr_fst_dt
        , a.mrp_flg                        AS a_mrp_flg
        , a.mx_fst_dt                      AS a_mx_fst_dt
        , a.mx_news_auth_n_eff_dt          AS a_mx_news_auth_n_eff_dt
        , a.mx_newsltr_fst_dt              AS a_mx_newsltr_fst_dt
        , a.news_auth_cnt                  AS a_news_auth_cnt
        , a.news_auth_y_cnt                AS a_news_auth_y_cnt
        , a.news_best_buy_drug_flg         AS a_news_best_buy_drug_flg
        , a.news_car_watch_flg             AS a_news_car_watch_flg
        , a.news_cro_whats_flg             AS a_news_cro_whats_flg
        , a.news_green_choice_flg          AS a_news_green_choice_flg
        , a.news_hlth_after_60_flg         AS a_news_hlth_after_60_flg
        , a.news_hlth_alert_flg            AS a_news_hlth_alert_flg
        , a.news_hlth_child_teen_flg       AS a_news_hlth_child_teen_flg
        , a.news_hlth_diabetes_flg         AS a_news_hlth_diabetes_flg
        , a.news_hlth_heart_flg            AS a_news_hlth_heart_flg
        , a.news_hlth_women_flg            AS a_news_hlth_women_flg
        , a.news_safety_alert_flg          AS a_news_safety_alert_flg
    FROM agg.indiv_ind_xref_temp xr
        LEFT JOIN cte_adv_indiv adv
            ON xr.individual_id = adv.individual_id
        LEFT JOIN prod.agg_constituent adv_mt
            ON xr.individual_id = adv_mt.individual_id
        LEFT JOIN agg.indiv_books_ord_temp bo
            ON xr.individual_id = bo.individual_id
        LEFT JOIN agg.indiv_cga_temp cga
            ON xr.individual_id = cga.individual_id
        LEFT JOIN agg.INDIV_DE_EXPIRES_TEMP de
            ON xr.individual_id = de.individual_id
        LEFT JOIN agg.indiv_ext_ref_dt_temp er
            ON xr.individual_id = er.individual_id
        LEFT JOIN cte_fr_comb_primary frc
            ON xr.individual_id = frc.primary_individual_id
        LEFT JOIN cte_fr_comb_secondary frc2
            ON xr.individual_id = frc2.secondary_individual_id
        LEFT JOIN cte_name_address na
            ON xr.individual_id = na.individual_id
        LEFT JOIN agg.indiv_acct_merge_temp oaa
            ON xr.individual_id = oaa.individual_id
        LEFT JOIN agg.indiv_off_canc_temp ofo
            ON xr.individual_id = ofo.individual_id
        LEFT JOIN agg.indiv_oli_sku_temp oli
            ON xr.individual_id = oli.individual_id
        LEFT JOIN agg.indiv_onl_item_ord_temp olo
            ON xr.individual_id = olo.individual_id
        LEFT JOIN agg.indiv_preference_temp a
            ON xr.individual_id = a.individual_id;

analyse agg.indiv_precalc_2_temp;

DROP TABLE IF EXISTS    agg.indiv_precalc_3_temp;
CREATE TABLE            agg.indiv_precalc_3_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS WITH cte_email_address AS (SELECT
                                      ie.individual_id
                                      , ie.email_address
                                  FROM prod.individual_email ie
    ), cte_agg_acc_num AS (SELECT
                               individual_id
                               , MAX(CASE WHEN GROUP_ID = '311'
            THEN 'Y'
                                     ELSE NULL END) AS an_emp_flg
                               , MAX(CASE WHEN usr_src_ind LIKE 'FORUM%'
            THEN 'Y'
                                     ELSE NULL END) AS an_forums_flag
                           FROM prod.agg_account_number
                           WHERE acct_prefix = 'PWI'
                           GROUP BY individual_id
    ), cte_ol_deprecated AS (SELECT
                                 --SD 20171017: Outside List are deprecated
                                   cast(NULL AS BIGINT) AS individual_id
                                 , cast(NULL AS DATE)   AS ol_match_recent_dt
                                 , 0                    AS ol_match_cnt
                                 , 0                       ol_match_cnt_6m
                                 , 0                       ol_match_cnt_7_12m
                                 , 0                       ol_match_cnt_13_18m)
    SELECT
        xr.individual_id
        , xr.hh_id
        , an.an_emp_flg                  AS an_an_emp_flg
        , an.an_forums_flag              AS an_an_forums_flag
        , an.individual_id               AS an_individual_id
        , bb.bnb_frst_sales_dt           AS bb_bnb_frst_sales_dt
        , bb.bnb_last_sales_dt           AS bb_bnb_last_sales_dt
        , bb.bnb_tot_sale_cnt            AS bb_bnb_tot_sale_cnt
        , bb.individual_id               AS bb_individual_id
        , brks.cr_prod_cnt               AS brks_cr_prod_cnt
        , brks.cr_prod_cnta              AS brks_cr_prod_cnta
        , brks.hl_prod_cnt               AS brks_hl_prod_cnt
        , brks.individual_id             AS brks_individual_id
        , brks.ma_prod_cnt               AS brks_ma_prod_cnt
        , brks.shm_prod_cnt              AS brks_shm_prod_cnt
        , bnb.individual_id              AS bnb_individual_id
        , bnb.bnb_frst_visitor_dt        AS bnb_bnb_frst_visitor_dt
        , bnb.bnb_last_visitor_dt        AS bnb_bnb_last_visitor_dt
        , bnb.bnb_frst_prospect_dt       AS bnb_bnb_frst_prospect_dt
        , bnb.bnb_last_prospect_dt       AS bnb_bnb_last_prospect_dt
        , bnb.bnb_frst_sales_dt          AS bnb_bnb_frst_sales_dt
        , bnb.bnb_last_sales_dt          AS bnb_bnb_last_sales_dt
        , bnb.bnb_tot_prospect_cnt       AS bnb_bnb_tot_prospect_cnt
        , bnb.bnb_tot_sale_cnt           AS bnb_bnb_tot_sale_cnt
        , bnb.bnb_best_status            AS bnb_bnb_best_status
        , bnb.bnb_best_status_dt         AS bnb_bnb_best_status_dt
        , bnb.bnb_most_recent_status     AS bnb_bnb_most_recent_status
        , bnb.bnb_most_recent_dt         AS bnb_bnb_most_recent_dt
        , bnb.bnb_nbr_new_sales          AS bnb_bnb_nbr_new_sales
        , bnb.bnb_nbr_used_sales         AS bnb_bnb_nbr_used_sales
        , bnb.bnb_most_recent_trans_type AS bnb_bnb_most_recent_trans_type
        , e.email_type_cd                AS e_email_type_cd
        , e.individual_id                AS e_individual_id
        , e.src_delv_ind                 AS e_src_delv_ind
        , e.src_valid_flag               AS e_src_valid_flag
        , e.valid_flag                   AS e_valid_flag
        , eua.individual_id              AS eua_individual_id
        , eua.min_create_date            AS eua_min_create_date
        , ibx.age_infobase               AS ibx_age_infobase
        , ibx.gender_infobase            AS ibx_gender_infobase
        , ibx.individual_id              AS ibx_individual_id
        , ol.individual_id               AS ol_individual_id
        , ol.ol_match_cnt                AS ol_ol_match_cnt
        , ol.ol_match_cnt_13_18m         AS ol_ol_match_cnt_13_18m
        , ol.ol_match_cnt_6m             AS ol_ol_match_cnt_6m
        , ol.ol_match_cnt_7_12m          AS ol_ol_match_cnt_7_12m
        , ol.ol_match_recent_dt          AS ol_ol_match_recent_dt
        , we.email_address               AS we_email_address
        , we.individual_id               AS we_individual_id

    FROM agg.indiv_ind_xref_temp xr
        LEFT JOIN agg.indiv_print_lkp_temp brks
            ON xr.individual_id = brks.individual_id
        LEFT JOIN prod.agg_email e
            ON xr.individual_id = e.individual_id
        LEFT JOIN cte_email_address we
            ON xr.individual_id = we.individual_id
        LEFT JOIN cte_agg_acc_num an
            ON xr.individual_id = an.individual_id
        LEFT JOIN cte_ol_deprecated ol
            ON xr.individual_id = ol.individual_id
        LEFT JOIN agg.indiv_ibx_temp ibx
            ON xr.individual_id = ibx.individual_id
        LEFT JOIN agg.indiv_print_crdt_temp eua
            ON xr.individual_id = eua.individual_id
        LEFT JOIN agg.indiv_bnb_temp bb
            ON xr.individual_id = bb.individual_id
        LEFT JOIN agg.indiv_bnb_temp bnb
            ON xr.individual_id = bnb.individual_id
;

analyse agg.indiv_precalc_3_temp;

DROP TABLE IF EXISTS    agg.indiv_precalc_4_temp;
CREATE TABLE            agg.indiv_precalc_4_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
SELECT
    xr.individual_id
    , xr.hh_id
    , coop.individual_id            AS coop_individual_id
    , erm.individual_id             AS erm_individual_id
    , erm.mx_cds_pub_dt             AS erm_mx_cds_pub_dt
    , ia.acx_country_code           AS ia_country_id
    , ia.individual_id              AS ia_individual_id
    , ofoxog.dc_corp_first_ind      AS ofoxog_dc_corp_first_ind
    , ofoxog.dc_corp_first_ind2     AS ofoxog_dc_corp_first_ind2
    , ofoxog.dc_corp_fst_crt_ind    AS ofoxog_dc_corp_fst_crt_ind
    , ofoxog.dc_offline_fst_crt_ind AS ofoxog_dc_offline_fst_crt_ind
    , ofoxog.individual_id          AS ofoxog_individual_id
    , olixog1.dc_last_aps_crt_ind   AS olixog1_dc_last_aps_crt_ind
    , olixog1.individual_id         AS olixog1_individual_id
    , olixog2.dc_online_fst_aps_ind AS olixog2_dc_online_fst_aps_ind
    , olixog2.dc_online_fst_crt_ind AS olixog2_dc_online_fst_crt_ind
    , olixog2.individual_id         AS olixog2_individual_id
    , page.hit_time                 AS page_hit_time
    , page.individual_id            AS page_individual_id
    , page.min_hit_time             AS page_min_hit_time
    , surv.answer_date              AS surv_answer_date
    , surv.individual_id            AS surv_individual_id
    , surv.min_answer_date          AS surv_min_answer_date

FROM agg.indiv_ind_xref_temp xr
    LEFT JOIN prod.individual_address ia
        ON xr.individual_id = ia.individual_id
    LEFT JOIN agg.indiv_print_maglkp_temp erm
        ON xr.individual_id = erm.individual_id
    LEFT JOIN agg.indiv_printsum_pref_temp coop
        ON xr.individual_id = coop.individual_id
    LEFT JOIN agg.indiv_survey_resp_dt_temp surv
        ON xr.individual_id = surv.individual_id
    LEFT JOIN agg.indiv_webpage_temp page
        ON xr.individual_id = page.individual_id
    LEFT JOIN agg.indiv_oli_xog_2_temp olixog2
        ON xr.individual_id = olixog2.individual_id
    LEFT JOIN agg.indiv_oli_xog_1_temp olixog1
        ON xr.individual_id = olixog1.individual_id
    LEFT JOIN  agg.indiv_ofo_xog_temp ofoxog
        ON xr.individual_id = ofoxog.individual_id
;


analyse agg.indiv_precalc_4_temp;

DROP TABLE IF EXISTS    agg.indiv_precalc_fin_temp;
CREATE TABLE            agg.indiv_precalc_fin_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT
            xr.individual_id
            , xr.hh_id
            , xr.osl_hh_id

            , precalc1.abq_abq_fst_don_dt               AS abq_abq_fst_don_dt
            , precalc1.abq_abq_fst_rsp_dt               AS abq_abq_fst_rsp_dt
            , precalc1.abq_abq_lst_don_amt              AS abq_abq_lst_don_amt
            , precalc1.abq_abq_lst_don_dt               AS abq_abq_lst_don_dt
            , precalc1.abq_abq_lst_rsp_dt               AS abq_abq_lst_rsp_dt
            , precalc1.abq_abq_ltd_don_amt              AS abq_abq_ltd_don_amt
            , precalc1.abq_abq_ltd_don_cnt              AS abq_abq_ltd_don_cnt
            , precalc1.abq_abq_ltd_mbr_cnt              AS abq_abq_ltd_mbr_cnt
            , precalc1.abq_abq_ltd_rsp_cnt              AS abq_abq_ltd_rsp_cnt
            , precalc1.abq_abq_max_don_amt              AS abq_abq_max_don_amt
            , precalc1.abq_abq_mbr_flg                  AS abq_abq_mbr_flg
            , precalc1.abq_individual_id                AS abq_individual_id
            , precalc1.fs_fr_fst_don_dt                 AS fs_fr_fst_don_dt
            , precalc1.fs_fr_lst_don_amt                AS fs_fr_lst_don_amt
            , precalc1.fs_fr_lst_don_dt                 AS fs_fr_lst_don_dt
            , precalc1.fs_fr_ltd_don_amt                AS fs_fr_ltd_don_amt
            , precalc1.fs_fr_ltd_don_cnt                AS fs_fr_ltd_don_cnt
            , precalc1.fs_fr_max_don_amt                AS fs_fr_max_don_amt
            , precalc1.fs_individual_id                 AS fs_individual_id
            , precalc1.ix_acx_gender_cd                 AS ix_acx_gender_cd
            , precalc1.ix_ad_apl_keycode                AS ix_ad_apl_keycode
            , precalc1.ix_customer_typ                  AS ix_customer_typ
            , precalc1.ix_dc_cdb_last_ord_dt            AS ix_dc_cdb_last_ord_dt
            , precalc1.ix_dc_corp_first_dt              AS ix_dc_corp_first_dt
            , precalc1.ix_dc_fund_first_dt              AS ix_dc_fund_first_dt
            , precalc1.ix_dc_last_aps_ord_dt            AS ix_dc_last_aps_ord_dt
            , precalc1.ix_dc_offline_first_dt           AS ix_dc_offline_first_dt
            , precalc1.ix_dc_online_first_dt            AS ix_dc_online_first_dt
            , precalc1.ix_individual_id                 AS ix_individual_id
            , precalc1.ofs_cr_actv_flg                  AS ofs_cr_actv_flg
            , precalc1.ofs_cr_flg                       AS ofs_cr_flg
            , precalc1.ofs_cr_fst_dnr_dt                AS ofs_cr_fst_dnr_dt
            , precalc1.ofs_cr_non_dnr_flg               AS ofs_cr_non_dnr_flg
            , precalc1.ofs_cr_non_sub_dnr_flg           AS ofs_cr_non_sub_dnr_flg
            , precalc1.ofs_cr_sub_dnr_flg               AS ofs_cr_sub_dnr_flg
            , precalc1.ofs_hl_actv_flg                  AS ofs_hl_actv_flg
            , precalc1.ofs_hl_flg                       AS ofs_hl_flg
            , precalc1.ofs_hl_fst_dnr_dt                AS ofs_hl_fst_dnr_dt
            , precalc1.ofs_hl_non_dnr_flg               AS ofs_hl_non_dnr_flg
            , precalc1.ofs_hl_non_sub_dnr_flg           AS ofs_hl_non_sub_dnr_flg
            , precalc1.ofs_hl_sub_dnr_flg               AS ofs_hl_sub_dnr_flg
            , precalc1.ofs_individual_id                AS ofs_individual_id
            , precalc1.ofs_ma_actv_flg                  AS ofs_ma_actv_flg
            , precalc1.ofs_ma_flg                       AS ofs_ma_flg
            , precalc1.ofs_ma_fst_dnr_dt                AS ofs_ma_fst_dnr_dt
            , precalc1.ofs_ma_non_dnr_flg               AS ofs_ma_non_dnr_flg
            , precalc1.ofs_ma_non_sub_dnr_flg           AS ofs_ma_non_sub_dnr_flg
            , precalc1.ofs_ma_sub_dnr_flg               AS ofs_ma_sub_dnr_flg
            , precalc1.ofs_shm_actv_flg                 AS ofs_shm_actv_flg
            , precalc1.ofs_shm_flg                      AS ofs_shm_flg
            , precalc1.ofs_shm_fst_dnr_dt               AS ofs_shm_fst_dnr_dt
            , precalc1.ofs_shm_non_dnr_flg              AS ofs_shm_non_dnr_flg
            , precalc1.ofs_shm_non_sub_dnr_flg          AS ofs_shm_non_sub_dnr_flg
            , precalc1.ofs_shm_sub_dnr_flg              AS ofs_shm_sub_dnr_flg
            , precalc1.ols_carp_actv_flg                AS ols_carp_actv_flg
            , precalc1.ols_crmg_flg                     AS ols_crmg_flg
            , precalc1.ols_cro_actv_flg                 AS ols_cro_actv_flg
            , precalc1.ols_cro_flg                      AS ols_cro_flg
            , precalc1.ols_cro_fst_dnr_dt               AS ols_cro_fst_dnr_dt
            , precalc1.ols_cro_non_dnr_flg              AS ols_cro_non_dnr_flg
            , precalc1.ols_cro_non_sub_dnr_flg          AS ols_cro_non_sub_dnr_flg
            , precalc1.ols_cro_sub_dnr_flg              AS ols_cro_sub_dnr_flg
            , precalc1.ols_individual_id                AS ols_individual_id
            , precalc1.ols_ncbk_flg                     AS ols_ncbk_flg
            , precalc1.ols_uc_rpts_flg                  AS ols_uc_rpts_flg
            , precalc1.ols_ucbk_flg                     AS ols_ucbk_flg

            , precalc2.adv_individual_id                AS adv_individual_id
            , precalc2.adv_mt_adv_lst_don_dt            AS adv_mt_adv_lst_don_dt
            , precalc2.adv_mt_adv_lst_don_amt           AS adv_mt_adv_lst_don_amt
            , precalc2.adv_mt_adv_ltd_don_amt           AS adv_mt_adv_ltd_don_amt
            , precalc2.adv_mt_adv_max_don_amt           AS adv_mt_adv_max_don_amt
            , precalc2.adv_mt_adv_fst_don_dt            AS adv_mt_adv_fst_don_dt
            , precalc2.adv_mt_adv_ltd_don_cnt           AS adv_mt_adv_ltd_don_cnt
            , precalc2.adv_mt_first_action_dt           AS adv_mt_first_action_dt
            , precalc2.adv_mt_individual_id             AS adv_mt_individual_id
            , precalc2.adv_mt_last_action_dt            AS adv_mt_last_action_dt
            , precalc2.adv_mt_adv_active_flg            AS adv_mt_adv_active_flg
            , precalc2.adv_mt_adv_volunteer_flg         AS adv_mt_adv_volunteer_flg
            , precalc2.adv_mt_adv_ltd_alrt_resp_cnt     AS adv_mt_adv_ltd_alrt_resp_cnt
            , precalc2.adv_mt_adv_ltd_sur_resp_cnt      AS adv_mt_adv_ltd_sur_resp_cnt
            , precalc2.bo_bo_ce_cnt                     AS bo_bo_ce_cnt
            , precalc2.bo_bo_fst_prod_cd                AS bo_bo_fst_prod_cd
            , precalc2.bo_bo_max_ord_dt                 AS bo_bo_max_ord_dt
            , precalc2.bo_bo_min_ord_dt                 AS bo_bo_min_ord_dt
            , precalc2.bo_bo_pmt_amt                    AS bo_bo_pmt_amt
            , precalc2.bo_bo_pmt_dt                     AS bo_bo_pmt_dt
            , precalc2.bo_bo_prod_dm_ord_cnt            AS bo_bo_prod_dm_ord_cnt
            , precalc2.bo_bo_prod_dnr_ord_cnt           AS bo_bo_prod_dnr_ord_cnt
            , precalc2.bo_bo_prod_em_ord_cnt            AS bo_bo_prod_em_ord_cnt
            , precalc2.bo_bo_prod_fst_ord_dt            AS bo_bo_prod_fst_ord_dt
            , precalc2.bo_bo_prod_lst_ord_dt            AS bo_bo_prod_lst_ord_dt
            , precalc2.bo_bo_prod_ord_cnt               AS bo_bo_prod_ord_cnt
            , precalc2.bo_bo_recip_cd_d_cnt             AS bo_bo_recip_cd_d_cnt
            , precalc2.bo_bo_recip_cd_not_d_cnt         AS bo_bo_recip_cd_not_d_cnt
            , precalc2.bo_bo_xce_cnt                    AS bo_bo_xce_cnt
            , precalc2.bo_individual_id                 AS bo_individual_id
            , precalc2.cga_cga_cnt                      AS cga_cga_cnt
            , precalc2.cga_cga_don_amt                  AS cga_cga_don_amt
            , precalc2.cga_individual_id                AS cga_individual_id
            , precalc2.cga_mn_don_dt                    AS cga_mn_don_dt
            , precalc2.cga_mx_don_dt                    AS cga_mx_don_dt
            , precalc2.de_de_cnt                        AS de_de_cnt
            , precalc2.de_de_cr_prod_cnt                AS de_de_cr_prod_cnt
            , precalc2.de_de_fst_prod_cd                AS de_de_fst_prod_cd
            , precalc2.de_de_hl_prod_cnt                AS de_de_hl_prod_cnt
            , precalc2.de_de_max_exp_dt                 AS de_de_max_exp_dt
            , precalc2.de_de_min_exp_dt                 AS de_de_min_exp_dt
            , precalc2.de_individual_id                 AS de_individual_id
            , precalc2.er_er_fst_dt                     AS er_er_fst_dt
            , precalc2.er_individual_id                 AS er_individual_id
            , precalc2.frc_mx_eff_dt                    AS frc_mx_eff_dt
            , precalc2.frc_prim_indiv_id                AS frc_prim_indiv_id
            , precalc2.frc2_secd_indiv_id               AS frc2_secd_indiv_id
            , precalc2.na_coa_date                      AS na_coa_date
            , precalc2.na_individual_id                 AS na_individual_id
            , precalc2.oaa_individual_id                AS oaa_individual_id
            , precalc2.oaa_mx_merge_dt                  AS oaa_mx_merge_dt
            , precalc2.ofo_individual_id                AS ofo_individual_id
            , precalc2.ofo_ofo_ce_cnt                   AS ofo_ofo_ce_cnt
            , precalc2.ofo_ofo_cr_prod_cnt              AS ofo_ofo_cr_prod_cnt
            , precalc2.ofo_ofo_fst_prod_cd              AS ofo_ofo_fst_prod_cd
            , precalc2.ofo_ofo_hl_prod_cnt              AS ofo_ofo_hl_prod_cnt
            , precalc2.ofo_ofo_ma_prod_cnt              AS ofo_ofo_ma_prod_cnt
            , precalc2.ofo_ofo_mn_ord_dt                AS ofo_ofo_mn_ord_dt
            , precalc2.ofo_ofo_mx_canc_dt               AS ofo_ofo_mx_canc_dt
            , precalc2.ofo_ofo_mx_cc_ord_dt             AS ofo_ofo_mx_cc_ord_dt
            , precalc2.ofo_ofo_mx_ord_dt                AS ofo_ofo_mx_ord_dt
            , precalc2.ofo_ofo_mx_pmt_dt                AS ofo_ofo_mx_pmt_dt
            , precalc2.ofo_ofo_pd_amt                   AS ofo_ofo_pd_amt
            , precalc2.ofo_ofo_prod_dm_ord_cnt          AS ofo_ofo_prod_dm_ord_cnt
            , precalc2.ofo_ofo_prod_dnr_ord_cnt         AS ofo_ofo_prod_dnr_ord_cnt
            , precalc2.ofo_ofo_prod_em_ord_cnt          AS ofo_ofo_prod_em_ord_cnt
            , precalc2.ofo_ofo_prod_fst_ord_dt          AS ofo_ofo_prod_fst_ord_dt
            , precalc2.ofo_ofo_prod_lst_canc_bad_dbt_dt AS ofo_ofo_prod_lst_canc_bad_dbt_dt
            , precalc2.ofo_ofo_prod_lst_ord_dt          AS ofo_ofo_prod_lst_ord_dt
            , precalc2.ofo_ofo_prod_ltd_pd_amt          AS ofo_ofo_prod_ltd_pd_amt
            , precalc2.ofo_ofo_prod_ord_cnt             AS ofo_ofo_prod_ord_cnt
            , precalc2.ofo_ofo_shm_prod_cnt             AS ofo_ofo_shm_prod_cnt
            , precalc2.ofo_ofo_xce_cnt                  AS ofo_ofo_xce_cnt
            , precalc2.oli_individual_id                AS oli_individual_id
            , precalc2.oli_oli_aps_prod_cnt             AS oli_oli_aps_prod_cnt
            , precalc2.oli_oli_carp_cnt                 AS oli_oli_carp_cnt
            , precalc2.oli_oli_carp_prod_cnt            AS oli_oli_carp_prod_cnt
            , precalc2.oli_oli_ce_cnt                   AS oli_oli_ce_cnt
            , precalc2.oli_oli_crmg_prod_cnt            AS oli_oli_crmg_prod_cnt
            , precalc2.oli_oli_cro_prod_cnt             AS oli_oli_cro_prod_cnt
            , precalc2.oli_oli_cro_prod_cnta            AS oli_oli_cro_prod_cnta
            , precalc2.oli_oli_fst_prod_cd              AS oli_oli_fst_prod_cd
            , precalc2.oli_oli_mn_crt_dt                AS oli_oli_mn_crt_dt
            , precalc2.oli_oli_mx_canc_dt               AS oli_oli_mx_canc_dt
            , precalc2.oli_oli_mx_crt_dt                AS oli_oli_mx_crt_dt
            , precalc2.oli_oli_mx_end_dt                AS oli_oli_mx_end_dt
            , precalc2.oli_oli_mx_pmt_dt                AS oli_oli_mx_pmt_dt
            , precalc2.oli_oli_prod_dnr_ord_cnt         AS oli_oli_prod_dnr_ord_cnt
            , precalc2.oli_oli_prod_lst_canc_bad_dbt_dt AS oli_oli_prod_lst_canc_bad_dbt_dt
            , precalc2.oli_oli_tot_amt                  AS oli_oli_tot_amt
            , precalc2.oli_oli_tot_amt2                 AS oli_oli_tot_amt2
            , precalc2.oli_oli_xce_cnt                  AS oli_oli_xce_cnt
            , precalc2.oli_oli_xce_d_cnt                AS oli_oli_xce_d_cnt
            , precalc2.oli_oli_xce_egknu_cnt            AS oli_oli_xce_egknu_cnt
            , precalc2.olo_individual_id                AS olo_individual_id
            , precalc2.olo_olo_mx_crt_dt                AS olo_olo_mx_crt_dt
            , precalc2.a_auth_cnt                       AS a_auth_cnt
            , precalc2.a_auth_flg_cnt                   AS a_auth_flg_cnt
            , precalc2.a_individual_id                  AS a_individual_id
            , precalc2.a_mn_fst_dt                      AS a_mn_fst_dt
            , precalc2.a_mn_newsltr_fst_dt              AS a_mn_newsltr_fst_dt
            , precalc2.a_mrp_flg                        AS a_mrp_flg
            , precalc2.a_mx_fst_dt                      AS a_mx_fst_dt
            , precalc2.a_mx_news_auth_n_eff_dt          AS a_mx_news_auth_n_eff_dt
            , precalc2.a_mx_newsltr_fst_dt              AS a_mx_newsltr_fst_dt
            , precalc2.a_news_auth_cnt                  AS a_news_auth_cnt
            , precalc2.a_news_auth_y_cnt                AS a_news_auth_y_cnt
            , precalc2.a_news_best_buy_drug_flg         AS a_news_best_buy_drug_flg
            , precalc2.a_news_car_watch_flg             AS a_news_car_watch_flg
            , precalc2.a_news_cro_whats_flg             AS a_news_cro_whats_flg
            , precalc2.a_news_green_choice_flg          AS a_news_green_choice_flg
            , precalc2.a_news_hlth_after_60_flg         AS a_news_hlth_after_60_flg
            , precalc2.a_news_hlth_alert_flg            AS a_news_hlth_alert_flg
            , precalc2.a_news_hlth_child_teen_flg       AS a_news_hlth_child_teen_flg
            , precalc2.a_news_hlth_diabetes_flg         AS a_news_hlth_diabetes_flg
            , precalc2.a_news_hlth_heart_flg            AS a_news_hlth_heart_flg
            , precalc2.a_news_hlth_women_flg            AS a_news_hlth_women_flg
            , precalc2.a_news_safety_alert_flg          AS a_news_safety_alert_flg
            , precalc3.an_an_emp_flg                    AS an_an_emp_flg
            , precalc3.an_an_forums_flag                AS an_an_forums_flag
            , precalc3.an_individual_id                 AS an_individual_id
            , precalc3.bb_bnb_frst_sales_dt             AS bb_bnb_frst_sales_dt
            , precalc3.bb_bnb_last_sales_dt             AS bb_bnb_last_sales_dt
            , precalc3.bb_bnb_tot_sale_cnt              AS bb_bnb_tot_sale_cnt
            , precalc3.bb_individual_id                 AS bb_individual_id
            , precalc3.brks_cr_prod_cnt                 AS brks_cr_prod_cnt
            , precalc3.brks_cr_prod_cnta                AS brks_cr_prod_cnta
            , precalc3.brks_hl_prod_cnt                 AS brks_hl_prod_cnt
            , precalc3.brks_individual_id               AS brks_individual_id
            , precalc3.brks_ma_prod_cnt                 AS brks_ma_prod_cnt
            , precalc3.brks_shm_prod_cnt                AS brks_shm_prod_cnt
            , precalc3.e_email_type_cd                  AS e_email_type_cd
            , precalc3.e_individual_id                  AS e_individual_id
            , precalc3.e_src_delv_ind                   AS e_src_delv_ind
            , precalc3.e_src_valid_flag                 AS e_src_valid_flag
            , precalc3.e_valid_flag                     AS e_valid_flag
            , precalc3.eua_individual_id                AS eua_individual_id
            , precalc3.eua_min_create_date              AS eua_min_create_date
            , precalc3.ibx_age_infobase                 AS ibx_age_infobase
            , precalc3.ibx_gender_infobase              AS ibx_gender_infobase
            , precalc3.ibx_individual_id                AS ibx_individual_id
            , precalc3.ol_individual_id                 AS ol_individual_id
            , precalc3.ol_ol_match_cnt                  AS ol_ol_match_cnt
            , precalc3.ol_ol_match_cnt_13_18m           AS ol_ol_match_cnt_13_18m
            , precalc3.ol_ol_match_cnt_6m               AS ol_ol_match_cnt_6m
            , precalc3.ol_ol_match_cnt_7_12m            AS ol_ol_match_cnt_7_12m
            , precalc3.ol_ol_match_recent_dt            AS ol_ol_match_recent_dt
            , precalc3.we_email_address                 AS we_email_address
            , precalc3.we_individual_id                 AS we_individual_id
            , precalc3.bnb_bnb_frst_visitor_dt          AS bnb_bnb_frst_visitor_dt
            , precalc3.bnb_bnb_last_visitor_dt          AS bnb_bnb_last_visitor_dt
            , precalc3.bnb_bnb_frst_prospect_dt         AS bnb_bnb_frst_prospect_dt
            , precalc3.bnb_bnb_last_prospect_dt         AS bnb_bnb_last_prospect_dt
            , precalc3.bnb_bnb_frst_sales_dt            AS bnb_bnb_frst_sales_dt
            , precalc3.bnb_bnb_last_sales_dt            AS bnb_bnb_last_sales_dt
            , precalc3.bnb_bnb_tot_prospect_cnt         AS bnb_bnb_tot_prospect_cnt
            , precalc3.bnb_bnb_tot_sale_cnt             AS bnb_bnb_tot_sale_cnt
            , precalc3.bnb_bnb_best_status              AS bnb_bnb_best_status
            , precalc3.bnb_bnb_best_status_dt           AS bnb_bnb_best_status_dt
            , precalc3.bnb_bnb_most_recent_status       AS bnb_bnb_most_recent_status
            , precalc3.bnb_bnb_most_recent_dt           AS bnb_bnb_most_recent_dt
            , precalc3.bnb_bnb_nbr_new_sales            AS bnb_bnb_nbr_new_sales
            , precalc3.bnb_bnb_nbr_used_sales           AS bnb_bnb_nbr_used_sales
            , precalc3.bnb_bnb_most_recent_trans_type   AS bnb_bnb_most_recent_trans_type

            , precalc4.coop_individual_id               AS coop_individual_id
            , precalc4.erm_individual_id                AS erm_individual_id
            , precalc4.erm_mx_cds_pub_dt                AS erm_mx_cds_pub_dt
            , precalc4.ia_country_id                    AS ia_country_id
            , precalc4.ia_individual_id                 AS ia_individual_id
            , precalc4.ofoxog_dc_corp_first_ind         AS ofoxog_dc_corp_first_ind
            , precalc4.ofoxog_dc_corp_first_ind2        AS ofoxog_dc_corp_first_ind2
            , precalc4.ofoxog_dc_corp_fst_crt_ind       AS ofoxog_dc_corp_fst_crt_ind
            , precalc4.ofoxog_dc_offline_fst_crt_ind    AS ofoxog_dc_offline_fst_crt_ind
            , precalc4.ofoxog_individual_id             AS ofoxog_individual_id
            , precalc4.olixog1_dc_last_aps_crt_ind      AS olixog1_dc_last_aps_crt_ind
            , precalc4.olixog1_individual_id            AS olixog1_individual_id
            , precalc4.olixog2_dc_online_fst_aps_ind    AS olixog2_dc_online_fst_aps_ind
            , precalc4.olixog2_dc_online_fst_crt_ind    AS olixog2_dc_online_fst_crt_ind
            , precalc4.olixog2_individual_id            AS olixog2_individual_id
            , precalc4.page_hit_time                    AS page_hit_time
            , precalc4.page_individual_id               AS page_individual_id
            , precalc4.page_min_hit_time                AS page_min_hit_time
            , precalc4.surv_answer_date                 AS surv_answer_date
            , precalc4.surv_individual_id               AS surv_individual_id
            , precalc4.surv_min_answer_date             AS surv_min_answer_date

            , miscagg1.bi_individual_id                 AS bi_individual_id
            , miscagg1.bi_bi_ret_canc_dt                AS bi_bi_ret_canc_dt
            , miscagg1.bi_bi_prod_lst_canc_bad_dbt_dt   AS bi_bi_prod_lst_canc_bad_dbt_dt
            , miscagg1.bi_bi_cnt                        AS bi_bi_cnt
            , miscagg1.bs_individual_id                 AS bs_individual_id
            , miscagg1.bs_bk_flg                        AS bs_bk_flg
            , miscagg1.bs_bk_actv_flg                   AS bs_bk_actv_flg
            , miscagg1.bs_bs_prod_ltd_pd_amt            AS bs_bs_prod_ltd_pd_amt
            , miscagg1.fr_individual_id                 AS fr_individual_id
            , miscagg1.fr_fr_lst_don_dt                 AS fr_fr_lst_don_dt
            , miscagg1.fr_fr_max_don_amt                AS fr_fr_max_don_amt
            , miscagg1.ar_individual_id                 AS ar_individual_id
            , miscagg1.ar_mn_last_resp_dt               AS ar_mn_last_resp_dt
            , miscagg1.ar_mx_last_resp_dt               AS ar_mx_last_resp_dt
            , miscagg1.ar_ar_entry_cnt                  AS ar_ar_entry_cnt
            , miscagg1.ar_cnt_resp_dt_gt_12             AS ar_cnt_resp_dt_gt_12
            , miscagg1.fp_individual_id                 AS fp_individual_id
            , miscagg1.fp_max_pldg_dt                   AS fp_max_pldg_dt
            , miscagg1.flp_individual_id                AS flp_individual_id
            , miscagg1.flp_max_pldg_dt                  AS flp_max_pldg_dt
            , miscagg1.abqd_individual_id               AS abqd_individual_id
            , miscagg1.abqd_max_abq_dt                  AS abqd_max_abq_dt
            , miscagg1.abqd_min_abq_dt                  AS abqd_min_abq_dt
            , miscagg1.abqr_individual_id               AS abqr_individual_id
            , miscagg1.abqr_min_abq_mbr_dt              AS abqr_min_abq_mbr_dt
            , miscagg1.abqr_max_abq_mbr_dt              AS abqr_max_abq_mbr_dt

            , miscagg2.app_individual_id                AS app_individual_id
            , miscagg2.app_max_visit_strt_time          AS app_max_visit_strt_time
            , miscagg2.app_max_days_since_fst_use       AS app_max_days_since_fst_use
            , miscagg2.key_individual_id                AS key_individual_id
            , miscagg2.key_email_fav_key                AS key_email_fav_key
            , miscagg2.advint_individual_id             AS advint_individual_id
            , miscagg2.advint_int_cnt                   AS advint_int_cnt
            , miscagg2.advsur_individual_id             AS advsur_individual_id
            , miscagg2.advsur_sur_cnt                   AS advsur_sur_cnt
            , miscagg2.advdon_individual_id             AS advdon_individual_id
            , miscagg2.advdon_don_cnt                   AS advdon_don_cnt
            , miscagg2.advalrt_individual_id            AS advalrt_individual_id
            , miscagg2.advalrt_alrt_cnt                 AS advalrt_alrt_cnt
            , miscagg2.eread_individual_id              AS eread_individual_id
            , miscagg2.eread_report_start_date          AS eread_report_start_date
            , miscagg2.eread_min_rpt_start_date         AS eread_min_rpt_start_date
            , miscagg2.eread_ereader_cnt                AS eread_ereader_cnt

            , proem.individual_id                       AS proem_individual_id
            , proem.ind_prospect_eml_mtch_ind           AS proem_ind_prospect_eml_mtch_ind
            , proem.prospect_email_match_dt             AS proem_prospect_email_match_dt
            , proem.prospect_email_match_cnt            AS proem_prospect_email_match_cnt

            , page2.page_view_within_year               AS page2_page_view_within_year

            , netflag.news_ind_actv_flg                 AS neflag_news_ind_actv_flg

        FROM agg.indiv_ind_xref_temp xr
            LEFT JOIN agg.indiv_precalc_1_temp precalc1
                ON xr.individual_id = precalc1.individual_id
            LEFT JOIN agg.indiv_precalc_2_temp precalc2
                ON xr.individual_id = precalc2.individual_id
            LEFT JOIN agg.indiv_precalc_3_temp precalc3
                ON xr.individual_id = precalc3.individual_id
            LEFT JOIN agg.indiv_precalc_4_temp precalc4
                ON xr.individual_id = precalc4.individual_id
            --MISCELLANEOUS
            LEFT JOIN agg.indiv_misc_agg_temp miscagg1
                ON xr.individual_id = miscagg1.individual_id
            LEFT JOIN agg.indiv_misc_agg_2_temp miscagg2
                ON xr.individual_id = miscagg2.individual_id
            LEFT JOIN agg.indiv_prospect_email_temp proem
                on xr.individual_id = proem.individual_id

            LEFT JOIN agg.indiv_webpage_2_temp page2
                ON xr.individual_id = page2.individual_id
            LEFT JOIN agg.indiv_email_fl_temp netflag
                ON xr.individual_id = netflag.individual_id
;

analyse agg.indiv_precalc_fin_temp;


DROP TABLE IF EXISTS    agg.indiv_aggregation_done_temp;
CREATE TABLE            agg.indiv_aggregation_done_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT

            fin.individual_id
            , fin.hh_id


            /***************************************************************************/
            --PART 1
            /***************************************************************************/

            , fin.ix_acx_gender_cd AS gender
            , fin.ix_customer_typ AS customer_typ
            , (substring(GREATEST(NVL(TO_CHAR(fin.fs_fr_lst_don_dt, 'YYYYMMDD'), '00010101') || '1' || cast((fin.fs_fr_lst_don_amt) AS TEXT), NVL(TO_CHAR(fin.adv_mt_adv_lst_don_dt, 'YYYYMMDD'), '00010101') || '2' || cast((fin.adv_mt_adv_lst_don_amt) AS TEXT), NVL(to_char(TO_DATE(DECODE(fin.abq_abq_lst_don_dt, NULL, NULL, fin.abq_abq_lst_don_dt || '0601'), 'YYYYMMDD'), 'YYYYMMDD'), '00010101') || '3' || cast((fin.abq_abq_lst_don_amt) AS TEXT), '00010101'), 10)) AS lst_don_amt
            , NVL(fin.fs_fr_ltd_don_amt, 0) + NVL(fin.abq_abq_ltd_don_amt, 0) + NVL(fin.adv_mt_adv_ltd_don_amt, 0) AS ltd_don_amt
            , GREATEST(NVL(fin.fs_fr_max_don_amt, 0), NVL(fin.adv_mt_adv_max_don_amt, 0), NVL(fin.abq_abq_max_don_amt, 0)) AS max_don_amt
            , NULLIF(LEAST(NVL(fin.fs_fr_fst_don_dt, TO_DATE('99990601', 'YYYYMMDD')), NVL(fin.adv_mt_adv_fst_don_dt, TO_DATE('99990601', 'YYYYMMDD')), NVL(TO_DATE(DECODE(fin.abq_abq_fst_don_dt, NULL, NULL, fin.abq_abq_fst_don_dt || '0601'), 'YYYYMMDD'), TO_DATE('99990601', 'YYYYMMDD'))), TO_DATE('99990601', 'YYYYMMDD')) AS fst_don_dt
            , NULLIF(GREATEST(NVL(fin.fs_fr_lst_don_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.adv_mt_adv_lst_don_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(TO_DATE(DECODE(fin.abq_abq_lst_don_dt, NULL, NULL, fin.abq_abq_lst_don_dt || '0601'), 'YYYYMMDD'), TO_DATE('00010101', 'YYYYMMDD'))), TO_DATE('00010101', 'YYYYMMDD')) AS lst_don_dt
            , NVL(fin.fs_fr_ltd_don_cnt, 0) + NVL(fin.abq_abq_ltd_don_cnt, 0) + NVL(fin.adv_mt_adv_ltd_don_cnt, 0) AS ltd_don_cnt
            , NULLIF(LEAST(NVL(fin.fs_fr_fst_don_dt, TO_DATE('99990601', 'YYYYMMDD')), NVL(fin.adv_mt_first_action_dt, TO_DATE('99990601', 'YYYYMMDD')), NVL(fin.a_mn_newsltr_fst_dt, TO_DATE('99990601', 'YYYYMMDD')), NVL(fin.cga_mn_don_dt, TO_DATE('99990601', 'YYYYMMDD')), NVL(fin.bb_bnb_frst_sales_dt, TO_DATE('99990601', 'YYYYMMDD')), NVL(fin.abqr_min_abq_mbr_dt, TO_DATE('99990601', 'YYYYMMDD')), NVL(fin.ix_dc_fund_first_dt, to_date('99990601', 'YYYYMMDD')), NVL(fin.abqd_min_abq_dt, to_date('99990601', 'YYYYMMDD')), NVL(fin.surv_min_answer_date, to_date('99990601', 'YYYYMMDD')), NVL(fin.page_min_hit_time, to_date('99990601', 'YYYYMMDD')), TO_DATE(NVL(fin.abq_abq_fst_rsp_dt, '9999') || '0601', 'YYYYMMDD'), NVL(fin.a_mn_fst_dt, TO_DATE('99990601', 'YYYYMMDD'))), TO_DATE('99990601', 'YYYYMMDD')) AS non_prod_fst_dt
            , substring(LEAST(NVL(TO_CHAR(fin.fs_fr_fst_don_dt, 'YYYYMMDD'), '99990601') || '1' || 'FR', NVL(TO_CHAR(fin.ix_dc_fund_first_dt, 'YYYYMMDD'), '99990601') || '1' || 'FR', NVL(to_char(fin.abqd_min_abq_dt, 'YYYYMMDD'), '99990601') || '1' || 'FR', NVL(TO_CHAR(fin.adv_mt_first_action_dt, 'YYYYMMDD'), '99990601') || '2' || 'ADV', NVL(TO_CHAR(fin.a_mn_fst_dt, 'YYYYMMDD'), '99990601') || '3' || 'SUR', TO_CHAR(TO_DATE(NVL(fin.abq_abq_fst_rsp_dt, '9999') || '0601', 'YYYYMMDD'), 'YYYYMMDD') || '3' || 'SUR', NVL(TO_CHAR(fin.surv_min_answer_date, 'YYYYMMDD'), '99990601') || '3' || 'SUR', NVL(TO_CHAR(fin.a_mn_newsltr_fst_dt, 'YYYYMMDD'), '99990601') || '4' || 'NL', NVL(TO_CHAR(fin.cga_mn_don_dt, 'YYYYMMDD'), '99990601') || '5' || 'CGA', NVL(TO_CHAR(fin.bb_bnb_frst_sales_dt, 'YYYYMMDD'), '99990601') || '6' || 'BNB', NVL(TO_CHAR(fin.page_min_hit_time, 'YYYYMMDD'), '99990601') || '7' || 'UR', '99990101'), 10) AS fst_non_prod_cd


            /***************************************************************************/
            --PART 2
            /***************************************************************************/


            , NVL(fin.fs_fr_ltd_don_cnt, 0) + NVL(fin.abq_abq_ltd_don_cnt, 0)
              + NVL(fin.adv_mt_adv_ltd_don_cnt, 0) + decode(nvl(fin.a_news_auth_cnt, 0), 0, 0, 1) + decode(nvl(fin.adv_mt_individual_id, 0), 0, 0, 1)
              + NVL(fin.bb_bnb_tot_sale_cnt, 0) + decode(nvl(fin.page_individual_id, 0), 0, 0, 1) + NVL(fin.cga_cga_cnt, 0)
              + decode(nvl(fin.surv_individual_id, 0), 0, decode(nvl(fin.a_auth_cnt, 0), 0, decode(nvl(fin.advsur_individual_id, 0), 0, decode(nvl(fin.abq_abq_ltd_rsp_cnt, 0), 0, 0, 1), 1), 1), 1) AS non_prod_cnt
            , fin.a_mrp_flg AS mrp_flg
            , NVL2(fin.frc2_secd_indiv_id, 'Y', 'N') AS fr_combined_flg
            , NULLIF(GREATEST(NVL(fin.er_er_fst_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.frc_mx_eff_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.oaa_mx_merge_dt, TO_DATE('00010101', 'YYYYMMDD'))), TO_DATE('00010101', 'YYYYMMDD')) AS ind_combined_dt
            , NVL(fin.ofo_ofo_pd_amt, 0) + NVL(fin.oli_oli_tot_amt, 0) + NVL(fin.oli_oli_tot_amt2, 0)
              + NVL(fin.fs_fr_ltd_don_amt, 0) + NVL(fin.cga_cga_don_amt, 0) + NVL(fin.bo_bo_pmt_amt, 0) + NVL(fin.adv_mt_adv_ltd_don_amt, 0) AS ind_ltd_amt
            , CASE WHEN GREATEST(NVL(fin.olo_olo_mx_crt_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.ofo_ofo_mx_cc_ord_dt, TO_DATE('00010101', 'YYYYMMDD'))) >
                        GREATEST(NVL(fin.oli_oli_mx_pmt_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.ofo_ofo_mx_pmt_dt, TO_DATE('00010101', 'YYYYMMDD')))
            THEN 'Y'
              ELSE 'N' END AS comp_flg
            , to_date(substring(LEAST(NVL(to_char(fin.bo_bo_min_ord_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.bo_bo_min_ord_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.ix_dc_cdb_last_ord_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.ix_dc_cdb_last_ord_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.ix_dc_offline_first_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.ix_dc_offline_first_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.ofo_ofo_mn_ord_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.ofo_ofo_mn_ord_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.ix_dc_online_first_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.ix_dc_online_first_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.oli_oli_mn_crt_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.oli_oli_mn_crt_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.de_de_min_exp_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.de_de_min_exp_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.ix_dc_fund_first_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.ix_dc_fund_first_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.ix_dc_last_aps_ord_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.ix_dc_last_aps_ord_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.fs_fr_fst_don_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.fs_fr_fst_don_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.cga_mn_don_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.cga_mn_don_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.a_mn_fst_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.a_mn_fst_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.ix_dc_corp_first_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.ix_dc_corp_first_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.adv_mt_first_action_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.adv_mt_first_action_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.a_mn_newsltr_fst_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.a_mn_newsltr_fst_dt, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.eread_min_rpt_start_date, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.eread_min_rpt_start_date, 'YYYYMMDDHH24MISS'), NVL(to_char(fin.bb_bnb_frst_sales_dt, 'YYYYMMDDHH24MISS'), '99990601000000') || to_char(fin.bb_bnb_frst_sales_dt, 'YYYYMMDDHH24MISS')), 15), 'YYYYMMDDHH24MISS') AS ind_fst_rel_dt
            , substring(LEAST(NVL(fin.ofo_ofo_fst_prod_cd, '99991231' || '99'), NVL(fin.de_de_fst_prod_cd, '99991231' || '99'), NVL(fin.oli_oli_fst_prod_cd, '99991231' || '99'), NVL(to_char(fin.ix_dc_corp_first_dt, 'YYYYMMDD'), '99991231') || 'C0' || 'CNS', NVL(to_char(fin.ix_dc_offline_first_dt, 'YYYYMMDD'), '99991231') || '60' || 'CNS', NVL(to_char(fin.ix_dc_fund_first_dt, 'YYYYMMDD'), '99991231') || 'C0' || 'CNS', NVL(to_char(fin.ix_dc_online_first_dt, 'YYYYMMDD'), '99991231') || '80' || 'CRO', NVL(to_char(fin.ix_dc_last_aps_ord_dt, 'YYYYMMDD'), '99991231') || '70' || 'NCPR', NVL(fin.bo_bo_fst_prod_cd, '99991231' || '99'), NVL(to_char(fin.ix_dc_cdb_last_ord_dt, 'YYYYMMDD'), '99991231') || 'B0' || 'CDR', NVL(to_char(fin.fs_fr_fst_don_dt, 'YYYYMMDD'), '99991231') || substring(least(NVL(to_char(fin.ix_dc_corp_first_dt, 'YYYYMMDD'), '99991231') || 'D5' || 'CNS', NVL(to_char(fin.ix_dc_offline_first_dt, 'YYYYMMDD'), '99991231') || 'D1' || 'CNS', NVL(to_char(fin.ix_dc_fund_first_dt, 'YYYYMMDD'), '99991231') || 'D5' || 'CNS', NVL(to_char(fin.ix_dc_online_first_dt, 'YYYYMMDD'), '99991231') || 'D3' || 'CRO', NVL(to_char(fin.ix_dc_last_aps_ord_dt, 'YYYYMMDD'), '99991231') || 'D2' || 'NCPR', NVL(to_char(fin.ix_dc_cdb_last_ord_dt, 'YYYYMMDD'), '99991231') || 'D4' || 'CDR', '99991230' || 'G0' || 'FR'), 9), NVL(to_char(fin.adv_mt_first_action_dt, 'YYYYMMDD'), '99991231') || 'E0' || 'ADV', NVL(to_char(fin.a_mn_newsltr_fst_dt, 'YYYYMMDD'), '99991231') || 'F0' || 'NL', NVL(to_char(fin.cga_mn_don_dt, 'YYYYMMDD'), '99991231') || 'H0' || 'CGA', NVL(to_char(fin.a_mn_fst_dt, 'YYYYMMDD'), '99991231') || 'I0' || 'SUR', NVL(to_char(fin.eread_report_start_date, 'YYYYMMDD'), '99991231') || 'J0' || 'IPAD', NVL(to_char(fin.bb_bnb_last_sales_dt, 'YYYYMMDD'), '99991231') || 'K0' || 'BNB', '99991230' || '00'), 11) AS ind_fst_rel_cd
            , NULLIF(GREATEST(NVL(fin.bo_bo_max_ord_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.bo_bo_pmt_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.bi_bi_ret_canc_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.ix_dc_cdb_last_ord_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.ix_dc_offline_first_dt, to_date('00010601', 'YYYYMMDD')), NVL(fin.ofo_ofo_mx_ord_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.ofo_ofo_mx_pmt_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.ofo_ofo_mx_canc_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.ix_dc_online_first_dt, to_date('00010601', 'YYYYMMDD')), NVL(fin.oli_oli_mx_crt_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.oli_oli_mx_canc_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.oli_oli_mx_pmt_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.ix_dc_last_aps_ord_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.ix_dc_corp_first_dt, to_date('00010601', 'YYYYMMDD')), NVL(fin.ix_dc_fund_first_dt, to_date('00010601', 'YYYYMMDD')), NVL(fin.fr_fr_lst_don_dt, TO_DATE('00010601', 'YYYYMMDD')), TO_DATE(NVL(fin.abq_abq_lst_rsp_dt, '0001') || '0601', 'YYYYMMDD'), NVL(fin.na_coa_date, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.cga_mx_don_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.a_mx_fst_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.adv_mt_last_action_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.eread_report_start_date, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.abqd_max_abq_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.surv_answer_date, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.bb_bnb_last_sales_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.page_hit_time, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.a_mx_newsltr_fst_dt, TO_DATE('00010601', 'YYYYMMDD'))), TO_DATE('00010601', 'YYYYMMDD')) AS ind_lst_act_dt


            /***************************************************************************/
            --PART 3
            /***************************************************************************/

            /*SD 2017/10/20 12:30: ind_actv_cnt change*/
            , DECODE(fin.ofs_cr_actv_flg, 'Y', 1, 0) + DECODE(fin.ofs_cr_sub_dnr_flg, 'Y', 1, 0) + DECODE(fin.ofs_cr_non_sub_dnr_flg, 'Y', 1, 0)
              + DECODE(fin.ofs_hl_actv_flg, 'Y', 1, 0) + DECODE(fin.ofs_hl_sub_dnr_flg, 'Y', 1, 0) + DECODE(fin.ofs_hl_non_sub_dnr_flg, 'Y', 1, 0)
              + DECODE(fin.ols_cro_actv_flg, 'Y', 1, 0) + DECODE(fin.ols_cro_sub_dnr_flg, 'Y', 1, 0) + DECODE(fin.ols_cro_non_sub_dnr_flg, 'Y', 1, 0)
              + CASE WHEN fin.fs_fr_lst_don_dt > ADD_MONTHS(SYSDATE, -12)
            THEN 1
                ELSE 0 END
              + DECODE(fin.bs_bk_actv_flg, 'Y', 1, 0)
              + CASE WHEN fin.surv_answer_date >= ADD_MONTHS(SYSDATE, -12)
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.bb_bnb_last_sales_dt > ADD_MONTHS(SYSDATE, -12)
            THEN 1
                ELSE 0 END
              + NVL(fin.page2_page_view_within_year, 0)
              + CASE WHEN nvl(fin.neflag_NEWS_IND_ACTV_FLG, 'N') = 'Y'
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.adv_mt_adv_active_flg = 'Y'
            THEN 1
                ELSE 0 END AS ind_actv_cnt
            /*SD 2017/10/20 12:30: ind_relship_cnt change*/
            , DECODE(NVL(fin.ofs_cr_flg, 'N'), 'N', 0, DECODE(fin.ofs_cr_fst_dnr_dt, NULL, 1, 2))
              + DECODE(NVL(fin.ofs_hl_flg, 'N'), 'N', 0, DECODE(fin.ofs_hl_fst_dnr_dt, NULL, 1, 2))
              + DECODE(NVL(fin.ofs_ma_flg, 'N'), 'N', 0, DECODE(fin.ofs_ma_fst_dnr_dt, NULL, 1, 2))
              + DECODE(NVL(fin.ofs_shm_flg, 'N'), 'N', 0, DECODE(fin.ofs_shm_fst_dnr_dt, NULL, 1, 2))
              + CASE WHEN fin.ofs_individual_id IS NULL AND (fin.ix_dc_offline_first_dt IS NOT NULL OR (fin.ix_dc_corp_first_dt IS NOT NULL AND NVL(fin.ofoxog_dc_corp_first_ind, 0) = 0))
            THEN 1
                ELSE 0 END
              + DECODE(NVL(fin.ols_cro_flg, 'N'), 'N', 0, DECODE(fin.ols_cro_fst_dnr_dt, NULL, 1, 2))
              + DECODE(fin.ols_crmg_flg, 'Y', 1, 0) + DECODE(fin.ols_ncbk_flg, 'Y', 1, 0) + DECODE(fin.ols_ucbk_flg, 'Y', 1, 0)
              + CASE WHEN fin.oli_oli_carp_cnt > 0
            THEN 1
                ELSE 0 END
              /*+ DECODE(fin.ols_nc_rpts_flg, 'Y', 1, 0)*/ + DECODE(fin.ols_uc_rpts_flg, 'Y', 1, 0)
              + CASE WHEN /*nvl(fin.ols_nc_rpts_flg, 'N') = 'N' AND*/ nvl(fin.ols_uc_rpts_flg, 'N') = 'N' AND fin.ix_dc_last_aps_ord_dt IS NOT NULL
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.ols_individual_id IS NULL AND fin.ix_dc_online_first_dt IS NOT NULL AND NVL(fin.olixog2_dc_online_fst_aps_ind, 0) = 0
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.fs_individual_id IS NOT NULL OR fin.abqd_individual_id IS NOT NULL
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.fs_individual_id IS NULL AND fin.abqd_individual_id IS NULL AND fin.ix_dc_fund_first_dt IS NOT NULL
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.abq_abq_ltd_rsp_cnt > 0 OR fin.surv_individual_id IS NOT NULL
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.bb_bnb_tot_sale_cnt > 0
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.a_news_auth_cnt > 0
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.page_individual_id IS NOT NULL
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.eread_individual_id IS NOT NULL
            THEN 1
                ELSE 0 END
              + DECODE(fin.bs_bk_flg, 'Y', 1, 0)
              + CASE WHEN fin.bs_individual_id IS NULL AND fin.ix_dc_cdb_last_ord_dt IS NOT NULL
            THEN 1
                ELSE 0 END
              + CASE WHEN NVL2(fin.adv_mt_individual_id, 'Y', 'N') = 'Y'
            THEN 1
                ELSE 0 END AS ind_relship_cnt

            , substring(LEAST(NVL(fin.ofo_ofo_fst_prod_cd, '99991231' || '99'), NVL(fin.de_de_fst_prod_cd, '99991231' || '99'), NVL(fin.oli_oli_fst_prod_cd, '99991231' || '99'), NVL(TO_CHAR(fin.ix_dc_corp_first_dt, 'YYYYMMDD'), '99991231') || 'C0' || 'CNS', NVL(TO_CHAR(fin.ix_dc_fund_first_dt, 'YYYYMMDD'), '99991231') || 'C0' || 'CNS', NVL(TO_CHAR(fin.ix_dc_offline_first_dt, 'YYYYMMDD'), '99991231') || '60' || 'CNS', NVL(TO_CHAR(fin.ix_dc_online_first_dt, 'YYYYMMDD'), '99991231') || '80' || 'CRO', NVL(TO_CHAR(fin.ix_dc_last_aps_ord_dt, 'YYYYMMDD'), '99991231') || '70' ||
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       'NCPR', NVL(fin.bo_bo_fst_prod_cd, '99991231' || '99'), NVL(TO_CHAR(fin.ix_dc_cdb_last_ord_dt, 'YYYYMMDD'), '99991231') || 'B0' || 'CDR', CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 WHEN fin.ix_dc_corp_first_dt IS NULL AND fin.ix_dc_offline_first_dt IS NULL AND fin.ix_dc_fund_first_dt IS NULL AND fin.ix_dc_online_first_dt IS NULL
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      AND fin.ix_dc_cdb_last_ord_dt IS NULL
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     THEN
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         '99991231' || 'D0'
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 ELSE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     NVL(TO_CHAR(fin.fs_fr_fst_don_dt, 'YYYYMMDD'), '99991231') || substring(least(NVL(TO_CHAR(fin.ix_dc_corp_first_dt, 'YYYYMMDD'), '99991231') ||
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   'C5' || 'CNS', NVL(TO_CHAR(fin.ix_dc_offline_first_dt, 'YYYYMMDD'), '99991231') || 'C1' || 'CNS', NVL(TO_CHAR(fin.ix_dc_fund_first_dt, 'YYYYMMDD'), '99991231') || 'C5' || 'CNS', NVL(TO_CHAR(fin.ix_dc_online_first_dt, 'YYYYMMDD'), '99991231') || 'C3' || 'CRO', NVL(TO_CHAR(fin.ix_dc_last_aps_ord_dt, 'YYYYMMDD'), '99991231') || 'C2' || 'NCPR', NVL(TO_CHAR(fin.ix_dc_cdb_last_ord_dt, 'YYYYMMDD'), '99991231') || 'C4' ||
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          'CDR', '99991230' || 'D0'), 9)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 END, NVL(TO_CHAR(fin.eread_min_rpt_start_date, 'YYYYMMDD'), '99991231') || 'E0' || 'IPAD', '99991231' || '00'), 11) AS fst_prod_cd
            , CASE WHEN NVL(fin.ofo_ofo_ce_cnt, 0) + NVL(fin.oli_oli_ce_cnt, 0) + NVL(fin.bo_bo_ce_cnt, 0) > 0
                        AND NVL(fin.ofo_ofo_xce_cnt, 0) + NVL(fin.oli_oli_xce_cnt, 0) + NVL(fin.bo_bo_xce_cnt, 0) = 0
            THEN 'Y'
              ELSE 'N' END AS ind_rec_prod_only_flg
            , NVL2(fin.an_an_emp_flg, 'Y', 'N') AS emp_flg
            , NVL2(fin.cga_individual_id, 'Y', 'N') AS cga_flg
            , fin.a_news_car_watch_flg AS news_car_watch_flg
            , fin.a_news_best_buy_drug_flg AS news_best_buy_drug_flg
            , fin.a_news_safety_alert_flg AS news_safety_alert_flg
            , fin.a_news_hlth_alert_flg AS news_hlth_alert_flg
            , fin.a_news_cro_whats_flg AS news_cro_whats_flg
            , fin.a_news_green_choice_flg AS news_green_choice_flg


            /***************************************************************************/
            --PART 4
            /***************************************************************************/


            , CASE WHEN (fin.bo_bo_recip_cd_d_cnt > 0
                         OR fin.ofs_cr_fst_dnr_dt IS NOT NULL OR fin.ofs_hl_fst_dnr_dt IS NOT NULL
                         OR fin.ofs_ma_fst_dnr_dt IS NOT NULL OR fin.ols_cro_fst_dnr_dt IS NOT NULL
                         OR fin.ofs_shm_fst_dnr_dt IS NOT NULL)
                        AND NVL(fin.ofs_cr_non_dnr_flg, 'N') != 'Y'
                        AND NVL(fin.ofs_hl_non_dnr_flg, 'N') != 'Y'
                        AND NVL(fin.ofs_ma_non_dnr_flg, 'N') != 'Y'
                        AND NVL(fin.ofs_shm_non_dnr_flg, 'N') != 'Y'
                        AND NVL(fin.ols_cro_non_dnr_flg, 'N') != 'Y'
                        AND NVL(fin.ols_crmg_flg, 'N') != 'Y'
                        AND NVL(fin.ols_ncbk_flg, 'N') != 'Y'
                        AND NVL(fin.ols_ucbk_flg, 'N') != 'Y'
                        /*AND NVL(fin.ols_nc_rpts_flg, 'N') != 'Y'*/
                        AND NVL(fin.ols_uc_rpts_flg, 'N') != 'Y'
                        AND NVL2(fin.fs_individual_id, 'Y', 'N') != 'Y'
                        AND NVL2(fin.abqd_individual_id, 'Y', 'N') != 'Y'
                        AND NVL2(fin.abqr_individual_id, 'Y', 'N') != 'Y'
                        AND NVL2(fin.ar_individual_id, 'Y', 'N') != 'Y'
                        AND NVL2(fin.adv_individual_id, 'Y', 'N') != 'Y'
                        AND fin.ix_dc_last_aps_ord_dt IS NULL
                        AND NVL(fin.a_auth_cnt, 0) = 0
                        AND NVL(fin.bo_bo_recip_cd_not_d_cnt, 0) = 0
            THEN 'Y'
              ELSE 'N' END AS ind_dnr_only_flg
            , fin.ix_dc_cdb_last_ord_dt AS dc_cdb_last_ord_dt
            , NVL(fin.ofo_ofo_prod_ltd_pd_amt, 0) + NVL(fin.oli_oli_tot_amt, 0) + NVL(fin.oli_oli_tot_amt2, 0) + NVL(fin.bs_bs_prod_ltd_pd_amt, 0) AS prod_ltd_pd_amt
            , NULLIF(LEAST(NVL(fin.ix_dc_cdb_last_ord_dt, TO_DATE('99991231', 'YYYYMMDD')), NVL(fin.bo_bo_prod_fst_ord_dt, TO_DATE('99991231', 'YYYYMMDD')), NVL(fin.ix_dc_offline_first_dt, TO_DATE('99991231', 'YYYYMMDD')), NVL(fin.ofo_ofo_prod_fst_ord_dt, TO_DATE('99991231', 'YYYYMMDD')), NVL(fin.ix_dc_last_aps_ord_dt, TO_DATE('99991231', 'YYYYMMDD')), NVL(fin.ix_dc_online_first_dt, to_date('99991231', 'YYYYMMDD')), NVL(fin.oli_oli_mn_crt_dt, TO_DATE('99991231', 'YYYYMMDD')), NVL(fin.de_de_min_exp_dt, TO_DATE('99991231', 'YYYYMMDD')), NVL(fin.ix_dc_corp_first_dt, TO_DATE('99991231', 'YYYYMMDD')), NVL(fin.ix_dc_fund_first_dt, TO_DATE('99991231', 'YYYYMMDD')), NVL(fin.eread_min_rpt_start_date, TO_DATE('99991231', 'YYYYMMDD')), CASE WHEN fin.ix_dc_corp_first_dt IS NOT NULL OR
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         fin.ix_dc_offline_first_dt IS NOT NULL OR
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         fin.ix_dc_online_first_dt IS NOT NULL OR
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         fin.ix_dc_fund_first_dt IS NOT NULL OR
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         fin.ix_dc_cdb_last_ord_dt IS NOT NULL OR
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         fin.ix_dc_last_aps_ord_dt IS NOT NULL
            THEN NVL(fin.fs_fr_fst_don_dt, TO_DATE('99991231', 'YYYYMMDD'))
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               ELSE TO_DATE('99991231', 'YYYYMMDD') END), TO_DATE('99991231', 'YYYYMMDD')) AS prod_fst_ord_dt

            /***************************************************************************/
            --PART 5
            /***************************************************************************/


            , NULLIF(GREATEST(NVL(fin.ofo_ofo_prod_lst_canc_bad_dbt_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.oli_oli_prod_lst_canc_bad_dbt_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.bi_bi_prod_lst_canc_bad_dbt_dt, TO_DATE('00010101', 'YYYYMMDD'))), TO_DATE('00010101', 'YYYYMMDD')) AS prod_lst_canc_bad_dbt_dt
            , NULLIF(GREATEST(NVL(fin.fr_fr_lst_don_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.adv_mt_last_action_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.cga_mx_don_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.abqr_max_abq_mbr_dt, TO_DATE('00010601', 'YYYYMMDD')), NVL(fin.ix_dc_fund_first_dt, to_date('00010601', 'YYYYMMDD')), NVL(fin.a_mx_newsltr_fst_dt, to_date('00010601', 'YYYYMMDD')), NVL(fin.page_hit_time, to_date('00010601', 'YYYYMMDD')), NVL(fin.abqd_max_abq_dt, to_date('00010601', 'YYYYMMDD')), TO_DATE(NVL(fin.abq_abq_lst_rsp_dt, '0001') || '0601', 'YYYYMMDD'), NVL(fin.surv_answer_date, to_date('00010601', 'YYYYMMDD')), NVL(fin.bb_bnb_last_sales_dt, to_date('00010601', 'YYYYMMDD')), NVL(fin.a_mx_fst_dt, TO_DATE('00010601', 'YYYYMMDD'))), TO_DATE('00010601', 'YYYYMMDD')) AS non_prod_lst_act_dt
            , NULLIF(GREATEST(NVL(fin.ix_dc_cdb_last_ord_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.bo_bo_prod_lst_ord_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.ix_dc_offline_first_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.ofo_ofo_prod_lst_ord_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.ix_dc_last_aps_ord_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.ix_dc_online_first_dt, to_date('00010101', 'YYYYMMDD')), NVL(fin.oli_oli_mx_crt_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.de_de_max_exp_dt, TO_DATE('00010101', 'YYYYMMDD')), NVL(fin.ix_dc_corp_first_dt, to_date('00010101', 'YYYYMMDD')), NVL(fin.eread_report_start_date, to_date('00010101', 'YYYYMMDD'))), TO_DATE('00010101', 'YYYYMMDD')) AS prod_lst_ord_dt

            /***************************************************************************/
            --PART 6
            /***************************************************************************/


            , NVL(fin.bo_bo_prod_ord_cnt, 0) + NVL(fin.ofo_ofo_prod_ord_cnt, 0) + NVL(fin.de_de_cnt, 0)
              + NVL(fin.oli_oli_xce_cnt, 0) + NVL2(fin.ix_dc_cdb_last_ord_dt, 1, 0) + NVL(fin.eread_ereader_cnt, 0)
              + CASE WHEN fin.ix_dc_offline_first_dt IS NOT NULL AND NVL(fin.ofoxog_dc_offline_fst_crt_ind, 0) = 0
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.ix_dc_corp_first_dt IS NOT NULL AND NVL(fin.ofoxog_dc_corp_first_ind2, 0) = 0
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.ix_dc_online_first_dt IS NOT NULL AND NVL(fin.olixog2_dc_online_fst_crt_ind, 0) = 0 AND NVL(fin.olixog2_dc_online_fst_aps_ind, 0) = 0
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.ix_dc_last_aps_ord_dt IS NOT NULL AND NVL(fin.olixog1_dc_last_aps_crt_ind, 0) = 0
            THEN 1
                ELSE 0 END AS prod_ord_cnt
            , NVL(fin.bo_bo_prod_dm_ord_cnt, 0) + NVL(fin.ofo_ofo_prod_dm_ord_cnt, 0) + NVL(fin.oli_oli_xce_d_cnt, 0) AS prod_dm_ord_cnt
            , NVL(fin.bo_bo_prod_em_ord_cnt, 0) + NVL(fin.ofo_ofo_prod_em_ord_cnt, 0) + NVL(fin.oli_oli_xce_egknu_cnt, 0) AS prod_em_ord_cnt
            , NVL(fin.ofo_ofo_prod_dnr_ord_cnt, 0) + NVL(fin.oli_oli_prod_dnr_ord_cnt, 0) + NVL(fin.bo_bo_prod_dnr_ord_cnt, 0) AS prod_dnr_ord_cnt

            /*SD 2017/10/20 12:32: prod_cnt change*/
            , NVL(fin.oli_oli_cro_prod_cnt, 0) + NVL(fin.oli_oli_carp_prod_cnt, 0) + NVL(fin.oli_oli_crmg_prod_cnt, 0) + NVL(fin.brks_cr_prod_cnt, 0) + NVL(fin.de_de_cr_prod_cnt, 0)
              + NVL(fin.ofo_ofo_cr_prod_cnt, 0) + NVL(fin.brks_hl_prod_cnt, 0) + NVL(fin.de_de_hl_prod_cnt, 0) + NVL(fin.ofo_ofo_hl_prod_cnt, 0)
              + NVL(fin.brks_ma_prod_cnt, 0) + NVL(fin.ofo_ofo_ma_prod_cnt, 0) + NVL(fin.brks_shm_prod_cnt, 0) + NVL(fin.ofo_ofo_shm_prod_cnt, 0)
              + NVL(fin.oli_oli_aps_prod_cnt, 0) + NVL(fin.bi_bi_cnt, 0) + NVL2(fin.ix_dc_cdb_last_ord_dt, 1, 0) + NVL(fin.eread_ereader_cnt, 0)
              + CASE WHEN fin.ix_dc_online_first_dt IS NOT NULL AND NVL(fin.olixog2_dc_online_fst_crt_ind, 0) = 0 AND NVL(fin.olixog2_dc_online_fst_aps_ind, 0) = 0
            THEN 1
                ELSE 0 END
              + CASE WHEN ((fin.ix_dc_offline_first_dt IS NOT NULL OR (fin.ix_dc_corp_first_dt IS NOT NULL AND NVL(fin.ofoxog_dc_corp_first_ind, 0) = 0)) AND
                           (NVL(fin.ofoxog_dc_offline_fst_crt_ind, 0) = 0 AND NVL(fin.ofoxog_dc_corp_fst_crt_ind, 0) = 0))
            THEN 1
                ELSE 0 END
              + CASE WHEN fin.ix_dc_last_aps_ord_dt IS NOT NULL AND NVL(fin.olixog1_dc_last_aps_crt_ind, 0) = 0
            THEN 1
                ELSE 0 END AS prod_cnt


            , fin.ix_ad_apl_keycode AS ad_apl_keycode
            , CASE WHEN fin.e_email_type_cd = 'I' AND (fin.e_valid_flag = 'N' OR fin.e_src_valid_flag = 'N' OR fin.e_src_delv_ind = '1')
            THEN 'N'
              WHEN fin.e_email_type_cd = 'I'
                  THEN 'Y' END AS ind_email_summary_flag
            , CASE WHEN fin.e_email_type_cd = 'M' AND (fin.e_valid_flag = 'N' OR fin.e_src_valid_flag = 'N' OR fin.e_src_delv_ind = '1')
            THEN 'N'
              WHEN fin.e_email_type_cd = 'M'
                  THEN 'Y' END AS mrp_email_summary_flag
            , CASE WHEN fin.e_email_type_cd = 'N' AND (fin.e_valid_flag = 'N' OR fin.e_src_valid_flag = 'N' OR fin.e_src_delv_ind = '1')
            THEN 'N'
              WHEN fin.e_email_type_cd = 'N'
                  THEN 'Y' END AS news_email_summary_flag
            , CASE WHEN NVL2(fin.adv_mt_individual_id, 'Y', 'N') = 'Y'
            THEN 'Y'
              ELSE 'N' END AS adv_ever
            , NVL(fin.adv_mt_adv_active_flg, 'N') AS adv_active
            , NULL verity_recency_flag /*SD: Ed says 2017/10/17 14:47: We got rid of MT_ALLIANT therefore this as well*/
            , NVL2(fin.an_an_forums_flag, 'Y', 'N') AS discussions_forum_flag
            , fin.a_news_hlth_heart_flg AS news_hlth_heart_flg
            , fin.a_news_hlth_child_teen_flg AS news_hlth_child_teen_flg
            , fin.a_news_hlth_diabetes_flg AS news_hlth_diabetes_flg
            , fin.a_news_hlth_women_flg AS news_hlth_women_flg
            , fin.a_news_hlth_after_60_flg AS news_hlth_after_60_flg
            , fin.ibx_age_infobase AS age_infobase
            , NVL(fin.ibx_gender_infobase, 'U') AS gender_infobase
            , CASE WHEN fin.eua_individual_id IS NOT NULL
            THEN fin.eua_min_create_date
              ELSE NULL END ipad_enabled_dt
            , fin.bnb_bnb_frst_visitor_dt AS bnb_frst_visitor_dt
            , fin.bnb_bnb_last_visitor_dt AS bnb_last_visitor_dt
            , fin.bnb_bnb_frst_prospect_dt AS bnb_frst_prospect_dt
            , fin.bnb_bnb_last_prospect_dt AS bnb_last_prospect_dt
            , fin.bnb_bnb_frst_sales_dt AS bnb_frst_sales_dt
            , fin.bnb_bnb_last_sales_dt AS bnb_last_sales_dt
            , fin.bnb_bnb_tot_prospect_cnt AS bnb_tot_prospect_cnt
            , fin.bnb_bnb_tot_sale_cnt AS bnb_tot_sale_cnt
            , fin.bnb_bnb_best_status AS bnb_best_status
            , fin.bnb_bnb_best_status_dt AS bnb_best_status_dt
            , fin.bnb_bnb_most_recent_status AS bnb_most_recent_status
            , fin.bnb_bnb_most_recent_dt AS bnb_most_recent_dt
            , fin.bnb_bnb_nbr_new_sales AS bnb_nbr_new_sales
            , fin.bnb_bnb_nbr_used_sales AS bnb_nbr_used_sales
            , fin.bnb_bnb_most_recent_trans_type AS bnb_most_recent_trans_type
            , fin.ol_ol_match_recent_dt AS ol_match_recent_dt
            , fin.ol_ol_match_cnt AS ol_match_cnt
            , fin.ol_ol_match_cnt_6m AS ol_match_cnt_6m
            , fin.ol_ol_match_cnt_7_12m AS ol_match_cnt_7_12m
            , fin.ol_ol_match_cnt_13_18m AS ol_match_cnt_13_18m
            , fin.proem_ind_prospect_eml_mtch_ind AS ind_prospect_eml_mtch_ind
            , fin.proem_prospect_email_match_dt AS prospect_email_match_dt
            , fin.proem_prospect_email_match_cnt AS prospect_email_match_cnt


            /***************************************************************************/
            --PART 7
            /***************************************************************************/


            , CASE WHEN lower(fin.we_email_address) LIKE '%.ca' OR fin.ia_country_id = 'CAN'
            THEN CASE WHEN months_between(trunc(sysdate), trunc(fin.ofo_ofo_mx_ord_dt)) < 23 OR
                           months_between(trunc(sysdate), trunc(fin.erm_mx_cds_pub_dt)) < 23 OR
                           months_between(trunc(sysdate), trunc(fin.oli_oli_mx_end_dt)) < 23 OR
                           months_between(trunc(sysdate), trunc(CASE WHEN fin.fr_fr_max_don_amt > 0
                               THEN fin.fs_fr_lst_don_dt END)) < 23 OR
                           months_between(trunc(sysdate), trunc(fin.cga_mx_don_dt)) < 23 OR
                           months_between(trunc(sysdate), trunc(fin.a_mx_news_auth_n_eff_dt)) < 23 OR
                           fin.a_news_auth_y_cnt > 0
                THEN 'Y'
                 ELSE 'N' END
              END AS CAN_SPAM_FLG
            , CASE WHEN fin.coop_individual_id IS NOT NULL
            THEN 'Y'
              ELSE 'N' END print_sub_coop_flg
            , CASE WHEN fin.app_individual_id IS NOT NULL
            THEN 'R'
              ELSE NULL END mobile_app_usage_ind
            , ROUND((extract(DAYS FROM sysdate - fin.app_max_visit_strt_time) + fin.app_max_days_since_fst_use)) AS rtg_app_days_since_fst_act
            , ROUND(extract(DAYS FROM sysdate - fin.app_max_visit_strt_time)) AS rtg_app_days_since_lst_act
            , fin.key_email_fav_key AS email_fav_key
            , NVL(fin.adv_mt_adv_ltd_alrt_resp_cnt, 0) + NVL(fin.adv_mt_adv_ltd_sur_resp_cnt, 0) + NVL(fin.adv_mt_adv_ltd_don_cnt, 0) AS adv_cnt



        FROM
            agg.indiv_precalc_fin_temp fin
;

analyse agg.indiv_aggregation_done_temp;


DROP TABLE IF EXISTS 	cr_temp.agg_individual_acx;
CREATE TABLE 			cr_temp.agg_individual_acx
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT
            individual_id
            , hh_id
            /*, gender*/
            , customer_typ
            , decode(lst_don_amt, '', cast(0 AS DECIMAL(20, 2)), cast(lst_don_amt AS DECIMAL(20, 2))) AS lst_don_amt
            , cast(ltd_don_amt AS DECIMAL(20, 2))                                                     AS ltd_don_amt
            , cast(max_don_amt AS DECIMAL(20, 2))                                                     AS max_don_amt
            , fst_don_dt
            , lst_don_dt
            , ltd_don_cnt
            , non_prod_fst_dt
            , decode(fst_non_prod_cd, '', 'NONE', NULL, 'NONE', fst_non_prod_cd)                      AS fst_non_prod_cd
            , non_prod_cnt
            /*, mrp_flg*/
            /*, fr_combined_flg*/
            , ind_combined_dt
            , ind_ltd_amt
            , comp_flg
            , ind_fst_rel_dt
            , ind_lst_act_dt
            , ind_actv_cnt
            , ind_relship_cnt
            , decode(fst_prod_cd, '', 'NONE', NULL, 'NONE', fst_prod_cd)                              AS fst_prod_cd
            , ind_rec_prod_only_flg
            , emp_flg
            , cga_flg
            , news_car_watch_flg
            , news_best_buy_drug_flg
            , news_safety_alert_flg
            , news_hlth_alert_flg
            , news_cro_whats_flg
            , news_green_choice_flg
            , ind_dnr_only_flg
            , dc_cdb_last_ord_dt
            , prod_ltd_pd_amt
            , prod_fst_ord_dt
            , prod_lst_canc_bad_dbt_dt
            , non_prod_lst_act_dt
            , prod_lst_ord_dt
            , prod_ord_cnt
            , prod_dm_ord_cnt
            , prod_em_ord_cnt
            , prod_dnr_ord_cnt
            , prod_cnt
            , ad_apl_keycode
            , MAX(ind_email_summary_flag)                                                             AS ind_email_summary_flag
            , MAX(mrp_email_summary_flag)                                                             AS mrp_email_summary_flag
            , MAX(news_email_summary_flag)                                                            AS news_email_summary_flag
            , MAX(adv_ever)                                                                           AS adv_ever
            , MAX(adv_active)                                                                         AS adv_active
            , MAX(verity_recency_flag)                                                                AS verity_recency_flag
            , MAX(discussions_forum_flag)                                                             AS discussions_forum_flag
            , MAX(news_hlth_heart_flg)                                                                AS news_hlth_heart_flg
            , MAX(news_hlth_child_teen_flg)                                                           AS news_hlth_child_teen_flg
            , MAX(news_hlth_diabetes_flg)                                                             AS news_hlth_diabetes_flg
            , MAX(news_hlth_women_flg)                                                                AS news_hlth_women_flg
            , MAX(news_hlth_after_60_flg)                                                             AS news_hlth_after_60_flg
            , MAX(age_infobase)                                                                       AS age_infobase
            , MAX(gender_infobase)                                                                    AS gender_infobase
            , MAX(decode(ind_fst_rel_cd, '', 'UNK', NULL, 'UNK', ind_fst_rel_cd))                     AS ind_fst_rel_cd
            , ipad_enabled_dt
            , MAX(bnb_frst_visitor_dt)                                                                AS bnb_frst_visitor_dt
            , MAX(bnb_last_visitor_dt)                                                                AS bnb_last_visitor_dt
            , MAX(bnb_frst_prospect_dt)                                                               AS bnb_frst_prospect_dt
            , MAX(bnb_last_prospect_dt)                                                               AS bnb_last_prospect_dt
            , MAX(bnb_frst_sales_dt)                                                                  AS bnb_frst_sales_dt
            , MAX(bnb_last_sales_dt)                                                                  AS bnb_last_sales_dt
            , MAX(bnb_tot_prospect_cnt)                                                               AS bnb_tot_prospect_cnt
            , MAX(bnb_tot_sale_cnt)                                                                   AS bnb_tot_sale_cnt
            , MAX(bnb_best_status)                                                                    AS bnb_best_status
            , MAX(bnb_best_status_dt)                                                                 AS bnb_best_status_dt
            , MAX(bnb_most_recent_status)                                                             AS bnb_most_recent_status
            , MAX(bnb_most_recent_dt)                                                                 AS bnb_most_recent_dt
            , MAX(bnb_nbr_new_sales)                                                                  AS bnb_nbr_new_sales
            , MAX(bnb_nbr_used_sales)                                                                 AS bnb_nbr_used_sales
            , MAX(bnb_most_recent_trans_type)                                                         AS bnb_most_recent_trans_type
            , MAX(ol_match_recent_dt)                                                                 AS ol_match_recent_dt
            , MAX(ol_match_cnt)                                                                       AS ol_match_cnt
            , MAX(ol_match_cnt_6m)                                                                    AS ol_match_cnt_6m
            , MAX(ol_match_cnt_7_12m)                                                                 AS ol_match_cnt_7_12m
            , MAX(ol_match_cnt_13_18m)                                                                AS ol_match_cnt_13_18m
            , NVL(MAX(ind_prospect_eml_mtch_ind), 'C')                                                AS ind_prospect_eml_mtch_ind
            , MAX(prospect_email_match_dt)                                                            AS prospect_email_match_dt
            , MAX(prospect_email_match_cnt)                                                           AS prospect_email_match_cnt
            , MAX(can_spam_flg)                                                                       AS can_spam_flg
            , MAX(print_sub_coop_flg)                                                                 AS print_sub_coop_flg
            , MAX(mobile_app_usage_ind)                                                               AS mobile_app_usage_ind
            , MAX(rtg_app_days_since_fst_act)                                                         AS rtg_app_days_since_fst_act
            , MAX(rtg_app_days_since_lst_act)                                                         AS rtg_app_days_since_lst_act
            , MAX(email_fav_key)                                                                      AS email_fav_key
            , MAX(adv_cnt)                                                                            AS adv_cnt
        FROM agg.indiv_aggregation_done_temp
        GROUP BY individual_id, hh_id, gender, customer_typ, lst_don_amt, ltd_don_amt, max_don_amt, fst_don_dt, lst_don_dt, ltd_don_cnt, non_prod_fst_dt, fst_non_prod_cd, non_prod_cnt, mrp_flg, fr_combined_flg, ind_combined_dt, ind_ltd_amt, comp_flg, ind_fst_rel_dt, ind_lst_act_dt, ind_actv_cnt, ind_relship_cnt, fst_prod_cd, ind_rec_prod_only_flg, emp_flg, cga_flg, news_car_watch_flg, news_best_buy_drug_flg, news_safety_alert_flg, news_hlth_alert_flg, news_cro_whats_flg, news_green_choice_flg, ind_dnr_only_flg, dc_cdb_last_ord_dt, prod_ltd_pd_amt, prod_fst_ord_dt, prod_lst_canc_bad_dbt_dt, non_prod_lst_act_dt, prod_lst_ord_dt, prod_ord_cnt, prod_dm_ord_cnt, prod_em_ord_cnt, prod_dnr_ord_cnt, prod_cnt, ad_apl_keycode, ipad_enabled_dt;

ANALYSE cr_temp.agg_individual_acx;
