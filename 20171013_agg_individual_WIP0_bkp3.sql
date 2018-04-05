/*
* *************************************************************************
*            (C) Copyright Consumer Reports 2017
*                    All Rights Reserved.
* --------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  agg_individual.sql
* Date       :  2017/10/16
* Dev & QA   :  Stanislav Dulko
* *************************************************************************
*/
/***************************************************************************/
-- AGG_INDIVIDUAL - Main temp tables
/***************************************************************************/

DROP TABLE IF EXISTS 	cr_temp.indiv_ofo_xog_temp;
CREATE TABLE 			cr_temp.indiv_ofo_xog_temp
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
          /*#TODO RM*/
          AND xo.individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
    GROUP BY xo.individual_id;


DROP TABLE IF EXISTS cr_temp.indiv_oli_xog_1_temp;
CREATE TABLE cr_temp.indiv_oli_xog_1_temp
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
          /*#TODO RM*/
          AND xo.individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
    GROUP BY xo.individual_id;


DROP TABLE IF EXISTS cr_temp.indiv_oli_xog_2_temp;
CREATE TABLE cr_temp.indiv_oli_xog_2_temp
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
          /*#TODO RM*/ AND xo.individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
    GROUP BY xo.individual_id;

    
/*------------------------------------UNTESTED------------------------------------*/
DROP TABLE IF EXISTS    cr_temp.indiv_survey_resp_dt_temp;
CREATE TABLE            cr_temp.indiv_survey_resp_dt_temp
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
        , max(answer_date) answer_date
        , min(answer_date) min_answer_date
    --## SEE 20171013_agg_individual_survey_resp_pivot.sql
    FROM cte_surv_cdhicd
    WHERE substring(individual_id, 1, 1) = 1
          AND individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
    GROUP BY individual_id
;
/*------------------------------------UNTESTED------------------------------------*/

DROP TABLE IF EXISTS    cr_temp.indiv_webpage_temp;
CREATE TABLE            cr_temp.indiv_webpage_temp
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
              /*#TODO RM*/ AND individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
        GROUP BY individual_id;

DROP TABLE IF EXISTS cr_temp.indiv_emailfavkey_temp;
CREATE TABLE cr_temp.indiv_emailfavkey_temp
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
    WHERE ae.email_type_cd = 'I'
          /*#TODO RM*/ AND ae.individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747);


DROP TABLE IF EXISTS cr_temp.indiv_ibx_temp;
CREATE TABLE cr_temp.indiv_ibx_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT
        individual_id
        , ROUND(MONTHS_BETWEEN(SYSDATE, TO_DATE(substring(IB8624_DOB_IP_IND_DFLT_1ST_IND, 1, 4) || LPAD(DECODE(substring(ib8624_dob_ip_ind_dflt_1st_ind, 5, 2), '00', CAST(TRUNC(random() * 13 + 1) AS TEXT), substring(IB8624_DOB_IP_IND_DFLT_1ST_IND, 5, 2)), 2, '0') || LPAD(DECODE(substring(ib8624_dob_ip_ind_dflt_1st_ind, 7, 2), '00', CAST(TRUNC(random() * 29 + 1) AS TEXT), substring(ib8624_dob_ip_ind_dflt_1st_ind, 7, 2)), 2, '0'), 'YYYYMMDD')) / 12) AS age_infobase
        , ib8688_gender_input_ind                                                                                                                                                                                                                                                                                                                                                                                                                                   AS gender_infobase
    FROM prod.agg_infobase_profile
    WHERE 1 = 1
          /*#TODO RM*/
          AND individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747);
/***************************************************************************/
-- AGG_INDIVIDUAL - Miscellaneous temp tables
/***************************************************************************/
DROP TABLE IF EXISTS cr_temp.indiv_ind_xref_temp;
CREATE TABLE cr_temp.indiv_ind_xref_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS

    SELECT DISTINCT
          ind.individual_id AS individual_id
        , ind.household_id  AS hh_id
        , ind.household_id  AS osl_hh_id
    FROM PROD.individual ind
    WHERE INDIVIDUAL_ID IS NOT NULL
          /*#TODO RM*/
          AND individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747);


DROP TABLE IF EXISTS cr_temp.indiv_ext_ref_dt_temp;
CREATE TABLE cr_temp.indiv_ext_ref_dt_temp
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
               /*#TODO RM*/
               AND individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
         GROUP BY individual_id, account_subtype_code
         HAVING COUNT(*) > 1) accer
    GROUP BY individual_id;


DROP TABLE IF EXISTS cr_temp.indiv_cga_temp;
CREATE TABLE cr_temp.indiv_cga_temp
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
          /*#TODO RM*/
          AND individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
    GROUP BY individual_id;


DROP TABLE IF EXISTS cr_temp.indiv_preference_temp;
CREATE TABLE cr_temp.indiv_preference_temp
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
          /*#TODO RM*/
          AND individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
    GROUP BY individual_id;


DROP TABLE IF EXISTS cr_temp.indiv_off_canc_temp;
CREATE TABLE cr_temp.indiv_off_canc_temp
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
                              AND NVL(substring(t1.keycode, 2, 1), 'X') != 'N')
                          OR (substring(t1.keycode, 1, 1) = 'B'
                              AND NVL(substring(t1.keycode, 9, 1), 'X') NOT IN ('A', 'B', 'C', 'S'))
                          OR (substring(t1.keycode, 1, 1) = 'U'
                              AND REGEXP_INSTR(NVL(substring(t1.keycode, 9, 1), '1'), '[^[:alpha:]]') = 1)
                          OR (substring(t1.keycode, 1, 1) = 'R'
                              AND NVL(substring(t1.keycode, 9, 1), 'X') NOT IN ('B', 'C', 'D', 'E', 'F', 'T'))
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
        LEFT JOIN prod.print_cancel oca
            ON t1.hash_account_id = oca.hash_action_id
    WHERE 1 = 1
          /*#TODO RM*/
          AND t1.individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
    GROUP BY t1.individual_id;


DROP TABLE IF EXISTS cr_temp.indiv_acct_merge_temp;
CREATE TABLE cr_temp.indiv_acct_merge_temp
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


DROP TABLE IF EXISTS cr_temp.indiv_oli_sku_temp;
CREATE TABLE cr_temp.indiv_oli_sku_temp
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
      /*#TODO RM*/
      AND t1.individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
GROUP BY t1.individual_id;


DROP TABLE IF EXISTS cr_temp.indiv_onl_item_ord_temp;
CREATE TABLE cr_temp.indiv_onl_item_ord_temp
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
          /*#TODO RM*/
          AND ot.individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
    GROUP BY ot.individual_id;


DROP TABLE IF EXISTS CR_TEMP.INDIV_DE_EXPIRES_TEMP;
CREATE TABLE CR_TEMP.INDIV_DE_EXPIRES_TEMP
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


DROP TABLE IF EXISTS CR_TEMP.INDIV_books_ord_TEMP;
CREATE TABLE CR_TEMP.INDIV_books_ord_TEMP
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
          /*#TODO RM*/
          AND individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
    GROUP BY individual_id;


DROP TABLE IF EXISTS cr_temp.indiv_misc_agg_temp;
CREATE TABLE cr_temp.indiv_misc_agg_temp
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
FROM cr_temp.indiv_ind_xref_temp xr
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
WHERE 1 = 1
      /*#TODO RM*/
      AND xr.individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747);


DROP TABLE IF EXISTS cr_temp.indiv_issue_lkp_acx_temp;
CREATE TABLE cr_temp.indiv_issue_lkp_acx_temp
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


DROP TABLE IF EXISTS cr_temp.indiv_print_lkp_temp;
CREATE TABLE cr_temp.indiv_print_lkp_temp
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
             LEFT JOIN cr_temp.indiv_issue_lkp_acx_temp end_lkp
                 ON oo.mag_cd = end_lkp.mag_cd
                    AND (oo.orig_strt_iss_num + oo.term_mth_cnt) = end_lkp.iss_num
             LEFT JOIN cr_temp.indiv_issue_lkp_acx_temp strt_lkp
                 ON oo.mag_cd = strt_lkp.mag_cd
                    AND oo.orig_strt_iss_num = strt_lkp.iss_num) polkp
    WHERE 1 = 1
          /*#TODO RM*/
          AND individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
    GROUP BY individual_id;


DROP TABLE IF EXISTS 	cr_temp.indiv_print_crdt_temp;
CREATE TABLE 			cr_temp.indiv_print_crdt_temp
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
          /*#TODO RM*/
          AND er.individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
    GROUP BY er.individual_id;


DROP TABLE IF EXISTS 	cr_temp.indiv_bnb_temp;
CREATE TABLE 			cr_temp.indiv_bnb_temp
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
WHERE 1 = 1
      /*#TODO RM*/
      AND individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747);


DROP TABLE IF EXISTS cr_temp.indiv_print_maglkp_temp;
CREATE TABLE cr_temp.indiv_print_maglkp_temp
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
          /*#TODO RM*/
          AND individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
    GROUP BY er.individual_id;


DROP TABLE IF EXISTS cr_temp.indiv_printsum_pref_temp;
CREATE TABLE cr_temp.indiv_printsum_pref_temp
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
      /*#TODO RM*/
      AND mos1.individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747)
GROUP BY mos1.individual_id;


/*DROP TABLE IF EXISTS    cr_temp.indiv_appsession_temp;
CREATE TABLE            cr_temp.indiv_appsession_temp
DISTSTYLE KEY
DISTKEY (individual_id)
SORTKEY (individual_id)
AS
SELECT
individual_id
, max(visit_start_time)     max_visit_strt_time
, max(days_since_first_use) max_days_since_fst_use
FROM prod.app_session
WHERE 1 = 1
#TODO RM AND individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107
, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416,
1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200558542)
GROUP BY individual_id;*/


DROP TABLE IF EXISTS cr_temp.indiv_misc_agg_2_temp;
CREATE TABLE cr_temp.indiv_misc_agg_2_temp
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
  FROM cr_temp.indiv_emailfavkey_temp
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
FROM cr_temp.indiv_ind_xref_temp xr
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
WHERE 1 = 1
      /*#TODO RM*/
      AND xr.individual_id IN (1216297895, 1200000029, 1200000039, 1200000050, 1200000058, 1200000059, 1200000060, 1200000092, 1200000100, 1200000107, 1200000204, 1200000206, 1200000244, 1200000253, 1200000265, 1200000301, 1200000089, 1200000178, 1200000270, 1200000346, 1200000405, 1200000416, 1200000433, 1200000489, 1200000536, 1200000554, 1200000602, 1200000612, 1200000629, 1200000636, 1200000640, 1200000678, 1200000002, 1200068747, 1200068747);


DROP TABLE IF EXISTS cr_temp.indiv_precalc_1_temp;
CREATE TABLE cr_temp.indiv_precalc_1_temp
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
    FROM cr_temp.indiv_ind_xref_temp xr
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


DROP TABLE IF EXISTS 	cr_temp.indiv_precalc_2_temp;
CREATE TABLE 			cr_temp.indiv_precalc_2_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
 WITH cte_adv_indiv AS
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
    , pref_a.auth_cnt                  AS pref_a_auth_cnt
    , pref_a.auth_flg_cnt              AS pref_a_auth_flg_cnt
    , pref_a.individual_id             AS pref_a_individual_id
    , pref_a.mn_fst_dt                 AS pref_a_mn_fst_dt
    , pref_a.mn_newsltr_fst_dt         AS pref_a_mn_newsltr_fst_dt
    , pref_a.mrp_flg                   AS pref_a_mrp_flg
    , pref_a.mx_fst_dt                 AS pref_a_mx_fst_dt
    , pref_a.mx_news_auth_n_eff_dt     AS pref_a_mx_news_auth_n_eff_dt
    , pref_a.mx_newsltr_fst_dt         AS pref_a_mx_newsltr_fst_dt
    , pref_a.news_auth_cnt             AS pref_a_news_auth_cnt
    , pref_a.news_auth_y_cnt           AS pref_a_news_auth_y_cnt
    , pref_a.news_best_buy_drug_flg    AS pref_a_news_best_buy_drug_flg
    , pref_a.news_car_watch_flg        AS pref_a_news_car_watch_flg
    , pref_a.news_cro_whats_flg        AS pref_a_news_cro_whats_flg
    , pref_a.news_green_choice_flg     AS pref_a_news_green_choice_flg
    , pref_a.news_hlth_after_60_flg    AS pref_a_news_hlth_after_60_flg
    , pref_a.news_hlth_alert_flg       AS pref_a_news_hlth_alert_flg
    , pref_a.news_hlth_child_teen_flg  AS pref_a_news_hlth_child_teen_flg
    , pref_a.news_hlth_diabetes_flg    AS pref_a_news_hlth_diabetes_flg
    , pref_a.news_hlth_heart_flg       AS pref_a_news_hlth_heart_flg
    , pref_a.news_hlth_women_flg       AS pref_a_news_hlth_women_flg
    , pref_a.news_safety_alert_flg     AS pref_a_news_safety_alert_flg
FROM cr_temp.indiv_ind_xref_temp xr
    LEFT JOIN cte_adv_indiv adv
        ON xr.individual_id = adv.individual_id
    LEFT JOIN prod.agg_constituent adv_mt
        ON xr.individual_id = adv_mt.individual_id
    LEFT JOIN cr_temp.indiv_books_ord_temp bo
        ON xr.individual_id = bo.individual_id
    LEFT JOIN cr_temp.indiv_cga_temp cga
        ON xr.individual_id = cga.individual_id
    LEFT JOIN CR_TEMP.INDIV_DE_EXPIRES_TEMP de
        ON xr.individual_id = de.individual_id
    LEFT JOIN cr_temp.indiv_ext_ref_dt_temp er
        ON xr.individual_id = er.individual_id
    LEFT JOIN cte_fr_comb_primary frc
        ON xr.individual_id = frc.primary_individual_id
    LEFT JOIN cte_fr_comb_secondary frc2
        ON xr.individual_id = frc2.secondary_individual_id
    LEFT JOIN cte_name_address na
        ON xr.individual_id = na.individual_id
    LEFT JOIN cr_temp.indiv_acct_merge_temp oaa
        ON xr.individual_id = oaa.individual_id
    LEFT JOIN cr_temp.indiv_off_canc_temp ofo
        ON xr.individual_id = ofo.individual_id
    LEFT JOIN cr_temp.indiv_oli_sku_temp oli
        ON xr.individual_id = oli.individual_id
    LEFT JOIN cr_temp.indiv_onl_item_ord_temp olo
        ON xr.individual_id = olo.individual_id
    LEFT JOIN cr_temp.indiv_preference_temp pref_a
        ON xr.individual_id = pref_a.individual_id
;



DROP TABLE IF EXISTS    cr_temp.indiv_precalc_3_temp;
CREATE TABLE            cr_temp.indiv_precalc_3_temp
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
        , an.an_emp_flg          AS an_an_emp_flg
        , an.an_forums_flag      AS an_an_forums_flag
        , an.individual_id       AS an_individual_id
        , bb.bnb_frst_sales_dt   AS bb_bnb_frst_sales_dt
        , bb.bnb_last_sales_dt   AS bb_bnb_last_sales_dt
        , bb.bnb_tot_sale_cnt    AS bb_bnb_tot_sale_cnt
        , bb.individual_id       AS bb_individual_id
        , brks.cr_prod_cnt       AS brks_cr_prod_cnt
        , brks.cr_prod_cnta      AS brks_cr_prod_cnta
        , brks.hl_prod_cnt       AS brks_hl_prod_cnt
        , brks.individual_id     AS brks_individual_id
        , brks.ma_prod_cnt       AS brks_ma_prod_cnt
        , brks.shm_prod_cnt      AS brks_shm_prod_cnt
        , e.email_type_cd        AS e_email_type_cd
        , e.individual_id        AS e_individual_id
        , e.src_delv_ind         AS e_src_delv_ind
        , e.src_valid_flag       AS e_src_valid_flag
        , e.valid_flag           AS e_valid_flag
        , eua.individual_id      AS eua_individual_id
        , eua.min_create_date    AS eua_min_create_date
        , ibx.age_infobase       AS ibx_age_infobase
        , ibx.gender_infobase    AS ibx_gender_infobase
        , ibx.individual_id      AS ibx_individual_id
        , ol.individual_id       AS ol_individual_id
        , ol.ol_match_cnt        AS ol_ol_match_cnt
        , ol.ol_match_cnt_13_18m AS ol_ol_match_cnt_13_18m
        , ol.ol_match_cnt_6m     AS ol_ol_match_cnt_6m
        , ol.ol_match_cnt_7_12m  AS ol_ol_match_cnt_7_12m
        , ol.ol_match_recent_dt  AS ol_ol_match_recent_dt
        , we.email_address       AS we_email_address
        , we.individual_id       AS we_individual_id
    FROM cr_temp.indiv_ind_xref_temp xr
        LEFT JOIN cr_temp.indiv_print_lkp_temp brks
            ON xr.individual_id = brks.individual_id
        LEFT JOIN prod.agg_email e
            ON xr.individual_id = e.individual_id
        LEFT JOIN cte_email_address we
            ON xr.individual_id = we.individual_id
        LEFT JOIN cte_agg_acc_num an
            ON xr.individual_id = an.individual_id
        LEFT JOIN cte_ol_deprecated ol
            ON xr.individual_id = ol.individual_id
        LEFT JOIN cr_temp.indiv_ibx_temp ibx
            ON xr.individual_id = ibx.individual_id
        LEFT JOIN cr_temp.indiv_print_crdt_temp eua
            ON xr.individual_id = eua.individual_id
        LEFT JOIN cr_temp.indiv_bnb_temp bb
            ON xr.individual_id = bb.individual_id
;
