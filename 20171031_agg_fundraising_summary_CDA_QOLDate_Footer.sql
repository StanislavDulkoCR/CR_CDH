/**************************************************************************
*            (C) Copyright Consumer Reports 2017
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  agg_fundraising_summary.sql
* Date       :  2017/10/09
* Dev & QA   :  Stanislav Dulko
**************************************************************************/

DROP TABLE IF EXISTS agg.fr_sum_basic_temp;
CREATE TABLE agg.fr_sum_basic_temp
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT
            individual_id
            , CASE WHEN substring(fr_mbr_basic_lst_keycode, 2, 2) NOT IN ('88')
                        AND substring(fr_mbr_basic_lst_keycode, 1, 4) NOT IN ('6808')
                        AND TO_CHAR(fr_mbr_basic_lst_dt, 'MM') IN ('01', '02', '03', '04', '05', '06', '07', '08', '09')
            THEN CAST(DECODE(TO_CHAR(fr_mbr_basic_lst_dt, 'YYYYMMDD'), NULL, NULL, to_date(TO_CHAR(fr_mbr_basic_lst_dt, 'YYYY') || '0831', 'YYYYMMDD')) AS TIMESTAMP)
              ELSE CAST(DECODE(TO_CHAR(fr_mbr_basic_lst_dt, 'YYYYMMDD'), NULL, NULL, to_date(TO_CHAR(fr_mbr_basic_lst_dt + 365, 'YYYY') || '0831', 'YYYYMMDD')) AS TIMESTAMP)
              END AS fr_mbr_basic_exp_dt
        FROM agg.mt_fr_summary_temp;


DROP TABLE IF EXISTS prod.agg_fundraising_summary;
CREATE TABLE prod.agg_fundraising_summary
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT
            fr.individual_id
            /*, fr.ind_urn*/
            , fr.hh_id
            , NVL(fr.fr_avg_don_amt, '0')                        AS fr_avg_don_amt
            , NVL(fr.fr_avg_gross_don_amt, '0')                  AS fr_avg_gross_don_amt
            , NVL(fr.fr_lst_don_amt, '0')                        AS fr_lst_don_amt
            , NVL(fr.fr_ltd_don_amt, '0')                        AS fr_ltd_don_amt
            , NVL(fr.fr_max_don_amt, '0')                        AS fr_max_don_amt
            , NVL(fr.fr_1_6_mth_don_amt, '0')                    AS fr_1_6_mth_don_amt
            , NVL(fr.fr_13_24_mth_don_amt, '0')                  AS fr_13_24_mth_don_amt
            , NVL(fr.fr_25_36_mth_don_amt, '0')                  AS fr_25_36_mth_don_amt
            , NVL(fr.fr_37_plus_mth_don_amt, '0')                AS fr_37_plus_mth_don_amt
            , NVL(fr.fr_7_12_mth_don_amt, '0')                   AS fr_7_12_mth_don_amt
            , NVL(fr.fr_prior_don_amt, '0')                      AS fr_prior_don_amt
            /*SD 20171009: all pledges are deprecated and dropped*/
            /*, NVL(fr.fr_lst_tm_pldg_amt,'0')      AS fr_lst_tm_pldg_amt*/
            /*, NVL(fr.fr_lst_lt_pldg_amt,'0')      AS fr_lst_lt_pldg_amt*/
            /*, NVL(fr.fr_ltd_pldg_amt,'0')         AS fr_ltd_pldg_amt*/
            /*, NVL(fr.fr_1_6_mth_pldg_amt,'0')     AS fr_1_6_mth_pldg_amt*/
            /*, NVL(fr.fr_13_24_mth_pldg_amt,'0')   AS fr_13_24_mth_pldg_amt*/
            /*, NVL(fr.fr_25_36_mth_pldg_amt,'0')   AS fr_25_36_mth_pldg_amt*/
            /*, NVL(fr.fr_37_plus_mth_pldg_amt,'0') AS fr_37_plus_mth_pldg_amt*/
            /*, NVL(fr.fr_7_12_mth_pldg_amt,'0')    AS fr_7_12_mth_pldg_amt*/
            , fr.fr_fst_don_dt
            , fr.fr_lst_don_dt
            /*, fr.fr_lst_lt_pldg_pmt_dt*/
            /*, fr.fr_lst_tm_pldg_dt*/
            /*, fr.fr_lst_lt_pldg_dt*/
            , fr.fr_prior_don_dt
            /*, fr.fr_lt_pldg_flg*/
            , fr.fr_dnr_act_flg
            , fr.fr_dnr_inact_flg
            , fr.fr_dnr_lps_flg
            , fr.fr_mbr_gvng_lvl_cd
            , cast(fr.fr_mbr_exp_dt AS TIMESTAMP)                AS fr_mbr_exp_dt
            , fr.fr_fst_act_keycode
            , fr.fr_lst_act_keycode
            , fr.fr_prior_act_keycode
            /*, fr.fr_lt_pldg_opn_flg*/
            , NVL(fr.fr_ltd_don_cnt, '0')                        AS fr_ltd_don_cnt
            , NVL(fr.fr_don_ref_cnt, '0')                        AS fr_don_ref_cnt
            /*, NVL(fr.fr_ltd_pldg_cnt,'0')*/
            , NVL(fr.fr_times_ren_cnt, '0')                      AS fr_times_ren_cnt
            , fr.fr_lst_don_src_cd
            /*, fr.fr_tm_pldg_opn_flg*/
            /*, fr.fr_fst_pldg_dt*/
            /*, fr.fr_fst_lt_pldg_dt*/
            , NVL(fr.fr_lst_rfl_don_amt, '0')                    AS fr_lst_rfl_don_amt
            , NVL(fr.fr_lst_non_rfl_don_amt, '0')                AS fr_lst_non_rfl_don_amt
            , cast(fr.fr_mbr_frst_strt_dt AS TIMESTAMP)          AS fr_mbr_frst_strt_dt
            , fr.fr_mbr_lst_keycode
            , NVL(fr.fr_mbr_lst_ren_don_amt, '0')                AS fr_mbr_lst_ren_don_amt
            , fr.fr_tof_cd
            , fr.fr_lt_dnr_flg
            , NVL(fr.fr_mbr_lst_add_don_amt, '0')                AS fr_mbr_lst_add_don_amt
            , cast(fr.fr_mbr_lst_add_don_dt AS TIMESTAMP)        AS fr_mbr_lst_add_don_dt
            , cast(fr.fr_mbr_pres_cir_frst_strt_dt AS TIMESTAMP) AS fr_mbr_pres_cir_frst_strt_dt
            , fr.fr_fst_don_keycode
            , fr.fr_max_don_amt_12_mth
            , fr.fr_max_don_amt_24_mth
            , fr.fr_max_don_amt_36_mth
            /*, fr.FR_LST_GUARDIAN_DON_DT*/
            /*, fr.FR_BRIDGE_PROGRAM_FLG*/
            , fr.fr_track_number
            , fr.fr_conv_tag_rsp_flg
            , fr.fr_fst_don_amt
            , cast(fr.fr_lst_rfl_don_dt AS TIMESTAMP)            AS fr_lst_rfl_don_dt
            , cast(fr.fr_lst_non_rfl_don_dt AS TIMESTAMP)        AS fr_lst_non_rfl_don_dt
            , fr.fr_lst_eml_don_amt
            , fr.fr_lst_eml_don_dt
            , CASE WHEN ind.ind_fst_rel_dt >= '3-JAN-2013'
                        AND ind.ind_fst_rel_dt < fr.fr_fst_don_dt
                        AND fr.fr_fst_don_dt >= '3-JAN-2013'
                        AND fr.fr_mbr_pres_cir_frst_strt_dt IS NULL
            THEN 'Y'
              ELSE 'N'
              END                                                AS fr_coop_eligible_flg
            , NVL(fr.fr_lst_dm_don_amt, '0')                     AS fr_lst_dm_don_amt
            , fr.fr_lst_dm_don_dt
            , NVL(fr.fr_lst_org_onl_don_amt, '0')                AS fr_lst_org_onl_don_amt
            , fr.fr_lst_org_onl_don_dt
            , NVL(fr.fr_lst_ecomm_don_amt, '0')                  AS fr_lst_ecomm_don_amt
            , fr.fr_lst_ecomm_don_dt
            , NVL(fr.fr_1_6_mth_don_cnt, '0')                    AS fr_1_6_mth_don_cnt
            , NVL(fr.fr_7_12_mth_don_cnt, '0')                   AS fr_7_12_mth_don_cnt
            , NVL(fr.fr_13_24_mth_don_cnt, '0')                  AS fr_13_24_mth_don_cnt
            , NVL(fr.fr_25_36_mth_don_cnt, '0')                  AS fr_25_36_mth_don_cnt
            , NVL(fr.fr_37_plus_mth_don_cnt, '0')                AS fr_37_plus_mth_don_cnt
            , fr.fr_dm_pros_mdl_rsp_flg
            , CASE WHEN fr.fr_mbr_gvng_lvl_cd = 'C'
            THEN 'C'
              WHEN fr.fr_mbr_gvng_lvl_cd IN ('S', 'G', 'P')
                   AND fr.fr_mbr_exp_dt >= ADD_MONTHS(sysdate, -24)
                  THEN 'L'
              WHEN CAST(tmp.fr_mbr_basic_exp_dt AS TIMESTAMP) >= sysdate
                  THEN 'B'
              ELSE 'N'
              END                                                AS fr_mbr_comb_level
            , CASE WHEN fr.fr_mbr_gvng_lvl_cd = 'C'
            THEN TO_DATE('01/01/2050', 'MM/DD/YYYY')
              WHEN fr.fr_mbr_gvng_lvl_cd IN ('S', 'G', 'P')
                   AND fr.fr_mbr_exp_dt >= ADD_MONTHS(sysdate, -24)
                  THEN ADD_MONTHS(fr.fr_mbr_exp_dt, 24)
              ELSE CAST(tmp.fr_mbr_basic_exp_dt AS TIMESTAMP)
              END                                                AS fr_mbr_comb_exp_dt
            , fr.fr_mbr_basic_fst_dt
            , NVL(fr.fr_mbr_basic_fst_amt, '0')                  AS fr_mbr_basic_fst_amt
            , fr.fr_mbr_basic_lst_dt
            , NVL(fr.fr_mbr_basic_lst_amt, '0')                  AS fr_mbr_basic_lst_amt
            , fr.fr_ch_status
            , NVL(fr.fr_ch_curr_ttl_don_amt, '0')                AS fr_ch_curr_ttl_don_amt
            , fr.fr_ch_lst_don_dt
            , fr.fr_ch_curr_strt_dt
            , NVL(fr.fr_ch_lst_don_amt, '0')                     AS fr_ch_lst_don_amt
            , NVL(fr.fr_ch_curr_don_cnt, '0')                    AS fr_ch_curr_don_cnt
            , fr.fr_ch_lst_don_keycd
            , fr.fr_ch_status_active_dt
            , cast(tmp.fr_mbr_basic_exp_dt AS TIMESTAMP)         AS fr_mbr_basic_exp_dt
            , fr.fr_mbr_basic_lst_keycode
            , fr.osl_hh_id
        FROM agg.mt_fr_summary_temp fr
            LEFT JOIN agg.fr_sum_basic_temp tmp
                ON fr.individual_id = tmp.individual_id
            LEFT JOIN prod.agg_individual ind
                ON fr.individual_id = ind.individual_id
        ORDER BY fr.individual_id ASC;

ANALYSE prod.agg_fundraising_summary;