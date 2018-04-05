DROP TABLE IF EXISTS    cr_temp.indiv_aggregation_done_temp;
CREATE TABLE            cr_temp.indiv_aggregation_done_temp
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
            cr_temp.indiv_precalc_fin_temp fin
;
