TRUNCATE TABLE prod.agg_digital_summary;

INSERT INTO prod.agg_digital_summary (individual_id
       , hh_id
       , cro_actv_flg
       , aps_actv_pd_flg
       , aps_rpts_actv_pd_flg
       , crmg_actv_pd_flg
       , cro_actv_pd_flg
       , ncbk_actv_pd_flg
       , ucbk_actv_pd_flg
       , crmg_annual_flg
       , cro_annual_flg
       , crmg_canc_bad_dbt_flg
       , cro_canc_bad_dbt_flg
       , crmg_canc_cust_flg
       , cro_canc_cust_flg
       , crmg_cr_stat_cd
       , cro_cr_stat_cd
       , crmg_exp_dt
       , cro_exp_dt
       , crmg_lst_canc_bad_dbt_dt
       , crmg_lst_canc_cust_dt
       , cro_lst_canc_cust_dt
       , aps_lst_ord_dt
       , nc_rpts_lst_ord_dt
       , ncbk_lst_ord_dt
       , uc_rpts_lst_ord_dt
       , ucbk_lst_ord_dt
       , crmg_lst_pmt_dt
       , cro_lst_pmt_dt
       , nc_rpts_lst_del_chnl_cd
       , uc_rpts_lst_del_chnl_cd
       , aps_flg
       , crmg_flg
       , cro_flg
       , nc_rpts_flg
       , ncbk_flg
       , uc_rpts_flg
       , ucbk_flg
       , crmg_curr_mbr_keycode
       , cro_curr_mbr_keycode
       , crmg_curr_ord_keycode
       , cro_curr_ord_keycode
       , crmg_fst_mbr_keycode
       , cro_fst_mbr_keycode
       , aps_fst_ord_keycode
       , nc_rpts_fst_ord_keycode
       , ncbk_fst_ord_keycode
       , uc_rpts_fst_ord_keycode
       , ucbk_fst_ord_keycode
       , aps_lst_ord_keycode
       , crmg_lst_ord_keycode
       , cro_lst_ord_keycode
       , nc_rpts_lst_ord_keycode
       , ncbk_lst_ord_keycode
       , uc_rpts_lst_ord_keycode
       , ucbk_lst_ord_keycode
       , crmg_lst_sub_amt
       , cro_lst_sub_amt
       , crmg_monthly_flg
       , cro_monthly_flg
       , cro_non_sub_dnr_flg
       , crmg_brks_cnt
       , cro_brks_cnt
       , crmg_rnw_cnt
       , cro_rnw_cnt
       , cro_rec_flg
       , crmg_svc_stat_cd
       , cro_svc_stat_cd
       , crmg_curr_mbr_src_cd
       , cro_curr_mbr_src_cd
       , crmg_curr_ord_src_cd
       , cro_curr_ord_src_cd
       , aps_fst_ord_src_cd
       , crmg_fst_ord_src_cd
       , cro_fst_ord_src_cd
       , nc_rpts_fst_ord_src_cd
       , ncbk_fst_ord_src_cd
       , uc_rpts_fst_ord_src_cd
       , ucbk_fst_ord_src_cd
       , aps_lst_ord_src_cd
       , crmg_lst_ord_src_cd
       , cro_lst_ord_src_cd
       , nc_rpts_lst_ord_src_cd
       , ncbk_lst_ord_src_cd
       , uc_rpts_lst_ord_src_cd
       , ucbk_lst_ord_src_cd
       , crmg_prior_mbr_src_cd
       , cro_prior_mbr_src_cd
       , cro_sub_dnr_flg
       , crmg_curr_ord_term
       , cro_curr_ord_term
       , aps_lst_prod_type
       , cro_fst_dnr_dt
       , cro_auto_rnw_flg
       , crmg_auto_rnw_flg
       , cro_non_dnr_flg
       , crmg_curr_mbr_dt
       , cro_curr_mbr_dt
       , aps_ltd_pd_amt
       , crmg_ltd_pd_amt
       , cro_ltd_pd_amt
       , crmg_curr_ord_dt
       , cro_curr_ord_dt
       , aps_fst_ord_dt
       , crmg_fst_ord_dt
       , cro_fst_ord_dt
       , nc_rpts_fst_ord_dt
       , ncbk_fst_ord_dt
       , uc_rpts_fst_ord_dt
       , ucbk_fst_ord_dt
       , cro_lst_canc_bad_dbt_dt
       , crmg_lst_ord_dt
       , cro_lst_ord_dt
       , cro_lst_dnr_ord_dt
       , crmg_canc_cust_cnt
       , cro_canc_cust_cnt
       , aps_ord_cnt
       , crmg_ord_cnt
       , cro_ord_cnt
       , crmg_dm_ord_cnt
       , cro_dm_ord_cnt
       , crmg_em_ord_cnt
       , cro_em_ord_cnt
       , cro_dnr_ord_cnt
       , aps_pd_prod_cnt
       , crmg_lst_sub_ord_role_cd
       , cro_lst_sub_ord_role_cd
       , lst_logn_dt
       , logn_cnt
       , cro_lt_sub_flg
       , crmg_lt_sub_flg
       , carp_actv_flg
       , carp_actv_pd_flg
       , osl_hh_id
)
WITH driver AS
(
		SELECT DISTINCT individual_id 
		FROM prod.agg_digital_order
    UNION
		SELECT individual_id
        FROM   prod.agg_individual_xographic
        WHERE  dc_last_aps_ord_dt IS NOT NULL
	UNION
         SELECT DISTINCT individual_id
         FROM   prod.agg_account_number
         WHERE  acct_prefix = 'PWI' AND
                group_id IN ( '310', '26401', '26402', '26403' )
)
, t2 AS
(
	SELECT 
		individual_id,
		SUBSTRING( MAX( CASE
						  WHEN mag_cd = 'CRMG' THEN NVL( TO_CHAR( end_dt, 'YYYYMMDD' ), '0000000' )
													|| acct_id
						  ELSE NULL
						END ), 9 )  crmg_comb_acct_id,
		SUBSTRING( MAX( CASE
						  WHEN mag_cd = 'CRO' THEN NVL( TO_CHAR( NVL( cro_acct_exp_dt, end_dt ), 'YYYYMMDD' ), '0000000' )
												   || NVL( LPAD( sku_num, 16, '0' ), '0000000000000000' )
												   ||acct_id
						  ELSE NULL
						END ), 25 ) cro_comb_acct_id,
		SUBSTRING( MAX( CASE
						  WHEN mag_cd = 'CARP' THEN NVL( TO_CHAR( end_dt, 'YYYYMMDD' ), '0000000' )
													|| acct_id
						  ELSE NULL
						END ), 9 )  carp_comb_acct_id
	FROM   prod.agg_digital_item
	GROUP  BY individual_id
)
, an AS
(
	SELECT
		individual_id,
		MAX( CASE
			   WHEN acct_prefix = 'PWI' THEN acct_lst_logn_dt
			 END )           lst_logn_dt,
		NVL( SUM( CASE
					WHEN acct_prefix = 'PWI' THEN acct_logn_cnt
				  END ), 0 ) logn_cnt,
		MIN( CASE
			   WHEN acct_prefix = 'PWI' AND
					group_id = '311' THEN 'N'
			   WHEN acct_prefix = 'PWI' AND
					group_id IN ( '310', '26401', '26403' ) THEN 'Y'
			   ELSE NULL
			 END )           cro_lt_sub_flg,
		MIN( CASE
			   WHEN acct_prefix = 'PWI' AND
					group_id = '311' THEN 'N'
			   WHEN acct_prefix = 'PWI' AND
					group_id IN ( '310', '26402', '26403' ) THEN 'Y'
			   ELSE NULL
			 END )           crmg_lt_sub_flg
	 FROM   prod.agg_account_number
	 GROUP  BY individual_id 
)
SELECT ix.individual_id,
       ix.household_id hh_id,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' AND
                   ( ( oli.set_cd IN ( 'C', 'E' ) AND
                       oli.svc_stat_cd = 'A' AND
                       olo.stat_cd = 'C' AND
                       oli.stat_cd = 'C' )  OR
                     ( ( ( oli.svc_stat_cd = 'A' AND
                           olo.stat_cd = 'C' )  OR
                         ( oli.svc_stat_cd = 'C' AND
                           olo.stat_cd = 'R' AND
                           oli.sub_rnw_ind = 'N' AND
                           oli.end_dt > SYSDATE ) ) AND
                       ( NVL( olo.entr_typ_cd, 'Z' ) != 'F' AND
                         oli.tot_amt > 0.01 AND
                         oli.term_mth_cnt > 0 ) ) ) AND
                   oli.acct_id = t2.cro_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 cro_actv_flg,
       MAX( CASE
              WHEN oli.stat_cd = 'C' AND
                   oli.mag_cd IN ( 'NCPR', 'UCPR' ) AND
                   oli.crt_dt >= ADD_MONTHS( SYSDATE, -12 ) THEN 'Y'
              WHEN oli.stat_cd = 'C' AND
                   oli.mag_cd IN ( 'NCBK', 'UCBK' ) AND
                   oli.strt_dt BETWEEN ADD_MONTHS( SYSDATE, -12 ) AND SYSDATE THEN 'Y'
              ELSE 'N'
            END )                                                 aps_actv_pd_flg,
       MAX( CASE
              WHEN oli.stat_cd = 'C' AND
                   oli.mag_cd IN ( 'NCPR', 'UCPR' ) AND
                   oli.crt_dt >= ADD_MONTHS( SYSDATE, -12 ) THEN 'Y'
              ELSE 'N'
            END )                                                 aps_rpts_actv_pd_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'CRMG' AND
                   ( ( oli.svc_stat_cd = 'A' AND
                       olo.stat_cd = 'C' )  OR
                     ( oli.svc_stat_cd = 'C' AND
                       olo.stat_cd = 'R' AND
                       oli.sub_rnw_ind = 'N' AND
                       oli.end_dt > SYSDATE ) ) AND
                   NVL( olo.entr_typ_cd, 'Z' ) != 'F' AND
                   oli.sku_num > 5000000 AND
                   oli.tot_amt > 0.01 AND
                   oli.term_mth_cnt > 0 AND
                   oli.acct_id = t2.crmg_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 crmg_actv_pd_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' AND
                   ( ( oli.svc_stat_cd = 'A' AND
                       olo.stat_cd = 'C' )  OR
                     ( oli.svc_stat_cd = 'C' AND
                       olo.stat_cd = 'R' AND
                       oli.sub_rnw_ind = 'N' AND
                       oli.end_dt > SYSDATE ) ) AND
                   NVL( olo.entr_typ_cd, 'Z' ) != 'F' AND
                   oli.tot_amt > 0.01 AND
                   oli.term_mth_cnt > 0 AND
                   oli.acct_id = t2.cro_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 cro_actv_pd_flg,
       MAX( CASE
              WHEN oli.stat_cd = 'C' AND
                   oli.strt_dt BETWEEN ADD_MONTHS( SYSDATE, -12 ) AND SYSDATE AND
                   oli.mag_cd = 'NCBK' THEN 'Y'
              ELSE 'N'
            END )                                                 ncbk_actv_pd_flg,
       MAX( CASE
              WHEN oli.stat_cd = 'C' AND
                   oli.strt_dt BETWEEN ADD_MONTHS( SYSDATE, -12 ) AND SYSDATE AND
                   oli.mag_cd = 'UCBK' THEN 'Y'
              ELSE 'N'
            END )                                                 ucbk_actv_pd_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'CRMG' AND
                   ( ( oli.svc_stat_cd = 'A' AND
                       olo.stat_cd = 'C' )  OR
                     ( oli.svc_stat_cd = 'C' AND
                       olo.stat_cd = 'R' AND
                       oli.sub_rnw_ind = 'N' AND
                       oli.end_dt > SYSDATE ) ) AND
                   ( ( sku.selection = 'A' AND
                       sku.product = 'CRMG' )  OR
                     ( sku.selection = 'X' AND
                       sku.product = 'CRMG' AND
                       oli.term_mth_cnt > 1 ) ) AND
                   oli.sku_num > 5000000 AND
                   oli.tot_amt > 0.01 AND
                   oli.term_mth_cnt > 0 AND
                   NVL( olo.entr_typ_cd, 'Z' ) != 'F' AND
                   oli.acct_id = t2.crmg_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 crmg_annual_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' AND
                   ( ( oli.svc_stat_cd = 'A' AND
                       olo.stat_cd = 'C' )  OR
                     ( oli.svc_stat_cd = 'C' AND
                       olo.stat_cd = 'R' AND
                       oli.sub_rnw_ind = 'N' AND
                       oli.end_dt > SYSDATE ) ) AND
                   ( ( sku.selection = 'A' AND
                       sku.product = 'CRO' )  OR
                     ( oli.sku_num IS NULL AND
                       oli.term_mth_cnt > 1 )  OR
                     ( sku.selection = 'X' AND
                       sku.product = 'CRO' AND
                       oli.term_mth_cnt > 1 ) ) AND
                   oli.tot_amt > 0.01 AND
                   oli.term_mth_cnt > 0 AND
                   NVL( olo.entr_typ_cd, 'Z' ) != 'F' AND
                   oli.acct_id = t2.cro_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 cro_annual_flg,
       CASE
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRMG' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                              || oli.crd_stat_cd
                                ELSE NULL
                              END ), 9 ) = 'F' THEN 'Y'
         ELSE 'N'
       END                                                        crmg_canc_bad_dbt_flg,
       CASE
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRO' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                             || oli.crd_stat_cd
                                ELSE NULL
                              END ), 9 ) = 'F' THEN 'Y'
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRO' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                             || oli.svc_stat_cd
                                                             || oli.canc_rsn_cd
                                ELSE NULL
                              END ), 9 ) IN ( 'C50', 'C06' ) THEN 'Y'
         ELSE 'N'
       END                                                        cro_canc_bad_dbt_flg,
       CASE
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRMG' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                              || oli.stat_cd
                                ELSE NULL
                              END ), 9, 1 ) = 'R' THEN 'Y'
         ELSE 'N'
       END                                                        crmg_canc_cust_flg,
       CASE
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRO' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                             || oli.stat_cd
                                ELSE NULL
                              END ), 9, 1 ) = 'R'  OR
              ( SUBSTRING( MAX( CASE
                                  WHEN oli.mag_cd = 'CRO' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                               || oli.svc_stat_cd
                                                               || oli.canc_rsn_cd
                                  ELSE NULL
                                END ), 9, 1 ) = 'C' AND
                SUBSTRING( MAX( CASE
                                  WHEN oli.mag_cd = 'CRO' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                               || oli.svc_stat_cd
                                                               || NVL( oli.canc_rsn_cd, 'xx' )
                                  ELSE NULL
                                END ), 10, 2 ) NOT IN ( '50', '06' ) ) THEN 'Y'
         ELSE 'N'
       END                                                        cro_canc_cust_flg,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'CRMG' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                       || oli.crd_stat_cd
                         ELSE NULL
                       END ), 9 )                                 crmg_cr_stat_cd,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'CRO' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                      || oli.crd_stat_cd
                         ELSE NULL
                       END ), 9 )                                 cro_cr_stat_cd,
       MAX( CASE
              WHEN oli.mag_cd = 'CRMG' THEN oli.end_dt
              ELSE NULL
            END )                                                 crmg_exp_dt,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' THEN NVL( oli.cro_acct_exp_dt, oli.end_dt )
              ELSE NULL
            END )                                                 cro_exp_dt,
       TO_DATE( SUBSTRING( MAX( CASE
                                  WHEN oli.mag_cd = 'CRMG' AND
                                       oli.crd_stat_cd = 'F' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                                  || TO_CHAR( oli.canc_dt, 'YYYYMMDD' )
                                  ELSE NULL
                                END ), 9 ), 'YYYYMMDD' )          crmg_lst_canc_bad_dbt_dt,
       TO_DATE( SUBSTRING( MAX( CASE
                                  WHEN oli.mag_cd = 'CRMG' AND
                                       oli.stat_cd = 'R' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                              || TO_CHAR( oli.canc_dt, 'YYYYMMDD' )
                                  ELSE NULL
                                END ), 9 ), 'YYYYMMDD' )          crmg_lst_canc_cust_dt,
       TO_DATE( SUBSTRING( MAX( CASE
                                  WHEN oli.mag_cd = 'CRO' AND
                                       ( ( oli.stat_cd = 'R' )  OR
                                         ( oli.svc_stat_cd = 'C' AND
                                           NVL( oli.canc_rsn_cd, '00' ) NOT IN ( '50', '06' ) ) ) THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                                                                       || TO_CHAR( NVL( oli.canc_dt, oli.cro_acct_exp_dt ), 'YYYYMMDD' )
                                  ELSE NULL
                                END ), 9 ), 'YYYYMMDD' )          cro_lst_canc_cust_dt,
       MAX( CASE
              WHEN oli.mag_cd IN ( 'NCBK', 'UCBK', 'NCPR', 'UCPR' ) THEN NULLIF( GREATEST( NVL( ixo.dc_last_aps_ord_dt, TO_DATE( '19000101', 'YYYYMMDD' ) ), NVL( oli.crt_dt, TO_DATE( '19000101', 'YYYYMMDD' ) ) ), TO_DATE( '19000101', 'YYYYMMDD' ) )
              ELSE NULLIF( NVL( ixo.dc_last_aps_ord_dt, TO_DATE( '19000101', 'YYYYMMDD' ) ), TO_DATE( '19000101', 'YYYYMMDD' ) )
            END )                                                 aps_lst_ord_dt,
       MAX( CASE
              WHEN oli.mag_cd = 'NCPR' THEN oli.crt_dt
              ELSE NULL
            END )                                                 nc_rpts_lst_ord_dt,
       MAX( CASE
              WHEN oli.mag_cd = 'NCBK' THEN oli.crt_dt
              ELSE NULL
            END )                                                 ncbk_lst_ord_dt,
       MAX( CASE
              WHEN oli.mag_cd = 'UCPR' THEN oli.crt_dt
              ELSE NULL
            END )                                                 uc_rpts_lst_ord_dt,
       MAX( CASE
              WHEN oli.mag_cd = 'UCBK' THEN oli.crt_dt
              ELSE NULL
            END )                                                 ucbk_lst_ord_dt,
       MAX( CASE
              WHEN oli.mag_cd = 'CRMG' THEN oli.pmt_dt
              ELSE NULL
            END )                                                 crmg_lst_pmt_dt,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' THEN oli.pmt_dt
              ELSE NULL
            END )                                                 cro_lst_pmt_dt,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'NCPR' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                       || oli.shp_meth_cd
                         ELSE NULL
                       END ), 15 )                                nc_rpts_lst_del_chnl_cd,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'UCPR' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                       || oli.shp_meth_cd
                         ELSE NULL
                       END ), 15 )                                uc_rpts_lst_del_chnl_cd,
       MAX( CASE
              WHEN ixo.dc_last_aps_ord_dt IS NOT NULL THEN 'Y'
              WHEN oli.mag_cd IN ( 'NCPR', 'UCPR', 'NCBK', 'UCBK' ) THEN 'Y'
              ELSE 'N'
            END )                                                 aps_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'CRMG' THEN 'Y'
              ELSE 'N'
            END )                                                 crmg_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' THEN 'Y'
              ELSE 'N'
            END )                                                 cro_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'NCPR' THEN 'Y'
              ELSE 'N'
            END )                                                 nc_rpts_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'NCBK' THEN 'Y'
              ELSE 'N'
            END )                                                 ncbk_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'UCPR' THEN 'Y'
              ELSE 'N'
            END )                                                 uc_rpts_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'UCBK' THEN 'Y'
              ELSE 'N'
            END )                                                 ucbk_flg,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'CRMG' AND
                              NVL( oli.set_cd, 'Z' ) NOT IN ( 'B', 'D' ) AND
                              oli.strt_dt > NVL( t1.last_crmg_brk_dt, TO_DATE( '00010101', 'YYYYMMDD' ) ) THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '99999999' )
                                                                                                               || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 9 )                                 crmg_curr_mbr_keycode,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'CRO' AND
                              oli.set_cd NOT IN ( 'B', 'D' ) AND
                              NVL( TO_CHAR( oli.strt_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                              || NVL( TO_CHAR( oli.end_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                              || LPAD( oli.itm_id, 21 ) > NVL( t1.last_cro_acct_brk_dt, '0000000000000000000000000000' ) AND
                              oli.acct_id = t1.cro_curr_mbr_dt_acct_id THEN NVL( TO_CHAR( oli.strt_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                                                                            || NVL( TO_CHAR( oli.end_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                                                                            || LPAD( oli.itm_id, 21 )
                                                                            || NVL( oli.ext_keycd, oli.int_keycd )
                       END ), 50 )                                cro_curr_mbr_keycode,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'CRMG' AND
                              oli.svc_stat_cd = 'A' AND
                              oli.sku_num > 5000000 AND
                              olo.stat_cd = 'C' AND
                              oli.acct_id = t2.crmg_comb_acct_id THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00000000' )
                                                                      || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 9 )                                 crmg_curr_ord_keycode,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'CRO' AND
                              oli.svc_stat_cd = 'A' AND
                              olo.stat_cd = 'C' AND
                              oli.acct_id = t2.cro_comb_acct_id THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00000000' )
                                                                     || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 9 )                                 cro_curr_ord_keycode,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'CRMG' AND
                              NVL( oli.set_cd, 'Z' ) NOT IN ( 'B', 'D' ) THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '99999999' )
                                                                              || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 9 )                                 crmg_fst_mbr_keycode,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'CRO' AND
                              oli.set_cd NOT IN ( 'B', 'D' ) THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '99999999' )
                                                                  || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 9 )                                 cro_fst_mbr_keycode,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd IN ( 'NCPR', 'UCPR', 'NCBK', 'UCBK' ) THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '99999999010101' )
                                                                                    || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 15 )                                aps_fst_ord_keycode,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'NCPR' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '99999999010101' )
                                                       || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 15 )                                nc_rpts_fst_ord_keycode,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'NCBK' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '99999999010101' )
                                                       || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 15 )                                ncbk_fst_ord_keycode,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'UCPR' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '99999999010101' )
                                                       || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 15 )                                uc_rpts_fst_ord_keycode,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'UCBK' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '99999999010101' )
                                                       || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 15 )                                ucbk_fst_ord_keycode,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd IN ( 'NCPR', 'UCPR', 'NCBK', 'UCBK' ) THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                                                    || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 15 )                                aps_lst_ord_keycode,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'CRMG' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                       || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 9 )                                 crmg_lst_ord_keycode,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'CRO' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                      || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 9 )                                 cro_lst_ord_keycode,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'NCPR' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                       || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 15 )                                nc_rpts_lst_ord_keycode,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'NCBK' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                       || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 15 )                                ncbk_lst_ord_keycode,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'UCPR' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                       || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 15 )                                uc_rpts_lst_ord_keycode,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'UCBK' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                       || NVL( oli.ext_keycd, oli.int_keycd )
                         ELSE NULL
                       END ), 15 )                                ucbk_lst_ord_keycode,
       TO_NUMBER( SUBSTRING( MAX( CASE
                                    WHEN oli.mag_cd = 'CRMG' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                                  || oli.tot_gross_amt
                                    ELSE NULL
                                  END ), 9 ), '99999999' )                    crmg_lst_sub_amt,
       TO_NUMBER( SUBSTRING( MAX( CASE
                                    WHEN oli.mag_cd = 'CRO' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                                 || oli.tot_gross_amt
                                    ELSE NULL
                                  END ), 9 ), '99999999' )                    cro_lst_sub_amt,
       MAX( CASE
              WHEN oli.mag_cd = 'CRMG' AND
                   ( ( oli.svc_stat_cd = 'A' AND
                       olo.stat_cd = 'C' )  OR
                     ( oli.svc_stat_cd = 'C' AND
                       olo.stat_cd = 'R' AND
                       oli.sub_rnw_ind = 'N' AND
                       oli.end_dt > SYSDATE ) ) AND
                   sku.product = 'CRMG' AND
                   sku.selection = 'M' AND
                   NVL( olo.entr_typ_cd, 'Z' ) != 'F' AND
                   oli.sku_num > 5000000 AND
                   oli.tot_amt > 0.01 AND
                   oli.term_mth_cnt > 0 AND
                   oli.acct_id = t2.crmg_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 crmg_monthly_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' AND
                   ( ( oli.svc_stat_cd = 'A' AND
                       olo.stat_cd = 'C' )  OR
                     ( oli.svc_stat_cd = 'C' AND
                       olo.stat_cd = 'R' AND
                       oli.sub_rnw_ind = 'N' AND
                       oli.end_dt > SYSDATE ) ) AND
                   ( ( sku.selection = 'M' AND
                       sku.product = 'CRO' )  OR
                     ( oli.sku_num IS NULL AND
                       oli.term_mth_cnt = 1 ) ) AND
                   NVL( olo.entr_typ_cd, 'Z' ) != 'F' AND
                   oli.tot_amt > 0.01 AND
                   oli.term_mth_cnt > 0 AND
                   oli.acct_id = t2.cro_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 cro_monthly_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' AND
                   oli.acct_id = t2.cro_comb_acct_id AND
                   oli.svc_stat_cd = 'D' AND
                   oli.set_cd IN ( 'B', 'D' ) AND
                   oli.crt_dt > ADD_MONTHS( SYSDATE, -12 ) THEN 'Y'
              ELSE 'N'
            END )                                                 cro_non_sub_dnr_flg,
       MAX( t1.crmg_brks_cnt )                                    crmg_brks_cnt,
       MAX( t1.cro_brks_cnt )                                     cro_brks_cnt,
       TRUNC( COUNT( CASE
                       WHEN oli.mag_cd = 'CRMG' AND
                            NVL( oli.set_cd, 'Z' ) NOT IN ( 'B', 'D' ) AND
                            oli.strt_dt > NVL( t1.last_crmg_brk_dt, TO_DATE( '00010101', 'YYYYMMDD' ) ) THEN 'x'
                       ELSE NULL
                     END ) - .1 )                                 crmg_rnw_cnt,
       TRUNC( COUNT( CASE
                       WHEN oli.mag_cd = 'CRO' AND
                            oli.set_cd NOT IN ( 'B', 'D' ) AND
                            NVL( oli.crd_stat_cd, 'x' ) != 'F' AND
                            NVL( oli.canc_rsn_cd, '00' ) NOT IN ( '50', '06' ) AND
                            NVL( TO_CHAR( oli.strt_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                            || NVL( TO_CHAR( oli.end_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                            || LPAD( oli.itm_id, 21 ) > NVL( t1.last_cro_acct_brk_dt, '0000000000000000000000000000' ) AND
                            oli.acct_id = t1.cro_curr_mbr_dt_acct_id THEN 'x'
                       ELSE NULL
                     END ) - .1 )                                 cro_rnw_cnt,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' AND
                   oli.svc_stat_cd = 'A' AND
                   oli.set_cd IN ( 'C', 'E' ) AND
                   oli.stat_cd = 'C' AND
                   olo.stat_cd = 'C' AND
                   oli.acct_id = t2.cro_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 cro_rec_flg,
       CASE
         WHEN ( ( MAX( NVL( an.crmg_lt_sub_flg, 'N' ) ) = 'Y' )  OR
                ( MAX( CASE
                         WHEN oli.mag_cd = 'CRMG' AND
                              oli.svc_stat_cd = 'A' AND
                              oli.sku_num > 5000000 AND
                              olo.stat_cd = 'C' AND
                              oli.acct_id = t2.crmg_comb_acct_id THEN '1'
                         ELSE '0'
                       END ) = '1' ) ) THEN 'A'
         ELSE SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRMG' AND
                                     oli.acct_id = t2.crmg_comb_acct_id THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00000000' )
                                                                             || DECODE( oli.svc_stat_cd, 'A', 'C',
                                                                                                         oli.svc_stat_cd )
                              END ), 9 )
       END                                                        crmg_svc_stat_cd,
       CASE
         WHEN ( ( MAX( NVL( an.cro_lt_sub_flg, 'N' ) ) = 'Y' )  OR
                ( MAX( CASE
                         WHEN oli.mag_cd = 'CRO' AND
                              oli.svc_stat_cd = 'A' AND
                              olo.stat_cd = 'C' AND
                              oli.acct_id = t2.cro_comb_acct_id THEN '1'
                         ELSE '0'
                       END ) = '1' ) ) THEN 'A'
         ELSE SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRO' AND
                                     oli.acct_id = t2.cro_comb_acct_id THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00000000' )
                                                                            || DECODE( oli.svc_stat_cd, 'A', 'C',
                                                                                                        oli.svc_stat_cd )
                              END ), 9 )
       END                                                        cro_svc_stat_cd,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'CRMG' AND
                              NVL( oli.set_cd, 'Z' ) NOT IN ( 'B', 'D' ) AND
                              oli.strt_dt > NVL( t1.last_crmg_brk_dt, TO_DATE( '00010101', 'YYYYMMDD' ) ) THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '99999999' )
                                                                                                               || oli.sub_src_cd
                         ELSE NULL
                       END ), 9 )                                 crmg_curr_mbr_src_cd,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'CRO' AND
                              oli.set_cd NOT IN ( 'B', 'D' ) AND
                              NVL( TO_CHAR( oli.strt_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                              || NVL( TO_CHAR( oli.end_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                              || LPAD( oli.itm_id, 21 ) > NVL( t1.last_cro_acct_brk_dt, '0000000000000000000000000000' ) AND
                              oli.acct_id = t1.cro_curr_mbr_dt_acct_id THEN NVL( TO_CHAR( oli.strt_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                                                                            || NVL( TO_CHAR( oli.end_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                                                                            || LPAD( oli.itm_id, 21 )
                                                                            || oli.sub_src_cd
                         ELSE NULL
                       END ), 50 )                                cro_curr_mbr_src_cd,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'CRMG' AND
                              oli.svc_stat_cd = 'A' AND
                              oli.sku_num > 5000000 AND
                              olo.stat_cd = 'C' AND
                              oli.acct_id = t2.crmg_comb_acct_id THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00000000' )
                                                                      || oli.sub_src_cd
                         ELSE NULL
                       END ), 9 )                                 crmg_curr_ord_src_cd,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'CRO' AND
                              oli.svc_stat_cd = 'A' AND
                              olo.stat_cd = 'C' AND
                              oli.acct_id = t2.cro_comb_acct_id THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00000000' )
                                                                     || oli.sub_src_cd
                         ELSE NULL
                       END ), 9 )                                 cro_curr_ord_src_cd,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd IN ( 'NCPR', 'UCPR', 'NCBK', 'UCBK' ) THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '99999999010101' )
                                                                                    || oli.aps_src_cd
                         ELSE NULL
                       END ), 15 )                                aps_fst_ord_src_cd,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'CRMG' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '99999999' )
                                                       || oli.sub_src_cd
                         ELSE NULL
                       END ), 9 )                                 crmg_fst_ord_src_cd,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'CRO' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '99999999' )
                                                      || oli.sub_src_cd
                         ELSE NULL
                       END ), 9 )                                 cro_fst_ord_src_cd,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'NCPR' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '99999999010101' )
                                                       || oli.aps_src_cd
                         ELSE NULL
                       END ), 15 )                                nc_rpts_fst_ord_src_cd,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'NCBK' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '99999999010101' )
                                                       || oli.aps_src_cd
                         ELSE NULL
                       END ), 15 )                                ncbk_fst_ord_src_cd,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'UCPR' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '99999999010101' )
                                                       || oli.aps_src_cd
                         ELSE NULL
                       END ), 15 )                                uc_rpts_fst_ord_src_cd,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'UCBK' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '99999999010101' )
                                                       || oli.aps_src_cd
                         ELSE NULL
                       END ), 15 )                                ucbk_fst_ord_src_cd,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd IN ( 'NCPR', 'UCPR', 'NCBK', 'UCBK' ) THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                                                    || oli.aps_src_cd
                         ELSE NULL
                       END ), 15 )                                aps_lst_ord_src_cd,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'CRMG' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                       || oli.sub_src_cd
                         ELSE NULL
                       END ), 9 )                                 crmg_lst_ord_src_cd,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'CRO' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00010101' )
                                                      || oli.sub_src_cd
                         ELSE NULL
                       END ), 9 )                                 cro_lst_ord_src_cd,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'NCPR' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                       || oli.aps_src_cd
                         ELSE NULL
                       END ), 15 )                                nc_rpts_lst_ord_src_cd,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'NCBK' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                       || oli.aps_src_cd
                         ELSE NULL
                       END ), 15 )                                ncbk_lst_ord_src_cd,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'UCPR' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                       || oli.aps_src_cd
                         ELSE NULL
                       END ), 15 )                                uc_rpts_lst_ord_src_cd,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd = 'UCBK' THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                       || oli.aps_src_cd
                         ELSE NULL
                       END ), 15 )                                ucbk_lst_ord_src_cd,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'CRMG' AND
                              NVL( oli.set_cd, 'Z' ) NOT IN ( 'B', 'D' ) AND
                              oli.crt_dt <= t1.last_crmg_brk_dt AND
                              oli.crt_dt > NVL( t1.prior_crmg_brk_dt, TO_DATE( '00010101', 'YYYYMMDD' ) ) THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '99999999' )
                                                                                                               || oli.sub_src_cd
                         ELSE NULL
                       END ), 9 )                                 crmg_prior_mbr_src_cd,
       SUBSTRING( MIN( CASE
                         WHEN oli.mag_cd = 'CRO' AND
                              oli.set_cd NOT IN ( 'B', 'D' ) AND
                              oli.crt_dt <= t1.last_cro_brk_dt AND
                              oli.crt_dt > NVL( t1.prior_cro_brk_dt, TO_DATE( '00010101', 'YYYYMMDD' ) ) THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '99999999' )
                                                                                                              || oli.sub_src_cd
                         ELSE NULL
                       END ), 9 )                                 cro_prior_mbr_src_cd,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' AND
                   oli.svc_stat_cd != 'D' AND
                   oli.set_cd IN ( 'B', 'D' ) AND
                   oli.crt_dt > ADD_MONTHS( SYSDATE, -12 ) AND
                   oli.acct_id = t2.cro_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 cro_sub_dnr_flg,
       TO_NUMBER( SUBSTRING( MAX( CASE
                                    WHEN oli.mag_cd = 'CRMG' AND
                                         oli.stat_cd = 'C' AND
                                         oli.svc_stat_cd = 'A' AND
                                         oli.sku_num > 5000000 AND
                                         olo.stat_cd = 'C' AND
                                         oli.acct_id = t2.crmg_comb_acct_id THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00000000' )
                                                                                 || oli.term_mth_cnt
                                  END ), 9 ), '99999999' )                    crmg_curr_ord_term,
       TO_NUMBER( SUBSTRING( MAX( CASE
                                    WHEN oli.mag_cd = 'CRO' AND
                                         oli.stat_cd = 'C' AND
                                         oli.svc_stat_cd = 'A' AND
                                         olo.stat_cd = 'C' AND
                                         oli.acct_id = t2.cro_comb_acct_id THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDD' ), '00000000' )
                                                                                || oli.term_mth_cnt
                                  END ), 9 ), '99999999' )                    cro_curr_ord_term,
       SUBSTRING( MAX( CASE
                         WHEN oli.mag_cd IN ( 'NCPR', 'UCPR', 'NCBK', 'UCBK' ) THEN NVL( TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' ), '00010101010101' )
                                                                                    || oli.mag_cd
                         WHEN NVL( oli.mag_cd, 'AAAA' ) NOT IN ( 'NCPR', 'UCPR', 'NCBK', 'UCBK' ) AND
                              ixo.dc_last_aps_ord_dt IS NOT NULL THEN '00010101010101'
                                                                      || 'REPORT'
                         ELSE NULL
                       END ), 15 )                                aps_lst_prod_type,
       MIN( CASE
              WHEN oli.mag_cd = 'CRO' AND
                   oli.set_cd IN ( 'B', 'D' ) THEN oli.crt_dt
              ELSE NULL
            END )                                                 cro_fst_dnr_dt,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' AND
                   oli.svc_stat_cd = 'A' AND
                   oli.sub_rnw_ind = 'Y' AND
                   olo.stat_cd = 'C' AND
                   oli.acct_id = t2.cro_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 cro_auto_rnw_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'CRMG' AND
                   oli.svc_stat_cd = 'A' AND
                   oli.sub_rnw_ind = 'Y' AND
                   oli.sku_num > 5000000 AND
                   olo.stat_cd = 'C' AND
                   oli.acct_id = t2.crmg_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 crmg_auto_rnw_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' AND
                   oli.set_cd IN ( 'A', 'C', 'E' ) THEN 'Y'
              ELSE 'N'
            END )                                                 cro_non_dnr_flg,
       MIN( CASE
              WHEN oli.mag_cd = 'CRMG' AND
                   NVL( oli.set_cd, 'Z' ) NOT IN ( 'B', 'D' ) AND
                   oli.strt_dt > NVL( t1.last_crmg_brk_dt, TO_DATE( '00010101', 'YYYYMMDD' ) ) THEN oli.crt_dt
              ELSE NULL
            END )                                                 crmg_curr_mbr_dt,
       TO_DATE( SUBSTRING( MIN( CASE
                                  WHEN oli.mag_cd = 'CRO' AND
                                       NVL( oli.set_cd, 'Z' ) NOT IN ( 'B', 'D' ) AND
                                       NVL( TO_CHAR( oli.strt_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                                       || NVL( TO_CHAR( oli.end_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                                       || LPAD( oli.itm_id, 21 ) > NVL( t1.last_cro_acct_brk_dt, '0000000000000000000000000000' ) AND
                                       oli.acct_id = t1.cro_curr_mbr_dt_acct_id THEN NVL( TO_CHAR( oli.strt_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                                                                                     || NVL( TO_CHAR( oli.end_dt, 'YYYYMMDDHH24MISS' ), '00000000000000' )
                                                                                     || LPAD( oli.itm_id, 21 )
                                                                                     || TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' )
                                  ELSE NULL
                                END ), 50 ), 'YYYYMMDDHH24MISS' ) cro_curr_mbr_dt,
       NVL( SUM( CASE
                   WHEN ( oli.mag_cd IN ( 'NCBK', 'UCBK', 'NCPR', 'UCPR' ) AND
                          ( ( oli.sku_num < '5000000' )  OR
                            ( oli.sku_num > '5000000' AND
                              oli.stat_cd = 'C' ) ) ) THEN oli.tot_amt
                 END ), 0 )                                       aps_ltd_pd_amt,
       NVL( SUM( CASE
                   WHEN ( oli.mag_cd = 'CRMG' AND
                          ( ( oli.sku_num < '5000000' )  OR
                            ( oli.sku_num > '5000000' AND
                              oli.stat_cd = 'C' ) ) ) THEN oli.tot_amt
                 END ), 0 )                                       crmg_ltd_pd_amt,
       NVL( SUM( CASE
                   WHEN ( oli.mag_cd = 'CRO' AND
                          ( ( NVL( oli.sku_num, '1' ) < '5000000' )  OR
                            ( oli.sku_num > '5000000' AND
                              oli.stat_cd = 'C' ) ) ) THEN oli.tot_amt
                 END ), 0 )                                       cro_ltd_pd_amt,
       MAX( CASE
              WHEN oli.svc_stat_cd = 'A' AND
                   NVL( oli.set_cd, 'x' ) NOT IN ( 'B', 'D' ) AND
                   oli.sku_num > 5000000 AND
                   olo.stat_cd = 'C' AND
                   oli.mag_cd = 'CRMG' THEN oli.crt_dt
            END )                                                 crmg_curr_ord_dt,
       MAX( CASE
              WHEN oli.svc_stat_cd = 'A' AND
                   NVL( oli.set_cd, 'x' ) NOT IN ( 'B', 'D' ) AND
                   olo.stat_cd = 'C' AND
                   oli.mag_cd = 'CRO' THEN oli.crt_dt
            END )                                                 cro_curr_ord_dt,
       MIN( CASE
              WHEN oli.mag_cd IN ( 'NCPR', 'UCPR', 'NCBK', 'UCBK' ) THEN NULLIF( LEAST( NVL( ixo.dc_last_aps_ord_dt, TO_DATE( '99991231', 'YYYYMMDD' ) ), NVL( oli.crt_dt, TO_DATE( '99991231', 'YYYYMMDD' ) ) ), TO_DATE( '99991231', 'YYYYMMDD' ) )
              ELSE NULLIF( NVL( ixo.dc_last_aps_ord_dt, TO_DATE( '99991231', 'YYYYMMDD' ) ), TO_DATE( '99991231', 'YYYYMMDD' ) )
            END )                                                 aps_fst_ord_dt,
       MIN( CASE
              WHEN oli.mag_cd = 'CRMG' AND
                   NVL( oli.set_cd, 'X' ) NOT IN ( 'C', 'E' ) THEN oli.crt_dt
            END )                                                 crmg_fst_ord_dt,
       MIN( CASE
              WHEN oli.mag_cd = 'CRO' THEN oli.crt_dt
            END )                                                 cro_fst_ord_dt,
       MIN( CASE
              WHEN oli.mag_cd = 'NCPR' THEN oli.crt_dt
            END )                                                 nc_rpts_fst_ord_dt,
       MIN( CASE
              WHEN oli.mag_cd = 'NCBK' THEN oli.crt_dt
            END )                                                 ncbk_fst_ord_dt,
       MIN( CASE
              WHEN oli.mag_cd = 'UCPR' THEN oli.crt_dt
            END )                                                 uc_rpts_fst_ord_dt,
       MIN( CASE
              WHEN oli.mag_cd = 'UCBK' THEN oli.crt_dt
            END )                                                 ucbk_fst_ord_dt,
       MAX( CASE
              WHEN ( oli.crd_stat_cd = 'F'  OR
                     ( oli.svc_stat_cd = 'C' AND
                       oli.canc_rsn_cd IN ( '50', '06' ) ) ) AND
                   oli.mag_cd = 'CRO' THEN oli.canc_dt
            END )                                                 cro_lst_canc_bad_dbt_dt,
       MAX( CASE
              WHEN NVL( set_cd, 'x' ) NOT IN ( 'C', 'E' ) AND
                   oli.mag_cd = 'CRMG' THEN oli.crt_dt
            END )                                                 crmg_lst_ord_dt,
       MAX( CASE
              WHEN oli.mag_cd = 'CRO' THEN oli.crt_dt
            END )                                                 cro_lst_ord_dt,
       MAX( CASE
              WHEN oli.set_cd IN ( 'B', 'D' ) AND
                   oli.mag_cd = 'CRO' THEN oli.crt_dt
            END )                                                 cro_lst_dnr_ord_dt,
       COUNT( DISTINCT CASE
                         WHEN oli.mag_cd = 'CRMG' AND
                              oli.canc_flg = 'Y' THEN oli.ord_id
                       END )                                      crmg_canc_cust_cnt,
       COUNT( DISTINCT CASE
                         WHEN oli.mag_cd = 'CRO' AND
                              oli.canc_flg = 'Y' THEN oli.ord_id
                       END )                                      cro_canc_cust_cnt,
       COUNT( DISTINCT CASE
                         WHEN oli.mag_cd IN ( 'NCBK', 'UCBK', 'NCPR', 'UCPR' ) THEN olo.individual_id
                                                                                    || olo.acct_id
                                                                                    || olo.ord_id
                       END )                                      aps_ord_cnt,
       COUNT( DISTINCT CASE
                         WHEN oli.mag_cd = 'CRMG' THEN oli.ord_id
                       END )                                      crmg_ord_cnt,
       COUNT( CASE
                WHEN oli.mag_cd = 'CRO' AND
                     NVL( oli.set_cd, 'x' ) NOT IN ( 'C', 'E' ) THEN 1
              END )                                               cro_ord_cnt,
       COUNT( DISTINCT CASE
                         WHEN oli.mag_cd = 'CRMG' AND
                              oli.sub_src_cd = 'D' THEN oli.ord_id
                       END )                                      crmg_dm_ord_cnt,
       COUNT( CASE
                WHEN oli.mag_cd = 'CRO' AND
                     NVL( oli.set_cd, 'x' ) NOT IN ( 'C', 'E' ) AND
                     oli.sub_src_cd = 'D' THEN 1
              END )                                               cro_dm_ord_cnt,
       COUNT( DISTINCT CASE
                         WHEN oli.mag_cd = 'CRMG' AND
                              oli.sub_src_cd IN ( 'E', 'G', 'K', 'N', 'U' ) THEN oli.ord_id
                       END )                                      crmg_em_ord_cnt,
       COUNT( CASE
                WHEN oli.mag_cd = 'CRO' AND
                     NVL( oli.set_cd, 'x' ) NOT IN ( 'C', 'E' ) AND
                     oli.sub_src_cd IN ( 'E', 'G', 'K', 'N', 'U' ) THEN 1
              END )                                               cro_em_ord_cnt,
       COUNT( DISTINCT CASE
                         WHEN oli.mag_cd = 'CRO' AND
                              oli.set_cd IN ( 'B', 'D' ) THEN oli.ord_id
                       END )                                      cro_dnr_ord_cnt,
       COUNT( CASE
                WHEN oli.mag_cd IN ( 'NCBK', 'UCBK', 'NCPR', 'UCPR' ) AND
                     oli.stat_cd = 'C' THEN 1
              END )                                               aps_pd_prod_cnt,
       CASE
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRMG' THEN TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' )
                                                              || oli.set_cd
                              END ), 15 ) IN ( 'C', 'E' ) THEN 'R'
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRMG' THEN TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' )
                                                              || NVL( oli.set_cd, 'A' )
                              END ), 15 ) = 'A' THEN 'O'
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRMG' THEN TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' )
                                                              || oli.set_cd
                                                              || oli.svc_stat_cd
                              END ), 15 ) IN ( 'BD', 'DD' ) THEN 'N'
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRMG' THEN TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' )
                                                              || oli.set_cd
                              END ), 15 ) IN ( 'B', 'D' ) THEN 'D'
       END                                                        crmg_lst_sub_ord_role_cd,
       CASE
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRO' THEN TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' )
                                                             || oli.set_cd
                              END ), 15 ) IN ( 'C', 'E' ) THEN 'R'
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRO' THEN TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' )
                                                             || oli.set_cd
                              END ), 15 ) = 'A' THEN 'O'
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRO' THEN TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' )
                                                             || oli.set_cd
                                                             || oli.svc_stat_cd
                              END ), 15 ) IN ( 'BD', 'DD' ) THEN 'N'
         WHEN SUBSTRING( MAX( CASE
                                WHEN oli.mag_cd = 'CRO' THEN TO_CHAR( oli.crt_dt, 'YYYYMMDDHH24MISS' )
                                                             || oli.set_cd
                              END ), 15 ) IN ( 'B', 'D' ) THEN 'D'
       END                                                        cro_lst_sub_ord_role_cd,
       MAX( an.lst_logn_dt )                                      lst_logn_dt,
       MAX( NVL( an.logn_cnt, 0 ) )                               logn_cnt,
       MAX( NVL( an.cro_lt_sub_flg, 'N' ) )                       cro_lt_sub_flg,
       MAX( NVL( an.crmg_lt_sub_flg, 'N' ) )                      crmg_lt_sub_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'CARP' AND
                   ( ( oli.set_cd IN ( 'C', 'E' ) AND
                       oli.svc_stat_cd = 'A' AND
                       olo.stat_cd = 'C' AND
                       oli.stat_cd = 'C' )  OR
                     ( ( ( oli.svc_stat_cd = 'A' AND
                           olo.stat_cd = 'C' )  OR
                         ( oli.svc_stat_cd = 'C' AND
                           olo.stat_cd = 'R' AND
                           oli.sub_rnw_ind = 'N' AND
                           oli.end_dt > SYSDATE ) ) AND
                       ( NVL( olo.entr_typ_cd, 'Z' ) != 'F' AND
                         oli.tot_amt > 0.01 AND
                         oli.term_mth_cnt > 0 ) ) ) AND
                   oli.acct_id = t2.carp_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 carp_actv_flg,
       MAX( CASE
              WHEN oli.mag_cd = 'CARP' AND
                   ( ( oli.svc_stat_cd = 'A' AND
                       olo.stat_cd = 'C' )  OR
                     ( oli.svc_stat_cd = 'C' AND
                       olo.stat_cd = 'R' AND
                       oli.sub_rnw_ind = 'N' AND
                       oli.end_dt > SYSDATE ) ) AND
                   NVL( olo.entr_typ_cd, 'Z' ) != 'F' AND
                   oli.tot_amt > 0.01 AND
                   oli.term_mth_cnt > 0 AND
                   oli.acct_id = t2.carp_comb_acct_id THEN 'Y'
              ELSE 'N'
            END )                                                 carp_actv_pd_flg,
       ix.household_id osl_hh_id
FROM   driver
       LEFT JOIN prod.individual ix ON driver.individual_id = ix.individual_id
       LEFT JOIN prod.agg_digital_order olo ON ix.individual_id = olo.individual_id
       LEFT JOIN prod.agg_digital_item oli ON olo.ord_id = oli.ord_id AND olo.acct_id = oli.acct_id
       LEFT JOIN prod.agg_tmp_onl_sum_temp1 t1 ON t1.individual_id = oli.individual_id AND oli.acct_id = t1.acct_id
       LEFT JOIN prod.sku_lkup sku ON oli.sku_num = sku.sku_num
       LEFT JOIN t2 ON ix.individual_id = t2.individual_id
       LEFT JOIN prod.agg_individual_xographic ixo ON ix.individual_id = ixo.individual_id
       LEFT JOIN an ON ix.individual_id = an.individual_id
--WHERE 1 = 0
GROUP BY
	  ix.individual_id
	--, ix.hh_id
	, ix.household_id
    --, ix.osl_hh_id
;
