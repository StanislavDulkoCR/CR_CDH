
        SELECT 


        xr.individual_id,
               xr.hh_id,

/***************************************************************************/
--PART 1
/***************************************************************************/

               ix.acx_gender_cd         AS gender,
               ix.customer_typ,
               to_number(substring(GREATEST(NVL(TO_CHAR(fs.fr_lst_don_dt,  'YYYYMMDD'),'00010101') || '1' || to_char(fs.fr_lst_don_amt),
                            NVL(TO_CHAR(adv_mt.adv_lst_don_dt, 'YYYYMMDD'),'00010101') ||  '2' || to_char(adv_mt.adv_lst_don_amt),
                            NVL(to_char(TO_DATE(DECODE(abq.abq_lst_don_dt, NULL, NULL, abq.abq_lst_don_dt || '0601'),'YYYYMMDD'),'YYYYMMDD'),'00010101') || '3' || to_char(abq.abq_lst_don_amt),
                            '00010101') ,10)) AS lst_don_amt,              
               NVL(fs.fr_ltd_don_amt,0) + NVL(abq.abq_ltd_don_amt,0) + NVL(adv_mt.adv_ltd_don_amt,0) AS ltd_don_amt,
               GREATEST(NVL(fs.fr_max_don_amt,0), NVL(adv_mt.adv_max_don_amt,0), NVL(abq.abq_max_don_amt,0)) AS max_don_amt,
               NULLIF(LEAST(NVL(fs.fr_fst_don_dt, TO_DATE('99990601','YYYYMMDD')), 
                            NVL(adv_mt.adv_fst_don_dt, TO_DATE('99990601','YYYYMMDD')),
                           NVL(TO_DATE(DECODE(abq.abq_fst_don_dt, NULL, NULL, abq.abq_fst_don_dt || '0601'),'YYYYMMDD'), TO_DATE('99990601','YYYYMMDD'))),
                            TO_DATE('99990601','YYYYMMDD')) AS fst_don_dt,
               NULLIF(GREATEST(NVL(fs.fr_lst_don_dt, TO_DATE('00010101','YYYYMMDD')), 
                               NVL(adv_mt.adv_lst_don_dt, TO_DATE('00010101','YYYYMMDD')),  
                               NVL(TO_DATE(DECODE(abq.abq_lst_don_dt, NULL, NULL, abq.abq_lst_don_dt || '0601'),'YYYYMMDD'), TO_DATE('00010101','YYYYMMDD'))),
                               TO_DATE('00010101','YYYYMMDD')) AS lst_don_dt,
               NVL(fs.fr_ltd_don_cnt,0) + NVL(abq.abq_ltd_don_cnt,0) + NVL(adv_mt.adv_ltd_don_cnt,0) AS ltd_don_cnt,
               NULLIF(LEAST(NVL(fs.fr_fst_don_dt,    TO_DATE('99990601','YYYYMMDD')),
                            NVL(adv_mt.first_action_dt, TO_DATE('99990601','YYYYMMDD')),
                            NVL(a.mn_newsltr_fst_dt, TO_DATE('99990601','YYYYMMDD')),
                            NVL(cga.mn_don_dt,       TO_DATE('99990601','YYYYMMDD')),
                            NVL(bb.bnb_frst_sales_dt,       TO_DATE('99990601','YYYYMMDD')),
                            NVL(abqr.min_abq_mbr_dt, TO_DATE('99990601','YYYYMMDD')),
                            NVL(ix.dc_fund_first_dt, to_date('99990601','YYYYMMDD')),
                            NVL(abqd.min_abq_dt, to_date('99990601','YYYYMMDD')),
                            NVL(surv.min_answer_date, to_date('99990601','YYYYMMDD')),
                            NVL(page.min_hit_time, to_date('99990601','YYYYMMDD')),
                            TO_DATE(NVL(abq.abq_fst_rsp_dt,'9999')||'0601','YYYYMMDD'),
                            NVL(a.mn_fst_dt,         TO_DATE('99990601','YYYYMMDD'))),
                      TO_DATE('99990601','YYYYMMDD')) AS non_prod_fst_dt,
           substring(LEAST(NVL(TO_CHAR(fs.fr_fst_don_dt,       'YYYYMMDD'),'99990601') || '1' || 'FR',
                            NVL(TO_CHAR(ix.dc_fund_first_dt,    'YYYYMMDD'),'99990601') || '1' || 'FR',
                            NVL(to_char(abqd.min_abq_dt,        'YYYYMMDD'),'99990601') || '1' || 'FR',
                            NVL(TO_CHAR(adv_mt.first_action_dt, 'YYYYMMDD'),'99990601') || '2' || 'ADV',
                            NVL(TO_CHAR(a.mn_fst_dt,            'YYYYMMDD'),'99990601') || '3' || 'SUR',
         TO_CHAR(TO_DATE(NVL(abq.abq_fst_rsp_dt,'9999')||'0601','YYYYMMDD'),'YYYYMMDD') || '3' || 'SUR',
                            NVL(TO_CHAR(surv.min_answer_date,   'YYYYMMDD'),'99990601') || '3' || 'SUR',
                            NVL(TO_CHAR(a.mn_newsltr_fst_dt,    'YYYYMMDD'),'99990601') || '4' || 'NL',
                            NVL(TO_CHAR(cga.mn_don_dt,          'YYYYMMDD'),'99990601') || '5' || 'CGA',
                            NVL(TO_CHAR(bb.bnb_frst_sales_dt,   'YYYYMMDD'),'99990601') || '6' || 'BNB',
                            NVL(TO_CHAR(page.min_hit_time,      'YYYYMMDD'),'99990601') || '7' || 'UR',
                            '99990101') ,10) AS fst_non_prod_cd,



/***************************************************************************/
--PART 2
/***************************************************************************/


               NVL(fs.fr_ltd_don_cnt,0) + NVL(abq.abq_ltd_don_cnt,0)
               + NVL(adv_mt.adv_ltd_don_cnt,0) + decode(nvl(a.news_auth_cnt,0),0,0,1) + decode(nvl(adv_mt.individual_id,0),0,0,1)
               + NVL(bb.bnb_tot_sale_cnt,0) + decode(nvl(page.individual_id,0),0,0,1) + NVL(cga.cga_cnt,0)
               + decode(nvl(surv.individual_id,0),0,decode(nvl(a.auth_cnt,0),0,decode(nvl(advsur.individual_id,0),0,decode(nvl(abq.abq_ltd_rsp_cnt,0),0,0,1),1),1),1) AS non_prod_cnt,
               a.mrp_flg,
               NVL2(frc2.secd_indiv_id,'Y','N') AS fr_combined_flg,
               NULLIF(GREATEST(NVL(er.er_fst_dt,   TO_DATE('00010101','YYYYMMDD')),
                               NVL(frc.mx_eff_dt,  TO_DATE('00010101','YYYYMMDD')),
                               NVL(oaa.mx_merge_dt,TO_DATE('00010101','YYYYMMDD'))),
                      TO_DATE('00010101','YYYYMMDD')) AS ind_combined_dt,
               NVL(ofo.ofo_pd_amt,0) + NVL(oli.oli_tot_amt,0) + NVL(oli.oli_tot_amt2,0)
               + NVL(fs.fr_ltd_don_amt,0) + NVL(cga.cga_don_amt,0) + NVL(bo.bo_pmt_amt, 0) + NVL(adv_mt.adv_ltd_don_amt,0) AS ind_ltd_amt,
               CASE WHEN GREATEST(NVL(olo.olo_mx_crt_dt,   TO_DATE('00010101','YYYYMMDD')),
                                  NVL(ofo.ofo_mx_cc_ord_dt,TO_DATE('00010101','YYYYMMDD'))) >
                         GREATEST(NVL(oli.oli_mx_pmt_dt,   TO_DATE('00010101','YYYYMMDD')),
                                  NVL(ofo.ofo_mx_pmt_dt,   TO_DATE('00010101','YYYYMMDD'))) THEN 'Y' ELSE 'N' END AS comp_flg,
             to_date(substring(LEAST(NVL(to_char(bo.bo_min_ord_dt      ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(bo.bo_min_ord_dt      ,'YYYYMMDDHH24MISS') 
                                   ,NVL(to_char(ix.dc_cdb_last_ord_dt ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(ix.dc_cdb_last_ord_dt ,'YYYYMMDDHH24MISS') 
                                   ,NVL(to_char(ix.dc_offline_first_dt,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(ix.dc_offline_first_dt,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(ofo.ofo_mn_ord_dt     ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(ofo.ofo_mn_ord_dt     ,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(ix.dc_online_first_dt ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(ix.dc_online_first_dt ,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(oli.oli_mn_crt_dt     ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(oli.oli_mn_crt_dt     ,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(de.de_min_exp_dt      ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(de.de_min_exp_dt      ,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(ix.dc_fund_first_dt   ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(ix.dc_fund_first_dt   ,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(ix.dc_last_aps_ord_dt ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(ix.dc_last_aps_ord_dt ,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(fs.fr_fst_don_dt      ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(fs.fr_fst_don_dt      ,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(cga.mn_don_dt         ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(cga.mn_don_dt         ,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(a.mn_fst_dt           ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(a.mn_fst_dt           ,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(ix.dc_corp_first_dt   ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(ix.dc_corp_first_dt   ,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(adv_mt.first_action_dt,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(adv_mt.first_action_dt,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(a.mn_newsltr_fst_dt   ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(a.mn_newsltr_fst_dt   ,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(eread.min_rpt_start_date,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(eread.min_rpt_start_date,'YYYYMMDDHH24MISS')
                                   ,NVL(to_char(bb.bnb_frst_sales_dt  ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(bb.bnb_frst_sales_dt  ,'YYYYMMDDHH24MISS')
                             ),15),'YYYYMMDDHH24MISS') AS ind_fst_rel_dt,
         substring(LEAST(NVL(ofo.ofo_fst_prod_cd,                                '99991231'||'99')
                           ,NVL(de.de_fst_prod_cd,                                  '99991231'||'99')
                           ,NVL(oli.oli_fst_prod_cd,                                '99991231'||'99')
                           ,NVL(to_char(ix.dc_corp_first_dt,    'YYYYMMDD'),'99991231')||'C0'||'CNS'
                           ,NVL(to_char(ix.dc_offline_first_dt, 'YYYYMMDD'),'99991231')||'60'||'CNS'
                           ,NVL(to_char(ix.dc_fund_first_dt,    'YYYYMMDD'),'99991231')||'C0'||'CNS'
                           ,NVL(to_char(ix.dc_online_first_dt,  'YYYYMMDD'),'99991231')||'80'||'CRO'
                           ,NVL(to_char(ix.dc_last_aps_ord_dt,  'YYYYMMDD'),'99991231')||'70'||'NCPR'
                           ,NVL(bo.bo_fst_prod_cd,                                  '99991231'||'99')
                           ,NVL(to_char(ix.dc_cdb_last_ord_dt,  'YYYYMMDD'),'99991231')||'B0'||'CDR'
                           ,NVL(to_char(fs.fr_fst_don_dt,       'YYYYMMDD'),'99991231')||substring(least(NVL(to_char(ix.dc_corp_first_dt,    'YYYYMMDD'),'99991231')||'D5'||'CNS'
                                                                                                     ,NVL(to_char(ix.dc_offline_first_dt, 'YYYYMMDD'),'99991231')||'D1'||'CNS'
                                                                                                     ,NVL(to_char(ix.dc_fund_first_dt,    'YYYYMMDD'),'99991231')||'D5'||'CNS'
                                                                                                     ,NVL(to_char(ix.dc_online_first_dt,  'YYYYMMDD'),'99991231')||'D3'||'CRO'
                                                                                                     ,NVL(to_char(ix.dc_last_aps_ord_dt,  'YYYYMMDD'),'99991231')||'D2'||'NCPR'
                                                                                                     ,NVL(to_char(ix.dc_cdb_last_ord_dt,  'YYYYMMDD'),'99991231')||'D4'||'CDR'
                                                                                                     ,'99991230'||'G0'||'FR'),9)
                           ,NVL(to_char(adv_mt.first_action_dt, 'YYYYMMDD'),'99991231')||'E0'||'ADV'
                           ,NVL(to_char(a.mn_newsltr_fst_dt,    'YYYYMMDD'),'99991231')||'F0'||'NL'
                           ,NVL(to_char(cga.mn_don_dt,          'YYYYMMDD'),'99991231')||'H0'||'CGA'
                           ,NVL(to_char(a.mn_fst_dt,            'YYYYMMDD'),'99991231')||'I0'||'SUR'
                           ,NVL(to_char(eread.report_start_date,'YYYYMMDD'),'99991231')||'J0'||'IPAD'
                           ,NVL(to_char(bb.bnb_last_sales_dt,   'YYYYMMDD'),'99991231')||'K0'||'BNB'
                           ,'99991230'||'00'),11) AS ind_fst_rel_cd,
               NULLIF(GREATEST(NVL(bo.bo_max_ord_dt,      TO_DATE('00010601','YYYYMMDD')), 
                               NVL(bo.bo_pmt_dt,          TO_DATE('00010601','YYYYMMDD')), 
                               NVL(bi.bi_ret_canc_dt,     TO_DATE('00010601','YYYYMMDD')),  
                               NVL(ix.dc_cdb_last_ord_dt, TO_DATE('00010601','YYYYMMDD')),  
                               NVL(ix.dc_offline_first_dt,to_date('00010601','YYYYMMDD')),
                               NVL(ofo.ofo_mx_ord_dt,     TO_DATE('00010601','YYYYMMDD')),
                               NVL(ofo.ofo_mx_pmt_dt,     TO_DATE('00010601','YYYYMMDD')),
                               NVL(ofo.ofo_mx_canc_dt,    TO_DATE('00010601','YYYYMMDD')),
                               NVL(ix.dc_online_first_dt, to_date('00010601','YYYYMMDD')),  
                               NVL(oli.oli_mx_crt_dt,     TO_DATE('00010601','YYYYMMDD')),
                               NVL(oli.oli_mx_canc_dt,    TO_DATE('00010601','YYYYMMDD')),
                               NVL(oli.oli_mx_pmt_dt,     TO_DATE('00010601','YYYYMMDD')),
                               NVL(ix.dc_last_aps_ord_dt, TO_DATE('00010601','YYYYMMDD')),
                               NVL(ix.dc_corp_first_dt,   to_date('00010601','YYYYMMDD')),
                               NVL(ix.dc_fund_first_dt,   to_date('00010601','YYYYMMDD')),
                               NVL(fr.fr_lst_don_dt,      TO_DATE('00010601','YYYYMMDD')),  
                               TO_DATE(NVL(abq.abq_lst_rsp_dt,'0001')||'0601','YYYYMMDD'),
                               NVL(na.coa_date,           TO_DATE('00010601','YYYYMMDD')),
                               NVL(cga.mx_don_dt,         TO_DATE('00010601','YYYYMMDD')),
                               NVL(a.mx_fst_dt,           TO_DATE('00010601','YYYYMMDD')),
                               NVL(adv_mt.last_action_dt,    TO_DATE('00010601','YYYYMMDD')),
                               NVL(eread.report_start_date,    TO_DATE('00010601','YYYYMMDD')),
                               NVL(abqd.max_abq_dt,    TO_DATE('00010601','YYYYMMDD')),
                               NVL(surv.answer_date,    TO_DATE('00010601','YYYYMMDD')),
                               NVL(bb.bnb_last_sales_dt,    TO_DATE('00010601','YYYYMMDD')),
                               NVL(page.hit_time,    TO_DATE('00010601','YYYYMMDD')),
                               NVL(a.mx_newsltr_fst_dt,      TO_DATE('00010601','YYYYMMDD'))),
                          TO_DATE('00010601','YYYYMMDD')) AS ind_lst_act_dt,



/***************************************************************************/
--PART 3
/***************************************************************************/

               DECODE(ofs.cr_actv_flg,'Y',1,0) + DECODE(ofs.cr_sub_dnr_flg,'Y',1,0) + DECODE(ofs.cr_non_sub_dnr_flg,'Y',1,0)
               + DECODE(ofs.hl_actv_flg,'Y',1,0) + DECODE(ofs.hl_sub_dnr_flg,'Y',1,0) + DECODE(ofs.hl_non_sub_dnr_flg,'Y',1,0)
               + DECODE(ofs.ma_actv_flg,'Y',1,0) + DECODE(ofs.ma_sub_dnr_flg,'Y',1,0) + DECODE(ofs.ma_non_sub_dnr_flg,'Y',1,0)
               + DECODE(ofs.shm_actv_flg,'Y',1,0) + DECODE(ofs.shm_sub_dnr_flg,'Y',1,0) + DECODE(ofs.shm_non_sub_dnr_flg,'Y',1,0)
               + DECODE(ols.cro_actv_flg,'Y',1,0) + DECODE(ols.cro_sub_dnr_flg,'Y',1,0) + DECODE(ols.cro_non_sub_dnr_flg,'Y',1,0)
               + DECODE(ols.carp_actv_flg,'Y',1,0)
               + CASE WHEN ols.nc_rpts_lst_ord_dt > ADD_MONTHS(SYSDATE,-12) THEN 1 ELSE 0 END
               + CASE WHEN ols.uc_rpts_lst_ord_dt > ADD_MONTHS(SYSDATE,-12) THEN 1 ELSE 0 END
               + CASE WHEN fs.fr_lst_don_dt > ADD_MONTHS(SYSDATE,-12) THEN 1 ELSE 0 END
               + CASE WHEN abq.abq_mbr_flg = 'Y' THEN 1 ELSE 0 END
               + CASE WHEN a.auth_flg_cnt > 0 THEN 1 ELSE 0 END
               + DECODE(bs.bk_actv_flg,'Y',1,0) + DECODE(a.news_car_watch_flg,'Y',1,0) + DECODE(a.news_best_buy_drug_flg,'Y',1,0)
               + DECODE(a.news_safety_alert_flg,'Y',1,0) + DECODE(a.news_hlth_alert_flg,'Y',1,0) + DECODE(a.news_cro_whats_flg,'Y',1,0)
               + DECODE(a.news_green_choice_flg,'Y',1,0)
               + CASE WHEN adv_mt.adv_active_flg = 'Y' OR adv_mt.adv_volunteer_flg = 'Y' THEN 1 ELSE 0 END AS ind_actv_cnt,
               DECODE(NVL(ofs.cr_flg,'N'),'N',0, DECODE(ofs.cr_fst_dnr_dt,NULL,1,2))
               + DECODE(NVL(ofs.hl_flg,'N'),'N',0, DECODE(ofs.hl_fst_dnr_dt,NULL,1,2))
               + DECODE(NVL(ofs.ma_flg,'N'),'N',0, DECODE(ofs.ma_fst_dnr_dt,NULL,1,2))
               + DECODE(NVL(ofs.shm_flg,'N'),'N',0, DECODE(ofs.shm_fst_dnr_dt,NULL,1,2))
               + CASE WHEN ofs.individual_id IS NULL AND (ix.dc_offline_first_dt IS NOT NULL OR ix.dc_corp_first_dt IS NOT NULL) THEN 1 ELSE 0 END
               + DECODE(NVL(ols.cro_flg,'N'),'N',0, DECODE(ols.cro_fst_dnr_dt,NULL,1,2))
               + DECODE(ols.crmg_flg,'Y',1,0) + DECODE(ols.ncbk_flg,'Y',1,0) + DECODE(ols.ucbk_flg,'Y',1,0)
               + CASE WHEN oli.oli_carp_cnt > 0 THEN 1 ELSE 0 END
               + DECODE(ols.nc_rpts_flg,'Y',1,0) + DECODE(ols.uc_rpts_flg,'Y',1,0)
               + CASE WHEN ols.nc_rpts_flg = 'N' AND ols.uc_rpts_flg = 'N' AND ix.dc_last_aps_ord_dt IS NOT NULL THEN 1 ELSE 0 END
               + CASE WHEN ols.individual_id IS NULL AND ix.dc_online_first_dt IS NOT NULL THEN 1 ELSE 0 END
               + NVL2(fs.individual_id,1,0)
               + CASE WHEN fs.individual_id IS NULL AND ix.dc_fund_first_dt IS NOT NULL THEN 1 ELSE 0 END
               + CASE WHEN abq.abq_ltd_mbr_cnt > 0 THEN 1 ELSE 0 END
               + CASE WHEN a.auth_cnt > 0          THEN 1 ELSE 0 END
               + CASE WHEN a.news_auth_cnt > 0     THEN 1 ELSE 0 END
               + DECODE(bs.bk_flg,'Y',1,0)
               + CASE WHEN bs.individual_id IS NULL AND ix.dc_cdb_last_ord_dt IS NOT NULL THEN 1 ELSE 0 END
               + CASE WHEN NVL2(adv_mt.individual_id,'Y','N') = 'Y' THEN 1 ELSE 0 END AS ind_relship_cnt,
               substring(LEAST(NVL(ofo.ofo_fst_prod_cd,'99991231'||'99'),
                            NVL(de.de_fst_prod_cd,  '99991231'||'99'),
                            NVL(oli.oli_fst_prod_cd,'99991231'||'99'),
                            NVL(to_char(ix.dc_corp_first_dt,    'YYYYMMDD'),'99991231')||'C0'||'CNS',
                            NVL(to_char(ix.dc_fund_first_dt,    'YYYYMMDD'),'99991231')||'C0'||'CNS',
                            NVL(to_char(ix.dc_offline_first_dt, 'YYYYMMDD'),'99991231')||'60'||'CNS',
                            NVL(to_char(ix.dc_online_first_dt,  'YYYYMMDD'),'99991231')||'80'||'CRO',
                            NVL(TO_CHAR(ix.dc_last_aps_ord_dt,  'YYYYMMDD'),'99991231')||'70'||'NCPR',
                            NVL(bo.bo_fst_prod_cd,  '99991231'||'99'), 
                            NVL(TO_CHAR(ix.dc_cdb_last_ord_dt,  'YYYYMMDD'),'99991231')||'B0'||'CDR',
                            CASE WHEN ix.dc_corp_first_dt IS NULL AND
                                      ix.dc_offline_first_dt IS NULL AND
                                      ix.dc_fund_first_dt IS NULL AND
                                      ix.dc_online_first_dt IS NULL AND
                                      ix.dc_cdb_last_ord_dt IS NULL THEN '99991231'||'D0' ELSE
                            NVL(to_char(fs.fr_fst_don_dt,       'YYYYMMDD'),'99991231')||substring(least(NVL(to_char(ix.dc_corp_first_dt,    'YYYYMMDD'),'99991231')||'C5'||'CNS'
                                                                                                     ,NVL(to_char(ix.dc_offline_first_dt, 'YYYYMMDD'),'99991231')||'C1'||'CNS'
                                                                                                     ,NVL(to_char(ix.dc_fund_first_dt,    'YYYYMMDD'),'99991231')||'C5'||'CNS'
                                                                                                     ,NVL(to_char(ix.dc_online_first_dt,  'YYYYMMDD'),'99991231')||'C3'||'CRO'
                                                                                                     ,NVL(to_char(ix.dc_last_aps_ord_dt,  'YYYYMMDD'),'99991231')||'C2'||'NCPR'
                                                                                                     ,NVL(to_char(ix.dc_cdb_last_ord_dt,  'YYYYMMDD'),'99991231')||'C4'||'CDR'
                                                                                                   ,'99991230'||'D0'),9) END                           
                            ,NVL(to_char(eread.min_rpt_start_date,    'YYYYMMDD'),'99991231')||'E0'||'IPAD',
                            '99991231'||'00'),11) AS fst_prod_cd,  
               CASE WHEN NVL(ofo.ofo_ce_cnt,0) + NVL(oli.oli_ce_cnt,0) + NVL(bo.bo_ce_cnt, 0) > 0
                     AND NVL(ofo.ofo_xce_cnt,0) + NVL(oli.oli_xce_cnt,0) + NVL(bo.bo_xce_cnt, 0) = 0 THEN 'Y' ELSE 'N' END AS ind_rec_prod_only_flg,
               NVL2(an.an_emp_flg,'Y','N')     AS emp_flg, 
               NVL2(cga.individual_id,'Y','N') AS cga_flg,
               a.news_car_watch_flg     AS news_car_watch_flg,
               a.news_best_buy_drug_flg AS news_best_buy_drug_flg,
               a.news_safety_alert_flg  AS news_safety_alert_flg,
               a.news_hlth_alert_flg    AS news_hlth_alert_flg,
               a.news_cro_whats_flg     AS news_cro_whats_flg,
               a.news_green_choice_flg  AS news_green_choice_flg,

/***************************************************************************/
--PART 4
/***************************************************************************/


               CASE WHEN (bo.bo_recip_cd_d_cnt > 0    
                      OR  ofs.cr_fst_dnr_dt IS NOT NULL OR ofs.hl_fst_dnr_dt IS NOT NULL
                      OR  ofs.ma_fst_dnr_dt IS NOT NULL OR ols.cro_fst_dnr_dt IS NOT NULL
                      OR  ofs.shm_fst_dnr_dt IS NOT NULL)  
                     AND NVL(ofs.cr_non_dnr_flg,'N') != 'Y'
                     AND NVL(ofs.hl_non_dnr_flg,'N') != 'Y'
                     AND NVL(ofs.ma_non_dnr_flg,'N') != 'Y'
                     AND NVL(ofs.shm_non_dnr_flg,'N') != 'Y' 
                     AND NVL(ols.cro_non_dnr_flg,'N') != 'Y'
                     AND NVL(ols.crmg_flg,'N') != 'Y'
                     AND NVL(ols.ncbk_flg,'N') != 'Y'
                     AND NVL(ols.ucbk_flg,'N') != 'Y'
                     AND NVL(ols.nc_rpts_flg,'N') != 'Y'
                     AND NVL(ols.uc_rpts_flg,'N') != 'Y'
                     AND NVL2(fs.individual_id,'Y','N') != 'Y'
                     AND NVL2(abqd.individual_id,'Y','N') != 'Y'
                     AND NVL2(abqr.individual_id,'Y','N') != 'Y'
                     AND NVL2(ar.individual_id,'Y','N') != 'Y'
                     AND NVL2(adv.individual_id,'Y','N') != 'Y'
                     AND ix.dc_last_aps_ord_dt IS NULL
                     AND NVL(a.auth_cnt,0) = 0
                     AND NVL(bo.bo_recip_cd_not_d_cnt, 0) = 0 THEN 'Y' ELSE 'N' END AS ind_dnr_only_flg,
               ix.dc_cdb_last_ord_dt,
               NVL(ofo.ofo_prod_ltd_pd_amt,0) + NVL(oli.oli_tot_amt,0) + NVL(oli.oli_tot_amt2,0) + NVL(bs.bs_prod_ltd_pd_amt,0) AS prod_ltd_pd_amt,
               NULLIF(LEAST(NVL(ix.dc_cdb_last_ord_dt,  TO_DATE('99991231','YYYYMMDD')),
                            NVL(bo.bo_prod_fst_ord_dt,  TO_DATE('99991231','YYYYMMDD')),
                            NVL(ix.dc_offline_first_dt, TO_DATE('99991231','YYYYMMDD')),
                            NVL(ofo.ofo_prod_fst_ord_dt,TO_DATE('99991231','YYYYMMDD')),
                            NVL(ix.dc_last_aps_ord_dt,  TO_DATE('99991231','YYYYMMDD')),
                            NVL(ix.dc_online_first_dt,  to_date('99991231','YYYYMMDD')), 
                            NVL(oli.oli_mn_crt_dt,      TO_DATE('99991231','YYYYMMDD')),
                            NVL(de.de_min_exp_dt,       TO_DATE('99991231','YYYYMMDD')),
                            NVL(ix.dc_corp_first_dt,    TO_DATE('99991231','YYYYMMDD')),
                            NVL(ix.dc_fund_first_dt,    TO_DATE('99991231','YYYYMMDD')),
                            NVL(eread.min_rpt_start_date, TO_DATE('99991231','YYYYMMDD')),
                            CASE when ix.dc_corp_first_dt is not null or 
                                      ix.dc_offline_first_dt is not null or
                                      ix.dc_online_first_dt is not null or
                                      ix.dc_fund_first_dt is not null or
                                      ix.dc_cdb_last_ord_dt is not null or
                                      ix.dc_last_aps_ord_dt is not null
                                  then NVL(fs.fr_fst_don_dt,    TO_DATE('99991231','YYYYMMDD'))
                                else TO_DATE('99991231','YYYYMMDD') end),
                      TO_DATE('99991231','YYYYMMDD')) AS prod_fst_ord_dt,


/***************************************************************************/
--PART 5
/***************************************************************************/


               NULLIF(GREATEST(NVL(ofo.ofo_prod_lst_canc_bad_dbt_dt,TO_DATE('00010101','YYYYMMDD')),
                               NVL(oli.oli_prod_lst_canc_bad_dbt_dt,TO_DATE('00010101','YYYYMMDD')),
                               NVL(bi.bi_prod_lst_canc_bad_dbt_dt,  TO_DATE('00010101','YYYYMMDD'))),
                      TO_DATE('00010101','YYYYMMDD')) AS prod_lst_canc_bad_dbt_dt,
               NULLIF(GREATEST(NVL(fr.fr_lst_don_dt,   TO_DATE('00010601','YYYYMMDD')),
                               NVL(adv_mt.last_action_dt, TO_DATE('00010601','YYYYMMDD')),
                               NVL(cga.mx_don_dt,      TO_DATE('00010601','YYYYMMDD')),
                               NVL(abqr.max_abq_mbr_dt,TO_DATE('00010601','YYYYMMDD')),
                               NVL(ix.dc_fund_first_dt,to_date('00010601','YYYYMMDD')),
                               NVL(a.mx_newsltr_fst_dt,   to_date('00010601','YYYYMMDD')),
                               NVL(page.hit_time,   to_date('00010601','YYYYMMDD')),
                               NVL(abqd.max_abq_dt,   to_date('00010601','YYYYMMDD')),
                               TO_DATE(NVL(abq.abq_lst_rsp_dt,'0001')||'0601','YYYYMMDD'),
                               NVL(surv.answer_date,   to_date('00010601','YYYYMMDD')),
                               NVL(bb.bnb_last_sales_dt,   to_date('00010601','YYYYMMDD')),
                               NVL(a.mx_fst_dt,        TO_DATE('00010601','YYYYMMDD'))),
                      TO_DATE('00010601','YYYYMMDD')) AS non_prod_lst_act_dt,
               NULLIF(GREATEST(NVL(ix.dc_cdb_last_ord_dt,   TO_DATE('00010101','YYYYMMDD')),
                               NVL(bo.bo_prod_lst_ord_dt,   TO_DATE('00010101','YYYYMMDD')),
                               NVL(ix.dc_offline_first_dt,  TO_DATE('00010101','YYYYMMDD')),
                               NVL(ofo.ofo_prod_lst_ord_dt, TO_DATE('00010101','YYYYMMDD')),
                               NVL(ix.dc_last_aps_ord_dt,   TO_DATE('00010101','YYYYMMDD')), 
                               NVL(ix.dc_online_first_dt,   to_date('00010101','YYYYMMDD')),
                               NVL(oli.oli_mx_crt_dt,       TO_DATE('00010101','YYYYMMDD')),
                               NVL(de.de_max_exp_dt,        TO_DATE('00010101','YYYYMMDD')),
                               NVL(ix.dc_corp_first_dt,     to_date('00010101','YYYYMMDD')),
                               NVL(eread.report_start_date, to_date('00010101','YYYYMMDD'))),
                      TO_DATE('00010101','YYYYMMDD')) AS prod_lst_ord_dt,

/***************************************************************************/
--PART 6
/***************************************************************************/


               NVL(bo.bo_prod_ord_cnt,0) + NVL(ofo.ofo_prod_ord_cnt,0) + NVL(de.de_cnt,0)
               + NVL(oli.oli_xce_cnt,0) + NVL2(ix.dc_cdb_last_ord_dt,1,0) + NVL(eread.ereader_cnt,0)
               + CASE WHEN ix.dc_offline_first_dt IS NOT NULL AND NVL(ofoxog.dc_offline_fst_crt_ind,0) = 0 THEN 1 ELSE 0 END
               + CASE WHEN ix.dc_corp_first_dt IS NOT NULL AND NVL(ofoxog.dc_corp_first_ind2,0) = 0 THEN 1 ELSE 0 END         
               + CASE WHEN ix.dc_online_first_dt IS NOT NULL AND NVL(olixog2.dc_online_fst_crt_ind,0) = 0 AND NVL(olixog2.dc_online_fst_aps_ind,0) = 0 THEN 1 ELSE 0 END
               + CASE WHEN ix.dc_last_aps_ord_dt IS NOT NULL AND NVL(olixog1.dc_last_aps_crt_ind,0) = 0 THEN 1 ELSE 0 END AS prod_ord_cnt,
               NVL(bo.bo_prod_dm_ord_cnt,0) + NVL(ofo.ofo_prod_dm_ord_cnt,0) + NVL(oli.oli_xce_d_cnt,0)          AS prod_dm_ord_cnt,
               NVL(bo.bo_prod_em_ord_cnt,0) + NVL(ofo.ofo_prod_em_ord_cnt,0) + NVL(oli.oli_xce_egknu_cnt,0)      AS prod_em_ord_cnt,
               NVL(ofo.ofo_prod_dnr_ord_cnt,0) + NVL(oli.oli_prod_dnr_ord_cnt,0) + NVL(bo.bo_prod_dnr_ord_cnt,0) AS prod_dnr_ord_cnt,
               NVL(oli.oli_cro_prod_cnt,0) + NVL(oli.oli_carp_prod_cnt,0) + NVL(oli.oli_crmg_prod_cnt,0) + NVL(brks.cr_prod_cnt,0) + NVL(de.de_cr_prod_cnt,0)
               + NVL(ofo.ofo_cr_prod_cnt,0) + NVL(brks.hl_prod_cnt,0) + NVL(de.de_hl_prod_cnt,0) + NVL(ofo.ofo_hl_prod_cnt,0)
               + NVL(brks.ma_prod_cnt,0) + NVL(ofo.ofo_ma_prod_cnt,0) + NVL(brks.shm_prod_cnt,0) + NVL(ofo.ofo_shm_prod_cnt,0) 
               + NVL(oli.oli_aps_prod_cnt,0) + NVL(bi.bi_cnt,0) + NVL2(ix.dc_cdb_last_ord_dt,1,0)
               + CASE WHEN ix.dc_online_first_dt IS NOT NULL AND nvl(oli.oli_cro_prod_cnta,0) = 0 THEN 1 ELSE 0 END
               + CASE WHEN (ix.dc_offline_first_dt IS NOT NULL OR ix.dc_corp_first_dt IS NOT NULL) AND nvl(brks.cr_prod_cnta,0) = 0 THEN 1 ELSE 0 END
               + CASE WHEN ix.dc_last_aps_ord_dt IS NOT NULL THEN 1 ELSE 0 END AS prod_cnt,
               ix.ad_apl_keycode,
               CASE WHEN e.email_type_cd = 'I' AND (e.valid_flag = 'N' OR e.src_valid_flag = 'N' OR e.src_delv_ind = '1') THEN 'N'
                    WHEN e.email_type_cd = 'I' THEN 'Y' END AS ind_email_summary_flag,
               CASE WHEN e.email_type_cd = 'M' AND (e.valid_flag = 'N' OR e.src_valid_flag = 'N' OR e.src_delv_ind = '1') THEN 'N'
                    WHEN e.email_type_cd = 'M' THEN 'Y' END AS mrp_email_summary_flag,
               CASE WHEN e.email_type_cd = 'N' AND (e.valid_flag = 'N' OR e.src_valid_flag = 'N' OR e.src_delv_ind = '1') THEN 'N'
                    WHEN e.email_type_cd = 'N' THEN 'Y' END AS news_email_summary_flag,
               CASE WHEN NVL2(adv_mt.individual_id,'Y','N') = 'Y' THEN 'Y' ELSE 'N' END AS adv_ever,
               NVL(adv_mt.adv_active_flg,'N') AS adv_active,
               NULL verity_recency_flag, -- Ed says 2017/10/17 14:47: We got rid of MT_ALLIANT therefore this as well
               NVL2(an.an_forums_flag,'Y','N') AS discussions_forum_flag,
               a.news_hlth_heart_flg           AS news_hlth_heart_flg,
               a.news_hlth_child_teen_flg      AS news_hlth_child_teen_flg,
               a.news_hlth_diabetes_flg        AS news_hlth_diabetes_flg,
               a.news_hlth_women_flg           AS news_hlth_women_flg,
               a.news_hlth_after_60_flg        AS news_hlth_after_60_flg,
               ibx.age_infobase                AS age_infobase,
               NVL(ibx.gender_infobase,'U')    AS gender_infobase,
               case when eua.individual_id is not null then eua.min_create_date else null end ipad_enabled_dt,
               bnb_frst_visitor_dt,
               bnb_last_visitor_dt,
               bnb_frst_prospect_dt,
               bnb_last_prospect_dt,
               bnb_frst_sales_dt,
               bnb_last_sales_dt,
               bnb_tot_prospect_cnt,
               bnb_tot_sale_cnt,
               bnb_best_status,
               bnb_best_status_dt,
               bnb_most_recent_status,
               bnb_most_recent_dt,
               bnb_nbr_new_sales,
               bnb_nbr_used_sales,
               bnb_most_recent_trans_type,
               ol.ol_match_recent_dt,
               ol.ol_match_cnt,
               ol.ol_match_cnt_6m,
               ol.ol_match_cnt_7_12m,
               ol.ol_match_cnt_13_18m,
               IND_PROSPECT_EML_MTCH_IND,
               PROSPECT_EMAIL_MATCH_DT,
               PROSPECT_EMAIL_MATCH_CNT,


/***************************************************************************/
--PART 7
/***************************************************************************/

               CASE WHEN lower(we.email_address) LIKE '%.ca' OR ia.country_id='CAN'
                         THEN CASE WHEN months_between(trunc(sysdate),trunc(ofo.ofo_mx_ord_dt)) < 23 OR
                                        months_between(trunc(sysdate),trunc(erm.mx_cds_pub_dt)) < 23 OR
                                        months_between(trunc(sysdate),trunc(oli.oli_mx_end_dt)) < 23 OR
                                        months_between(trunc(sysdate),trunc(CASE WHEN fr.fr_max_don_amt > 0 THEN fs.fr_lst_don_dt END)) < 23 OR
                                        months_between(trunc(sysdate),trunc(cga.mx_don_dt)) < 23 OR
                                        months_between(trunc(sysdate),trunc(a.mx_news_auth_n_eff_dt)) < 23 OR
                                        a.news_auth_y_cnt > 0
                                   THEN 'Y'
                              ELSE 'N' END
               END AS CAN_SPAM_FLG,
               CASE WHEN coop.individual_id IS NOT NULL THEN 'Y' ELSE 'N' END print_sub_coop_flg,
               CASE WHEN app.individual_id IS NOT NULL THEN 'R' ELSE NULL END mobile_app_usage_ind,
               ROUND((sysdate - max_visit_strt_time) + max_days_since_fst_use) as rtg_app_days_since_fst_act,
               ROUND(sysdate - max_visit_strt_time) as rtg_app_days_since_lst_act,
               key.email_fav_key,
               NVL(adv_mt.adv_ltd_alrt_resp_cnt,0) + NVL(adv_mt.adv_ltd_sur_resp_cnt,0) + NVL(adv_mt.adv_ltd_don_cnt,0) as adv_cnt
        
        FROM
            cr_temp.indiv_precalc_fin_temp

         -- WHERE xr.individual_id = ix.individual_id
         --   AND xr.individual_id = de.individual_id (+)
         --   AND xr.individual_id = abq.individual_id (+)
         --   AND xr.individual_id = fs.individual_id (+)
         --   AND xr.individual_id = ofs.individual_id (+)
         --   AND xr.individual_id = ols.individual_id (+)
         --   AND xr.individual_id = adv.individual_id (+)
         --   AND xr.individual_id = adv_mt.individual_id (+)
         --   AND xr.individual_id = advint.individual_id (+)
         --   AND xr.individual_id = advsur.individual_id (+)
         --   AND xr.individual_id = advdon.individual_id (+)
         --   AND xr.individual_id = advalrt.individual_id (+)
         --   AND xr.individual_id = cga.individual_id (+)
         --   AND xr.individual_id = a.individual_id (+)
         --   AND xr.individual_id = frc2.secd_indiv_id (+)
         --   AND xr.individual_id = er.individual_id (+)
         --   AND xr.individual_id = frc.prim_indiv_id (+)
         --   AND xr.individual_id = oaa.individual_id (+)
         --   AND xr.individual_id = ofo.individual_id (+)
         --   AND xr.individual_id = oli.individual_id (+)
         --   AND xr.individual_id = olo.individual_id (+)
         --   AND xr.individual_id = na.individual_id (+)
         --   AND xr.individual_id = bo.individual_id (+)
         --   AND xr.individual_id = bi.individual_id (+)
         --   AND xr.individual_id = bs.individual_id (+)
         --   AND xr.individual_id = fr.individual_id (+)
         --   AND xr.individual_id = ar.individual_id (+)
         --   AND xr.individual_id = fp.individual_id (+)
         --   AND xr.individual_id = flp.individual_id (+)
         --   AND xr.individual_id = abqd.individual_id (+)
         --   AND xr.individual_id = abqr.individual_id (+)
         --   AND xr.individual_id = brks.individual_id (+)
         --   AND xr.individual_id = e.individual_id (+)
         --   AND xr.individual_id = we.individual_id (+)
         --   AND xr.individual_id = an.individual_id (+)
         --   AND xr.individual_id = al.individual_id (+)
         --   AND xr.individual_id = ibx.individual_id (+)
         --   AND xr.individual_id = ol.individual_id (+)
         --   AND xr.individual_id = eua.individual_id (+) 
         --   AND xr.individual_id = bb.individual_id (+)
         --   AND xr.individual_id = pem.individual_id (+)
         --   AND xr.active_flag = 'A'
         --   AND xr.individual_id = ia.individual_id (+)
         --   AND xr.individual_id = erm.individual_id (+)
         --   AND xr.individual_id = coop.individual_id (+)
         --   AND xr.individual_id = app.individual_id (+)
         --   AND xr.individual_id = key.individual_id (+)
         --   AND xr.individual_id = eread.individual_id (+)
         --   AND xr.individual_id = surv.individual_id (+)
         --   AND xr.individual_id = page.individual_id (+)
         --   AND xr.individual_id = olixog2.individual_id (+)
         --   AND xr.individual_id = ofoxog.individual_id (+)
         --   AND xr.individual_id = olixog1.individual_id (+)
       

/*olixog1 >> ofoxog
ofoxog >> olixog1*/