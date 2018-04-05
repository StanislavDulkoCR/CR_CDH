  xr.individual_id
, xr.hh_id




FROM           
/*               cr_temp.indiv_ind_xref_temp xr,
               prod.agg_individual_xographic ix,
               prod.agg_abq_summary abq,
               prod.agg_fundraising_summary fs,
               prod.agg_print_summary ofs,
               prod.agg_digital_summary ols,
2017/10/18 11:55*/--cr_temp.indiv_precalc_1_temp

/*               (select distinct individual_id from prod.constituent ) adv,
               prod.agg_constituent adv_mt,
               (SELECT individual_id
                       , coa_date
                FROM prod.agg_name_address
                WHERE NVL(coa_source, 'XXX') != 'NCA') na,
               cr_temp.indiv_cga_temp cga,
               cr_temp.indiv_preference_temp pref_a,
               (SELECT DISTINCT secondary_individual_id
                  FROM prod.fundraising_combination) frc2,
               cr_temp.indiv_ext_ref_dt_temp er,
               (SELECT primary_individual_id
                       , max(join_eff_date) mx_eff_dt
                FROM prod.fundraising_combination
                GROUP BY primary_individual_id) frc,
               cr_temp.indiv_acct_merge_temp oaa,
               cr_temp.indiv_off_canc_temp ofo,
               cr_temp.indiv_oli_sku_temp oli,
               cr_temp.indiv_onl_item_ord_temp olo,
               CR_TEMP.INDIV_DE_EXPIRES_TEMP de,
               CR_TEMP.INDIV_books_ord_TEMP bo,
2017/10/18 11:55*/--cr_temp.indiv_precalc_2_temp

/*               --MISC
               (indiv_misc_agg_temp) bi,
               (indiv_misc_agg_temp) bs,
               (indiv_misc_agg_temp) fr,
               (indiv_misc_agg_temp) ar,
               (indiv_misc_agg_temp) fp,
               (indiv_misc_agg_temp) flp,
               (indiv_misc_agg_temp) abqd,
               (indiv_misc_agg_temp) abqr,
               --MISC
2017/10/18 11:59*/--cr_temp.indiv_misc_agg_temp

/*               cr_temp.indiv_print_lkp_temp brks,
               prod.agg_email e,
               (SELECT
                    ie.individual_id
                    , ie.email_address
                FROM prod.individual_email ie) we,
               (SELECT individual_id,
                       MAX(CASE WHEN GROUP_ID = '311' THEN 'Y' ELSE NULL END)          AS an_emp_flg,
                       MAX(CASE WHEN usr_src_ind LIKE 'FORUM%' THEN 'Y' ELSE NULL END) AS an_forums_flag
                  FROM prod.agg_account_number
                 WHERE acct_prefix = 'PWI'
                 GROUP BY individual_id) an,
               (SELECT --SD 20171017: Outside List are deprecated
                  cast(NULL AS BIGINT) AS individual_id
                , cast(NULL AS DATE)   AS ol_match_recent_dt
                , 0                    AS ol_match_cnt
                , 0                       ol_match_cnt_6m
                , 0                       ol_match_cnt_7_12m
                , 0                       ol_match_cnt_13_18m) ol,
               mt_alliant al, -- #TODO 20171017: Such a table does not exist
               cr_temp.indiv_ibx_temp ibx ,
                cr_temp.indiv_print_crdt_temp eua,
                cr_temp.indiv_bnb_temp bb,
2017/10/18 12:10*/--cr_temp.indiv_precalc_3_temp




/*                 (cr_temp.indiv_misc_agg_2_temp) app,
                 (cr_temp.indiv_misc_agg_2_temp) key, --cr_temp.indiv_emailfavkey_temp  
                 (cr_temp.indiv_misc_agg_2_temp) advint,
                 (cr_temp.indiv_misc_agg_2_temp) advsur,
                 (cr_temp.indiv_misc_agg_2_temp) advdon,
                 (cr_temp.indiv_misc_agg_2_temp) advalrt,
                 (cr_temp.indiv_misc_agg_2_temp) eread,
2017/10/18 12:30*/--cr_temp.indiv_misc_agg_2_temp


                     /*mt_pros_email_ind_temp pem,*/ -- #TODO 20171017: Table not used anywhere
/*               prod.individual_address ia,
               cr_temp.indiv_print_maglkp_temp erm,
                cr_temp.indiv_printsum_pref_temp coop,
                   cr_temp.indiv_survey_resp_dt_temp surv, -- mt_indiv_surv , -- #TODO 20171017: Not ready in CDH - [PH]
                    cr_temp.indiv_webpage_temp page,
                    cr_temp.indiv_oli_xog_2_temp olixog2,   -- mt_oli_xog_temp olixog,
                    cr_temp.indiv_oli_xog_1_temp olixog1,   -- mt_oli_aps_temp oliaps,
                    cr_temp.indiv_ofo_xog_temp ofoxog       -- mt_olo_xog_temp oloxog
2017/10/18 13:58*/--cr_temp.indiv_precalc_4_temp




         WHERE xr.individual_id = ix.individual_id
           AND xr.individual_id = de.individual_id (+)
           AND xr.individual_id = abq.individual_id (+)
           AND xr.individual_id = fs.individual_id (+)
           AND xr.individual_id = ofs.individual_id (+)
           AND xr.individual_id = ols.individual_id (+)
           AND xr.individual_id = adv.individual_id (+)
           AND xr.individual_id = adv_mt.individual_id (+)
           AND xr.individual_id = advint.individual_id (+)
           AND xr.individual_id = advsur.individual_id (+)
           AND xr.individual_id = advdon.individual_id (+)
           AND xr.individual_id = advalrt.individual_id (+)
           AND xr.individual_id = cga.individual_id (+)
           AND xr.individual_id = a.individual_id (+)
           AND xr.individual_id = frc2.secd_indiv_id (+)
           AND xr.individual_id = er.individual_id (+)
           AND xr.individual_id = frc.prim_indiv_id (+)
           AND xr.individual_id = oaa.individual_id (+)
           AND xr.individual_id = ofo.individual_id (+)
           AND xr.individual_id = oli.individual_id (+)
           AND xr.individual_id = olo.individual_id (+)
           AND xr.individual_id = na.individual_id (+)
           AND xr.individual_id = bo.individual_id (+)
           AND xr.individual_id = bi.individual_id (+)
           AND xr.individual_id = bs.individual_id (+)
           AND xr.individual_id = fr.individual_id (+)
           AND xr.individual_id = ar.individual_id (+)
           AND xr.individual_id = fp.individual_id (+)
           AND xr.individual_id = flp.individual_id (+)
           AND xr.individual_id = abqd.individual_id (+)
           AND xr.individual_id = abqr.individual_id (+)
           AND xr.individual_id = brks.individual_id (+)
           AND xr.individual_id = e.individual_id (+)
           AND xr.individual_id = we.individual_id (+)
           AND xr.individual_id = an.individual_id (+)
           AND xr.individual_id = al.individual_id (+)
           AND xr.individual_id = ibx.individual_id (+)
           AND xr.individual_id = ol.individual_id (+)
           AND xr.individual_id = eua.individual_id (+) 
           AND xr.individual_id = bb.individual_id (+)
           AND xr.individual_id = pem.individual_id (+)
           AND xr.active_flag = 'A'
           AND xr.individual_id = ia.individual_id (+)
           AND xr.individual_id = erm.individual_id (+)
           AND xr.individual_id = coop.individual_id (+)
           AND xr.individual_id = app.individual_id (+)
           AND xr.individual_id = key.individual_id (+)
           AND xr.individual_id = eread.individual_id (+)
           AND xr.individual_id = surv.individual_id (+)
           AND xr.individual_id = page.individual_id (+)
           AND xr.individual_id = olixog.individual_id (+)
           AND xr.individual_id = oloxog.individual_id (+)
           AND xr.individual_id = oliaps.individual_id (+)
       
 GROUP BY individual_id,
          ind_urn,
          hh_id,
          gender,
          customer_typ,
          lst_don_amt,
          ltd_don_amt,
          max_don_amt,
          fst_don_dt,
          lst_don_dt,
          ltd_don_cnt,
          non_prod_fst_dt,
          fst_non_prod_cd,
          non_prod_cnt,
          mrp_flg,
          fr_combined_flg,
          ind_combined_dt,
          ind_ltd_amt,
          comp_flg,
          ind_fst_rel_dt,
          ind_lst_act_dt,
          ind_actv_cnt,
          ind_relship_cnt,
          fst_prod_cd,
          ind_rec_prod_only_flg,
          emp_flg,
          cga_flg,
          news_car_watch_flg,
          news_best_buy_drug_flg,
          news_safety_alert_flg,
          news_hlth_alert_flg,
          news_cro_whats_flg,
          news_green_choice_flg,
          ind_dnr_only_flg,
          dc_cdb_last_ord_dt,
          prod_ltd_pd_amt,
          prod_fst_ord_dt,
          prod_lst_canc_bad_dbt_dt,
          non_prod_lst_act_dt,
          prod_lst_ord_dt,
          prod_ord_cnt,
          prod_dm_ord_cnt,
          prod_em_ord_cnt,
          prod_dnr_ord_cnt,
          prod_cnt,
          ad_apl_keycode,
          ipad_enabled_dt
;

