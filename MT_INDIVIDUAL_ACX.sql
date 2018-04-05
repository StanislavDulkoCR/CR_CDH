/***************************************************************************
*            (C) Copyright Acxiom Corporation 2006
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project:      Consumers Union Acxiom Database Solution - Middle Tier ETL
* Module Name:  middle_tier2.mt_individual.sql
* Date:         4/10/2006
***************************************************************************/

create table middle_tier2.mt_olo_xog_temp
as
select xo.individual_id, 
       max(case when xo.dc_online_first_dt is null and xo.dc_last_aps_ord_dt is null and xo.dc_cdb_last_ord_dt is null then NULL
                when NVL(to_char(trunc(xo.dc_corp_first_dt),'YYYYMMDD'),'00000000') = nvl(to_char(trunc(xo.dc_online_first_dt),'YYYYMMDD'),'99999999')
                  OR NVL(to_char(trunc(xo.dc_corp_first_dt),'YYYYMMDD'),'00000000') = nvl(to_char(trunc(xo.dc_last_aps_ord_dt),'YYYYMMDD'),'99999999') 
                  OR NVL(to_char(trunc(xo.dc_corp_first_dt),'YYYYMMDD'),'00000000') = nvl(to_char(trunc(xo.dc_cdb_last_ord_dt),'YYYYMMDD'),'99999999')then 1 else 0 end) as dc_corp_first_ind,
       max(case when xo.dc_online_first_dt is null and xo.dc_offline_first_dt is null and xo.dc_last_aps_ord_dt is null and xo.dc_cdb_last_ord_dt is null then NULL
                when NVL(to_char(trunc(xo.dc_corp_first_dt),'YYYYMMDD'),'00000000') = nvl(to_char(trunc(xo.dc_online_first_dt),'YYYYMMDD'),'99999999')
                  OR NVL(to_char(trunc(xo.dc_corp_first_dt),'YYYYMMDD'),'00000000') = nvl(to_char(trunc(xo.dc_offline_first_dt),'YYYYMMDD'),'99999999') 
                  OR NVL(to_char(trunc(xo.dc_corp_first_dt),'YYYYMMDD'),'00000000') = nvl(to_char(trunc(xo.dc_last_aps_ord_dt),'YYYYMMDD'),'99999999') 
                  OR NVL(to_char(trunc(xo.dc_corp_first_dt),'YYYYMMDD'),'00000000') = nvl(to_char(trunc(xo.dc_cdb_last_ord_dt),'YYYYMMDD'),'99999999')then 1 else 0 end) as dc_corp_first_ind2,
       max(case when olo.ord_dt is null then NULL
                when NVL(to_char(trunc(xo.dc_offline_first_dt),'YYYYMMDD'),'00000000') = nvl(to_char(trunc(olo.ord_dt),'YYYYMMDD'),'99999999') then 1 else 0 end) as dc_offline_fst_crt_ind,
       max(case when olo.ord_dt is null then NULL
                when NVL(to_char(trunc(xo.dc_corp_first_dt),'YYYYMMDD'),'00000000') = nvl(to_char(trunc(olo.ord_dt),'YYYYMMDD'),'99999999') then 1 else 0 end) as dc_corp_fst_crt_ind            
from warehouse2.individual_xographic xo,
     middle_tier2.mt_offline_ord olo
where xo.individual_id = olo.individual_id (+)
group by xo.individual_id;

create table middle_tier2.mt_oli_aps_temp
as
select xo.individual_id, 
       max(case when oli.crt_dt is null then NULL
                when NVL(to_char(trunc(xo.dc_last_aps_ord_dt),'YYYYMMDD'),'00000000') = nvl(to_char(trunc(oli.crt_dt),'YYYYMMDD'),'99999999') then 1 else 0 end) as dc_last_aps_crt_ind
from warehouse2.individual_xographic xo,
     middle_tier2.mt_online_item oli
where xo.individual_id = oli.individual_id (+)
  and oli.mag_cd in ('NCBK','UCBK','NCPR','UCPR')
group by xo.individual_id;

create table middle_tier2.mt_oli_xog_temp
as
select xo.individual_id, 
       max(case when oli.crt_dt is null then NULL 
                when NVL(to_char(trunc(xo.dc_online_first_dt),'YYYYMMDD'),'00000000') = nvl(to_char(trunc(oli.crt_dt),'YYYYMMDD'),'99999999') then 1 else 0 end) as dc_online_fst_crt_ind,
       max(case when xo.dc_last_aps_ord_dt is null then NULL
                when NVL(to_char(trunc(xo.dc_online_first_dt),'YYYYMMDD'),'00000000') = nvl(to_char(trunc(xo.dc_last_aps_ord_dt),'YYYYMMDD'),'99999999') then 1 else 0 end) as dc_online_fst_aps_ind
from warehouse2.individual_xographic xo,
     middle_tier2.mt_online_item oli
where xo.individual_id = oli.individual_id (+)
group by xo.individual_id;

create table middle_tier2.mt_indiv_surv
as
select individual_id, max(answer_date) answer_date, min(answer_date) min_answer_date
from warehouse2.survey_response_fact
where substr(individual_id,1,1) = 1
group by individual_id;

create table middle_tier2.mt_indiv_page_temp
as
select individual_id, max(hit_time) hit_time, min(hit_time) min_hit_time
from warehouse2.web_page
where event_list_id = 233
group by individual_id;

create table email_fav_key
 as
SELECT ind_urn,
       individual_id,
       DENSE_RANK () OVER (ORDER BY email_id) email_fav_key
 FROM middle_tier2.mt_EMAIL
WHERE email_type_cd = 'I';
       
CREATE TABLE middle_tier2.mt_individual_ibx
  AS
SELECT individual_id,
       ROUND(MONTHS_BETWEEN(SYSDATE,
                            TO_DATE(SUBSTR(ib8624_dob_ip_ind_dflt_1st_ind,1,4) ||
                                    LPAD(DECODE(SUBSTR(ib8624_dob_ip_ind_dflt_1st_ind,5,2), '00',TRUNC(dbms_random.value(1, 13)),
                                                SUBSTR(ib8624_dob_ip_ind_dflt_1st_ind,5,2)),2,'0')||
                                    LPAD(DECODE(SUBSTR(ib8624_dob_ip_ind_dflt_1st_ind,7,2), '00',TRUNC(dbms_random.value(1, 29)),
                                                SUBSTR(ib8624_dob_ip_ind_dflt_1st_ind,7,2)),2,'0'),
                                    'YYYYMMDD')) / 12) AS age_infobase,
       ib8688_gender_input_ind AS gender_infobase
  FROM middle_tier2.mt_infobase_x
;

CREATE TABLE middle_tier2.mt_individual
AS
SELECT individual_id,
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
       NVL(fst_non_prod_cd,'NONE') fst_non_prod_cd,
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
       NVL(fst_prod_cd,'NONE') fst_prod_cd,
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
       MAX(ind_email_summary_flag)   AS ind_email_summary_flag,
       MAX(mrp_email_summary_flag)   AS mrp_email_summary_flag,
       MAX(news_email_summary_flag)  AS news_email_summary_flag,
       MAX(adv_ever)                 AS adv_ever,
       MAX(adv_active)               AS adv_active,
       MAX(verity_recency_flag)      AS verity_recency_flag,
       MAX(discussions_forum_flag)   AS discussions_forum_flag,
       MAX(news_hlth_heart_flg)      AS news_hlth_heart_flg,
       MAX(news_hlth_child_teen_flg) AS news_hlth_child_teen_flg,
       MAX(news_hlth_diabetes_flg)   AS news_hlth_diabetes_flg,
       MAX(news_hlth_women_flg)      AS news_hlth_women_flg,
       MAX(news_hlth_after_60_flg)   AS news_hlth_after_60_flg,
       MAX(age_infobase)             AS age_infobase,
       MAX(gender_infobase)          AS gender_infobase,
       MAX(NVL(ind_fst_rel_cd,'UNK'))           AS ind_fst_rel_cd,
       ipad_enabled_dt,
       MAX(bnb_frst_visitor_dt)      AS bnb_frst_visitor_dt,
       MAX(bnb_last_visitor_dt)      AS bnb_last_visitor_dt,
       MAX(bnb_frst_prospect_dt)     AS bnb_frst_prospect_dt,
       MAX(bnb_last_prospect_dt)     AS bnb_last_prospect_dt,
       MAX(bnb_frst_sales_dt)        AS bnb_frst_sales_dt,
       MAX(bnb_last_sales_dt)        AS bnb_last_sales_dt,
       MAX(bnb_tot_prospect_cnt)     AS bnb_tot_prospect_cnt,
       MAX(bnb_tot_sale_cnt)         AS bnb_tot_sale_cnt,
       MAX(bnb_best_status)          AS bnb_best_status,
       MAX(bnb_best_status_dt)       AS bnb_best_status_dt,
       MAX(bnb_most_recent_status)   AS bnb_most_recent_status,
       MAX(bnb_most_recent_dt)       AS bnb_most_recent_dt,
       MAX(bnb_nbr_new_sales)        AS bnb_nbr_new_sales,
       MAX(bnb_nbr_used_sales)       AS bnb_nbr_used_sales,
       MAX(bnb_most_recent_trans_type)           AS bnb_most_recent_trans_type,
       MAX(ol_match_recent_dt)       AS ol_match_recent_dt,
       MAX(ol_match_cnt)             AS ol_match_cnt,
       MAX(ol_match_cnt_6m)          AS ol_match_cnt_6m,
       MAX(ol_match_cnt_7_12m)       AS ol_match_cnt_7_12m,
       MAX(ol_match_cnt_13_18m)      AS ol_match_cnt_13_18m,
       NVL(MAX(IND_PROSPECT_EML_MTCH_IND),'C') AS IND_PROSPECT_EML_MTCH_IND,
       MAX(PROSPECT_EMAIL_MATCH_DT)   AS PROSPECT_EMAIL_MATCH_DT,
       MAX(PROSPECT_EMAIL_MATCH_CNT)  AS PROSPECT_EMAIL_MATCH_CNT,
       MAX(CAN_SPAM_FLG)              AS CAN_SPAM_FLG,
       MAX(print_sub_coop_flg)        AS print_sub_coop_flg,
       MAX(mobile_app_usage_ind)      AS mobile_app_usage_ind,
       MAX(rtg_app_days_since_fst_act) AS rtg_app_days_since_fst_act,
       MAX(rtg_app_days_since_lst_act) AS rtg_app_days_since_lst_act,
       MAX(email_fav_key) AS email_fav_key,
       MAX(adv_cnt) AS adv_cnt
  FROM (
        SELECT xr.individual_id,
               xr.ind_urn,
               xr.hh_id,
               ix.acx_gender_cd         AS gender,
               ix.customer_typ,
               to_number(SUBSTR(GREATEST(NVL(TO_CHAR(fs.fr_lst_don_dt,  'YYYYMMDD'),'00010101') || '1' || to_char(fs.fr_lst_don_amt),
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
           SUBSTR(LEAST(NVL(TO_CHAR(fs.fr_fst_don_dt,       'YYYYMMDD'),'99990601') || '1' || 'FR',
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
             to_date(substr(LEAST(NVL(to_char(bo.bo_min_ord_dt      ,'YYYYMMDDHH24MISS'),'99990601000000')||to_char(bo.bo_min_ord_dt      ,'YYYYMMDDHH24MISS') 
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
         substr(LEAST(NVL(ofo.ofo_fst_prod_cd,                                '99991231'||'99')
                           ,NVL(de.de_fst_prod_cd,                                  '99991231'||'99')
                           ,NVL(oli.oli_fst_prod_cd,                                '99991231'||'99')
                           ,NVL(to_char(ix.dc_corp_first_dt,    'YYYYMMDD'),'99991231')||'C0'||'CNS'
                           ,NVL(to_char(ix.dc_offline_first_dt, 'YYYYMMDD'),'99991231')||'60'||'CNS'
                           ,NVL(to_char(ix.dc_fund_first_dt,    'YYYYMMDD'),'99991231')||'C0'||'CNS'
                           ,NVL(to_char(ix.dc_online_first_dt,  'YYYYMMDD'),'99991231')||'80'||'CRO'
                           ,NVL(to_char(ix.dc_last_aps_ord_dt,  'YYYYMMDD'),'99991231')||'70'||'NCPR'
                           ,NVL(bo.bo_fst_prod_cd,                                  '99991231'||'99')
                           ,NVL(to_char(ix.dc_cdb_last_ord_dt,  'YYYYMMDD'),'99991231')||'B0'||'CDR'
                           ,NVL(to_char(fs.fr_fst_don_dt,       'YYYYMMDD'),'99991231')||substr(least(NVL(to_char(ix.dc_corp_first_dt,    'YYYYMMDD'),'99991231')||'D5'||'CNS'
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


               
               SUBSTR(LEAST(NVL(ofo.ofo_fst_prod_cd,'99991231'||'99'),
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
                            NVL(to_char(fs.fr_fst_don_dt,       'YYYYMMDD'),'99991231')||substr(least(NVL(to_char(ix.dc_corp_first_dt,    'YYYYMMDD'),'99991231')||'C5'||'CNS'
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
               NVL(bo.bo_prod_ord_cnt,0) + NVL(ofo.ofo_prod_ord_cnt,0) + NVL(de.de_cnt,0)
               + NVL(oli.oli_xce_cnt,0) + NVL2(ix.dc_cdb_last_ord_dt,1,0) + NVL(eread.ereader_cnt,0)
               + CASE WHEN ix.dc_offline_first_dt IS NOT NULL AND NVL(oloxog.dc_offline_fst_crt_ind,0) = 0 THEN 1 ELSE 0 END
               + CASE WHEN ix.dc_corp_first_dt IS NOT NULL AND NVL(oloxog.dc_corp_first_ind2,0) = 0 THEN 1 ELSE 0 END         
               + CASE WHEN ix.dc_online_first_dt IS NOT NULL AND NVL(olixog.dc_online_fst_crt_ind,0) = 0 AND NVL(olixog.dc_online_fst_aps_ind,0) = 0 THEN 1 ELSE 0 END
               + CASE WHEN ix.dc_last_aps_ord_dt IS NOT NULL AND NVL(oliaps.dc_last_aps_crt_ind,0) = 0 THEN 1 ELSE 0 END AS prod_ord_cnt,
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
               CASE WHEN al.m_hh_tot_tot_dayslast_order BETWEEN 1 AND 730 OR al.m_hh_tot_tot_dayslast_paymen BETWEEN 1 AND 730 THEN 'A'
                    WHEN al.m_hh_tot_tot_dayslast_order > 730 OR al.m_hh_tot_tot_dayslast_paymen > 730 THEN 'B'
               END AS verity_recency_flag,
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
          FROM ind_xref xr,
               warehouse2.individual_xographic ix,
               middle_tier2.mt_abq_summary abq,
               middle_tier2.mt_fr_summary fs,
               middle_tier2.mt_offline_summary ofs,
               middle_tier2.mt_online_summary ols,
               warehouse2.advocacy adv,
               middle_tier2.mt_advocacy adv_mt,
               (SELECT individual_id,
                       coa_date
                  FROM middle_tier2.mt_name_address
                 WHERE NVL(coa_source,'XXX') != 'NCA'
               ) na,
               (SELECT individual_id,
                       MIN(don_dt)  AS mn_don_dt,
                       MAX(don_dt)  AS mx_don_dt,
                       COUNT(*)     AS cga_cnt,
                       SUM(don_amt) AS cga_don_amt
                  FROM middle_tier2.mt_cga
                 GROUP BY individual_id
               ) cga,
               (SELECT individual_id,
                       MIN(CASE WHEN scp_cd = 'PNL' THEN fst_dt ELSE NULL END) AS mn_fst_dt,
                       MAX(CASE WHEN scp_cd = 'PNL' THEN fst_dt ELSE NULL END) AS mx_fst_dt,
                       COUNT(CASE WHEN scp_cd = 'PNL' THEN 'x' ELSE NULL END) AS auth_cnt,
                       MAX(CASE WHEN scp_cd = 'PNL' AND auth_cd = 'EMAIL' THEN auth_flg ELSE NULL END) AS mrp_flg,
                       COUNT(CASE WHEN scp_cd = 'PNL' AND auth_flg = 'Y' THEN 'x' ELSE NULL END) AS auth_flg_cnt,
                       MAX(CASE WHEN scp_cd = 'PNL' THEN eff_dt ELSE NULL END) AS mx_eff_dt,
                       MIN(CASE WHEN scp_cd LIKE 'NEW%' AND TO_CHAR(fst_dt,'YYYYMMDD') != '19000101' THEN fst_dt ELSE NULL END) AS mn_news_fst_dt,
                       MIN(CASE WHEN scp_cd LIKE 'NEW%' THEN fst_dt ELSE NULL END) AS mn_newsltr_fst_dt,
                       MAX(CASE WHEN scp_cd LIKE 'NEW%' AND to_char(fst_dt,'YYYYMMDD') != '19000101' THEN fst_dt ELSE NULL END) AS mx_news_fst_dt,
                       MAX(CASE WHEN scp_cd LIKE 'NEW%' THEN fst_dt ELSE NULL END) AS mx_newsltr_fst_dt,
                       MAX(CASE WHEN scp_cd LIKE 'NEW%' THEN eff_dt ELSE NULL END) AS mx_news_eff_dt,
                       COUNT(CASE WHEN scp_cd LIKE 'NEW%' THEN 'x' ELSE NULL END)  AS news_auth_cnt,
                       COUNT(CASE WHEN scp_cd LIKE 'NEW%' AND auth_flg = 'Y' THEN 'x' ELSE NULL END) AS news_auth_y_cnt,
                       MAX(CASE WHEN scp_cd LIKE 'NEW%'  AND auth_flg = 'N' THEN eff_dt ELSE NULL END) AS mx_news_auth_n_eff_dt,
                       MAX(CASE WHEN scp_cd = 'NEWCRAUTO'     AND auth_flg = 'Y' THEN 'Y'
                                WHEN scp_cd = 'NEWCRAUTO'     AND auth_flg = 'N' THEN 'N' ELSE NULL END) AS news_car_watch_flg,
                       MAX(CASE WHEN scp_cd = 'NEWHNBBD'      AND auth_flg = 'Y' THEN 'Y' 
                                WHEN scp_cd = 'NEWHNBBD'      AND auth_flg = 'N' THEN 'N' ELSE NULL END) AS news_best_buy_drug_flg,
                       MAX(CASE WHEN scp_cd = 'NEWCRALERT'    AND auth_flg = 'Y' THEN 'Y'
                                WHEN scp_cd = 'NEWCRALERT'    AND auth_flg = 'N' THEN 'N' ELSE NULL END) AS news_safety_alert_flg,
                       MAX(CASE WHEN scp_cd = 'NEWCRMHANLP'   AND auth_flg = 'Y' THEN 'Y'
                                WHEN scp_cd = 'NEWCRMHANLP'   AND auth_flg = 'N' THEN 'N' ELSE NULL END) AS news_hlth_alert_flg,
                       MAX(CASE WHEN scp_cd = 'NEWWHATS'      AND auth_flg = 'Y' THEN 'Y'
                                WHEN scp_cd = 'NEWWHATS'      AND auth_flg = 'N' THEN 'N' ELSE NULL END) AS news_cro_whats_flg,
                       MAX(CASE WHEN scp_cd = 'NEWGRNCHC'     AND auth_flg = 'Y' THEN 'Y'
                                WHEN scp_cd = 'NEWGRNCHC'     AND auth_flg = 'N' THEN 'N' ELSE NULL END) AS news_green_choice_flg,
                       MAX(CASE WHEN scp_cd = 'NEWHHEART'     AND auth_flg = 'Y' THEN 'Y'
                                WHEN scp_cd = 'NEWHHEART'     AND auth_flg = 'N' THEN 'N' ELSE NULL END) AS news_hlth_heart_flg,
                       MAX(CASE WHEN scp_cd = 'NEWHCHILDTEEN' AND auth_flg = 'Y' THEN 'Y'
                                WHEN scp_cd = 'NEWHCHILDTEEN' AND auth_flg = 'N' THEN 'N' ELSE NULL END) AS news_hlth_child_teen_flg,
                       MAX(CASE WHEN scp_cd = 'NEWHDIABETES'  AND auth_flg = 'Y' THEN 'Y'
                                WHEN scp_cd = 'NEWHDIABETES'  AND auth_flg = 'N' THEN 'N' ELSE NULL END) AS news_hlth_diabetes_flg,
                       MAX(CASE WHEN scp_cd = 'NEWHWOMEN'     AND auth_flg = 'Y' THEN 'Y'
                                WHEN scp_cd = 'NEWHWOMEN'     AND auth_flg = 'N' THEN 'N' ELSE NULL END) AS news_hlth_women_flg,
                       MAX(CASE WHEN scp_cd = 'NEWHAFTER60'   AND auth_flg = 'Y' THEN 'Y'
                                WHEN scp_cd = 'NEWHAFTER60'   AND auth_flg = 'N' THEN 'N' ELSE NULL END) AS news_hlth_after_60_flg
                  FROM middle_tier2.mt_authorization
                 WHERE data_source = 'DGI'
                   AND scp_cd IN ('PNL','NEWCRAUTO','NEWHNBBD','NEWCRALERT', 'NEWCRMGHA','NEWCRMHANLP','NEWWHATS','NEWGRNCHC',
                                  'NEWHHEART','NEWHCHILDTEEN','NEWHDIABETES','NEWHWOMEN','NEWHAFTER60')
                 GROUP BY individual_id
               ) a,
               (SELECT DISTINCT secd_indiv_id
                  FROM warehouse2.fundraising_combination
               ) frc2,
               (SELECT individual_id,
                       MAX(er_fst_dt) AS er_fst_dt
                  FROM (SELECT individual_id,
                               SUBSTR(external_key,1,3),
                               MAX(first_date) er_fst_dt
                          FROM warehouse2.external_ref
                         WHERE SUBSTR(external_key,1,3) IN ('CNS','CRM','CRH','CRE','SHM')
                         GROUP BY individual_id,
                                  SUBSTR(external_key,1,3)
                        HAVING COUNT(*) > 1
                       )
                 GROUP BY individual_id
               ) er,
               (SELECT prim_indiv_id,
                       MAX(eff_dt) mx_eff_dt
                  FROM warehouse2.fundraising_combination
                 GROUP BY prim_indiv_id
               ) frc,
               (SELECT er.individual_id,
                       MAX(t1.merge_dt) AS mx_merge_dt
                  FROM warehouse2.external_ref er,
                       warehouse2.offline_account_arch t1
                 WHERE SUBSTR(er.external_key,1,13) = t1.new_acct
                 GROUP BY er.individual_id
               ) oaa,
               (SELECT t1.individual_id,
                       SUM(t1.pd_amt) AS ofo_pd_amt,
                       MAX(CASE WHEN t1.stat_cd = 'B' AND t1.src_cd IN ('CA','CC') AND SUBSTR(t1.keycode,1,4) != 'LIFE' THEN t1.ord_dt
                                WHEN t1.stat_cd = 'B' AND t1.src_cd IN ('CA','CC') AND t1.entr_typ_cd = 'F'             THEN t1.ord_dt
                                ELSE NULL END) AS ofo_mx_cc_ord_dt,
                       MAX(t1.pmt_dt) AS ofo_mx_pmt_dt,
                       MIN(t1.ord_dt) AS ofo_mn_ord_dt,
                       COUNT(CASE WHEN NVL(t1.set_cd,'X') IN ('C','E') THEN 'x' ELSE NULL END) AS ofo_ce_cnt,
                       COUNT(CASE WHEN NVL(t1.set_cd,'X') NOT IN ('C','E') THEN 'x' ELSE NULL END) AS ofo_xce_cnt,
                       MAX(t1.ord_dt) AS ofo_mx_ord_dt,
                       MAX(CASE WHEN NVL(oca.canc_rsn_cd,'X') NOT IN ('06','14','50') THEN t1.canc_dt ELSE NULL END) AS ofo_mx_canc_dt,
                       MIN(CASE WHEN NVL(t1.set_cd,'X') IN ('C','E') THEN NVL(TO_CHAR(t1.ord_dt,'YYYYMMDD'),'99991231')||'2'||decode(t1.mag_cd,'CNS',1,'CRH',2,'CRM',3,'SHM',4,'CRT',5,6)||t1.mag_cd||' REC'
                                ELSE NVL(TO_CHAR(t1.ord_dt,'YYYYMMDD'),'99991231')||'1'||decode(t1.mag_cd,'CNS',1,'CRH',2,'CRM',3,'SHM',4,'CRT',5,6)||t1.mag_cd END)    AS ofo_fst_prod_cd,
                       SUM(CASE WHEN NVL(t1.mag_cd,'X') != 'CRT' THEN t1.pd_amt ELSE NULL END)          AS ofo_prod_ltd_pd_amt,
                       MIN(CASE WHEN nvl(t1.KEYCODE,'x') not in ('ZFREE', 'VFREE') THEN t1.ord_dt END) AS ofo_prod_fst_ord_dt,
                       MAX(CASE WHEN nvl(t1.KEYCODE,'x') not in ('ZFREE', 'VFREE') THEN t1.ord_dt END) AS ofo_prod_lst_ord_dt,
                      MAX(CASE WHEN t1.canc_rsn_cd IN ('06','50') AND NVL(t1.mag_cd,'X') != 'CRT'
                                THEN t1.canc_dt ELSE NULL END) AS ofo_prod_lst_canc_bad_dbt_dt,
                        COUNT(CASE WHEN NVL(t1.set_cd,'X') NOT IN ('C','E') AND NVL(t1.mag_cd,'X') != 'CRT' and nvl(t1.KEYCODE,'x') not in ('ZFREE', 'VFREE')
                                  THEN 'x' ELSE NULL END) AS ofo_prod_ord_cnt,
                       COUNT(CASE WHEN t1.mag_cd = 'CNS' AND t1.set_cd IN ('B','D') THEN 1 END) AS ofo_cr_prod_cnt,
                       COUNT(CASE WHEN t1.mag_cd = 'CRH' AND t1.set_cd IN ('B','D') THEN 1 END) AS ofo_hl_prod_cnt,
                       COUNT(CASE WHEN t1.mag_cd = 'CRM' AND t1.set_cd IN ('B','D') THEN 1 END) AS ofo_ma_prod_cnt,
                       COUNT(CASE WHEN t1.mag_cd = 'SHM' AND t1.set_cd IN ('B','D') THEN 1 END) AS ofo_shm_prod_cnt,
                       COUNT(CASE WHEN t1.set_cd IN ('B','D') AND NVL(t1.mag_cd,'X') != 'CRT' THEN 1 END) AS ofo_prod_dnr_ord_cnt,
                       COUNT(CASE WHEN NVL(t1.set_cd,'X') NOT IN ('C','E') AND t1.mag_cd IN ('CNS','CRH','CRM','SHM')
                                   AND ((SUBSTR(t1.keycode, 1,1) IN ('A','G','X'))
                                    OR  (SUBSTR(t1.keycode,1,1) = 'D' AND NVL(SUBSTR(t1.keycode,2,1),'X') != 'N')
                                    OR  (SUBSTR(t1.keycode,1,1) = 'B' AND NVL(SUBSTR(t1.keycode,9,1),'X') NOT IN ('A','B','C','S'))
                                    OR  (SUBSTR(t1.keycode,1,1) = 'U' AND REGEXP_INSTR(NVL(SUBSTR(t1.keycode,9,1),'1'),'[^[:alpha:]]') = 1)
                                    OR  (SUBSTR(t1.keycode,1,1) = 'R' AND NVL(SUBSTR(t1.keycode,9,1),'X') NOT IN ('B','C','D','E','F','T'))
                                    OR  (SUBSTR(t1.keycode,1,1) = 'U' AND LENGTH(t1.keycode) = 7 AND REGEXP_INSTR(SUBSTR(t1.keycode,6,1),'[^[:alpha:]]') = 1))
                                  THEN 1 END) AS ofo_prod_dm_ord_cnt,
                       COUNT(CASE WHEN NVL(t1.set_cd,'X') NOT IN ('C','E') AND t1.mag_cd IN ('CNS','CRH','CRM','SHM')
                                   AND ((SUBSTR(t1.keycode,1,1) = 'E')
                                    OR  (SUBSTR(t1.keycode,1,1) = 'D' AND SUBSTR(t1.keycode, 2, 1) = 'N')
                                    OR  (SUBSTR(t1.keycode,1,1) = 'B' AND SUBSTR(t1.keycode, 9, 1) IN ('A','B','C','S'))
                                    OR  (SUBSTR(t1.keycode,1,1) = 'U' AND REGEXP_INSTR(SUBSTR(t1.keycode,9,1),'[[:alpha:]]') = 1)
                                    OR  (SUBSTR(t1.keycode,1,1) = 'R' AND SUBSTR(t1.keycode, 9, 1) IN ('B','C','D','E','F'))
                                    OR  (SUBSTR(t1.keycode,4,6) = 'FAILCC')
                                    OR  (SUBSTR(t1.keycode,1,1) = 'U' AND LENGTH(t1.keycode) = 7 AND SUBSTR(t1.keycode,6,1) IN ('A','B','C','D','E','F')))
                                  THEN 1 END) AS ofo_prod_em_ord_cnt
                  FROM middle_tier2.mt_offline_ord t1,
                       warehouse2.offline_cancel_act oca
                 WHERE t1.ord_id = oca.ord_id (+)
                   AND t1.acct_id = oca.acct_id (+)
                 GROUP BY individual_id
               ) ofo,
               (SELECT t1.individual_id,
                       SUM(CASE WHEN NVL(t1.sku_num,'1') < '5000000' THEN t1.tot_amt ELSE NULL END)             AS oli_tot_amt,
                       SUM(CASE WHEN t1.sku_num > '5000000' AND t1.stat_cd = 'C' THEN t1.tot_amt ELSE NULL END) AS oli_tot_amt2,
                       MIN(CASE WHEN NVL(t1.set_cd,'X') NOT IN ('C','E') THEN t1.crt_dt ELSE NULL END)          AS olo_prod_fst_ord_dt,
                       MAX(CASE WHEN NVL(t1.set_cd,'X') NOT IN ('C','E') THEN t1.crt_dt ELSE NULL END)          AS olo_prod_lst_ord_dt,
                       MAX(t1.pmt_dt) AS oli_mx_pmt_dt,
                       MIN(t1.crt_dt) as oli_mn_crt_dt,
                       MAX(t1.crt_dt) as oli_mx_crt_dt,
                       COUNT(CASE WHEN NVL(t1.set_cd,'X') IN ('C','E') THEN 'x' ELSE NULL END)                  AS oli_ce_cnt,
                       COUNT(CASE WHEN NVL(t1.set_cd,'X') NOT IN ('C','E') THEN 'x' ELSE NULL END)              AS oli_xce_cnt,
                       COUNT(CASE WHEN NVL(t1.set_cd,'X') NOT IN ('C','E') AND t1.sub_src_cd = 'D' THEN 'x' ELSE NULL END) AS oli_xce_d_cnt,
                       COUNT(CASE WHEN NVL(t1.set_cd,'X') NOT IN ('C','E') AND t1.sub_src_cd in ('E','G','K','N','U') THEN 'x' ELSE NULL END) AS oli_xce_egknu_cnt,
                       MAX(CASE WHEN t1.stat_cd = 'R' THEN t1.canc_dt ELSE NULL END)                            AS oli_mx_canc_dt,
                       MIN(CASE WHEN NVL(set_cd,'X') IN ('C','E') THEN NVL(TO_CHAR(crt_dt,'YYYYMMDD'),'99991231')||'5'||decode(mag_cd,'CRO',0,'CRMG',1,'HL',2,'NCBK',3,'UCBK',4,'NCPR',5,'UCPR',6,'CAPS',7,'CARP',8,9)||mag_cd||' REC'
                                ELSE NVL(TO_CHAR(crt_dt,'YYYYMMDD'),'99991231')||'4'||decode(mag_cd,'CRO',0,'CRMG',1,'HL',2,'NCBK',3,'UCBK',4,'NCPR',5,'UCPR',6,'CAPS',7,'CARP',8,9)||mag_cd END) AS oli_fst_prod_cd,
                       MAX(CASE WHEN crd_stat_cd = 'F' OR (svc_stat_cd = 'C' AND canc_rsn_cd IN ('50','06')) 
                                THEN canc_dt ELSE NULL END)                                                     AS oli_prod_lst_canc_bad_dbt_dt,
                       COUNT(DISTINCT CASE WHEN mag_cd = 'CRO' AND set_cd = 'A' THEN 1 END)
                       + COUNT(cro_break_flg)
                       + COUNT(CASE WHEN mag_cd = 'CRO' AND set_cd IN ('B','D') THEN 1 END)                     AS oli_cro_prod_cnt,
                       COUNT(DISTINCT CASE WHEN mag_cd = 'CARP' AND set_cd = 'A' THEN 1 END)
                       + COUNT(carp_break_flg)
                       + COUNT(CASE WHEN mag_cd = 'CARP' AND set_cd IN ('B','D') THEN 1 END)                    AS oli_carp_prod_cnt,
                       COUNT(DISTINCT CASE WHEN mag_cd = 'CRMG' AND NVL(set_cd,'A') = 'A' THEN 1 END)
                       + COUNT(crmg_break_flg)
                       + COUNT(CASE WHEN mag_cd = 'CRMG' AND set_cd IN ('B','D') THEN 1 END)                    AS oli_crmg_prod_cnt,
                       COUNT(CASE WHEN mag_cd IN ('NCBK','UCBK','NCPR','UCPR')   THEN 1 END)                    AS oli_aps_prod_cnt,
                       COUNT(CASE WHEN mag_cd = 'CARP' THEN 1 END)                                              AS oli_carp_cnt,
                       MAX(CASE WHEN mag_cd = 'CRO'  AND set_cd = 'A' THEN 1 ELSE 0 END)                        AS oli_cro_prod_cnta,
                       COUNT(CASE WHEN set_cd IN ('B','D') THEN 1 END)                                          AS oli_prod_dnr_ord_cnt,
                      MAX(CASE WHEN canc_dt IS null THEN end_dt
                               WHEN canc_dt IS NOT null AND sub_rnw_ind IN ('Y','N') THEN end_dt END)           AS oli_mx_end_dt
                 FROM (SELECT individual_id,
                              tot_amt,
                              pmt_dt,
                              set_cd,
                              stat_cd,
                              canc_dt,
                              crt_dt,
                              mag_cd,
                              crd_stat_cd,
                              sku_num,
                              canc_rsn_cd,
                              svc_stat_cd,
                              sub_src_cd,
                              end_dt,
                              sub_rnw_ind,
                              CASE WHEN SUBSTR(NVL(ext_keycd, int_keycd),1,2) = 'WF' THEN NULL
                                   WHEN mag_cd = 'CRO' AND set_cd = 'A'
                                    AND MONTHS_BETWEEN(strt_dt, MAX(end_dt)
                                               OVER (PARTITION BY individual_id, mag_cd, set_cd
                                                     ORDER BY strt_dt, NVL(ext_keycd, int_keycd) desc
                                                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) > 2 THEN 1 END AS cro_break_flg,
                              CASE WHEN SUBSTR(NVL(ext_keycd, int_keycd),1,2) = 'WF' THEN NULL
                                   WHEN mag_cd = 'CARP' AND set_cd = 'A'
                                    AND MONTHS_BETWEEN(strt_dt, MAX(end_dt)
                                               OVER (PARTITION BY individual_id, mag_cd, set_cd
                                                     ORDER BY strt_dt, NVL(ext_keycd, int_keycd) desc
                                                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) > 2 THEN 1 END AS carp_break_flg,
                              CASE WHEN SUBSTR(NVL(ext_keycd, int_keycd),1,2) = 'WF' THEN NULL
                                   WHEN mag_cd = 'CRMG' AND NVL(set_cd,'A') = 'A'
                                    AND MONTHS_BETWEEN(strt_dt, MAX(end_dt)
                                               OVER (PARTITION BY individual_id, mag_cd, NVL(set_cd,'A')
                                                     ORDER BY strt_dt, NVL(ext_keycd, int_keycd) desc
                                                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) > 2 THEN 1 END AS crmg_break_flg
                         FROM middle_tier2.mt_online_item
                       ) t1,
                       warehouse2.sku_lkp sku
                 WHERE t1.sku_num = sku.sku_num (+)
                 GROUP BY t1.individual_id
               ) oli,
               (SELECT ot.individual_id,
                       MAX(CASE WHEN ot.entr_typ_cd = 'F' AND NVL(it.strt_dt,TO_DATE('00010101','YYYYMMDD')) < SYSDATE
                                 AND SYSDATE < NVL(it.end_dt,TO_DATE('99991231','YYYYMMDD')) THEN ot.crt_dt ELSE NULL END) AS olo_mx_crt_dt,
                       MIN(CASE WHEN NVL(it.crd_stat_cd,'X') != 'F' AND ot.individual_id= it.individual_id
                                 AND ot.acct_id= it.acct_id AND ot.ord_id= it.ord_id THEN ot.ord_dt ELSE NULL END) AS olo_mn_ord_dt,
                       MAX(ot.ord_dt) AS olo_mx_ord_dt,
                       COUNT(DISTINCT CASE WHEN NVL(it.set_cd,'X') NOT IN ('C','E') AND ot.acct_id = it.acct_id AND ot.ord_id = it.ord_id
                                           THEN ot.individual_id||ot.acct_id||ot.ord_id END) AS olo_prod_ord_cnt,
                       COUNT(DISTINCT CASE WHEN NVL(it.set_cd,'X') NOT IN ('C','E') AND it.sub_src_cd = 'D'
                                            AND ot.acct_id = it.acct_id AND ot.ord_id = it.ord_id
                                           THEN ot.individual_id||ot.acct_id||ot.ord_id END) AS olo_prod_dm_ord_cnt,
                       COUNT(DISTINCT CASE WHEN NVL(it.set_cd,'X') NOT IN ('C','E') AND it.sub_src_cd IN ('E','G','K','N','U')
                                            AND ot.acct_id = it.acct_id AND ot.ord_id = it.ord_id
                                           THEN ot.individual_id||ot.acct_id||ot.ord_id END) AS olo_prod_em_ord_cnt
                  FROM middle_tier2.mt_online_ord ot,
                       middle_tier2.mt_online_item it
                 WHERE ot.ord_id = it.ord_id (+)
                 GROUP BY ot.individual_id
               ) olo,
               (SELECT individual_id,
                       MIN(NVL(TO_CHAR(exp_dt,'YYYYMMDD'),'99991231')||'3'||decode(mag_cd,'CNS',1,'CRH',2,3)||mag_cd) AS de_fst_prod_cd,
                       MIN(exp_dt) AS de_min_exp_dt,
                       MAX(exp_dt) AS de_max_exp_dt,
                       COUNT(*)    AS de_cnt,
                       COUNT(CASE WHEN mag_cd = 'CNS' THEN 1 END) AS de_cr_prod_cnt,
                       COUNT(CASE WHEN mag_cd = 'CRH' THEN 1 END) AS de_hl_prod_cnt
                  FROM warehouse2.dead_expires
                 GROUP BY individual_id
               ) de,
               (SELECT individual_id,
                       SUM(pmt_amt)    AS bo_pmt_amt,
                       MIN(ord_dt)     AS bo_min_ord_dt,
                       MIN(CASE WHEN NVL(dnr_recip_cd,'X') != 'R' THEN ord_dt END) AS bo_prod_fst_ord_dt,
                       MAX(ord_dt)     AS bo_max_ord_dt,
                       MAX(lst_pmt_dt) AS bo_pmt_dt,
                       COUNT(CASE WHEN dnr_recip_cd = 'R' OR ord_prem_flg = 'Y' 
                                  THEN 'x' ELSE NULL END) AS bo_ce_cnt,
                       COUNT(CASE WHEN NVL(dnr_recip_cd,'X') != 'R' AND NVL(ord_prem_flg,'X') != 'Y'
                                  THEN 'x' ELSE NULL END) AS bo_xce_cnt,
                       COUNT(CASE WHEN dnr_recip_cd = 'D' 
                                  THEN 'x' ELSE NULL END) AS bo_recip_cd_d_cnt,
                       COUNT(CASE WHEN NVL(dnr_recip_cd,'X') != 'D'
                                  THEN 'x' ELSE NULL END) AS bo_recip_cd_not_d_cnt,
                       MIN(CASE WHEN ord_cdr_flg = 'Y' THEN NVL(TO_CHAR(ord_dt, 'YYYYMMDD'), '99991231')||'90'||'CDR'
                                WHEN ord_cdr_flg = 'N' THEN NVL(TO_CHAR(ord_dt, 'YYYYMMDD'), '99991231')||'A0'||'NCDR'
                           END) AS bo_fst_prod_cd,
                       MAX(CASE WHEN NVL(dnr_recip_cd,'X') != 'R' THEN ord_dt END) AS bo_prod_lst_ord_dt,
                       COUNT(CASE WHEN NVL(dnr_recip_cd,'X') != 'R' THEN 'x' END)                   AS bo_prod_ord_cnt,
                       COUNT(CASE WHEN NVL(dnr_recip_cd,'X') != 'R' AND src_cd = '5'  THEN 'x' END) AS bo_prod_dm_ord_cnt,
                       COUNT(CASE WHEN NVL(dnr_recip_cd,'X') != 'R' AND src_cd = '11' THEN 'x' END) AS bo_prod_em_ord_cnt,
                       COUNT(CASE WHEN dnr_recip_cd = 'D' THEN 'x' END) AS bo_prod_dnr_ord_cnt
                  FROM middle_tier2.mt_books_ord
                 GROUP BY individual_id
               ) bo,
               (SELECT individual_id,
                       MAX(ret_canc_dt)              AS bi_ret_canc_dt,
                       MAX(CASE WHEN ret_canc_rsn_cd IN ('AR','AY','BG','BH','BK','BM','DE','DI','DJ','DW','EA')
                               THEN ret_canc_dt END) AS bi_prod_lst_canc_bad_dbt_dt,
                       COUNT(*)                      AS bi_cnt
                  FROM middle_tier2.mt_books_item
                 GROUP BY individual_id
               ) bi,
               (SELECT individual_id,
                       bk_flg,
                       bk_actv_flg,
                       bk_ltd_pd_amt bs_prod_ltd_pd_amt
                  FROM middle_tier2.mt_books_summary
               ) bs,
               (SELECT individual_id,
                       MAX(don_dt)  AS fr_lst_don_dt,
                       max(don_amt) AS fr_max_don_amt
                  FROM middle_tier2.mt_fr_donation
                 GROUP BY individual_id
               ) fr,
               (SELECT individual_id,
                       MIN(last_resp_dt)    AS mn_last_resp_dt,
                       MAX(last_resp_dt)    AS mx_last_resp_dt,
                       COUNT(individual_id) AS ar_entry_cnt,
                       SUM(CASE WHEN last_resp_dt >= ADD_MONTHS(SYSDATE, -12) THEN 1 ELSE 0 END) cnt_resp_dt_gt_12
                  FROM middle_tier2.mt_adv_response
                 GROUP BY individual_id
               ) ar,
               (SELECT individual_id,
                       MAX(pldg_dt) AS max_pldg_dt
                  FROM middle_tier2.mt_fr_pledge
                 GROUP BY individual_id
               ) fp,
               (SELECT individual_id,
                       MAX(pldg_dt) AS max_pldg_dt
                  FROM middle_tier2.mt_fr_lt_pledge
                 GROUP BY individual_id
               ) flp,
               (SELECT individual_id,
                       TO_DATE(MAX(abq_yr) || '0601','YYYYMMDD') max_abq_dt,
                       TO_DATE(MIN(abq_yr) || '0601','YYYYMMDD') min_abq_dt
                  FROM middle_tier2.mt_abq_donation
                 GROUP BY individual_id
               ) abqd,
               (SELECT individual_id,
                       TO_DATE(MIN(abq_yr) || '0601','YYYYMMDD') min_abq_mbr_dt,
                       TO_DATE(MAX(abq_yr) || '0601','YYYYMMDD') max_abq_mbr_dt
                  FROM middle_tier2.mt_abq_response
                 WHERE mbr_flg = 'Y'
                 GROUP BY individual_id
                ) abqr,
               (SELECT individual_id,
                       COUNT(DISTINCT CASE WHEN mag_cd = 'CNS' AND set_cd = 'A' THEN 1 END) + COUNT(cr_break_flg)  AS cr_prod_cnt,
                       MAX(CASE WHEN mag_cd = 'CNS' AND set_cd = 'A' THEN 1 END)                                   AS cr_prod_cnta,
                       COUNT(DISTINCT CASE WHEN mag_cd = 'CRH' AND set_cd = 'A' THEN 1 END) + COUNT(hl_break_flg)  AS hl_prod_cnt,
                       COUNT(DISTINCT CASE WHEN mag_cd = 'CRM' AND set_cd = 'A' THEN 1 END) + COUNT(ma_break_flg)  AS ma_prod_cnt,
                       COUNT(DISTINCT CASE WHEN mag_cd = 'SHM' AND set_cd = 'A' THEN 1 END) + COUNT(shm_break_flg) AS shm_prod_cnt
                  FROM (SELECT oo.individual_id,
                               oo.mag_cd,
                               oo.set_cd,
                               CASE WHEN oo.mag_cd = 'CNS' AND oo.set_cd = 'A'
                                     AND MONTHS_BETWEEN(strt_lkp.pub_dt, MAX(end_lkp.pub_dt)
                                            OVER(PARTITION BY oo.individual_id, oo.mag_cd, oo.set_cd ORDER BY strt_lkp.pub_dt
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) > 6 THEN 1 END AS cr_break_flg,
                               CASE WHEN oo.mag_cd = 'CRH' AND oo.set_cd = 'A'
                                     AND MONTHS_BETWEEN(strt_lkp.pub_dt, MAX(end_lkp.pub_dt)
                                            OVER(PARTITION BY oo.individual_id, oo.mag_cd, oo.set_cd ORDER BY strt_lkp.pub_dt
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) > 6 THEN 1 END AS hl_break_flg,
                               CASE WHEN oo.mag_cd = 'CRM' AND oo.set_cd = 'A'
                                     AND MONTHS_BETWEEN(strt_lkp.pub_dt, MAX(end_lkp.pub_dt)
                                            OVER(PARTITION BY oo.individual_id, oo.mag_cd, oo.set_cd ORDER BY strt_lkp.pub_dt
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) > 6 THEN 1 END AS ma_break_flg,
                               CASE WHEN oo.mag_cd = 'SHM' AND oo.set_cd = 'A'
                                     AND MONTHS_BETWEEN(strt_lkp.pub_dt, MAX(end_lkp.pub_dt)
                                            OVER(PARTITION BY oo.individual_id, oo.mag_cd, oo.set_cd ORDER BY strt_lkp.pub_dt
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) > 6 THEN 1 END AS shm_break_flg
                          FROM middle_tier2.mt_offline_ord oo,
                               warehouse2.issue_lkp strt_lkp,
                               warehouse2.issue_lkp end_lkp
                         WHERE end_lkp.mag_cd (+) = oo.mag_cd
                           AND end_lkp.iss_num (+) = (oo.orig_strt_iss_num + oo.term_mth_cnt)
                           AND strt_lkp.mag_cd (+) = oo.mag_cd
                           AND strt_lkp.iss_num (+) = oo.orig_strt_iss_num
                       )
                 GROUP BY individual_id
               ) brks,
               middle_tier2.mt_email e,
               warehouse2.email we,
               (SELECT individual_id,
                       MAX(CASE WHEN GROUP_ID = '311' THEN 'Y' ELSE NULL END)          AS an_emp_flg,
                       MAX(CASE WHEN usr_src_ind LIKE 'FORUM%' THEN 'Y' ELSE NULL END) AS an_forums_flag
                  FROM middle_tier2.mt_account_number
                 WHERE acct_prefix = 'PWI'
                 GROUP BY individual_id
               ) an,
               (SELECT individual_id,
                       MAX(maint_dt) AS ol_match_recent_dt,
                       NULLIF(COUNT(*), 0) AS ol_match_cnt,
                       NULLIF(SUM(CASE WHEN maint_dt >= ADD_MONTHS(SYSDATE, -6) THEN 1 ELSE 0 END), 0) ol_match_cnt_6m,
                       NULLIF(SUM(CASE WHEN maint_dt >= ADD_MONTHS(SYSDATE, -12) 
                                        AND maint_dt < ADD_MONTHS(SYSDATE, -6) THEN 1 ELSE 0 END), 0) ol_match_cnt_7_12m,
                       NULLIF(SUM(CASE WHEN maint_dt >= ADD_MONTHS(SYSDATE, -18)
                                        AND maint_dt < ADD_MONTHS(SYSDATE, -12) THEN 1 ELSE 0 END), 0) ol_match_cnt_13_18m
                  FROM warehouse2.outside_list
                 GROUP BY individual_id) ol,
               middle_tier2.mt_alliant al,
               middle_tier2.mt_individual_ibx ibx ,
                (select  min(eua.create_date) as min_create_date, er.individual_id
              from warehouse2.erights_user_association eua, 
                   warehouse2.external_ref er
               where eua.cds_target_id = SUBSTR(er.external_key,5)     
                  and eua.source_description = 'iPad'
                  and eua.source_id = 1 
                  and  eua.enabled = 'Y' 
                  and eua.source_name = 'user or csr flow' 
                  and er.data_source = 'CDS'          
              group by er.individual_id                                      
                     )eua,
                (select b.individual_id,
                        min(b.bnb_last_visit_dt) AS bnb_frst_visitor_dt,
                        max(b.bnb_last_visit_dt) AS bnb_last_visitor_dt,
                        MIN(CASE WHEN b.bnb_status = 'P' THEN b.bnb_lead_recvd_dt END) bnb_frst_prospect_dt,
                        MAX(CASE WHEN b.bnb_status = 'P' THEN b.bnb_lead_recvd_dt END) bnb_last_prospect_dt,
                        min(b.bnb_lead_sales_dt) AS bnb_frst_sales_dt,
                        max(b.bnb_lead_sales_dt) AS bnb_last_sales_dt,
                        COUNT(DISTINCT CASE WHEN b.bnb_status = 'P' THEN b.bnb_trans_id END) bnb_tot_prospect_cnt,
                        COUNT(DISTINCT CASE WHEN b.bnb_status = 'S' THEN b.bnb_trans_id END) bnb_tot_sale_cnt,
                        CASE
                         WHEN MAX(CASE WHEN b.bnb_status = 'S' THEN 1 END) = 1 THEN 'S'
                         WHEN MAX(CASE WHEN b.bnb_status = 'P' THEN 1 END) = 1 THEN 'P'
                         WHEN MAX(CASE WHEN b.bnb_status = 'V' THEN 1 END) = 1 THEN 'V'
                         ELSE NULL
                        END AS bnb_best_status,
                        CASE
                         WHEN MAX(CASE WHEN b.bnb_status = 'S' THEN 1 END) = 1 THEN MAX(b.bnb_lead_sales_dt)
                         WHEN MAX(CASE WHEN b.bnb_status = 'P' THEN 1 END) = 1 THEN MAX(b.bnb_lead_recvd_dt)
                         WHEN MAX(CASE WHEN b.bnb_status = 'V' THEN 1 END) = 1 THEN MAX(b.bnb_last_visit_dt)
                         ELSE NULL
                        END AS bnb_best_status_dt,
                        CASE
                             WHEN MAX(GREATEST(nvl(b.bnb_lead_recvd_dt,to_date('19000101', 'YYYYMMDD')), 
                                               nvl(b.bnb_lead_sales_dt,to_date('19000101', 'YYYYMMDD')), 
                                               nvl(b.bnb_last_visit_dt,to_date('19000101', 'YYYYMMDD')))) = MAX(nvl(b.bnb_lead_sales_dt,to_date('19000101', 'YYYYMMDD')))
                             THEN 'S'
                             WHEN MAX(GREATEST(nvl(b.bnb_lead_recvd_dt,to_date('19000101', 'YYYYMMDD')), 
                                               nvl(b.bnb_lead_sales_dt,to_date('19000101', 'YYYYMMDD')), 
                                               nvl(b.bnb_last_visit_dt,to_date('19000101', 'YYYYMMDD')))) = MAX(nvl(b.bnb_lead_recvd_dt,to_date('19000101', 'YYYYMMDD')))
                             THEN 'P'
                             WHEN MAX(GREATEST(nvl(b.bnb_lead_recvd_dt,to_date('19000101', 'YYYYMMDD')), 
                                               nvl(b.bnb_lead_sales_dt,to_date('19000101', 'YYYYMMDD')), 
                                               nvl(b.bnb_last_visit_dt,to_date('19000101', 'YYYYMMDD')))) = MAX(nvl(b.bnb_last_visit_dt,to_date('19000101', 'YYYYMMDD')))
                             THEN 'V'
                             ELSE cast(null as VARCHAR2(1))
                        END AS bnb_most_recent_status,
                        MAX(GREATEST(nvl(b.bnb_lead_recvd_dt,to_date('19000101', 'YYYYMMDD')), 
                                     nvl(b.bnb_lead_sales_dt,to_date('19000101', 'YYYYMMDD')), 
                                     nvl(b.bnb_last_visit_dt,to_date('19000101', 'YYYYMMDD')))) bnb_most_recent_dt,
                        COUNT(DISTINCT CASE WHEN b.bnb_status = 'S' AND b.bnb_new_used = 'NEW' THEN bnb_trans_id END) bnb_nbr_new_sales,
                        COUNT(DISTINCT CASE WHEN b.bnb_status = 'S' AND b.bnb_new_used = 'USED' THEN bnb_trans_id END) bnb_nbr_used_sales,
                        UPPER(SUBSTR(MAX(greatest(NVL(to_char(bnb_lead_recvd_dt,'YYYYMMDDHH24MISS'),'19000101000000')||bnb_trans_type, 
                                 NVL(to_char(bnb_lead_sales_dt,'YYYYMMDDHH24MISS'),'19000101000000')||bnb_trans_type, 
                                 NVL(to_char(bnb_last_visit_dt,'YYYYMMDDHH24MISS'),'19000101000000')||bnb_trans_type)),15)) BNB_MOST_RECENT_TRANS_TYPE
                    from middle_tier2.mt_build_and_buy b
                    group by b.individual_id
                     ) bb,
                     middle_tier2.mt_pros_email_ind_temp pem,
               warehouse2.individual_address ia,
               (SELECT er.individual_id,
                       MAX(il.pub_dt) AS mx_cds_pub_dt
                  FROM warehouse2.external_ref er
                       INNER JOIN warehouse2.offline_account oa ON oa.acct_id = er.internal_key
                       INNER JOIN warehouse2.issue_lkp il ON il.mag_cd = oa.mag_cd AND il.iss_num = oa.lst_iss_num
                 WHERE er.data_source IN ('CDS')
                 GROUP BY er.individual_id
               ) erm,
                (SELECT mos1.individual_id
                  FROM middle_tier2.mt_offline_summary mos1,
                       (SELECT individual_id
                          FROM middle_tier2.mt_offline_summary
                         WHERE (   (cr_actv_flg  = 'Y' AND cr_curr_mbr_dt  <= TO_DATE('08/01/2014','MM/DD/YYYY'))
                                OR (hl_actv_flg  = 'Y' AND hl_curr_mbr_dt  <= TO_DATE('08/01/2014','MM/DD/YYYY'))
                                OR (ma_actv_flg  = 'Y' AND ma_curr_mbr_dt  <= TO_DATE('08/01/2014','MM/DD/YYYY'))
                                OR (shm_actv_flg = 'Y' AND shm_curr_mbr_dt <= TO_DATE('08/01/2014','MM/DD/YYYY')))
                         GROUP BY individual_id) mos2,
                       (SELECT individual_id
                          FROM middle_tier2.mt_auth_summary
                         WHERE non_prom_list_rent_flg = 'Y'
                         GROUP BY individual_id) mas,
                       (SELECT individual_id
                          FROM middle_tier2.mt_authorization
                         WHERE (   (data_source = 'CCC' AND auth_flg = 'N' AND auth_cd IN ('SOY','MAIL','DNM','DNS'))
                                OR (data_source = 'CU'  AND auth_flg = 'N' AND auth_cd = 'MAIL'))
                         GROUP BY individual_id) auth
                 WHERE (   (mos1.cr_curr_mbr_dt  > TO_DATE('08/01/2014','MM/DD/YYYY') AND mos1.cr_actv_flg = 'Y')
                        OR (mos1.hl_curr_mbr_dt  > TO_DATE('08/01/2014','MM/DD/YYYY') AND mos1.hl_actv_flg = 'Y')
                        OR (mos1.ma_curr_mbr_dt  > TO_DATE('08/01/2014','MM/DD/YYYY') AND mos1.ma_actv_flg = 'Y')
                        OR (mos1.shm_curr_mbr_dt > TO_DATE('08/01/2014','MM/DD/YYYY') AND mos1.shm_actv_flg = 'Y'))
                   AND mos1.individual_id = mos2.individual_id (+)
                   AND mos2.individual_id IS NULL
                   AND mos1.individual_id = mas.individual_id (+)
                   AND mas.individual_id IS NULL
                   AND mos1.individual_id = auth.individual_id (+)
                   AND auth.individual_id IS NULL
                 GROUP BY mos1.individual_id) coop,
                 (select individual_id,
                         max(visit_start_time) max_visit_strt_time,
                         max(days_since_first_use) max_days_since_fst_use
                  from warehouse2.app_session
                  group by individual_id) app,
                  email_fav_key key,
                  (select individual_id,
                          count(*) int_cnt
                   from middle_tier2.mt_adv_interests
                   group by individual_id) advint,
                  (select individual_id,
                          count(*) sur_cnt
                   from middle_tier2.mt_adv_survey_resp
                   group by individual_id) advsur,
                   (select individual_id,
                         count(*) don_cnt
                     from middle_tier2.mt_adv_donation
                     group by individual_id) advdon,
                    (select individual_id,
                         count(*) alrt_cnt
                         from middle_tier2.mt_adv_alert_resp
                         group by individual_id) advalrt,
                 (select individual_id, max(report_start_date) report_start_date, min(report_start_date) min_rpt_start_date, count(*) ereader_cnt
                   from middle_tier2.mt_ereader_sub
                   group by individual_id) eread,
                    middle_tier2.mt_indiv_surv surv,
                    middle_tier2.mt_indiv_page_temp page,
                    middle_tier2.mt_oli_xog_temp olixog,
                    middle_tier2.mt_oli_aps_temp oliaps,
                    middle_tier2.mt_olo_xog_temp oloxog
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
       )
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

