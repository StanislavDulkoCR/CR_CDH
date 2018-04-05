;
create table cr_temp.GTT_MT_OFFLINE_SUMMARY 
	DISTSTYLE KEY DISTKEY(cdh_id) 
	INTERLEAVED SORTKEY(icd_id, cdh_id) 
as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(cr_actv_flg, cr_actv_flg::text,'(NULL)') as cr_actv_flg
, NVL2(hl_actv_flg, hl_actv_flg::text,'(NULL)') as hl_actv_flg
, NVL2(ma_actv_flg, ma_actv_flg::text,'(NULL)') as ma_actv_flg
, NVL2(cr_actv_rptg_flg, cr_actv_rptg_flg::text,'(NULL)') as cr_actv_rptg_flg
, NVL2(hl_actv_rptg_flg, hl_actv_rptg_flg::text,'(NULL)') as hl_actv_rptg_flg
, NVL2(ma_actv_rptg_flg, ma_actv_rptg_flg::text,'(NULL)') as ma_actv_rptg_flg
, NVL2(cr_actv_pd_flg, cr_actv_pd_flg::text,'(NULL)') as cr_actv_pd_flg
, NVL2(hl_actv_pd_flg, hl_actv_pd_flg::text,'(NULL)') as hl_actv_pd_flg
, NVL2(ma_actv_pd_flg, ma_actv_pd_flg::text,'(NULL)') as ma_actv_pd_flg
, NVL2(cr_canc_bad_dbt_flg, cr_canc_bad_dbt_flg::text,'(NULL)') as cr_canc_bad_dbt_flg
, NVL2(hl_canc_bad_dbt_flg, hl_canc_bad_dbt_flg::text,'(NULL)') as hl_canc_bad_dbt_flg
, NVL2(ma_canc_bad_dbt_flg, ma_canc_bad_dbt_flg::text,'(NULL)') as ma_canc_bad_dbt_flg
, NVL2(cr_canc_cust_flg, cr_canc_cust_flg::text,'(NULL)') as cr_canc_cust_flg
, NVL2(hl_canc_cust_flg, hl_canc_cust_flg::text,'(NULL)') as hl_canc_cust_flg
, NVL2(ma_canc_cust_flg, ma_canc_cust_flg::text,'(NULL)') as ma_canc_cust_flg
, NVL2(cr_crd_pend_flg, cr_crd_pend_flg::text,'(NULL)') as cr_crd_pend_flg
, NVL2(hl_crd_pend_flg, hl_crd_pend_flg::text,'(NULL)') as hl_crd_pend_flg
, NVL2(ma_crd_pend_flg, ma_crd_pend_flg::text,'(NULL)') as ma_crd_pend_flg
, NVL2(cr_crd_stat_cd, cr_crd_stat_cd::text,'(NULL)') as cr_crd_stat_cd
, NVL2(hl_crd_stat_cd, hl_crd_stat_cd::text,'(NULL)') as hl_crd_stat_cd
, NVL2(ma_crd_stat_cd, ma_crd_stat_cd::text,'(NULL)') as ma_crd_stat_cd
, NVL2(ofo_crd_stat_cd, ofo_crd_stat_cd::text,'(NULL)') as ofo_crd_stat_cd
, NVL2(TO_CHAR(cr_exp_dt,'DD-MON-YY'), TO_CHAR(cr_exp_dt,'DD-MON-YY')::text,'(NULL)') as cr_exp_dt
, NVL2(TO_CHAR(hl_exp_dt,'DD-MON-YY'), TO_CHAR(hl_exp_dt,'DD-MON-YY')::text,'(NULL)') as hl_exp_dt
, NVL2(TO_CHAR(ma_exp_dt,'DD-MON-YY'), TO_CHAR(ma_exp_dt,'DD-MON-YY')::text,'(NULL)') as ma_exp_dt
, NVL2(TO_CHAR(cr_lst_canc_dt,'DD-MON-YY'), TO_CHAR(cr_lst_canc_dt,'DD-MON-YY')::text,'(NULL)') as cr_lst_canc_dt
, NVL2(TO_CHAR(hl_lst_canc_dt,'DD-MON-YY'), TO_CHAR(hl_lst_canc_dt,'DD-MON-YY')::text,'(NULL)') as hl_lst_canc_dt
, NVL2(TO_CHAR(ma_lst_canc_dt,'DD-MON-YY'), TO_CHAR(ma_lst_canc_dt,'DD-MON-YY')::text,'(NULL)') as ma_lst_canc_dt
, NVL2(TO_CHAR(tl_lst_ord_dt,'DD-MON-YY'), TO_CHAR(tl_lst_ord_dt,'DD-MON-YY')::text,'(NULL)') as tl_lst_ord_dt
, NVL2(TO_CHAR(cr_lst_pmt_dt,'DD-MON-YY'), TO_CHAR(cr_lst_pmt_dt,'DD-MON-YY')::text,'(NULL)') as cr_lst_pmt_dt
, NVL2(TO_CHAR(hl_lst_pmt_dt,'DD-MON-YY'), TO_CHAR(hl_lst_pmt_dt,'DD-MON-YY')::text,'(NULL)') as hl_lst_pmt_dt
, NVL2(TO_CHAR(ma_lst_pmt_dt,'DD-MON-YY'), TO_CHAR(ma_lst_pmt_dt,'DD-MON-YY')::text,'(NULL)') as ma_lst_pmt_dt
, NVL2(cr_flg, cr_flg::text,'(NULL)') as cr_flg
, NVL2(hl_flg, hl_flg::text,'(NULL)') as hl_flg
, NVL2(ma_flg, ma_flg::text,'(NULL)') as ma_flg
, NVL2(cr_exp_flg, cr_exp_flg::text,'(NULL)') as cr_exp_flg
, NVL2(hl_exp_flg, hl_exp_flg::text,'(NULL)') as hl_exp_flg
, NVL2(ma_exp_flg, ma_exp_flg::text,'(NULL)') as ma_exp_flg
, NVL2(cr_curr_mbr_keycode, cr_curr_mbr_keycode::text,'(NULL)') as cr_curr_mbr_keycode
, NVL2(hl_curr_mbr_keycode, hl_curr_mbr_keycode::text,'(NULL)') as hl_curr_mbr_keycode
, NVL2(ma_curr_mbr_keycode, ma_curr_mbr_keycode::text,'(NULL)') as ma_curr_mbr_keycode
, NVL2(cr_curr_ord_keycode, cr_curr_ord_keycode::text,'(NULL)') as cr_curr_ord_keycode
, NVL2(hl_curr_ord_keycode, hl_curr_ord_keycode::text,'(NULL)') as hl_curr_ord_keycode
, NVL2(ma_curr_ord_keycode, ma_curr_ord_keycode::text,'(NULL)') as ma_curr_ord_keycode
, NVL2(cr_fst_mbr_keycode, cr_fst_mbr_keycode::text,'(NULL)') as cr_fst_mbr_keycode
, NVL2(hl_fst_mbr_keycode, hl_fst_mbr_keycode::text,'(NULL)') as hl_fst_mbr_keycode
, NVL2(ma_fst_mbr_keycode, ma_fst_mbr_keycode::text,'(NULL)') as ma_fst_mbr_keycode
, NVL2(cr_lst_ord_keycode, cr_lst_ord_keycode::text,'(NULL)') as cr_lst_ord_keycode
, NVL2(hl_lst_ord_keycode, hl_lst_ord_keycode::text,'(NULL)') as hl_lst_ord_keycode
, NVL2(ma_lst_ord_keycode, ma_lst_ord_keycode::text,'(NULL)') as ma_lst_ord_keycode
, NVL2(cr_lt_sub_flg, cr_lt_sub_flg::text,'(NULL)') as cr_lt_sub_flg
, NVL2(hl_lt_sub_flg, hl_lt_sub_flg::text,'(NULL)') as hl_lt_sub_flg
, NVL2(ma_lt_sub_flg, ma_lt_sub_flg::text,'(NULL)') as ma_lt_sub_flg
, NVL2(cr_non_sub_dnr_flg, cr_non_sub_dnr_flg::text,'(NULL)') as cr_non_sub_dnr_flg
, NVL2(hl_non_sub_dnr_flg, hl_non_sub_dnr_flg::text,'(NULL)') as hl_non_sub_dnr_flg
, NVL2(ma_non_sub_dnr_flg, ma_non_sub_dnr_flg::text,'(NULL)') as ma_non_sub_dnr_flg
, NVL2(trunc(nvl(cr_brks_cnt,'0')), trunc(nvl(cr_brks_cnt,'0'))::text,'(NULL)') as cr_brks_cnt
, NVL2(trunc(nvl(hl_brks_cnt,'0')), trunc(nvl(hl_brks_cnt,'0'))::text,'(NULL)') as hl_brks_cnt
, NVL2(trunc(nvl(ma_brks_cnt,'0')), trunc(nvl(ma_brks_cnt,'0'))::text,'(NULL)') as ma_brks_cnt
, NVL2(trunc(nvl(cr_rnw_cnt,'0')), trunc(nvl(cr_rnw_cnt,'0'))::text,'(NULL)') as cr_rnw_cnt
, NVL2(trunc(nvl(hl_rnw_cnt,'0')), trunc(nvl(hl_rnw_cnt,'0'))::text,'(NULL)') as hl_rnw_cnt
, NVL2(trunc(nvl(ma_rnw_cnt,'0')), trunc(nvl(ma_rnw_cnt,'0'))::text,'(NULL)') as ma_rnw_cnt
, NVL2(cr_rec_flg, cr_rec_flg::text,'(NULL)') as cr_rec_flg
, NVL2(hl_rec_flg, hl_rec_flg::text,'(NULL)') as hl_rec_flg
, NVL2(ma_rec_flg, ma_rec_flg::text,'(NULL)') as ma_rec_flg
, NVL2(cr_svc_stat_cd, cr_svc_stat_cd::text,'(NULL)') as cr_svc_stat_cd
, NVL2(hl_svc_stat_cd, hl_svc_stat_cd::text,'(NULL)') as hl_svc_stat_cd
, NVL2(ma_svc_stat_cd, ma_svc_stat_cd::text,'(NULL)') as ma_svc_stat_cd
, NVL2(cr_curr_mbr_src_cd, cr_curr_mbr_src_cd::text,'(NULL)') as cr_curr_mbr_src_cd
, NVL2(hl_curr_mbr_src_cd, hl_curr_mbr_src_cd::text,'(NULL)') as hl_curr_mbr_src_cd
, NVL2(ma_curr_mbr_src_cd, ma_curr_mbr_src_cd::text,'(NULL)') as ma_curr_mbr_src_cd
, NVL2(cr_curr_ord_src_cd, cr_curr_ord_src_cd::text,'(NULL)') as cr_curr_ord_src_cd
, NVL2(hl_curr_ord_src_cd, hl_curr_ord_src_cd::text,'(NULL)') as hl_curr_ord_src_cd
, NVL2(ma_curr_ord_src_cd, ma_curr_ord_src_cd::text,'(NULL)') as ma_curr_ord_src_cd
, NVL2(cr_fst_ord_src_cd, cr_fst_ord_src_cd::text,'(NULL)') as cr_fst_ord_src_cd
/*---END---*/

from cr_temp.MT_OFFLINE_SUMMARY T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;

CREATE TABLE cr_temp.gtt_agg_print_summary 
	DISTSTYLE KEY DISTKEY(cdh_id) 
	INTERLEAVED SORTKEY(icd_id, cdh_id) 
AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/, NVL2(cr_actv_flg, cr_actv_flg::text,'(NULL)') as cr_actv_flg
, NVL2(hl_actv_flg, hl_actv_flg::text,'(NULL)') as hl_actv_flg
, NVL2(ma_actv_flg, ma_actv_flg::text,'(NULL)') as ma_actv_flg
, NVL2(cr_actv_rptg_flg, cr_actv_rptg_flg::text,'(NULL)') as cr_actv_rptg_flg
, NVL2(hl_actv_rptg_flg, hl_actv_rptg_flg::text,'(NULL)') as hl_actv_rptg_flg
, NVL2(ma_actv_rptg_flg, ma_actv_rptg_flg::text,'(NULL)') as ma_actv_rptg_flg
, NVL2(cr_actv_pd_flg, cr_actv_pd_flg::text,'(NULL)') as cr_actv_pd_flg
, NVL2(hl_actv_pd_flg, hl_actv_pd_flg::text,'(NULL)') as hl_actv_pd_flg
, NVL2(ma_actv_pd_flg, ma_actv_pd_flg::text,'(NULL)') as ma_actv_pd_flg
, NVL2(cr_canc_bad_dbt_flg, cr_canc_bad_dbt_flg::text,'(NULL)') as cr_canc_bad_dbt_flg
, NVL2(hl_canc_bad_dbt_flg, hl_canc_bad_dbt_flg::text,'(NULL)') as hl_canc_bad_dbt_flg
, NVL2(ma_canc_bad_dbt_flg, ma_canc_bad_dbt_flg::text,'(NULL)') as ma_canc_bad_dbt_flg
, NVL2(cr_canc_cust_flg, cr_canc_cust_flg::text,'(NULL)') as cr_canc_cust_flg
, NVL2(hl_canc_cust_flg, hl_canc_cust_flg::text,'(NULL)') as hl_canc_cust_flg
, NVL2(ma_canc_cust_flg, ma_canc_cust_flg::text,'(NULL)') as ma_canc_cust_flg
, NVL2(cr_crd_pend_flg, cr_crd_pend_flg::text,'(NULL)') as cr_crd_pend_flg
, NVL2(hl_crd_pend_flg, hl_crd_pend_flg::text,'(NULL)') as hl_crd_pend_flg
, NVL2(ma_crd_pend_flg, ma_crd_pend_flg::text,'(NULL)') as ma_crd_pend_flg
, NVL2(cr_crd_stat_cd, cr_crd_stat_cd::text,'(NULL)') as cr_crd_stat_cd
, NVL2(hl_crd_stat_cd, hl_crd_stat_cd::text,'(NULL)') as hl_crd_stat_cd
, NVL2(ma_crd_stat_cd, ma_crd_stat_cd::text,'(NULL)') as ma_crd_stat_cd
, NVL2(ofo_crd_stat_cd, ofo_crd_stat_cd::text,'(NULL)') as ofo_crd_stat_cd
, NVL2(TO_CHAR(cr_exp_dt,'DD-MON-YY'), TO_CHAR(cr_exp_dt,'DD-MON-YY')::text,'(NULL)') as cr_exp_dt
, NVL2(TO_CHAR(hl_exp_dt,'DD-MON-YY'), TO_CHAR(hl_exp_dt,'DD-MON-YY')::text,'(NULL)') as hl_exp_dt
, NVL2(TO_CHAR(ma_exp_dt,'DD-MON-YY'), TO_CHAR(ma_exp_dt,'DD-MON-YY')::text,'(NULL)') as ma_exp_dt
, NVL2(TO_CHAR(cr_lst_canc_dt,'DD-MON-YY'), TO_CHAR(cr_lst_canc_dt,'DD-MON-YY')::text,'(NULL)') as cr_lst_canc_dt
, NVL2(TO_CHAR(hl_lst_canc_dt,'DD-MON-YY'), TO_CHAR(hl_lst_canc_dt,'DD-MON-YY')::text,'(NULL)') as hl_lst_canc_dt
, NVL2(TO_CHAR(ma_lst_canc_dt,'DD-MON-YY'), TO_CHAR(ma_lst_canc_dt,'DD-MON-YY')::text,'(NULL)') as ma_lst_canc_dt
, NVL2(TO_CHAR(tl_lst_ord_dt,'DD-MON-YY'), TO_CHAR(tl_lst_ord_dt,'DD-MON-YY')::text,'(NULL)') as tl_lst_ord_dt
, NVL2(TO_CHAR(cr_lst_pmt_dt,'DD-MON-YY'), TO_CHAR(cr_lst_pmt_dt,'DD-MON-YY')::text,'(NULL)') as cr_lst_pmt_dt
, NVL2(TO_CHAR(hl_lst_pmt_dt,'DD-MON-YY'), TO_CHAR(hl_lst_pmt_dt,'DD-MON-YY')::text,'(NULL)') as hl_lst_pmt_dt
, NVL2(TO_CHAR(ma_lst_pmt_dt,'DD-MON-YY'), TO_CHAR(ma_lst_pmt_dt,'DD-MON-YY')::text,'(NULL)') as ma_lst_pmt_dt
, NVL2(cr_flg, cr_flg::text,'(NULL)') as cr_flg
, NVL2(hl_flg, hl_flg::text,'(NULL)') as hl_flg
, NVL2(ma_flg, ma_flg::text,'(NULL)') as ma_flg
, NVL2(cr_exp_flg, cr_exp_flg::text,'(NULL)') as cr_exp_flg
, NVL2(hl_exp_flg, hl_exp_flg::text,'(NULL)') as hl_exp_flg
, NVL2(ma_exp_flg, ma_exp_flg::text,'(NULL)') as ma_exp_flg
, NVL2(cr_curr_mbr_keycode, cr_curr_mbr_keycode::text,'(NULL)') as cr_curr_mbr_keycode
, NVL2(hl_curr_mbr_keycode, hl_curr_mbr_keycode::text,'(NULL)') as hl_curr_mbr_keycode
, NVL2(ma_curr_mbr_keycode, ma_curr_mbr_keycode::text,'(NULL)') as ma_curr_mbr_keycode
, NVL2(cr_curr_ord_keycode, cr_curr_ord_keycode::text,'(NULL)') as cr_curr_ord_keycode
, NVL2(hl_curr_ord_keycode, hl_curr_ord_keycode::text,'(NULL)') as hl_curr_ord_keycode
, NVL2(ma_curr_ord_keycode, ma_curr_ord_keycode::text,'(NULL)') as ma_curr_ord_keycode
, NVL2(cr_fst_mbr_keycode, cr_fst_mbr_keycode::text,'(NULL)') as cr_fst_mbr_keycode
, NVL2(hl_fst_mbr_keycode, hl_fst_mbr_keycode::text,'(NULL)') as hl_fst_mbr_keycode
, NVL2(ma_fst_mbr_keycode, ma_fst_mbr_keycode::text,'(NULL)') as ma_fst_mbr_keycode
, NVL2(cr_lst_ord_keycode, cr_lst_ord_keycode::text,'(NULL)') as cr_lst_ord_keycode
, NVL2(hl_lst_ord_keycode, hl_lst_ord_keycode::text,'(NULL)') as hl_lst_ord_keycode
, NVL2(ma_lst_ord_keycode, ma_lst_ord_keycode::text,'(NULL)') as ma_lst_ord_keycode
, NVL2(cr_lt_sub_flg, cr_lt_sub_flg::text,'(NULL)') as cr_lt_sub_flg
, NVL2(hl_lt_sub_flg, hl_lt_sub_flg::text,'(NULL)') as hl_lt_sub_flg
, NVL2(ma_lt_sub_flg, ma_lt_sub_flg::text,'(NULL)') as ma_lt_sub_flg
, NVL2(cr_non_sub_dnr_flg, cr_non_sub_dnr_flg::text,'(NULL)') as cr_non_sub_dnr_flg
, NVL2(hl_non_sub_dnr_flg, hl_non_sub_dnr_flg::text,'(NULL)') as hl_non_sub_dnr_flg
, NVL2(ma_non_sub_dnr_flg, ma_non_sub_dnr_flg::text,'(NULL)') as ma_non_sub_dnr_flg
, NVL2(trunc(nvl(cr_brks_cnt,'0')), trunc(nvl(cr_brks_cnt,'0'))::text,'(NULL)') as cr_brks_cnt
, NVL2(trunc(nvl(hl_brks_cnt,'0')), trunc(nvl(hl_brks_cnt,'0'))::text,'(NULL)') as hl_brks_cnt
, NVL2(trunc(nvl(ma_brks_cnt,'0')), trunc(nvl(ma_brks_cnt,'0'))::text,'(NULL)') as ma_brks_cnt
, NVL2(trunc(nvl(cr_rnw_cnt,'0')), trunc(nvl(cr_rnw_cnt,'0'))::text,'(NULL)') as cr_rnw_cnt
, NVL2(trunc(nvl(hl_rnw_cnt,'0')), trunc(nvl(hl_rnw_cnt,'0'))::text,'(NULL)') as hl_rnw_cnt
, NVL2(trunc(nvl(ma_rnw_cnt,'0')), trunc(nvl(ma_rnw_cnt,'0'))::text,'(NULL)') as ma_rnw_cnt
, NVL2(cr_rec_flg, cr_rec_flg::text,'(NULL)') as cr_rec_flg
, NVL2(hl_rec_flg, hl_rec_flg::text,'(NULL)') as hl_rec_flg
, NVL2(ma_rec_flg, ma_rec_flg::text,'(NULL)') as ma_rec_flg
, NVL2(cr_svc_stat_cd, cr_svc_stat_cd::text,'(NULL)') as cr_svc_stat_cd
, NVL2(hl_svc_stat_cd, hl_svc_stat_cd::text,'(NULL)') as hl_svc_stat_cd
, NVL2(ma_svc_stat_cd, ma_svc_stat_cd::text,'(NULL)') as ma_svc_stat_cd
, NVL2(cr_curr_mbr_src_cd, cr_curr_mbr_src_cd::text,'(NULL)') as cr_curr_mbr_src_cd
, NVL2(hl_curr_mbr_src_cd, hl_curr_mbr_src_cd::text,'(NULL)') as hl_curr_mbr_src_cd
, NVL2(ma_curr_mbr_src_cd, ma_curr_mbr_src_cd::text,'(NULL)') as ma_curr_mbr_src_cd
, NVL2(cr_curr_ord_src_cd, cr_curr_ord_src_cd::text,'(NULL)') as cr_curr_ord_src_cd
, NVL2(hl_curr_ord_src_cd, hl_curr_ord_src_cd::text,'(NULL)') as hl_curr_ord_src_cd
, NVL2(ma_curr_ord_src_cd, ma_curr_ord_src_cd::text,'(NULL)') as ma_curr_ord_src_cd
, NVL2(cr_fst_ord_src_cd, cr_fst_ord_src_cd::text,'(NULL)') as cr_fst_ord_src_cd
/*---END---*/
from prod.agg_print_summary T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;



------------------------------------------------------------------------TABLE CREATION END\;
CREATE TABLE cr_temp.diff_mt_offline_summary 
	INTERLEAVED SORTKEY(icd_id, cdh_id) 
AS
with hash_diff as (

select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID     /*, individual_id*/, cr_actv_flg, hl_actv_flg, ma_actv_flg, cr_actv_rptg_flg, hl_actv_rptg_flg, ma_actv_rptg_flg, cr_actv_pd_flg, hl_actv_pd_flg, ma_actv_pd_flg, cr_canc_bad_dbt_flg, hl_canc_bad_dbt_flg, ma_canc_bad_dbt_flg, cr_canc_cust_flg, hl_canc_cust_flg, ma_canc_cust_flg, cr_crd_pend_flg, hl_crd_pend_flg, ma_crd_pend_flg, cr_crd_stat_cd, hl_crd_stat_cd, ma_crd_stat_cd, ofo_crd_stat_cd, cr_exp_dt, hl_exp_dt, ma_exp_dt, cr_lst_canc_dt, hl_lst_canc_dt, ma_lst_canc_dt, tl_lst_ord_dt, cr_lst_pmt_dt, hl_lst_pmt_dt, ma_lst_pmt_dt, cr_flg, hl_flg, ma_flg, cr_exp_flg, hl_exp_flg, ma_exp_flg, cr_curr_mbr_keycode, hl_curr_mbr_keycode, ma_curr_mbr_keycode, cr_curr_ord_keycode, hl_curr_ord_keycode, ma_curr_ord_keycode, cr_fst_mbr_keycode, hl_fst_mbr_keycode, ma_fst_mbr_keycode, cr_lst_ord_keycode, hl_lst_ord_keycode, ma_lst_ord_keycode, cr_lt_sub_flg, hl_lt_sub_flg, ma_lt_sub_flg, cr_non_sub_dnr_flg, hl_non_sub_dnr_flg, ma_non_sub_dnr_flg, cr_brks_cnt, hl_brks_cnt, ma_brks_cnt, cr_rnw_cnt, hl_rnw_cnt, ma_rnw_cnt, cr_rec_flg, hl_rec_flg, ma_rec_flg, cr_svc_stat_cd, hl_svc_stat_cd, ma_svc_stat_cd, cr_curr_mbr_src_cd, hl_curr_mbr_src_cd, ma_curr_mbr_src_cd, cr_curr_ord_src_cd, hl_curr_ord_src_cd, ma_curr_ord_src_cd, cr_fst_ord_src_cd
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL /*, individual_id*/, cr_actv_flg, hl_actv_flg, ma_actv_flg, cr_actv_rptg_flg, hl_actv_rptg_flg, ma_actv_rptg_flg, cr_actv_pd_flg, hl_actv_pd_flg, ma_actv_pd_flg, cr_canc_bad_dbt_flg, hl_canc_bad_dbt_flg, ma_canc_bad_dbt_flg, cr_canc_cust_flg, hl_canc_cust_flg, ma_canc_cust_flg, cr_crd_pend_flg, hl_crd_pend_flg, ma_crd_pend_flg, cr_crd_stat_cd, hl_crd_stat_cd, ma_crd_stat_cd, ofo_crd_stat_cd, cr_exp_dt, hl_exp_dt, ma_exp_dt, cr_lst_canc_dt, hl_lst_canc_dt, ma_lst_canc_dt, tl_lst_ord_dt, cr_lst_pmt_dt, hl_lst_pmt_dt, ma_lst_pmt_dt, cr_flg, hl_flg, ma_flg, cr_exp_flg, hl_exp_flg, ma_exp_flg, cr_curr_mbr_keycode, hl_curr_mbr_keycode, ma_curr_mbr_keycode, cr_curr_ord_keycode, hl_curr_ord_keycode, ma_curr_ord_keycode, cr_fst_mbr_keycode, hl_fst_mbr_keycode, ma_fst_mbr_keycode, cr_lst_ord_keycode, hl_lst_ord_keycode, ma_lst_ord_keycode, cr_lt_sub_flg, hl_lt_sub_flg, ma_lt_sub_flg, cr_non_sub_dnr_flg, hl_non_sub_dnr_flg, ma_non_sub_dnr_flg, cr_brks_cnt, hl_brks_cnt, ma_brks_cnt, cr_rnw_cnt, hl_rnw_cnt, ma_rnw_cnt, cr_rec_flg, hl_rec_flg, ma_rec_flg, cr_svc_stat_cd, hl_svc_stat_cd, ma_svc_stat_cd, cr_curr_mbr_src_cd, hl_curr_mbr_src_cd, ma_curr_mbr_src_cd, cr_curr_ord_src_cd, hl_curr_ord_src_cd, ma_curr_ord_src_cd, cr_fst_ord_src_cd
from cr_temp.GTT_MT_OFFLINE_SUMMARY
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL /*, individual_id*/, cr_actv_flg, hl_actv_flg, ma_actv_flg, cr_actv_rptg_flg, hl_actv_rptg_flg, ma_actv_rptg_flg, cr_actv_pd_flg, hl_actv_pd_flg, ma_actv_pd_flg, cr_canc_bad_dbt_flg, hl_canc_bad_dbt_flg, ma_canc_bad_dbt_flg, cr_canc_cust_flg, hl_canc_cust_flg, ma_canc_cust_flg, cr_crd_pend_flg, hl_crd_pend_flg, ma_crd_pend_flg, cr_crd_stat_cd, hl_crd_stat_cd, ma_crd_stat_cd, ofo_crd_stat_cd, cr_exp_dt, hl_exp_dt, ma_exp_dt, cr_lst_canc_dt, hl_lst_canc_dt, ma_lst_canc_dt, tl_lst_ord_dt, cr_lst_pmt_dt, hl_lst_pmt_dt, ma_lst_pmt_dt, cr_flg, hl_flg, ma_flg, cr_exp_flg, hl_exp_flg, ma_exp_flg, cr_curr_mbr_keycode, hl_curr_mbr_keycode, ma_curr_mbr_keycode, cr_curr_ord_keycode, hl_curr_ord_keycode, ma_curr_ord_keycode, cr_fst_mbr_keycode, hl_fst_mbr_keycode, ma_fst_mbr_keycode, cr_lst_ord_keycode, hl_lst_ord_keycode, ma_lst_ord_keycode, cr_lt_sub_flg, hl_lt_sub_flg, ma_lt_sub_flg, cr_non_sub_dnr_flg, hl_non_sub_dnr_flg, ma_non_sub_dnr_flg, cr_brks_cnt, hl_brks_cnt, ma_brks_cnt, cr_rnw_cnt, hl_rnw_cnt, ma_rnw_cnt, cr_rec_flg, hl_rec_flg, ma_rec_flg, cr_svc_stat_cd, hl_svc_stat_cd, ma_svc_stat_cd, cr_curr_mbr_src_cd, hl_curr_mbr_src_cd, ma_curr_mbr_src_cd, cr_curr_ord_src_cd, hl_curr_ord_src_cd, ma_curr_ord_src_cd, cr_fst_ord_src_cd
from cr_temp.gtt_agg_print_summary
)

group by ICD_ID, CDH_ID                                                              /*, individual_id*/, cr_actv_flg, hl_actv_flg, ma_actv_flg, cr_actv_rptg_flg, hl_actv_rptg_flg, ma_actv_rptg_flg, cr_actv_pd_flg, hl_actv_pd_flg, ma_actv_pd_flg, cr_canc_bad_dbt_flg, hl_canc_bad_dbt_flg, ma_canc_bad_dbt_flg, cr_canc_cust_flg, hl_canc_cust_flg, ma_canc_cust_flg, cr_crd_pend_flg, hl_crd_pend_flg, ma_crd_pend_flg, cr_crd_stat_cd, hl_crd_stat_cd, ma_crd_stat_cd, ofo_crd_stat_cd, cr_exp_dt, hl_exp_dt, ma_exp_dt, cr_lst_canc_dt, hl_lst_canc_dt, ma_lst_canc_dt, tl_lst_ord_dt, cr_lst_pmt_dt, hl_lst_pmt_dt, ma_lst_pmt_dt, cr_flg, hl_flg, ma_flg, cr_exp_flg, hl_exp_flg, ma_exp_flg, cr_curr_mbr_keycode, hl_curr_mbr_keycode, ma_curr_mbr_keycode, cr_curr_ord_keycode, hl_curr_ord_keycode, ma_curr_ord_keycode, cr_fst_mbr_keycode, hl_fst_mbr_keycode, ma_fst_mbr_keycode, cr_lst_ord_keycode, hl_lst_ord_keycode, ma_lst_ord_keycode, cr_lt_sub_flg, hl_lt_sub_flg, ma_lt_sub_flg, cr_non_sub_dnr_flg, hl_non_sub_dnr_flg, ma_non_sub_dnr_flg, cr_brks_cnt, hl_brks_cnt, ma_brks_cnt, cr_rnw_cnt, hl_rnw_cnt, ma_rnw_cnt, cr_rec_flg, hl_rec_flg, ma_rec_flg, cr_svc_stat_cd, hl_svc_stat_cd, ma_svc_stat_cd, cr_curr_mbr_src_cd, hl_curr_mbr_src_cd, ma_curr_mbr_src_cd, cr_curr_ord_src_cd, hl_curr_ord_src_cd, ma_curr_ord_src_cd, cr_fst_ord_src_cd
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select * from hash_diff hd
where exists (select null from cr_temp.id_xref_ncbi_excl_mt_off_ord fm where hd.cdh_id = fm.cdh_id);
;
select count(*) from cr_temp.MT_OFFLINE_SUMMARY
;

select count(*) from cr_temp.GTT_MT_OFFLINE_SUMMARY
 where 1=1
 order by icd_id, cdh_id, cnt_icd_fl desc, cnt_cdh_fl desc;


with icd as (select * from cr_temp.diff_mt_offline_summary where 1=1 and cnt_icd_fl = 1)
,    cdh as (select * from cr_temp.diff_mt_offline_summary where 1=1 and cnt_cdh_fl = 1)

select top 1000 icd.cnt_icd_fl||cdh.cnt_cdh_fl as match_fl, icd.icd_id ,cdh.cdh_id
--, case when icd.individual_id = cdh.individual_id then '' else icd.individual_id||'<>'||cdh.individual_id /*#*/end individual_id_diff, case when icd.cr_actv_flg = cdh.cr_actv_flg then '' else icd.cr_actv_flg||'<>'||cdh.cr_actv_flg /*#*/end cr_actv_flg_diff
, case when icd.hl_actv_flg = cdh.hl_actv_flg then '' else icd.hl_actv_flg||'<>'||cdh.hl_actv_flg /*#*/end hl_actv_flg_diff
, case when icd.ma_actv_flg = cdh.ma_actv_flg then '' else icd.ma_actv_flg||'<>'||cdh.ma_actv_flg /*#*/end ma_actv_flg_diff
, case when icd.cr_actv_rptg_flg = cdh.cr_actv_rptg_flg then '' else icd.cr_actv_rptg_flg||'<>'||cdh.cr_actv_rptg_flg /*#*/end cr_actv_rptg_flg_diff
, case when icd.hl_actv_rptg_flg = cdh.hl_actv_rptg_flg then '' else icd.hl_actv_rptg_flg||'<>'||cdh.hl_actv_rptg_flg /*#*/end hl_actv_rptg_flg_diff
, case when icd.ma_actv_rptg_flg = cdh.ma_actv_rptg_flg then '' else icd.ma_actv_rptg_flg||'<>'||cdh.ma_actv_rptg_flg /*#*/end ma_actv_rptg_flg_diff
, case when icd.cr_actv_pd_flg = cdh.cr_actv_pd_flg then '' else icd.cr_actv_pd_flg||'<>'||cdh.cr_actv_pd_flg /*#*/end cr_actv_pd_flg_diff
, case when icd.hl_actv_pd_flg = cdh.hl_actv_pd_flg then '' else icd.hl_actv_pd_flg||'<>'||cdh.hl_actv_pd_flg /*#*/end hl_actv_pd_flg_diff
, case when icd.ma_actv_pd_flg = cdh.ma_actv_pd_flg then '' else icd.ma_actv_pd_flg||'<>'||cdh.ma_actv_pd_flg /*#*/end ma_actv_pd_flg_diff
, case when icd.cr_canc_bad_dbt_flg = cdh.cr_canc_bad_dbt_flg then '' else icd.cr_canc_bad_dbt_flg||'<>'||cdh.cr_canc_bad_dbt_flg /*#*/end cr_canc_bad_dbt_flg_diff
, case when icd.hl_canc_bad_dbt_flg = cdh.hl_canc_bad_dbt_flg then '' else icd.hl_canc_bad_dbt_flg||'<>'||cdh.hl_canc_bad_dbt_flg /*#*/end hl_canc_bad_dbt_flg_diff
, case when icd.ma_canc_bad_dbt_flg = cdh.ma_canc_bad_dbt_flg then '' else icd.ma_canc_bad_dbt_flg||'<>'||cdh.ma_canc_bad_dbt_flg /*#*/end ma_canc_bad_dbt_flg_diff
, case when icd.cr_canc_cust_flg = cdh.cr_canc_cust_flg then '' else icd.cr_canc_cust_flg||'<>'||cdh.cr_canc_cust_flg /*#*/end cr_canc_cust_flg_diff
, case when icd.hl_canc_cust_flg = cdh.hl_canc_cust_flg then '' else icd.hl_canc_cust_flg||'<>'||cdh.hl_canc_cust_flg /*#*/end hl_canc_cust_flg_diff
, case when icd.ma_canc_cust_flg = cdh.ma_canc_cust_flg then '' else icd.ma_canc_cust_flg||'<>'||cdh.ma_canc_cust_flg /*#*/end ma_canc_cust_flg_diff
, case when icd.cr_crd_pend_flg = cdh.cr_crd_pend_flg then '' else icd.cr_crd_pend_flg||'<>'||cdh.cr_crd_pend_flg /*#*/end cr_crd_pend_flg_diff
, case when icd.hl_crd_pend_flg = cdh.hl_crd_pend_flg then '' else icd.hl_crd_pend_flg||'<>'||cdh.hl_crd_pend_flg /*#*/end hl_crd_pend_flg_diff
, case when icd.ma_crd_pend_flg = cdh.ma_crd_pend_flg then '' else icd.ma_crd_pend_flg||'<>'||cdh.ma_crd_pend_flg /*#*/end ma_crd_pend_flg_diff
, case when icd.cr_crd_stat_cd = cdh.cr_crd_stat_cd then '' else icd.cr_crd_stat_cd||'<>'||cdh.cr_crd_stat_cd /*#*/end cr_crd_stat_cd_diff
, case when icd.hl_crd_stat_cd = cdh.hl_crd_stat_cd then '' else icd.hl_crd_stat_cd||'<>'||cdh.hl_crd_stat_cd /*#*/end hl_crd_stat_cd_diff
, case when icd.ma_crd_stat_cd = cdh.ma_crd_stat_cd then '' else icd.ma_crd_stat_cd||'<>'||cdh.ma_crd_stat_cd /*#*/end ma_crd_stat_cd_diff
, case when icd.ofo_crd_stat_cd = cdh.ofo_crd_stat_cd then '' else icd.ofo_crd_stat_cd||'<>'||cdh.ofo_crd_stat_cd /*#*/end ofo_crd_stat_cd_diff
, case when icd.cr_exp_dt = cdh.cr_exp_dt then '' else icd.cr_exp_dt||'<>'||cdh.cr_exp_dt /*#*/end cr_exp_dt_diff
, case when icd.hl_exp_dt = cdh.hl_exp_dt then '' else icd.hl_exp_dt||'<>'||cdh.hl_exp_dt /*#*/end hl_exp_dt_diff
, case when icd.ma_exp_dt = cdh.ma_exp_dt then '' else icd.ma_exp_dt||'<>'||cdh.ma_exp_dt /*#*/end ma_exp_dt_diff
, case when icd.cr_lst_canc_dt = cdh.cr_lst_canc_dt then '' else icd.cr_lst_canc_dt||'<>'||cdh.cr_lst_canc_dt /*#*/end cr_lst_canc_dt_diff
, case when icd.hl_lst_canc_dt = cdh.hl_lst_canc_dt then '' else icd.hl_lst_canc_dt||'<>'||cdh.hl_lst_canc_dt /*#*/end hl_lst_canc_dt_diff
, case when icd.ma_lst_canc_dt = cdh.ma_lst_canc_dt then '' else icd.ma_lst_canc_dt||'<>'||cdh.ma_lst_canc_dt /*#*/end ma_lst_canc_dt_diff
, case when icd.tl_lst_ord_dt = cdh.tl_lst_ord_dt then '' else icd.tl_lst_ord_dt||'<>'||cdh.tl_lst_ord_dt /*#*/end tl_lst_ord_dt_diff
, case when icd.cr_lst_pmt_dt = cdh.cr_lst_pmt_dt then '' else icd.cr_lst_pmt_dt||'<>'||cdh.cr_lst_pmt_dt /*#*/end cr_lst_pmt_dt_diff
, case when icd.hl_lst_pmt_dt = cdh.hl_lst_pmt_dt then '' else icd.hl_lst_pmt_dt||'<>'||cdh.hl_lst_pmt_dt /*#*/end hl_lst_pmt_dt_diff
, case when icd.ma_lst_pmt_dt = cdh.ma_lst_pmt_dt then '' else icd.ma_lst_pmt_dt||'<>'||cdh.ma_lst_pmt_dt /*#*/end ma_lst_pmt_dt_diff
, case when icd.cr_flg = cdh.cr_flg then '' else icd.cr_flg||'<>'||cdh.cr_flg /*#*/end cr_flg_diff
, case when icd.hl_flg = cdh.hl_flg then '' else icd.hl_flg||'<>'||cdh.hl_flg /*#*/end hl_flg_diff
, case when icd.ma_flg = cdh.ma_flg then '' else icd.ma_flg||'<>'||cdh.ma_flg /*#*/end ma_flg_diff
, case when icd.cr_exp_flg = cdh.cr_exp_flg then '' else icd.cr_exp_flg||'<>'||cdh.cr_exp_flg /*#*/end cr_exp_flg_diff
, case when icd.hl_exp_flg = cdh.hl_exp_flg then '' else icd.hl_exp_flg||'<>'||cdh.hl_exp_flg /*#*/end hl_exp_flg_diff
, case when icd.ma_exp_flg = cdh.ma_exp_flg then '' else icd.ma_exp_flg||'<>'||cdh.ma_exp_flg /*#*/end ma_exp_flg_diff
, case when icd.cr_curr_mbr_keycode = cdh.cr_curr_mbr_keycode then '' else icd.cr_curr_mbr_keycode||'<>'||cdh.cr_curr_mbr_keycode /*#*/end cr_curr_mbr_keycode_diff
, case when icd.hl_curr_mbr_keycode = cdh.hl_curr_mbr_keycode then '' else icd.hl_curr_mbr_keycode||'<>'||cdh.hl_curr_mbr_keycode /*#*/end hl_curr_mbr_keycode_diff
, case when icd.ma_curr_mbr_keycode = cdh.ma_curr_mbr_keycode then '' else icd.ma_curr_mbr_keycode||'<>'||cdh.ma_curr_mbr_keycode /*#*/end ma_curr_mbr_keycode_diff
, case when icd.cr_curr_ord_keycode = cdh.cr_curr_ord_keycode then '' else icd.cr_curr_ord_keycode||'<>'||cdh.cr_curr_ord_keycode /*#*/end cr_curr_ord_keycode_diff
, case when icd.hl_curr_ord_keycode = cdh.hl_curr_ord_keycode then '' else icd.hl_curr_ord_keycode||'<>'||cdh.hl_curr_ord_keycode /*#*/end hl_curr_ord_keycode_diff
, case when icd.ma_curr_ord_keycode = cdh.ma_curr_ord_keycode then '' else icd.ma_curr_ord_keycode||'<>'||cdh.ma_curr_ord_keycode /*#*/end ma_curr_ord_keycode_diff
, case when icd.cr_fst_mbr_keycode = cdh.cr_fst_mbr_keycode then '' else icd.cr_fst_mbr_keycode||'<>'||cdh.cr_fst_mbr_keycode /*#*/end cr_fst_mbr_keycode_diff
, case when icd.hl_fst_mbr_keycode = cdh.hl_fst_mbr_keycode then '' else icd.hl_fst_mbr_keycode||'<>'||cdh.hl_fst_mbr_keycode /*#*/end hl_fst_mbr_keycode_diff
, case when icd.ma_fst_mbr_keycode = cdh.ma_fst_mbr_keycode then '' else icd.ma_fst_mbr_keycode||'<>'||cdh.ma_fst_mbr_keycode /*#*/end ma_fst_mbr_keycode_diff
, case when icd.cr_lst_ord_keycode = cdh.cr_lst_ord_keycode then '' else icd.cr_lst_ord_keycode||'<>'||cdh.cr_lst_ord_keycode /*#*/end cr_lst_ord_keycode_diff
, case when icd.hl_lst_ord_keycode = cdh.hl_lst_ord_keycode then '' else icd.hl_lst_ord_keycode||'<>'||cdh.hl_lst_ord_keycode /*#*/end hl_lst_ord_keycode_diff
, case when icd.ma_lst_ord_keycode = cdh.ma_lst_ord_keycode then '' else icd.ma_lst_ord_keycode||'<>'||cdh.ma_lst_ord_keycode /*#*/end ma_lst_ord_keycode_diff
, case when icd.cr_lt_sub_flg = cdh.cr_lt_sub_flg then '' else icd.cr_lt_sub_flg||'<>'||cdh.cr_lt_sub_flg /*#*/end cr_lt_sub_flg_diff
, case when icd.hl_lt_sub_flg = cdh.hl_lt_sub_flg then '' else icd.hl_lt_sub_flg||'<>'||cdh.hl_lt_sub_flg /*#*/end hl_lt_sub_flg_diff
, case when icd.ma_lt_sub_flg = cdh.ma_lt_sub_flg then '' else icd.ma_lt_sub_flg||'<>'||cdh.ma_lt_sub_flg /*#*/end ma_lt_sub_flg_diff
, case when icd.cr_non_sub_dnr_flg = cdh.cr_non_sub_dnr_flg then '' else icd.cr_non_sub_dnr_flg||'<>'||cdh.cr_non_sub_dnr_flg /*#*/end cr_non_sub_dnr_flg_diff
, case when icd.hl_non_sub_dnr_flg = cdh.hl_non_sub_dnr_flg then '' else icd.hl_non_sub_dnr_flg||'<>'||cdh.hl_non_sub_dnr_flg /*#*/end hl_non_sub_dnr_flg_diff
, case when icd.ma_non_sub_dnr_flg = cdh.ma_non_sub_dnr_flg then '' else icd.ma_non_sub_dnr_flg||'<>'||cdh.ma_non_sub_dnr_flg /*#*/end ma_non_sub_dnr_flg_diff
, case when icd.cr_brks_cnt = cdh.cr_brks_cnt then '' else icd.cr_brks_cnt||'<>'||cdh.cr_brks_cnt /*#*/end cr_brks_cnt_diff
, case when icd.hl_brks_cnt = cdh.hl_brks_cnt then '' else icd.hl_brks_cnt||'<>'||cdh.hl_brks_cnt /*#*/end hl_brks_cnt_diff
, case when icd.ma_brks_cnt = cdh.ma_brks_cnt then '' else icd.ma_brks_cnt||'<>'||cdh.ma_brks_cnt /*#*/end ma_brks_cnt_diff
, case when icd.cr_rnw_cnt = cdh.cr_rnw_cnt then '' else icd.cr_rnw_cnt||'<>'||cdh.cr_rnw_cnt /*#*/end cr_rnw_cnt_diff
, case when icd.hl_rnw_cnt = cdh.hl_rnw_cnt then '' else icd.hl_rnw_cnt||'<>'||cdh.hl_rnw_cnt /*#*/end hl_rnw_cnt_diff
, case when icd.ma_rnw_cnt = cdh.ma_rnw_cnt then '' else icd.ma_rnw_cnt||'<>'||cdh.ma_rnw_cnt /*#*/end ma_rnw_cnt_diff
, case when icd.cr_rec_flg = cdh.cr_rec_flg then '' else icd.cr_rec_flg||'<>'||cdh.cr_rec_flg /*#*/end cr_rec_flg_diff
, case when icd.hl_rec_flg = cdh.hl_rec_flg then '' else icd.hl_rec_flg||'<>'||cdh.hl_rec_flg /*#*/end hl_rec_flg_diff
, case when icd.ma_rec_flg = cdh.ma_rec_flg then '' else icd.ma_rec_flg||'<>'||cdh.ma_rec_flg /*#*/end ma_rec_flg_diff
, case when icd.cr_svc_stat_cd = cdh.cr_svc_stat_cd then '' else icd.cr_svc_stat_cd||'<>'||cdh.cr_svc_stat_cd /*#*/end cr_svc_stat_cd_diff
, case when icd.hl_svc_stat_cd = cdh.hl_svc_stat_cd then '' else icd.hl_svc_stat_cd||'<>'||cdh.hl_svc_stat_cd /*#*/end hl_svc_stat_cd_diff
, case when icd.ma_svc_stat_cd = cdh.ma_svc_stat_cd then '' else icd.ma_svc_stat_cd||'<>'||cdh.ma_svc_stat_cd /*#*/end ma_svc_stat_cd_diff
, case when icd.cr_curr_mbr_src_cd = cdh.cr_curr_mbr_src_cd then '' else icd.cr_curr_mbr_src_cd||'<>'||cdh.cr_curr_mbr_src_cd /*#*/end cr_curr_mbr_src_cd_diff
, case when icd.hl_curr_mbr_src_cd = cdh.hl_curr_mbr_src_cd then '' else icd.hl_curr_mbr_src_cd||'<>'||cdh.hl_curr_mbr_src_cd /*#*/end hl_curr_mbr_src_cd_diff
, case when icd.ma_curr_mbr_src_cd = cdh.ma_curr_mbr_src_cd then '' else icd.ma_curr_mbr_src_cd||'<>'||cdh.ma_curr_mbr_src_cd /*#*/end ma_curr_mbr_src_cd_diff
, case when icd.cr_curr_ord_src_cd = cdh.cr_curr_ord_src_cd then '' else icd.cr_curr_ord_src_cd||'<>'||cdh.cr_curr_ord_src_cd /*#*/end cr_curr_ord_src_cd_diff
, case when icd.hl_curr_ord_src_cd = cdh.hl_curr_ord_src_cd then '' else icd.hl_curr_ord_src_cd||'<>'||cdh.hl_curr_ord_src_cd /*#*/end hl_curr_ord_src_cd_diff
, case when icd.ma_curr_ord_src_cd = cdh.ma_curr_ord_src_cd then '' else icd.ma_curr_ord_src_cd||'<>'||cdh.ma_curr_ord_src_cd /*#*/end ma_curr_ord_src_cd_diff
, case when icd.cr_fst_ord_src_cd = cdh.cr_fst_ord_src_cd then '' else icd.cr_fst_ord_src_cd||'<>'||cdh.cr_fst_ord_src_cd /*#*/end cr_fst_ord_src_cd_diff

, '||                    ICD                    >>' as sep1, icd.*
, '||                    CDH                    >>' as sep2, cdh.*

from  icd full join cdh
on icd.icd_id = cdh.icd_id and icd.cdh_id = cdh.cdh_id
where icd.cr_canc_cust_flg = cdh.cr_canc_cust_flg and icd.hl_canc_cust_flg = cdh.hl_canc_cust_flg and  icd.ma_canc_cust_flg = cdh.ma_canc_cust_flg

------------------------------------------------------------------------RUN ALL ABOVE
;
drop table  cr_temp.diff_mt_offline_summary
;
select cr_curr_mbr_keycode, cr_fst_mbr_keycode, cr_lst_ord_keycode
from   cr_temp.GTT_MT_OFFLINE_SUMMARY  where icd_id = 10346900009921000
;
select cr_curr_mbr_keycode, cr_fst_mbr_keycode, cr_lst_ord_keycode
from   cr_temp.gtt_agg_print_summary  where cdh_id = 1222222492
;
select count(*) from   cr_temp.GTT_MT_OFFLINE_SUMMARY
;
select count(*) from   cr_temp.gtt_agg_print_summary
;
--truncate table    cr_temp.GTT_MT_OFFLINE_SUMMARY;
drop table  cr_temp.GTT_MT_OFFLINE_SUMMARY;
--truncate table cr_temp.gtt_agg_print_summary;
drop table  cr_temp.gtt_agg_print_summary;

;

     select hl_actv_flg, cr_actv_rptg_flg, hl_actv_pd_flg
  from prod.agg_print_summary
 where 1=1 and individual_id = 1201773447
 limit 1000;
 
 select hl_actv_flg, cr_actv_rptg_flg, hl_actv_pd_flg
  from cr_temp.mt_offline_summary
 where 1=1 and individual_id  = 10002000129088050
 limit 1000;
 
 select count(*) from cr_temp.mt_account_number;
 
 with tt1 as (
 select 
   substring(MAX(CASE WHEN oa.magazine_code = 'CNS'
                   AND oa.hash_account_id = ct.cr_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       NVL(oa.svc_status_code,'')
                  WHEN de.magazine_code = 'CNS'
                  THEN '-1000000'||
                       'C'
             END),9) cr_svc_stat_cd
	FROM                agg.print_external_ref_temp     er      
        left join       agg.offline_account_tmp4        oa      on er.hash_account_id  = oa.hash_account_id
        left join       agg.agg_print_order_tmp3        ofo     on oa.hash_account_id  = ofo.hash_account_id
        left join       agg.print_cancel_act            OCA     on ofo.ord_id          = oca.action_id      
        left join       agg.mt_ofl_brks_temp            obt     on er.individual_id    = obt.individual_id              
        left join       etl_proc.lookup_magazine        iss     on oa.magazine_code    = iss.magazine_code  and oa.expr_iss_num = iss.iss_num
        left join       agg.mt_ofl_de_temp              de      on er.individual_id    = de.individual_id
        left join       agg.mt_ofl_combine_temp         ct      on er.individual_id    = ct.individual_id
        left join       agg.mt_ofl_sum_tmp1             de_cnt  on er.individual_id    = de_cnt.individual_id
        left join       agg.mt_ofl_sum_tmp2             ofo2    on er.individual_id    = ofo2.individual_id
WHERE 
    NOT (oa.hash_account_id IS NULL AND oa.source_account_id IS NULL AND de.individual_id IS NULL)
	and individual_id = 1216602929
GROUP BY er.individual_id)
select *
from tt1