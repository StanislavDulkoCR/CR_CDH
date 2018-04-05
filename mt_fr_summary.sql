;
drop table if exists  cr_temp.GTT_MT_FR_SUMMARY;
create table cr_temp.GTT_MT_FR_SUMMARY as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
, NVL2(trunc(nvl(fr_avg_don_amt,'0')), trunc(nvl(fr_avg_don_amt,'0'))::text,'(NULL)') as fr_avg_don_amt
, NVL2(trunc(nvl(fr_avg_gross_don_amt,'0')), trunc(nvl(fr_avg_gross_don_amt,'0'))::text,'(NULL)') as fr_avg_gross_don_amt
, NVL2(trunc(nvl(fr_lst_don_amt,'0')), trunc(nvl(fr_lst_don_amt,'0'))::text,'(NULL)') as fr_lst_don_amt
, NVL2(trunc(nvl(fr_ltd_don_amt,'0')), trunc(nvl(fr_ltd_don_amt,'0'))::text,'(NULL)') as fr_ltd_don_amt
, NVL2(trunc(nvl(fr_max_don_amt,'0')), trunc(nvl(fr_max_don_amt,'0'))::text,'(NULL)') as fr_max_don_amt, NVL2(trunc(nvl(fr_prior_don_amt,'0')), trunc(nvl(fr_prior_don_amt,'0'))::text,'(NULL)') as fr_prior_don_amt
, NVL2(TO_CHAR(fr_fst_don_dt,'DD-MON-YY'), TO_CHAR(fr_fst_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_fst_don_dt
, NVL2(TO_CHAR(fr_lst_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_don_dt, NVL2(TO_CHAR(fr_prior_don_dt,'DD-MON-YY'), TO_CHAR(fr_prior_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_prior_don_dt, NVL2(fr_dnr_act_flg, fr_dnr_act_flg::text,'(NULL)') as fr_dnr_act_flg
, NVL2(fr_dnr_inact_flg, fr_dnr_inact_flg::text,'(NULL)') as fr_dnr_inact_flg
, NVL2(fr_dnr_lps_flg, fr_dnr_lps_flg::text,'(NULL)') as fr_dnr_lps_flg
, NVL2(fr_mbr_gvng_lvl_cd, fr_mbr_gvng_lvl_cd::text,'(NULL)') as fr_mbr_gvng_lvl_cd
, NVL2(TO_CHAR(fr_mbr_exp_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_exp_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_exp_dt
, NVL2(fr_fst_act_keycode, fr_fst_act_keycode::text,'(NULL)') as fr_fst_act_keycode
, NVL2(fr_lst_act_keycode, fr_lst_act_keycode::text,'(NULL)') as fr_lst_act_keycode
, NVL2(fr_prior_act_keycode, fr_prior_act_keycode::text,'(NULL)') as fr_prior_act_keycode, NVL2(trunc(nvl(fr_ltd_don_cnt,'0')), trunc(nvl(fr_ltd_don_cnt,'0'))::text,'(NULL)') as fr_ltd_don_cnt
, NVL2(trunc(nvl(fr_don_ref_cnt,'0')), trunc(nvl(fr_don_ref_cnt,'0'))::text,'(NULL)') as fr_don_ref_cnt, NVL2(trunc(nvl(fr_times_ren_cnt,'0')), trunc(nvl(fr_times_ren_cnt,'0'))::text,'(NULL)') as fr_times_ren_cnt
, NVL2(fr_lst_don_src_cd, fr_lst_don_src_cd::text,'(NULL)') as fr_lst_don_src_cd, NVL2(trunc(nvl(fr_lst_rfl_don_amt,'0')), trunc(nvl(fr_lst_rfl_don_amt,'0'))::text,'(NULL)') as fr_lst_rfl_don_amt
, NVL2(trunc(nvl(fr_lst_non_rfl_don_amt,'0')), trunc(nvl(fr_lst_non_rfl_don_amt,'0'))::text,'(NULL)') as fr_lst_non_rfl_don_amt
, NVL2(TO_CHAR(fr_mbr_frst_strt_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_frst_strt_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_frst_strt_dt
, NVL2(fr_mbr_lst_keycode, fr_mbr_lst_keycode::text,'(NULL)') as fr_mbr_lst_keycode
, NVL2(trunc(nvl(fr_mbr_lst_ren_don_amt,'0')), trunc(nvl(fr_mbr_lst_ren_don_amt,'0'))::text,'(NULL)') as fr_mbr_lst_ren_don_amt
, NVL2(fr_tof_cd, fr_tof_cd::text,'(NULL)') as fr_tof_cd
, NVL2(fr_lt_dnr_flg, fr_lt_dnr_flg::text,'(NULL)') as fr_lt_dnr_flg
, NVL2(trunc(nvl(fr_mbr_lst_add_don_amt,'0')), trunc(nvl(fr_mbr_lst_add_don_amt,'0'))::text,'(NULL)') as fr_mbr_lst_add_don_amt
, NVL2(TO_CHAR(fr_mbr_lst_add_don_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_lst_add_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_lst_add_don_dt
, NVL2(TO_CHAR(fr_mbr_pres_cir_frst_strt_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_pres_cir_frst_strt_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_pres_cir_frst_strt_dt
, NVL2(fr_fst_don_keycode, fr_fst_don_keycode::text,'(NULL)') as fr_fst_don_keycode
, NVL2(trunc(nvl(fr_max_don_amt_12_mth,'0')), trunc(nvl(fr_max_don_amt_12_mth,'0'))::text,'(NULL)') as fr_max_don_amt_12_mth
, NVL2(trunc(nvl(fr_max_don_amt_24_mth,'0')), trunc(nvl(fr_max_don_amt_24_mth,'0'))::text,'(NULL)') as fr_max_don_amt_24_mth
, NVL2(trunc(nvl(fr_max_don_amt_36_mth,'0')), trunc(nvl(fr_max_don_amt_36_mth,'0'))::text,'(NULL)') as fr_max_don_amt_36_mth
, NVL2(fr_track_number, fr_track_number::text,'(NULL)') as fr_track_number
, NVL2(fr_conv_tag_rsp_flg, fr_conv_tag_rsp_flg::text,'(NULL)') as fr_conv_tag_rsp_flg
, NVL2(trunc(nvl(fr_fst_don_amt,'0')), trunc(nvl(fr_fst_don_amt,'0'))::text,'(NULL)') as fr_fst_don_amt
, NVL2(TO_CHAR(fr_lst_rfl_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_rfl_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_rfl_don_dt
, NVL2(TO_CHAR(fr_lst_non_rfl_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_non_rfl_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_non_rfl_don_dt
, NVL2(trunc(nvl(fr_lst_eml_don_amt,'0')), trunc(nvl(fr_lst_eml_don_amt,'0'))::text,'(NULL)') as fr_lst_eml_don_amt
, NVL2(TO_CHAR(fr_lst_eml_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_eml_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_eml_don_dt
, NVL2(fr_coop_eligible_flg, fr_coop_eligible_flg::text,'(NULL)') as fr_coop_eligible_flg
, NVL2(trunc(nvl(fr_lst_dm_don_amt,'0')), trunc(nvl(fr_lst_dm_don_amt,'0'))::text,'(NULL)') as fr_lst_dm_don_amt
, NVL2(TO_CHAR(fr_lst_dm_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_dm_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_dm_don_dt
, NVL2(trunc(nvl(fr_lst_org_onl_don_amt,'0')), trunc(nvl(fr_lst_org_onl_don_amt,'0'))::text,'(NULL)') as fr_lst_org_onl_don_amt
, NVL2(TO_CHAR(fr_lst_org_onl_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_org_onl_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_org_onl_don_dt
, NVL2(trunc(nvl(fr_lst_ecomm_don_amt,'0')), trunc(nvl(fr_lst_ecomm_don_amt,'0'))::text,'(NULL)') as fr_lst_ecomm_don_amt
, NVL2(TO_CHAR(fr_lst_ecomm_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_ecomm_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_ecomm_don_dt
/*---END---*/

from cr_temp.MT_FR_SUMMARY T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;

drop table if exists  cr_temp.gtt_agg_fundraising_summary;
CREATE TABLE cr_temp.gtt_agg_fundraising_summary AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
, NVL2(trunc(nvl(fr_avg_don_amt,'0')), trunc(nvl(fr_avg_don_amt,'0'))::text,'(NULL)') as fr_avg_don_amt
, NVL2(trunc(nvl(fr_avg_gross_don_amt,'0')), trunc(nvl(fr_avg_gross_don_amt,'0'))::text,'(NULL)') as fr_avg_gross_don_amt
, NVL2(trunc(nvl(fr_lst_don_amt,'0')), trunc(nvl(fr_lst_don_amt,'0'))::text,'(NULL)') as fr_lst_don_amt
, NVL2(trunc(nvl(fr_ltd_don_amt,'0')), trunc(nvl(fr_ltd_don_amt,'0'))::text,'(NULL)') as fr_ltd_don_amt
, NVL2(trunc(nvl(fr_max_don_amt,'0')), trunc(nvl(fr_max_don_amt,'0'))::text,'(NULL)') as fr_max_don_amt, NVL2(trunc(nvl(fr_prior_don_amt,'0')), trunc(nvl(fr_prior_don_amt,'0'))::text,'(NULL)') as fr_prior_don_amt
, NVL2(TO_CHAR(fr_fst_don_dt,'DD-MON-YY'), TO_CHAR(fr_fst_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_fst_don_dt
, NVL2(TO_CHAR(fr_lst_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_don_dt, NVL2(TO_CHAR(fr_prior_don_dt,'DD-MON-YY'), TO_CHAR(fr_prior_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_prior_don_dt, NVL2(fr_dnr_act_flg, fr_dnr_act_flg::text,'(NULL)') as fr_dnr_act_flg
, NVL2(fr_dnr_inact_flg, fr_dnr_inact_flg::text,'(NULL)') as fr_dnr_inact_flg
, NVL2(fr_dnr_lps_flg, fr_dnr_lps_flg::text,'(NULL)') as fr_dnr_lps_flg
, NVL2(fr_mbr_gvng_lvl_cd, fr_mbr_gvng_lvl_cd::text,'(NULL)') as fr_mbr_gvng_lvl_cd
, NVL2(TO_CHAR(fr_mbr_exp_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_exp_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_exp_dt
, NVL2(fr_fst_act_keycode, fr_fst_act_keycode::text,'(NULL)') as fr_fst_act_keycode
, NVL2(fr_lst_act_keycode, fr_lst_act_keycode::text,'(NULL)') as fr_lst_act_keycode
, NVL2(fr_prior_act_keycode, fr_prior_act_keycode::text,'(NULL)') as fr_prior_act_keycode, NVL2(trunc(nvl(fr_ltd_don_cnt,'0')), trunc(nvl(fr_ltd_don_cnt,'0'))::text,'(NULL)') as fr_ltd_don_cnt
, NVL2(trunc(nvl(fr_don_ref_cnt,'0')), trunc(nvl(fr_don_ref_cnt,'0'))::text,'(NULL)') as fr_don_ref_cnt, NVL2(trunc(nvl(fr_times_ren_cnt,'0')), trunc(nvl(fr_times_ren_cnt,'0'))::text,'(NULL)') as fr_times_ren_cnt
, NVL2(fr_lst_don_src_cd, fr_lst_don_src_cd::text,'(NULL)') as fr_lst_don_src_cd, NVL2(trunc(nvl(fr_lst_rfl_don_amt,'0')), trunc(nvl(fr_lst_rfl_don_amt,'0'))::text,'(NULL)') as fr_lst_rfl_don_amt
, NVL2(trunc(nvl(fr_lst_non_rfl_don_amt,'0')), trunc(nvl(fr_lst_non_rfl_don_amt,'0'))::text,'(NULL)') as fr_lst_non_rfl_don_amt
, NVL2(TO_CHAR(fr_mbr_frst_strt_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_frst_strt_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_frst_strt_dt
, NVL2(fr_mbr_lst_keycode, fr_mbr_lst_keycode::text,'(NULL)') as fr_mbr_lst_keycode
, NVL2(trunc(nvl(fr_mbr_lst_ren_don_amt,'0')), trunc(nvl(fr_mbr_lst_ren_don_amt,'0'))::text,'(NULL)') as fr_mbr_lst_ren_don_amt
, NVL2(fr_tof_cd, fr_tof_cd::text,'(NULL)') as fr_tof_cd
, NVL2(fr_lt_dnr_flg, fr_lt_dnr_flg::text,'(NULL)') as fr_lt_dnr_flg
, NVL2(trunc(nvl(fr_mbr_lst_add_don_amt,'0')), trunc(nvl(fr_mbr_lst_add_don_amt,'0'))::text,'(NULL)') as fr_mbr_lst_add_don_amt
, NVL2(TO_CHAR(fr_mbr_lst_add_don_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_lst_add_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_lst_add_don_dt
, NVL2(TO_CHAR(fr_mbr_pres_cir_frst_strt_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_pres_cir_frst_strt_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_pres_cir_frst_strt_dt
, NVL2(fr_fst_don_keycode, fr_fst_don_keycode::text,'(NULL)') as fr_fst_don_keycode
, NVL2(trunc(nvl(fr_max_don_amt_12_mth,'0')), trunc(nvl(fr_max_don_amt_12_mth,'0'))::text,'(NULL)') as fr_max_don_amt_12_mth
, NVL2(trunc(nvl(fr_max_don_amt_24_mth,'0')), trunc(nvl(fr_max_don_amt_24_mth,'0'))::text,'(NULL)') as fr_max_don_amt_24_mth
, NVL2(trunc(nvl(fr_max_don_amt_36_mth,'0')), trunc(nvl(fr_max_don_amt_36_mth,'0'))::text,'(NULL)') as fr_max_don_amt_36_mth
, NVL2(fr_track_number, fr_track_number::text,'(NULL)') as fr_track_number
, NVL2(fr_conv_tag_rsp_flg, fr_conv_tag_rsp_flg::text,'(NULL)') as fr_conv_tag_rsp_flg
, NVL2(trunc(nvl(fr_fst_don_amt,'0')), trunc(nvl(fr_fst_don_amt,'0'))::text,'(NULL)') as fr_fst_don_amt
, NVL2(TO_CHAR(fr_lst_rfl_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_rfl_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_rfl_don_dt
, NVL2(TO_CHAR(fr_lst_non_rfl_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_non_rfl_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_non_rfl_don_dt
, NVL2(trunc(nvl(fr_lst_eml_don_amt,'0')), trunc(nvl(fr_lst_eml_don_amt,'0'))::text,'(NULL)') as fr_lst_eml_don_amt
, NVL2(TO_CHAR(fr_lst_eml_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_eml_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_eml_don_dt
, NVL2(fr_coop_eligible_flg, fr_coop_eligible_flg::text,'(NULL)') as fr_coop_eligible_flg
, NVL2(trunc(nvl(fr_lst_dm_don_amt,'0')), trunc(nvl(fr_lst_dm_don_amt,'0'))::text,'(NULL)') as fr_lst_dm_don_amt
, NVL2(TO_CHAR(fr_lst_dm_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_dm_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_dm_don_dt
, NVL2(trunc(nvl(fr_lst_org_onl_don_amt,'0')), trunc(nvl(fr_lst_org_onl_don_amt,'0'))::text,'(NULL)') as fr_lst_org_onl_don_amt
, NVL2(TO_CHAR(fr_lst_org_onl_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_org_onl_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_org_onl_don_dt
, NVL2(trunc(nvl(fr_lst_ecomm_don_amt,'0')), trunc(nvl(fr_lst_ecomm_don_amt,'0'))::text,'(NULL)') as fr_lst_ecomm_don_amt
, NVL2(TO_CHAR(fr_lst_ecomm_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_ecomm_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_ecomm_don_dt
/*---END---*/
from prod.agg_fundraising_summary T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;



------------------------------------------------------------------------TABLE CREATION END\;
drop table if exists  cr_temp.diff_MT_FR_SUMMARY;
CREATE TABLE cr_temp.diff_MT_FR_SUMMARY AS
with hash_diff as (

select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID     , fr_avg_don_amt, fr_avg_gross_don_amt, fr_lst_don_amt, fr_ltd_don_amt, fr_max_don_amt, fr_prior_don_amt, fr_fst_don_dt, fr_lst_don_dt, fr_prior_don_dt, fr_dnr_act_flg, fr_dnr_inact_flg, fr_dnr_lps_flg, fr_mbr_gvng_lvl_cd, fr_mbr_exp_dt, fr_fst_act_keycode, fr_lst_act_keycode, fr_prior_act_keycode, fr_ltd_don_cnt, fr_don_ref_cnt, fr_times_ren_cnt, fr_lst_don_src_cd, fr_lst_rfl_don_amt, fr_lst_non_rfl_don_amt, fr_mbr_frst_strt_dt, fr_mbr_lst_keycode, fr_mbr_lst_ren_don_amt, fr_tof_cd, fr_lt_dnr_flg, fr_mbr_lst_add_don_amt, fr_mbr_lst_add_don_dt, fr_mbr_pres_cir_frst_strt_dt, fr_fst_don_keycode, fr_max_don_amt_12_mth, fr_max_don_amt_24_mth, fr_max_don_amt_36_mth, fr_track_number, fr_conv_tag_rsp_flg, fr_fst_don_amt, fr_lst_rfl_don_dt, fr_lst_non_rfl_don_dt, fr_lst_eml_don_amt, fr_lst_eml_don_dt, fr_coop_eligible_flg, fr_lst_dm_don_amt, fr_lst_dm_don_dt, fr_lst_org_onl_don_amt, fr_lst_org_onl_don_dt, fr_lst_ecomm_don_amt, fr_lst_ecomm_don_dt
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL , fr_avg_don_amt, fr_avg_gross_don_amt, fr_lst_don_amt, fr_ltd_don_amt, fr_max_don_amt, fr_prior_don_amt, fr_fst_don_dt, fr_lst_don_dt, fr_prior_don_dt, fr_dnr_act_flg, fr_dnr_inact_flg, fr_dnr_lps_flg, fr_mbr_gvng_lvl_cd, fr_mbr_exp_dt, fr_fst_act_keycode, fr_lst_act_keycode, fr_prior_act_keycode, fr_ltd_don_cnt, fr_don_ref_cnt, fr_times_ren_cnt, fr_lst_don_src_cd, fr_lst_rfl_don_amt, fr_lst_non_rfl_don_amt, fr_mbr_frst_strt_dt, fr_mbr_lst_keycode, fr_mbr_lst_ren_don_amt, fr_tof_cd, fr_lt_dnr_flg, fr_mbr_lst_add_don_amt, fr_mbr_lst_add_don_dt, fr_mbr_pres_cir_frst_strt_dt, fr_fst_don_keycode, fr_max_don_amt_12_mth, fr_max_don_amt_24_mth, fr_max_don_amt_36_mth, fr_track_number, fr_conv_tag_rsp_flg, fr_fst_don_amt, fr_lst_rfl_don_dt, fr_lst_non_rfl_don_dt, fr_lst_eml_don_amt, fr_lst_eml_don_dt, fr_coop_eligible_flg, fr_lst_dm_don_amt, fr_lst_dm_don_dt, fr_lst_org_onl_don_amt, fr_lst_org_onl_don_dt, fr_lst_ecomm_don_amt, fr_lst_ecomm_don_dt
from cr_temp.GTT_MT_FR_SUMMARY
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL , fr_avg_don_amt, fr_avg_gross_don_amt, fr_lst_don_amt, fr_ltd_don_amt, fr_max_don_amt, fr_prior_don_amt, fr_fst_don_dt, fr_lst_don_dt, fr_prior_don_dt, fr_dnr_act_flg, fr_dnr_inact_flg, fr_dnr_lps_flg, fr_mbr_gvng_lvl_cd, fr_mbr_exp_dt, fr_fst_act_keycode, fr_lst_act_keycode, fr_prior_act_keycode, fr_ltd_don_cnt, fr_don_ref_cnt, fr_times_ren_cnt, fr_lst_don_src_cd, fr_lst_rfl_don_amt, fr_lst_non_rfl_don_amt, fr_mbr_frst_strt_dt, fr_mbr_lst_keycode, fr_mbr_lst_ren_don_amt, fr_tof_cd, fr_lt_dnr_flg, fr_mbr_lst_add_don_amt, fr_mbr_lst_add_don_dt, fr_mbr_pres_cir_frst_strt_dt, fr_fst_don_keycode, fr_max_don_amt_12_mth, fr_max_don_amt_24_mth, fr_max_don_amt_36_mth, fr_track_number, fr_conv_tag_rsp_flg, fr_fst_don_amt, fr_lst_rfl_don_dt, fr_lst_non_rfl_don_dt, fr_lst_eml_don_amt, fr_lst_eml_don_dt, fr_coop_eligible_flg, fr_lst_dm_don_amt, fr_lst_dm_don_dt, fr_lst_org_onl_don_amt, fr_lst_org_onl_don_dt, fr_lst_ecomm_don_amt, fr_lst_ecomm_don_dt
from cr_temp.gtt_agg_fundraising_summary
)

group by ICD_ID, CDH_ID                                                              , fr_avg_don_amt, fr_avg_gross_don_amt, fr_lst_don_amt, fr_ltd_don_amt, fr_max_don_amt, fr_prior_don_amt, fr_fst_don_dt, fr_lst_don_dt, fr_prior_don_dt, fr_dnr_act_flg, fr_dnr_inact_flg, fr_dnr_lps_flg, fr_mbr_gvng_lvl_cd, fr_mbr_exp_dt, fr_fst_act_keycode, fr_lst_act_keycode, fr_prior_act_keycode, fr_ltd_don_cnt, fr_don_ref_cnt, fr_times_ren_cnt, fr_lst_don_src_cd, fr_lst_rfl_don_amt, fr_lst_non_rfl_don_amt, fr_mbr_frst_strt_dt, fr_mbr_lst_keycode, fr_mbr_lst_ren_don_amt, fr_tof_cd, fr_lt_dnr_flg, fr_mbr_lst_add_don_amt, fr_mbr_lst_add_don_dt, fr_mbr_pres_cir_frst_strt_dt, fr_fst_don_keycode, fr_max_don_amt_12_mth, fr_max_don_amt_24_mth, fr_max_don_amt_36_mth, fr_track_number, fr_conv_tag_rsp_flg, fr_fst_don_amt, fr_lst_rfl_don_dt, fr_lst_non_rfl_don_dt, fr_lst_eml_don_amt, fr_lst_eml_don_dt, fr_coop_eligible_flg, fr_lst_dm_don_amt, fr_lst_dm_don_dt, fr_lst_org_onl_don_amt, fr_lst_org_onl_don_dt, fr_lst_ecomm_don_amt, fr_lst_ecomm_don_dt
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select * from hash_diff hd

 where exists (select null from cr_temp.fullmatch_id_agg_fundraising_don  fullmatch where hd.cdh_id = fullmatch.cdh_id)
 
;
select count(distinct icd_id) as unique_icd_id, count(distinct cdh_id) as unique_cdh_id, count(*) as rows_total_count, count(*)/2 as rowshalf_total_count, (select count(*) from cr_temp.GTT_MT_FR_SUMMARY) as total_ICD, (select count(*) from cr_temp.gtt_agg_fundraising_summary)  as total_CDH 
from cr_temp.diff_MT_FR_SUMMARY

;
drop table if exists cr_temp.comp_diff_MT_FR_SUMMARY;
CREATE TABLE cr_temp.comp_diff_MT_FR_SUMMARY AS
with icd as (select * from cr_temp.diff_MT_FR_SUMMARY where 1=1 and cnt_icd_fl = 1)
,    cdh as (select * from cr_temp.diff_MT_FR_SUMMARY where 1=1 and cnt_cdh_fl = 1)
select icd.cnt_icd_fl||cdh.cnt_cdh_fl as match_fl, nvl(icd.icd_id,cdh.icd_id) as icd_id ,cdh.cdh_id
, case when icd.fr_avg_don_amt = cdh.fr_avg_don_amt then '' else icd.fr_avg_don_amt||'<>'||cdh.fr_avg_don_amt /*#*/end fr_avg_don_amt_diff
, case when icd.fr_avg_gross_don_amt = cdh.fr_avg_gross_don_amt then '' else icd.fr_avg_gross_don_amt||'<>'||cdh.fr_avg_gross_don_amt /*#*/end fr_avg_gross_don_amt_diff
, case when icd.fr_lst_don_amt = cdh.fr_lst_don_amt then '' else icd.fr_lst_don_amt||'<>'||cdh.fr_lst_don_amt /*#*/end fr_lst_don_amt_diff
, case when icd.fr_ltd_don_amt = cdh.fr_ltd_don_amt then '' else icd.fr_ltd_don_amt||'<>'||cdh.fr_ltd_don_amt /*#*/end fr_ltd_don_amt_diff
, case when icd.fr_max_don_amt = cdh.fr_max_don_amt then '' else icd.fr_max_don_amt||'<>'||cdh.fr_max_don_amt /*#*/end fr_max_don_amt_diff, case when icd.fr_prior_don_amt = cdh.fr_prior_don_amt then '' else icd.fr_prior_don_amt||'<>'||cdh.fr_prior_don_amt /*#*/end fr_prior_don_amt_diff
, case when icd.fr_fst_don_dt = cdh.fr_fst_don_dt then '' else icd.fr_fst_don_dt||'<>'||cdh.fr_fst_don_dt /*#*/end fr_fst_don_dt_diff
, case when icd.fr_lst_don_dt = cdh.fr_lst_don_dt then '' else icd.fr_lst_don_dt||'<>'||cdh.fr_lst_don_dt /*#*/end fr_lst_don_dt_diff, case when icd.fr_prior_don_dt = cdh.fr_prior_don_dt then '' else icd.fr_prior_don_dt||'<>'||cdh.fr_prior_don_dt /*#*/end fr_prior_don_dt_diff, case when icd.fr_dnr_act_flg = cdh.fr_dnr_act_flg then '' else icd.fr_dnr_act_flg||'<>'||cdh.fr_dnr_act_flg /*#*/end fr_dnr_act_flg_diff
, case when icd.fr_dnr_inact_flg = cdh.fr_dnr_inact_flg then '' else icd.fr_dnr_inact_flg||'<>'||cdh.fr_dnr_inact_flg /*#*/end fr_dnr_inact_flg_diff
, case when icd.fr_dnr_lps_flg = cdh.fr_dnr_lps_flg then '' else icd.fr_dnr_lps_flg||'<>'||cdh.fr_dnr_lps_flg /*#*/end fr_dnr_lps_flg_diff
, case when icd.fr_mbr_gvng_lvl_cd = cdh.fr_mbr_gvng_lvl_cd then '' else icd.fr_mbr_gvng_lvl_cd||'<>'||cdh.fr_mbr_gvng_lvl_cd /*#*/end fr_mbr_gvng_lvl_cd_diff
, case when icd.fr_mbr_exp_dt = cdh.fr_mbr_exp_dt then '' else icd.fr_mbr_exp_dt||'<>'||cdh.fr_mbr_exp_dt /*#*/end fr_mbr_exp_dt_diff
, case when icd.fr_fst_act_keycode = cdh.fr_fst_act_keycode then '' else icd.fr_fst_act_keycode||'<>'||cdh.fr_fst_act_keycode /*#*/end fr_fst_act_keycode_diff
, case when icd.fr_lst_act_keycode = cdh.fr_lst_act_keycode then '' else icd.fr_lst_act_keycode||'<>'||cdh.fr_lst_act_keycode /*#*/end fr_lst_act_keycode_diff
, case when icd.fr_prior_act_keycode = cdh.fr_prior_act_keycode then '' else icd.fr_prior_act_keycode||'<>'||cdh.fr_prior_act_keycode /*#*/end fr_prior_act_keycode_diff, case when icd.fr_ltd_don_cnt = cdh.fr_ltd_don_cnt then '' else icd.fr_ltd_don_cnt||'<>'||cdh.fr_ltd_don_cnt /*#*/end fr_ltd_don_cnt_diff
, case when icd.fr_don_ref_cnt = cdh.fr_don_ref_cnt then '' else icd.fr_don_ref_cnt||'<>'||cdh.fr_don_ref_cnt /*#*/end fr_don_ref_cnt_diff, case when icd.fr_times_ren_cnt = cdh.fr_times_ren_cnt then '' else icd.fr_times_ren_cnt||'<>'||cdh.fr_times_ren_cnt /*#*/end fr_times_ren_cnt_diff
, case when icd.fr_lst_don_src_cd = cdh.fr_lst_don_src_cd then '' else icd.fr_lst_don_src_cd||'<>'||cdh.fr_lst_don_src_cd /*#*/end fr_lst_don_src_cd_diff, case when icd.fr_lst_rfl_don_amt = cdh.fr_lst_rfl_don_amt then '' else icd.fr_lst_rfl_don_amt||'<>'||cdh.fr_lst_rfl_don_amt /*#*/end fr_lst_rfl_don_amt_diff
, case when icd.fr_lst_non_rfl_don_amt = cdh.fr_lst_non_rfl_don_amt then '' else icd.fr_lst_non_rfl_don_amt||'<>'||cdh.fr_lst_non_rfl_don_amt /*#*/end fr_lst_non_rfl_don_amt_diff
, case when icd.fr_mbr_frst_strt_dt = cdh.fr_mbr_frst_strt_dt then '' else icd.fr_mbr_frst_strt_dt||'<>'||cdh.fr_mbr_frst_strt_dt /*#*/end fr_mbr_frst_strt_dt_diff
, case when icd.fr_mbr_lst_keycode = cdh.fr_mbr_lst_keycode then '' else icd.fr_mbr_lst_keycode||'<>'||cdh.fr_mbr_lst_keycode /*#*/end fr_mbr_lst_keycode_diff
, case when icd.fr_mbr_lst_ren_don_amt = cdh.fr_mbr_lst_ren_don_amt then '' else icd.fr_mbr_lst_ren_don_amt||'<>'||cdh.fr_mbr_lst_ren_don_amt /*#*/end fr_mbr_lst_ren_don_amt_diff
, case when icd.fr_tof_cd = cdh.fr_tof_cd then '' else icd.fr_tof_cd||'<>'||cdh.fr_tof_cd /*#*/end fr_tof_cd_diff
, case when icd.fr_lt_dnr_flg = cdh.fr_lt_dnr_flg then '' else icd.fr_lt_dnr_flg||'<>'||cdh.fr_lt_dnr_flg /*#*/end fr_lt_dnr_flg_diff
, case when icd.fr_mbr_lst_add_don_amt = cdh.fr_mbr_lst_add_don_amt then '' else icd.fr_mbr_lst_add_don_amt||'<>'||cdh.fr_mbr_lst_add_don_amt /*#*/end fr_mbr_lst_add_don_amt_diff
, case when icd.fr_mbr_lst_add_don_dt = cdh.fr_mbr_lst_add_don_dt then '' else icd.fr_mbr_lst_add_don_dt||'<>'||cdh.fr_mbr_lst_add_don_dt /*#*/end fr_mbr_lst_add_don_dt_diff
, case when icd.fr_mbr_pres_cir_frst_strt_dt = cdh.fr_mbr_pres_cir_frst_strt_dt then '' else icd.fr_mbr_pres_cir_frst_strt_dt||'<>'||cdh.fr_mbr_pres_cir_frst_strt_dt /*#*/end fr_mbr_pres_cir_frst_strt_dt_diff
, case when icd.fr_fst_don_keycode = cdh.fr_fst_don_keycode then '' else icd.fr_fst_don_keycode||'<>'||cdh.fr_fst_don_keycode /*#*/end fr_fst_don_keycode_diff
, case when icd.fr_max_don_amt_12_mth = cdh.fr_max_don_amt_12_mth then '' else icd.fr_max_don_amt_12_mth||'<>'||cdh.fr_max_don_amt_12_mth /*#*/end fr_max_don_amt_12_mth_diff
, case when icd.fr_max_don_amt_24_mth = cdh.fr_max_don_amt_24_mth then '' else icd.fr_max_don_amt_24_mth||'<>'||cdh.fr_max_don_amt_24_mth /*#*/end fr_max_don_amt_24_mth_diff
, case when icd.fr_max_don_amt_36_mth = cdh.fr_max_don_amt_36_mth then '' else icd.fr_max_don_amt_36_mth||'<>'||cdh.fr_max_don_amt_36_mth /*#*/end fr_max_don_amt_36_mth_diff
, case when icd.fr_track_number = cdh.fr_track_number then '' else icd.fr_track_number||'<>'||cdh.fr_track_number /*#*/end fr_track_number_diff
, case when icd.fr_conv_tag_rsp_flg = cdh.fr_conv_tag_rsp_flg then '' else icd.fr_conv_tag_rsp_flg||'<>'||cdh.fr_conv_tag_rsp_flg /*#*/end fr_conv_tag_rsp_flg_diff
, case when icd.fr_fst_don_amt = cdh.fr_fst_don_amt then '' else icd.fr_fst_don_amt||'<>'||cdh.fr_fst_don_amt /*#*/end fr_fst_don_amt_diff
, case when icd.fr_lst_rfl_don_dt = cdh.fr_lst_rfl_don_dt then '' else icd.fr_lst_rfl_don_dt||'<>'||cdh.fr_lst_rfl_don_dt /*#*/end fr_lst_rfl_don_dt_diff
, case when icd.fr_lst_non_rfl_don_dt = cdh.fr_lst_non_rfl_don_dt then '' else icd.fr_lst_non_rfl_don_dt||'<>'||cdh.fr_lst_non_rfl_don_dt /*#*/end fr_lst_non_rfl_don_dt_diff
, case when icd.fr_lst_eml_don_amt = cdh.fr_lst_eml_don_amt then '' else icd.fr_lst_eml_don_amt||'<>'||cdh.fr_lst_eml_don_amt /*#*/end fr_lst_eml_don_amt_diff
, case when icd.fr_lst_eml_don_dt = cdh.fr_lst_eml_don_dt then '' else icd.fr_lst_eml_don_dt||'<>'||cdh.fr_lst_eml_don_dt /*#*/end fr_lst_eml_don_dt_diff
, case when icd.fr_coop_eligible_flg = cdh.fr_coop_eligible_flg then '' else icd.fr_coop_eligible_flg||'<>'||cdh.fr_coop_eligible_flg /*#*/end fr_coop_eligible_flg_diff
, case when icd.fr_lst_dm_don_amt = cdh.fr_lst_dm_don_amt then '' else icd.fr_lst_dm_don_amt||'<>'||cdh.fr_lst_dm_don_amt /*#*/end fr_lst_dm_don_amt_diff
, case when icd.fr_lst_dm_don_dt = cdh.fr_lst_dm_don_dt then '' else icd.fr_lst_dm_don_dt||'<>'||cdh.fr_lst_dm_don_dt /*#*/end fr_lst_dm_don_dt_diff
, case when icd.fr_lst_org_onl_don_amt = cdh.fr_lst_org_onl_don_amt then '' else icd.fr_lst_org_onl_don_amt||'<>'||cdh.fr_lst_org_onl_don_amt /*#*/end fr_lst_org_onl_don_amt_diff
, case when icd.fr_lst_org_onl_don_dt = cdh.fr_lst_org_onl_don_dt then '' else icd.fr_lst_org_onl_don_dt||'<>'||cdh.fr_lst_org_onl_don_dt /*#*/end fr_lst_org_onl_don_dt_diff
, case when icd.fr_lst_ecomm_don_amt = cdh.fr_lst_ecomm_don_amt then '' else icd.fr_lst_ecomm_don_amt||'<>'||cdh.fr_lst_ecomm_don_amt /*#*/end fr_lst_ecomm_don_amt_diff
, case when icd.fr_lst_ecomm_don_dt = cdh.fr_lst_ecomm_don_dt then '' else icd.fr_lst_ecomm_don_dt||'<>'||cdh.fr_lst_ecomm_don_dt /*#*/end fr_lst_ecomm_don_dt_diff

/*, '||                    ICD                    >>' as sep1/*, icd.* */*/
/*, '||                    CDH                    >>' as sep2/*, cdh.**/*/

from  icd full join cdh
on icd.icd_id = cdh.icd_id and icd.cdh_id = cdh.cdh_id
where icd.icd_id is not null
/*additional joins for PK*/
;
select 'MT_FR_SUMMARY' as icd_table_name, 'agg_fundraising_summary' as cdh_table_name
, sum( case when fr_avg_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_avg_don_amt
, sum( case when fr_avg_gross_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_avg_gross_don_amt
, sum( case when fr_lst_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_lst_don_amt
, sum( case when fr_ltd_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_ltd_don_amt
, sum( case when fr_max_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_max_don_amt, sum( case when fr_prior_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_prior_don_amt
, sum( case when fr_fst_don_dt_diff = '' then 0 else 1 end) as /*#*/fr_fst_don_dt
, sum( case when fr_lst_don_dt_diff = '' then 0 else 1 end) as /*#*/fr_lst_don_dt, sum( case when fr_prior_don_dt_diff = '' then 0 else 1 end) as /*#*/fr_prior_don_dt, sum( case when fr_dnr_act_flg_diff = '' then 0 else 1 end) as /*#*/fr_dnr_act_flg
, sum( case when fr_dnr_inact_flg_diff = '' then 0 else 1 end) as /*#*/fr_dnr_inact_flg
, sum( case when fr_dnr_lps_flg_diff = '' then 0 else 1 end) as /*#*/fr_dnr_lps_flg
, sum( case when fr_mbr_gvng_lvl_cd_diff = '' then 0 else 1 end) as /*#*/fr_mbr_gvng_lvl_cd
, sum( case when fr_mbr_exp_dt_diff = '' then 0 else 1 end) as /*#*/fr_mbr_exp_dt
, sum( case when fr_fst_act_keycode_diff = '' then 0 else 1 end) as /*#*/fr_fst_act_keycode
, sum( case when fr_lst_act_keycode_diff = '' then 0 else 1 end) as /*#*/fr_lst_act_keycode
, sum( case when fr_prior_act_keycode_diff = '' then 0 else 1 end) as /*#*/fr_prior_act_keycode, sum( case when fr_ltd_don_cnt_diff = '' then 0 else 1 end) as /*#*/fr_ltd_don_cnt
, sum( case when fr_don_ref_cnt_diff = '' then 0 else 1 end) as /*#*/fr_don_ref_cnt, sum( case when fr_times_ren_cnt_diff = '' then 0 else 1 end) as /*#*/fr_times_ren_cnt
, sum( case when fr_lst_don_src_cd_diff = '' then 0 else 1 end) as /*#*/fr_lst_don_src_cd, sum( case when fr_lst_rfl_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_lst_rfl_don_amt
, sum( case when fr_lst_non_rfl_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_lst_non_rfl_don_amt
, sum( case when fr_mbr_frst_strt_dt_diff = '' then 0 else 1 end) as /*#*/fr_mbr_frst_strt_dt
, sum( case when fr_mbr_lst_keycode_diff = '' then 0 else 1 end) as /*#*/fr_mbr_lst_keycode
, sum( case when fr_mbr_lst_ren_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_mbr_lst_ren_don_amt
, sum( case when fr_tof_cd_diff = '' then 0 else 1 end) as /*#*/fr_tof_cd
, sum( case when fr_lt_dnr_flg_diff = '' then 0 else 1 end) as /*#*/fr_lt_dnr_flg
, sum( case when fr_mbr_lst_add_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_mbr_lst_add_don_amt
, sum( case when fr_mbr_lst_add_don_dt_diff = '' then 0 else 1 end) as /*#*/fr_mbr_lst_add_don_dt
, sum( case when fr_mbr_pres_cir_frst_strt_dt_diff = '' then 0 else 1 end) as /*#*/fr_mbr_pres_cir_frst_strt_dt
, sum( case when fr_fst_don_keycode_diff = '' then 0 else 1 end) as /*#*/fr_fst_don_keycode
, sum( case when fr_max_don_amt_12_mth_diff = '' then 0 else 1 end) as /*#*/fr_max_don_amt_12_mth
, sum( case when fr_max_don_amt_24_mth_diff = '' then 0 else 1 end) as /*#*/fr_max_don_amt_24_mth
, sum( case when fr_max_don_amt_36_mth_diff = '' then 0 else 1 end) as /*#*/fr_max_don_amt_36_mth
, sum( case when fr_track_number_diff = '' then 0 else 1 end) as /*#*/fr_track_number
, sum( case when fr_conv_tag_rsp_flg_diff = '' then 0 else 1 end) as /*#*/fr_conv_tag_rsp_flg
, sum( case when fr_fst_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_fst_don_amt
, sum( case when fr_lst_rfl_don_dt_diff = '' then 0 else 1 end) as /*#*/fr_lst_rfl_don_dt
, sum( case when fr_lst_non_rfl_don_dt_diff = '' then 0 else 1 end) as /*#*/fr_lst_non_rfl_don_dt
, sum( case when fr_lst_eml_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_lst_eml_don_amt
, sum( case when fr_lst_eml_don_dt_diff = '' then 0 else 1 end) as /*#*/fr_lst_eml_don_dt
, sum( case when fr_coop_eligible_flg_diff = '' then 0 else 1 end) as /*#*/fr_coop_eligible_flg
, sum( case when fr_lst_dm_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_lst_dm_don_amt
, sum( case when fr_lst_dm_don_dt_diff = '' then 0 else 1 end) as /*#*/fr_lst_dm_don_dt
, sum( case when fr_lst_org_onl_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_lst_org_onl_don_amt
, sum( case when fr_lst_org_onl_don_dt_diff = '' then 0 else 1 end) as /*#*/fr_lst_org_onl_don_dt
, sum( case when fr_lst_ecomm_don_amt_diff = '' then 0 else 1 end) as /*#*/fr_lst_ecomm_don_amt
, sum( case when fr_lst_ecomm_don_dt_diff = '' then 0 else 1 end) as /*#*/fr_lst_ecomm_don_dt
from cr_temp.comp_diff_MT_FR_SUMMARY
;
select top 1000 match_fl, icd_id, cdh_id 

,  fr_avg_don_amt_diff
,  fr_avg_gross_don_amt_diff
,  fr_lst_don_amt_diff
,  fr_ltd_don_amt_diff
,  fr_max_don_amt_diff
,  fr_prior_don_amt_diff
,  fr_fst_don_dt_diff
,  fr_lst_don_dt_diff
,  fr_prior_don_dt_diff
,  fr_dnr_act_flg_diff
,  fr_dnr_inact_flg_diff
,  fr_dnr_lps_flg_diff
,  fr_mbr_gvng_lvl_cd_diff
,  fr_mbr_exp_dt_diff
,  fr_fst_act_keycode_diff
,  fr_lst_act_keycode_diff
,  fr_prior_act_keycode_diff
,  fr_ltd_don_cnt_diff
,  fr_don_ref_cnt_diff
,  fr_times_ren_cnt_diff
,  fr_lst_don_src_cd_diff
,  fr_lst_rfl_don_amt_diff
,  fr_lst_non_rfl_don_amt_diff
,  fr_mbr_frst_strt_dt_diff
,  fr_mbr_lst_keycode_diff
,  fr_mbr_lst_ren_don_amt_diff
,  fr_tof_cd_diff
,  fr_lt_dnr_flg_diff
,  fr_mbr_lst_add_don_amt_diff
,  fr_mbr_lst_add_don_dt_diff
,  fr_mbr_pres_cir_frst_strt_dt_diff
,  fr_fst_don_keycode_diff
,  fr_max_don_amt_12_mth_diff
,  fr_max_don_amt_24_mth_diff
,  fr_max_don_amt_36_mth_diff
,  fr_track_number_diff
,  fr_conv_tag_rsp_flg_diff
,  fr_fst_don_amt_diff
,  fr_lst_rfl_don_dt_diff
,  fr_lst_non_rfl_don_dt_diff
,  fr_lst_eml_don_amt_diff
,  fr_lst_eml_don_dt_diff
,  fr_coop_eligible_flg_diff
,  fr_lst_dm_don_amt_diff
,  fr_lst_dm_don_dt_diff
,  fr_lst_org_onl_don_amt_diff
,  fr_lst_org_onl_don_dt_diff
,  fr_lst_ecomm_don_amt_diff
,  fr_lst_ecomm_don_dt_diff
from cr_temp.comp_diff_MT_FR_SUMMARY
 where 1=1 and ( 1=2
-- or fr_avg_don_amt_diff != ''			--amt_diff due don_ind other than 'C3'
-- or fr_avg_gross_don_amt_diff != ''   --amt_diff due don_ind other than 'C3'
-- or fr_lst_don_amt_diff != ''			--amt_diff due don_ind other than 'C3'
-- or fr_ltd_don_amt_diff != ''			--amt_diff due don_ind other than 'C3'
-- or fr_max_don_amt_diff != ''			--amt_diff due don_ind other than 'C3'
-- or fr_prior_don_amt_diff != ''		--amt_diff due don_ind other than 'C3'
 or fr_fst_don_dt_diff != ''
 or fr_lst_don_dt_diff != ''
-- or fr_prior_don_dt_diff != ''
-- or fr_dnr_act_flg_diff != ''
-- or fr_dnr_inact_flg_diff != ''
-- or fr_dnr_lps_flg_diff != ''
-- or fr_mbr_gvng_lvl_cd_diff != ''
-- or fr_mbr_exp_dt_diff != ''
-- or fr_fst_act_keycode_diff != ''
-- or fr_lst_act_keycode_diff != ''
-- or fr_prior_act_keycode_diff != ''
-- or fr_ltd_don_cnt_diff != ''
-- or fr_don_ref_cnt_diff != ''
-- or fr_times_ren_cnt_diff != ''
-- or fr_lst_don_src_cd_diff != ''
-- or fr_lst_rfl_don_amt_diff != ''
-- or fr_lst_non_rfl_don_amt_diff != ''
-- or fr_mbr_frst_strt_dt_diff != ''
-- or fr_mbr_lst_keycode_diff != ''
-- or fr_mbr_lst_ren_don_amt_diff != ''
-- or fr_tof_cd_diff != ''
-- or fr_lt_dnr_flg_diff != ''
-- or fr_mbr_lst_add_don_amt_diff != ''
-- or fr_mbr_lst_add_don_dt_diff != ''
-- or fr_mbr_pres_cir_frst_strt_dt_diff != ''
-- or fr_fst_don_keycode_diff != ''
-- or fr_max_don_amt_12_mth_diff != ''
-- or fr_max_don_amt_24_mth_diff != ''
-- or fr_max_don_amt_36_mth_diff != ''
-- or fr_track_number_diff != ''
-- or fr_conv_tag_rsp_flg_diff != ''
-- or fr_fst_don_amt_diff != ''
-- or fr_lst_rfl_don_dt_diff != ''
-- or fr_lst_non_rfl_don_dt_diff != ''
-- or fr_lst_eml_don_amt_diff != ''
-- or fr_lst_eml_don_dt_diff != ''
-- or fr_coop_eligible_flg_diff != ''
-- or fr_lst_dm_don_amt_diff != ''
-- or fr_lst_dm_don_dt_diff != ''
-- or fr_lst_org_onl_don_amt_diff != ''
-- or fr_lst_org_onl_don_dt_diff != ''
-- or fr_lst_ecomm_don_amt_diff != ''
-- or fr_lst_ecomm_don_dt_diff != ''
)

;
------------------------------------------------------------------------RUN ALL ABOVE


;
select * from   cr_temp.GTT_MT_FR_SUMMARY  where icd_id = 00000000000000000000
;
select * from   cr_temp.gtt_agg_fundraising_summary  where cdh_id = 00000000000000000000









