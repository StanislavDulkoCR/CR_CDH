;
drop table if exists  cr_temp.GTT_MT_FR_SUMMARY;
create table cr_temp.GTT_MT_FR_SUMMARY as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
, NVL2(trunc(nvl(fr_avg_don_amt,'0')), trunc(nvl(fr_avg_don_amt,'0'))::text,'(NULL)') as fr_avg_don_amt
, NVL2(trunc(nvl(fr_avg_gross_don_amt,'0')), trunc(nvl(fr_avg_gross_don_amt,'0'))::text,'(NULL)') as fr_avg_gross_don_amt
, NVL2(trunc(nvl(fr_lst_don_amt,'0')), trunc(nvl(fr_lst_don_amt,'0'))::text,'(NULL)') as fr_lst_don_amt
, NVL2(trunc(nvl(fr_ltd_don_amt,'0')), trunc(nvl(fr_ltd_don_amt,'0'))::text,'(NULL)') as fr_ltd_don_amt
, NVL2(trunc(nvl(fr_max_don_amt,'0')), trunc(nvl(fr_max_don_amt,'0'))::text,'(NULL)') as fr_max_don_amt
, NVL2(trunc(nvl(fr_1_6_mth_don_amt,'0')), trunc(nvl(fr_1_6_mth_don_amt,'0'))::text,'(NULL)') as fr_1_6_mth_don_amt
, NVL2(trunc(nvl(fr_13_24_mth_don_amt,'0')), trunc(nvl(fr_13_24_mth_don_amt,'0'))::text,'(NULL)') as fr_13_24_mth_don_amt
, NVL2(trunc(nvl(fr_25_36_mth_don_amt,'0')), trunc(nvl(fr_25_36_mth_don_amt,'0'))::text,'(NULL)') as fr_25_36_mth_don_amt
, NVL2(trunc(nvl(fr_37_plus_mth_don_amt,'0')), trunc(nvl(fr_37_plus_mth_don_amt,'0'))::text,'(NULL)') as fr_37_plus_mth_don_amt
, NVL2(trunc(nvl(fr_7_12_mth_don_amt,'0')), trunc(nvl(fr_7_12_mth_don_amt,'0'))::text,'(NULL)') as fr_7_12_mth_don_amt
, NVL2(trunc(nvl(fr_prior_don_amt,'0')), trunc(nvl(fr_prior_don_amt,'0'))::text,'(NULL)') as fr_prior_don_amt
, NVL2(TO_CHAR(fr_fst_don_dt,'DD-MON-YY'), TO_CHAR(fr_fst_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_fst_don_dt
, NVL2(TO_CHAR(fr_lst_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_don_dt
, NVL2(TO_CHAR(fr_prior_don_dt,'DD-MON-YY'), TO_CHAR(fr_prior_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_prior_don_dt
, NVL2(fr_dnr_act_flg, fr_dnr_act_flg::text,'(NULL)') as fr_dnr_act_flg
, NVL2(fr_dnr_inact_flg, fr_dnr_inact_flg::text,'(NULL)') as fr_dnr_inact_flg
, NVL2(fr_dnr_lps_flg, fr_dnr_lps_flg::text,'(NULL)') as fr_dnr_lps_flg
, NVL2(fr_mbr_gvng_lvl_cd, fr_mbr_gvng_lvl_cd::text,'(NULL)') as fr_mbr_gvng_lvl_cd
, NVL2(TO_CHAR(fr_mbr_exp_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_exp_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_exp_dt
, NVL2(fr_fst_act_keycode, fr_fst_act_keycode::text,'(NULL)') as fr_fst_act_keycode
, NVL2(fr_lst_act_keycode, fr_lst_act_keycode::text,'(NULL)') as fr_lst_act_keycode
, NVL2(fr_prior_act_keycode, fr_prior_act_keycode::text,'(NULL)') as fr_prior_act_keycode
, NVL2(trunc(nvl(fr_ltd_don_cnt,'0')), trunc(nvl(fr_ltd_don_cnt,'0'))::text,'(NULL)') as fr_ltd_don_cnt
, NVL2(trunc(nvl(fr_don_ref_cnt,'0')), trunc(nvl(fr_don_ref_cnt,'0'))::text,'(NULL)') as fr_don_ref_cnt
, NVL2(trunc(nvl(fr_times_ren_cnt,'0')), trunc(nvl(fr_times_ren_cnt,'0'))::text,'(NULL)') as fr_times_ren_cnt
, NVL2(fr_lst_don_src_cd, fr_lst_don_src_cd::text,'(NULL)') as fr_lst_don_src_cd
, NVL2(trunc(nvl(fr_lst_rfl_don_amt,'0')), trunc(nvl(fr_lst_rfl_don_amt,'0'))::text,'(NULL)') as fr_lst_rfl_don_amt
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
, NVL2(trunc(nvl(fr_1_6_mth_don_cnt,'0')), trunc(nvl(fr_1_6_mth_don_cnt,'0'))::text,'(NULL)') as fr_1_6_mth_don_cnt
, NVL2(trunc(nvl(fr_7_12_mth_don_cnt,'0')), trunc(nvl(fr_7_12_mth_don_cnt,'0'))::text,'(NULL)') as fr_7_12_mth_don_cnt
, NVL2(trunc(nvl(fr_13_24_mth_don_cnt,'0')), trunc(nvl(fr_13_24_mth_don_cnt,'0'))::text,'(NULL)') as fr_13_24_mth_don_cnt
, NVL2(trunc(nvl(fr_25_36_mth_don_cnt,'0')), trunc(nvl(fr_25_36_mth_don_cnt,'0'))::text,'(NULL)') as fr_25_36_mth_don_cnt
, NVL2(trunc(nvl(fr_37_plus_mth_don_cnt,'0')), trunc(nvl(fr_37_plus_mth_don_cnt,'0'))::text,'(NULL)') as fr_37_plus_mth_don_cnt
, NVL2(fr_dm_pros_mdl_rsp_flg, fr_dm_pros_mdl_rsp_flg::text,'(NULL)') as fr_dm_pros_mdl_rsp_flg
, NVL2(fr_mbr_comb_level, fr_mbr_comb_level::text,'(NULL)') as fr_mbr_comb_level
, NVL2(TO_CHAR(fr_mbr_comb_exp_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_comb_exp_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_comb_exp_dt
, NVL2(TO_CHAR(fr_mbr_basic_fst_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_basic_fst_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_basic_fst_dt
, NVL2(trunc(nvl(fr_mbr_basic_fst_amt,'0')), trunc(nvl(fr_mbr_basic_fst_amt,'0'))::text,'(NULL)') as fr_mbr_basic_fst_amt
, NVL2(TO_CHAR(fr_mbr_basic_lst_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_basic_lst_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_basic_lst_dt
, NVL2(trunc(nvl(fr_mbr_basic_lst_amt,'0')), trunc(nvl(fr_mbr_basic_lst_amt,'0'))::text,'(NULL)') as fr_mbr_basic_lst_amt
, NVL2(fr_ch_status, fr_ch_status::text,'(NULL)') as fr_ch_status
, NVL2(trunc(nvl(fr_ch_curr_ttl_don_amt,'0')), trunc(nvl(fr_ch_curr_ttl_don_amt,'0'))::text,'(NULL)') as fr_ch_curr_ttl_don_amt
, NVL2(TO_CHAR(fr_ch_lst_don_dt,'DD-MON-YY'), TO_CHAR(fr_ch_lst_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_ch_lst_don_dt
, NVL2(TO_CHAR(fr_ch_curr_strt_dt,'DD-MON-YY'), TO_CHAR(fr_ch_curr_strt_dt,'DD-MON-YY')::text,'(NULL)') as fr_ch_curr_strt_dt
, NVL2(trunc(nvl(fr_ch_lst_don_amt,'0')), trunc(nvl(fr_ch_lst_don_amt,'0'))::text,'(NULL)') as fr_ch_lst_don_amt
, NVL2(trunc(nvl(fr_ch_curr_don_cnt,'0')), trunc(nvl(fr_ch_curr_don_cnt,'0'))::text,'(NULL)') as fr_ch_curr_don_cnt
, NVL2(fr_ch_lst_don_keycd, fr_ch_lst_don_keycd::text,'(NULL)') as fr_ch_lst_don_keycd
, NVL2(TO_CHAR(fr_ch_status_active_dt,'DD-MON-YY'), TO_CHAR(fr_ch_status_active_dt,'DD-MON-YY')::text,'(NULL)') as fr_ch_status_active_dt
, NVL2(TO_CHAR(fr_mbr_basic_exp_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_basic_exp_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_basic_exp_dt
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
, NVL2(trunc(nvl(fr_max_don_amt,'0')), trunc(nvl(fr_max_don_amt,'0'))::text,'(NULL)') as fr_max_don_amt
, NVL2(trunc(nvl(fr_1_6_mth_don_amt,'0')), trunc(nvl(fr_1_6_mth_don_amt,'0'))::text,'(NULL)') as fr_1_6_mth_don_amt
, NVL2(trunc(nvl(fr_13_24_mth_don_amt,'0')), trunc(nvl(fr_13_24_mth_don_amt,'0'))::text,'(NULL)') as fr_13_24_mth_don_amt
, NVL2(trunc(nvl(fr_25_36_mth_don_amt,'0')), trunc(nvl(fr_25_36_mth_don_amt,'0'))::text,'(NULL)') as fr_25_36_mth_don_amt
, NVL2(trunc(nvl(fr_37_plus_mth_don_amt,'0')), trunc(nvl(fr_37_plus_mth_don_amt,'0'))::text,'(NULL)') as fr_37_plus_mth_don_amt
, NVL2(trunc(nvl(fr_7_12_mth_don_amt,'0')), trunc(nvl(fr_7_12_mth_don_amt,'0'))::text,'(NULL)') as fr_7_12_mth_don_amt
, NVL2(trunc(nvl(fr_prior_don_amt,'0')), trunc(nvl(fr_prior_don_amt,'0'))::text,'(NULL)') as fr_prior_don_amt
, NVL2(TO_CHAR(fr_fst_don_dt,'DD-MON-YY'), TO_CHAR(fr_fst_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_fst_don_dt
, NVL2(TO_CHAR(fr_lst_don_dt,'DD-MON-YY'), TO_CHAR(fr_lst_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_don_dt
, NVL2(TO_CHAR(fr_prior_don_dt,'DD-MON-YY'), TO_CHAR(fr_prior_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_prior_don_dt
, NVL2(fr_dnr_act_flg, fr_dnr_act_flg::text,'(NULL)') as fr_dnr_act_flg
, NVL2(fr_dnr_inact_flg, fr_dnr_inact_flg::text,'(NULL)') as fr_dnr_inact_flg
, NVL2(fr_dnr_lps_flg, fr_dnr_lps_flg::text,'(NULL)') as fr_dnr_lps_flg
, NVL2(fr_mbr_gvng_lvl_cd, fr_mbr_gvng_lvl_cd::text,'(NULL)') as fr_mbr_gvng_lvl_cd
, NVL2(TO_CHAR(fr_mbr_exp_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_exp_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_exp_dt
, NVL2(fr_fst_act_keycode, fr_fst_act_keycode::text,'(NULL)') as fr_fst_act_keycode
, NVL2(fr_lst_act_keycode, fr_lst_act_keycode::text,'(NULL)') as fr_lst_act_keycode
, NVL2(fr_prior_act_keycode, fr_prior_act_keycode::text,'(NULL)') as fr_prior_act_keycode
, NVL2(trunc(nvl(fr_ltd_don_cnt,'0')), trunc(nvl(fr_ltd_don_cnt,'0'))::text,'(NULL)') as fr_ltd_don_cnt
, NVL2(trunc(nvl(fr_don_ref_cnt,'0')), trunc(nvl(fr_don_ref_cnt,'0'))::text,'(NULL)') as fr_don_ref_cnt
, NVL2(trunc(nvl(fr_times_ren_cnt,'0')), trunc(nvl(fr_times_ren_cnt,'0'))::text,'(NULL)') as fr_times_ren_cnt
, NVL2(fr_lst_don_src_cd, fr_lst_don_src_cd::text,'(NULL)') as fr_lst_don_src_cd
, NVL2(trunc(nvl(fr_lst_rfl_don_amt,'0')), trunc(nvl(fr_lst_rfl_don_amt,'0'))::text,'(NULL)') as fr_lst_rfl_don_amt
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
, NVL2(trunc(nvl(fr_1_6_mth_don_cnt,'0')), trunc(nvl(fr_1_6_mth_don_cnt,'0'))::text,'(NULL)') as fr_1_6_mth_don_cnt
, NVL2(trunc(nvl(fr_7_12_mth_don_cnt,'0')), trunc(nvl(fr_7_12_mth_don_cnt,'0'))::text,'(NULL)') as fr_7_12_mth_don_cnt
, NVL2(trunc(nvl(fr_13_24_mth_don_cnt,'0')), trunc(nvl(fr_13_24_mth_don_cnt,'0'))::text,'(NULL)') as fr_13_24_mth_don_cnt
, NVL2(trunc(nvl(fr_25_36_mth_don_cnt,'0')), trunc(nvl(fr_25_36_mth_don_cnt,'0'))::text,'(NULL)') as fr_25_36_mth_don_cnt
, NVL2(trunc(nvl(fr_37_plus_mth_don_cnt,'0')), trunc(nvl(fr_37_plus_mth_don_cnt,'0'))::text,'(NULL)') as fr_37_plus_mth_don_cnt
, NVL2(fr_dm_pros_mdl_rsp_flg, fr_dm_pros_mdl_rsp_flg::text,'(NULL)') as fr_dm_pros_mdl_rsp_flg
, NVL2(fr_mbr_comb_level, fr_mbr_comb_level::text,'(NULL)') as fr_mbr_comb_level
, NVL2(TO_CHAR(fr_mbr_comb_exp_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_comb_exp_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_comb_exp_dt
, NVL2(TO_CHAR(fr_mbr_basic_fst_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_basic_fst_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_basic_fst_dt
, NVL2(trunc(nvl(fr_mbr_basic_fst_amt,'0')), trunc(nvl(fr_mbr_basic_fst_amt,'0'))::text,'(NULL)') as fr_mbr_basic_fst_amt
, NVL2(TO_CHAR(fr_mbr_basic_lst_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_basic_lst_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_basic_lst_dt
, NVL2(trunc(nvl(fr_mbr_basic_lst_amt,'0')), trunc(nvl(fr_mbr_basic_lst_amt,'0'))::text,'(NULL)') as fr_mbr_basic_lst_amt
, NVL2(fr_ch_status, fr_ch_status::text,'(NULL)') as fr_ch_status
, NVL2(trunc(nvl(fr_ch_curr_ttl_don_amt,'0')), trunc(nvl(fr_ch_curr_ttl_don_amt,'0'))::text,'(NULL)') as fr_ch_curr_ttl_don_amt
, NVL2(TO_CHAR(fr_ch_lst_don_dt,'DD-MON-YY'), TO_CHAR(fr_ch_lst_don_dt,'DD-MON-YY')::text,'(NULL)') as fr_ch_lst_don_dt
, NVL2(TO_CHAR(fr_ch_curr_strt_dt,'DD-MON-YY'), TO_CHAR(fr_ch_curr_strt_dt,'DD-MON-YY')::text,'(NULL)') as fr_ch_curr_strt_dt
, NVL2(trunc(nvl(fr_ch_lst_don_amt,'0')), trunc(nvl(fr_ch_lst_don_amt,'0'))::text,'(NULL)') as fr_ch_lst_don_amt
, NVL2(trunc(nvl(fr_ch_curr_don_cnt,'0')), trunc(nvl(fr_ch_curr_don_cnt,'0'))::text,'(NULL)') as fr_ch_curr_don_cnt
, NVL2(fr_ch_lst_don_keycd, fr_ch_lst_don_keycd::text,'(NULL)') as fr_ch_lst_don_keycd
, NVL2(TO_CHAR(fr_ch_status_active_dt,'DD-MON-YY'), TO_CHAR(fr_ch_status_active_dt,'DD-MON-YY')::text,'(NULL)') as fr_ch_status_active_dt
, NVL2(TO_CHAR(fr_mbr_basic_exp_dt,'DD-MON-YY'), TO_CHAR(fr_mbr_basic_exp_dt,'DD-MON-YY')::text,'(NULL)') as fr_mbr_basic_exp_dt
/*---END---*/
from prod.agg_fundraising_summary T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;



------------------------------------------------------------------------TABLE CREATION END\												
												
;
drop table if exists  cr_temp.diff_mt_fr_summary;
CREATE TABLE cr_temp.diff_mt_fr_summary AS
with hash_diff as (

select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID     , fr_avg_don_amt, fr_avg_gross_don_amt, fr_lst_don_amt, fr_ltd_don_amt, fr_max_don_amt, fr_1_6_mth_don_amt, fr_13_24_mth_don_amt, fr_25_36_mth_don_amt, fr_37_plus_mth_don_amt, fr_7_12_mth_don_amt, fr_prior_don_amt, fr_fst_don_dt, fr_lst_don_dt, fr_prior_don_dt, fr_dnr_act_flg, fr_dnr_inact_flg, fr_dnr_lps_flg, fr_mbr_gvng_lvl_cd, fr_mbr_exp_dt, fr_fst_act_keycode, fr_lst_act_keycode, fr_prior_act_keycode, fr_ltd_don_cnt, fr_don_ref_cnt, fr_times_ren_cnt, fr_lst_don_src_cd, fr_lst_rfl_don_amt, fr_lst_non_rfl_don_amt, fr_mbr_frst_strt_dt, fr_mbr_lst_keycode, fr_mbr_lst_ren_don_amt, fr_tof_cd, fr_lt_dnr_flg, fr_mbr_lst_add_don_amt, fr_mbr_lst_add_don_dt, fr_mbr_pres_cir_frst_strt_dt, fr_fst_don_keycode, fr_max_don_amt_12_mth, fr_max_don_amt_24_mth, fr_max_don_amt_36_mth, fr_track_number, fr_conv_tag_rsp_flg, fr_fst_don_amt, fr_lst_rfl_don_dt, fr_lst_non_rfl_don_dt, fr_lst_eml_don_amt, fr_lst_eml_don_dt, fr_coop_eligible_flg, fr_lst_dm_don_amt, fr_lst_dm_don_dt, fr_lst_org_onl_don_amt, fr_lst_org_onl_don_dt, fr_lst_ecomm_don_amt, fr_lst_ecomm_don_dt, fr_1_6_mth_don_cnt, fr_7_12_mth_don_cnt, fr_13_24_mth_don_cnt, fr_25_36_mth_don_cnt, fr_37_plus_mth_don_cnt, fr_dm_pros_mdl_rsp_flg, fr_mbr_comb_level, fr_mbr_comb_exp_dt, fr_mbr_basic_fst_dt, fr_mbr_basic_fst_amt, fr_mbr_basic_lst_dt, fr_mbr_basic_lst_amt, fr_ch_status, fr_ch_curr_ttl_don_amt, fr_ch_lst_don_dt, fr_ch_curr_strt_dt, fr_ch_lst_don_amt, fr_ch_curr_don_cnt, fr_ch_lst_don_keycd, fr_ch_status_active_dt, fr_mbr_basic_exp_dt
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL , fr_avg_don_amt, fr_avg_gross_don_amt, fr_lst_don_amt, fr_ltd_don_amt, fr_max_don_amt, fr_1_6_mth_don_amt, fr_13_24_mth_don_amt, fr_25_36_mth_don_amt, fr_37_plus_mth_don_amt, fr_7_12_mth_don_amt, fr_prior_don_amt, fr_fst_don_dt, fr_lst_don_dt, fr_prior_don_dt, fr_dnr_act_flg, fr_dnr_inact_flg, fr_dnr_lps_flg, fr_mbr_gvng_lvl_cd, fr_mbr_exp_dt, fr_fst_act_keycode, fr_lst_act_keycode, fr_prior_act_keycode, fr_ltd_don_cnt, fr_don_ref_cnt, fr_times_ren_cnt, fr_lst_don_src_cd, fr_lst_rfl_don_amt, fr_lst_non_rfl_don_amt, fr_mbr_frst_strt_dt, fr_mbr_lst_keycode, fr_mbr_lst_ren_don_amt, fr_tof_cd, fr_lt_dnr_flg, fr_mbr_lst_add_don_amt, fr_mbr_lst_add_don_dt, fr_mbr_pres_cir_frst_strt_dt, fr_fst_don_keycode, fr_max_don_amt_12_mth, fr_max_don_amt_24_mth, fr_max_don_amt_36_mth, fr_track_number, fr_conv_tag_rsp_flg, fr_fst_don_amt, fr_lst_rfl_don_dt, fr_lst_non_rfl_don_dt, fr_lst_eml_don_amt, fr_lst_eml_don_dt, fr_coop_eligible_flg, fr_lst_dm_don_amt, fr_lst_dm_don_dt, fr_lst_org_onl_don_amt, fr_lst_org_onl_don_dt, fr_lst_ecomm_don_amt, fr_lst_ecomm_don_dt, fr_1_6_mth_don_cnt, fr_7_12_mth_don_cnt, fr_13_24_mth_don_cnt, fr_25_36_mth_don_cnt, fr_37_plus_mth_don_cnt, fr_dm_pros_mdl_rsp_flg, fr_mbr_comb_level, fr_mbr_comb_exp_dt, fr_mbr_basic_fst_dt, fr_mbr_basic_fst_amt, fr_mbr_basic_lst_dt, fr_mbr_basic_lst_amt, fr_ch_status, fr_ch_curr_ttl_don_amt, fr_ch_lst_don_dt, fr_ch_curr_strt_dt, fr_ch_lst_don_amt, fr_ch_curr_don_cnt, fr_ch_lst_don_keycd, fr_ch_status_active_dt, fr_mbr_basic_exp_dt
from cr_temp.GTT_MT_FR_SUMMARY
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL , fr_avg_don_amt, fr_avg_gross_don_amt, fr_lst_don_amt, fr_ltd_don_amt, fr_max_don_amt, fr_1_6_mth_don_amt, fr_13_24_mth_don_amt, fr_25_36_mth_don_amt, fr_37_plus_mth_don_amt, fr_7_12_mth_don_amt, fr_prior_don_amt, fr_fst_don_dt, fr_lst_don_dt, fr_prior_don_dt, fr_dnr_act_flg, fr_dnr_inact_flg, fr_dnr_lps_flg, fr_mbr_gvng_lvl_cd, fr_mbr_exp_dt, fr_fst_act_keycode, fr_lst_act_keycode, fr_prior_act_keycode, fr_ltd_don_cnt, fr_don_ref_cnt, fr_times_ren_cnt, fr_lst_don_src_cd, fr_lst_rfl_don_amt, fr_lst_non_rfl_don_amt, fr_mbr_frst_strt_dt, fr_mbr_lst_keycode, fr_mbr_lst_ren_don_amt, fr_tof_cd, fr_lt_dnr_flg, fr_mbr_lst_add_don_amt, fr_mbr_lst_add_don_dt, fr_mbr_pres_cir_frst_strt_dt, fr_fst_don_keycode, fr_max_don_amt_12_mth, fr_max_don_amt_24_mth, fr_max_don_amt_36_mth, fr_track_number, fr_conv_tag_rsp_flg, fr_fst_don_amt, fr_lst_rfl_don_dt, fr_lst_non_rfl_don_dt, fr_lst_eml_don_amt, fr_lst_eml_don_dt, fr_coop_eligible_flg, fr_lst_dm_don_amt, fr_lst_dm_don_dt, fr_lst_org_onl_don_amt, fr_lst_org_onl_don_dt, fr_lst_ecomm_don_amt, fr_lst_ecomm_don_dt, fr_1_6_mth_don_cnt, fr_7_12_mth_don_cnt, fr_13_24_mth_don_cnt, fr_25_36_mth_don_cnt, fr_37_plus_mth_don_cnt, fr_dm_pros_mdl_rsp_flg, fr_mbr_comb_level, fr_mbr_comb_exp_dt, fr_mbr_basic_fst_dt, fr_mbr_basic_fst_amt, fr_mbr_basic_lst_dt, fr_mbr_basic_lst_amt, fr_ch_status, fr_ch_curr_ttl_don_amt, fr_ch_lst_don_dt, fr_ch_curr_strt_dt, fr_ch_lst_don_amt, fr_ch_curr_don_cnt, fr_ch_lst_don_keycd, fr_ch_status_active_dt, fr_mbr_basic_exp_dt
from cr_temp.gtt_agg_fundraising_summary
)

group by ICD_ID, CDH_ID                                                              , fr_avg_don_amt, fr_avg_gross_don_amt, fr_lst_don_amt, fr_ltd_don_amt, fr_max_don_amt, fr_1_6_mth_don_amt, fr_13_24_mth_don_amt, fr_25_36_mth_don_amt, fr_37_plus_mth_don_amt, fr_7_12_mth_don_amt, fr_prior_don_amt, fr_fst_don_dt, fr_lst_don_dt, fr_prior_don_dt, fr_dnr_act_flg, fr_dnr_inact_flg, fr_dnr_lps_flg, fr_mbr_gvng_lvl_cd, fr_mbr_exp_dt, fr_fst_act_keycode, fr_lst_act_keycode, fr_prior_act_keycode, fr_ltd_don_cnt, fr_don_ref_cnt, fr_times_ren_cnt, fr_lst_don_src_cd, fr_lst_rfl_don_amt, fr_lst_non_rfl_don_amt, fr_mbr_frst_strt_dt, fr_mbr_lst_keycode, fr_mbr_lst_ren_don_amt, fr_tof_cd, fr_lt_dnr_flg, fr_mbr_lst_add_don_amt, fr_mbr_lst_add_don_dt, fr_mbr_pres_cir_frst_strt_dt, fr_fst_don_keycode, fr_max_don_amt_12_mth, fr_max_don_amt_24_mth, fr_max_don_amt_36_mth, fr_track_number, fr_conv_tag_rsp_flg, fr_fst_don_amt, fr_lst_rfl_don_dt, fr_lst_non_rfl_don_dt, fr_lst_eml_don_amt, fr_lst_eml_don_dt, fr_coop_eligible_flg, fr_lst_dm_don_amt, fr_lst_dm_don_dt, fr_lst_org_onl_don_amt, fr_lst_org_onl_don_dt, fr_lst_ecomm_don_amt, fr_lst_ecomm_don_dt, fr_1_6_mth_don_cnt, fr_7_12_mth_don_cnt, fr_13_24_mth_don_cnt, fr_25_36_mth_don_cnt, fr_37_plus_mth_don_cnt, fr_dm_pros_mdl_rsp_flg, fr_mbr_comb_level, fr_mbr_comb_exp_dt, fr_mbr_basic_fst_dt, fr_mbr_basic_fst_amt, fr_mbr_basic_lst_dt, fr_mbr_basic_lst_amt, fr_ch_status, fr_ch_curr_ttl_don_amt, fr_ch_lst_don_dt, fr_ch_curr_strt_dt, fr_ch_lst_don_amt, fr_ch_curr_don_cnt, fr_ch_lst_don_keycd, fr_ch_status_active_dt, fr_mbr_basic_exp_dt
having COUNT(ICD_FL)<>COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select * from hash_diff hd
-- where exists (select null from xxxxxxxxxxxxxxxxxxxxxxxx fullmatch where hd.cdh_id = fullmatch.cdh_id)
;
select to_char(count(distinct icd_id),'FM999,999,999,990') as unique_icd_id, to_char(count(distinct cdh_id),'FM999,999,999,990') as unique_cdh_id
, to_char(count(*),'FM999,999,999,990') as rows_total_count, to_char(count(*)/2,'FM999,999,999,990') as rowshalf_total_count
, to_char((select count(*) from cr_temp.GTT_MT_FR_SUMMARY),'FM999,999,999,990') as total_ICD
, to_char((select count(*) from cr_temp.gtt_agg_fundraising_summary),'FM999,999,999,990')  as total_CDH 
from cr_temp.diff_mt_fr_summary

;
drop table if exists cr_temp.comp_diff_mt_fr_summary;
CREATE TABLE cr_temp.comp_diff_mt_fr_summary AS
with icd as (select * from cr_temp.diff_mt_fr_summary where 1=1 and cnt_icd_fl = 1)
,    cdh as (select * from cr_temp.diff_mt_fr_summary where 1=1 and cnt_cdh_fl = 1)
select icd.cnt_icd_fl||cdh.cnt_cdh_fl as match_fl, nvl(icd.icd_id,cdh.icd_id) as icd_id ,cdh.cdh_id
, case when icd.fr_avg_don_amt = cdh.fr_avg_don_amt then '' else icd.fr_avg_don_amt||'<>'||cdh.fr_avg_don_amt /*=*/end fr_avg_don_amt
, case when icd.fr_avg_gross_don_amt = cdh.fr_avg_gross_don_amt then '' else icd.fr_avg_gross_don_amt||'<>'||cdh.fr_avg_gross_don_amt /*=*/end fr_avg_gross_don_amt
, case when icd.fr_lst_don_amt = cdh.fr_lst_don_amt then '' else icd.fr_lst_don_amt||'<>'||cdh.fr_lst_don_amt /*=*/end fr_lst_don_amt
, case when icd.fr_ltd_don_amt = cdh.fr_ltd_don_amt then '' else icd.fr_ltd_don_amt||'<>'||cdh.fr_ltd_don_amt /*=*/end fr_ltd_don_amt
, case when icd.fr_max_don_amt = cdh.fr_max_don_amt then '' else icd.fr_max_don_amt||'<>'||cdh.fr_max_don_amt /*=*/end fr_max_don_amt
, case when icd.fr_1_6_mth_don_amt = cdh.fr_1_6_mth_don_amt then '' else icd.fr_1_6_mth_don_amt||'<>'||cdh.fr_1_6_mth_don_amt /*=*/end fr_1_6_mth_don_amt
, case when icd.fr_13_24_mth_don_amt = cdh.fr_13_24_mth_don_amt then '' else icd.fr_13_24_mth_don_amt||'<>'||cdh.fr_13_24_mth_don_amt /*=*/end fr_13_24_mth_don_amt
, case when icd.fr_25_36_mth_don_amt = cdh.fr_25_36_mth_don_amt then '' else icd.fr_25_36_mth_don_amt||'<>'||cdh.fr_25_36_mth_don_amt /*=*/end fr_25_36_mth_don_amt
, case when icd.fr_37_plus_mth_don_amt = cdh.fr_37_plus_mth_don_amt then '' else icd.fr_37_plus_mth_don_amt||'<>'||cdh.fr_37_plus_mth_don_amt /*=*/end fr_37_plus_mth_don_amt
, case when icd.fr_7_12_mth_don_amt = cdh.fr_7_12_mth_don_amt then '' else icd.fr_7_12_mth_don_amt||'<>'||cdh.fr_7_12_mth_don_amt /*=*/end fr_7_12_mth_don_amt
, case when icd.fr_prior_don_amt = cdh.fr_prior_don_amt then '' else icd.fr_prior_don_amt||'<>'||cdh.fr_prior_don_amt /*=*/end fr_prior_don_amt
, case when icd.fr_fst_don_dt = cdh.fr_fst_don_dt then '' else icd.fr_fst_don_dt||'<>'||cdh.fr_fst_don_dt /*=*/end fr_fst_don_dt
, case when icd.fr_lst_don_dt = cdh.fr_lst_don_dt then '' else icd.fr_lst_don_dt||'<>'||cdh.fr_lst_don_dt /*=*/end fr_lst_don_dt
, case when icd.fr_prior_don_dt = cdh.fr_prior_don_dt then '' else icd.fr_prior_don_dt||'<>'||cdh.fr_prior_don_dt /*=*/end fr_prior_don_dt
, case when icd.fr_dnr_act_flg = cdh.fr_dnr_act_flg then '' else icd.fr_dnr_act_flg||'<>'||cdh.fr_dnr_act_flg /*=*/end fr_dnr_act_flg
, case when icd.fr_dnr_inact_flg = cdh.fr_dnr_inact_flg then '' else icd.fr_dnr_inact_flg||'<>'||cdh.fr_dnr_inact_flg /*=*/end fr_dnr_inact_flg
, case when icd.fr_dnr_lps_flg = cdh.fr_dnr_lps_flg then '' else icd.fr_dnr_lps_flg||'<>'||cdh.fr_dnr_lps_flg /*=*/end fr_dnr_lps_flg
, case when icd.fr_mbr_gvng_lvl_cd = cdh.fr_mbr_gvng_lvl_cd then '' else icd.fr_mbr_gvng_lvl_cd||'<>'||cdh.fr_mbr_gvng_lvl_cd /*=*/end fr_mbr_gvng_lvl_cd
, case when icd.fr_mbr_exp_dt = cdh.fr_mbr_exp_dt then '' else icd.fr_mbr_exp_dt||'<>'||cdh.fr_mbr_exp_dt /*=*/end fr_mbr_exp_dt
, case when icd.fr_fst_act_keycode = cdh.fr_fst_act_keycode then '' else icd.fr_fst_act_keycode||'<>'||cdh.fr_fst_act_keycode /*=*/end fr_fst_act_keycode
, case when icd.fr_lst_act_keycode = cdh.fr_lst_act_keycode then '' else icd.fr_lst_act_keycode||'<>'||cdh.fr_lst_act_keycode /*=*/end fr_lst_act_keycode
, case when icd.fr_prior_act_keycode = cdh.fr_prior_act_keycode then '' else icd.fr_prior_act_keycode||'<>'||cdh.fr_prior_act_keycode /*=*/end fr_prior_act_keycode
, case when icd.fr_ltd_don_cnt = cdh.fr_ltd_don_cnt then '' else icd.fr_ltd_don_cnt||'<>'||cdh.fr_ltd_don_cnt /*=*/end fr_ltd_don_cnt
, case when icd.fr_don_ref_cnt = cdh.fr_don_ref_cnt then '' else icd.fr_don_ref_cnt||'<>'||cdh.fr_don_ref_cnt /*=*/end fr_don_ref_cnt
, case when icd.fr_times_ren_cnt = cdh.fr_times_ren_cnt then '' else icd.fr_times_ren_cnt||'<>'||cdh.fr_times_ren_cnt /*=*/end fr_times_ren_cnt
, case when icd.fr_lst_don_src_cd = cdh.fr_lst_don_src_cd then '' else icd.fr_lst_don_src_cd||'<>'||cdh.fr_lst_don_src_cd /*=*/end fr_lst_don_src_cd
, case when icd.fr_lst_rfl_don_amt = cdh.fr_lst_rfl_don_amt then '' else icd.fr_lst_rfl_don_amt||'<>'||cdh.fr_lst_rfl_don_amt /*=*/end fr_lst_rfl_don_amt
, case when icd.fr_lst_non_rfl_don_amt = cdh.fr_lst_non_rfl_don_amt then '' else icd.fr_lst_non_rfl_don_amt||'<>'||cdh.fr_lst_non_rfl_don_amt /*=*/end fr_lst_non_rfl_don_amt
, case when icd.fr_mbr_frst_strt_dt = cdh.fr_mbr_frst_strt_dt then '' else icd.fr_mbr_frst_strt_dt||'<>'||cdh.fr_mbr_frst_strt_dt /*=*/end fr_mbr_frst_strt_dt
, case when icd.fr_mbr_lst_keycode = cdh.fr_mbr_lst_keycode then '' else icd.fr_mbr_lst_keycode||'<>'||cdh.fr_mbr_lst_keycode /*=*/end fr_mbr_lst_keycode
, case when icd.fr_mbr_lst_ren_don_amt = cdh.fr_mbr_lst_ren_don_amt then '' else icd.fr_mbr_lst_ren_don_amt||'<>'||cdh.fr_mbr_lst_ren_don_amt /*=*/end fr_mbr_lst_ren_don_amt
, case when icd.fr_tof_cd = cdh.fr_tof_cd then '' else icd.fr_tof_cd||'<>'||cdh.fr_tof_cd /*=*/end fr_tof_cd
, case when icd.fr_lt_dnr_flg = cdh.fr_lt_dnr_flg then '' else icd.fr_lt_dnr_flg||'<>'||cdh.fr_lt_dnr_flg /*=*/end fr_lt_dnr_flg
, case when icd.fr_mbr_lst_add_don_amt = cdh.fr_mbr_lst_add_don_amt then '' else icd.fr_mbr_lst_add_don_amt||'<>'||cdh.fr_mbr_lst_add_don_amt /*=*/end fr_mbr_lst_add_don_amt
, case when icd.fr_mbr_lst_add_don_dt = cdh.fr_mbr_lst_add_don_dt then '' else icd.fr_mbr_lst_add_don_dt||'<>'||cdh.fr_mbr_lst_add_don_dt /*=*/end fr_mbr_lst_add_don_dt
, case when icd.fr_mbr_pres_cir_frst_strt_dt = cdh.fr_mbr_pres_cir_frst_strt_dt then '' else icd.fr_mbr_pres_cir_frst_strt_dt||'<>'||cdh.fr_mbr_pres_cir_frst_strt_dt /*=*/end fr_mbr_pres_cir_frst_strt_dt
, case when icd.fr_fst_don_keycode = cdh.fr_fst_don_keycode then '' else icd.fr_fst_don_keycode||'<>'||cdh.fr_fst_don_keycode /*=*/end fr_fst_don_keycode
, case when icd.fr_max_don_amt_12_mth = cdh.fr_max_don_amt_12_mth then '' else icd.fr_max_don_amt_12_mth||'<>'||cdh.fr_max_don_amt_12_mth /*=*/end fr_max_don_amt_12_mth
, case when icd.fr_max_don_amt_24_mth = cdh.fr_max_don_amt_24_mth then '' else icd.fr_max_don_amt_24_mth||'<>'||cdh.fr_max_don_amt_24_mth /*=*/end fr_max_don_amt_24_mth
, case when icd.fr_max_don_amt_36_mth = cdh.fr_max_don_amt_36_mth then '' else icd.fr_max_don_amt_36_mth||'<>'||cdh.fr_max_don_amt_36_mth /*=*/end fr_max_don_amt_36_mth
, case when icd.fr_track_number = cdh.fr_track_number then '' else icd.fr_track_number||'<>'||cdh.fr_track_number /*=*/end fr_track_number
, case when icd.fr_conv_tag_rsp_flg = cdh.fr_conv_tag_rsp_flg then '' else icd.fr_conv_tag_rsp_flg||'<>'||cdh.fr_conv_tag_rsp_flg /*=*/end fr_conv_tag_rsp_flg
, case when icd.fr_fst_don_amt = cdh.fr_fst_don_amt then '' else icd.fr_fst_don_amt||'<>'||cdh.fr_fst_don_amt /*=*/end fr_fst_don_amt
, case when icd.fr_lst_rfl_don_dt = cdh.fr_lst_rfl_don_dt then '' else icd.fr_lst_rfl_don_dt||'<>'||cdh.fr_lst_rfl_don_dt /*=*/end fr_lst_rfl_don_dt
, case when icd.fr_lst_non_rfl_don_dt = cdh.fr_lst_non_rfl_don_dt then '' else icd.fr_lst_non_rfl_don_dt||'<>'||cdh.fr_lst_non_rfl_don_dt /*=*/end fr_lst_non_rfl_don_dt
, case when icd.fr_lst_eml_don_amt = cdh.fr_lst_eml_don_amt then '' else icd.fr_lst_eml_don_amt||'<>'||cdh.fr_lst_eml_don_amt /*=*/end fr_lst_eml_don_amt
, case when icd.fr_lst_eml_don_dt = cdh.fr_lst_eml_don_dt then '' else icd.fr_lst_eml_don_dt||'<>'||cdh.fr_lst_eml_don_dt /*=*/end fr_lst_eml_don_dt
, case when icd.fr_coop_eligible_flg = cdh.fr_coop_eligible_flg then '' else icd.fr_coop_eligible_flg||'<>'||cdh.fr_coop_eligible_flg /*=*/end fr_coop_eligible_flg
, case when icd.fr_lst_dm_don_amt = cdh.fr_lst_dm_don_amt then '' else icd.fr_lst_dm_don_amt||'<>'||cdh.fr_lst_dm_don_amt /*=*/end fr_lst_dm_don_amt
, case when icd.fr_lst_dm_don_dt = cdh.fr_lst_dm_don_dt then '' else icd.fr_lst_dm_don_dt||'<>'||cdh.fr_lst_dm_don_dt /*=*/end fr_lst_dm_don_dt
, case when icd.fr_lst_org_onl_don_amt = cdh.fr_lst_org_onl_don_amt then '' else icd.fr_lst_org_onl_don_amt||'<>'||cdh.fr_lst_org_onl_don_amt /*=*/end fr_lst_org_onl_don_amt
, case when icd.fr_lst_org_onl_don_dt = cdh.fr_lst_org_onl_don_dt then '' else icd.fr_lst_org_onl_don_dt||'<>'||cdh.fr_lst_org_onl_don_dt /*=*/end fr_lst_org_onl_don_dt
, case when icd.fr_lst_ecomm_don_amt = cdh.fr_lst_ecomm_don_amt then '' else icd.fr_lst_ecomm_don_amt||'<>'||cdh.fr_lst_ecomm_don_amt /*=*/end fr_lst_ecomm_don_amt
, case when icd.fr_lst_ecomm_don_dt = cdh.fr_lst_ecomm_don_dt then '' else icd.fr_lst_ecomm_don_dt||'<>'||cdh.fr_lst_ecomm_don_dt /*=*/end fr_lst_ecomm_don_dt
, case when icd.fr_1_6_mth_don_cnt = cdh.fr_1_6_mth_don_cnt then '' else icd.fr_1_6_mth_don_cnt||'<>'||cdh.fr_1_6_mth_don_cnt /*=*/end fr_1_6_mth_don_cnt
, case when icd.fr_7_12_mth_don_cnt = cdh.fr_7_12_mth_don_cnt then '' else icd.fr_7_12_mth_don_cnt||'<>'||cdh.fr_7_12_mth_don_cnt /*=*/end fr_7_12_mth_don_cnt
, case when icd.fr_13_24_mth_don_cnt = cdh.fr_13_24_mth_don_cnt then '' else icd.fr_13_24_mth_don_cnt||'<>'||cdh.fr_13_24_mth_don_cnt /*=*/end fr_13_24_mth_don_cnt
, case when icd.fr_25_36_mth_don_cnt = cdh.fr_25_36_mth_don_cnt then '' else icd.fr_25_36_mth_don_cnt||'<>'||cdh.fr_25_36_mth_don_cnt /*=*/end fr_25_36_mth_don_cnt
, case when icd.fr_37_plus_mth_don_cnt = cdh.fr_37_plus_mth_don_cnt then '' else icd.fr_37_plus_mth_don_cnt||'<>'||cdh.fr_37_plus_mth_don_cnt /*=*/end fr_37_plus_mth_don_cnt
, case when icd.fr_dm_pros_mdl_rsp_flg = cdh.fr_dm_pros_mdl_rsp_flg then '' else icd.fr_dm_pros_mdl_rsp_flg||'<>'||cdh.fr_dm_pros_mdl_rsp_flg /*=*/end fr_dm_pros_mdl_rsp_flg
, case when icd.fr_mbr_comb_level = cdh.fr_mbr_comb_level then '' else icd.fr_mbr_comb_level||'<>'||cdh.fr_mbr_comb_level /*=*/end fr_mbr_comb_level
, case when icd.fr_mbr_comb_exp_dt = cdh.fr_mbr_comb_exp_dt then '' else icd.fr_mbr_comb_exp_dt||'<>'||cdh.fr_mbr_comb_exp_dt /*=*/end fr_mbr_comb_exp_dt
, case when icd.fr_mbr_basic_fst_dt = cdh.fr_mbr_basic_fst_dt then '' else icd.fr_mbr_basic_fst_dt||'<>'||cdh.fr_mbr_basic_fst_dt /*=*/end fr_mbr_basic_fst_dt
, case when icd.fr_mbr_basic_fst_amt = cdh.fr_mbr_basic_fst_amt then '' else icd.fr_mbr_basic_fst_amt||'<>'||cdh.fr_mbr_basic_fst_amt /*=*/end fr_mbr_basic_fst_amt
, case when icd.fr_mbr_basic_lst_dt = cdh.fr_mbr_basic_lst_dt then '' else icd.fr_mbr_basic_lst_dt||'<>'||cdh.fr_mbr_basic_lst_dt /*=*/end fr_mbr_basic_lst_dt
, case when icd.fr_mbr_basic_lst_amt = cdh.fr_mbr_basic_lst_amt then '' else icd.fr_mbr_basic_lst_amt||'<>'||cdh.fr_mbr_basic_lst_amt /*=*/end fr_mbr_basic_lst_amt
, case when icd.fr_ch_status = cdh.fr_ch_status then '' else icd.fr_ch_status||'<>'||cdh.fr_ch_status /*=*/end fr_ch_status
, case when icd.fr_ch_curr_ttl_don_amt = cdh.fr_ch_curr_ttl_don_amt then '' else icd.fr_ch_curr_ttl_don_amt||'<>'||cdh.fr_ch_curr_ttl_don_amt /*=*/end fr_ch_curr_ttl_don_amt
, case when icd.fr_ch_lst_don_dt = cdh.fr_ch_lst_don_dt then '' else icd.fr_ch_lst_don_dt||'<>'||cdh.fr_ch_lst_don_dt /*=*/end fr_ch_lst_don_dt
, case when icd.fr_ch_curr_strt_dt = cdh.fr_ch_curr_strt_dt then '' else icd.fr_ch_curr_strt_dt||'<>'||cdh.fr_ch_curr_strt_dt /*=*/end fr_ch_curr_strt_dt
, case when icd.fr_ch_lst_don_amt = cdh.fr_ch_lst_don_amt then '' else icd.fr_ch_lst_don_amt||'<>'||cdh.fr_ch_lst_don_amt /*=*/end fr_ch_lst_don_amt
, case when icd.fr_ch_curr_don_cnt = cdh.fr_ch_curr_don_cnt then '' else icd.fr_ch_curr_don_cnt||'<>'||cdh.fr_ch_curr_don_cnt /*=*/end fr_ch_curr_don_cnt
, case when icd.fr_ch_lst_don_keycd = cdh.fr_ch_lst_don_keycd then '' else icd.fr_ch_lst_don_keycd||'<>'||cdh.fr_ch_lst_don_keycd /*=*/end fr_ch_lst_don_keycd
, case when icd.fr_ch_status_active_dt = cdh.fr_ch_status_active_dt then '' else icd.fr_ch_status_active_dt||'<>'||cdh.fr_ch_status_active_dt /*=*/end fr_ch_status_active_dt
, case when icd.fr_mbr_basic_exp_dt = cdh.fr_mbr_basic_exp_dt then '' else icd.fr_mbr_basic_exp_dt||'<>'||cdh.fr_mbr_basic_exp_dt /*=*/end fr_mbr_basic_exp_dt

/*, '||                    ICD                    >>' as sep1/*, icd.* */*/
/*, '||                    CDH                    >>' as sep2/*, cdh.**/*/

from  icd full join cdh
on icd.icd_id = cdh.icd_id and icd.cdh_id = cdh.cdh_id												
/*additional joins for PK*/
;
WITH cte_summ AS (select 'MT_FR_SUMMARY' as icd_table_name, 'agg_fundraising_summary' as cdh_table_name
, to_char(sum( case when fr_avg_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_avg_don_amt
, to_char(sum( case when fr_avg_gross_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_avg_gross_don_amt
, to_char(sum( case when fr_lst_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_don_amt
, to_char(sum( case when fr_ltd_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_ltd_don_amt
, to_char(sum( case when fr_max_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_max_don_amt
, to_char(sum( case when fr_1_6_mth_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_1_6_mth_don_amt
, to_char(sum( case when fr_13_24_mth_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_13_24_mth_don_amt
, to_char(sum( case when fr_25_36_mth_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_25_36_mth_don_amt
, to_char(sum( case when fr_37_plus_mth_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_37_plus_mth_don_amt
, to_char(sum( case when fr_7_12_mth_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_7_12_mth_don_amt
, to_char(sum( case when fr_prior_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_prior_don_amt
, to_char(sum( case when fr_fst_don_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_fst_don_dt
, to_char(sum( case when fr_lst_don_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_don_dt
, to_char(sum( case when fr_prior_don_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_prior_don_dt
, to_char(sum( case when fr_dnr_act_flg = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_dnr_act_flg
, to_char(sum( case when fr_dnr_inact_flg = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_dnr_inact_flg
, to_char(sum( case when fr_dnr_lps_flg = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_dnr_lps_flg
, to_char(sum( case when fr_mbr_gvng_lvl_cd = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_gvng_lvl_cd
, to_char(sum( case when fr_mbr_exp_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_exp_dt
, to_char(sum( case when fr_fst_act_keycode = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_fst_act_keycode
, to_char(sum( case when fr_lst_act_keycode = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_act_keycode
, to_char(sum( case when fr_prior_act_keycode = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_prior_act_keycode
, to_char(sum( case when fr_ltd_don_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_ltd_don_cnt
, to_char(sum( case when fr_don_ref_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_don_ref_cnt
, to_char(sum( case when fr_times_ren_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_times_ren_cnt
, to_char(sum( case when fr_lst_don_src_cd = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_don_src_cd
, to_char(sum( case when fr_lst_rfl_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_rfl_don_amt
, to_char(sum( case when fr_lst_non_rfl_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_non_rfl_don_amt
, to_char(sum( case when fr_mbr_frst_strt_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_frst_strt_dt
, to_char(sum( case when fr_mbr_lst_keycode = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_lst_keycode
, to_char(sum( case when fr_mbr_lst_ren_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_lst_ren_don_amt
, to_char(sum( case when fr_tof_cd = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_tof_cd
, to_char(sum( case when fr_lt_dnr_flg = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lt_dnr_flg
, to_char(sum( case when fr_mbr_lst_add_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_lst_add_don_amt
, to_char(sum( case when fr_mbr_lst_add_don_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_lst_add_don_dt
, to_char(sum( case when fr_mbr_pres_cir_frst_strt_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_pres_cir_frst_strt_dt
, to_char(sum( case when fr_fst_don_keycode = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_fst_don_keycode
, to_char(sum( case when fr_max_don_amt_12_mth = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_max_don_amt_12_mth
, to_char(sum( case when fr_max_don_amt_24_mth = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_max_don_amt_24_mth
, to_char(sum( case when fr_max_don_amt_36_mth = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_max_don_amt_36_mth
, to_char(sum( case when fr_track_number = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_track_number
, to_char(sum( case when fr_conv_tag_rsp_flg = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_conv_tag_rsp_flg
, to_char(sum( case when fr_fst_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_fst_don_amt
, to_char(sum( case when fr_lst_rfl_don_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_rfl_don_dt
, to_char(sum( case when fr_lst_non_rfl_don_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_non_rfl_don_dt
, to_char(sum( case when fr_lst_eml_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_eml_don_amt
, to_char(sum( case when fr_lst_eml_don_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_eml_don_dt
, to_char(sum( case when fr_coop_eligible_flg = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_coop_eligible_flg
, to_char(sum( case when fr_lst_dm_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_dm_don_amt
, to_char(sum( case when fr_lst_dm_don_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_dm_don_dt
, to_char(sum( case when fr_lst_org_onl_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_org_onl_don_amt
, to_char(sum( case when fr_lst_org_onl_don_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_org_onl_don_dt
, to_char(sum( case when fr_lst_ecomm_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_ecomm_don_amt
, to_char(sum( case when fr_lst_ecomm_don_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_ecomm_don_dt
, to_char(sum( case when fr_1_6_mth_don_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_1_6_mth_don_cnt
, to_char(sum( case when fr_7_12_mth_don_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_7_12_mth_don_cnt
, to_char(sum( case when fr_13_24_mth_don_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_13_24_mth_don_cnt
, to_char(sum( case when fr_25_36_mth_don_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_25_36_mth_don_cnt
, to_char(sum( case when fr_37_plus_mth_don_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_37_plus_mth_don_cnt
, to_char(sum( case when fr_dm_pros_mdl_rsp_flg = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_dm_pros_mdl_rsp_flg
, to_char(sum( case when fr_mbr_comb_level = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_comb_level
, to_char(sum( case when fr_mbr_comb_exp_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_comb_exp_dt
, to_char(sum( case when fr_mbr_basic_fst_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_basic_fst_dt
, to_char(sum( case when fr_mbr_basic_fst_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_basic_fst_amt
, to_char(sum( case when fr_mbr_basic_lst_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_basic_lst_dt
, to_char(sum( case when fr_mbr_basic_lst_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_basic_lst_amt
, to_char(sum( case when fr_ch_status = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_ch_status
, to_char(sum( case when fr_ch_curr_ttl_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_ch_curr_ttl_don_amt
, to_char(sum( case when fr_ch_lst_don_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_ch_lst_don_dt
, to_char(sum( case when fr_ch_curr_strt_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_ch_curr_strt_dt
, to_char(sum( case when fr_ch_lst_don_amt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_ch_lst_don_amt
, to_char(sum( case when fr_ch_curr_don_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_ch_curr_don_cnt
, to_char(sum( case when fr_ch_lst_don_keycd = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_ch_lst_don_keycd
, to_char(sum( case when fr_ch_status_active_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_ch_status_active_dt
, to_char(sum( case when fr_mbr_basic_exp_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_mbr_basic_exp_dt
from cr_temp.comp_diff_mt_fr_summary
where match_fl is not null)


select
*

from cte_summ
;
select top 1000 1
    ,icd_id,cdh_id

, fr_mbr_gvng_lvl_cd
, fr_mbr_exp_dt
, fr_mbr_frst_strt_dt
, fr_mbr_lst_keycode
, fr_mbr_lst_ren_don_amt
, fr_mbr_lst_add_don_amt
, fr_mbr_lst_add_don_dt
, fr_mbr_pres_cir_frst_strt_dt

from cr_temp.comp_diff_mt_fr_summary
 where 1=1 and ( 1=2
-- or fr_avg_don_amt <> ''
-- or fr_avg_gross_don_amt <> ''
-- or fr_lst_don_amt <> ''
-- or fr_ltd_don_amt <> ''
-- or fr_max_don_amt <> ''
-- or fr_1_6_mth_don_amt <> ''
-- or fr_13_24_mth_don_amt <> ''
-- or fr_25_36_mth_don_amt <> ''
-- or fr_37_plus_mth_don_amt <> ''
-- or fr_7_12_mth_don_amt <> ''
-- or fr_prior_don_amt <> ''
-- or fr_fst_don_dt <> ''
-- or fr_lst_don_dt <> ''
-- or fr_prior_don_dt <> ''
-- or fr_dnr_act_flg <> ''
-- or fr_dnr_inact_flg <> ''
-- or fr_dnr_lps_flg <> ''
or fr_mbr_gvng_lvl_cd <> ''
-- or fr_mbr_exp_dt <> ''
-- or fr_fst_act_keycode <> ''
-- or fr_lst_act_keycode <> ''
-- or fr_prior_act_keycode <> ''
-- or fr_ltd_don_cnt <> ''
-- or fr_don_ref_cnt <> ''
-- or fr_times_ren_cnt <> ''
-- or fr_lst_don_src_cd <> ''
-- or fr_lst_rfl_don_amt <> ''
-- or fr_lst_non_rfl_don_amt <> ''
-- or fr_mbr_frst_strt_dt <> ''
or fr_mbr_lst_keycode <> ''
-- or fr_mbr_lst_ren_don_amt <> ''
-- or fr_tof_cd <> ''
-- or fr_lt_dnr_flg <> ''
-- or fr_mbr_lst_add_don_amt <> ''
-- or fr_mbr_lst_add_don_dt <> ''
-- or fr_mbr_pres_cir_frst_strt_dt <> ''
-- or fr_fst_don_keycode <> ''
-- or fr_max_don_amt_12_mth <> ''
-- or fr_max_don_amt_24_mth <> ''
-- or fr_max_don_amt_36_mth <> ''
-- or fr_track_number <> ''
-- or fr_conv_tag_rsp_flg <> ''
-- or fr_fst_don_amt <> ''
-- or fr_lst_rfl_don_dt <> ''
-- or fr_lst_non_rfl_don_dt <> ''
-- or fr_lst_eml_don_amt <> ''
-- or fr_lst_eml_don_dt <> ''
-- or fr_coop_eligible_flg <> ''
-- or fr_lst_dm_don_amt <> ''
-- or fr_lst_dm_don_dt <> ''
-- or fr_lst_org_onl_don_amt <> '
-- or fr_lst_org_onl_don_dt <> ''
-- or fr_lst_ecomm_don_amt <> ''
-- or fr_lst_ecomm_don_dt <> ''
-- or fr_1_6_mth_don_cnt <> ''
-- or fr_7_12_mth_don_cnt <> ''
-- or fr_13_24_mth_don_cnt <> ''
-- or fr_25_36_mth_don_cnt <> ''
-- or fr_37_plus_mth_don_cnt <> ''
-- or fr_dm_pros_mdl_rsp_flg <> ''
-- or fr_mbr_comb_level <> ''
-- or fr_mbr_comb_exp_dt <> ''
-- or fr_mbr_basic_fst_dt <> ''
-- or fr_mbr_basic_fst_amt <> ''
-- or fr_mbr_basic_lst_dt <> ''
-- or fr_mbr_basic_lst_amt <> ''
-- or fr_ch_status <> ''
-- or fr_ch_curr_ttl_don_amt <> ''
-- or fr_ch_lst_don_dt <> ''
-- or fr_ch_curr_strt_dt <> ''
-- or fr_ch_lst_don_amt <> ''
-- or fr_ch_curr_don_cnt <> ''
-- or fr_ch_lst_don_keycd <> ''
-- or fr_ch_status_active_dt <> ''
-- or fr_mbr_basic_exp_dt <> ''
)

;
------------------------------------------------------------------------RUN ALL ABOVE


;
select * from   cr_temp.GTT_MT_FR_SUMMARY  where icd_id = 00000000000000000000
;
select * from   cr_temp.gtt_agg_fundraising_summary  where cdh_id = 00000000000000000000

;


SELECT count(*)
FROM cr_temp.comp_diff_mt_fr_summary
    where match_fl is null--1393
LIMIT 100;



SELECT md5(email_address,10),*
FROM prod.agg_email
    LIMIT 100;

SELECT *
FROM prod.individual_email
LIMIT 100;