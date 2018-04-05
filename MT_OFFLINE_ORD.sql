;
create table cr_temp.GTT_MT_OFFLINE_ORD as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
/*, acct_id*/
, NVL2(ord_num, ord_num::text,'(NULL)') as ord_num
, NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)') as ord_dt
, NVL2(stat_cd, stat_cd::text,'(NULL)') as stat_cd
, NVL2(TO_CHAR(cplt_dt,'DD-MON-YY'), TO_CHAR(cplt_dt,'DD-MON-YY')::text,'(NULL)') as cplt_dt
, NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)') as entr_typ_cd
, NVL2(cr_stat_cd, cr_stat_cd::text,'(NULL)') as cr_stat_cd
, NVL2(canc_flg, canc_flg::text,'(NULL)') as canc_flg
, NVL2(canc_rsn_cd, canc_rsn_cd::text,'(NULL)') as canc_rsn_cd
, NVL2(TO_CHAR(canc_dt,'DD-MON-YY'), TO_CHAR(canc_dt,'DD-MON-YY')::text,'(NULL)') as canc_dt
, NVL2(iss_canc_num, iss_canc_num::text,'(NULL)') as iss_canc_num
, NVL2(trunc(nvl(term_mth_cnt,'0')), trunc(nvl(term_mth_cnt,'0'))::text,'(NULL)') as term_mth_cnt
, NVL2(trunc(nvl(net_val_amt,'0')), trunc(nvl(net_val_amt,'0'))::text,'(NULL)') as net_val_amt
, NVL2(trunc(nvl(agcy_grs_val_amt,'0')), trunc(nvl(agcy_grs_val_amt,'0'))::text,'(NULL)') as agcy_grs_val_amt
, NVL2(ofr_chngd_flg, ofr_chngd_flg::text,'(NULL)') as ofr_chngd_flg
, NVL2(rnw_cd, rnw_cd::text,'(NULL)') as rnw_cd
, NVL2(mlt_cpy_flg, mlt_cpy_flg::text,'(NULL)') as mlt_cpy_flg
, NVL2(src_cd, src_cd::text,'(NULL)') as src_cd
, NVL2(md_cd, md_cd::text,'(NULL)') as md_cd
, NVL2(mag_cd, mag_cd::text,'(NULL)') as mag_cd
, NVL2(mag_catg_cd, mag_catg_cd::text,'(NULL)') as mag_catg_cd
, NVL2(set_cd, set_cd::text,'(NULL)') as set_cd
, NVL2(bulk_flg, bulk_flg::text,'(NULL)') as bulk_flg
, NVL2(keycode, keycode::text,'(NULL)') as keycode
, NVL2(spec_info_txt, spec_info_txt::text,'(NULL)') as spec_info_txt
, NVL2(agcy_cd, agcy_cd::text,'(NULL)') as agcy_cd
, NVL2(prev_exp_iss_num, prev_exp_iss_num::text,'(NULL)') as prev_exp_iss_num
, NVL2(orig_strt_iss_num, orig_strt_iss_num::text,'(NULL)') as orig_strt_iss_num
, NVL2(orig_ord_flg, orig_ord_flg::text,'(NULL)') as orig_ord_flg
, NVL2(TO_CHAR(first_dt,'DD-MON-YY'), TO_CHAR(first_dt,'DD-MON-YY')::text,'(NULL)') as first_dt
, NVL2(trunc(nvl(pd_amt,'0')), trunc(nvl(pd_amt,'0'))::text,'(NULL)') as pd_amt
, NVL2(TO_CHAR(pmt_dt,'DD-MON-YY'), TO_CHAR(pmt_dt,'DD-MON-YY')::text,'(NULL)') as pmt_dt
, NVL2(pmt_mthd_cd, pmt_mthd_cd::text,'(NULL)') as pmt_mthd_cd
, NVL2(pmt_typ_cd, pmt_typ_cd::text,'(NULL)') as pmt_typ_cd
, NVL2(trunc(nvl(tax_pd_amt,'0')), trunc(nvl(tax_pd_amt,'0'))::text,'(NULL)') as tax_pd_amt
, NVL2(trunc(nvl(spec_hdlg_pd_amt,'0')), trunc(nvl(spec_hdlg_pd_amt,'0'))::text,'(NULL)') as spec_hdlg_pd_amt
, NVL2(blg_key_num, blg_key_num::text,'(NULL)') as blg_key_num
/*---END---*/
,md5(
ICD_ID||cdh_id
/*--START--*/
/*||individual_id*/
/*||acct_id*/
||NVL2(ord_num, ord_num::text,'(NULL)')
||NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(stat_cd, stat_cd::text,'(NULL)')
||NVL2(TO_CHAR(cplt_dt,'DD-MON-YY'), TO_CHAR(cplt_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)')
||NVL2(cr_stat_cd, cr_stat_cd::text,'(NULL)')
||NVL2(canc_flg, canc_flg::text,'(NULL)')
||NVL2(canc_rsn_cd, canc_rsn_cd::text,'(NULL)')
||NVL2(TO_CHAR(canc_dt,'DD-MON-YY'), TO_CHAR(canc_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(iss_canc_num, iss_canc_num::text,'(NULL)')
||NVL2(trunc(nvl(term_mth_cnt,'0')), trunc(nvl(term_mth_cnt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(net_val_amt,'0')), trunc(nvl(net_val_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(agcy_grs_val_amt,'0')), trunc(nvl(agcy_grs_val_amt,'0'))::text,'(NULL)')
||NVL2(ofr_chngd_flg, ofr_chngd_flg::text,'(NULL)')
||NVL2(rnw_cd, rnw_cd::text,'(NULL)')
||NVL2(mlt_cpy_flg, mlt_cpy_flg::text,'(NULL)')
||NVL2(src_cd, src_cd::text,'(NULL)')
||NVL2(md_cd, md_cd::text,'(NULL)')
||NVL2(mag_cd, mag_cd::text,'(NULL)')
||NVL2(mag_catg_cd, mag_catg_cd::text,'(NULL)')
||NVL2(set_cd, set_cd::text,'(NULL)')
||NVL2(bulk_flg, bulk_flg::text,'(NULL)')
||NVL2(keycode, keycode::text,'(NULL)')
||NVL2(spec_info_txt, spec_info_txt::text,'(NULL)')
||NVL2(agcy_cd, agcy_cd::text,'(NULL)')
||NVL2(prev_exp_iss_num, prev_exp_iss_num::text,'(NULL)')
||NVL2(orig_strt_iss_num, orig_strt_iss_num::text,'(NULL)')
||NVL2(orig_ord_flg, orig_ord_flg::text,'(NULL)')
||NVL2(TO_CHAR(first_dt,'DD-MON-YY'), TO_CHAR(first_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(trunc(nvl(pd_amt,'0')), trunc(nvl(pd_amt,'0'))::text,'(NULL)')
||NVL2(TO_CHAR(pmt_dt,'DD-MON-YY'), TO_CHAR(pmt_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(pmt_mthd_cd, pmt_mthd_cd::text,'(NULL)')
||NVL2(pmt_typ_cd, pmt_typ_cd::text,'(NULL)')
||NVL2(trunc(nvl(tax_pd_amt,'0')), trunc(nvl(tax_pd_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(spec_hdlg_pd_amt,'0')), trunc(nvl(spec_hdlg_pd_amt,'0'))::text,'(NULL)')
||NVL2(blg_key_num, blg_key_num::text,'(NULL)')
/*---END---*/
) as hash_value
from cr_temp.MT_OFFLINE_ORD T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;
CREATE TABLE cr_temp.gtt_agg_print_order AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
/*, acct_id*/
, NVL2(ord_num, ord_num::text,'(NULL)') as ord_num
, NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)') as ord_dt
, NVL2(stat_cd, stat_cd::text,'(NULL)') as stat_cd
, NVL2(TO_CHAR(cplt_dt,'DD-MON-YY'), TO_CHAR(cplt_dt,'DD-MON-YY')::text,'(NULL)') as cplt_dt
, NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)') as entr_typ_cd
, NVL2(cr_stat_cd, cr_stat_cd::text,'(NULL)') as cr_stat_cd
, NVL2(canc_flg, canc_flg::text,'(NULL)') as canc_flg
, NVL2(canc_rsn_cd, canc_rsn_cd::text,'(NULL)') as canc_rsn_cd
, NVL2(TO_CHAR(canc_dt,'DD-MON-YY'), TO_CHAR(canc_dt,'DD-MON-YY')::text,'(NULL)') as canc_dt
, NVL2(iss_canc_num, iss_canc_num::text,'(NULL)') as iss_canc_num
, NVL2(trunc(nvl(term_mth_cnt,'0')), trunc(nvl(term_mth_cnt,'0'))::text,'(NULL)') as term_mth_cnt
, NVL2(trunc(nvl(net_val_amt,'0')), trunc(nvl(net_val_amt,'0'))::text,'(NULL)') as net_val_amt
, NVL2(trunc(nvl(agcy_grs_val_amt,'0')), trunc(nvl(agcy_grs_val_amt,'0'))::text,'(NULL)') as agcy_grs_val_amt
, NVL2(ofr_chngd_flg, ofr_chngd_flg::text,'(NULL)') as ofr_chngd_flg
, NVL2(rnw_cd, rnw_cd::text,'(NULL)') as rnw_cd
, NVL2(mlt_cpy_flg, mlt_cpy_flg::text,'(NULL)') as mlt_cpy_flg
, NVL2(src_cd, src_cd::text,'(NULL)') as src_cd
, NVL2(md_cd, md_cd::text,'(NULL)') as md_cd
, NVL2(mag_cd, mag_cd::text,'(NULL)') as mag_cd
, NVL2(mag_catg_cd, mag_catg_cd::text,'(NULL)') as mag_catg_cd
, NVL2(set_cd, set_cd::text,'(NULL)') as set_cd
, NVL2(bulk_flg, bulk_flg::text,'(NULL)') as bulk_flg
, NVL2(keycode, keycode::text,'(NULL)') as keycode
, NVL2(spec_info_txt, spec_info_txt::text,'(NULL)') as spec_info_txt
, NVL2(agcy_cd, agcy_cd::text,'(NULL)') as agcy_cd
, NVL2(prev_exp_iss_num, prev_exp_iss_num::text,'(NULL)') as prev_exp_iss_num
, NVL2(orig_strt_iss_num, orig_strt_iss_num::text,'(NULL)') as orig_strt_iss_num
, NVL2(orig_ord_flg, orig_ord_flg::text,'(NULL)') as orig_ord_flg
, NVL2(TO_CHAR(first_dt,'DD-MON-YY'), TO_CHAR(first_dt,'DD-MON-YY')::text,'(NULL)') as first_dt
, NVL2(trunc(nvl(pd_amt,'0')), trunc(nvl(pd_amt,'0'))::text,'(NULL)') as pd_amt
, NVL2(TO_CHAR(pmt_dt,'DD-MON-YY'), TO_CHAR(pmt_dt,'DD-MON-YY')::text,'(NULL)') as pmt_dt
, NVL2(pmt_mthd_cd, pmt_mthd_cd::text,'(NULL)') as pmt_mthd_cd
, NVL2(pmt_typ_cd, pmt_typ_cd::text,'(NULL)') as pmt_typ_cd
, NVL2(trunc(nvl(tax_pd_amt,'0')), trunc(nvl(tax_pd_amt,'0'))::text,'(NULL)') as tax_pd_amt
, NVL2(trunc(nvl(spec_hdlg_pd_amt,'0')), trunc(nvl(spec_hdlg_pd_amt,'0'))::text,'(NULL)') as spec_hdlg_pd_amt
, NVL2(blg_key_num, blg_key_num::text,'(NULL)') as blg_key_num
/*---END---*/
,md5(
ICD_ID||cdh_id
/*--START--*/
/*||individual_id*/
/*||acct_id*/
||NVL2(ord_num, ord_num::text,'(NULL)')
||NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(stat_cd, stat_cd::text,'(NULL)')
||NVL2(TO_CHAR(cplt_dt,'DD-MON-YY'), TO_CHAR(cplt_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)')
||NVL2(cr_stat_cd, cr_stat_cd::text,'(NULL)')
||NVL2(canc_flg, canc_flg::text,'(NULL)')
||NVL2(canc_rsn_cd, canc_rsn_cd::text,'(NULL)')
||NVL2(TO_CHAR(canc_dt,'DD-MON-YY'), TO_CHAR(canc_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(iss_canc_num, iss_canc_num::text,'(NULL)')
||NVL2(trunc(nvl(term_mth_cnt,'0')), trunc(nvl(term_mth_cnt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(net_val_amt,'0')), trunc(nvl(net_val_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(agcy_grs_val_amt,'0')), trunc(nvl(agcy_grs_val_amt,'0'))::text,'(NULL)')
||NVL2(ofr_chngd_flg, ofr_chngd_flg::text,'(NULL)')
||NVL2(rnw_cd, rnw_cd::text,'(NULL)')
||NVL2(mlt_cpy_flg, mlt_cpy_flg::text,'(NULL)')
||NVL2(src_cd, src_cd::text,'(NULL)')
||NVL2(md_cd, md_cd::text,'(NULL)')
||NVL2(mag_cd, mag_cd::text,'(NULL)')
||NVL2(mag_catg_cd, mag_catg_cd::text,'(NULL)')
||NVL2(set_cd, set_cd::text,'(NULL)')
||NVL2(bulk_flg, bulk_flg::text,'(NULL)')
||NVL2(keycode, keycode::text,'(NULL)')
||NVL2(spec_info_txt, spec_info_txt::text,'(NULL)')
||NVL2(agcy_cd, agcy_cd::text,'(NULL)')
||NVL2(prev_exp_iss_num, prev_exp_iss_num::text,'(NULL)')
||NVL2(orig_strt_iss_num, orig_strt_iss_num::text,'(NULL)')
||NVL2(orig_ord_flg, orig_ord_flg::text,'(NULL)')
||NVL2(TO_CHAR(first_dt,'DD-MON-YY'), TO_CHAR(first_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(trunc(nvl(pd_amt,'0')), trunc(nvl(pd_amt,'0'))::text,'(NULL)')
||NVL2(TO_CHAR(pmt_dt,'DD-MON-YY'), TO_CHAR(pmt_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(pmt_mthd_cd, pmt_mthd_cd::text,'(NULL)')
||NVL2(pmt_typ_cd, pmt_typ_cd::text,'(NULL)')
||NVL2(trunc(nvl(tax_pd_amt,'0')), trunc(nvl(tax_pd_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(spec_hdlg_pd_amt,'0')), trunc(nvl(spec_hdlg_pd_amt,'0'))::text,'(NULL)')
||NVL2(blg_key_num, blg_key_num::text,'(NULL)')
/*---END---*/
) as hash_value
from prod.agg_print_order T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;
------------------------------------------------------------------------TABLE CREATION END\

;
create table crprod_cdh.cr_temp.id_xref_ncbi_excl_mt_off_ord as 
with hash_diff as (
select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID, HASH_VALUE
from (
select  ICD_ID, cdh_id, 1 as ICD_FL, null as CDH_FL, HASH_VALUE
from cr_temp.GTT_MT_OFFLINE_ORD
union all
select  ICD_ID, cdh_id, null as ICD_FL, 1 as CDH_FL, HASH_VALUE
from cr_temp.gtt_agg_print_order
)
group by ICD_ID, CDH_ID, hash_value
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)--select top 50 * from hash_diff
select distinct icd_id, cdh_id 
from crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts xref1
where not exists (select null from hash_diff dff where dff.icd_id = xref1.icd_id and dff.cdh_id = xref1.cdh_id)
-----------------
;
with hash_diff as (
select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID, HASH_VALUE
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.GTT_MT_OFFLINE_ORD
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.gtt_agg_print_order
)
group by ICD_ID, CDH_ID, hash_value
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select top 50 * from hash_diff;
------------------------------------------------------------------------RUN ALL ABOVE
;
select * from   cr_temp.GTT_MT_OFFLINE_ORD  where icd_id = 10002000516237070
;
select * from   cr_temp.gtt_agg_print_order  where cdh_id = 1218390242
;
--truncate table    cr_temp.GTT_MT_OFFLINE_ORD;
drop table  cr_temp.GTT_MT_OFFLINE_ORD;
--truncate table cr_temp.gtt_agg_print_order;
drop table  cr_temp.gtt_agg_print_order;


create table crprod_cdh.cr_temp.id_xref_ncbi_excl_mt_off_ord as 
with hash_diff as (
select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID, HASH_VALUE
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.GTT_MT_OFFLINE_ORD
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.gtt_agg_print_order
)
group by ICD_ID, CDH_ID, hash_value
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select distinct icd_id, cdh_id 
from crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts xref1
where not exists (select null from hash_diff dff where dff.icd_id = xref1.icd_id and dff.cdh_id = xref1.cdh_id)