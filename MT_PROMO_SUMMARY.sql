;
drop table if exists  cr_temp.GTT_MT_PROMO_SUMMARY_2;
create table cr_temp.GTT_MT_PROMO_SUMMARY_2 as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
, NVL2(TO_CHAR(cr_lst_prom_dt,'DD-MON-YY'), TO_CHAR(cr_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as cr_lst_prom_dt
, NVL2(TO_CHAR(crsch_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(crsch_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as crsch_em_lst_prom_dt
, NVL2(TO_CHAR(hl_lst_prom_dt,'DD-MON-YY'), TO_CHAR(hl_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as hl_lst_prom_dt
, NVL2(TO_CHAR(fr_tm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(fr_tm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as fr_tm_lst_prom_dt
, NVL2(abq_lst_keycode, abq_lst_keycode::text,'(NULL)') as abq_lst_keycode
, NVL2(TO_CHAR(adv_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(adv_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as adv_em_lst_prom_dt
, NVL2(trunc(nvl(adv_em_promo_cnt,'0')), trunc(nvl(adv_em_promo_cnt,'0'))::text,'(NULL)') as adv_em_promo_cnt
, NVL2(trunc(nvl(adv_sla_em_promo_cnt,'0')), trunc(nvl(adv_sla_em_promo_cnt,'0'))::text,'(NULL)') as adv_sla_em_promo_cnt
, NVL2(TO_CHAR(prod_lst_prom_dt,'DD-MON-YY'), TO_CHAR(prod_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as prod_lst_prom_dt
, NVL2(TO_CHAR(non_prod_lst_prom_dt,'DD-MON-YY'), TO_CHAR(non_prod_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as non_prod_lst_prom_dt
, NVL2(TO_CHAR(cro_lst_prom_dt,'DD-MON-YY'), TO_CHAR(cro_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as cro_lst_prom_dt
, NVL2(TO_CHAR(prod_dm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(prod_dm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as prod_dm_lst_prom_dt
, NVL2(TO_CHAR(cr_dm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(cr_dm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as cr_dm_lst_prom_dt
, NVL2(TO_CHAR(cro_dm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(cro_dm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as cro_dm_lst_prom_dt
, NVL2(TO_CHAR(fr_dm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(fr_dm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as fr_dm_lst_prom_dt
, NVL2(TO_CHAR(hl_dm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(hl_dm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as hl_dm_lst_prom_dt
, NVL2(TO_CHAR(ind_dm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(ind_dm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as ind_dm_lst_prom_dt
, NVL2(TO_CHAR(prod_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(prod_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as prod_em_lst_prom_dt
, NVL2(TO_CHAR(cr_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(cr_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as cr_em_lst_prom_dt
, NVL2(TO_CHAR(cro_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(cro_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as cro_em_lst_prom_dt
, NVL2(TO_CHAR(hl_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(hl_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as hl_em_lst_prom_dt
, NVL2(TO_CHAR(ind_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(ind_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as ind_em_lst_prom_dt
, NVL2(TO_CHAR(fr_lst_prom_dt,'DD-MON-YY'), TO_CHAR(fr_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_prom_dt
, NVL2(TO_CHAR(ind_lst_prom_dt,'DD-MON-YY'), TO_CHAR(ind_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as ind_lst_prom_dt
, NVL2(trunc(nvl(prod_prom_cnt,'0')), trunc(nvl(prod_prom_cnt,'0'))::text,'(NULL)') as prod_prom_cnt
, NVL2(trunc(nvl(non_prod_prom_cnt,'0')), trunc(nvl(non_prod_prom_cnt,'0'))::text,'(NULL)') as non_prod_prom_cnt
, NVL2(trunc(nvl(cr_prom_cnt,'0')), trunc(nvl(cr_prom_cnt,'0'))::text,'(NULL)') as cr_prom_cnt
, NVL2(trunc(nvl(cro_prom_cnt,'0')), trunc(nvl(cro_prom_cnt,'0'))::text,'(NULL)') as cro_prom_cnt
, NVL2(trunc(nvl(prod_dm_prom_cnt,'0')), trunc(nvl(prod_dm_prom_cnt,'0'))::text,'(NULL)') as prod_dm_prom_cnt
, NVL2(trunc(nvl(cr_dm_prom_cnt,'0')), trunc(nvl(cr_dm_prom_cnt,'0'))::text,'(NULL)') as cr_dm_prom_cnt
, NVL2(trunc(nvl(cro_dm_prom_cnt,'0')), trunc(nvl(cro_dm_prom_cnt,'0'))::text,'(NULL)') as cro_dm_prom_cnt
, NVL2(trunc(nvl(fr_dm_prom_cnt,'0')), trunc(nvl(fr_dm_prom_cnt,'0'))::text,'(NULL)') as fr_dm_prom_cnt
, NVL2(trunc(nvl(hl_dm_prom_cnt,'0')), trunc(nvl(hl_dm_prom_cnt,'0'))::text,'(NULL)') as hl_dm_prom_cnt
, NVL2(trunc(nvl(prod_em_prom_cnt,'0')), trunc(nvl(prod_em_prom_cnt,'0'))::text,'(NULL)') as prod_em_prom_cnt
, NVL2(trunc(nvl(cr_em_prom_cnt,'0')), trunc(nvl(cr_em_prom_cnt,'0'))::text,'(NULL)') as cr_em_prom_cnt
, NVL2(trunc(nvl(cro_em_prom_cnt,'0')), trunc(nvl(cro_em_prom_cnt,'0'))::text,'(NULL)') as cro_em_prom_cnt
, NVL2(trunc(nvl(crsch_em_prom_cnt,'0')), trunc(nvl(crsch_em_prom_cnt,'0'))::text,'(NULL)') as crsch_em_prom_cnt
, NVL2(trunc(nvl(fr_prom_cnt,'0')), trunc(nvl(fr_prom_cnt,'0'))::text,'(NULL)') as fr_prom_cnt
, NVL2(trunc(nvl(cr_dnr_prom_cnt,'0')), trunc(nvl(cr_dnr_prom_cnt,'0'))::text,'(NULL)') as cr_dnr_prom_cnt
, NVL2(trunc(nvl(cro_dnr_prom_cnt,'0')), trunc(nvl(cro_dnr_prom_cnt,'0'))::text,'(NULL)') as cro_dnr_prom_cnt
, NVL2(trunc(nvl(hl_prom_cnt,'0')), trunc(nvl(hl_prom_cnt,'0'))::text,'(NULL)') as hl_prom_cnt
, NVL2(trunc(nvl(ind_prom_cnt,'0')), trunc(nvl(ind_prom_cnt,'0'))::text,'(NULL)') as ind_prom_cnt
, NVL2(trunc(nvl(non_prod_sla_prom_cnt,'0')), trunc(nvl(non_prod_sla_prom_cnt,'0'))::text,'(NULL)') as non_prod_sla_prom_cnt
, NVL2(trunc(nvl(fr_sla_dm_prom_cnt,'0')), trunc(nvl(fr_sla_dm_prom_cnt,'0'))::text,'(NULL)') as fr_sla_dm_prom_cnt
, NVL2(trunc(nvl(fr_sla_prom_cnt,'0')), trunc(nvl(fr_sla_prom_cnt,'0'))::text,'(NULL)') as fr_sla_prom_cnt
, NVL2(trunc(nvl(fr_sla_tm_prom_cnt,'0')), trunc(nvl(fr_sla_tm_prom_cnt,'0'))::text,'(NULL)') as fr_sla_tm_prom_cnt
, NVL2(trunc(nvl(prod_slo_prom_cnt,'0')), trunc(nvl(prod_slo_prom_cnt,'0'))::text,'(NULL)') as prod_slo_prom_cnt
, NVL2(trunc(nvl(cr_slo_prom_cnt,'0')), trunc(nvl(cr_slo_prom_cnt,'0'))::text,'(NULL)') as cr_slo_prom_cnt
, NVL2(trunc(nvl(cro_slo_prom_cnt,'0')), trunc(nvl(cro_slo_prom_cnt,'0'))::text,'(NULL)') as cro_slo_prom_cnt
, NVL2(trunc(nvl(cr_slo_dm_prom_cnt,'0')), trunc(nvl(cr_slo_dm_prom_cnt,'0'))::text,'(NULL)') as cr_slo_dm_prom_cnt
, NVL2(trunc(nvl(cro_slo_dm_prom_cnt,'0')), trunc(nvl(cro_slo_dm_prom_cnt,'0'))::text,'(NULL)') as cro_slo_dm_prom_cnt
, NVL2(trunc(nvl(cro_slo_em_prom_cnt,'0')), trunc(nvl(cro_slo_em_prom_cnt,'0'))::text,'(NULL)') as cro_slo_em_prom_cnt
, NVL2(trunc(nvl(hl_slo_prom_cnt,'0')), trunc(nvl(hl_slo_prom_cnt,'0'))::text,'(NULL)') as hl_slo_prom_cnt
, NVL2(trunc(nvl(offline_slo_prom_cnt,'0')), trunc(nvl(offline_slo_prom_cnt,'0'))::text,'(NULL)') as offline_slo_prom_cnt
, NVL2(trunc(nvl(online_slo_prom_cnt,'0')), trunc(nvl(online_slo_prom_cnt,'0'))::text,'(NULL)') as online_slo_prom_cnt
, NVL2(trunc(nvl(fr_tm_prom_cnt,'0')), trunc(nvl(fr_tm_prom_cnt,'0'))::text,'(NULL)') as fr_tm_prom_cnt

/*---END---*/

from cr_temp.mt_promo_summary_3 T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID 
--where exists (select null from cr_temp.GTT_MT_RESP_SUMMARY_0 mt where mt.icd_id = T_MAIN.individual_id )
;

drop table if exists  cr_temp.gtt_agg_promotion_summary_ac;
CREATE TABLE cr_temp.gtt_agg_promotion_summary_ac AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
, NVL2(TO_CHAR(cr_lst_prom_dt,'DD-MON-YY'), TO_CHAR(cr_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as cr_lst_prom_dt
, NVL2(TO_CHAR(crsch_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(crsch_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as crsch_em_lst_prom_dt
, NVL2(TO_CHAR(hl_lst_prom_dt,'DD-MON-YY'), TO_CHAR(hl_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as hl_lst_prom_dt
, NVL2(TO_CHAR(fr_tm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(fr_tm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as fr_tm_lst_prom_dt
, NVL2(abq_lst_keycode, abq_lst_keycode::text,'(NULL)') as abq_lst_keycode
, NVL2(TO_CHAR(adv_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(adv_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as adv_em_lst_prom_dt
, NVL2(trunc(nvl(adv_em_promo_cnt,'0')), trunc(nvl(adv_em_promo_cnt,'0'))::text,'(NULL)') as adv_em_promo_cnt
, NVL2(trunc(nvl(adv_sla_em_promo_cnt,'0')), trunc(nvl(adv_sla_em_promo_cnt,'0'))::text,'(NULL)') as adv_sla_em_promo_cnt
, NVL2(TO_CHAR(prod_lst_prom_dt,'DD-MON-YY'), TO_CHAR(prod_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as prod_lst_prom_dt
, NVL2(TO_CHAR(non_prod_lst_prom_dt,'DD-MON-YY'), TO_CHAR(non_prod_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as non_prod_lst_prom_dt
, NVL2(TO_CHAR(cro_lst_prom_dt,'DD-MON-YY'), TO_CHAR(cro_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as cro_lst_prom_dt
, NVL2(TO_CHAR(prod_dm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(prod_dm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as prod_dm_lst_prom_dt
, NVL2(TO_CHAR(cr_dm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(cr_dm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as cr_dm_lst_prom_dt
, NVL2(TO_CHAR(cro_dm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(cro_dm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as cro_dm_lst_prom_dt
, NVL2(TO_CHAR(fr_dm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(fr_dm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as fr_dm_lst_prom_dt
, NVL2(TO_CHAR(hl_dm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(hl_dm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as hl_dm_lst_prom_dt
, NVL2(TO_CHAR(ind_dm_lst_prom_dt,'DD-MON-YY'), TO_CHAR(ind_dm_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as ind_dm_lst_prom_dt
, NVL2(TO_CHAR(prod_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(prod_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as prod_em_lst_prom_dt
, NVL2(TO_CHAR(cr_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(cr_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as cr_em_lst_prom_dt
, NVL2(TO_CHAR(cro_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(cro_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as cro_em_lst_prom_dt
, NVL2(TO_CHAR(hl_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(hl_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as hl_em_lst_prom_dt
, NVL2(TO_CHAR(ind_em_lst_prom_dt,'DD-MON-YY'), TO_CHAR(ind_em_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as ind_em_lst_prom_dt
, NVL2(TO_CHAR(fr_lst_prom_dt,'DD-MON-YY'), TO_CHAR(fr_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as fr_lst_prom_dt
, NVL2(TO_CHAR(ind_lst_prom_dt,'DD-MON-YY'), TO_CHAR(ind_lst_prom_dt,'DD-MON-YY')::text,'(NULL)') as ind_lst_prom_dt
, NVL2(trunc(nvl(prod_prom_cnt,'0')), trunc(nvl(prod_prom_cnt,'0'))::text,'(NULL)') as prod_prom_cnt
, NVL2(trunc(nvl(non_prod_prom_cnt,'0')), trunc(nvl(non_prod_prom_cnt,'0'))::text,'(NULL)') as non_prod_prom_cnt
, NVL2(trunc(nvl(cr_prom_cnt,'0')), trunc(nvl(cr_prom_cnt,'0'))::text,'(NULL)') as cr_prom_cnt
, NVL2(trunc(nvl(cro_prom_cnt,'0')), trunc(nvl(cro_prom_cnt,'0'))::text,'(NULL)') as cro_prom_cnt
, NVL2(trunc(nvl(prod_dm_prom_cnt,'0')), trunc(nvl(prod_dm_prom_cnt,'0'))::text,'(NULL)') as prod_dm_prom_cnt
, NVL2(trunc(nvl(cr_dm_prom_cnt,'0')), trunc(nvl(cr_dm_prom_cnt,'0'))::text,'(NULL)') as cr_dm_prom_cnt
, NVL2(trunc(nvl(cro_dm_prom_cnt,'0')), trunc(nvl(cro_dm_prom_cnt,'0'))::text,'(NULL)') as cro_dm_prom_cnt
, NVL2(trunc(nvl(fr_dm_prom_cnt,'0')), trunc(nvl(fr_dm_prom_cnt,'0'))::text,'(NULL)') as fr_dm_prom_cnt
, NVL2(trunc(nvl(hl_dm_prom_cnt,'0')), trunc(nvl(hl_dm_prom_cnt,'0'))::text,'(NULL)') as hl_dm_prom_cnt
, NVL2(trunc(nvl(prod_em_prom_cnt,'0')), trunc(nvl(prod_em_prom_cnt,'0'))::text,'(NULL)') as prod_em_prom_cnt
, NVL2(trunc(nvl(cr_em_prom_cnt,'0')), trunc(nvl(cr_em_prom_cnt,'0'))::text,'(NULL)') as cr_em_prom_cnt
, NVL2(trunc(nvl(cro_em_prom_cnt,'0')), trunc(nvl(cro_em_prom_cnt,'0'))::text,'(NULL)') as cro_em_prom_cnt
, NVL2(trunc(nvl(crsch_em_prom_cnt,'0')), trunc(nvl(crsch_em_prom_cnt,'0'))::text,'(NULL)') as crsch_em_prom_cnt
, NVL2(trunc(nvl(fr_prom_cnt,'0')), trunc(nvl(fr_prom_cnt,'0'))::text,'(NULL)') as fr_prom_cnt
, NVL2(trunc(nvl(cr_dnr_prom_cnt,'0')), trunc(nvl(cr_dnr_prom_cnt,'0'))::text,'(NULL)') as cr_dnr_prom_cnt
, NVL2(trunc(nvl(cro_dnr_prom_cnt,'0')), trunc(nvl(cro_dnr_prom_cnt,'0'))::text,'(NULL)') as cro_dnr_prom_cnt
, NVL2(trunc(nvl(hl_prom_cnt,'0')), trunc(nvl(hl_prom_cnt,'0'))::text,'(NULL)') as hl_prom_cnt
, NVL2(trunc(nvl(ind_prom_cnt,'0')), trunc(nvl(ind_prom_cnt,'0'))::text,'(NULL)') as ind_prom_cnt
, NVL2(trunc(nvl(non_prod_sla_prom_cnt,'0')), trunc(nvl(non_prod_sla_prom_cnt,'0'))::text,'(NULL)') as non_prod_sla_prom_cnt
, NVL2(trunc(nvl(fr_sla_dm_prom_cnt,'0')), trunc(nvl(fr_sla_dm_prom_cnt,'0'))::text,'(NULL)') as fr_sla_dm_prom_cnt
, NVL2(trunc(nvl(fr_sla_prom_cnt,'0')), trunc(nvl(fr_sla_prom_cnt,'0'))::text,'(NULL)') as fr_sla_prom_cnt
, NVL2(trunc(nvl(fr_sla_tm_prom_cnt,'0')), trunc(nvl(fr_sla_tm_prom_cnt,'0'))::text,'(NULL)') as fr_sla_tm_prom_cnt
, NVL2(trunc(nvl(prod_slo_prom_cnt,'0')), trunc(nvl(prod_slo_prom_cnt,'0'))::text,'(NULL)') as prod_slo_prom_cnt
, NVL2(trunc(nvl(cr_slo_prom_cnt,'0')), trunc(nvl(cr_slo_prom_cnt,'0'))::text,'(NULL)') as cr_slo_prom_cnt
, NVL2(trunc(nvl(cro_slo_prom_cnt,'0')), trunc(nvl(cro_slo_prom_cnt,'0'))::text,'(NULL)') as cro_slo_prom_cnt
, NVL2(trunc(nvl(cr_slo_dm_prom_cnt,'0')), trunc(nvl(cr_slo_dm_prom_cnt,'0'))::text,'(NULL)') as cr_slo_dm_prom_cnt
, NVL2(trunc(nvl(cro_slo_dm_prom_cnt,'0')), trunc(nvl(cro_slo_dm_prom_cnt,'0'))::text,'(NULL)') as cro_slo_dm_prom_cnt
, NVL2(trunc(nvl(cro_slo_em_prom_cnt,'0')), trunc(nvl(cro_slo_em_prom_cnt,'0'))::text,'(NULL)') as cro_slo_em_prom_cnt
, NVL2(trunc(nvl(hl_slo_prom_cnt,'0')), trunc(nvl(hl_slo_prom_cnt,'0'))::text,'(NULL)') as hl_slo_prom_cnt
, NVL2(trunc(nvl(offline_slo_prom_cnt,'0')), trunc(nvl(offline_slo_prom_cnt,'0'))::text,'(NULL)') as offline_slo_prom_cnt
, NVL2(trunc(nvl(online_slo_prom_cnt,'0')), trunc(nvl(online_slo_prom_cnt,'0'))::text,'(NULL)') as online_slo_prom_cnt
, NVL2(trunc(nvl(fr_tm_prom_cnt,'0')), trunc(nvl(fr_tm_prom_cnt,'0'))::text,'(NULL)') as fr_tm_prom_cnt

/*---END---*/
from prod.agg_promotion_summary T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID 
--where exists (select null from cr_temp.GTT_MT_RESP_SUMMARY_0 mt where mt.cdh_id = T_MAIN.individual_id )
;

------------------------------------------------------------------------TABLE CREATION END\												
												
;
drop table if exists  cr_temp.diff_mt_promo_summary_2;
CREATE TABLE cr_temp.diff_mt_promo_summary_2 AS
with hash_diff as (

select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID     , cr_lst_prom_dt, crsch_em_lst_prom_dt, hl_lst_prom_dt, fr_tm_lst_prom_dt, abq_lst_keycode, adv_em_lst_prom_dt, adv_em_promo_cnt, adv_sla_em_promo_cnt, prod_lst_prom_dt, non_prod_lst_prom_dt, cro_lst_prom_dt, prod_dm_lst_prom_dt, cr_dm_lst_prom_dt, cro_dm_lst_prom_dt, fr_dm_lst_prom_dt, hl_dm_lst_prom_dt, ind_dm_lst_prom_dt, prod_em_lst_prom_dt, cr_em_lst_prom_dt, cro_em_lst_prom_dt, hl_em_lst_prom_dt, ind_em_lst_prom_dt, fr_lst_prom_dt, ind_lst_prom_dt, prod_prom_cnt, non_prod_prom_cnt, cr_prom_cnt, cro_prom_cnt, prod_dm_prom_cnt, cr_dm_prom_cnt, cro_dm_prom_cnt, fr_dm_prom_cnt, hl_dm_prom_cnt, prod_em_prom_cnt, cr_em_prom_cnt, cro_em_prom_cnt, crsch_em_prom_cnt, fr_prom_cnt, cr_dnr_prom_cnt, cro_dnr_prom_cnt, hl_prom_cnt, ind_prom_cnt, non_prod_sla_prom_cnt, fr_sla_dm_prom_cnt, fr_sla_prom_cnt, fr_sla_tm_prom_cnt, prod_slo_prom_cnt, cr_slo_prom_cnt, cro_slo_prom_cnt, cr_slo_dm_prom_cnt, cro_slo_dm_prom_cnt, cro_slo_em_prom_cnt, hl_slo_prom_cnt, offline_slo_prom_cnt, online_slo_prom_cnt, fr_tm_prom_cnt
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL , cr_lst_prom_dt, crsch_em_lst_prom_dt, hl_lst_prom_dt, fr_tm_lst_prom_dt, abq_lst_keycode, adv_em_lst_prom_dt, adv_em_promo_cnt, adv_sla_em_promo_cnt, prod_lst_prom_dt, non_prod_lst_prom_dt, cro_lst_prom_dt, prod_dm_lst_prom_dt, cr_dm_lst_prom_dt, cro_dm_lst_prom_dt, fr_dm_lst_prom_dt, hl_dm_lst_prom_dt, ind_dm_lst_prom_dt, prod_em_lst_prom_dt, cr_em_lst_prom_dt, cro_em_lst_prom_dt, hl_em_lst_prom_dt, ind_em_lst_prom_dt, fr_lst_prom_dt, ind_lst_prom_dt, prod_prom_cnt, non_prod_prom_cnt, cr_prom_cnt, cro_prom_cnt, prod_dm_prom_cnt, cr_dm_prom_cnt, cro_dm_prom_cnt, fr_dm_prom_cnt, hl_dm_prom_cnt, prod_em_prom_cnt, cr_em_prom_cnt, cro_em_prom_cnt, crsch_em_prom_cnt, fr_prom_cnt, cr_dnr_prom_cnt, cro_dnr_prom_cnt, hl_prom_cnt, ind_prom_cnt, non_prod_sla_prom_cnt, fr_sla_dm_prom_cnt, fr_sla_prom_cnt, fr_sla_tm_prom_cnt, prod_slo_prom_cnt, cr_slo_prom_cnt, cro_slo_prom_cnt, cr_slo_dm_prom_cnt, cro_slo_dm_prom_cnt, cro_slo_em_prom_cnt, hl_slo_prom_cnt, offline_slo_prom_cnt, online_slo_prom_cnt, fr_tm_prom_cnt
from cr_temp.GTT_MT_PROMO_SUMMARY_2
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL , cr_lst_prom_dt, crsch_em_lst_prom_dt, hl_lst_prom_dt, fr_tm_lst_prom_dt, abq_lst_keycode, adv_em_lst_prom_dt, adv_em_promo_cnt, adv_sla_em_promo_cnt, prod_lst_prom_dt, non_prod_lst_prom_dt, cro_lst_prom_dt, prod_dm_lst_prom_dt, cr_dm_lst_prom_dt, cro_dm_lst_prom_dt, fr_dm_lst_prom_dt, hl_dm_lst_prom_dt, ind_dm_lst_prom_dt, prod_em_lst_prom_dt, cr_em_lst_prom_dt, cro_em_lst_prom_dt, hl_em_lst_prom_dt, ind_em_lst_prom_dt, fr_lst_prom_dt, ind_lst_prom_dt, prod_prom_cnt, non_prod_prom_cnt, cr_prom_cnt, cro_prom_cnt, prod_dm_prom_cnt, cr_dm_prom_cnt, cro_dm_prom_cnt, fr_dm_prom_cnt, hl_dm_prom_cnt, prod_em_prom_cnt, cr_em_prom_cnt, cro_em_prom_cnt, crsch_em_prom_cnt, fr_prom_cnt, cr_dnr_prom_cnt, cro_dnr_prom_cnt, hl_prom_cnt, ind_prom_cnt, non_prod_sla_prom_cnt, fr_sla_dm_prom_cnt, fr_sla_prom_cnt, fr_sla_tm_prom_cnt, prod_slo_prom_cnt, cr_slo_prom_cnt, cro_slo_prom_cnt, cr_slo_dm_prom_cnt, cro_slo_dm_prom_cnt, cro_slo_em_prom_cnt, hl_slo_prom_cnt, offline_slo_prom_cnt, online_slo_prom_cnt, fr_tm_prom_cnt
from cr_temp.gtt_agg_promotion_summary_ac
)

group by ICD_ID, CDH_ID                                                              , cr_lst_prom_dt, crsch_em_lst_prom_dt, hl_lst_prom_dt, fr_tm_lst_prom_dt, abq_lst_keycode, adv_em_lst_prom_dt, adv_em_promo_cnt, adv_sla_em_promo_cnt, prod_lst_prom_dt, non_prod_lst_prom_dt, cro_lst_prom_dt, prod_dm_lst_prom_dt, cr_dm_lst_prom_dt, cro_dm_lst_prom_dt, fr_dm_lst_prom_dt, hl_dm_lst_prom_dt, ind_dm_lst_prom_dt, prod_em_lst_prom_dt, cr_em_lst_prom_dt, cro_em_lst_prom_dt, hl_em_lst_prom_dt, ind_em_lst_prom_dt, fr_lst_prom_dt, ind_lst_prom_dt, prod_prom_cnt, non_prod_prom_cnt, cr_prom_cnt, cro_prom_cnt, prod_dm_prom_cnt, cr_dm_prom_cnt, cro_dm_prom_cnt, fr_dm_prom_cnt, hl_dm_prom_cnt, prod_em_prom_cnt, cr_em_prom_cnt, cro_em_prom_cnt, crsch_em_prom_cnt, fr_prom_cnt, cr_dnr_prom_cnt, cro_dnr_prom_cnt, hl_prom_cnt, ind_prom_cnt, non_prod_sla_prom_cnt, fr_sla_dm_prom_cnt, fr_sla_prom_cnt, fr_sla_tm_prom_cnt, prod_slo_prom_cnt, cr_slo_prom_cnt, cro_slo_prom_cnt, cr_slo_dm_prom_cnt, cro_slo_dm_prom_cnt, cro_slo_em_prom_cnt, hl_slo_prom_cnt, offline_slo_prom_cnt, online_slo_prom_cnt, fr_tm_prom_cnt
having COUNT(ICD_FL)<>COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select * from hash_diff hd
-- where exists (select null from xxxxxxxxxxxxxxxxxxxxxxxx fullmatch where hd.cdh_id = fullmatch.cdh_id)
;
select to_char(count(distinct icd_id),'FM999,999,999,990') as unique_icd_id, to_char(count(distinct cdh_id),'FM999,999,999,990') as unique_cdh_id
, to_char(count(*),'FM999,999,999,990') as rows_total_count, to_char(count(*)/2,'FM999,999,999,990') as rowshalf_total_count
, to_char((select count(*) from cr_temp.GTT_MT_PROMO_SUMMARY_2),'FM999,999,999,990') as total_ICD
, to_char((select count(*) from cr_temp.gtt_agg_promotion_summary_ac),'FM999,999,999,990')  as total_CDH 
from cr_temp.diff_mt_promo_summary_2

;
drop table if exists cr_temp.comp_diff_mt_promo_summary_2;
CREATE TABLE cr_temp.comp_diff_mt_promo_summary_2 AS
with icd as (select * from cr_temp.diff_mt_promo_summary_2 where 1=1 and cnt_icd_fl = 1)
,    cdh as (select * from cr_temp.diff_mt_promo_summary_2 where 1=1 and cnt_cdh_fl = 1)
select icd.cnt_icd_fl||cdh.cnt_cdh_fl as match_fl, nvl(icd.icd_id,cdh.icd_id) as icd_id ,cdh.cdh_id
, case when icd.cr_lst_prom_dt = cdh.cr_lst_prom_dt then '' else icd.cr_lst_prom_dt||'<>'||cdh.cr_lst_prom_dt /*=*/end cr_lst_prom_dt
, case when icd.crsch_em_lst_prom_dt = cdh.crsch_em_lst_prom_dt then '' else icd.crsch_em_lst_prom_dt||'<>'||cdh.crsch_em_lst_prom_dt /*=*/end crsch_em_lst_prom_dt
, case when icd.hl_lst_prom_dt = cdh.hl_lst_prom_dt then '' else icd.hl_lst_prom_dt||'<>'||cdh.hl_lst_prom_dt /*=*/end hl_lst_prom_dt
, case when icd.fr_tm_lst_prom_dt = cdh.fr_tm_lst_prom_dt then '' else icd.fr_tm_lst_prom_dt||'<>'||cdh.fr_tm_lst_prom_dt /*=*/end fr_tm_lst_prom_dt
, case when icd.abq_lst_keycode = cdh.abq_lst_keycode then '' else icd.abq_lst_keycode||'<>'||cdh.abq_lst_keycode /*=*/end abq_lst_keycode
, case when icd.adv_em_lst_prom_dt = cdh.adv_em_lst_prom_dt then '' else icd.adv_em_lst_prom_dt||'<>'||cdh.adv_em_lst_prom_dt /*=*/end adv_em_lst_prom_dt
, case when icd.adv_em_promo_cnt = cdh.adv_em_promo_cnt then '' else icd.adv_em_promo_cnt||'<>'||cdh.adv_em_promo_cnt /*=*/end adv_em_promo_cnt
, case when icd.adv_sla_em_promo_cnt = cdh.adv_sla_em_promo_cnt then '' else icd.adv_sla_em_promo_cnt||'<>'||cdh.adv_sla_em_promo_cnt /*=*/end adv_sla_em_promo_cnt
, case when icd.prod_lst_prom_dt = cdh.prod_lst_prom_dt then '' else icd.prod_lst_prom_dt||'<>'||cdh.prod_lst_prom_dt /*=*/end prod_lst_prom_dt
, case when icd.non_prod_lst_prom_dt = cdh.non_prod_lst_prom_dt then '' else icd.non_prod_lst_prom_dt||'<>'||cdh.non_prod_lst_prom_dt /*=*/end non_prod_lst_prom_dt
, case when icd.cro_lst_prom_dt = cdh.cro_lst_prom_dt then '' else icd.cro_lst_prom_dt||'<>'||cdh.cro_lst_prom_dt /*=*/end cro_lst_prom_dt
, case when icd.prod_dm_lst_prom_dt = cdh.prod_dm_lst_prom_dt then '' else icd.prod_dm_lst_prom_dt||'<>'||cdh.prod_dm_lst_prom_dt /*=*/end prod_dm_lst_prom_dt
, case when icd.cr_dm_lst_prom_dt = cdh.cr_dm_lst_prom_dt then '' else icd.cr_dm_lst_prom_dt||'<>'||cdh.cr_dm_lst_prom_dt /*=*/end cr_dm_lst_prom_dt
, case when icd.cro_dm_lst_prom_dt = cdh.cro_dm_lst_prom_dt then '' else icd.cro_dm_lst_prom_dt||'<>'||cdh.cro_dm_lst_prom_dt /*=*/end cro_dm_lst_prom_dt
, case when icd.fr_dm_lst_prom_dt = cdh.fr_dm_lst_prom_dt then '' else icd.fr_dm_lst_prom_dt||'<>'||cdh.fr_dm_lst_prom_dt /*=*/end fr_dm_lst_prom_dt
, case when icd.hl_dm_lst_prom_dt = cdh.hl_dm_lst_prom_dt then '' else icd.hl_dm_lst_prom_dt||'<>'||cdh.hl_dm_lst_prom_dt /*=*/end hl_dm_lst_prom_dt
, case when icd.ind_dm_lst_prom_dt = cdh.ind_dm_lst_prom_dt then '' else icd.ind_dm_lst_prom_dt||'<>'||cdh.ind_dm_lst_prom_dt /*=*/end ind_dm_lst_prom_dt
, case when icd.prod_em_lst_prom_dt = cdh.prod_em_lst_prom_dt then '' else icd.prod_em_lst_prom_dt||'<>'||cdh.prod_em_lst_prom_dt /*=*/end prod_em_lst_prom_dt
, case when icd.cr_em_lst_prom_dt = cdh.cr_em_lst_prom_dt then '' else icd.cr_em_lst_prom_dt||'<>'||cdh.cr_em_lst_prom_dt /*=*/end cr_em_lst_prom_dt
, case when icd.cro_em_lst_prom_dt = cdh.cro_em_lst_prom_dt then '' else icd.cro_em_lst_prom_dt||'<>'||cdh.cro_em_lst_prom_dt /*=*/end cro_em_lst_prom_dt
, case when icd.hl_em_lst_prom_dt = cdh.hl_em_lst_prom_dt then '' else icd.hl_em_lst_prom_dt||'<>'||cdh.hl_em_lst_prom_dt /*=*/end hl_em_lst_prom_dt
, case when icd.ind_em_lst_prom_dt = cdh.ind_em_lst_prom_dt then '' else icd.ind_em_lst_prom_dt||'<>'||cdh.ind_em_lst_prom_dt /*=*/end ind_em_lst_prom_dt
, case when icd.fr_lst_prom_dt = cdh.fr_lst_prom_dt then '' else icd.fr_lst_prom_dt||'<>'||cdh.fr_lst_prom_dt /*=*/end fr_lst_prom_dt
, case when icd.ind_lst_prom_dt = cdh.ind_lst_prom_dt then '' else icd.ind_lst_prom_dt||'<>'||cdh.ind_lst_prom_dt /*=*/end ind_lst_prom_dt
, case when icd.prod_prom_cnt = cdh.prod_prom_cnt then '' else icd.prod_prom_cnt||'<>'||cdh.prod_prom_cnt /*=*/end prod_prom_cnt
, case when icd.non_prod_prom_cnt = cdh.non_prod_prom_cnt then '' else icd.non_prod_prom_cnt||'<>'||cdh.non_prod_prom_cnt /*=*/end non_prod_prom_cnt
, case when icd.cr_prom_cnt = cdh.cr_prom_cnt then '' else icd.cr_prom_cnt||'<>'||cdh.cr_prom_cnt /*=*/end cr_prom_cnt
, case when icd.cro_prom_cnt = cdh.cro_prom_cnt then '' else icd.cro_prom_cnt||'<>'||cdh.cro_prom_cnt /*=*/end cro_prom_cnt
, case when icd.prod_dm_prom_cnt = cdh.prod_dm_prom_cnt then '' else icd.prod_dm_prom_cnt||'<>'||cdh.prod_dm_prom_cnt /*=*/end prod_dm_prom_cnt
, case when icd.cr_dm_prom_cnt = cdh.cr_dm_prom_cnt then '' else icd.cr_dm_prom_cnt||'<>'||cdh.cr_dm_prom_cnt /*=*/end cr_dm_prom_cnt
, case when icd.cro_dm_prom_cnt = cdh.cro_dm_prom_cnt then '' else icd.cro_dm_prom_cnt||'<>'||cdh.cro_dm_prom_cnt /*=*/end cro_dm_prom_cnt
, case when icd.fr_dm_prom_cnt = cdh.fr_dm_prom_cnt then '' else icd.fr_dm_prom_cnt||'<>'||cdh.fr_dm_prom_cnt /*=*/end fr_dm_prom_cnt
, case when icd.hl_dm_prom_cnt = cdh.hl_dm_prom_cnt then '' else icd.hl_dm_prom_cnt||'<>'||cdh.hl_dm_prom_cnt /*=*/end hl_dm_prom_cnt
, case when icd.prod_em_prom_cnt = cdh.prod_em_prom_cnt then '' else icd.prod_em_prom_cnt||'<>'||cdh.prod_em_prom_cnt /*=*/end prod_em_prom_cnt
, case when icd.cr_em_prom_cnt = cdh.cr_em_prom_cnt then '' else icd.cr_em_prom_cnt||'<>'||cdh.cr_em_prom_cnt /*=*/end cr_em_prom_cnt
, case when icd.cro_em_prom_cnt = cdh.cro_em_prom_cnt then '' else icd.cro_em_prom_cnt||'<>'||cdh.cro_em_prom_cnt /*=*/end cro_em_prom_cnt
, case when icd.crsch_em_prom_cnt = cdh.crsch_em_prom_cnt then '' else icd.crsch_em_prom_cnt||'<>'||cdh.crsch_em_prom_cnt /*=*/end crsch_em_prom_cnt
, case when icd.fr_prom_cnt = cdh.fr_prom_cnt then '' else icd.fr_prom_cnt||'<>'||cdh.fr_prom_cnt /*=*/end fr_prom_cnt
, case when icd.cr_dnr_prom_cnt = cdh.cr_dnr_prom_cnt then '' else icd.cr_dnr_prom_cnt||'<>'||cdh.cr_dnr_prom_cnt /*=*/end cr_dnr_prom_cnt
, case when icd.cro_dnr_prom_cnt = cdh.cro_dnr_prom_cnt then '' else icd.cro_dnr_prom_cnt||'<>'||cdh.cro_dnr_prom_cnt /*=*/end cro_dnr_prom_cnt
, case when icd.hl_prom_cnt = cdh.hl_prom_cnt then '' else icd.hl_prom_cnt||'<>'||cdh.hl_prom_cnt /*=*/end hl_prom_cnt
, case when icd.ind_prom_cnt = cdh.ind_prom_cnt then '' else icd.ind_prom_cnt||'<>'||cdh.ind_prom_cnt /*=*/end ind_prom_cnt
, case when icd.non_prod_sla_prom_cnt = cdh.non_prod_sla_prom_cnt then '' else icd.non_prod_sla_prom_cnt||'<>'||cdh.non_prod_sla_prom_cnt /*=*/end non_prod_sla_prom_cnt
, case when icd.fr_sla_dm_prom_cnt = cdh.fr_sla_dm_prom_cnt then '' else icd.fr_sla_dm_prom_cnt||'<>'||cdh.fr_sla_dm_prom_cnt /*=*/end fr_sla_dm_prom_cnt
, case when icd.fr_sla_prom_cnt = cdh.fr_sla_prom_cnt then '' else icd.fr_sla_prom_cnt||'<>'||cdh.fr_sla_prom_cnt /*=*/end fr_sla_prom_cnt
, case when icd.fr_sla_tm_prom_cnt = cdh.fr_sla_tm_prom_cnt then '' else icd.fr_sla_tm_prom_cnt||'<>'||cdh.fr_sla_tm_prom_cnt /*=*/end fr_sla_tm_prom_cnt
, case when icd.prod_slo_prom_cnt = cdh.prod_slo_prom_cnt then '' else icd.prod_slo_prom_cnt||'<>'||cdh.prod_slo_prom_cnt /*=*/end prod_slo_prom_cnt
, case when icd.cr_slo_prom_cnt = cdh.cr_slo_prom_cnt then '' else icd.cr_slo_prom_cnt||'<>'||cdh.cr_slo_prom_cnt /*=*/end cr_slo_prom_cnt
, case when icd.cro_slo_prom_cnt = cdh.cro_slo_prom_cnt then '' else icd.cro_slo_prom_cnt||'<>'||cdh.cro_slo_prom_cnt /*=*/end cro_slo_prom_cnt
, case when icd.cr_slo_dm_prom_cnt = cdh.cr_slo_dm_prom_cnt then '' else icd.cr_slo_dm_prom_cnt||'<>'||cdh.cr_slo_dm_prom_cnt /*=*/end cr_slo_dm_prom_cnt
, case when icd.cro_slo_dm_prom_cnt = cdh.cro_slo_dm_prom_cnt then '' else icd.cro_slo_dm_prom_cnt||'<>'||cdh.cro_slo_dm_prom_cnt /*=*/end cro_slo_dm_prom_cnt
, case when icd.cro_slo_em_prom_cnt = cdh.cro_slo_em_prom_cnt then '' else icd.cro_slo_em_prom_cnt||'<>'||cdh.cro_slo_em_prom_cnt /*=*/end cro_slo_em_prom_cnt
, case when icd.hl_slo_prom_cnt = cdh.hl_slo_prom_cnt then '' else icd.hl_slo_prom_cnt||'<>'||cdh.hl_slo_prom_cnt /*=*/end hl_slo_prom_cnt
, case when icd.offline_slo_prom_cnt = cdh.offline_slo_prom_cnt then '' else icd.offline_slo_prom_cnt||'<>'||cdh.offline_slo_prom_cnt /*=*/end offline_slo_prom_cnt
, case when icd.online_slo_prom_cnt = cdh.online_slo_prom_cnt then '' else icd.online_slo_prom_cnt||'<>'||cdh.online_slo_prom_cnt /*=*/end online_slo_prom_cnt
, case when icd.fr_tm_prom_cnt = cdh.fr_tm_prom_cnt then '' else icd.fr_tm_prom_cnt||'<>'||cdh.fr_tm_prom_cnt /*=*/end fr_tm_prom_cnt


/*, '||                    ICD                    >>' as sep1/*, icd.* */*/
/*, '||                    CDH                    >>' as sep2/*, cdh.**/*/

from  icd full join cdh
on icd.icd_id = cdh.icd_id and icd.cdh_id = cdh.cdh_id												
/*additional joins for PK*/
;
select 'MT_PROMO_SUMMARY_2' as icd_table_name, 'agg_promotion_summary_acx' as cdh_table_name
, to_char(sum( case when cr_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cr_lst_prom_dt
, to_char(sum( case when crsch_em_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/crsch_em_lst_prom_dt
, to_char(sum( case when hl_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/hl_lst_prom_dt
, to_char(sum( case when fr_tm_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_tm_lst_prom_dt
, to_char(sum( case when abq_lst_keycode = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/abq_lst_keycode
, to_char(sum( case when adv_em_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/adv_em_lst_prom_dt
, to_char(sum( case when adv_em_promo_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/adv_em_promo_cnt
, to_char(sum( case when adv_sla_em_promo_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/adv_sla_em_promo_cnt
, to_char(sum( case when prod_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/prod_lst_prom_dt
, to_char(sum( case when non_prod_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/non_prod_lst_prom_dt
, to_char(sum( case when cro_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cro_lst_prom_dt
, to_char(sum( case when prod_dm_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/prod_dm_lst_prom_dt
, to_char(sum( case when cr_dm_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cr_dm_lst_prom_dt
, to_char(sum( case when cro_dm_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cro_dm_lst_prom_dt
, to_char(sum( case when fr_dm_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_dm_lst_prom_dt
, to_char(sum( case when hl_dm_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/hl_dm_lst_prom_dt
, to_char(sum( case when ind_dm_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/ind_dm_lst_prom_dt
, to_char(sum( case when prod_em_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/prod_em_lst_prom_dt
, to_char(sum( case when cr_em_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cr_em_lst_prom_dt
, to_char(sum( case when cro_em_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cro_em_lst_prom_dt
, to_char(sum( case when hl_em_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/hl_em_lst_prom_dt
, to_char(sum( case when ind_em_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/ind_em_lst_prom_dt
, to_char(sum( case when fr_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_lst_prom_dt
, to_char(sum( case when ind_lst_prom_dt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/ind_lst_prom_dt
, to_char(sum( case when prod_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/prod_prom_cnt
, to_char(sum( case when non_prod_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/non_prod_prom_cnt
, to_char(sum( case when cr_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cr_prom_cnt
, to_char(sum( case when cro_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cro_prom_cnt
, to_char(sum( case when prod_dm_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/prod_dm_prom_cnt
, to_char(sum( case when cr_dm_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cr_dm_prom_cnt
, to_char(sum( case when cro_dm_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cro_dm_prom_cnt
, to_char(sum( case when fr_dm_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_dm_prom_cnt
, to_char(sum( case when hl_dm_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/hl_dm_prom_cnt
, to_char(sum( case when prod_em_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/prod_em_prom_cnt
, to_char(sum( case when cr_em_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cr_em_prom_cnt
, to_char(sum( case when cro_em_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cro_em_prom_cnt
, to_char(sum( case when crsch_em_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/crsch_em_prom_cnt
, to_char(sum( case when fr_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_prom_cnt
, to_char(sum( case when cr_dnr_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cr_dnr_prom_cnt
, to_char(sum( case when cro_dnr_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cro_dnr_prom_cnt
, to_char(sum( case when hl_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/hl_prom_cnt
, to_char(sum( case when ind_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/ind_prom_cnt
, to_char(sum( case when non_prod_sla_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/non_prod_sla_prom_cnt
, to_char(sum( case when fr_sla_dm_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_sla_dm_prom_cnt
, to_char(sum( case when fr_sla_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_sla_prom_cnt
, to_char(sum( case when fr_sla_tm_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_sla_tm_prom_cnt
, to_char(sum( case when prod_slo_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/prod_slo_prom_cnt
, to_char(sum( case when cr_slo_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cr_slo_prom_cnt
, to_char(sum( case when cro_slo_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cro_slo_prom_cnt
, to_char(sum( case when cr_slo_dm_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cr_slo_dm_prom_cnt
, to_char(sum( case when cro_slo_dm_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cro_slo_dm_prom_cnt
, to_char(sum( case when cro_slo_em_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/cro_slo_em_prom_cnt
, to_char(sum( case when hl_slo_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/hl_slo_prom_cnt
, to_char(sum( case when offline_slo_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/offline_slo_prom_cnt
, to_char(sum( case when online_slo_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/online_slo_prom_cnt
, to_char(sum( case when fr_tm_prom_cnt = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*=*/fr_tm_prom_cnt

from cr_temp.comp_diff_mt_promo_summary_2
;
select top 1000 * 

,  cr_lst_prom_dt
,  crsch_em_lst_prom_dt
,  hl_lst_prom_dt
,  fr_tm_lst_prom_dt
,  abq_lst_keycode
,  adv_em_lst_prom_dt
,  adv_em_promo_cnt
,  adv_sla_em_promo_cnt
,  prod_lst_prom_dt
,  non_prod_lst_prom_dt
,  cro_lst_prom_dt
,  prod_dm_lst_prom_dt
,  cr_dm_lst_prom_dt
,  cro_dm_lst_prom_dt
,  fr_dm_lst_prom_dt
,  hl_dm_lst_prom_dt
,  ind_dm_lst_prom_dt
,  prod_em_lst_prom_dt
,  cr_em_lst_prom_dt
,  cro_em_lst_prom_dt
,  hl_em_lst_prom_dt
,  ind_em_lst_prom_dt
,  fr_lst_prom_dt
,  ind_lst_prom_dt
,  prod_prom_cnt
,  non_prod_prom_cnt
,  cr_prom_cnt
,  cro_prom_cnt
,  prod_dm_prom_cnt
,  cr_dm_prom_cnt
,  cro_dm_prom_cnt
,  fr_dm_prom_cnt
,  hl_dm_prom_cnt
,  prod_em_prom_cnt
,  cr_em_prom_cnt
,  cro_em_prom_cnt
,  crsch_em_prom_cnt
,  fr_prom_cnt
,  cr_dnr_prom_cnt
,  cro_dnr_prom_cnt
,  hl_prom_cnt
,  ind_prom_cnt
,  non_prod_sla_prom_cnt
,  fr_sla_dm_prom_cnt
,  fr_sla_prom_cnt
,  fr_sla_tm_prom_cnt
,  prod_slo_prom_cnt
,  cr_slo_prom_cnt
,  cro_slo_prom_cnt
,  cr_slo_dm_prom_cnt
,  cro_slo_dm_prom_cnt
,  cro_slo_em_prom_cnt
,  hl_slo_prom_cnt
,  offline_slo_prom_cnt
,  online_slo_prom_cnt
,  fr_tm_prom_cnt

from cr_temp.comp_diff_mt_promo_summary_2
 where 1=1 and ( 1=2
-- or cr_lst_prom_dt <> ''
-- or crsch_em_lst_prom_dt <> ''
-- or hl_lst_prom_dt <> ''
-- or fr_tm_lst_prom_dt <> ''
-- or abq_lst_keycode <> ''
-- or adv_em_lst_prom_dt <> ''
-- or adv_em_promo_cnt <> ''
-- or adv_sla_em_promo_cnt <> ''
-- or prod_lst_prom_dt <> ''
-- or non_prod_lst_prom_dt <> ''
-- or cro_lst_prom_dt <> ''
-- or prod_dm_lst_prom_dt <> ''
-- or cr_dm_lst_prom_dt <> ''
-- or cro_dm_lst_prom_dt <> ''
-- or fr_dm_lst_prom_dt <> ''
-- or hl_dm_lst_prom_dt <> ''
-- or ind_dm_lst_prom_dt <> ''
-- or prod_em_lst_prom_dt <> ''
-- or cr_em_lst_prom_dt <> ''
-- or cro_em_lst_prom_dt <> ''
-- or hl_em_lst_prom_dt <> ''
-- or ind_em_lst_prom_dt <> ''
-- or fr_lst_prom_dt <> ''
-- or ind_lst_prom_dt <> ''
-- or prod_prom_cnt <> ''
-- or non_prod_prom_cnt <> ''
-- or cr_prom_cnt <> ''
-- or cro_prom_cnt <> ''
-- or prod_dm_prom_cnt <> ''
-- or cr_dm_prom_cnt <> ''
-- or cro_dm_prom_cnt <> ''
-- or fr_dm_prom_cnt <> ''
-- or hl_dm_prom_cnt <> ''
-- or prod_em_prom_cnt <> ''
-- or cr_em_prom_cnt <> ''
-- or cro_em_prom_cnt <> ''
-- or crsch_em_prom_cnt <> ''
-- or fr_prom_cnt <> ''
-- or cr_dnr_prom_cnt <> ''
-- or cro_dnr_prom_cnt <> ''
-- or hl_prom_cnt <> ''
-- or ind_prom_cnt <> ''
-- or non_prod_sla_prom_cnt <> ''
-- or fr_sla_dm_prom_cnt <> ''
-- or fr_sla_prom_cnt <> ''
-- or fr_sla_tm_prom_cnt <> ''
-- or prod_slo_prom_cnt <> ''
-- or cr_slo_prom_cnt <> ''
-- or cro_slo_prom_cnt <> ''
-- or cr_slo_dm_prom_cnt <> ''
-- or cro_slo_dm_prom_cnt <> ''
-- or cro_slo_em_prom_cnt <> ''
-- or hl_slo_prom_cnt <> ''
-- or offline_slo_prom_cnt <> ''
-- or online_slo_prom_cnt <> ''
-- or fr_tm_prom_cnt <> ''

)

;
------------------------------------------------------------------------RUN ALL ABOVE
;
select 213805-213070

;
select * from   cr_temp.GTT_MT_PROMO_SUMMARY_2  where icd_id = 00000000000000000000
;
select * from   cr_temp.gtt_agg_promotion_summary_ac  where cdh_id = 00000000000000000000











												
