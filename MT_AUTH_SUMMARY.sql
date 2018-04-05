;
create table cr_temp.GTT_MT_AUTH_SUMMARY_0 as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(dcd_flg, dcd_flg::text,'(NULL)') as dcd_flg
, NVL2(fr_mail_freq_cd, fr_mail_freq_cd::text,'(NULL)') as fr_mail_freq_cd
, NVL2(prison_flg, prison_flg::text,'(NULL)') as prison_flg
, NVL2(fr_non_ack_flg, fr_non_ack_flg::text,'(NULL)') as fr_non_ack_flg
, NVL2(non_prom_auto_rnw_flg, non_prom_auto_rnw_flg::text,'(NULL)') as non_prom_auto_rnw_flg
, NVL2(non_prom_auto_rnw_gft_flg, non_prom_auto_rnw_gft_flg::text,'(NULL)') as non_prom_auto_rnw_gft_flg
, NVL2(abq_non_prom_dm_flg, abq_non_prom_dm_flg::text,'(NULL)') as abq_non_prom_dm_flg
, NVL2(abq_non_prom_em_flg, abq_non_prom_em_flg::text,'(NULL)') as abq_non_prom_em_flg
, NVL2(adv_non_prom_em_flg, adv_non_prom_em_flg::text,'(NULL)') as adv_non_prom_em_flg
, NVL2(aps_non_prom_em_flg, aps_non_prom_em_flg::text,'(NULL)') as aps_non_prom_em_flg
, NVL2(cr_non_prom_em_flg, cr_non_prom_em_flg::text,'(NULL)') as cr_non_prom_em_flg
, NVL2(crmg_non_prom_em_flg, crmg_non_prom_em_flg::text,'(NULL)') as crmg_non_prom_em_flg
, NVL2(cro_non_prom_em_flg, cro_non_prom_em_flg::text,'(NULL)') as cro_non_prom_em_flg
, NVL2(crsch_non_prom_em_flg, crsch_non_prom_em_flg::text,'(NULL)') as crsch_non_prom_em_flg
, NVL2(fr_non_prom_em_flg, fr_non_prom_em_flg::text,'(NULL)') as fr_non_prom_em_flg
, NVL2(non_prom_em_flg, non_prom_em_flg::text,'(NULL)') as non_prom_em_flg
, NVL2(hl_non_prom_em_flg, hl_non_prom_em_flg::text,'(NULL)') as hl_non_prom_em_flg
, NVL2(ma_non_prom_em_flg, ma_non_prom_em_flg::text,'(NULL)') as ma_non_prom_em_flg
, NVL2(ncbk_non_prom_em_flg, ncbk_non_prom_em_flg::text,'(NULL)') as ncbk_non_prom_em_flg
, NVL2(ucbk_non_prom_em_flg, ucbk_non_prom_em_flg::text,'(NULL)') as ucbk_non_prom_em_flg
, NVL2(non_prom_list_rent_flg, non_prom_list_rent_flg::text,'(NULL)') as non_prom_list_rent_flg
, NVL2(non_prom_pander_flg, non_prom_pander_flg::text,'(NULL)') as non_prom_pander_flg
, NVL2(non_prom_rfl_flg, non_prom_rfl_flg::text,'(NULL)') as non_prom_rfl_flg
, NVL2(non_prom_tm_flg, non_prom_tm_flg::text,'(NULL)') as non_prom_tm_flg
, NVL2(fr_non_prom_dm_flg, fr_non_prom_dm_flg::text,'(NULL)') as fr_non_prom_dm_flg
, NVL2(fr_non_prom_tm_flg, fr_non_prom_tm_flg::text,'(NULL)') as fr_non_prom_tm_flg
, NVL2(fr_non_prom_rnw_flg, fr_non_prom_rnw_flg::text,'(NULL)') as fr_non_prom_rnw_flg
, NVL2(fr_non_prom_tof_flg, fr_non_prom_tof_flg::text,'(NULL)') as fr_non_prom_tof_flg
, NVL2(fr_non_prom_tm_rem_flg, fr_non_prom_tm_rem_flg::text,'(NULL)') as fr_non_prom_tm_rem_flg
, NVL2(fr_non_prom_cu_flg, fr_non_prom_cu_flg::text,'(NULL)') as fr_non_prom_cu_flg
, NVL2(fr_non_prom_prem_flg, fr_non_prom_prem_flg::text,'(NULL)') as fr_non_prom_prem_flg
, NVL2(fr_rnw_mail_freq_cd, fr_rnw_mail_freq_cd::text,'(NULL)') as fr_rnw_mail_freq_cd
, NVL2(fr_tm_freq_cd, fr_tm_freq_cd::text,'(NULL)') as fr_tm_freq_cd
, NVL2(fr_cu_ins_flg, fr_cu_ins_flg::text,'(NULL)') as fr_cu_ins_flg
, NVL2(cu_board_flg, cu_board_flg::text,'(NULL)') as cu_board_flg
, NVL2(dma_non_prom_dm_flg, dma_non_prom_dm_flg::text,'(NULL)') as dma_non_prom_dm_flg
, NVL2(dma_non_prom_tm_flg, dma_non_prom_tm_flg::text,'(NULL)') as dma_non_prom_tm_flg
, NVL2(dma_non_prom_em_flg, dma_non_prom_em_flg::text,'(NULL)') as dma_non_prom_em_flg
, NVL2(acx_non_prom_dm_flg, acx_non_prom_dm_flg::text,'(NULL)') as acx_non_prom_dm_flg
, NVL2(acx_non_prom_tm_flg, acx_non_prom_tm_flg::text,'(NULL)') as acx_non_prom_tm_flg
, NVL2(acx_non_prom_em_flg, acx_non_prom_em_flg::text,'(NULL)') as acx_non_prom_em_flg
, NVL2(bk_non_prom_flg, bk_non_prom_flg::text,'(NULL)') as bk_non_prom_flg
, NVL2(non_prom_gft_flg, non_prom_gft_flg::text,'(NULL)') as non_prom_gft_flg
, NVL2(shm_non_prom_em_flg, shm_non_prom_em_flg::text,'(NULL)') as shm_non_prom_em_flg
, NVL2(carp_non_prom_em_flg, carp_non_prom_em_flg::text,'(NULL)') as carp_non_prom_em_flg
, NVL2(non_prom_cu_fr_rfl_flg, non_prom_cu_fr_rfl_flg::text,'(NULL)') as non_prom_cu_fr_rfl_flg
, NVL2(ib_deceased_flg, ib_deceased_flg::text,'(NULL)') as ib_deceased_flg
, NVL2(test_user_flg, test_user_flg::text,'(NULL)') as test_user_flg
, NVL2(fr_non_share_flg, fr_non_share_flg::text,'(NULL)') as fr_non_share_flg
, NVL2(adv_non_prom_news_em_flg, adv_non_prom_news_em_flg::text,'(NULL)') as adv_non_prom_news_em_flg
, NVL2(adv_non_prom_aa_em_flg, adv_non_prom_aa_em_flg::text,'(NULL)') as adv_non_prom_aa_em_flg
, NVL2(adv_non_prom_dm_flg, adv_non_prom_dm_flg::text,'(NULL)') as adv_non_prom_dm_flg
, NVL2(advfr_non_prom_em_flg, advfr_non_prom_em_flg::text,'(NULL)') as advfr_non_prom_em_flg

/*---END---*/

from cr_temp.MT_AUTH_SUMMARY_0 T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;

CREATE TABLE cr_temp.gtt_agg_uth_summary AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(dcd_flg, dcd_flg::text,'(NULL)') as dcd_flg
, NVL2(fr_mail_freq_cd, fr_mail_freq_cd::text,'(NULL)') as fr_mail_freq_cd
, NVL2(prison_flg, prison_flg::text,'(NULL)') as prison_flg
, NVL2(fr_non_ack_flg, fr_non_ack_flg::text,'(NULL)') as fr_non_ack_flg
, NVL2(non_prom_auto_rnw_flg, non_prom_auto_rnw_flg::text,'(NULL)') as non_prom_auto_rnw_flg
, NVL2(non_prom_auto_rnw_gft_flg, non_prom_auto_rnw_gft_flg::text,'(NULL)') as non_prom_auto_rnw_gft_flg
, NVL2(abq_non_prom_dm_flg, abq_non_prom_dm_flg::text,'(NULL)') as abq_non_prom_dm_flg
, NVL2(abq_non_prom_em_flg, abq_non_prom_em_flg::text,'(NULL)') as abq_non_prom_em_flg
, NVL2(adv_non_prom_em_flg, adv_non_prom_em_flg::text,'(NULL)') as adv_non_prom_em_flg
, NVL2(aps_non_prom_em_flg, aps_non_prom_em_flg::text,'(NULL)') as aps_non_prom_em_flg
, NVL2(cr_non_prom_em_flg, cr_non_prom_em_flg::text,'(NULL)') as cr_non_prom_em_flg
, NVL2(crmg_non_prom_em_flg, crmg_non_prom_em_flg::text,'(NULL)') as crmg_non_prom_em_flg
, NVL2(cro_non_prom_em_flg, cro_non_prom_em_flg::text,'(NULL)') as cro_non_prom_em_flg
, NVL2(crsch_non_prom_em_flg, crsch_non_prom_em_flg::text,'(NULL)') as crsch_non_prom_em_flg
, NVL2(fr_non_prom_em_flg, fr_non_prom_em_flg::text,'(NULL)') as fr_non_prom_em_flg
, NVL2(non_prom_em_flg, non_prom_em_flg::text,'(NULL)') as non_prom_em_flg
, NVL2(hl_non_prom_em_flg, hl_non_prom_em_flg::text,'(NULL)') as hl_non_prom_em_flg
, NVL2(ma_non_prom_em_flg, ma_non_prom_em_flg::text,'(NULL)') as ma_non_prom_em_flg
, NVL2(ncbk_non_prom_em_flg, ncbk_non_prom_em_flg::text,'(NULL)') as ncbk_non_prom_em_flg
, NVL2(ucbk_non_prom_em_flg, ucbk_non_prom_em_flg::text,'(NULL)') as ucbk_non_prom_em_flg
, NVL2(non_prom_list_rent_flg, non_prom_list_rent_flg::text,'(NULL)') as non_prom_list_rent_flg
, NVL2(non_prom_pander_flg, non_prom_pander_flg::text,'(NULL)') as non_prom_pander_flg
, NVL2(non_prom_rfl_flg, non_prom_rfl_flg::text,'(NULL)') as non_prom_rfl_flg
, NVL2(non_prom_tm_flg, non_prom_tm_flg::text,'(NULL)') as non_prom_tm_flg
, NVL2(fr_non_prom_dm_flg, fr_non_prom_dm_flg::text,'(NULL)') as fr_non_prom_dm_flg
, NVL2(fr_non_prom_tm_flg, fr_non_prom_tm_flg::text,'(NULL)') as fr_non_prom_tm_flg
, NVL2(fr_non_prom_rnw_flg, fr_non_prom_rnw_flg::text,'(NULL)') as fr_non_prom_rnw_flg
, NVL2(fr_non_prom_tof_flg, fr_non_prom_tof_flg::text,'(NULL)') as fr_non_prom_tof_flg
, NVL2(fr_non_prom_tm_rem_flg, fr_non_prom_tm_rem_flg::text,'(NULL)') as fr_non_prom_tm_rem_flg
, NVL2(fr_non_prom_cu_flg, fr_non_prom_cu_flg::text,'(NULL)') as fr_non_prom_cu_flg
, NVL2(fr_non_prom_prem_flg, fr_non_prom_prem_flg::text,'(NULL)') as fr_non_prom_prem_flg
, NVL2(fr_rnw_mail_freq_cd, fr_rnw_mail_freq_cd::text,'(NULL)') as fr_rnw_mail_freq_cd
, NVL2(fr_tm_freq_cd, fr_tm_freq_cd::text,'(NULL)') as fr_tm_freq_cd
, NVL2(fr_cu_ins_flg, fr_cu_ins_flg::text,'(NULL)') as fr_cu_ins_flg
, NVL2(cu_board_flg, cu_board_flg::text,'(NULL)') as cu_board_flg
, NVL2(dma_non_prom_dm_flg, dma_non_prom_dm_flg::text,'(NULL)') as dma_non_prom_dm_flg
, NVL2(dma_non_prom_tm_flg, dma_non_prom_tm_flg::text,'(NULL)') as dma_non_prom_tm_flg
, NVL2(dma_non_prom_em_flg, dma_non_prom_em_flg::text,'(NULL)') as dma_non_prom_em_flg
, NVL2(acx_non_prom_dm_flg, acx_non_prom_dm_flg::text,'(NULL)') as acx_non_prom_dm_flg
, NVL2(acx_non_prom_tm_flg, acx_non_prom_tm_flg::text,'(NULL)') as acx_non_prom_tm_flg
, NVL2(acx_non_prom_em_flg, acx_non_prom_em_flg::text,'(NULL)') as acx_non_prom_em_flg
, NVL2(bk_non_prom_flg, bk_non_prom_flg::text,'(NULL)') as bk_non_prom_flg
, NVL2(non_prom_gft_flg, non_prom_gft_flg::text,'(NULL)') as non_prom_gft_flg
, NVL2(shm_non_prom_em_flg, shm_non_prom_em_flg::text,'(NULL)') as shm_non_prom_em_flg
, NVL2(carp_non_prom_em_flg, carp_non_prom_em_flg::text,'(NULL)') as carp_non_prom_em_flg
, NVL2(non_prom_cu_fr_rfl_flg, non_prom_cu_fr_rfl_flg::text,'(NULL)') as non_prom_cu_fr_rfl_flg
, NVL2(ib_deceased_flg, ib_deceased_flg::text,'(NULL)') as ib_deceased_flg
, NVL2(test_user_flg, test_user_flg::text,'(NULL)') as test_user_flg
, NVL2(fr_non_share_flg, fr_non_share_flg::text,'(NULL)') as fr_non_share_flg
, NVL2(adv_non_prom_news_em_flg, adv_non_prom_news_em_flg::text,'(NULL)') as adv_non_prom_news_em_flg
, NVL2(adv_non_prom_aa_em_flg, adv_non_prom_aa_em_flg::text,'(NULL)') as adv_non_prom_aa_em_flg
, NVL2(adv_non_prom_dm_flg, adv_non_prom_dm_flg::text,'(NULL)') as adv_non_prom_dm_flg
, NVL2(advfr_non_prom_em_flg, advfr_non_prom_em_flg::text,'(NULL)') as advfr_non_prom_em_flg

/*---END---*/
from prod.agg_preference_summary T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;



------------------------------------------------------------------------TABLE CREATION END\;
CREATE TABLE cr_temp.diff_mt_auth_summary_0 AS
with hash_diff as (

select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID     /*, individual_id*/, dcd_flg, fr_mail_freq_cd, prison_flg, fr_non_ack_flg, non_prom_auto_rnw_flg, non_prom_auto_rnw_gft_flg, abq_non_prom_dm_flg, abq_non_prom_em_flg, adv_non_prom_em_flg, aps_non_prom_em_flg, cr_non_prom_em_flg, crmg_non_prom_em_flg, cro_non_prom_em_flg, crsch_non_prom_em_flg, fr_non_prom_em_flg, non_prom_em_flg, hl_non_prom_em_flg, ma_non_prom_em_flg, ncbk_non_prom_em_flg, ucbk_non_prom_em_flg, non_prom_list_rent_flg, non_prom_pander_flg, non_prom_rfl_flg, non_prom_tm_flg, fr_non_prom_dm_flg, fr_non_prom_tm_flg, fr_non_prom_rnw_flg, fr_non_prom_tof_flg, fr_non_prom_tm_rem_flg, fr_non_prom_cu_flg, fr_non_prom_prem_flg, fr_rnw_mail_freq_cd, fr_tm_freq_cd, fr_cu_ins_flg, cu_board_flg, dma_non_prom_dm_flg, dma_non_prom_tm_flg, dma_non_prom_em_flg, acx_non_prom_dm_flg, acx_non_prom_tm_flg, acx_non_prom_em_flg, bk_non_prom_flg, non_prom_gft_flg, shm_non_prom_em_flg, carp_non_prom_em_flg, non_prom_cu_fr_rfl_flg, ib_deceased_flg, test_user_flg, fr_non_share_flg, adv_non_prom_news_em_flg, adv_non_prom_aa_em_flg, adv_non_prom_dm_flg, advfr_non_prom_em_flg
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL /*, individual_id*/, dcd_flg, fr_mail_freq_cd, prison_flg, fr_non_ack_flg, non_prom_auto_rnw_flg, non_prom_auto_rnw_gft_flg, abq_non_prom_dm_flg, abq_non_prom_em_flg, adv_non_prom_em_flg, aps_non_prom_em_flg, cr_non_prom_em_flg, crmg_non_prom_em_flg, cro_non_prom_em_flg, crsch_non_prom_em_flg, fr_non_prom_em_flg, non_prom_em_flg, hl_non_prom_em_flg, ma_non_prom_em_flg, ncbk_non_prom_em_flg, ucbk_non_prom_em_flg, non_prom_list_rent_flg, non_prom_pander_flg, non_prom_rfl_flg, non_prom_tm_flg, fr_non_prom_dm_flg, fr_non_prom_tm_flg, fr_non_prom_rnw_flg, fr_non_prom_tof_flg, fr_non_prom_tm_rem_flg, fr_non_prom_cu_flg, fr_non_prom_prem_flg, fr_rnw_mail_freq_cd, fr_tm_freq_cd, fr_cu_ins_flg, cu_board_flg, dma_non_prom_dm_flg, dma_non_prom_tm_flg, dma_non_prom_em_flg, acx_non_prom_dm_flg, acx_non_prom_tm_flg, acx_non_prom_em_flg, bk_non_prom_flg, non_prom_gft_flg, shm_non_prom_em_flg, carp_non_prom_em_flg, non_prom_cu_fr_rfl_flg, ib_deceased_flg, test_user_flg, fr_non_share_flg, adv_non_prom_news_em_flg, adv_non_prom_aa_em_flg, adv_non_prom_dm_flg, advfr_non_prom_em_flg
from cr_temp.GTT_MT_AUTH_SUMMARY_0
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL /*, individual_id*/, dcd_flg, fr_mail_freq_cd, prison_flg, fr_non_ack_flg, non_prom_auto_rnw_flg, non_prom_auto_rnw_gft_flg, abq_non_prom_dm_flg, abq_non_prom_em_flg, adv_non_prom_em_flg, aps_non_prom_em_flg, cr_non_prom_em_flg, crmg_non_prom_em_flg, cro_non_prom_em_flg, crsch_non_prom_em_flg, fr_non_prom_em_flg, non_prom_em_flg, hl_non_prom_em_flg, ma_non_prom_em_flg, ncbk_non_prom_em_flg, ucbk_non_prom_em_flg, non_prom_list_rent_flg, non_prom_pander_flg, non_prom_rfl_flg, non_prom_tm_flg, fr_non_prom_dm_flg, fr_non_prom_tm_flg, fr_non_prom_rnw_flg, fr_non_prom_tof_flg, fr_non_prom_tm_rem_flg, fr_non_prom_cu_flg, fr_non_prom_prem_flg, fr_rnw_mail_freq_cd, fr_tm_freq_cd, fr_cu_ins_flg, cu_board_flg, dma_non_prom_dm_flg, dma_non_prom_tm_flg, dma_non_prom_em_flg, acx_non_prom_dm_flg, acx_non_prom_tm_flg, acx_non_prom_em_flg, bk_non_prom_flg, non_prom_gft_flg, shm_non_prom_em_flg, carp_non_prom_em_flg, non_prom_cu_fr_rfl_flg, ib_deceased_flg, test_user_flg, fr_non_share_flg, adv_non_prom_news_em_flg, adv_non_prom_aa_em_flg, adv_non_prom_dm_flg, advfr_non_prom_em_flg
from cr_temp.gtt_agg_uth_summary
)

group by ICD_ID, CDH_ID                                                              /*, individual_id*/, dcd_flg, fr_mail_freq_cd, prison_flg, fr_non_ack_flg, non_prom_auto_rnw_flg, non_prom_auto_rnw_gft_flg, abq_non_prom_dm_flg, abq_non_prom_em_flg, adv_non_prom_em_flg, aps_non_prom_em_flg, cr_non_prom_em_flg, crmg_non_prom_em_flg, cro_non_prom_em_flg, crsch_non_prom_em_flg, fr_non_prom_em_flg, non_prom_em_flg, hl_non_prom_em_flg, ma_non_prom_em_flg, ncbk_non_prom_em_flg, ucbk_non_prom_em_flg, non_prom_list_rent_flg, non_prom_pander_flg, non_prom_rfl_flg, non_prom_tm_flg, fr_non_prom_dm_flg, fr_non_prom_tm_flg, fr_non_prom_rnw_flg, fr_non_prom_tof_flg, fr_non_prom_tm_rem_flg, fr_non_prom_cu_flg, fr_non_prom_prem_flg, fr_rnw_mail_freq_cd, fr_tm_freq_cd, fr_cu_ins_flg, cu_board_flg, dma_non_prom_dm_flg, dma_non_prom_tm_flg, dma_non_prom_em_flg, acx_non_prom_dm_flg, acx_non_prom_tm_flg, acx_non_prom_em_flg, bk_non_prom_flg, non_prom_gft_flg, shm_non_prom_em_flg, carp_non_prom_em_flg, non_prom_cu_fr_rfl_flg, ib_deceased_flg, test_user_flg, fr_non_share_flg, adv_non_prom_news_em_flg, adv_non_prom_aa_em_flg, adv_non_prom_dm_flg, advfr_non_prom_em_flg
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select * from hash_diff hd
--where exists (select null from cr_temp.fullmatch_id_agg_preference fm where hd.cdh_id = fm.cdh_id)
;

select top 300 * from cr_temp.diff_mt_auth_summary_0
 where 1=1
;


with icd as (select * from cr_temp.diff_mt_auth_summary_0 where 1=1 and cnt_icd_fl = 1)
,    cdh as (select * from cr_temp.diff_mt_auth_summary_0 where 1=1 and cnt_cdh_fl = 1)
select top 300 icd.cnt_icd_fl||cdh.cnt_cdh_fl as match_fl, icd.icd_id ,cdh.cdh_id
--, case when icd.individual_id = cdh.individual_id then '' else icd.individual_id||'<>'||cdh.individual_id /*#*/end individual_id_diff
, case when icd.dcd_flg = cdh.dcd_flg then '' else icd.dcd_flg||'<>'||cdh.dcd_flg /*#*/end dcd_flg_diff
, case when icd.fr_mail_freq_cd = cdh.fr_mail_freq_cd then '' else icd.fr_mail_freq_cd||'<>'||cdh.fr_mail_freq_cd /*#*/end fr_mail_freq_cd_diff
, case when icd.prison_flg = cdh.prison_flg then '' else icd.prison_flg||'<>'||cdh.prison_flg /*#*/end prison_flg_diff
, case when icd.fr_non_ack_flg = cdh.fr_non_ack_flg then '' else icd.fr_non_ack_flg||'<>'||cdh.fr_non_ack_flg /*#*/end fr_non_ack_flg_diff
, case when icd.non_prom_auto_rnw_flg = cdh.non_prom_auto_rnw_flg then '' else icd.non_prom_auto_rnw_flg||'<>'||cdh.non_prom_auto_rnw_flg /*#*/end non_prom_auto_rnw_flg_diff
, case when icd.non_prom_auto_rnw_gft_flg = cdh.non_prom_auto_rnw_gft_flg then '' else icd.non_prom_auto_rnw_gft_flg||'<>'||cdh.non_prom_auto_rnw_gft_flg /*#*/end non_prom_auto_rnw_gft_flg_diff
, case when icd.abq_non_prom_dm_flg = cdh.abq_non_prom_dm_flg then '' else icd.abq_non_prom_dm_flg||'<>'||cdh.abq_non_prom_dm_flg /*#*/end abq_non_prom_dm_flg_diff
, case when icd.abq_non_prom_em_flg = cdh.abq_non_prom_em_flg then '' else icd.abq_non_prom_em_flg||'<>'||cdh.abq_non_prom_em_flg /*#*/end abq_non_prom_em_flg_diff
, case when icd.adv_non_prom_em_flg = cdh.adv_non_prom_em_flg then '' else icd.adv_non_prom_em_flg||'<>'||cdh.adv_non_prom_em_flg /*#*/end adv_non_prom_em_flg_diff
, case when icd.aps_non_prom_em_flg = cdh.aps_non_prom_em_flg then '' else icd.aps_non_prom_em_flg||'<>'||cdh.aps_non_prom_em_flg /*#*/end aps_non_prom_em_flg_diff
, case when icd.cr_non_prom_em_flg = cdh.cr_non_prom_em_flg then '' else icd.cr_non_prom_em_flg||'<>'||cdh.cr_non_prom_em_flg /*#*/end cr_non_prom_em_flg_diff
, case when icd.crmg_non_prom_em_flg = cdh.crmg_non_prom_em_flg then '' else icd.crmg_non_prom_em_flg||'<>'||cdh.crmg_non_prom_em_flg /*#*/end crmg_non_prom_em_flg_diff
, case when icd.cro_non_prom_em_flg = cdh.cro_non_prom_em_flg then '' else icd.cro_non_prom_em_flg||'<>'||cdh.cro_non_prom_em_flg /*#*/end cro_non_prom_em_flg_diff
, case when icd.crsch_non_prom_em_flg = cdh.crsch_non_prom_em_flg then '' else icd.crsch_non_prom_em_flg||'<>'||cdh.crsch_non_prom_em_flg /*#*/end crsch_non_prom_em_flg_diff
, case when icd.fr_non_prom_em_flg = cdh.fr_non_prom_em_flg then '' else icd.fr_non_prom_em_flg||'<>'||cdh.fr_non_prom_em_flg /*#*/end fr_non_prom_em_flg_diff
, case when icd.non_prom_em_flg = cdh.non_prom_em_flg then '' else icd.non_prom_em_flg||'<>'||cdh.non_prom_em_flg /*#*/end non_prom_em_flg_diff
, case when icd.hl_non_prom_em_flg = cdh.hl_non_prom_em_flg then '' else icd.hl_non_prom_em_flg||'<>'||cdh.hl_non_prom_em_flg /*#*/end hl_non_prom_em_flg_diff
, case when icd.ma_non_prom_em_flg = cdh.ma_non_prom_em_flg then '' else icd.ma_non_prom_em_flg||'<>'||cdh.ma_non_prom_em_flg /*#*/end ma_non_prom_em_flg_diff
, case when icd.ncbk_non_prom_em_flg = cdh.ncbk_non_prom_em_flg then '' else icd.ncbk_non_prom_em_flg||'<>'||cdh.ncbk_non_prom_em_flg /*#*/end ncbk_non_prom_em_flg_diff
, case when icd.ucbk_non_prom_em_flg = cdh.ucbk_non_prom_em_flg then '' else icd.ucbk_non_prom_em_flg||'<>'||cdh.ucbk_non_prom_em_flg /*#*/end ucbk_non_prom_em_flg_diff
, case when icd.non_prom_list_rent_flg = cdh.non_prom_list_rent_flg then '' else icd.non_prom_list_rent_flg||'<>'||cdh.non_prom_list_rent_flg /*#*/end non_prom_list_rent_flg_diff
, case when icd.non_prom_pander_flg = cdh.non_prom_pander_flg then '' else icd.non_prom_pander_flg||'<>'||cdh.non_prom_pander_flg /*#*/end non_prom_pander_flg_diff
, case when icd.non_prom_rfl_flg = cdh.non_prom_rfl_flg then '' else icd.non_prom_rfl_flg||'<>'||cdh.non_prom_rfl_flg /*#*/end non_prom_rfl_flg_diff
, case when icd.non_prom_tm_flg = cdh.non_prom_tm_flg then '' else icd.non_prom_tm_flg||'<>'||cdh.non_prom_tm_flg /*#*/end non_prom_tm_flg_diff
, case when icd.fr_non_prom_dm_flg = cdh.fr_non_prom_dm_flg then '' else icd.fr_non_prom_dm_flg||'<>'||cdh.fr_non_prom_dm_flg /*#*/end fr_non_prom_dm_flg_diff
, case when icd.fr_non_prom_tm_flg = cdh.fr_non_prom_tm_flg then '' else icd.fr_non_prom_tm_flg||'<>'||cdh.fr_non_prom_tm_flg /*#*/end fr_non_prom_tm_flg_diff
, case when icd.fr_non_prom_rnw_flg = cdh.fr_non_prom_rnw_flg then '' else icd.fr_non_prom_rnw_flg||'<>'||cdh.fr_non_prom_rnw_flg /*#*/end fr_non_prom_rnw_flg_diff
, case when icd.fr_non_prom_tof_flg = cdh.fr_non_prom_tof_flg then '' else icd.fr_non_prom_tof_flg||'<>'||cdh.fr_non_prom_tof_flg /*#*/end fr_non_prom_tof_flg_diff
, case when icd.fr_non_prom_tm_rem_flg = cdh.fr_non_prom_tm_rem_flg then '' else icd.fr_non_prom_tm_rem_flg||'<>'||cdh.fr_non_prom_tm_rem_flg /*#*/end fr_non_prom_tm_rem_flg_diff
, case when icd.fr_non_prom_cu_flg = cdh.fr_non_prom_cu_flg then '' else icd.fr_non_prom_cu_flg||'<>'||cdh.fr_non_prom_cu_flg /*#*/end fr_non_prom_cu_flg_diff
, case when icd.fr_non_prom_prem_flg = cdh.fr_non_prom_prem_flg then '' else icd.fr_non_prom_prem_flg||'<>'||cdh.fr_non_prom_prem_flg /*#*/end fr_non_prom_prem_flg_diff
, case when icd.fr_rnw_mail_freq_cd = cdh.fr_rnw_mail_freq_cd then '' else icd.fr_rnw_mail_freq_cd||'<>'||cdh.fr_rnw_mail_freq_cd /*#*/end fr_rnw_mail_freq_cd_diff
, case when icd.fr_tm_freq_cd = cdh.fr_tm_freq_cd then '' else icd.fr_tm_freq_cd||'<>'||cdh.fr_tm_freq_cd /*#*/end fr_tm_freq_cd_diff
, case when icd.fr_cu_ins_flg = cdh.fr_cu_ins_flg then '' else icd.fr_cu_ins_flg||'<>'||cdh.fr_cu_ins_flg /*#*/end fr_cu_ins_flg_diff
, case when icd.cu_board_flg = cdh.cu_board_flg then '' else icd.cu_board_flg||'<>'||cdh.cu_board_flg /*#*/end cu_board_flg_diff
, case when icd.dma_non_prom_dm_flg = cdh.dma_non_prom_dm_flg then '' else icd.dma_non_prom_dm_flg||'<>'||cdh.dma_non_prom_dm_flg /*#*/end dma_non_prom_dm_flg_diff
, case when icd.dma_non_prom_tm_flg = cdh.dma_non_prom_tm_flg then '' else icd.dma_non_prom_tm_flg||'<>'||cdh.dma_non_prom_tm_flg /*#*/end dma_non_prom_tm_flg_diff
, case when icd.dma_non_prom_em_flg = cdh.dma_non_prom_em_flg then '' else icd.dma_non_prom_em_flg||'<>'||cdh.dma_non_prom_em_flg /*#*/end dma_non_prom_em_flg_diff
, case when icd.acx_non_prom_dm_flg = cdh.acx_non_prom_dm_flg then '' else icd.acx_non_prom_dm_flg||'<>'||cdh.acx_non_prom_dm_flg /*#*/end acx_non_prom_dm_flg_diff
, case when icd.acx_non_prom_tm_flg = cdh.acx_non_prom_tm_flg then '' else icd.acx_non_prom_tm_flg||'<>'||cdh.acx_non_prom_tm_flg /*#*/end acx_non_prom_tm_flg_diff
, case when icd.acx_non_prom_em_flg = cdh.acx_non_prom_em_flg then '' else icd.acx_non_prom_em_flg||'<>'||cdh.acx_non_prom_em_flg /*#*/end acx_non_prom_em_flg_diff
, case when icd.bk_non_prom_flg = cdh.bk_non_prom_flg then '' else icd.bk_non_prom_flg||'<>'||cdh.bk_non_prom_flg /*#*/end bk_non_prom_flg_diff
, case when icd.non_prom_gft_flg = cdh.non_prom_gft_flg then '' else icd.non_prom_gft_flg||'<>'||cdh.non_prom_gft_flg /*#*/end non_prom_gft_flg_diff
, case when icd.shm_non_prom_em_flg = cdh.shm_non_prom_em_flg then '' else icd.shm_non_prom_em_flg||'<>'||cdh.shm_non_prom_em_flg /*#*/end shm_non_prom_em_flg_diff
, case when icd.carp_non_prom_em_flg = cdh.carp_non_prom_em_flg then '' else icd.carp_non_prom_em_flg||'<>'||cdh.carp_non_prom_em_flg /*#*/end carp_non_prom_em_flg_diff
, case when icd.non_prom_cu_fr_rfl_flg = cdh.non_prom_cu_fr_rfl_flg then '' else icd.non_prom_cu_fr_rfl_flg||'<>'||cdh.non_prom_cu_fr_rfl_flg /*#*/end non_prom_cu_fr_rfl_flg_diff
, case when icd.ib_deceased_flg = cdh.ib_deceased_flg then '' else icd.ib_deceased_flg||'<>'||cdh.ib_deceased_flg /*#*/end ib_deceased_flg_diff
, case when icd.test_user_flg = cdh.test_user_flg then '' else icd.test_user_flg||'<>'||cdh.test_user_flg /*#*/end test_user_flg_diff
, case when icd.fr_non_share_flg = cdh.fr_non_share_flg then '' else icd.fr_non_share_flg||'<>'||cdh.fr_non_share_flg /*#*/end fr_non_share_flg_diff
, case when icd.adv_non_prom_news_em_flg = cdh.adv_non_prom_news_em_flg then '' else icd.adv_non_prom_news_em_flg||'<>'||cdh.adv_non_prom_news_em_flg /*#*/end adv_non_prom_news_em_flg_diff
, case when icd.adv_non_prom_aa_em_flg = cdh.adv_non_prom_aa_em_flg then '' else icd.adv_non_prom_aa_em_flg||'<>'||cdh.adv_non_prom_aa_em_flg /*#*/end adv_non_prom_aa_em_flg_diff
, case when icd.adv_non_prom_dm_flg = cdh.adv_non_prom_dm_flg then '' else icd.adv_non_prom_dm_flg||'<>'||cdh.adv_non_prom_dm_flg /*#*/end adv_non_prom_dm_flg_diff
, case when icd.advfr_non_prom_em_flg = cdh.advfr_non_prom_em_flg then '' else icd.advfr_non_prom_em_flg||'<>'||cdh.advfr_non_prom_em_flg /*#*/end advfr_non_prom_em_flg_diff


, '||                    ICD                    >>' as sep1, icd.*
, '||                    CDH                    >>' as sep2, cdh.*

from  icd full join cdh
on icd.icd_id = cdh.icd_id and icd.cdh_id = cdh.cdh_id


------------------------------------------------------------------------RUN ALL ABOVE
;
drop table  cr_temp.diff_mt_auth_summary_0
;
select non_prom_tm_flg
from   cr_temp.GTT_MT_AUTH_SUMMARY_0  where icd_id = 10002000000784080
;
select non_prom_tm_flg
from   cr_temp.gtt_agg_uth_summary  where cdh_id = 1203194565
;
select count(*) from   cr_temp.GTT_MT_AUTH_SUMMARY_0
;
select count(*) from   cr_temp.gtt_agg_uth_summary
;
--truncate table    cr_temp.GTT_MT_AUTH_SUMMARY_0;
drop table  cr_temp.GTT_MT_AUTH_SUMMARY_0;
--truncate table cr_temp.gtt_agg_uth_summary;
drop table  cr_temp.gtt_agg_uth_summary;
;
"Evaluate the following MT_AUTHORIZATION entries:
MT_AUTHORIZATION.DATA_SOURCE IS 'DGI' and MT_AUTHORIZATION.AUTH_CD is 'EMAIL' and MT_AUTHORIZATION.SCP_CD = 'ADV';
MT_AUTHORIZATION.DATA_SOURCE IS 'PWI' or 'BMO' and MT_AUTHORIZATION.AUTH_CD is 'EMAIL' and MT_AUTHORIZATION.SCP_CD = 'ADV';
MT_AUTHORIZATION.DATA_SOURCE is 'CVO' and MT_AUTHORIZATION.ASSOC_TYP_CD = 'E' and MT_AUTHORIZATION.AUTH_CD = 'EMAIL' and MT_AUTHORIZATION.SCP_CD = 'ADV';
MT_AUTHORIZATION.AUTH_CD is 'EMAIL' and MT_AUTHORIZATION.SCP_CD = 'GLB' and MT_AUTHORIZATION.DATA_SOURCE is not ('AX', 'DM').
If the entry with the maximum MT_AUTHORIZATION.EFF_DT is the record with the MT_AUTHORIZATION.SCP_CD = ‘GLB’ and the MT_AUTHORIZATION.AUTH_FLG = ‘N’ then code as ‘Y’
Else for entries where the MT_AUTHORIZATION.SCP_CD is not ‘GLB’
(If the entry with the maximum MT_AUTHORIZATION.EFF_DT is the record where the MT_AUTHORIZATION.AUTH_FLG = ‘N’ then code as ‘Y’
Else if the entry with the maximum MT_AUTHORIZATION.EFF_DT is the record where the MT_AUTHORIZATION.AUTH_FLG = ‘Y’ then code as ‘N’)
Else code as null."
;
;
select adv_non_prom_em_flg, individual_id   
from   prod.agg_preference_summary  where individual_id  = 1204767987
limit 1
;
with tt1 as (
select *
  from prod.agg_preference
 where 1=1 and individual_id  = 1204767987
 and data_source = 'DGI' and auth_cd = 'EMAIL' and scp_cd = 'ADV'
union
select *
  from prod.agg_preference
 where 1=1 and individual_id  = 1204767987
 and data_source in ('PWI','BMO') and auth_cd = 'EMAIL' and scp_cd = 'ADV'
 union
 select *
  from prod.agg_preference a
 where 1=1 and individual_id  = 1204767987
 and data_source = 'CVO' and ASSOC_TYP_CD = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV'
 union 
  select *
  from prod.agg_preference
 where 1=1 and individual_id  = 1204767987
 and SCP_CD = 'GLB' and auth_cd = 'EMAIL' and DATA_SOURCE not in ('AX', 'DM')
-- union 
--  select *
--  from prod.agg_preference a
-- where 1=1 and individual_id  = 1204767987 --and scp_cd != 'GLB'
-- and  ((a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'ADV')
--                              OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
--                              OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
--                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')))

)
select
   CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'ADV')
                              OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
                              OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'ADV')
                                  OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
                                  OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'ADV') 
                                  OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
                                  OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END adv_non_prom_em_flg
  from tt1 a
  
  ;
    select a.*, CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd IN ('DNT', 'XXX')
            AND a.auth_flg = 'Y'
           THEN 'Y'
           WHEN a.data_source IN ('TLS', 'DM', 'AG', 'FC', 'CDB', 'CDS')
            AND a.auth_cd = 'PHONE'
            AND a.auth_flg = 'N'
           THEN 'Y'
           WHEN a.data_source = 'TLS'
            AND a.auth_cd = 'PHONE_PRIV'
            AND a.auth_flg = 'N'
           THEN 'Y'
           WHEN a.data_source = 'DMP'
            AND a.auth_cd = 'DNT'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null end
  from prod.agg_preference a
 where 1=1 and individual_id  = 1203194565 --and scp_cd != 'GLB'
;

;
;
select non_prom_tm_flg
from   cr_temp.GTT_MT_AUTH_SUMMARY_0  where icd_id = 10002000000784080
;
select non_prom_tm_flg
from   cr_temp.gtt_agg_uth_summary  where cdh_id = 1203194565