;
drop table if exists  cr_temp.GTT_MT_ONLINE_SUMMARY;
create table cr_temp.GTT_MT_ONLINE_SUMMARY as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/

, NVL2(CRO_ACTV_FLG, CRO_ACTV_FLG::text,'(NULL)') as CRO_ACTV_FLG
, NVL2(CRO_ACTV_PD_FLG, CRO_ACTV_PD_FLG::text,'(NULL)') as CRO_ACTV_PD_FLG
, NVL2(NCBK_ACTV_PD_FLG, NCBK_ACTV_PD_FLG::text,'(NULL)') as NCBK_ACTV_PD_FLG
, NVL2(CRO_ANNUAL_FLG, CRO_ANNUAL_FLG::text,'(NULL)') as CRO_ANNUAL_FLG
, NVL2(CRMG_CANC_BAD_DBT_FLG, CRMG_CANC_BAD_DBT_FLG::text,'(NULL)') as CRMG_CANC_BAD_DBT_FLG
, NVL2(CRO_CANC_BAD_DBT_FLG, CRO_CANC_BAD_DBT_FLG::text,'(NULL)') as CRO_CANC_BAD_DBT_FLG
, NVL2(CRMG_CANC_CUST_FLG, CRMG_CANC_CUST_FLG::text,'(NULL)') as CRMG_CANC_CUST_FLG
, NVL2(CRO_CANC_CUST_FLG, CRO_CANC_CUST_FLG::text,'(NULL)') as CRO_CANC_CUST_FLG
, NVL2(CRO_CR_STAT_CD, CRO_CR_STAT_CD::text,'(NULL)') as CRO_CR_STAT_CD
, NVL2(TO_CHAR(CRO_EXP_DT,'DD-MON-YY'), TO_CHAR(CRO_EXP_DT,'DD-MON-YY')::text,'(NULL)') as CRO_EXP_DT
, NVL2(TO_CHAR(CRMG_LST_CANC_BAD_DBT_DT,'DD-MON-YY'), TO_CHAR(CRMG_LST_CANC_BAD_DBT_DT,'DD-MON-YY')::text,'(NULL)') as CRMG_LST_CANC_BAD_DBT_DT
, NVL2(TO_CHAR(CRMG_LST_CANC_CUST_DT,'DD-MON-YY'), TO_CHAR(CRMG_LST_CANC_CUST_DT,'DD-MON-YY')::text,'(NULL)') as CRMG_LST_CANC_CUST_DT
, NVL2(TO_CHAR(CRO_LST_CANC_CUST_DT,'DD-MON-YY'), TO_CHAR(CRO_LST_CANC_CUST_DT,'DD-MON-YY')::text,'(NULL)') as CRO_LST_CANC_CUST_DT
, NVL2(TO_CHAR(NCBK_LST_ORD_DT,'DD-MON-YY'), TO_CHAR(NCBK_LST_ORD_DT,'DD-MON-YY')::text,'(NULL)') as NCBK_LST_ORD_DT
, NVL2(TO_CHAR(CRO_LST_PMT_DT,'DD-MON-YY'), TO_CHAR(CRO_LST_PMT_DT,'DD-MON-YY')::text,'(NULL)') as CRO_LST_PMT_DT
, NVL2(APS_FLG, APS_FLG::text,'(NULL)') as APS_FLG
, NVL2(CRMG_FLG, CRMG_FLG::text,'(NULL)') as CRMG_FLG
, NVL2(CRO_FLG, CRO_FLG::text,'(NULL)') as CRO_FLG
, NVL2(NCBK_FLG, NCBK_FLG::text,'(NULL)') as NCBK_FLG
, NVL2(UC_RPTS_FLG, UC_RPTS_FLG::text,'(NULL)') as UC_RPTS_FLG
, NVL2(UCBK_FLG, UCBK_FLG::text,'(NULL)') as UCBK_FLG
, NVL2(CRO_CURR_MBR_KEYCODE, CRO_CURR_MBR_KEYCODE::text,'(NULL)') as CRO_CURR_MBR_KEYCODE
, NVL2(CRO_CURR_ORD_KEYCODE, CRO_CURR_ORD_KEYCODE::text,'(NULL)') as CRO_CURR_ORD_KEYCODE
, NVL2(CRO_FST_MBR_KEYCODE, CRO_FST_MBR_KEYCODE::text,'(NULL)') as CRO_FST_MBR_KEYCODE
, NVL2(NCBK_FST_ORD_KEYCODE, NCBK_FST_ORD_KEYCODE::text,'(NULL)') as NCBK_FST_ORD_KEYCODE
, NVL2(CRO_LST_ORD_KEYCODE, CRO_LST_ORD_KEYCODE::text,'(NULL)') as CRO_LST_ORD_KEYCODE
, NVL2(NCBK_LST_ORD_KEYCODE, NCBK_LST_ORD_KEYCODE::text,'(NULL)') as NCBK_LST_ORD_KEYCODE
, NVL2(trunc(nvl(CRO_LST_SUB_AMT,'0')), trunc(nvl(CRO_LST_SUB_AMT,'0'))::text,'(NULL)') as CRO_LST_SUB_AMT
, NVL2(CRO_MONTHLY_FLG, CRO_MONTHLY_FLG::text,'(NULL)') as CRO_MONTHLY_FLG
, NVL2(CRO_NON_SUB_DNR_FLG, CRO_NON_SUB_DNR_FLG::text,'(NULL)') as CRO_NON_SUB_DNR_FLG
, NVL2(trunc(nvl(CRO_BRKS_CNT,'0')), trunc(nvl(CRO_BRKS_CNT,'0'))::text,'(NULL)') as CRO_BRKS_CNT
, NVL2(trunc(nvl(CRO_RNW_CNT,'0')), trunc(nvl(CRO_RNW_CNT,'0'))::text,'(NULL)') as CRO_RNW_CNT
, NVL2(CRO_REC_FLG, CRO_REC_FLG::text,'(NULL)') as CRO_REC_FLG
, NVL2(CRO_SVC_STAT_CD, CRO_SVC_STAT_CD::text,'(NULL)') as CRO_SVC_STAT_CD
, NVL2(CRO_CURR_MBR_SRC_CD, CRO_CURR_MBR_SRC_CD::text,'(NULL)') as CRO_CURR_MBR_SRC_CD
, NVL2(CRO_CURR_ORD_SRC_CD, CRO_CURR_ORD_SRC_CD::text,'(NULL)') as CRO_CURR_ORD_SRC_CD
, NVL2(CRO_FST_ORD_SRC_CD, CRO_FST_ORD_SRC_CD::text,'(NULL)') as CRO_FST_ORD_SRC_CD
, NVL2(NCBK_FST_ORD_SRC_CD, NCBK_FST_ORD_SRC_CD::text,'(NULL)') as NCBK_FST_ORD_SRC_CD
, NVL2(CRO_LST_ORD_SRC_CD, CRO_LST_ORD_SRC_CD::text,'(NULL)') as CRO_LST_ORD_SRC_CD
, NVL2(NCBK_LST_ORD_SRC_CD, NCBK_LST_ORD_SRC_CD::text,'(NULL)') as NCBK_LST_ORD_SRC_CD
, NVL2(CRO_PRIOR_MBR_SRC_CD, CRO_PRIOR_MBR_SRC_CD::text,'(NULL)') as CRO_PRIOR_MBR_SRC_CD
, NVL2(CRO_SUB_DNR_FLG, CRO_SUB_DNR_FLG::text,'(NULL)') as CRO_SUB_DNR_FLG
, NVL2(CRO_CURR_ORD_TERM, CRO_CURR_ORD_TERM::text,'(NULL)') as CRO_CURR_ORD_TERM
, NVL2(TO_CHAR(CRO_FST_DNR_DT,'DD-MON-YY'), TO_CHAR(CRO_FST_DNR_DT,'DD-MON-YY')::text,'(NULL)') as CRO_FST_DNR_DT
, NVL2(CRO_AUTO_RNW_FLG, CRO_AUTO_RNW_FLG::text,'(NULL)') as CRO_AUTO_RNW_FLG
, NVL2(CRO_NON_DNR_FLG, CRO_NON_DNR_FLG::text,'(NULL)') as CRO_NON_DNR_FLG
, NVL2(TO_CHAR(CRO_CURR_MBR_DT,'DD-MON-YY'), TO_CHAR(CRO_CURR_MBR_DT,'DD-MON-YY')::text,'(NULL)') as CRO_CURR_MBR_DT
, NVL2(trunc(nvl(CRO_LTD_PD_AMT,'0')), trunc(nvl(CRO_LTD_PD_AMT,'0'))::text,'(NULL)') as CRO_LTD_PD_AMT
, NVL2(TO_CHAR(CRO_CURR_ORD_DT,'DD-MON-YY'), TO_CHAR(CRO_CURR_ORD_DT,'DD-MON-YY')::text,'(NULL)') as CRO_CURR_ORD_DT
, NVL2(TO_CHAR(CRO_FST_ORD_DT,'DD-MON-YY'), TO_CHAR(CRO_FST_ORD_DT,'DD-MON-YY')::text,'(NULL)') as CRO_FST_ORD_DT
, NVL2(TO_CHAR(NCBK_FST_ORD_DT,'DD-MON-YY'), TO_CHAR(NCBK_FST_ORD_DT,'DD-MON-YY')::text,'(NULL)') as NCBK_FST_ORD_DT
, NVL2(TO_CHAR(CRO_LST_CANC_BAD_DBT_DT,'DD-MON-YY'), TO_CHAR(CRO_LST_CANC_BAD_DBT_DT,'DD-MON-YY')::text,'(NULL)') as CRO_LST_CANC_BAD_DBT_DT
, NVL2(TO_CHAR(CRO_LST_ORD_DT,'DD-MON-YY'), TO_CHAR(CRO_LST_ORD_DT,'DD-MON-YY')::text,'(NULL)') as CRO_LST_ORD_DT
, NVL2(TO_CHAR(CRO_LST_DNR_ORD_DT,'DD-MON-YY'), TO_CHAR(CRO_LST_DNR_ORD_DT,'DD-MON-YY')::text,'(NULL)') as CRO_LST_DNR_ORD_DT
, NVL2(trunc(nvl(CRMG_CANC_CUST_CNT,'0')), trunc(nvl(CRMG_CANC_CUST_CNT,'0'))::text,'(NULL)') as CRMG_CANC_CUST_CNT
, NVL2(trunc(nvl(CRO_CANC_CUST_CNT,'0')), trunc(nvl(CRO_CANC_CUST_CNT,'0'))::text,'(NULL)') as CRO_CANC_CUST_CNT
, NVL2(trunc(nvl(CRO_ORD_CNT,'0')), trunc(nvl(CRO_ORD_CNT,'0'))::text,'(NULL)') as CRO_ORD_CNT
, NVL2(trunc(nvl(CRO_DM_ORD_CNT,'0')), trunc(nvl(CRO_DM_ORD_CNT,'0'))::text,'(NULL)') as CRO_DM_ORD_CNT
, NVL2(trunc(nvl(CRO_EM_ORD_CNT,'0')), trunc(nvl(CRO_EM_ORD_CNT,'0'))::text,'(NULL)') as CRO_EM_ORD_CNT
, NVL2(trunc(nvl(CRO_DNR_ORD_CNT,'0')), trunc(nvl(CRO_DNR_ORD_CNT,'0'))::text,'(NULL)') as CRO_DNR_ORD_CNT
, NVL2(CRO_LST_SUB_ORD_ROLE_CD, CRO_LST_SUB_ORD_ROLE_CD::text,'(NULL)') as CRO_LST_SUB_ORD_ROLE_CD
, NVL2(TO_CHAR(LST_LOGN_DT,'DD-MON-YY'), TO_CHAR(LST_LOGN_DT,'DD-MON-YY')::text,'(NULL)') as LST_LOGN_DT
, NVL2(trunc(nvl(LOGN_CNT,'0')), trunc(nvl(LOGN_CNT,'0'))::text,'(NULL)') as LOGN_CNT
, NVL2(CRO_LT_SUB_FLG, CRO_LT_SUB_FLG::text,'(NULL)') as CRO_LT_SUB_FLG
, NVL2(CARP_ACTV_FLG, CARP_ACTV_FLG::text,'(NULL)') as CARP_ACTV_FLG
, NVL2(CARP_ACTV_PD_FLG, CARP_ACTV_PD_FLG::text,'(NULL)') as CARP_ACTV_PD_FLG
/*---END---*/

from cr_temp.MT_ONLINE_SUMMARY T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;

drop table if exists  cr_temp.gtt_agg_digital_summary;
CREATE TABLE cr_temp.gtt_agg_digital_summary AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/

, NVL2(CRO_ACTV_FLG, CRO_ACTV_FLG::text,'(NULL)') as CRO_ACTV_FLG
, NVL2(CRO_ACTV_PD_FLG, CRO_ACTV_PD_FLG::text,'(NULL)') as CRO_ACTV_PD_FLG
, NVL2(NCBK_ACTV_PD_FLG, NCBK_ACTV_PD_FLG::text,'(NULL)') as NCBK_ACTV_PD_FLG
, NVL2(CRO_ANNUAL_FLG, CRO_ANNUAL_FLG::text,'(NULL)') as CRO_ANNUAL_FLG
, NVL2(CRMG_CANC_BAD_DBT_FLG, CRMG_CANC_BAD_DBT_FLG::text,'(NULL)') as CRMG_CANC_BAD_DBT_FLG
, NVL2(CRO_CANC_BAD_DBT_FLG, CRO_CANC_BAD_DBT_FLG::text,'(NULL)') as CRO_CANC_BAD_DBT_FLG
, NVL2(CRMG_CANC_CUST_FLG, CRMG_CANC_CUST_FLG::text,'(NULL)') as CRMG_CANC_CUST_FLG
, NVL2(CRO_CANC_CUST_FLG, CRO_CANC_CUST_FLG::text,'(NULL)') as CRO_CANC_CUST_FLG
, NVL2(CRO_CR_STAT_CD, CRO_CR_STAT_CD::text,'(NULL)') as CRO_CR_STAT_CD
, NVL2(TO_CHAR(CRO_EXP_DT,'DD-MON-YY'), TO_CHAR(CRO_EXP_DT,'DD-MON-YY')::text,'(NULL)') as CRO_EXP_DT
, NVL2(TO_CHAR(CRMG_LST_CANC_BAD_DBT_DT,'DD-MON-YY'), TO_CHAR(CRMG_LST_CANC_BAD_DBT_DT,'DD-MON-YY')::text,'(NULL)') as CRMG_LST_CANC_BAD_DBT_DT
, NVL2(TO_CHAR(CRMG_LST_CANC_CUST_DT,'DD-MON-YY'), TO_CHAR(CRMG_LST_CANC_CUST_DT,'DD-MON-YY')::text,'(NULL)') as CRMG_LST_CANC_CUST_DT
, NVL2(TO_CHAR(CRO_LST_CANC_CUST_DT,'DD-MON-YY'), TO_CHAR(CRO_LST_CANC_CUST_DT,'DD-MON-YY')::text,'(NULL)') as CRO_LST_CANC_CUST_DT
, NVL2(TO_CHAR(NCBK_LST_ORD_DT,'DD-MON-YY'), TO_CHAR(NCBK_LST_ORD_DT,'DD-MON-YY')::text,'(NULL)') as NCBK_LST_ORD_DT
, NVL2(TO_CHAR(CRO_LST_PMT_DT,'DD-MON-YY'), TO_CHAR(CRO_LST_PMT_DT,'DD-MON-YY')::text,'(NULL)') as CRO_LST_PMT_DT
, NVL2(APS_FLG, APS_FLG::text,'(NULL)') as APS_FLG
, NVL2(CRMG_FLG, CRMG_FLG::text,'(NULL)') as CRMG_FLG
, NVL2(CRO_FLG, CRO_FLG::text,'(NULL)') as CRO_FLG
, NVL2(NCBK_FLG, NCBK_FLG::text,'(NULL)') as NCBK_FLG
, NVL2(UC_RPTS_FLG, UC_RPTS_FLG::text,'(NULL)') as UC_RPTS_FLG
, NVL2(UCBK_FLG, UCBK_FLG::text,'(NULL)') as UCBK_FLG
, NVL2(CRO_CURR_MBR_KEYCODE, CRO_CURR_MBR_KEYCODE::text,'(NULL)') as CRO_CURR_MBR_KEYCODE
, NVL2(CRO_CURR_ORD_KEYCODE, CRO_CURR_ORD_KEYCODE::text,'(NULL)') as CRO_CURR_ORD_KEYCODE
, NVL2(CRO_FST_MBR_KEYCODE, CRO_FST_MBR_KEYCODE::text,'(NULL)') as CRO_FST_MBR_KEYCODE
, NVL2(NCBK_FST_ORD_KEYCODE, NCBK_FST_ORD_KEYCODE::text,'(NULL)') as NCBK_FST_ORD_KEYCODE
, NVL2(CRO_LST_ORD_KEYCODE, CRO_LST_ORD_KEYCODE::text,'(NULL)') as CRO_LST_ORD_KEYCODE
, NVL2(NCBK_LST_ORD_KEYCODE, NCBK_LST_ORD_KEYCODE::text,'(NULL)') as NCBK_LST_ORD_KEYCODE
, NVL2(trunc(nvl(CRO_LST_SUB_AMT,'0')), trunc(nvl(CRO_LST_SUB_AMT,'0'))::text,'(NULL)') as CRO_LST_SUB_AMT
, NVL2(CRO_MONTHLY_FLG, CRO_MONTHLY_FLG::text,'(NULL)') as CRO_MONTHLY_FLG
, NVL2(CRO_NON_SUB_DNR_FLG, CRO_NON_SUB_DNR_FLG::text,'(NULL)') as CRO_NON_SUB_DNR_FLG
, NVL2(trunc(nvl(CRO_BRKS_CNT,'0')), trunc(nvl(CRO_BRKS_CNT,'0'))::text,'(NULL)') as CRO_BRKS_CNT
, NVL2(trunc(nvl(CRO_RNW_CNT,'0')), trunc(nvl(CRO_RNW_CNT,'0'))::text,'(NULL)') as CRO_RNW_CNT
, NVL2(CRO_REC_FLG, CRO_REC_FLG::text,'(NULL)') as CRO_REC_FLG
, NVL2(CRO_SVC_STAT_CD, CRO_SVC_STAT_CD::text,'(NULL)') as CRO_SVC_STAT_CD
, NVL2(CRO_CURR_MBR_SRC_CD, CRO_CURR_MBR_SRC_CD::text,'(NULL)') as CRO_CURR_MBR_SRC_CD
, NVL2(CRO_CURR_ORD_SRC_CD, CRO_CURR_ORD_SRC_CD::text,'(NULL)') as CRO_CURR_ORD_SRC_CD
, NVL2(CRO_FST_ORD_SRC_CD, CRO_FST_ORD_SRC_CD::text,'(NULL)') as CRO_FST_ORD_SRC_CD
, NVL2(NCBK_FST_ORD_SRC_CD, NCBK_FST_ORD_SRC_CD::text,'(NULL)') as NCBK_FST_ORD_SRC_CD
, NVL2(CRO_LST_ORD_SRC_CD, CRO_LST_ORD_SRC_CD::text,'(NULL)') as CRO_LST_ORD_SRC_CD
, NVL2(NCBK_LST_ORD_SRC_CD, NCBK_LST_ORD_SRC_CD::text,'(NULL)') as NCBK_LST_ORD_SRC_CD
, NVL2(CRO_PRIOR_MBR_SRC_CD, CRO_PRIOR_MBR_SRC_CD::text,'(NULL)') as CRO_PRIOR_MBR_SRC_CD
, NVL2(CRO_SUB_DNR_FLG, CRO_SUB_DNR_FLG::text,'(NULL)') as CRO_SUB_DNR_FLG
, NVL2(CRO_CURR_ORD_TERM, CRO_CURR_ORD_TERM::text,'(NULL)') as CRO_CURR_ORD_TERM
, NVL2(TO_CHAR(CRO_FST_DNR_DT,'DD-MON-YY'), TO_CHAR(CRO_FST_DNR_DT,'DD-MON-YY')::text,'(NULL)') as CRO_FST_DNR_DT
, NVL2(CRO_AUTO_RNW_FLG, CRO_AUTO_RNW_FLG::text,'(NULL)') as CRO_AUTO_RNW_FLG
, NVL2(CRO_NON_DNR_FLG, CRO_NON_DNR_FLG::text,'(NULL)') as CRO_NON_DNR_FLG
, NVL2(TO_CHAR(CRO_CURR_MBR_DT,'DD-MON-YY'), TO_CHAR(CRO_CURR_MBR_DT,'DD-MON-YY')::text,'(NULL)') as CRO_CURR_MBR_DT
, NVL2(trunc(nvl(CRO_LTD_PD_AMT,'0')), trunc(nvl(CRO_LTD_PD_AMT,'0'))::text,'(NULL)') as CRO_LTD_PD_AMT
, NVL2(TO_CHAR(CRO_CURR_ORD_DT,'DD-MON-YY'), TO_CHAR(CRO_CURR_ORD_DT,'DD-MON-YY')::text,'(NULL)') as CRO_CURR_ORD_DT
, NVL2(TO_CHAR(CRO_FST_ORD_DT,'DD-MON-YY'), TO_CHAR(CRO_FST_ORD_DT,'DD-MON-YY')::text,'(NULL)') as CRO_FST_ORD_DT
, NVL2(TO_CHAR(NCBK_FST_ORD_DT,'DD-MON-YY'), TO_CHAR(NCBK_FST_ORD_DT,'DD-MON-YY')::text,'(NULL)') as NCBK_FST_ORD_DT
, NVL2(TO_CHAR(CRO_LST_CANC_BAD_DBT_DT,'DD-MON-YY'), TO_CHAR(CRO_LST_CANC_BAD_DBT_DT,'DD-MON-YY')::text,'(NULL)') as CRO_LST_CANC_BAD_DBT_DT
, NVL2(TO_CHAR(CRO_LST_ORD_DT,'DD-MON-YY'), TO_CHAR(CRO_LST_ORD_DT,'DD-MON-YY')::text,'(NULL)') as CRO_LST_ORD_DT
, NVL2(TO_CHAR(CRO_LST_DNR_ORD_DT,'DD-MON-YY'), TO_CHAR(CRO_LST_DNR_ORD_DT,'DD-MON-YY')::text,'(NULL)') as CRO_LST_DNR_ORD_DT
, NVL2(trunc(nvl(CRMG_CANC_CUST_CNT,'0')), trunc(nvl(CRMG_CANC_CUST_CNT,'0'))::text,'(NULL)') as CRMG_CANC_CUST_CNT
, NVL2(trunc(nvl(CRO_CANC_CUST_CNT,'0')), trunc(nvl(CRO_CANC_CUST_CNT,'0'))::text,'(NULL)') as CRO_CANC_CUST_CNT
, NVL2(trunc(nvl(CRO_ORD_CNT,'0')), trunc(nvl(CRO_ORD_CNT,'0'))::text,'(NULL)') as CRO_ORD_CNT
, NVL2(trunc(nvl(CRO_DM_ORD_CNT,'0')), trunc(nvl(CRO_DM_ORD_CNT,'0'))::text,'(NULL)') as CRO_DM_ORD_CNT
, NVL2(trunc(nvl(CRO_EM_ORD_CNT,'0')), trunc(nvl(CRO_EM_ORD_CNT,'0'))::text,'(NULL)') as CRO_EM_ORD_CNT
, NVL2(trunc(nvl(CRO_DNR_ORD_CNT,'0')), trunc(nvl(CRO_DNR_ORD_CNT,'0'))::text,'(NULL)') as CRO_DNR_ORD_CNT
, NVL2(CRO_LST_SUB_ORD_ROLE_CD, CRO_LST_SUB_ORD_ROLE_CD::text,'(NULL)') as CRO_LST_SUB_ORD_ROLE_CD
, NVL2(TO_CHAR(LST_LOGN_DT,'DD-MON-YY'), TO_CHAR(LST_LOGN_DT,'DD-MON-YY')::text,'(NULL)') as LST_LOGN_DT
, NVL2(trunc(nvl(LOGN_CNT,'0')), trunc(nvl(LOGN_CNT,'0'))::text,'(NULL)') as LOGN_CNT
, NVL2(CRO_LT_SUB_FLG, CRO_LT_SUB_FLG::text,'(NULL)') as CRO_LT_SUB_FLG
, NVL2(CARP_ACTV_FLG, CARP_ACTV_FLG::text,'(NULL)') as CARP_ACTV_FLG
, NVL2(CARP_ACTV_PD_FLG, CARP_ACTV_PD_FLG::text,'(NULL)') as CARP_ACTV_PD_FLG
/*---END---*/
from prod.agg_digital_summary_v2 T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID 
where 1=1 and exists (select null from cr_temp.GTT_MT_ONLINE_SUMMARY rmncbi where rmncbi.cdh_id = individual_id ) 


------------------------------------------------------------------------TABLE CREATION END\												
												
;
drop table if exists  cr_temp.diff_mt_online_summary;
CREATE TABLE cr_temp.diff_mt_online_summary AS
with hash_diff as (

select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID     , CRO_ACTV_FLG, CRO_ACTV_PD_FLG, NCBK_ACTV_PD_FLG, CRO_ANNUAL_FLG, CRMG_CANC_BAD_DBT_FLG, CRO_CANC_BAD_DBT_FLG, CRMG_CANC_CUST_FLG, CRO_CANC_CUST_FLG, CRO_CR_STAT_CD, CRO_EXP_DT, CRMG_LST_CANC_BAD_DBT_DT, CRMG_LST_CANC_CUST_DT, CRO_LST_CANC_CUST_DT, NCBK_LST_ORD_DT, CRO_LST_PMT_DT, APS_FLG, CRMG_FLG, CRO_FLG, NCBK_FLG, UC_RPTS_FLG, UCBK_FLG, CRO_CURR_MBR_KEYCODE, CRO_CURR_ORD_KEYCODE, CRO_FST_MBR_KEYCODE, NCBK_FST_ORD_KEYCODE, CRO_LST_ORD_KEYCODE, NCBK_LST_ORD_KEYCODE, CRO_LST_SUB_AMT, CRO_MONTHLY_FLG, CRO_NON_SUB_DNR_FLG, CRO_BRKS_CNT, CRO_RNW_CNT, CRO_REC_FLG, CRO_SVC_STAT_CD, CRO_CURR_MBR_SRC_CD, CRO_CURR_ORD_SRC_CD, CRO_FST_ORD_SRC_CD, NCBK_FST_ORD_SRC_CD, CRO_LST_ORD_SRC_CD, NCBK_LST_ORD_SRC_CD, CRO_PRIOR_MBR_SRC_CD, CRO_SUB_DNR_FLG, CRO_CURR_ORD_TERM, CRO_FST_DNR_DT, CRO_AUTO_RNW_FLG, CRO_NON_DNR_FLG, CRO_CURR_MBR_DT, CRO_LTD_PD_AMT, CRO_CURR_ORD_DT, CRO_FST_ORD_DT, NCBK_FST_ORD_DT, CRO_LST_CANC_BAD_DBT_DT, CRO_LST_ORD_DT, CRO_LST_DNR_ORD_DT, CRMG_CANC_CUST_CNT, CRO_CANC_CUST_CNT, CRO_ORD_CNT, CRO_DM_ORD_CNT, CRO_EM_ORD_CNT, CRO_DNR_ORD_CNT, CRO_LST_SUB_ORD_ROLE_CD, LST_LOGN_DT, LOGN_CNT, CRO_LT_SUB_FLG, CARP_ACTV_FLG, CARP_ACTV_PD_FLG
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL , CRO_ACTV_FLG, CRO_ACTV_PD_FLG, NCBK_ACTV_PD_FLG, CRO_ANNUAL_FLG, CRMG_CANC_BAD_DBT_FLG, CRO_CANC_BAD_DBT_FLG, CRMG_CANC_CUST_FLG, CRO_CANC_CUST_FLG, CRO_CR_STAT_CD, CRO_EXP_DT, CRMG_LST_CANC_BAD_DBT_DT, CRMG_LST_CANC_CUST_DT, CRO_LST_CANC_CUST_DT, NCBK_LST_ORD_DT, CRO_LST_PMT_DT, APS_FLG, CRMG_FLG, CRO_FLG, NCBK_FLG, UC_RPTS_FLG, UCBK_FLG, CRO_CURR_MBR_KEYCODE, CRO_CURR_ORD_KEYCODE, CRO_FST_MBR_KEYCODE, NCBK_FST_ORD_KEYCODE, CRO_LST_ORD_KEYCODE, NCBK_LST_ORD_KEYCODE, CRO_LST_SUB_AMT, CRO_MONTHLY_FLG, CRO_NON_SUB_DNR_FLG, CRO_BRKS_CNT, CRO_RNW_CNT, CRO_REC_FLG, CRO_SVC_STAT_CD, CRO_CURR_MBR_SRC_CD, CRO_CURR_ORD_SRC_CD, CRO_FST_ORD_SRC_CD, NCBK_FST_ORD_SRC_CD, CRO_LST_ORD_SRC_CD, NCBK_LST_ORD_SRC_CD, CRO_PRIOR_MBR_SRC_CD, CRO_SUB_DNR_FLG, CRO_CURR_ORD_TERM, CRO_FST_DNR_DT, CRO_AUTO_RNW_FLG, CRO_NON_DNR_FLG, CRO_CURR_MBR_DT, CRO_LTD_PD_AMT, CRO_CURR_ORD_DT, CRO_FST_ORD_DT, NCBK_FST_ORD_DT, CRO_LST_CANC_BAD_DBT_DT, CRO_LST_ORD_DT, CRO_LST_DNR_ORD_DT, CRMG_CANC_CUST_CNT, CRO_CANC_CUST_CNT, CRO_ORD_CNT, CRO_DM_ORD_CNT, CRO_EM_ORD_CNT, CRO_DNR_ORD_CNT, CRO_LST_SUB_ORD_ROLE_CD, LST_LOGN_DT, LOGN_CNT, CRO_LT_SUB_FLG, CARP_ACTV_FLG, CARP_ACTV_PD_FLG
from cr_temp.GTT_MT_ONLINE_SUMMARY
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL , CRO_ACTV_FLG, CRO_ACTV_PD_FLG, NCBK_ACTV_PD_FLG, CRO_ANNUAL_FLG, CRMG_CANC_BAD_DBT_FLG, CRO_CANC_BAD_DBT_FLG, CRMG_CANC_CUST_FLG, CRO_CANC_CUST_FLG, CRO_CR_STAT_CD, CRO_EXP_DT, CRMG_LST_CANC_BAD_DBT_DT, CRMG_LST_CANC_CUST_DT, CRO_LST_CANC_CUST_DT, NCBK_LST_ORD_DT, CRO_LST_PMT_DT, APS_FLG, CRMG_FLG, CRO_FLG, NCBK_FLG, UC_RPTS_FLG, UCBK_FLG, CRO_CURR_MBR_KEYCODE, CRO_CURR_ORD_KEYCODE, CRO_FST_MBR_KEYCODE, NCBK_FST_ORD_KEYCODE, CRO_LST_ORD_KEYCODE, NCBK_LST_ORD_KEYCODE, CRO_LST_SUB_AMT, CRO_MONTHLY_FLG, CRO_NON_SUB_DNR_FLG, CRO_BRKS_CNT, CRO_RNW_CNT, CRO_REC_FLG, CRO_SVC_STAT_CD, CRO_CURR_MBR_SRC_CD, CRO_CURR_ORD_SRC_CD, CRO_FST_ORD_SRC_CD, NCBK_FST_ORD_SRC_CD, CRO_LST_ORD_SRC_CD, NCBK_LST_ORD_SRC_CD, CRO_PRIOR_MBR_SRC_CD, CRO_SUB_DNR_FLG, CRO_CURR_ORD_TERM, CRO_FST_DNR_DT, CRO_AUTO_RNW_FLG, CRO_NON_DNR_FLG, CRO_CURR_MBR_DT, CRO_LTD_PD_AMT, CRO_CURR_ORD_DT, CRO_FST_ORD_DT, NCBK_FST_ORD_DT, CRO_LST_CANC_BAD_DBT_DT, CRO_LST_ORD_DT, CRO_LST_DNR_ORD_DT, CRMG_CANC_CUST_CNT, CRO_CANC_CUST_CNT, CRO_ORD_CNT, CRO_DM_ORD_CNT, CRO_EM_ORD_CNT, CRO_DNR_ORD_CNT, CRO_LST_SUB_ORD_ROLE_CD, LST_LOGN_DT, LOGN_CNT, CRO_LT_SUB_FLG, CARP_ACTV_FLG, CARP_ACTV_PD_FLG
from cr_temp.gtt_agg_digital_summary
)

group by ICD_ID, CDH_ID                                                              , CRO_ACTV_FLG, CRO_ACTV_PD_FLG, NCBK_ACTV_PD_FLG, CRO_ANNUAL_FLG, CRMG_CANC_BAD_DBT_FLG, CRO_CANC_BAD_DBT_FLG, CRMG_CANC_CUST_FLG, CRO_CANC_CUST_FLG, CRO_CR_STAT_CD, CRO_EXP_DT, CRMG_LST_CANC_BAD_DBT_DT, CRMG_LST_CANC_CUST_DT, CRO_LST_CANC_CUST_DT, NCBK_LST_ORD_DT, CRO_LST_PMT_DT, APS_FLG, CRMG_FLG, CRO_FLG, NCBK_FLG, UC_RPTS_FLG, UCBK_FLG, CRO_CURR_MBR_KEYCODE, CRO_CURR_ORD_KEYCODE, CRO_FST_MBR_KEYCODE, NCBK_FST_ORD_KEYCODE, CRO_LST_ORD_KEYCODE, NCBK_LST_ORD_KEYCODE, CRO_LST_SUB_AMT, CRO_MONTHLY_FLG, CRO_NON_SUB_DNR_FLG, CRO_BRKS_CNT, CRO_RNW_CNT, CRO_REC_FLG, CRO_SVC_STAT_CD, CRO_CURR_MBR_SRC_CD, CRO_CURR_ORD_SRC_CD, CRO_FST_ORD_SRC_CD, NCBK_FST_ORD_SRC_CD, CRO_LST_ORD_SRC_CD, NCBK_LST_ORD_SRC_CD, CRO_PRIOR_MBR_SRC_CD, CRO_SUB_DNR_FLG, CRO_CURR_ORD_TERM, CRO_FST_DNR_DT, CRO_AUTO_RNW_FLG, CRO_NON_DNR_FLG, CRO_CURR_MBR_DT, CRO_LTD_PD_AMT, CRO_CURR_ORD_DT, CRO_FST_ORD_DT, NCBK_FST_ORD_DT, CRO_LST_CANC_BAD_DBT_DT, CRO_LST_ORD_DT, CRO_LST_DNR_ORD_DT, CRMG_CANC_CUST_CNT, CRO_CANC_CUST_CNT, CRO_ORD_CNT, CRO_DM_ORD_CNT, CRO_EM_ORD_CNT, CRO_DNR_ORD_CNT, CRO_LST_SUB_ORD_ROLE_CD, LST_LOGN_DT, LOGN_CNT, CRO_LT_SUB_FLG, CARP_ACTV_FLG, CARP_ACTV_PD_FLG
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select * from hash_diff hd
-- where exists (select null from xxxxxxxxxxxxxxxxxxxxxxxx fullmatch where hd.cdh_id = fullmatch.cdh_id)
;
select to_char(count(distinct icd_id),'FM999,999,999,990') as unique_icd_id, to_char(count(distinct cdh_id),'FM999,999,999,990') as unique_cdh_id
, to_char(count(*),'FM999,999,999,990') as rows_total_count, to_char(count(*)/2,'FM999,999,999,990') as rowshalf_total_count
, to_char((select count(*) from cr_temp.GTT_MT_ONLINE_SUMMARY),'FM999,999,999,990') as total_ICD
, to_char((select count(*) from cr_temp.gtt_agg_digital_summary),'FM999,999,999,990')  as total_CDH 
from cr_temp.diff_mt_online_summary

;
drop table if exists cr_temp.comp_diff_mt_online_summary;
CREATE TABLE cr_temp.comp_diff_mt_online_summary AS
with icd as (select * from cr_temp.diff_mt_online_summary where 1=1 and cnt_icd_fl = 1)
,    cdh as (select * from cr_temp.diff_mt_online_summary where 1=1 and cnt_cdh_fl = 1)
select icd.cnt_icd_fl||cdh.cnt_cdh_fl as match_fl, nvl(icd.icd_id,cdh.icd_id) as icd_id ,cdh.cdh_id

, case when icd.CRO_ACTV_FLG = cdh.CRO_ACTV_FLG then '' else icd.CRO_ACTV_FLG||'<>'||cdh.CRO_ACTV_FLG /*#*/end CRO_ACTV_FLG_diff
, case when icd.CRO_ACTV_PD_FLG = cdh.CRO_ACTV_PD_FLG then '' else icd.CRO_ACTV_PD_FLG||'<>'||cdh.CRO_ACTV_PD_FLG /*#*/end CRO_ACTV_PD_FLG_diff
, case when icd.NCBK_ACTV_PD_FLG = cdh.NCBK_ACTV_PD_FLG then '' else icd.NCBK_ACTV_PD_FLG||'<>'||cdh.NCBK_ACTV_PD_FLG /*#*/end NCBK_ACTV_PD_FLG_diff
, case when icd.CRO_ANNUAL_FLG = cdh.CRO_ANNUAL_FLG then '' else icd.CRO_ANNUAL_FLG||'<>'||cdh.CRO_ANNUAL_FLG /*#*/end CRO_ANNUAL_FLG_diff
, case when icd.CRMG_CANC_BAD_DBT_FLG = cdh.CRMG_CANC_BAD_DBT_FLG then '' else icd.CRMG_CANC_BAD_DBT_FLG||'<>'||cdh.CRMG_CANC_BAD_DBT_FLG /*#*/end CRMG_CANC_BAD_DBT_FLG_diff
, case when icd.CRO_CANC_BAD_DBT_FLG = cdh.CRO_CANC_BAD_DBT_FLG then '' else icd.CRO_CANC_BAD_DBT_FLG||'<>'||cdh.CRO_CANC_BAD_DBT_FLG /*#*/end CRO_CANC_BAD_DBT_FLG_diff
, case when icd.CRMG_CANC_CUST_FLG = cdh.CRMG_CANC_CUST_FLG then '' else icd.CRMG_CANC_CUST_FLG||'<>'||cdh.CRMG_CANC_CUST_FLG /*#*/end CRMG_CANC_CUST_FLG_diff
, case when icd.CRO_CANC_CUST_FLG = cdh.CRO_CANC_CUST_FLG then '' else icd.CRO_CANC_CUST_FLG||'<>'||cdh.CRO_CANC_CUST_FLG /*#*/end CRO_CANC_CUST_FLG_diff
, case when icd.CRO_CR_STAT_CD = cdh.CRO_CR_STAT_CD then '' else icd.CRO_CR_STAT_CD||'<>'||cdh.CRO_CR_STAT_CD /*#*/end CRO_CR_STAT_CD_diff
, case when icd.CRO_EXP_DT = cdh.CRO_EXP_DT then '' else icd.CRO_EXP_DT||'<>'||cdh.CRO_EXP_DT /*#*/end CRO_EXP_DT_diff
, case when icd.CRMG_LST_CANC_BAD_DBT_DT = cdh.CRMG_LST_CANC_BAD_DBT_DT then '' else icd.CRMG_LST_CANC_BAD_DBT_DT||'<>'||cdh.CRMG_LST_CANC_BAD_DBT_DT /*#*/end CRMG_LST_CANC_BAD_DBT_DT_diff
, case when icd.CRMG_LST_CANC_CUST_DT = cdh.CRMG_LST_CANC_CUST_DT then '' else icd.CRMG_LST_CANC_CUST_DT||'<>'||cdh.CRMG_LST_CANC_CUST_DT /*#*/end CRMG_LST_CANC_CUST_DT_diff
, case when icd.CRO_LST_CANC_CUST_DT = cdh.CRO_LST_CANC_CUST_DT then '' else icd.CRO_LST_CANC_CUST_DT||'<>'||cdh.CRO_LST_CANC_CUST_DT /*#*/end CRO_LST_CANC_CUST_DT_diff
, case when icd.NCBK_LST_ORD_DT = cdh.NCBK_LST_ORD_DT then '' else icd.NCBK_LST_ORD_DT||'<>'||cdh.NCBK_LST_ORD_DT /*#*/end NCBK_LST_ORD_DT_diff
, case when icd.CRO_LST_PMT_DT = cdh.CRO_LST_PMT_DT then '' else icd.CRO_LST_PMT_DT||'<>'||cdh.CRO_LST_PMT_DT /*#*/end CRO_LST_PMT_DT_diff
, case when icd.APS_FLG = cdh.APS_FLG then '' else icd.APS_FLG||'<>'||cdh.APS_FLG /*#*/end APS_FLG_diff
, case when icd.CRMG_FLG = cdh.CRMG_FLG then '' else icd.CRMG_FLG||'<>'||cdh.CRMG_FLG /*#*/end CRMG_FLG_diff
, case when icd.CRO_FLG = cdh.CRO_FLG then '' else icd.CRO_FLG||'<>'||cdh.CRO_FLG /*#*/end CRO_FLG_diff
, case when icd.NCBK_FLG = cdh.NCBK_FLG then '' else icd.NCBK_FLG||'<>'||cdh.NCBK_FLG /*#*/end NCBK_FLG_diff
, case when icd.UC_RPTS_FLG = cdh.UC_RPTS_FLG then '' else icd.UC_RPTS_FLG||'<>'||cdh.UC_RPTS_FLG /*#*/end UC_RPTS_FLG_diff
, case when icd.UCBK_FLG = cdh.UCBK_FLG then '' else icd.UCBK_FLG||'<>'||cdh.UCBK_FLG /*#*/end UCBK_FLG_diff
, case when icd.CRO_CURR_MBR_KEYCODE = cdh.CRO_CURR_MBR_KEYCODE then '' else icd.CRO_CURR_MBR_KEYCODE||'<>'||cdh.CRO_CURR_MBR_KEYCODE /*#*/end CRO_CURR_MBR_KEYCODE_diff
, case when icd.CRO_CURR_ORD_KEYCODE = cdh.CRO_CURR_ORD_KEYCODE then '' else icd.CRO_CURR_ORD_KEYCODE||'<>'||cdh.CRO_CURR_ORD_KEYCODE /*#*/end CRO_CURR_ORD_KEYCODE_diff
, case when icd.CRO_FST_MBR_KEYCODE = cdh.CRO_FST_MBR_KEYCODE then '' else icd.CRO_FST_MBR_KEYCODE||'<>'||cdh.CRO_FST_MBR_KEYCODE /*#*/end CRO_FST_MBR_KEYCODE_diff
, case when icd.NCBK_FST_ORD_KEYCODE = cdh.NCBK_FST_ORD_KEYCODE then '' else icd.NCBK_FST_ORD_KEYCODE||'<>'||cdh.NCBK_FST_ORD_KEYCODE /*#*/end NCBK_FST_ORD_KEYCODE_diff
, case when icd.CRO_LST_ORD_KEYCODE = cdh.CRO_LST_ORD_KEYCODE then '' else icd.CRO_LST_ORD_KEYCODE||'<>'||cdh.CRO_LST_ORD_KEYCODE /*#*/end CRO_LST_ORD_KEYCODE_diff
, case when icd.NCBK_LST_ORD_KEYCODE = cdh.NCBK_LST_ORD_KEYCODE then '' else icd.NCBK_LST_ORD_KEYCODE||'<>'||cdh.NCBK_LST_ORD_KEYCODE /*#*/end NCBK_LST_ORD_KEYCODE_diff
, case when icd.CRO_LST_SUB_AMT = cdh.CRO_LST_SUB_AMT then '' else icd.CRO_LST_SUB_AMT||'<>'||cdh.CRO_LST_SUB_AMT /*#*/end CRO_LST_SUB_AMT_diff
, case when icd.CRO_MONTHLY_FLG = cdh.CRO_MONTHLY_FLG then '' else icd.CRO_MONTHLY_FLG||'<>'||cdh.CRO_MONTHLY_FLG /*#*/end CRO_MONTHLY_FLG_diff
, case when icd.CRO_NON_SUB_DNR_FLG = cdh.CRO_NON_SUB_DNR_FLG then '' else icd.CRO_NON_SUB_DNR_FLG||'<>'||cdh.CRO_NON_SUB_DNR_FLG /*#*/end CRO_NON_SUB_DNR_FLG_diff
, case when icd.CRO_BRKS_CNT = cdh.CRO_BRKS_CNT then '' else icd.CRO_BRKS_CNT||'<>'||cdh.CRO_BRKS_CNT /*#*/end CRO_BRKS_CNT_diff
, case when icd.CRO_RNW_CNT = cdh.CRO_RNW_CNT then '' else icd.CRO_RNW_CNT||'<>'||cdh.CRO_RNW_CNT /*#*/end CRO_RNW_CNT_diff
, case when icd.CRO_REC_FLG = cdh.CRO_REC_FLG then '' else icd.CRO_REC_FLG||'<>'||cdh.CRO_REC_FLG /*#*/end CRO_REC_FLG_diff
, case when icd.CRO_SVC_STAT_CD = cdh.CRO_SVC_STAT_CD then '' else icd.CRO_SVC_STAT_CD||'<>'||cdh.CRO_SVC_STAT_CD /*#*/end CRO_SVC_STAT_CD_diff
, case when icd.CRO_CURR_MBR_SRC_CD = cdh.CRO_CURR_MBR_SRC_CD then '' else icd.CRO_CURR_MBR_SRC_CD||'<>'||cdh.CRO_CURR_MBR_SRC_CD /*#*/end CRO_CURR_MBR_SRC_CD_diff
, case when icd.CRO_CURR_ORD_SRC_CD = cdh.CRO_CURR_ORD_SRC_CD then '' else icd.CRO_CURR_ORD_SRC_CD||'<>'||cdh.CRO_CURR_ORD_SRC_CD /*#*/end CRO_CURR_ORD_SRC_CD_diff
, case when icd.CRO_FST_ORD_SRC_CD = cdh.CRO_FST_ORD_SRC_CD then '' else icd.CRO_FST_ORD_SRC_CD||'<>'||cdh.CRO_FST_ORD_SRC_CD /*#*/end CRO_FST_ORD_SRC_CD_diff
, case when icd.NCBK_FST_ORD_SRC_CD = cdh.NCBK_FST_ORD_SRC_CD then '' else icd.NCBK_FST_ORD_SRC_CD||'<>'||cdh.NCBK_FST_ORD_SRC_CD /*#*/end NCBK_FST_ORD_SRC_CD_diff
, case when icd.CRO_LST_ORD_SRC_CD = cdh.CRO_LST_ORD_SRC_CD then '' else icd.CRO_LST_ORD_SRC_CD||'<>'||cdh.CRO_LST_ORD_SRC_CD /*#*/end CRO_LST_ORD_SRC_CD_diff
, case when icd.NCBK_LST_ORD_SRC_CD = cdh.NCBK_LST_ORD_SRC_CD then '' else icd.NCBK_LST_ORD_SRC_CD||'<>'||cdh.NCBK_LST_ORD_SRC_CD /*#*/end NCBK_LST_ORD_SRC_CD_diff
, case when icd.CRO_PRIOR_MBR_SRC_CD = cdh.CRO_PRIOR_MBR_SRC_CD then '' else icd.CRO_PRIOR_MBR_SRC_CD||'<>'||cdh.CRO_PRIOR_MBR_SRC_CD /*#*/end CRO_PRIOR_MBR_SRC_CD_diff
, case when icd.CRO_SUB_DNR_FLG = cdh.CRO_SUB_DNR_FLG then '' else icd.CRO_SUB_DNR_FLG||'<>'||cdh.CRO_SUB_DNR_FLG /*#*/end CRO_SUB_DNR_FLG_diff
, case when icd.CRO_CURR_ORD_TERM = cdh.CRO_CURR_ORD_TERM then '' else icd.CRO_CURR_ORD_TERM||'<>'||cdh.CRO_CURR_ORD_TERM /*#*/end CRO_CURR_ORD_TERM_diff
, case when icd.CRO_FST_DNR_DT = cdh.CRO_FST_DNR_DT then '' else icd.CRO_FST_DNR_DT||'<>'||cdh.CRO_FST_DNR_DT /*#*/end CRO_FST_DNR_DT_diff
, case when icd.CRO_AUTO_RNW_FLG = cdh.CRO_AUTO_RNW_FLG then '' else icd.CRO_AUTO_RNW_FLG||'<>'||cdh.CRO_AUTO_RNW_FLG /*#*/end CRO_AUTO_RNW_FLG_diff
, case when icd.CRO_NON_DNR_FLG = cdh.CRO_NON_DNR_FLG then '' else icd.CRO_NON_DNR_FLG||'<>'||cdh.CRO_NON_DNR_FLG /*#*/end CRO_NON_DNR_FLG_diff
, case when icd.CRO_CURR_MBR_DT = cdh.CRO_CURR_MBR_DT then '' else icd.CRO_CURR_MBR_DT||'<>'||cdh.CRO_CURR_MBR_DT /*#*/end CRO_CURR_MBR_DT_diff
, case when icd.CRO_LTD_PD_AMT = cdh.CRO_LTD_PD_AMT then '' else icd.CRO_LTD_PD_AMT||'<>'||cdh.CRO_LTD_PD_AMT /*#*/end CRO_LTD_PD_AMT_diff
, case when icd.CRO_CURR_ORD_DT = cdh.CRO_CURR_ORD_DT then '' else icd.CRO_CURR_ORD_DT||'<>'||cdh.CRO_CURR_ORD_DT /*#*/end CRO_CURR_ORD_DT_diff
, case when icd.CRO_FST_ORD_DT = cdh.CRO_FST_ORD_DT then '' else icd.CRO_FST_ORD_DT||'<>'||cdh.CRO_FST_ORD_DT /*#*/end CRO_FST_ORD_DT_diff
, case when icd.NCBK_FST_ORD_DT = cdh.NCBK_FST_ORD_DT then '' else icd.NCBK_FST_ORD_DT||'<>'||cdh.NCBK_FST_ORD_DT /*#*/end NCBK_FST_ORD_DT_diff
, case when icd.CRO_LST_CANC_BAD_DBT_DT = cdh.CRO_LST_CANC_BAD_DBT_DT then '' else icd.CRO_LST_CANC_BAD_DBT_DT||'<>'||cdh.CRO_LST_CANC_BAD_DBT_DT /*#*/end CRO_LST_CANC_BAD_DBT_DT_diff
, case when icd.CRO_LST_ORD_DT = cdh.CRO_LST_ORD_DT then '' else icd.CRO_LST_ORD_DT||'<>'||cdh.CRO_LST_ORD_DT /*#*/end CRO_LST_ORD_DT_diff
, case when icd.CRO_LST_DNR_ORD_DT = cdh.CRO_LST_DNR_ORD_DT then '' else icd.CRO_LST_DNR_ORD_DT||'<>'||cdh.CRO_LST_DNR_ORD_DT /*#*/end CRO_LST_DNR_ORD_DT_diff
, case when icd.CRMG_CANC_CUST_CNT = cdh.CRMG_CANC_CUST_CNT then '' else icd.CRMG_CANC_CUST_CNT||'<>'||cdh.CRMG_CANC_CUST_CNT /*#*/end CRMG_CANC_CUST_CNT_diff
, case when icd.CRO_CANC_CUST_CNT = cdh.CRO_CANC_CUST_CNT then '' else icd.CRO_CANC_CUST_CNT||'<>'||cdh.CRO_CANC_CUST_CNT /*#*/end CRO_CANC_CUST_CNT_diff
, case when icd.CRO_ORD_CNT = cdh.CRO_ORD_CNT then '' else icd.CRO_ORD_CNT||'<>'||cdh.CRO_ORD_CNT /*#*/end CRO_ORD_CNT_diff
, case when icd.CRO_DM_ORD_CNT = cdh.CRO_DM_ORD_CNT then '' else icd.CRO_DM_ORD_CNT||'<>'||cdh.CRO_DM_ORD_CNT /*#*/end CRO_DM_ORD_CNT_diff
, case when icd.CRO_EM_ORD_CNT = cdh.CRO_EM_ORD_CNT then '' else icd.CRO_EM_ORD_CNT||'<>'||cdh.CRO_EM_ORD_CNT /*#*/end CRO_EM_ORD_CNT_diff
, case when icd.CRO_DNR_ORD_CNT = cdh.CRO_DNR_ORD_CNT then '' else icd.CRO_DNR_ORD_CNT||'<>'||cdh.CRO_DNR_ORD_CNT /*#*/end CRO_DNR_ORD_CNT_diff
, case when icd.CRO_LST_SUB_ORD_ROLE_CD = cdh.CRO_LST_SUB_ORD_ROLE_CD then '' else icd.CRO_LST_SUB_ORD_ROLE_CD||'<>'||cdh.CRO_LST_SUB_ORD_ROLE_CD /*#*/end CRO_LST_SUB_ORD_ROLE_CD_diff
, case when icd.LST_LOGN_DT = cdh.LST_LOGN_DT then '' else icd.LST_LOGN_DT||'<>'||cdh.LST_LOGN_DT /*#*/end LST_LOGN_DT_diff
, case when icd.LOGN_CNT = cdh.LOGN_CNT then '' else icd.LOGN_CNT||'<>'||cdh.LOGN_CNT /*#*/end LOGN_CNT_diff
, case when icd.CRO_LT_SUB_FLG = cdh.CRO_LT_SUB_FLG then '' else icd.CRO_LT_SUB_FLG||'<>'||cdh.CRO_LT_SUB_FLG /*#*/end CRO_LT_SUB_FLG_diff
, case when icd.CARP_ACTV_FLG = cdh.CARP_ACTV_FLG then '' else icd.CARP_ACTV_FLG||'<>'||cdh.CARP_ACTV_FLG /*#*/end CARP_ACTV_FLG_diff
, case when icd.CARP_ACTV_PD_FLG = cdh.CARP_ACTV_PD_FLG then '' else icd.CARP_ACTV_PD_FLG||'<>'||cdh.CARP_ACTV_PD_FLG /*#*/end CARP_ACTV_PD_FLG_diff

/*, '||                    ICD                    >>' as sep1/*, icd.* */*/
/*, '||                    CDH                    >>' as sep2/*, cdh.**/*/

from  icd full join cdh
on icd.icd_id = cdh.icd_id and icd.cdh_id = cdh.cdh_id
/*additional joins for PK*/
;
select 'MT_ONLINE_SUMMARY' as icd_table_name, 'agg_digital_summary' as cdh_table_name

, to_char(sum( case when CRO_ACTV_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_ACTV_FLG
, to_char(sum( case when CRO_ACTV_PD_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_ACTV_PD_FLG
, to_char(sum( case when NCBK_ACTV_PD_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/NCBK_ACTV_PD_FLG
, to_char(sum( case when CRO_ANNUAL_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_ANNUAL_FLG
, to_char(sum( case when CRMG_CANC_BAD_DBT_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRMG_CANC_BAD_DBT_FLG
, to_char(sum( case when CRO_CANC_BAD_DBT_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_CANC_BAD_DBT_FLG
, to_char(sum( case when CRMG_CANC_CUST_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRMG_CANC_CUST_FLG
, to_char(sum( case when CRO_CANC_CUST_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_CANC_CUST_FLG
, to_char(sum( case when CRO_CR_STAT_CD_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_CR_STAT_CD
, to_char(sum( case when CRO_EXP_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_EXP_DT
, to_char(sum( case when CRMG_LST_CANC_BAD_DBT_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRMG_LST_CANC_BAD_DBT_DT
, to_char(sum( case when CRMG_LST_CANC_CUST_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRMG_LST_CANC_CUST_DT
, to_char(sum( case when CRO_LST_CANC_CUST_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_LST_CANC_CUST_DT
, to_char(sum( case when NCBK_LST_ORD_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/NCBK_LST_ORD_DT
, to_char(sum( case when CRO_LST_PMT_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_LST_PMT_DT
, to_char(sum( case when APS_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/APS_FLG
, to_char(sum( case when CRMG_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRMG_FLG
, to_char(sum( case when CRO_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_FLG
, to_char(sum( case when NCBK_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/NCBK_FLG
, to_char(sum( case when UC_RPTS_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/UC_RPTS_FLG
, to_char(sum( case when UCBK_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/UCBK_FLG
, to_char(sum( case when CRO_CURR_MBR_KEYCODE_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_CURR_MBR_KEYCODE
, to_char(sum( case when CRO_CURR_ORD_KEYCODE_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_CURR_ORD_KEYCODE
, to_char(sum( case when CRO_FST_MBR_KEYCODE_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_FST_MBR_KEYCODE
, to_char(sum( case when NCBK_FST_ORD_KEYCODE_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/NCBK_FST_ORD_KEYCODE
, to_char(sum( case when CRO_LST_ORD_KEYCODE_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_LST_ORD_KEYCODE
, to_char(sum( case when NCBK_LST_ORD_KEYCODE_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/NCBK_LST_ORD_KEYCODE
, to_char(sum( case when CRO_LST_SUB_AMT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_LST_SUB_AMT
, to_char(sum( case when CRO_MONTHLY_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_MONTHLY_FLG
, to_char(sum( case when CRO_NON_SUB_DNR_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_NON_SUB_DNR_FLG
, to_char(sum( case when CRO_BRKS_CNT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_BRKS_CNT
, to_char(sum( case when CRO_RNW_CNT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_RNW_CNT
, to_char(sum( case when CRO_REC_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_REC_FLG
, to_char(sum( case when CRO_SVC_STAT_CD_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_SVC_STAT_CD
, to_char(sum( case when CRO_CURR_MBR_SRC_CD_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_CURR_MBR_SRC_CD
, to_char(sum( case when CRO_CURR_ORD_SRC_CD_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_CURR_ORD_SRC_CD
, to_char(sum( case when CRO_FST_ORD_SRC_CD_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_FST_ORD_SRC_CD
, to_char(sum( case when NCBK_FST_ORD_SRC_CD_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/NCBK_FST_ORD_SRC_CD
, to_char(sum( case when CRO_LST_ORD_SRC_CD_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_LST_ORD_SRC_CD
, to_char(sum( case when NCBK_LST_ORD_SRC_CD_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/NCBK_LST_ORD_SRC_CD
, to_char(sum( case when CRO_PRIOR_MBR_SRC_CD_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_PRIOR_MBR_SRC_CD
, to_char(sum( case when CRO_SUB_DNR_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_SUB_DNR_FLG
, to_char(sum( case when CRO_CURR_ORD_TERM_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_CURR_ORD_TERM
, to_char(sum( case when CRO_FST_DNR_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_FST_DNR_DT
, to_char(sum( case when CRO_AUTO_RNW_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_AUTO_RNW_FLG
, to_char(sum( case when CRO_NON_DNR_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_NON_DNR_FLG
, to_char(sum( case when CRO_CURR_MBR_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_CURR_MBR_DT
, to_char(sum( case when CRO_LTD_PD_AMT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_LTD_PD_AMT
, to_char(sum( case when CRO_CURR_ORD_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_CURR_ORD_DT
, to_char(sum( case when CRO_FST_ORD_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_FST_ORD_DT
, to_char(sum( case when NCBK_FST_ORD_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/NCBK_FST_ORD_DT
, to_char(sum( case when CRO_LST_CANC_BAD_DBT_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_LST_CANC_BAD_DBT_DT
, to_char(sum( case when CRO_LST_ORD_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_LST_ORD_DT
, to_char(sum( case when CRO_LST_DNR_ORD_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_LST_DNR_ORD_DT
, to_char(sum( case when CRMG_CANC_CUST_CNT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRMG_CANC_CUST_CNT
, to_char(sum( case when CRO_CANC_CUST_CNT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_CANC_CUST_CNT
, to_char(sum( case when CRO_ORD_CNT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_ORD_CNT
, to_char(sum( case when CRO_DM_ORD_CNT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_DM_ORD_CNT
, to_char(sum( case when CRO_EM_ORD_CNT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_EM_ORD_CNT
, to_char(sum( case when CRO_DNR_ORD_CNT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_DNR_ORD_CNT
, to_char(sum( case when CRO_LST_SUB_ORD_ROLE_CD_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_LST_SUB_ORD_ROLE_CD
, to_char(sum( case when LST_LOGN_DT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/LST_LOGN_DT
, to_char(sum( case when LOGN_CNT_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/LOGN_CNT
, to_char(sum( case when CRO_LT_SUB_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CRO_LT_SUB_FLG
, to_char(sum( case when CARP_ACTV_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CARP_ACTV_FLG
, to_char(sum( case when CARP_ACTV_PD_FLG_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/CARP_ACTV_PD_FLG
from cr_temp.comp_diff_mt_online_summary
;
select top 1000 
cro_curr_mbr_keycode_diff, cro_rnw_cnt_diff, cro_curr_mbr_src_cd_diff, cro_curr_mbr_dt_diff
,* 
--,  CRO_ACTV_FLG_diff
--,  CRO_ACTV_PD_FLG_diff
--,  NCBK_ACTV_PD_FLG_diff
--,  CRO_ANNUAL_FLG_diff
--,  CRMG_CANC_BAD_DBT_FLG_diff
--,  CRO_CANC_BAD_DBT_FLG_diff
--,  CRMG_CANC_CUST_FLG_diff
--,  CRO_CANC_CUST_FLG_diff
--,  CRO_CR_STAT_CD_diff
--,  CRO_EXP_DT_diff
--,  CRMG_LST_CANC_BAD_DBT_DT_diff
--,  CRMG_LST_CANC_CUST_DT_diff
--,  CRO_LST_CANC_CUST_DT_diff
--,  NCBK_LST_ORD_DT_diff
--,  CRO_LST_PMT_DT_diff
--,  APS_FLG_diff
--,  CRMG_FLG_diff
--,  CRO_FLG_diff
--,  NCBK_FLG_diff
--,  UC_RPTS_FLG_diff
--,  UCBK_FLG_diff
--,  CRO_CURR_MBR_KEYCODE_diff
--,  CRO_CURR_ORD_KEYCODE_diff
--,  CRO_FST_MBR_KEYCODE_diff
--,  NCBK_FST_ORD_KEYCODE_diff
--,  CRO_LST_ORD_KEYCODE_diff
--,  NCBK_LST_ORD_KEYCODE_diff
--,  CRO_LST_SUB_AMT_diff
--,  CRO_MONTHLY_FLG_diff
--,  CRO_NON_SUB_DNR_FLG_diff
--,  CRO_BRKS_CNT_diff
--,  CRO_RNW_CNT_diff
--,  CRO_REC_FLG_diff
--,  CRO_SVC_STAT_CD_diff
--,  CRO_CURR_MBR_SRC_CD_diff
--,  CRO_CURR_ORD_SRC_CD_diff
--,  CRO_FST_ORD_SRC_CD_diff
--,  NCBK_FST_ORD_SRC_CD_diff
--,  CRO_LST_ORD_SRC_CD_diff
--,  NCBK_LST_ORD_SRC_CD_diff
--,  CRO_PRIOR_MBR_SRC_CD_diff
--,  CRO_SUB_DNR_FLG_diff
--,  CRO_CURR_ORD_TERM_diff
--,  CRO_FST_DNR_DT_diff
--,  CRO_AUTO_RNW_FLG_diff
--,  CRO_NON_DNR_FLG_diff
--,  CRO_CURR_MBR_DT_diff
--,  CRO_LTD_PD_AMT_diff
--,  CRO_CURR_ORD_DT_diff
--,  CRO_FST_ORD_DT_diff
--,  NCBK_FST_ORD_DT_diff
--,  CRO_LST_CANC_BAD_DBT_DT_diff
--,  CRO_LST_ORD_DT_diff
--,  CRO_LST_DNR_ORD_DT_diff
--,  CRMG_CANC_CUST_CNT_diff
--,  CRO_CANC_CUST_CNT_diff
--,  CRO_ORD_CNT_diff
--,  CRO_DM_ORD_CNT_diff
--,  CRO_EM_ORD_CNT_diff
--,  CRO_DNR_ORD_CNT_diff
--,  CRO_LST_SUB_ORD_ROLE_CD_diff
--,  LST_LOGN_DT_diff
--,  LOGN_CNT_diff
--,  CRO_LT_SUB_FLG_diff
--,  CARP_ACTV_FLG_diff
--,  CARP_ACTV_PD_FLG_diff
from cr_temp.comp_diff_mt_online_summary
 where 1=1 and ( 1=2

--or CRO_ACTV_FLG_diff             != ''
--or CRO_ACTV_PD_FLG_diff          != ''
--or NCBK_ACTV_PD_FLG_diff         != ''
--or CRO_ANNUAL_FLG_diff           != ''
--or CRMG_CANC_BAD_DBT_FLG_diff    != ''
--or CRO_CANC_BAD_DBT_FLG_diff     != ''
--or CRMG_CANC_CUST_FLG_diff       != ''
--or CRO_CANC_CUST_FLG_diff        != ''
--or CRO_CR_STAT_CD_diff           != ''
--or CRO_EXP_DT_diff               != ''
--or CRMG_LST_CANC_BAD_DBT_DT_diff != ''
--or CRMG_LST_CANC_CUST_DT_diff    != ''
--or CRO_LST_CANC_CUST_DT_diff     != ''
--or NCBK_LST_ORD_DT_diff          != ''
--or CRO_LST_PMT_DT_diff           != ''
--or APS_FLG_diff                  != ''
--or CRMG_FLG_diff                 != ''
--or CRO_FLG_diff                  != ''
--or NCBK_FLG_diff                 != ''
--or UC_RPTS_FLG_diff              != ''
--or UCBK_FLG_diff                 != ''
or CRO_CURR_MBR_KEYCODE_diff     != ''
--or CRO_CURR_ORD_KEYCODE_diff     != ''
--or CRO_FST_MBR_KEYCODE_diff      != ''
--or NCBK_FST_ORD_KEYCODE_diff     != ''
--or CRO_LST_ORD_KEYCODE_diff      != ''
--or NCBK_LST_ORD_KEYCODE_diff     != ''
--or CRO_LST_SUB_AMT_diff          != ''
--or CRO_MONTHLY_FLG_diff          != ''
--or CRO_NON_SUB_DNR_FLG_diff      != ''
--or CRO_BRKS_CNT_diff             != ''
or CRO_RNW_CNT_diff              != ''
--or CRO_REC_FLG_diff              != ''
--or CRO_SVC_STAT_CD_diff          != ''
or CRO_CURR_MBR_SRC_CD_diff      != ''
--or CRO_CURR_ORD_SRC_CD_diff      != ''
--or CRO_FST_ORD_SRC_CD_diff       != ''
--or NCBK_FST_ORD_SRC_CD_diff      != ''
--or CRO_LST_ORD_SRC_CD_diff       != ''
--or NCBK_LST_ORD_SRC_CD_diff      != ''
--or CRO_PRIOR_MBR_SRC_CD_diff     != ''
--or CRO_SUB_DNR_FLG_diff          != ''
--or CRO_CURR_ORD_TERM_diff        != ''
--or CRO_FST_DNR_DT_diff           != ''
--or CRO_AUTO_RNW_FLG_diff         != ''
--or CRO_NON_DNR_FLG_diff          != ''
or CRO_CURR_MBR_DT_diff          != ''
--or CRO_LTD_PD_AMT_diff           != ''
--or CRO_CURR_ORD_DT_diff          != ''
--or CRO_FST_ORD_DT_diff           != ''
--or NCBK_FST_ORD_DT_diff          != ''
--or CRO_LST_CANC_BAD_DBT_DT_diff  != ''
--or CRO_LST_ORD_DT_diff           != ''
--or CRO_LST_DNR_ORD_DT_diff       != ''
--or CRMG_CANC_CUST_CNT_diff       != ''
--or CRO_CANC_CUST_CNT_diff        != ''
--or CRO_ORD_CNT_diff              != ''
--or CRO_DM_ORD_CNT_diff           != ''
--or CRO_EM_ORD_CNT_diff           != ''
--or CRO_DNR_ORD_CNT_diff          != ''
--or CRO_LST_SUB_ORD_ROLE_CD_diff  != ''
--or LST_LOGN_DT_diff              != ''
--or LOGN_CNT_diff                 != ''
--or CRO_LT_SUB_FLG_diff           != ''
--or CARP_ACTV_FLG_diff            != ''
--or CARP_ACTV_PD_FLG_diff         != ''
)

;
------------------------------------------------------------------------RUN ALL ABOVE


;
--select * from   cr_temp.GTT_MT_ONLINE_SUMMARY    where icd_id = 00000000000000000000
--;
--select * from   cr_temp.gtt_agg_digital_summary  where cdh_id = 00000000000000000000




;
select count(*) from   cr_temp.GTT_MT_ONLINE_SUMMARY  
;
select count(*) from   cr_temp.gtt_agg_digital_summary
;




												
