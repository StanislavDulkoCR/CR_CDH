/* TEMP AGG digitem_external_ref_temp */
DROP TABLE IF EXISTS    cr_temp.digitem_external_ref_temp;
CREATE TABLE            cr_temp.digitem_external_ref_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(hash_account_id)
as
select distinct 
  INDIVIDUAL_ID
, HASH_ACCOUNT_ID
, source_name
, account_subtype_code
, source_account_id as acct_id
from PROD.account
where INDIVIDUAL_ID is not null;


 
 
DROP TABLE IF EXISTS    cr_temp.digitem_itm_ord_sub_acx_temp;
CREATE TABLE            cr_temp.digitem_itm_ord_sub_acx_temp
       DISTSTYLE KEY DISTKEY(OL_HASH_ACCOUNT_ID)
       SORTKEY(OLI_ORD_ID, OLO_ORD_ID, OLS_ORD_ID)
as
select
  ah.INDIVIDUAL_ID                 as INDIVIDUAL_ID
, ah.HASH_ACCOUNT_ID               as HASH_ACCOUNT_ID
-- Action Header - Duplicate columns for efficiency - Very short on time here
, ah.hash_account_id               as OL_HASH_ACCOUNT_ID
, ah.hash_action_id                as OL_HASH_ACTION_ID
-- Online Item 
, ah.action_id                     as OLI_ORD_ID
, ah.source_account_id             as OLI_ACCT_ID 
, ah.hash_account_id               as OLI_HASH_ACCOUNT_ID  
, dtd.line_item_id                 as OLI_ITM_ID
, dtd.line_item_seq                as OLI_SEQ_NUM
, dtd.line_item_status             as OLI_STAT_CD
, dtd.sku_num                      as OLI_SKU_NUM
, dtd.line_item_total_amount       as OLI_TOT_AMT
, dtd.line_item_order_start_date   as OLI_STRT_DT
, dtd.line_item_order_end_date     as OLI_END_DT
, dtd.line_item_magazine_code      as OLI_MAG_CD
, dtd.credit_status                as OLI_CR_STAT_CD
, dtd.line_item_cancel_flag        as OLI_CANC_FLG
, dtd.line_item_term               as OLI_TERM_MTH_CNT
, dtd.renewable_order_ind          as OLI_RNW_FLG
, dtd.set_code                     as OLI_SET_CD
, dtd.external_keycode             as OLI_EXT_KEYCD
, dtd.internal_keycode             as OLI_INT_KEYCD
, dtd.cancel_reason_code           as OLI_CANC_RSN_CD
, dtd.ship_method_code             as OLI_SHP_METH_CD
, dtd.line_item_order_create_date  as OLI_CRT_DT
, dtd.line_item_order_modify_date  as OLI_MDFY_DT
, dtd.report_name                  as OLI_RPT_NAM
, dtd.reference_id                 as OLI_REF_ID
, dtd.line_item_order_end_date     as OLI_PMT_DT            -- SD 20171010: #TODO ONLINE_ITEM.PMT_DT       DataPump#4. Should be PMT_DT
-- Online Order
, ah.action_id                     as OLO_ORD_ID
, ah.source_account_id             as OLO_ACCT_ID 
, ah.hash_account_id               as OLO_HASH_ACCOUNT_ID
, ah.plan_status_code              as OLO_STAT_CD
, dt.order_type                    as OLO_TYP_CD
, ah.action_date                   as OLO_ORD_DT
, ah.action_subtype_code           as OLO_ENTR_TYP_CD
, ah.paid_date                     as OLO_PD_DT
, ah.action_amount                 as OLO_PD_AMT
, dt.create_date                   as OLO_CRT_DT
, ah.update_date                   as OLO_MAINT_DT
, dtd.line_item_order_modify_date  as OLO_MDFY_DT           -- SD 20171010: #TODO ONLINE_ORDER.MDFY_DT     DataPump#4. Should be MDFY_DT
-- Online Subscription
, dtd.line_item_id                 as OLS_ITM_ID
, ah.action_id                     as OLS_ORD_ID
-- , ah.addtl_action_id               as ACCT_ID /*SD 20171010: Account_id mistaken for action_id?*/
, dtd.subscription_id              as OLS_SUB_ID
, dtd.renewable_subsrc_flag        as OLS_RNW_IND
, dtd.payment_status               as OLS_PYMT_STAT_CD
, dtd.service_status               as OLS_SVC_STAT_CD
, dtd.line_item_subsrc_start_date  as OLS_STRT_DT
, dtd.line_item_subsrc_end_date    as OLS_END_DT
, dtd.line_item_subsrc_modify_date as OLS_MDFY_DT
, dtd.line_item_magazine_code      as OLS_MAG_CD
, dtd.times_renewed                as OLS_TMS_RNW_NUM
, dtd.cancel_reason_code           as OLS_CANC_RSN_CD
, dtd.line_item_subsrc_create_date as OLS_CRT_DT
, dtd.update_date                  as OLS_MAINT_DT
, dtd.cancel_date                  as OLS_CANCEL_DT
, dtd.cancelled_by                 as OLS_CANCELLED_BY

from prod.action_header                                 ah 
    inner join prod.digital_transaction                 dt 
        on ah.hash_action_id = dt.hash_action_id
    inner join prod.digital_transaction_detail          dtd 
        on ah.hash_action_id = dtd.hash_action_id;


DROP TABLE IF EXISTS    cr_temp.digitem_sku_lkup_acx_temp;
CREATE TABLE            cr_temp.digitem_sku_lkup_acx_temp
       DISTSTYLE KEY DISTKEY(SKU_SKU_NUM)
       SORTKEY(SKU_PRODUCT)
as
select

  sku.sku_num       as SKU_SKU_NUM
, sku.sub_type_code as SKU_SUB_TYP_CD
, sku.sku_desc      as SKU_SKU_DES
, sku.product       as SKU_PRODUCT
, sku.value         as SKU_VALUE
, sku.unit_flag     as SKU_UNIT_FLG
, sku.selection     as SKU_SELECTION
, sku.comp_flag     as SKU_COMP_FLG
, sku.value_range   as SKU_AMT
, sku.update_date   as SKU_MAINT_DT
, sku.valid_from    as SKU_VALID_FROM
, sku.valid_to      as SKU_VALID_TO

from prod.sku_lkup sku;



DROP TABLE IF EXISTS    cr_temp.digitem_digital_acx_temp;
CREATE TABLE            cr_temp.digitem_digital_acx_temp
       DISTSTYLE KEY DISTKEY(hash_account_id)
       SORTKEY(individual_id)
as
with cte_digital
(  INDIVIDUAL_ID , ITM_ID , ORD_ID , hash_account_id, source_name, account_subtype_code, ACCT_ID , SEQ_NUM , STAT_CD , SKU_NUM , TOT_GROSS_AMT , TOT_AMT , STRT_DT , END_DT , MAG_CD , CRD_STAT_CD , CANC_FLG , TERM_MTH_CNT , RNW_CD , SET_CD , EXT_KEYCD , INT_KEYCD , CANC_RSN_CD , SHP_METH_CD , CRT_DT , RPT_NAM , SUB_ID , SUB_RNW_IND , SVC_STAT_CD , CANC_DT , PMT_DT , SUB_SRC_CD , APS_SRC_CD , CRO_ACCT_EXP_DT , CANCELLED_BY)

 as (
SELECT ol.individual_id
  , ol.oli_itm_id
  , ol.oli_ord_id
  , er.hash_account_id
  , er.source_name
  , er.account_subtype_code
  , er.acct_id 
  , ol.oli_seq_num
  , DECODE(ol.oli_stat_cd,'H','C',ol.oli_stat_cd) AS stat_cd
  , ol.oli_sku_num
  , ol.oli_tot_amt tot_gross_amt
  , (ol.oli_tot_amt + NVL(oli2.tot_amt,0)) tot_amt
  , CASE
        WHEN ol.oli_sku_num > 5000000
        THEN NVL(ol.oli_strt_dt,ol.oli_crt_dt)
        ELSE NVL(ol.ols_strt_dt,NVL(ol.oli_strt_dt,ol.oli_crt_dt))
    END strt_dt
  , CASE
        WHEN ol.ols_end_dt IS NOT NULL
        THEN ol.ols_end_dt
        WHEN ol.oli_end_dt IS NOT NULL
        THEN ol.oli_end_dt
        WHEN NVL(ol.oli_mag_cd,sku.sku_product) IN ('NCPR','UCPR')
        THEN
            CASE
                WHEN ol.oli_sku_num > 5000000
                THEN NVL(ol.oli_strt_dt,ol.oli_crt_dt)
                ELSE NVL(ol.ols_strt_dt,NVL(ol.oli_strt_dt,ol.oli_crt_dt))
            END
        ELSE add_months(
            CASE
                WHEN ol.oli_sku_num > 5000000
                THEN NVL(ol.oli_strt_dt,ol.oli_crt_dt)
                ELSE NVL(ol.ols_strt_dt,NVL(ol.oli_strt_dt,ol.oli_crt_dt))
            END, CAST((
            CASE
                WHEN (ol.oli_term_mth_cnt IS NULL
                    OR ol.oli_term_mth_cnt = 0)
                    AND NVL(ol.oli_mag_cd,sku.sku_product) NOT IN ('NCPR','UCPR')
                THEN
                    CASE
                        WHEN sku.sku_unit_flg = 'D'
                        THEN ceil(sku.sku_value/30)
                        ELSE sku.sku_value
                    END
                ELSE ol.oli_term_mth_cnt
            END) AS bigint))
    END end_dt
  , NVL(ol.oli_mag_cd,sku.sku_product) mag_cd
  , NVL(ol.ols_pymt_stat_cd, NVL(ol.oli_cr_stat_cd, CASE
        WHEN ol.olo_stat_cd IN ('C','H','M','R','T','Y')
        THEN 'C'
        WHEN ol.olo_stat_cd = 'J'
        THEN 'F'
        WHEN ol.olo_stat_cd IN ('P','S','N')
        THEN 'P'
        ELSE NULL
    END)) crd_stat_cd
  , NVL(ol.oli_canc_flg, CASE
        WHEN ol.oli_stat_cd IN ('R','J')
        THEN 'Y'
        ELSE 'N'
    END) canc_flg
  , CASE
        WHEN (ol.oli_term_mth_cnt IS NULL
            OR ol.oli_term_mth_cnt = 0)
            AND NVL(ol.oli_mag_cd,sku.sku_product) NOT IN ('NCPR','UCPR')
        THEN
            CASE
                WHEN sku.sku_unit_flg = 'D'
                THEN ceil(sku.sku_value/30)
                ELSE sku.sku_value
            END
        ELSE ol.oli_term_mth_cnt
    END term_mth_cnt
  , ol.oli_rnw_flg rnw_cd
  , ol.oli_set_cd
  , UPPER(ol.oli_ext_keycd) AS ext_keycd
  , UPPER(ol.oli_int_keycd) AS int_keycd
  , NVL(ol.oli_canc_rsn_cd, CASE
        WHEN ol.ols_svc_stat_cd = 'C'
        THEN '50'
        ELSE NULL
    END) canc_rsn_cd
  , ol.oli_shp_meth_cd
  , CASE
        WHEN ol.oli_mag_cd IN ('CRO','CRMG')
            AND TO_CHAR(ol.oli_crt_dt,'YYYYMMDD') < '20080731'
        THEN NVL(ol.oli_strt_dt,ol.oli_crt_dt)
        ELSE ol.oli_crt_dt
    END crt_dt
  , UPPER(ol.oli_rpt_nam) AS rpt_nam
  , ol.ols_sub_id
  , ol.ols_rnw_ind sub_rnw_ind
  , DECODE(ol.ols_svc_stat_cd,'I','C',ol.ols_svc_stat_cd) svc_stat_cd
  , CASE
        WHEN ol.ols_cancel_dt IS NOT NULL
        THEN ol.ols_cancel_dt
        WHEN ol.ols_svc_stat_cd = 'C'
            OR ol.ols_pymt_stat_cd ='F'
        THEN ol.ols_mdfy_dt
        WHEN ol.oli_stat_cd = 'R'
            AND ol.oli_cr_stat_cd IS NULL
        THEN ol.oli_mdfy_dt
        WHEN ol.oli_cr_stat_cd IS NULL
            AND ol.olo_stat_cd IN ('J','R')
        THEN ol.olo_mdfy_dt
        WHEN ol.oli_cr_stat_cd = 'F'
        THEN ol.oli_mdfy_dt
        ELSE NULL
    END canc_dt
  , CASE
                WHEN NVL(ol.ols_pymt_stat_cd, NVL(ol.oli_cr_stat_cd, CASE
                WHEN ol.olo_stat_cd IN ('C','H','M','R','T','Y')
                THEN 'C'
                WHEN ol.olo_stat_cd = 'J'
                THEN 'F'
                WHEN ol.olo_stat_cd IN ('P','S','N')
                THEN 'P'
                ELSE NULL
            END)) = 'C'
            AND ol.oli_set_cd IN ('A', 'B', 'D')
        THEN NVL(ol.oli_pmt_dt , CASE
                WHEN ol.oli_mag_cd IN ('CRO','CRMG')
                    AND TO_CHAR(ol.oli_crt_dt,'YYYYMMDD') < '20080731'
                THEN NVL(ol.oli_strt_dt,ol.oli_crt_dt)
                ELSE ol.oli_crt_dt
            END)
    END pmt_dt
  , CASE
        WHEN NVL(ol.oli_mag_cd,sku.sku_product) IN ('CRO','CARP')
        THEN
            CASE
                WHEN ol.oli_ext_keycd IS NULL
                    AND ol.oli_int_keycd IS NULL
                THEN 'I'
                WHEN substring(NVL(UPPER(ol.oli_ext_keycd),UPPER(ol.oli_int_keycd)),1,2) = 'WF'
                THEN 'X'
                ELSE substring(NVL(UPPER(ol.oli_ext_keycd),UPPER(ol.oli_int_keycd)),1,1)
            END
        WHEN NVL(ol.oli_mag_cd,sku.sku_product) = 'CRMG'
        THEN
            CASE
                WHEN ol.oli_ext_keycd IS NULL
                    AND ol.oli_int_keycd IS NULL
                THEN 'I'
                WHEN NVL(UPPER(ol.oli_ext_keycd),UPPER(ol.oli_int_keycd)) = 'VOLUNTARY ORDER'
                THEN 'I'
                WHEN substring(NVL(UPPER(ol.oli_ext_keycd),UPPER(ol.oli_int_keycd)),1,1) = 'H'
                THEN substring(NVL(UPPER(ol.oli_ext_keycd),UPPER(ol.oli_int_keycd)),6,1)
                ELSE substring(NVL(UPPER(ol.oli_ext_keycd),UPPER(ol.oli_int_keycd)),1,1)
            END
    END sub_src_cd
  , CASE WHEN nvl(ol.oli_mag_cd,sku.sku_product) IN ('NCBK','NCPR','UCBK','UCPR')
         THEN CASE WHEN substring(ol.oli_int_keycd,1,1) BETWEEN '0' AND '9'
                   THEN '1'
                   ELSE decode(substring(nvl(UPPER(ol.oli_ext_keycd),UPPER(ol.oli_int_keycd)),1,6),
                               'UCBK10','56',
                               'UCBK11','57',
                               'UCBK12','58',
                               'UCBK13','59',
                               'UCBK14','60',
                               'UCBK15','61',
                               decode(substring(nvl(UPPER(ol.oli_ext_keycd),UPPER(ol.oli_int_keycd)),1,5),
                                      'CBK10','42',
                                      'CBK11','43',
                                      'CBK12','44',
                                      'CBK13','45',
                                      'CBK14','46',
                                      'CBK15','47',
                                      'UCBK1','48',
                                      'UCBK2','49',
                                      'UCBK3','50',
                                      'UCBK4','51',
                                      'UCBK5','52',
                                      'UCBK6','53',
                                      'UCBK7','54',
                                      'UCBK8','55',
                                      decode(substring(nvl(UPPER(ol.oli_ext_keycd),UPPER(ol.oli_int_keycd)),1,4),
                                             'NC10','11',
                                             'NC11','12',
                                             'NC12','13',
                                             'NC13','14',
                                             'NC14','15',
                                             'NC15','16',
                                             'UC10','26',
                                             'UC11','27',
                                             'UC12','28',
                                             'UC13','29',
                                             'UC14','30',
                                             'UC15','31',
                                             'CBK1','32',
                                             'CBK2','33',
                                             'CBK3','34',
                                             'CBK4','35',
                                             'CBK5','36',
                                             'CBK6','37',
                                             'CBK7','38',
                                             'CBK8','39',
                                             decode(substring(nvl(UPPER(ol.oli_ext_keycd),UPPER(ol.oli_int_keycd)),1,3),
                                                    'NC1','2',
                                                    'NC2','3',
                                                    'NC3','4',
                                                    'NC4','5',
                                                    'NC5','6',
                                                    'NC6','7',
                                                    'NC7','8',
                                                    'NC8','9',
                                                    'NC9','10',
                                                    'UC1','17',
                                                    'UC2','18',
                                                    'UC3','19',
                                                    'UC4','20',
                                                    'UC5','21',
                                                    'UC6','22',
                                                    'UC7','23',
                                                    'UC8','24',
                                                    'UC9','25',
                                                    'APS','1',
                                                    'NKA','35',
                                                    'NRA','4',
                                                    'UKA','51',
                                                    'URA','19',
                                                    'NKD','39',
                                                    'NRD','5',
                                                    'UKD','55',
                                                    'URD','20',
                                                    'NKE','36',
                                                    'NRE','7',
                                                    'UKE','52',
                                                    'URE','22',
                                                    'NKN','42',
                                                    'NRN','9',
                                                    'UKN','56',
                                                    'URN','24',
                                                    'NKP','34',
                                                    'NRP','2',
                                                    'UKP','50',
                                                    'URP','17',
                                                    'NKL','38',
                                                    'NRL','10',
                                                    'UKL','54',
                                                    'URL','25',
                                                    'NKS','37',
                                                    'NRS','8',
                                                    'UKS','53',
                                                    'URS','23',
                                                    'NKI','32',
                                                    'NRI','6',
                                                    'UKI','48',
                                                    'URI','21',
                                                    null,null,
                                                    decode(nvl(ol.oli_mag_cd,sku.sku_product),
                                                           'NCBK','32',
                                                           'NCPR','6',
                                                           'UCBK','48',
                                                           'UCPR','21')))))
              END                        
         ELSE null
    END aps_src_cd
  , null cro_acct_exp_dt
  , ol.ols_cancelled_by

  FROM cr_temp.digitem_external_ref_temp er 
    inner join cr_temp.digitem_itm_ord_sub_acx_temp ol 
        on      er.hash_account_id = ol.hash_account_id
            and er.individual_id = ol.individual_id
    left join (  select oli_ref_id as ref_id, sum(oli_tot_amt) as tot_amt 
                 from cr_temp.digitem_itm_ord_sub_acx_temp oli_ss
                 where nvl(oli_ref_id,0) != 0
                 group by oli_ref_id) oli2 
        on ol.oli_itm_id = oli2.ref_id
    left join cr_temp.digitem_sku_lkup_acx_temp sku 
        on ol.oli_sku_num = sku.sku_sku_num
  WHERE nvl(ol.oli_ref_id,0) = 0
  )

SELECT INDIVIDUAL_ID
  ,ITM_ID
  ,ORD_ID
  , hash_account_id
  , source_name
  , account_subtype_code
  ,ACCT_ID
  ,SEQ_NUM
  ,STAT_CD
  ,SKU_NUM
  ,TOT_GROSS_AMT
  ,TOT_AMT
  ,STRT_DT
  ,CASE
        WHEN (crd_stat_cd = 'F'
            OR canc_rsn_cd IN ('50','06')
            OR cancelled_by = 'CBFEED')
            AND canc_dt < '8-FEB-2013'
        THEN canc_dt
        ELSE end_dt
    END AS END_DT
  ,MAG_CD
  ,CRD_STAT_CD
  ,CASE
        WHEN (crd_stat_cd = 'F'
            OR canc_rsn_cd IN ('50','06'))
            AND canc_dt < '8-FEB-2013'
        THEN 'Y'
        ELSE canc_flg
    END AS CANC_FLG
  ,TERM_MTH_CNT
  ,RNW_CD
  ,SET_CD
  ,EXT_KEYCD
  ,INT_KEYCD
  ,CANC_RSN_CD
  ,SHP_METH_CD
  ,CRT_DT
  ,RPT_NAM
  ,SUB_ID
  ,SUB_RNW_IND
  ,SVC_STAT_CD
  ,CANC_DT
  ,PMT_DT
  ,SUB_SRC_CD
  ,APS_SRC_CD
  ,CRO_ACCT_EXP_DT
  ,CANCELLED_BY
FROM cte_digital;