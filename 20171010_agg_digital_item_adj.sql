/***************************************************************************
*            (C) Copyright Consumer Reports 2017
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  agg_digital_item.sql
* Date       :  2017/10/11
* Dev & QA   :  Stanislav Dulko
***************************************************************************/

DROP TABLE IF EXISTS    agg.digitem_external_ref_temp;
CREATE TABLE            agg.digitem_external_ref_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(hash_account_id)
as
select distinct 
  individual_id         as individual_id
, hash_account_id       as hash_account_id
, source_name           as source_name
, account_subtype_code  as account_subtype_code
, source_account_id     as acct_id
from PROD.account acc
where INDIVIDUAL_ID is not null
;


DROP TABLE IF EXISTS    agg.digitem_off_canc_acx_temp;
CREATE TABLE            agg.digitem_off_canc_acx_temp
       DISTSTYLE KEY DISTKEY(acct_id)
       SORTKEY(ORD_ID)
as
WITH cte_offcanc_acx AS
    (SELECT
        -- offline cancel
        ah.action_id          AS ORD_ID
      , ah.hash_account_id    AS ACCT_ID
      , pc.paid_amount        AS PD_AMT
      , pc.cancel_reason_code AS CANC_RSN_CD
      , ah.action_date        AS ACT_DT
    FROM prod.action_header ah
        inner JOIN prod.print_cancel pc
            ON  ah.hash_action_id = pc.hash_action_id
    where ah.action_type = 'CANCEL'
    )
, cte_decode as (
SELECT 
    ca.acct_id
  , ca.ord_id
  , SUM(ca.pd_amt) tot_amt
  , substring(MAX(NVL(TO_CHAR(ca.act_dt, 'YYYYMMDD'),'00010101') || nvl(ca.canc_rsn_cd,'')),9) canc_rsn_cd
  , MAX(ca.act_dt) canc_dt
FROM cte_offcanc_acx ca
GROUP BY ca.acct_id
  , ca.ord_id)

select  acct_id
,       ord_id
,       tot_amt
,       decode(canc_rsn_cd,'',null,canc_rsn_cd) as canc_rsn_cd
,       canc_dt
from cte_decode;



DROP TABLE IF EXISTS    agg.digitem_sku_lkup_acx_temp;
CREATE TABLE            agg.digitem_sku_lkup_acx_temp
       DISTSTYLE KEY DISTKEY(SKU_SKU_NUM)
       SORTKEY(SKU_PRODUCT)
as
select

  sku.sku_num       as SKU_SKU_NUM
-- , sku.sub_type_code as SKU_SUB_TYP_CD
-- , sku.sku_desc      as SKU_SKU_DES
, sku.product       as SKU_PRODUCT
, sku.value         as SKU_VALUE
, sku.unit_flag     as SKU_UNIT_FLG
-- , sku.selection     as SKU_SELECTION
-- , sku.comp_flag     as SKU_COMP_FLG
-- , sku.value_range   as SKU_AMT
-- , sku.update_date   as SKU_MAINT_DT
-- , sku.valid_from    as SKU_VALID_FROM
-- , sku.valid_to      as SKU_VALID_TO

from prod.sku_lkup sku;

 


DROP TABLE IF EXISTS    agg.digitem_issue_lkp_acx_temp;
CREATE TABLE            agg.digitem_issue_lkp_acx_temp
       DISTSTYLE KEY DISTKEY(MAG_CD)
       SORTKEY(PUB_DT)
as
select
  lkpmag.magazine_code as MAG_CD
, lkpmag.iss_num       as ISS_NUM
, lkpmag.pub_date      as PUB_DT

from etl_proc.lookup_magazine lkpmag;


/***************************************************************************/
-- DIGITAL ITEM - Digital orders raw data
/***************************************************************************/


DROP TABLE IF EXISTS    agg.digitem_itm_ord_sub_acx_temp;
CREATE TABLE            agg.digitem_itm_ord_sub_acx_temp
       DISTSTYLE KEY DISTKEY(OL_HASH_ACCOUNT_ID)
       SORTKEY(OLI_ORD_ID, OLO_ORD_ID, OLS_ORD_ID)
as
select
  ah.individual_id                 as INDIVIDUAL_ID
, ah.hash_account_id               as HASH_ACCOUNT_ID
, ah.hash_action_id                as HASH_ACTION_ID
, ah.action_type                   as action_type
-- Action Header - Duplicate columns for efficiency - Very short on time here
, ah.hash_account_id               as OL_HASH_ACCOUNT_ID
-- , ah.hash_action_id                as OL_HASH_ACTION_ID
-- Account
, er.individual_id                 as ACC_INDIVIDUAL_ID
, er.hash_account_id               as ACC_HASH_ACCOUNT_ID
, er.source_name                   as ACC_SOURCE_NAME
, er.account_subtype_code          as ACC_ACCOUNT_SUBTYPE_CODE
, er.acct_id                       as ACC_ACCT_ID
-- Online Item 
, ah.action_id                     as OLI_ORD_ID
-- , ah.source_account_id             as OLI_ACCT_ID 
-- , ah.hash_account_id               as OLI_HASH_ACCOUNT_ID  
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
, dtd.line_item_order_end_date     as OLI_PMT_DT            -- SD 20171010: #TODO ONLINE_ITEM.PMT_DT       DataPump#4. Should be PMT_DT. -- Temporary Online ITEM END_DT
-- Online Order
, ah.action_id                     as OLO_ORD_ID
-- , ah.source_account_id             as OLO_ACCT_ID 
-- , ah.hash_account_id               as OLO_HASH_ACCOUNT_ID
, ah.plan_status_code              as OLO_STAT_CD
-- , dt.order_type                    as OLO_TYP_CD
-- , ah.action_date                   as OLO_ORD_DT
-- , ah.action_subtype_code           as OLO_ENTR_TYP_CD
-- , ah.paid_date                     as OLO_PD_DT
-- , ah.action_amount                 as OLO_PD_AMT
-- , dt.create_date                   as OLO_CRT_DT
-- , ah.update_date                   as OLO_MAINT_DT
, dt.order_modified_date           as OLO_MDFY_DT           -- SD 20171010: #TODO ONLINE_ORDER.MDFY_DT     DataPump#4. Should be MDFY_DT. -- Temporary Online ITEM mdfy_dt --SD 2017/10/11 10:12 Changed to digital_transaction.order_modified_date
-- Online Subscription
-- , dtd.line_item_id                 as OLS_ITM_ID
, ah.action_id                     as OLS_ORD_ID
-- , ah.addtl_action_id               as ACCT_ID /*SD 20171010: Account_id mistaken for action_id?*/
, dtd.subscription_id              as OLS_SUB_ID
, dtd.renewable_subsrc_flag        as OLS_RNW_IND
, dtd.payment_status               as OLS_PYMT_STAT_CD
, dtd.service_status               as OLS_SVC_STAT_CD
, dtd.line_item_subsrc_start_date  as OLS_STRT_DT
, dtd.line_item_subsrc_end_date    as OLS_END_DT
, dtd.line_item_subsrc_modify_date as OLS_MDFY_DT
-- , dtd.line_item_magazine_code      as OLS_MAG_CD
-- , dtd.times_renewed                as OLS_TMS_RNW_NUM
-- , dtd.cancel_reason_code           as OLS_CANC_RSN_CD
-- , dtd.line_item_subsrc_create_date as OLS_CRT_DT
-- , dtd.update_date                  as OLS_MAINT_DT
, dtd.cancel_date                  as OLS_CANCEL_DT
, dtd.cancelled_by                 as OLS_CANCELLED_BY

from agg.digitem_external_ref_temp er
    inner join prod.action_header ah
        on er.hash_account_id = ah.hash_account_id
    inner join prod.digital_transaction                 dt 
        on ah.hash_action_id = dt.hash_action_id
    inner join prod.digital_transaction_detail          dtd 
        on ah.hash_action_id = dtd.hash_action_id;


/***************************************************************************/
-- DIGITAL ITEM - Print orders raw data
/***************************************************************************/



DROP TABLE IF EXISTS    agg.digitem_oford_ofacc_acx_temp;
CREATE TABLE            agg.digitem_oford_ofacc_acx_temp
       DISTSTYLE KEY DISTKEY(HASH_ACCOUNT_ID)
       SORTKEY(OFO_ORD_ID)
as
select
  ah.individual_id           as INDIVIDUAL_ID
, ah.hash_account_id         as HASH_ACCOUNT_ID
, ah.hash_action_id          as HASH_ACTION_ID
, ah.action_type             as action_type
-- action header - duplicate columns for efficiency - Very short on time here
-- Account
, er.individual_id           as ACC_INDIVIDUAL_ID
, er.hash_account_id         as ACC_HASH_ACCOUNT_ID
, er.source_name             as ACC_SOURCE_NAME
, er.account_subtype_code    as ACC_ACCOUNT_SUBTYPE_CODE
, er.acct_id                 as ACC_ACCT_ID
-- offline order
, ah.action_id               as OFO_ORD_ID
, ah.hash_account_id         as OFO_ACCT_ID
, ah.addtl_action_id         as OFO_ORD_NUM
, ah.plan_status_code        as OFO_STAT_CD
-- , pt.cplt_date               as OFO_CPLT_DT
-- , ah.action_subtype_code     as OFO_ENTR_TYP_CD
, ah.credit_status_code      as OFO_CR_STAT_CD
, pt.cancel_flag             as OFO_CANC_FLG
, pt.term_month_cnt          as OFO_TERM_MTH_CNT
, ah.action_amount           as OFO_NET_VAL_AMT
, pt.agcy_grs_val_amount     as OFO_AGCY_GRS_VAL_AMT
-- , pt.tax_val_amount          as OFO_TAX_VAL_AMT
-- , pt.ofr_chngd_flag          as OFO_OFR_CHNGD_FLG
, pt.renewal_code            as OFO_RNW_CD
-- , pt.mlt_cpy_flag            as OFO_MLT_CPY_FLG
-- , pt.source_transaction_code as OFO_SRC_CD
-- , pt.md_code                 as OFO_MD_CD
-- , pt.magazine_catg_code      as OFO_MAG_CATG_CD
, pt.set_code                as OFO_SET_CD
-- , pt.bulk_flag               as OFO_BULK_FLG
, ah.keycode                 as OFO_KEYCD
-- , pt.blg_key_num             as OFO_BLG_KEY_NUM
-- , pt.spec_info_txt           as OFO_SPEC_INFO_TXT
-- , pt.agcy_code               as OFO_AGCY_CD
-- , pt.prev_expr_iss_num       as OFO_PREV_EXPR_ISS_NUM
, pt.original_strt_iss_num   as OFO_ORIG_STRT_ISS_NUM
-- , pt.original_order_flag     as OFO_ORIG_ORD_FLAG
, ah.action_date             as OFO_ORD_DT
-- , pt.create_date             as OFO_FIRST_DT
-- , pt.update_date             as OFO_MAINT_DT

-- Offline Account
-- , ah.hash_account_id         as OFA_HASH_ACCOUNT_ID
, pad.magazine_code          as OFA_MAG_CD
-- , pad.address_from_date   as OFA_ADDR_FROM_DT
-- , pad.address_thru_date   as OFA_ADDR_THRU_DT
-- , pad.nlsn_code           as OFA_NLSN_CD
, pad.svc_status_code        as OFA_SVC_STAT_CD
-- , pad.strt_iss_num        as OFA_STRT_ISS_NUM
-- , pad.expr_iss_num        as OFA_EXPR_ISS_NUM
-- , pad.rsm_iss_num         as OFA_RSM_ISS_NUM
-- , pad.iss_extd_cnt        as OFA_ISS_EXTD_CNT
-- , pad.cpy_cnt             as OFA_CPY_CNT
-- , pad.renewal_cnt         as OFA_RNW_CNT
-- , pad.spec_prod_code      as OFA_SPEC_PROD_CD
, pad.keycode                as OFA_KEYCD
-- , pad.gift_keycode        as OFA_GFT_KEYCD
-- , pad.group_1_code        as OFA_GRP_1_CD
-- , pad.group_5_code        as OFA_GRP_5_CD
-- , pad.group_7_code        as OFA_GRP_7_CD
-- , pad.promotion_rst_code  as OFA_PROM_RST_CD
-- , pad.sweep_rsp_flag      as OFA_SWEEP_RSP_FLG
-- , pad.bm_user_name_1      as OFA_BM_USERNAM1
-- , pad.bm_user_name_2      as OFA_BM_USERNAM2
, pad.cro_exp_date           as OFA_CRO_EXP_DT
-- , pad.iss_num             as OFA_LST_ISS_NUM
-- , pad.delivery_type       as OFA_DELY_CD
-- , pad.create_date         as OFA_FIRST_DT
-- , pad.update_date         as OFA_MAINT_DT
-- , pad.purged_ind          as OFA_PURGED_IND




from agg.digitem_external_ref_temp er
    inner join prod.action_header ah
        on er.hash_account_id = ah.hash_account_id
    inner join prod.print_transaction pt 
        on ah.hash_action_id = pt.hash_action_id
    inner join prod.print_account_detail pad 
        on pad.hash_account_id = ah.hash_account_id;






/***************************************************************************/
-- DIGITAL ITEM - Donor - Recipient relationship payment calculation
/***************************************************************************/

DROP TABLE IF EXISTS    agg.digitem_oford_donrec3_temp;
CREATE TABLE            agg.digitem_oford_donrec3_temp
       DISTSTYLE KEY DISTKEY(acct_id)
       SORTKEY(ord_id)
AS
SELECT
  acct_id,
  ord_id,
  (nvl(max(dnr_pd_amt2),0) + nvl(sum(rcpt_pd_amt2),0)) pd_amt2,
  max(ord_dt) pmt_dt2
FROM (
            SELECT ofo.acct_id
              , ofo.ord_id
              , ofo.net_val_amt AS dnr_pd_amt2
              , NULL rcpt_pd_amt2
              , ofo.ord_dt
            FROM (SELECT 
                          hash_account_id
                        , OFO_ORD_ID            as ORD_ID
                        , OFO_ACCT_ID           as ACCT_ID
                        , OFO_NET_VAL_AMT       as NET_VAL_AMT
                        , OFO_AGCY_GRS_VAL_AMT  as AGCY_GRS_VAL_AMT
                        , OFO_SET_CD            as SET_CD
                        , OFO_ORD_DT            as ORD_DT
                        , ofo_ord_num           as ord_num

                        from agg.digitem_oford_ofacc_acx_temp ofo
                       WHERE ofo.action_type = 'ORDER'
                            and ofo.acc_account_subtype_code IN ('CNS','CRH','CRM','SHM','CRT','CRE')
                            AND ofo.ofo_set_cd = 'A') ofo

-------------------------------------------------------------------------------------
      UNION ALL
-------------------------------------------------------------------------------------
      SELECT 
        ofo2.acct_id,
        ofo2.ord_id,
                ofo2.net_val_amt dnr_pd_amt2,
                ofo3.net_val_amt rcpt_pd_amt2,
                ofo2.ord_dt 
      FROM         (SELECT 
                          hash_account_id
                        , OFO_ORD_ID            as ORD_ID
                        , OFO_ACCT_ID           as ACCT_ID
                        , OFO_NET_VAL_AMT       as NET_VAL_AMT
                        , OFO_AGCY_GRS_VAL_AMT  as AGCY_GRS_VAL_AMT
                        , OFO_SET_CD            as SET_CD
                        , OFO_ORD_DT            as ORD_DT
                        , ofo_ord_num           as ord_num

                        from agg.digitem_oford_ofacc_acx_temp ofo
                    WHERE ofo.action_type = 'ORDER'
                        and ofo_set_cd IN ('B','D')
                        and ofo.acc_account_subtype_code IN ('CNS','CRH','CRM','SHM','CRT','CRE')

                     ) ofo2

                left join
                    (SELECT 
                          hash_account_id
                        , OFO_ORD_ID            as ORD_ID
                        , OFO_ACCT_ID           as ACCT_ID
                        , OFO_NET_VAL_AMT       as NET_VAL_AMT
                        , OFO_AGCY_GRS_VAL_AMT  as AGCY_GRS_VAL_AMT
                        , OFO_SET_CD            as SET_CD
                        , OFO_ORD_DT            as ORD_DT
                        , ofo_ord_num           as ord_num

                        from agg.digitem_oford_ofacc_acx_temp ofo
                       WHERE ofo.action_type = 'ORDER'
                            and ofo_set_cd IN ('C','E')
                     ) ofo3
                        on ofo2.ord_num = ofo3.ord_num
        )
GROUP BY acct_id, ord_id
;

/***************************************************************************/
-- DIGITAL ITEM - ONLINE
/***************************************************************************/


DROP TABLE IF EXISTS    agg.digitem_digital_acx_temp;
CREATE TABLE            agg.digitem_digital_acx_temp
       DISTSTYLE KEY DISTKEY(hash_account_id)
       SORTKEY(individual_id)
as
with cte_digital
(  INDIVIDUAL_ID , ITM_ID , ORD_ID , hash_account_id, source_name, account_subtype_code, ACCT_ID , SEQ_NUM , STAT_CD , SKU_NUM , TOT_GROSS_AMT , TOT_AMT , STRT_DT , END_DT , MAG_CD , CRD_STAT_CD , CANC_FLG , TERM_MTH_CNT , RNW_CD , SET_CD , EXT_KEYCD , INT_KEYCD , CANC_RSN_CD , SHP_METH_CD , CRT_DT , RPT_NAM , SUB_ID , SUB_RNW_IND , SVC_STAT_CD , CANC_DT , PMT_DT , SUB_SRC_CD , APS_SRC_CD , CRO_ACCT_EXP_DT , CANCELLED_BY)

 as (
SELECT ol.individual_id
  , ol.oli_itm_id
  , ol.oli_ord_id
  , ol.acc_hash_account_id      as hash_account_id
  , ol.acc_source_name          as source_name
  , ol.acc_account_subtype_code as account_subtype_code
  , ol.acc_acct_id              as acct_id
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
  , cast(NULL as date) cro_acct_exp_dt
  , ol.ols_cancelled_by

  FROM  agg.digitem_itm_ord_sub_acx_temp ol 
    left join (  select oli_ref_id as ref_id, sum(oli_tot_amt) as tot_amt 
                 from agg.digitem_itm_ord_sub_acx_temp oli_ss
                 where nvl(oli_ref_id,0) != 0
                 group by oli_ref_id) oli2 
        on ol.oli_itm_id = oli2.ref_id
    left join agg.digitem_sku_lkup_acx_temp sku 
        on ol.oli_sku_num = sku.sku_sku_num
  WHERE nvl(ol.oli_ref_id,0) = 0
  )

SELECT  INDIVIDUAL_ID
      , ITM_ID
      , ORD_ID
      , hash_account_id
      , source_name
      , account_subtype_code
      , ACCT_ID
      , SEQ_NUM
      , STAT_CD
      , SKU_NUM
      , TOT_GROSS_AMT
      , TOT_AMT
      , STRT_DT
      , CASE
             WHEN (crd_stat_cd = 'F'
                 OR canc_rsn_cd IN ('50','06')
                 OR cancelled_by = 'CBFEED')
                 AND canc_dt < '8-FEB-2013'
             THEN canc_dt
             ELSE end_dt
        END AS END_DT
      , MAG_CD
      , CRD_STAT_CD
      , CASE
             WHEN (crd_stat_cd = 'F'
                 OR canc_rsn_cd IN ('50','06'))
                 AND canc_dt < '8-FEB-2013'
             THEN 'Y'
             ELSE canc_flg
        END AS CANC_FLG
      , TERM_MTH_CNT
      , RNW_CD
      , SET_CD
      , EXT_KEYCD
      , INT_KEYCD
      , CANC_RSN_CD
      , SHP_METH_CD
      , CRT_DT
      , RPT_NAM
      , SUB_ID
      , SUB_RNW_IND
      , SVC_STAT_CD
      , CANC_DT
      , PMT_DT
      , SUB_SRC_CD
      , APS_SRC_CD
      , CRO_ACCT_EXP_DT
      , CANCELLED_BY
FROM cte_digital;


/***************************************************************************/
-- DIGITAL ITEM - PRINT
/***************************************************************************/

DROP TABLE IF EXISTS    agg.digitem_print_acx_temp;
CREATE TABLE            agg.digitem_print_acx_temp
       DISTSTYLE KEY DISTKEY(hash_account_id)
       SORTKEY(individual_id)
as
with cte_print
(  INDIVIDUAL_ID , ITM_ID , ORD_ID , hash_account_id, source_name, account_subtype_code, ACCT_ID , SEQ_NUM , STAT_CD , SKU_NUM , TOT_GROSS_AMT , TOT_AMT , STRT_DT , END_DT , MAG_CD , CRD_STAT_CD , CANC_FLG , TERM_MTH_CNT , RNW_CD , SET_CD , EXT_KEYCD , INT_KEYCD , CANC_RSN_CD , SHP_METH_CD , CRT_DT , RPT_NAM , SUB_ID , SUB_RNW_IND , SVC_STAT_CD , CANC_DT , PMT_DT , SUB_SRC_CD , APS_SRC_CD , CRO_ACCT_EXP_DT , CANCELLED_BY)

 as (
  SELECT 
    ofl.individual_id
  , ofl.ofo_ord_id itm_id
  , ofl.ofo_ord_id
  , ofl.acc_hash_account_id      as hash_account_id
  , ofl.acc_source_name          as source_name
  , ofl.acc_account_subtype_code as account_subtype_code
  , ofl.acc_acct_id              as acct_id
  , cast(NULL as bigint) seq_num
  , ofl.ofo_cr_stat_cd
  , cast(NULL as bigint) sku_num
  , ofl.ofo_net_val_amt
  , t1.pd_amt2 as tot_amt
  , CASE
        WHEN ofl.ofo_orig_strt_iss_num IS NULL
            OR ofl.ofo_orig_strt_iss_num = 0
        THEN add_months(ofl.ofo_ord_dt,2)
        ELSE ilkp.strt_dt
    END strt_dt
  , CASE
        WHEN ofl.ofo_orig_strt_iss_num IS NULL
            OR ofl.ofo_orig_strt_iss_num = 0
        THEN add_months(ofl.ofo_ord_dt,cast(2 + ofl.ofo_term_mth_cnt as bigint))
        ELSE ilkp.end_dt
    END end_dt
  , 'CRO' mag_cd
  , DECODE(ofl.ofo_cr_stat_cd,'G','C',NULL) crd_stat_cd
  , DECODE(ofl.ofo_canc_flg,'A','N','B','Y',NULL) canc_flg
  , cast(ofl.ofo_term_mth_cnt as bigint) term_mth_cnt
  , DECODE(ofl.ofo_rnw_cd,'C','B',ofl.ofo_rnw_cd) rnw_cd
  , ofl.ofo_set_cd
  , cast(NULL as text) ext_keycd
  , ofl.ofo_keycd int_keycd
  , CASE
        WHEN oca.canc_rsn_cd = ''
        THEN NULL
        ELSE oca.canc_rsn_cd
    END AS oca_canc_rsn_cd
  , NULL shp_meth_cd
  , ofl.ofo_ord_dt
  , cast(NULL as text) rpt_nam
  , cast(NULL as bigint) sub_id
  , DECODE(substring(ofl.ofa_keycd,1,4),'AUTO','Y','N') sub_rnw_ind
  , CASE
        WHEN ofl.ofo_stat_cd = 'B'
            AND ofl.ofa_svc_stat_cd = 'C'
            AND ofl.ofa_cro_exp_dt >= TRUNC(next_day((sysdate - 7),'THU'))
        THEN 'A'
        WHEN ofl.ofo_stat_cd = 'B'
            AND ofl.ofa_svc_stat_cd = 'C'
            AND ofl.ofa_cro_exp_dt < TRUNC(next_day((sysdate - 7),'THU'))
        THEN 'C'
        WHEN ofl.ofo_stat_cd = 'B'
        THEN DECODE(ofl.ofa_svc_stat_cd,'B','C','E','P',ofl.ofa_svc_stat_cd)
        ELSE DECODE(ofl.ofo_stat_cd,'A','F','C','E',ofl.ofo_stat_cd)
    END svc_stat_cd
  , oca.canc_dt
  , t1.pmt_dt2 as pmt_dt
  , CASE
        WHEN ofl.ofo_keycd IS NULL
        THEN 'I'
        WHEN substring(UPPER(ofl.ofo_keycd),1,2) = 'WF'
        THEN 'X'
        ELSE substring(UPPER(ofl.ofo_keycd),1,1)
    END sub_src_cd
  , cast(NULL as text) aps_src_cd
  , ofl.ofa_cro_exp_dt cro_acct_exp_dt
  , cast(NULL as text) cancelled_by

  FROM agg.digitem_oford_ofacc_acx_temp ofl
    left join agg.digitem_off_canc_acx_temp oca
        on      ofl.ofo_acct_id    = oca.acct_id
        and ofl.ofo_ord_id         = oca.ord_id

    left join ( SELECT ofo2.OFO_ACCT_ID as acct_id
                     , ofo2.ofo_ord_id  as ord_id
                     , ilk1.pub_dt      as strt_dt
                     , ilk2.pub_dt      as end_dt
                FROM agg.digitem_oford_ofacc_acx_temp ofo2
                INNER JOIN agg.digitem_issue_lkp_acx_temp ilk1
                ON  ofo2.OFO_ORIG_STRT_ISS_NUM = ilk1.iss_num
                    AND ofo2.OFO_ORIG_STRT_ISS_NUM = ilk1.iss_num
                INNER JOIN agg.digitem_issue_lkp_acx_temp ilk2
                ON (ofo2.OFO_ORIG_STRT_ISS_NUM + ofo2.ofo_term_mth_cnt) = ilk2.iss_num
                WHERE 1=1
                    AND ilk1.mag_cd = 'CRE'
                    AND ilk2.mag_cd = 'CRE') ilkp
        on      ofl.ofo_acct_id = ilkp.acct_id
            and ofl.ofo_ord_id  = ilkp.ord_id
    left join agg.digitem_oford_donrec3_temp t1
        on     ofl.ofo_acct_id = t1.acct_id
            and ofl.ofo_ord_id = t1.ord_id


  WHERE 1=1
    AND ofl.OFA_MAG_CD = ('CRE')
    )

 select 
   INDIVIDUAL_ID
  ,ITM_ID
  ,ORD_ID
  ,hash_account_id
  ,source_name
  ,account_subtype_code
  ,ACCT_ID
  ,SEQ_NUM
  ,STAT_CD
  ,SKU_NUM
  ,TOT_GROSS_AMT
  ,TOT_AMT
  ,STRT_DT
  ,END_DT
  ,MAG_CD
  ,CRD_STAT_CD
  ,CANC_FLG
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
  from cte_print;





TRUNCATE TABLE  prod.agg_digital_item_acx;

/***************************************************************************/
-- DIGITAL ITEM - Separate Insert - Online
/***************************************************************************/


INSERT INTO     prod.agg_digital_item_acx (
      individual_id
    , itm_id
    , ord_id
    , hash_account_id
    , source_name
    , account_subtype_code
    , acct_id
    , seq_num
    , stat_cd
    , sku_num
    , tot_gross_amt
    , tot_amt
    , strt_dt
    , end_dt
    , mag_cd
    , crd_stat_cd
    , canc_flg
    , term_mth_cnt
    , rnw_cd
    , set_cd
    , ext_keycd
    , int_keycd
    , canc_rsn_cd
    , shp_meth_cd
    , crt_dt
    , rpt_nam
    , sub_id
    , sub_rnw_ind
    , svc_stat_cd
    , canc_dt
    , pmt_dt
    , sub_src_cd
    , aps_src_cd
    , cro_acct_exp_dt
    , cancelled_by
)
select 
      individual_id
    , itm_id
    , ord_id
    , hash_account_id
    , source_name
    , account_subtype_code
    , acct_id
    , seq_num
    , stat_cd
    , sku_num
    , tot_gross_amt
    , tot_amt
    , strt_dt
    , end_dt
    , mag_cd
    , crd_stat_cd
    , canc_flg
    , term_mth_cnt
    , rnw_cd
    , set_cd
    , ext_keycd
    , int_keycd
    , canc_rsn_cd
    , shp_meth_cd
    , crt_dt
    , rpt_nam
    , sub_id
    , sub_rnw_ind
    , svc_stat_cd
    , canc_dt
    , pmt_dt
    , sub_src_cd
    , aps_src_cd
    , cro_acct_exp_dt
    , cancelled_by
from agg.digitem_digital_acx_temp;
 
/***************************************************************************/
-- DIGITAL ITEM - Separate insert - Offline
/***************************************************************************/


INSERT INTO     prod.agg_digital_item_acx (
      individual_id
    , itm_id
    , ord_id
    , hash_account_id
    , source_name
    , account_subtype_code
    , acct_id
    , seq_num
    , stat_cd
    , sku_num
    , tot_gross_amt
    , tot_amt
    , strt_dt
    , end_dt
    , mag_cd
    , crd_stat_cd
    , canc_flg
    , term_mth_cnt
    , rnw_cd
    , set_cd
    , ext_keycd
    , int_keycd
    , canc_rsn_cd
    , shp_meth_cd
    , crt_dt
    , rpt_nam
    , sub_id
    , sub_rnw_ind
    , svc_stat_cd
    , canc_dt
    , pmt_dt
    , sub_src_cd
    , aps_src_cd
    , cro_acct_exp_dt
    , cancelled_by
)
select 
      individual_id
    , itm_id
    , ord_id
    , hash_account_id
    , source_name
    , account_subtype_code
    , acct_id
    , seq_num
    , stat_cd
    , sku_num
    , tot_gross_amt
    , tot_amt
    , strt_dt
    , end_dt
    , mag_cd
    , crd_stat_cd
    , canc_flg
    , term_mth_cnt
    , rnw_cd
    , set_cd
    , ext_keycd
    , int_keycd
    , canc_rsn_cd
    , shp_meth_cd
    , crt_dt
    , rpt_nam
    , sub_id
    , sub_rnw_ind
    , svc_stat_cd
    , canc_dt
    , pmt_dt
    , sub_src_cd
    , aps_src_cd
    , cro_acct_exp_dt
    , cancelled_by
from agg.digitem_print_acx_temp;

/***************************************************************************/
-- DIGITAL ITEM - END
/***************************************************************************/



