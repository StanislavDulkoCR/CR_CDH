


DROP TABLE IF EXISTS    cr_temp.digitem_issue_lkp_acx_temp;
CREATE TABLE            cr_temp.digitem_issue_lkp_acx_temp
       DISTSTYLE KEY DISTKEY(MAG_CD)
       SORTKEY(PUB_DT)
as
select
  lkpmag.magazine_code as MAG_CD
, lkpmag.iss_num       as ISS_NUM
, lkpmag.pub_date      as PUB_DT

from etl_proc.lookup_magazine lkpmag;



DROP TABLE IF EXISTS    cr_temp.digitem_oford_ofacc_acx_temp;
CREATE TABLE            cr_temp.digitem_oford_ofacc_acx_temp
       DISTSTYLE KEY DISTKEY(HASH_ACCOUNT_ID)
       SORTKEY(OFO_ORD_ID)
as
select
  ah.individual_id           as INDIVIDUAL_ID
, ah.hash_account_id         as HASH_ACCOUNT_ID
, ah.hash_action_id          as HASH_ACTION_ID
, ah.action_type             as action_type
-- action header - duplicate columns for efficiency - Very short on time here
-- offline order
, ah.action_id               as OFO_ORD_ID
, ah.hash_account_id         as OFO_ACCT_ID
, ah.addtl_action_id         as OFO_ORD_NUM
, ah.plan_status_code        as OFO_STAT_CD
, pt.cplt_date               as OFO_CPLT_DT
, ah.action_subtype_code     as OFO_ENTR_TYP_CD
, ah.credit_status_code      as OFO_CR_STAT_CD
, pt.cancel_flag             as OFO_CANC_FLG
, pt.term_month_cnt          as OFO_TERM_MTH_CNT
, ah.action_amount           as OFO_NET_VAL_AMT
, pt.agcy_grs_val_amount     as OFO_AGCY_GRS_VAL_AMT
, pt.tax_val_amount          as OFO_TAX_VAL_AMT
, pt.ofr_chngd_flag          as OFO_OFR_CHNGD_FLG
, pt.renewal_code            as OFO_RNW_CD
, pt.mlt_cpy_flag            as OFO_MLT_CPY_FLG
, pt.source_transaction_code as OFO_SRC_CD
, pt.md_code                 as OFO_MD_CD
, pt.magazine_catg_code      as OFO_MAG_CATG_CD
, pt.set_code                as OFO_SET_CD
, pt.bulk_flag               as OFO_BULK_FLG
, ah.keycode                 as OFO_KEYCD
, pt.blg_key_num             as OFO_BLG_KEY_NUM
, pt.spec_info_txt           as OFO_SPEC_INFO_TXT
, pt.agcy_code               as OFO_AGCY_CD
, pt.prev_expr_iss_num       as OFO_PREV_EXPR_ISS_NUM
, pt.original_strt_iss_num   as OFO_ORIG_STRT_ISS_NUM
, pt.original_order_flag     as OFO_ORIG_ORD_FLAG
, ah.action_date             as OFO_ORD_DT
, pt.create_date             as OFO_FIRST_DT
, pt.update_date             as OFO_MAINT_DT

-- Offline Account
, ah.hash_account_id         as OFA_HASH_ACCOUNT_ID
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




from prod.action_header ah
    inner join prod.print_transaction pt 
        on ah.hash_action_id = pt.hash_action_id
    inner join prod.print_account_detail pad 
        on pad.hash_account_id = ah.hash_account_id


    /*#TODO RMV */ where ah.individual_id in (  1200910039 , 1200911397 , 1201049183 , 1204048080 , 1211020440 , 1211492962 , 1211630972 , 1212393279 , 1212735076 , 1212851399 , 1218317424 , 1218614500 , 1218615317 , 1218973374 , 1221137079 , 1221798177 , 1222160652 , 1211348776 , 1210494821 , 1215710530 , 1202534519 )
;
DROP TABLE IF EXISTS    cr_temp.digitem_off_pay_acx_temp;
CREATE TABLE            cr_temp.digitem_off_pay_acx_temp
       DISTSTYLE KEY DISTKEY(HASH_ACCOUNT_ID)
       SORTKEY(OPA_ORD_ID)
as
select 
  ah.individual_id           as INDIVIDUAL_ID
, ah.hash_account_id         as HASH_ACCOUNT_ID
, ah.hash_action_id          as HASH_ACTION_ID
, ah.action_type             as action_type
-- Offline Payment Act
, pay.HASH_ACTION_ID         as OPA_HASH_ACTION_ID
, ah.action_id               as OPA_ORD_ID
, ah.hash_account_id         as OPA_ACCT_ID
, ah.addtl_action_id         as OPA_ORD_NUM
, ah.action_amount           as OPA_PD_AMT
, ah.order_entry_code        as OPA_PMT_METH_CD
, pay.payment_type_code      as OPA_PMT_TYP_CD
, pay.tax_paid_amount        as OPA_TAX_PD_AMT
, pay.spec_hdlg_paid_amount  as OPA_SPEC_HDLG_PD_AMT
, pay.rsm_iss_num            as OPA_RSM_ISS_NUM
, pay.blg_key_num            as OPA_BLG_KEY_NUM
, pay.term_month_cnt         as OPA_TERM_MTH_CNT
, pay.cpy_cnt                as OPA_CPY_CNT
, ah.action_date             as OPA_ACT_DT
, pay.update_date            as OPA_MAINT_DT

from prod.action_header ah
    inner join prod.payment pay
        on ah.hash_action_id = pay.hash_action_id
   /*#TODO RMV */ where ah.individual_id in (  1200910039 , 1200911397 , 1201049183 , 1204048080 , 1211020440 , 1211492962 , 1211630972 , 1212393279 , 1212735076 , 1212851399 , 1218317424 , 1218614500 , 1218615317 , 1218973374 , 1221137079 , 1221798177 , 1222160652 , 1211348776 , 1210494821 , 1215710530 , 1202534519)

;


DROP TABLE IF EXISTS    cr_temp.digitem_off_canc_acx_temp;
CREATE TABLE            cr_temp.digitem_off_canc_acx_temp
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
    LEFT JOIN prod.print_cancel pc
    ON  ah.hash_action_id = pc.hash_action_id
    where ah.action_type = 'CANCEL'
     /*#TODO RMV */ and ah.individual_id in (  1200910039 , 1200911397 , 1201049183 , 1204048080 , 1211020440 , 1211492962 , 1211630972 , 1212393279 , 1212735076 , 1212851399 , 1218317424 , 1218614500 , 1218615317 , 1218973374 , 1221137079 , 1221798177 , 1222160652 , 1211348776 , 1210494821 , 1215710530 , 1202534519)
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

DROP TABLE IF EXISTS    cr_temp.digitem_oford_donrec3_temp;
CREATE TABLE            cr_temp.digitem_oford_donrec3_temp
       DISTSTYLE KEY DISTKEY(acct_id)
       SORTKEY(ord_id)
AS
SELECT
  acct_id,
  ord_id,
  (nvl(max(dnr_pd_amt2),0) + nvl(sum(rcpt_pd_amt2),0)) pd_amt2,
  max(ord_dt) pmt_dt2
FROM (
            (SELECT ofo.acct_id
              , ofo.ord_id
              , ofo.net_val_amt AS dnr_pd_amt2
              , NULL rcpt_pd_amt2
              , ofo.ord_dt
            FROM cr_temp.digitem_external_ref_temp er
                inner join 
                    (SELECT 
                          hash_account_id
                        , OFO_ORD_ID            as ORD_ID
                        , OFO_ACCT_ID           as ACCT_ID
                        , OFO_NET_VAL_AMT       as NET_VAL_AMT
                        , OFO_AGCY_GRS_VAL_AMT  as AGCY_GRS_VAL_AMT
                        , OFO_SET_CD            as SET_CD
                        , OFO_ORD_DT            as ORD_DT
                        , ofo_ord_num           as ord_num

                        from cr_temp.digitem_oford_ofacc_acx_temp ofo
                       WHERE ofo.action_type = 'ORDER'
                     ) ofo
                    on er.hash_account_id = ofo.acct_id
      WHERE er.account_subtype_code IN ('CNS','CRH','CRM','SHM','CRT','CRE')
        AND ofo.set_cd = 'A')
-------------------------------------------------------------------------------------
      UNION ALL
-------------------------------------------------------------------------------------
      (SELECT 
        ofo2.acct_id,
        ofo2.ord_id,
                ofo2.net_val_amt dnr_pd_amt2,
                ofo3.net_val_amt rcpt_pd_amt2,
                ofo2.ord_dt 
      FROM cr_temp.digitem_external_ref_temp er2
                inner join 
                    (SELECT 
                          hash_account_id
                        , OFO_ORD_ID            as ORD_ID
                        , OFO_ACCT_ID           as ACCT_ID
                        , OFO_NET_VAL_AMT       as NET_VAL_AMT
                        , OFO_AGCY_GRS_VAL_AMT  as AGCY_GRS_VAL_AMT
                        , OFO_SET_CD            as SET_CD
                        , OFO_ORD_DT            as ORD_DT
                        , ofo_ord_num           as ord_num

                        from cr_temp.digitem_oford_ofacc_acx_temp ofo
                       WHERE ofo.action_type = 'ORDER'
                            and ofo_set_cd IN ('B','D')
                     ) ofo2
                        on er2.hash_account_id = ofo2.acct_id
                inner join
                    (SELECT 
                          hash_account_id
                        , OFO_ORD_ID            as ORD_ID
                        , OFO_ACCT_ID           as ACCT_ID
                        , OFO_NET_VAL_AMT       as NET_VAL_AMT
                        , OFO_AGCY_GRS_VAL_AMT  as AGCY_GRS_VAL_AMT
                        , OFO_SET_CD            as SET_CD
                        , OFO_ORD_DT            as ORD_DT
                        , ofo_ord_num           as ord_num

                        from cr_temp.digitem_oford_ofacc_acx_temp ofo
                       WHERE ofo.action_type = 'ORDER'
                            and ofo_set_cd IN ('C','E')
                     ) ofo3
                        on ofo2.ord_num = ofo3.ord_num


      WHERE er2.account_subtype_code IN ('CNS','CRH','CRM','SHM','CRT','CRE'))
        )
GROUP BY acct_id, ord_id
;


DROP TABLE IF EXISTS    cr_temp.digitem_print_acx_temp;
CREATE TABLE            cr_temp.digitem_print_acx_temp
       DISTSTYLE KEY DISTKEY(hash_account_id)
       SORTKEY(individual_id)
as
with cte_print
(  INDIVIDUAL_ID , ITM_ID , ORD_ID , hash_account_id, source_name, account_subtype_code, ACCT_ID , SEQ_NUM , STAT_CD , SKU_NUM , TOT_GROSS_AMT , TOT_AMT , STRT_DT , END_DT , MAG_CD , CRD_STAT_CD , CANC_FLG , TERM_MTH_CNT , RNW_CD , SET_CD , EXT_KEYCD , INT_KEYCD , CANC_RSN_CD , SHP_METH_CD , CRT_DT , RPT_NAM , SUB_ID , SUB_RNW_IND , SVC_STAT_CD , CANC_DT , PMT_DT , SUB_SRC_CD , APS_SRC_CD , CRO_ACCT_EXP_DT , CANCELLED_BY)

 as (
  SELECT er.individual_id
  , ofl.ofo_ord_id itm_id
  , ofl.ofo_ord_id
  , er.hash_account_id
  , er.source_name
  , er.account_subtype_code
  , ofl.ofo_acct_id acct_id
  , NULL seq_num
  , ofl.ofo_cr_stat_cd
  , NULL sku_num
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
  , NULL ext_keycd
  , ofl.ofo_keycd int_keycd
  , CASE
        WHEN oca.canc_rsn_cd = ''
        THEN NULL
        ELSE oca.canc_rsn_cd
    END AS oca_canc_rsn_cd
  , NULL shp_meth_cd
  , ofl.ofo_ord_dt
  , NULL rpt_nam
  , NULL sub_id
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
  , NULL aps_src_cd
  , ofl.ofa_cro_exp_dt cro_acct_exp_dt
  , NULL cancelled_by

  FROM cr_temp.digitem_external_ref_temp er 
    inner join cr_temp.digitem_oford_ofacc_acx_temp ofl
        on      er.hash_account_id = ofl.hash_account_id
        and er.individual_id       = ofl.individual_id
    left join cr_temp.digitem_off_canc_acx_temp oca
        on      ofl.ofo_acct_id    = oca.acct_id
        and ofl.ofo_ord_id         = oca.ord_id

    left join ( SELECT ofo2.OFO_ACCT_ID as acct_id
                     , ofo2.ofo_ord_id  as ord_id
                     , ilk1.pub_dt      as strt_dt
                     , ilk2.pub_dt      as end_dt
                FROM cr_temp.digitem_oford_ofacc_acx_temp ofo2
                INNER JOIN cr_temp.digitem_issue_lkp_acx_temp ilk1
                ON  ofo2.OFO_ORIG_STRT_ISS_NUM = ilk1.iss_num
                    AND ofo2.OFO_ORIG_STRT_ISS_NUM = ilk1.iss_num
                INNER JOIN cr_temp.digitem_issue_lkp_acx_temp ilk2
                ON (ofo2.OFO_ORIG_STRT_ISS_NUM + ofo2.ofo_term_mth_cnt) = ilk2.iss_num
                WHERE 1=1
                    AND ilk1.mag_cd = 'CRE'
                    AND ilk2.mag_cd = 'CRE') ilkp
        on      ofl.ofo_acct_id = ilkp.acct_id
            and ofl.ofo_ord_id  = ilkp.ord_id
    left join cr_temp.digitem_oford_donrec3_temp t1
        on     ofl.ofo_acct_id = t1.acct_id
            and ofl.ofo_ord_id = t1.ord_id


  WHERE 1=1
    AND ofl.OFA_MAG_CD = ('CRE')
    /*#TODO RMV */and er.individual_id in (  1200910039 , 1200911397 , 1201049183 , 1204048080 , 1211020440 , 1211492962 , 1211630972 , 1212393279 , 1212735076 , 1212851399 , 1218317424 , 1218614500 , 1218615317 , 1218973374 , 1221137079 , 1221798177 , 1222160652 , 1211348776 , 1210494821 , 1215710530 , 1202534519)
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
  from cte_print
;
