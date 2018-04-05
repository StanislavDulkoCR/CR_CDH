
DROP TABLE IF EXISTS    agg.digitem_off_pay_acx_temp;
/*CREATE TABLE            agg.digitem_off_pay_acx_temp
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
        on ah.hash_action_id = pay.hash_action_id;*/


DROP TABLE IF EXISTS    cr_temp.digitem_oford_donrec1_temp;
/*CREATE TABLE            cr_temp.digitem_oford_donrec1_temp
       DISTSTYLE KEY DISTKEY(acct_id)
       SORTKEY(ord_id)
AS
SELECT 
  acct_id                                                                       as acct_id
  , ord_id                                                                      as ORD_ID
  , (NVL(MAX(dnr_pd_amt),0) + NVL(SUM(rcpt_pd_amt),0))                          as pd_amt
  , MAX(act_dt)                                                                 as pmt_dt
  , decode(substring(MAX(pmt_mthd_cd),9),'',null,substring(MAX(pmt_mthd_cd),9)) as pmt_mthd_cd
  , decode(substring(MAX(pmt_typ_cd),9),'',null,substring(MAX(pmt_typ_cd),9))   as pmt_typ_cd
  , (NVL(MAX(dnr_tax_pd_amt),0) + NVL(SUM(rcpt_tax_pd_amt),0))                  as tax_pd_amt
  , (NVL(MAX(dnr_spec_amt),0)   + NVL(SUM(rcpt_spec_amt),0))                    as spec_hdlg_pd_amt
  , substring(MAX(blg_key_num),9)                                               as blg_key_num
FROM (
    (SELECT 
        ofo.ofo_acct_id                                                                         as acct_id
      , ofo.ofo_ord_id                                                                          as ord_id
      , least(opa.opa_pd_amt,ofo.ofo_agcy_grs_val_amt)                                          as dnr_pd_amt
      , NULL                                                                                    as rcpt_pd_amt
      , opa.opa_act_dt                                                                          as act_dt
      , (NVL(TO_CHAR(opa.opa_act_dt,'YYYYMMDD'),'00010101') || nvl(opa.opa_pmt_meth_cd  ,'')  ) as pmt_mthd_cd
      , (NVL(TO_CHAR(opa.opa_act_dt,'YYYYMMDD'),'00010101') || nvl(opa.opa_pmt_typ_cd   ,'')  ) as pmt_typ_cd
      , opa.opa_tax_pd_amt                                                                      as dnr_tax_pd_amt
      , NULL                                                                                    as rcpt_tax_pd_amt
      , opa.opa_spec_hdlg_pd_amt                                                                as dnr_spec_amt
      , NULL                                                                                    as rcpt_spec_amt
      , (NVL(TO_CHAR(opa.opa_act_dt,'YYYYMMDD'),'00010101') || nvl(opa.opa_blg_key_num  ,'')  ) as blg_key_num
    FROM cr_temp.digitem_external_ref_temp er
            inner join cr_temp.digitem_oford_ofacc_acx_temp ofo
                on er.hash_account_id = ofo.ofo_acct_id
            inner join cr_temp.digitem_off_pay_acx_temp opa 
                    on ofo.ofo_acct_id = opa.opa_acct_id
                        and ofo.ofo_ord_id = opa.opa_ord_id
    WHERE 1=1
        AND er.account_subtype_code IN ('CNS','CRH','CRM','SHM','CRT','CRE')
        AND ofo.ofo_set_cd = 'A'
        and opa.opa_HASH_ACTION_ID is not null)
-------------------------------------------------------------------------------------
      UNION ALL
-------------------------------------------------------------------------------------
   (SELECT 
    ofo2.acct_id
  , ofo2.ord_id
  , ofo2.pd_amt                                                             as dnr_pd_amt
  , ofo3.pd_amt                                                             as rcpt_pd_amt
  , NULLIF(
            greatest(   NVL(ofo2.act_dt,to_date('00010101','YYYYMMDD'))
                    ,   NVL(ofo3.act_dt,to_date('00010101','YYYYMMDD'))
                    ), to_date('00010101','YYYYMMDD'))                      as act_dt
  , greatest(  (NVL(TO_CHAR(  ofo2.act_dt,'YYYYMMDD'),'00010101')   || nvl(ofo2.pmt_meth_cd ,'' )  )
             , (NVL(TO_CHAR(ofo3.act_dt,'YYYYMMDD'),'00010101')     || nvl(ofo3.pmt_meth_cd,''  )  )) as pmt_mthd_cd
  , greatest(  (NVL(TO_CHAR(  ofo2.act_dt,'YYYYMMDD'),'00010101')   || nvl(ofo2.pmt_typ_cd  ,'' )  )
             , (NVL(TO_CHAR(ofo3.act_dt,'YYYYMMDD'),'00010101')     || nvl(ofo3.pmt_typ_cd ,''  )  )) as pmt_typ_cd
  , ofo2.tax_pd_amt                                                         as dnr_tax_pd_amt
  , ofo3.tax_pd_amt                                                         as rcpt_tax_pd_amt
  , ofo2.spec_hdlg_pd_amt                                                   as dnr_spec_amt
  , ofo3.spec_hdlg_pd_amt                                                   as rcpt_spec_amt
  , greatest(   (NVL(TO_CHAR(ofo2.act_dt,'YYYYMMDD'),'00010101')    || nvl(ofo2.blg_key_num,''  )  )
                , (NVL(TO_CHAR(ofo3.act_dt,'YYYYMMDD'),'00010101')  || nvl(ofo3.blg_key_num,''  )  )) asblg_key_num
      FROM cr_temp.digitem_external_ref_temp er2 
            inner join 
          (
           select   ofo.ofo_ord_num          as ord_num
                  , ofo.ofo_acct_id          as acct_id
                  , ofo.ofo_ord_id           as ord_id
                  , opa.opa_pd_amt           as pd_amt
                  , opa.opa_act_dt           as act_dt
                  , opa.opa_pmt_meth_cd      as pmt_meth_cd
                  , opa.opa_tax_pd_amt       as tax_pd_amt
                  , opa.opa_spec_hdlg_pd_amt as spec_hdlg_pd_amt
                  , opa.opa_blg_key_num      as blg_key_num
                  , opa_pmt_typ_cd       as pmt_typ_cd
                FROM cr_temp.digitem_oford_ofacc_acx_temp ofo
                    inner join cr_temp.digitem_off_pay_acx_temp opa 
                        on ofo.ofo_acct_id = opa.opa_acct_id
                            and ofo.ofo_ord_id = opa.opa_ord_id
                WHERE ofo_set_cd IN ('B','D')
           ) ofo2
                on er2.hash_account_id = ofo2.acct_id
            left join 
           (
           select   ofo.ofo_ord_num          as ord_num
                  , ofo.ofo_acct_id          as acct_id
                  , ofo.ofo_ord_id           as ord_id
                  , opa.opa_pd_amt           as pd_amt
                  , opa.opa_act_dt           as act_dt
                  , opa.opa_pmt_meth_cd      as pmt_meth_cd
                  , opa.opa_tax_pd_amt       as tax_pd_amt
                  , opa.opa_spec_hdlg_pd_amt as spec_hdlg_pd_amt
                  , opa.opa_blg_key_num      as blg_key_num
                  , opa.opa_pmt_typ_cd       as pmt_typ_cd
                FROM cr_temp.digitem_oford_ofacc_acx_temp ofo
                    left join cr_temp.digitem_off_pay_acx_temp opa 
                        on ofo.ofo_acct_id = opa.opa_acct_id
                            and ofo.ofo_ord_id = opa.opa_ord_id
           WHERE ofo_set_cd in ('C','E')
           ) ofo3
                on ofo2.ord_num = ofo3.ord_num
        WHERE 1=1
            AND er2.account_subtype_code IN ('CNS','CRH','CRM','SHM','CRT','CRE'))
            )
GROUP BY acct_id, ord_id
;*/

DROP TABLE IF EXISTS    cr_temp.digitem_oford_donrec2_temp;
/*CREATE TABLE            cr_temp.digitem_oford_donrec2_temp
       DISTSTYLE KEY DISTKEY(acct_id)
       SORTKEY(ord_id)
AS
SELECT 
  acct_id                                                as acct_id
  , ord_id                                               as ord_id
  , (NVL(MAX(dnr_pd_amt2),0) + NVL(SUM(rcpt_pd_amt2),0)) as pd_amt2
  , (NVL(MAX(dnr_pd_amt3),0) + NVL(SUM(rcpt_pd_amt3),0)) as pd_amt3
  , MAX(ord_dt)                                          as pmt_dt2
FROM (
    (SELECT
        ofo.acct_id,
        ofo.ord_id,
        ofo.agcy_grs_val_amt dnr_pd_amt2,
        null rcpt_pd_amt2,
        ofo.net_val_amt dnr_pd_amt3,
        null rcpt_pd_amt3,
        ofo.ord_dt
      FROM cr_temp.digitem_external_ref_temp er 
            inner join
               (SELECT 
                  ofo.hash_account_id
                , OFO_ORD_ID            as ORD_ID
                , OFO_ACCT_ID           as ACCT_ID
                , OFO_NET_VAL_AMT       as NET_VAL_AMT
                , OFO_AGCY_GRS_VAL_AMT  as AGCY_GRS_VAL_AMT
                , OFO_SET_CD            as SET_CD
                , OFO_ORD_DT            as ORD_DT
                , ofo.ofo_ord_num       as ord_num

                from cr_temp.digitem_oford_ofacc_acx_temp ofo
                   WHERE ((ofo_agcy_cd != '00000') or (ofo_agcy_cd = '00000'
                        and ofo.ofo_cr_stat_cd in ('C','D','E','F','G','I')))
                        and ofo.action_type = 'ORDER'
                   ) ofo
                on er.hash_account_id = ofo.hash_account_id
      WHERE er.account_subtype_code IN ('CNS','CRH','CRM','SHM','CRT')
        AND ofo.set_cd = 'A')
-------------------------------------------------------------------------------------
      UNION ALL
-------------------------------------------------------------------------------------
      (SELECT 
            ofo2.acct_id                as acct_id
          , ofo2.ord_id                 as ord_id
          , ofo2.agcy_grs_val_amt       as dnr_pd_amt2
          , ofo3.agcy_grs_val_amt       as rcpt_pd_amt2
          , ofo2.net_val_amt            as dnr_pd_amt3
          , ofo3.net_val_amt            as rcpt_pd_amt3
          , ofo2.ord_dt                 as ord_dt
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
                       WHERE OFO_SET_CD IN ('B','D')
                         and ((ofo_agcy_cd != '00000') or (ofo_agcy_cd = '00000'
                         and ofo_cr_stat_cd in ('C','D','E','F','G','I')))
                         and ofo.action_type = 'ORDER'
                     ) ofo2
                    on er.hash_account_id = ofo2.acct_id
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

                        from cr_temp.digitem_oford_ofacc_acx_temp ofo
                       WHERE set_cd IN ('C','E')
                         and ((ofo_agcy_cd != '00000') or (ofo_agcy_cd = '00000'
                         and ofo_cr_stat_cd in ('C','D','E','F','G','I')))
                         and ofo.action_type = 'ORDER'
                     ) ofo3 
                    on ofo2.ord_num = ofo3.ord_num

      WHERE  er.account_subtype_code IN ('CNS','CRH','CRM','SHM','CRT') )
        )
GROUP BY acct_id, ord_id
;*/


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


