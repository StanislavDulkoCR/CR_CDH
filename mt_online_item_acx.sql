/***************************************************************************
*            (C) Copyright Acxiom Corporation 2006
*                    All Rights Reserved.
***************************************************************************/


define w_name         = mt_online_item
define w_tmp_name     = mt_ofl_ord_temp1_onl
define w_aud_source   = mt_online_item.sql
define w_aud_type     = update
define w_schema       = middle_tier
define w_warehouse    = warehouse
DEFINE w_ind_xref     = &w_schema..ind_xref
DEFINE w_external_ref = &w_warehouse..external_ref

--create table
CREATE TABLE mt_online_item
  PCTUSED 99
  COMPRESS
  TABLESPACE ts_mt_large_dat
  NOLOGGING
  PARALLEL (degree 4)
AS
SELECT INDIVIDUAL_ID
      ,IND_URN
      ,ITM_ID
      ,ORD_ID
      ,ACCT_ID
      ,SEQ_NUM
      ,STAT_CD
      ,SKU_NUM
      ,TOT_GROSS_AMT
      ,TOT_AMT
      ,STRT_DT
      ,CASE WHEN (crd_stat_cd = 'F' OR canc_rsn_cd IN ('50','06') OR cancelled_by = 'CBFEED') AND
                 canc_dt < '8-FEB-2013' THEN canc_dt
            ELSE end_dt
       END AS END_DT
      ,MAG_CD 
      ,CRD_STAT_CD  
      ,CASE WHEN (crd_stat_cd = 'F' OR canc_rsn_cd IN ('50','06')) AND canc_dt < '8-FEB-2013' THEN 'Y'
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
  FROM (SELECT /*+ use_hash(olo,oli,oli2,ols) */
    ix.individual_id,
    ix.ind_urn,
    oli.itm_id,
    oli.ord_id,
    oli.acct_id,
    oli.seq_num,
    decode(oli.stat_cd,'H','C',oli.stat_cd) as stat_cd,
    oli.sku_num,
    oli.tot_amt tot_gross_amt,
    (oli.tot_amt + nvl(oli2.tot_amt,0)) tot_amt,
    case when oli.sku_num > 5000000 
         then nvl(oli.strt_dt,oli.crt_dt)
         else nvl(ols.strt_dt,nvl(oli.strt_dt,oli.crt_dt)) 
    end strt_dt,
    CASE when ols.end_dt is not null then ols.end_dt 
         when oli.end_dt is not null then oli.end_dt
         when nvl(oli.mag_cd,sku.product) in ('NCPR','UCPR')  
         then case when oli.sku_num > 5000000 
                   then nvl(oli.strt_dt,oli.crt_dt)
                   else nvl(ols.strt_dt,nvl(oli.strt_dt,oli.crt_dt)) 
              end 
         else add_months(case when oli.sku_num > 5000000 
                              then nvl(oli.strt_dt,oli.crt_dt)
                              else nvl(ols.strt_dt,nvl(oli.strt_dt,oli.crt_dt)) 
                         end, 
                         CASE when (oli.term_mth_cnt is null 
                                or oli.term_mth_cnt = 0) 
                               and nvl(oli.mag_cd,sku.product) NOT IN ('NCPR','UCPR') 
                              then CASE WHEN sku.unit_flg = 'D'
                                        THEN ceil(sku.value/30)
                                        ELSE sku.value
                                   END
                              else oli.term_mth_cnt
                         END) 
    end end_dt,
    nvl(oli.mag_cd,sku.product) mag_cd,
    nvl(ols.pymt_stat_cd,
        nvl(oli.cr_stat_cd,
            CASE WHEN olo.stat_cd IN ('C','H','M','R','T','Y')
                 THEN 'C'
                 WHEN olo.stat_cd = 'J'
                 THEN 'F'
                 WHEN olo.stat_cd IN ('P','S','N')
                 THEN 'P'
                 ELSE null
            END)) crd_stat_cd,
    nvl(oli.canc_flg, CASE WHEN oli.stat_cd IN ('R','J') THEN 'Y' ELSE 'N' END) canc_flg,
    CASE when (oli.term_mth_cnt is null 
              or oli.term_mth_cnt = 0) 
              and nvl(oli.mag_cd,sku.product) NOT IN ('NCPR','UCPR') 
         then CASE WHEN sku.unit_flg = 'D'
                   THEN ceil(sku.value/30)
                   ELSE sku.value
              END
         else oli.term_mth_cnt
    END term_mth_cnt,
    oli.rnw_flg rnw_cd,
    oli.set_cd,
    UPPER(oli.ext_keycd) as ext_keycd,
    UPPER(oli.int_keycd) as int_keycd,
    nvl(oli.canc_rsn_cd,
        CASE WHEN ols.svc_stat_cd = 'C'
             THEN '50'
             ELSE null
        END) canc_rsn_cd,
    oli.shp_meth_cd,
    CASE WHEN oli.mag_cd in ('CRO','CRMG')
         and to_char(oli.crt_dt,'YYYYMMDD') < '20080731'
         THEN nvl(oli.strt_dt,oli.crt_dt)
         ELSE oli.crt_dt
    END crt_dt,
    UPPER(oli.rpt_nam) as rpt_nam,
    ols.sub_id,
    ols.rnw_ind sub_rnw_ind,
    decode(ols.svc_stat_cd,'I','C',ols.svc_stat_cd) svc_stat_cd,
    CASE WHEN ols.cancel_dt is not null
         THEN ols.cancel_dt
         WHEN ols.svc_stat_cd = 'C' OR ols.pymt_stat_cd ='F'
         THEN ols.mdfy_dt
         WHEN oli.stat_cd = 'R' AND oli.cr_stat_cd is null
         THEN oli.mdfy_dt
         WHEN oli.cr_stat_cd is null and olo.stat_cd in ('J','R')
         THEN olo.mdfy_dt
         WHEN oli.cr_stat_cd = 'F'
         THEN oli.mdfy_dt
         ELSE null
    END canc_dt,
        CASE WHEN nvl(ols.pymt_stat_cd,
                  nvl(oli.cr_stat_cd,
                      CASE WHEN olo.stat_cd IN ('C','H','M','R','T','Y')
                           THEN 'C'
                           WHEN olo.stat_cd = 'J'
                           THEN 'F'
                           WHEN olo.stat_cd IN ('P','S','N')
                           THEN 'P'
                           ELSE null
                      END)) = 'C'
          and oli.set_cd in ('A', 'B', 'D')
         THEN nvl(oli.pmt_dt
                 , CASE WHEN oli.mag_cd in ('CRO','CRMG')
                         and to_char(oli.crt_dt,'YYYYMMDD') < '20080731'
                        THEN nvl(oli.strt_dt,oli.crt_dt)
                        ELSE oli.crt_dt
                   END)
    END pmt_dt,
    CASE WHEN nvl(oli.mag_cd,sku.product) in ('CRO','CARP')
           THEN case when oli.ext_keycd is null and oli.int_keycd is null 
                       then 'I'
                     when substr(nvl(UPPER(oli.ext_keycd),UPPER(oli.int_keycd)),1,2) = 'WF' 
                       then 'X'
                     else substr(nvl(UPPER(oli.ext_keycd),UPPER(oli.int_keycd)),1,1)
                     end
         WHEN nvl(oli.mag_cd,sku.product) = 'CRMG'
           THEN case when oli.ext_keycd is null and oli.int_keycd is null 
                       then 'I'
                     when nvl(UPPER(oli.ext_keycd),UPPER(oli.int_keycd)) = 'VOLUNTARY ORDER' 
                       then 'I'
                     when substr(nvl(UPPER(oli.ext_keycd),UPPER(oli.int_keycd)),1,1) = 'H' 
                       then substr(nvl(UPPER(oli.ext_keycd),UPPER(oli.int_keycd)),6,1)
                     else substr(nvl(UPPER(oli.ext_keycd),UPPER(oli.int_keycd)),1,1)
                     end
    END sub_src_cd,
    CASE WHEN nvl(oli.mag_cd,sku.product) IN ('NCBK','NCPR','UCBK','UCPR')
         THEN CASE WHEN substr(oli.int_keycd,1,1) BETWEEN '0' AND '9'
                   THEN '1'
                   ELSE decode(substr(nvl(UPPER(oli.ext_keycd),UPPER(oli.int_keycd)),1,6),
                               'UCBK10','56',
                               'UCBK11','57',
                               'UCBK12','58',
                               'UCBK13','59',
                               'UCBK14','60',
                               'UCBK15','61',
                               decode(substr(nvl(UPPER(oli.ext_keycd),UPPER(oli.int_keycd)),1,5),
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
                                      decode(substr(nvl(UPPER(oli.ext_keycd),UPPER(oli.int_keycd)),1,4),
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
                                             decode(substr(nvl(UPPER(oli.ext_keycd),UPPER(oli.int_keycd)),1,3),
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
                                                    decode(nvl(oli.mag_cd,sku.product),
                                                           'NCBK','32',
                                                           'NCPR','6',
                                                           'UCBK','48',
                                                           'UCPR','21')))))
              END                        
         ELSE null
    END aps_src_cd,
    null cro_acct_exp_dt,
    ols.cancelled_by
  FROM &w_ind_xref ix,
       (select distinct individual_id, internal_key from &w_external_ref) er,
       &w_warehouse..online_order olo,
       &w_warehouse..online_item oli,
       (select ref_id, sum(tot_amt) as tot_amt 
        from &w_warehouse..online_item
        where nvl(ref_id,0) != 0
        group by ref_id) oli2,
       &w_warehouse..online_subscription ols,
       &w_warehouse..sku_lkp sku
  WHERE ix.individual_id = er.individual_id
    AND ix.active_flag = 'A'
    AND er.internal_key = olo.acct_id
    AND olo.acct_id = oli.acct_id
    AND olo.ord_id = oli.ord_id
    AND oli.itm_id = oli2.ref_id(+)
    AND oli.acct_id = ols.acct_id(+)
    AND oli.ord_id = ols.ord_id(+)
    AND oli.itm_id = ols.itm_id(+)
    AND oli.sku_num = sku.sku_num(+)
    AND nvl(oli.ref_id,0) = 0
   )
----------------------------------------------------------------------
UNION ALL
----------------------------------------------------------------------
  SELECT
    ix.individual_id,
    ix.ind_urn,
    ofo.ord_id itm_id,
    ofo.ord_id,
    ofo.acct_id acct_id,
    null seq_num,
    ofo.cr_stat_cd,
    null sku_num,
    ofo.net_val_amt,
    t1.pd_amt2 as tot_amt,
    CASE WHEN ofo.orig_strt_iss_num is null
              or ofo.orig_strt_iss_num = 0
         THEN add_months(ofo.ord_dt,2)
         ELSE ilkp.strt_dt
    END strt_dt,
    CASE WHEN ofo.orig_strt_iss_num is null
              or ofo.orig_strt_iss_num = 0
         THEN add_months(ofo.ord_dt,2 + ofo.term_mth_cnt)
         ELSE ilkp.end_dt
    END end_dt,
    'CRO' mag_cd,
    decode(ofo.cr_stat_cd,'G','C',null) crd_stat_cd,
    decode(ofo.canc_flg,'A','N','B','Y',null) canc_flg,
    to_number(ofo.term_mth_cnt) term_mth_cnt,
    decode(ofo.rnw_cd,'C','B',ofo.rnw_cd) rnw_cd,
    ofo.set_cd,
    null ext_keycd,
    ofo.keycd int_keycd,
    oca.canc_rsn_cd,
    null shp_meth_cd,
    ofo.ord_dt,
    null rpt_nam,
    null sub_id,
    decode(substr(ofa.keycd,1,4),'AUTO','Y','N') sub_rnw_ind,
    CASE WHEN ofo.stat_cd = 'B'
          AND ofa.svc_stat_cd = 'C'
          AND ofa.cro_exp_dt >= trunc(next_day((sysdate - 7),'THU'))
         THEN 'A'
         WHEN ofo.stat_cd = 'B'
          AND ofa.svc_stat_cd = 'C'
          AND ofa.cro_exp_dt < trunc(next_day((sysdate - 7),'THU'))
         THEN 'C' 
         WHEN ofo.stat_cd = 'B'
         THEN decode(ofa.svc_stat_cd,'B','C','E','P',ofa.svc_stat_cd)
         ELSE decode(ofo.stat_cd,'A','F','C','E',ofo.stat_cd)
    END svc_stat_cd,
    oca.canc_dt,
    t1.pmt_dt2 as pmt_dt,
    CASE WHEN ofo.keycd is null
         THEN 'I'
         WHEN substr(UPPER(ofo.keycd),1,2) = 'WF'
         THEN 'X'
         ELSE substr(UPPER(ofo.keycd),1,1)
    END sub_src_cd,
    null aps_src_cd,
    ofa.cro_exp_dt cro_acct_exp_dt,
    null cancelled_by
  FROM &w_ind_xref ix,
       (select distinct individual_id, internal_key from &w_external_ref) er,
       &w_warehouse..offline_order ofo,
       &w_schema..mt_ofl_ord_temp1_onl t1,
       (SELECT
          ofo2.acct_id,
          ofo2.ord_id,
          ilk1.pub_dt strt_dt,
          ilk2.pub_dt end_dt
        FROM (select distinct individual_id, internal_key from &w_external_ref) er2,
             &w_warehouse..offline_order ofo2,
             &w_warehouse..issue_lkp ilk1,
             &w_warehouse..issue_lkp ilk2
        WHERE er2.internal_key = ofo2.acct_id
          AND ofo2.orig_strt_iss_num = ilk1.iss_num
          AND (ofo2.orig_strt_iss_num + ofo2.term_mth_cnt) = ilk2.iss_num
          AND ilk1.mag_cd = 'CRE'
          AND ilk2.mag_cd = 'CRE') ilkp,
       (SELECT
          ca.acct_id,
          ca.ord_id,
          sum(ca.pd_amt) tot_amt,
          substr(max(nvl(to_char(ca.act_dt, 'YYYYMMDD'),'00010101') || ca.canc_rsn_cd),9) canc_rsn_cd,
          max(ca.act_dt) canc_dt
        FROM (select distinct individual_id, internal_key from &w_external_ref) er3,
             &w_warehouse..offline_cancel_act ca
        WHERE er3.internal_key = ca.acct_id
        GROUP BY ca.acct_id, ca.ord_id) oca,
       &w_warehouse..offline_account ofa
  WHERE ix.individual_id = er.individual_id
    AND ix.active_flag = 'A'
    AND er.internal_key = ofa.acct_id
    AND ofo.acct_id = ofa.acct_id
    AND ofo.acct_id = t1.acct_id(+)
    AND ofo.ord_id = t1.ord_id(+)
    AND ofo.acct_id = ilkp.acct_id(+)
    AND ofo.ord_id = ilkp.ord_id(+)
    AND ofo.acct_id = oca.acct_id(+)
    AND ofo.ord_id = oca.ord_id(+)
    AND ofa.mag_cd = ('CRE');
exit;

