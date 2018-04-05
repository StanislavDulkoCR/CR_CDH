/***************************************************************************
*            (C) Copyright Acxiom Corporation 2006
*                    All Rights Reserved.
*
***************************************************************************/

define w_name = mt_ofl_ord_temp1
define w_aud_source = mt_ofl_ord_temp1.sql
define w_aud_type = temp
define w_schema = middle_tier

CREATE TABLE mt_ofl_ord_temp1
  PCTUSED 99
  COMPRESS
  TABLESPACE ts_mt_large_dat
  NOLOGGING
  PARALLEL(degree 8)
AS
SELECT
  acct_id,
  ord_id,
  (nvl(max(dnr_pd_amt),0) + nvl(sum(rcpt_pd_amt),0)) pd_amt,
  max(act_dt) pmt_dt,
  substr(max(pmt_mthd_cd),9) pmt_mthd_cd,
  substr(max(pmt_typ_cd),9) pmt_typ_cd,
  (nvl(max(dnr_tax_pd_amt),0) + nvl(sum(rcpt_tax_pd_amt),0)) tax_pd_amt,
  (nvl(max(dnr_spec_amt),0) + nvl(sum(rcpt_spec_amt),0)) spec_hdlg_pd_amt,
  substr(max(blg_key_num),9) blg_key_num
FROM (SELECT
        ofo.acct_id,
        ofo.ord_id,
        least(opa.pd_amt,ofo.agcy_grs_val_amt) dnr_pd_amt,
        null rcpt_pd_amt,
        opa.act_dt,
        (nvl(to_char(opa.act_dt,'YYYYMMDD'),'00010101') || opa.pmt_meth_cd) pmt_mthd_cd,
        (nvl(to_char(opa.act_dt,'YYYYMMDD'),'00010101') || opa.pmt_typ_cd) pmt_typ_cd,
        opa.tax_pd_amt dnr_tax_pd_amt,
        null rcpt_tax_pd_amt,
        opa.spec_hdlg_pd_amt dnr_spec_amt,
        null rcpt_spec_amt,
        (nvl(to_char(opa.act_dt,'YYYYMMDD'),'00010101') || opa.blg_key_num) blg_key_num
      FROM warehouse2.external_ref er,
           warehouse2.offline_order ofo,
           warehouse2.offline_payment_act opa
      WHERE er.internal_key = ofo.acct_id
        AND ofo.acct_id = opa.acct_id
        AND ofo.ord_id = opa.ord_id
        AND substr(er.external_key,1,3) IN ('CNS','CRH','CRM','SHM','CRT','CRE')
        AND ofo.set_cd = 'A'
-------------------------------------------------------------------------------------
      UNION ALL
-------------------------------------------------------------------------------------
      SELECT /*+ use_hash(ofo2,opa,ofo3,opa2) */
        ofo2.acct_id,
        ofo2.ord_id,
        opa.pd_amt dnr_pd_amt,
        opa2.pd_amt rcpt_pd_amt,
        nullif(greatest(nvl(opa.act_dt,to_date('00010101','YYYYMMDD')),
                  nvl(opa2.act_dt,to_date('00010101','YYYYMMDD'))),
         to_date('00010101','YYYYMMDD')) act_dt,
        greatest((nvl(to_char(opa.act_dt,'YYYYMMDD'),'00010101') ||
                   opa.pmt_meth_cd),
                 (nvl(to_char(opa2.act_dt,'YYYYMMDD'),'00010101') ||
                   opa2.pmt_meth_cd)) pmt_mthd_cd,
        greatest((nvl(to_char(opa.act_dt,'YYYYMMDD'),'00010101') ||
                   opa.pmt_typ_cd),
                 (nvl(to_char(opa2.act_dt,'YYYYMMDD'),'00010101') ||
                   opa2.pmt_typ_cd)) pmt_typ_cd,
        opa.tax_pd_amt dnr_tax_pd_amt,
        opa2.tax_pd_amt rcpt_tax_pd_amt,
        opa.spec_hdlg_pd_amt dnr_spec_amt,
        opa2.spec_hdlg_pd_amt rcpt_spec_amt,
        greatest((nvl(to_char(opa.act_dt,'YYYYMMDD'),'00010101') ||
                   opa.blg_key_num),
                 (nvl(to_char(opa2.act_dt,'YYYYMMDD'),'00010101') ||
                   opa2.blg_key_num)) blg_key_num
      FROM warehouse2.external_ref er2,
          (SELECT * from warehouse2.offline_order
           WHERE set_cd in ('B','D')) ofo2,
           warehouse2.offline_payment_act opa,
           (SELECT * from warehouse2.offline_order
           WHERE set_cd in ('C','E')) ofo3,
           warehouse2.offline_payment_act opa2
      WHERE er2.internal_key = ofo2.acct_id
        AND ofo2.acct_id = opa.acct_id
        AND ofo2.ord_id = opa.ord_id
        AND ofo2.ord_num = ofo3.ord_num (+)
        AND ofo3.acct_id = opa2.acct_id (+)
        AND ofo3.ord_id = opa2.ord_id (+)
        AND substr(er2.external_key,1,3) IN ('CNS','CRH','CRM','SHM','CRT','CRE'))
GROUP BY acct_id, ord_id
;

define w_name2 = mt_ofl_ord_temp2
define w_aud_source2 = mt_ofl_ord_temp2.sql
define w_aud_type2 = temp
define w_schema2 = middle_tier

CREATE TABLE mt_ofl_ord_temp2
  PCTUSED 99
  COMPRESS
  TABLESPACE ts_mt_large_dat
  NOLOGGING
  PARALLEL(degree 8)
AS
SELECT
  acct_id,
  ord_id,
  (nvl(max(dnr_pd_amt2),0) + nvl(sum(rcpt_pd_amt2),0)) pd_amt2,
  (nvl(max(dnr_pd_amt3),0) + nvl(sum(rcpt_pd_amt3),0)) pd_amt3,
  max(ord_dt) pmt_dt2
FROM (SELECT
        ofo.acct_id,
        ofo.ord_id,
                ofo.agcy_grs_val_amt dnr_pd_amt2,
        null rcpt_pd_amt2,
        ofo.net_val_amt dnr_pd_amt3,
        null rcpt_pd_amt3,
        ofo.ord_dt
      FROM warehouse2.external_ref er,
           (SELECT * from warehouse2.offline_order
               WHERE ((agcy_cd != '00000') or (agcy_cd = '00000'
                 and cr_stat_cd in ('C','D','E','F','G','I')))) ofo
      WHERE er.internal_key = ofo.acct_id
        AND substr(er.external_key,1,3) IN ('CNS','CRH','CRM','SHM','CRT')
        AND ofo.set_cd = 'A'
-------------------------------------------------------------------------------------
      UNION ALL
-------------------------------------------------------------------------------------
      SELECT /*+ use_hash(er2,ofo2,ofo3) */
        ofo2.acct_id,
        ofo2.ord_id,
        ofo2.agcy_grs_val_amt dnr_pd_amt2,
        ofo3.agcy_grs_val_amt rcpt_pd_amt2,
        ofo2.net_val_amt dnr_pd_amt3,
        ofo3.net_val_amt rcpt_pd_amt3,
        ofo2.ord_dt
      FROM warehouse2.external_ref er2,
           (SELECT * FROM warehouse2.offline_order 
               WHERE set_cd IN ('B','D')
                 and ((agcy_cd != '00000') or (agcy_cd = '00000'
                 and cr_stat_cd in ('C','D','E','F','G','I')))) ofo2,
           (SELECT * FROM warehouse2.offline_order 
               WHERE set_cd IN ('C','E')
                 and ((agcy_cd != '00000') or (agcy_cd = '00000'
                 and cr_stat_cd in ('C','D','E','F','G','I')))) ofo3
      WHERE er2.internal_key = ofo2.acct_id
        AND ofo2.ord_num = ofo3.ord_num (+)
        AND SUBSTR(er2.external_key,1,3) IN ('CNS','CRH','CRM','SHM','CRT'))
GROUP BY acct_id, ord_id
;


define w_name3 = mt_ofl_ord_temp1_onl
define w_aud_source3 = mt_ofl_ord_temp1_onl.sql
define w_aud_type3 = temp
define w_schema3 = middle_tier

CREATE TABLE mt_ofl_ord_temp1_onl
  PCTUSED 99
  COMPRESS
  TABLESPACE ts_mt_large_dat
  NOLOGGING
  PARALLEL(degree 8)
AS
SELECT
  acct_id,
  ord_id,
  (nvl(max(dnr_pd_amt2),0) + nvl(sum(rcpt_pd_amt2),0)) pd_amt2,
  max(ord_dt) pmt_dt2
FROM (SELECT
        ofo.acct_id,
        ofo.ord_id,
                ofo.net_val_amt as dnr_pd_amt2,
        null rcpt_pd_amt2,
                ofo.ord_dt
      FROM warehouse2.external_ref er,
           warehouse2.offline_order ofo
      WHERE er.internal_key = ofo.acct_id
        AND substr(er.external_key,1,3) IN ('CNS','CRH','CRM','SHM','CRT','CRE')
        AND ofo.set_cd = 'A'
-------------------------------------------------------------------------------------
      UNION ALL
-------------------------------------------------------------------------------------
      SELECT /*+ use_hash(er2,ofo2,ofo3) */
        ofo2.acct_id,
        ofo2.ord_id,
                ofo2.net_val_amt dnr_pd_amt2,
                ofo3.net_val_amt rcpt_pd_amt2,
                ofo2.ord_dt --< added by jephil 2006.08.30 >
      FROM warehouse2.external_ref er2,
           (SELECT * FROM warehouse2.offline_order 
            WHERE set_cd IN ('B','D')) ofo2,
           (SELECT * FROM warehouse2.offline_order 
            WHERE set_cd IN ('C','E')) ofo3
      WHERE er2.internal_key = ofo2.acct_id
        AND ofo2.ord_num = ofo3.ord_num(+)
        AND SUBSTR(er2.external_key,1,3) IN ('CNS','CRH','CRM','SHM','CRT','CRE'))
GROUP BY acct_id, ord_id
;


exit;
