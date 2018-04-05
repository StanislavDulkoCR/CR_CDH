/***************************************************************************
*            (C) Copyright Consumer Reports 2017
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  agg_digital_summary.sql
* Date       :  2017/10/30
* Dev & QA   :  Stanislav Dulko
***************************************************************************/



set wlm_query_slot_count = 10;

/* TEMP AGG digital_external_ref_temp */
DROP TABLE IF EXISTS    agg.digital_external_ref_temp;
CREATE TABLE            agg.digital_external_ref_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(hash_account_id)
as
select distinct INDIVIDUAL_ID, HASH_ACCOUNT_ID
from PROD.account
where INDIVIDUAL_ID is not null;

analyse agg.digital_external_ref_temp;

/* TEMP AGG digital_driver_temp */
DROP TABLE IF EXISTS    agg.digital_driver_temp;
CREATE TABLE            agg.digital_driver_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       INTERLEAVED SORTKEY(individual_id)
AS
SELECT DISTINCT individual_id FROM prod.agg_digital_order
    UNION
    SELECT individual_id
    FROM prod.agg_individual_xographic
    WHERE dc_last_aps_ord_dt IS NOT NULL
    UNION
    SELECT DISTINCT individual_id
    FROM prod.agg_account_number
    WHERE acct_prefix = 'PWI'
        AND group_id IN ('310','26401','26402','26403')
;

analyse agg.digital_driver_temp;

/* TEMP AGG digital_ind_xref_temp */
DROP TABLE IF EXISTS    agg.digital_ind_xref_temp;
CREATE TABLE            agg.digital_ind_xref_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       INTERLEAVED SORTKEY(individual_id)
as
select distinct indv.INDIVIDUAL_ID, indv.household_id
from PROD.individual indv inner join agg.digital_driver_temp driver
on indv.individual_id = driver.individual_id
where indv.INDIVIDUAL_ID is not null;

analyse agg.digital_ind_xref_temp;

/* TEMP AGG digital_sku_temp_temp */
DROP TABLE IF EXISTS    agg.digital_sku_temp_temp;
CREATE TABLE            agg.digital_sku_temp_temp
       DISTSTYLE ALL
       SORTKEY(SKU_NUM)
as
select
      sku_num             as SKU_NUM
     ,sub_type_code       as SUB_TYP_CD
     ,sku_desc            as SKU_DES
     ,product             as PRODUCT
     ,value               as VALUE
     ,unit_flag           as UNIT_FLG
     ,selection           as SELECTION
     ,comp_flag           as COMP_FLG
     ,value_range         as AMT
     ,update_date         as MAINT_DT
     ,valid_from          as VALID_FROM
     ,valid_to            as VALID_TO
    from prod.sku_lkup;

analyse agg.digital_sku_temp_temp;

/* TEMP AGG mt_onl_sum_temp1 */
DROP TABLE IF EXISTS    agg.mt_onl_sum_temp1;
CREATE TABLE            agg.mt_onl_sum_temp1
       DISTSTYLE KEY DISTKEY(hash_account_id)
       INTERLEAVED SORTKEY(hash_account_id)
AS
SELECT
  individual_id,
  hash_account_id,
  max(last_crmg_brk_dt) last_crmg_brk_dt,
  max(prior_crmg_brk_dt) prior_crmg_brk_dt,
  max(last_cro_brk_dt) last_cro_brk_dt,
  max(prior_cro_brk_dt) prior_cro_brk_dt,
  max(last_cro_acct_brk_dt) last_cro_acct_brk_dt,
  max(crmg_brks_cnt) crmg_brks_cnt,
  max(cro_brks_cnt) cro_brks_cnt,
  case when max(max(cro_actv)) over (partition by individual_id) = 'Y'
       then first_value(hash_account_id) over (partition by individual_id
                                           order by max(cro_actv) desc nulls last
                                                  , to_date(SUBSTRING(min(case when mag_cd = 'CRO'
                                                                             and nvl(set_cd, 'Z') not in ('B','D')
                                                                             and nvl(to_char(strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || nvl(to_char(end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || lpad(nvl(itm_id,'0'),21) > nvl(last_cro_acct_brk_dt,'0000000000000000000000000000')
                                                                            then nvl(to_char(strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || nvl(to_char(end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || lpad(nvl(itm_id,'0'),21) || to_char(crt_dt,'YYYYMMDDHH24MISS')
                                                                            else null
                                                                       end),50),'YYYYMMDDHH24MISS') rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
       else first_value(hash_account_id) over (partition by individual_id
                                           order by max(cro_actv) desc nulls last
                                                  , to_date(SUBSTRING(min(case when mag_cd = 'CRO'
                                                                             and nvl(set_cd, 'Z') not in ('B','D')
                                                                             and nvl(to_char(strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || nvl(to_char(end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || lpad(nvl(itm_id,'0'),21) > nvl(last_cro_acct_brk_dt,'0000000000000000000000000000')
                                                                            then nvl(to_char(strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || nvl(to_char(end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || lpad(nvl(itm_id,'0'),21) || to_char(crt_dt,'YYYYMMDDHH24MISS')
                                                                            else null
                                                                       end),50),'YYYYMMDDHH24MISS') desc nulls last rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  end cro_curr_mbr_dt_acct_id
FROM
  (SELECT
     t2.individual_id,
     t2.hash_account_id,
     t2.itm_id,
     t2.mag_cd,
     t2.set_cd,
     t2.strt_dt,
     t2.end_dt,
     t2.crt_dt,
     first_value(crmg_brk_dt) OVER (PARTITION BY t2.individual_id
                                        ORDER BY crmg_brk_dt desc nulls last rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_crmg_brk_dt,
     nth_value(crmg_brk_dt,2) OVER (PARTITION BY t2.individual_id
                                        ORDER BY crmg_brk_dt desc nulls last
                                    rows between unbounded preceding and unbounded following) prior_crmg_brk_dt,
     first_value(cro_brk_dt ) OVER (PARTITION BY t2.individual_id
                                        ORDER BY cro_brk_dt desc nulls last rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_cro_brk_dt,
     nth_value(cro_brk_dt ,2) OVER (PARTITION BY t2.individual_id
                                        ORDER BY cro_brk_dt desc nulls last
                                    rows between unbounded preceding and unbounded following) prior_cro_brk_dt,
     first_value(cro_acct_brk_dt ) OVER (PARTITION BY t2.individual_id, t2.hash_account_id
                                        ORDER BY cro_acct_brk_dt desc nulls last rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_cro_acct_brk_dt,
     count(crmg_brk_dt) over (partition by individual_id) crmg_brks_cnt,
     count(cro_brk_dt) over (partition by individual_id) cro_brks_cnt,
     cro_actv
   FROM
     (SELECT
        t1.individual_id,
        t1.hash_account_id,
        t1.itm_id,
                  t1.mag_cd,
                  t1.set_cd,
                  t1.strt_dt,
                  t1.end_dt,
                  t1.crt_dt,
        CASE WHEN t1.mag_cd = 'CRMG'
              AND nvl(t1.set_cd, 'Z') NOT IN ('B', 'D')
              AND SUBSTRING(nvl(t1.ext_keycd, nvl(t1.int_keycd, 'ZZ')),1,2) != 'WF'
             THEN CASE WHEN months_between(t1.strt_dt,
                                           nvl(max(t1.end_dt) over (PARTITION BY
                                                              t1.individual_id,
                                                              t1.mag_cd,
                                                              case when nvl(t1.set_cd, 'Z') NOT IN ('B', 'D') then 1 else 0 end
                                                            ORDER BY t1.strt_dt, nvl(t1.ext_keycd, t1.int_keycd)
                                                            rows BETWEEN unbounded preceding and 1 PRECEDING)
                                               ,t1.strt_dt)) > 2
                       THEN max(t1.end_dt) over (PARTITION BY
                                                              t1.individual_id,
                                                              t1.mag_cd,
                                                              case when nvl(t1.set_cd, 'Z') NOT IN ('B', 'D') then 1 else 0 end
                                                            ORDER BY t1.strt_dt, nvl(t1.ext_keycd, t1.int_keycd)
                                                            rows BETWEEN unbounded preceding and 1 PRECEDING)
                       ELSE null
                  END
             ELSE null
        END crmg_brk_dt,
        CASE WHEN t1.mag_cd = 'CRO'
              AND t1.set_cd NOT IN ('B', 'D')
             THEN CASE WHEN decode(t1.term_mth_cnt,1,1,2)
                              != lead(decode(t1.term_mth_cnt,1,1,2)) over (partition by t1.individual_id
                                                                                      , t1.mag_cd
                                                                                      , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
                                                                               order by t1.strt_dt, t1.end_dt, t1.itm_id)
                       THEN t1.end_dt
                       WHEN t1.term_mth_cnt > 1
                        AND extract(days FROM lead(t1.strt_dt) over (partition by t1.individual_id
                                                                   , t1.mag_cd
                                                                   , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
                                                            order by t1.strt_dt, t1.end_dt, t1.itm_id)
                                   - t1.end_dt) > 49
                       THEN t1.end_dt
                       WHEN t1.term_mth_cnt = 1
                        AND extract(days FROM lead(t1.strt_dt) over (partition by t1.individual_id
                                                                   , t1.mag_cd
                                                                   , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
                                                            order by t1.strt_dt, t1.end_dt, t1.itm_id)
                                   - t1.end_dt) > 25
                       THEN t1.end_dt
                  END
        END cro_brk_dt,
        CASE WHEN t1.mag_cd = 'CRO'
              AND t1.set_cd NOT IN ('B', 'D')
             THEN CASE WHEN decode(t1.term_mth_cnt,1,1,2)
                              != lead(decode(t1.term_mth_cnt,1,1,2)) over (partition by t1.individual_id
                                                                                      , t1.hash_account_id
                                                                                      , t1.mag_cd
                                                                                      , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
                                                                               order by t1.strt_dt, t1.end_dt, t1.itm_id)
                       THEN nvl(to_char(t1.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || nvl(to_char(t1.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || lpad(nvl(t1.itm_id,'0'),21)
                       WHEN t1.term_mth_cnt > 1
                        AND extract(days FROM lead(t1.strt_dt) over (partition by t1.individual_id
                                                              , t1.hash_account_id
                                                                   , t1.mag_cd
                                                                   , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
                                                            order by t1.strt_dt, t1.end_dt, t1.itm_id)
                                   - t1.end_dt) > 49
                       THEN nvl(to_char(t1.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || nvl(to_char(t1.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || lpad(nvl(t1.itm_id,'0'),21)
                       WHEN t1.term_mth_cnt = 1
                        AND extract(days FROM lead(t1.strt_dt) over (partition by t1.individual_id
                                                              , t1.hash_account_id
                                                                   , t1.mag_cd
                                                                   , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
                                                            order by t1.strt_dt, t1.end_dt, t1.itm_id)
                                   - t1.end_dt) > 25
                       THEN nvl(to_char(t1.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || nvl(to_char(t1.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || lpad(nvl(t1.itm_id,'0'),21)
                  END
        END cro_acct_brk_dt,
        case when ((t1.svc_stat_cd = 'A'
                       AND olo.stat_cd = 'C')
                     OR (t1.svc_stat_cd = 'C'
                         AND olo.stat_cd = 'R'
                         AND t1.sub_rnw_ind = 'N'
                         AND t1.end_dt > sysdate))
                   AND t1.mag_cd = 'CRO'
                   AND nvl(olo.entr_typ_cd,'Z') != 'F'
                   AND t1.tot_amt > 0.01
                   AND t1.term_mth_cnt > 0
                  then 'Y'
             else 'N'
        end cro_actv
    FROM prod.agg_digital_item t1
      left join prod.agg_digital_order olo on t1.hash_account_id = olo.hash_account_id and t1.ord_id = olo.ord_id) t2)
GROUP BY individual_id, hash_account_id;

analyse agg.mt_onl_sum_temp1;


/* TEMP AGG digital_t2_temp */
DROP TABLE IF EXISTS    agg.digital_t2_temp;
CREATE TABLE            agg.digital_t2_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       INTERLEAVED SORTKEY(individual_id)
AS
SELECT individual_id
      , substring(MAX(
        CASE
            WHEN mag_cd = 'CRMG'
            THEN NVL(TO_CHAR(end_dt,'YYYYMMDD'),'0000000') || hash_account_id
            ELSE NULL
        END),9) crmg_comb_acct_id
      , substring(MAX(
        CASE
            WHEN mag_cd = 'CRO'
            THEN NVL(TO_CHAR(NVL(cro_acct_exp_dt,end_dt),'YYYYMMDD'),'0000000') || NVL(lpad(sku_num,16,'0'),'0000000000000000')|| hash_account_id
            ELSE NULL
        END),25) cro_comb_acct_id
      , substring(MAX(
        CASE
            WHEN mag_cd = 'CARP'
            THEN NVL(TO_CHAR(end_dt,'YYYYMMDD'),'0000000') || hash_account_id
            ELSE NULL
        END),9) carp_comb_acct_id
    FROM prod.agg_digital_item
    GROUP BY individual_id
;

analyse agg.digital_t2_temp;

/* TEMP AGG digital_an_temp */
DROP TABLE IF EXISTS    agg.digital_an_temp;
CREATE TABLE            agg.digital_an_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       INTERLEAVED SORTKEY(individual_id)
AS
SELECT individual_id
      , MAX(
        CASE
            WHEN acct_prefix = 'PWI'
            THEN acct_lst_logn_dt
        END) lst_logn_dt
      , NVL(SUM(
        CASE
            WHEN acct_prefix = 'PWI'
            THEN acct_logn_cnt
        END),0) logn_cnt
      , MIN(
        CASE
            WHEN acct_prefix = 'PWI'
                AND group_id = '311'
            THEN 'N'
            WHEN acct_prefix = 'PWI'
                AND group_id IN ('310', '26401', '26403')
            THEN 'Y'
            ELSE NULL
        END) cro_lt_sub_flg
      , MIN(
        CASE
            WHEN acct_prefix = 'PWI'
                AND group_id = '311'
            THEN 'N'
            WHEN acct_prefix = 'PWI'
                AND group_id IN ('310', '26402', '26403')
            THEN 'Y'
            ELSE NULL
        END) crmg_lt_sub_flg
    FROM prod.agg_account_number
    GROUP BY individual_id
;

analyse agg.digital_an_temp;

/* TEMP AGG digital_driver_ind_xref_temp */
DROP TABLE IF EXISTS    agg.digital_driver_ind_xref_temp;
CREATE TABLE            agg.digital_driver_ind_xref_temp
       DISTSTYLE KEY DISTKEY(hash_account_id)
       SORTKEY(hash_account_id)
AS
select   ix.individual_id
       , ix.household_id
       , er.hash_account_id
FROM
     agg.digital_driver_temp driver
     inner join agg.digital_ind_xref_temp ix     on driver.individual_id = ix.individual_id
     left join agg.digital_external_ref_temp er  on ix.individual_id     = er.individual_id;

analyse agg.digital_driver_ind_xref_temp;

/* TEMP AGG digital_full_transaction_temp */
DROP TABLE IF EXISTS    agg.digital_full_transaction_temp;
CREATE TABLE            agg.digital_full_transaction_temp
       DISTSTYLE KEY DISTKEY(olo_hash_account_id)
       SORTKEY(olo_hash_account_id)
AS
SELECT    olo.individual_id           as olo_individual_id
    , olo.hash_account_id         as olo_hash_account_id
    , olo.source_name             as olo_source_name
    , olo.account_subtype_code    as olo_account_subtype_code
    , olo.acct_id                 as olo_acct_id
    , olo.ord_id                  as olo_ord_id
    , olo.stat_cd                 as olo_stat_cd
    , olo.ord_dt                  as olo_ord_dt
    , olo.entr_typ_cd             as olo_entr_typ_cd
    , olo.pd_dt                   as olo_pd_dt
    , olo.pd_amt                  as olo_pd_amt
    , olo.crt_dt                  as olo_crt_dt
    , oli.itm_id                  as oli_itm_id
    , oli.ord_id                  as oli_ord_id
    , oli.source_name             as oli_source_name
    , oli.account_subtype_code    as oli_account_subtype_code
    , oli.acct_id                 as oli_acct_id
    , oli.seq_num                 as oli_seq_num
    , oli.stat_cd                 as oli_stat_cd
    , oli.sku_num                 as oli_sku_num
    , oli.tot_gross_amt           as oli_tot_gross_amt
    , oli.tot_amt                 as oli_tot_amt
    , oli.strt_dt                 as oli_strt_dt
    , oli.end_dt                  as oli_end_dt
    , oli.mag_cd                  as oli_mag_cd
    , oli.crd_stat_cd             as oli_crd_stat_cd
    , oli.canc_flg                as oli_canc_flg
    , oli.term_mth_cnt            as oli_term_mth_cnt
    , oli.rnw_cd                  as oli_rnw_cd
    , oli.set_cd                  as oli_set_cd
    , oli.ext_keycd               as oli_ext_keycd
    , oli.int_keycd               as oli_int_keycd
    , oli.canc_rsn_cd             as oli_canc_rsn_cd
    , oli.shp_meth_cd             as oli_shp_meth_cd
    , oli.crt_dt                  as oli_crt_dt
    , oli.rpt_nam                 as oli_rpt_nam
    , oli.sub_id                  as oli_sub_id
    , oli.sub_rnw_ind             as oli_sub_rnw_ind
    , oli.svc_stat_cd             as oli_svc_stat_cd
    , oli.canc_dt                 as oli_canc_dt
    , oli.pmt_dt                  as oli_pmt_dt
    , oli.sub_src_cd              as oli_sub_src_cd
    , oli.aps_src_cd              as oli_aps_src_cd
    , oli.cro_acct_exp_dt         as oli_cro_acct_exp_dt
    , oli.cancelled_by            as oli_cancelled_by
    , t1.last_crmg_brk_dt         as t1_last_crmg_brk_dt
    , t1.prior_crmg_brk_dt        as t1_prior_crmg_brk_dt
    , t1.last_cro_brk_dt          as t1_last_cro_brk_dt
    , t1.prior_cro_brk_dt         as t1_prior_cro_brk_dt
    , t1.last_cro_acct_brk_dt     as t1_last_cro_acct_brk_dt
    , t1.crmg_brks_cnt            as t1_crmg_brks_cnt
    , t1.cro_brks_cnt             as t1_cro_brks_cnt
    , t1.cro_curr_mbr_dt_acct_id  as t1_cro_curr_mbr_dt_acct_id
FROM  prod.agg_digital_order olo                        -- SD 20171006
    LEFT JOIN prod.agg_digital_item oli
        ON  olo.hash_account_id = oli.hash_account_id
            AND olo.ord_id = oli.ord_id                         -- SD 20171006
    LEFT JOIN agg.mt_onl_sum_temp1 t1
        ON  oli.individual_id = t1.individual_id                -- SD 20171006
            AND oli.hash_account_id = t1.hash_account_id;

analyse agg.digital_full_transaction_temp;


/* TEMP AGG digital_full_transaction_temp_driver */
DROP TABLE IF EXISTS    agg.digital_full_transaction_temp_driver;
CREATE TABLE            agg.digital_full_transaction_temp_driver
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(individual_id)
AS
SELECT    er.individual_id
        , er.hash_account_id
        , er.household_id
        , olo_individual_id
        , olo_hash_account_id
        , olo_source_name
        , olo_account_subtype_code
        , olo_acct_id
        , olo_ord_id
        , olo_stat_cd
        , olo_ord_dt
        , olo_entr_typ_cd
        , olo_pd_dt
        , olo_pd_amt
        , olo_crt_dt
        , oli_itm_id
        , oli_ord_id
        , oli_source_name
        , oli_account_subtype_code
        , oli_acct_id
        , oli_seq_num
        , oli_stat_cd
        , oli_sku_num
        , oli_tot_gross_amt
        , oli_tot_amt
        , oli_strt_dt
        , oli_end_dt
        , oli_mag_cd
        , oli_crd_stat_cd
        , oli_canc_flg
        , oli_term_mth_cnt
        , oli_rnw_cd
        , oli_set_cd
        , oli_ext_keycd
        , oli_int_keycd
        , oli_canc_rsn_cd
        , oli_shp_meth_cd
        , oli_crt_dt
        , oli_rpt_nam
        , oli_sub_id
        , oli_sub_rnw_ind
        , oli_svc_stat_cd
        , oli_canc_dt
        , oli_pmt_dt
        , oli_sub_src_cd
        , oli_aps_src_cd
        , oli_cro_acct_exp_dt
        , oli_cancelled_by
        , t1_last_crmg_brk_dt
        , t1_prior_crmg_brk_dt
        , t1_last_cro_brk_dt
        , t1_prior_cro_brk_dt
        , t1_last_cro_acct_brk_dt
        , t1_crmg_brks_cnt
        , t1_cro_brks_cnt
        , t1_cro_curr_mbr_dt_acct_id
FROM agg.digital_driver_ind_xref_temp er
    LEFT JOIN agg.digital_full_transaction_temp olo
        ON  er.hash_account_id = olo.olo_hash_account_id;
		
analyse agg.digital_full_transaction_temp_driver;


/* PROD agg_digital_summary */
/*
SD 20171009:: Full list of columns dropped in CDH:
CRMG_ACTV_FLG|APS_ACTV_PD_FLG|APS_RPTS_ACTV_PD_FLG|CRMG_ACTV_PD_FLG|UCBK_ACTV_PD_FLG|CRMG_ANNUAL_FLG|CRMG_CR_STAT_CD|CRMG_EXP_DT|APS_LST_ORD_DT|NC_RPTS_LST_ORD_DT|UC_RPTS_LST_ORD_DT|UCBK_LST_ORD_DT|CRMG_LST_PMT_DT|NC_RPTS_LST_DEL_CHNL_CD|UC_RPTS_LST_DEL_CHNL_CD|NC_RPTS_FLG|CRMG_CURR_MBR_KEYCODE|CRMG_CURR_ORD_KEYCODE|CRMG_FST_MBR_KEYCODE|APS_FST_ORD_KEYCODE|NC_RPTS_FST_ORD_KEYCODE|UC_RPTS_FST_ORD_KEYCODE|UCBK_FST_ORD_KEYCODE|APS_LST_ORD_KEYCODE|CRMG_LST_ORD_KEYCODE|NC_RPTS_LST_ORD_KEYCODE|UC_RPTS_LST_ORD_KEYCODE|UCBK_LST_ORD_KEYCODE|CRMG_LST_SUB_AMT|CRMG_MONTHLY_FLG|CRMG_BRKS_CNT|CRMG_RNW_CNT|CRMG_SVC_STAT_CD|CRMG_CURR_MBR_SRC_CD|CRMG_CURR_ORD_SRC_CD|APS_FST_ORD_SRC_CD|CRMG_FST_ORD_SRC_CD|NC_RPTS_FST_ORD_SRC_CD|UC_RPTS_FST_ORD_SRC_CD|UCBK_FST_ORD_SRC_CD|APS_LST_ORD_SRC_CD|CRMG_LST_ORD_SRC_CD|NC_RPTS_LST_ORD_SRC_CD|UC_RPTS_LST_ORD_SRC_CD|UCBK_LST_ORD_SRC_CD|CRMG_PRIOR_MBR_SRC_CD|CRMG_CURR_ORD_TERM|APS_LST_PROD_TYPE|CRMG_AUTO_RNW_FLG|CRMG_CURR_MBR_DT|APS_LTD_PD_AMT|CRMG_LTD_PD_AMT|CRMG_CURR_ORD_DT|APS_FST_ORD_DT|CRMG_FST_ORD_DT|NC_RPTS_FST_ORD_DT|UC_RPTS_FST_ORD_DT|UCBK_FST_ORD_DT|CRMG_LST_ORD_DT|APS_ORD_CNT|CRMG_ORD_CNT|CRMG_DM_ORD_CNT|CRMG_EM_ORD_CNT|APS_PD_PROD_CNT|CRMG_LST_SUB_ORD_ROLE_CD|CRMG_LT_SUB_FLG
*/

DROP TABLE IF EXISTS    prod.agg_digital_summary;
CREATE TABLE            prod.agg_digital_summary
       DISTSTYLE KEY DISTKEY(individual_id)
       INTERLEAVED SORTKEY(individual_id)
AS
SELECT
    dft.individual_id
    , dft.household_id AS                  hh_id
    /*MAX(CASE WHEN dft.oli_mag_cd = 'CRMG'
            AND ((dft.oli_svc_stat_cd = 'A'
                  AND dft.olo_stat_cd = 'C')
                 OR (dft.oli_svc_stat_cd = 'C'
                     AND dft.olo_stat_cd = 'R'
                     AND dft.oli_sub_rnw_ind = 'N'
                     AND dft.oli_end_dt > sysdate))
            AND nvl(dft.olo_entr_typ_cd,'Z') != 'F'
            AND dft.oli_sku_num > 5000000
            AND dft.oli_tot_amt > 0.01
            AND dft.oli_term_mth_cnt > 0
            AND dft.olo_hash_account_id = t2.crmg_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) crmg_actv_flg,*/
    , MAX(CASE WHEN dft.oli_mag_cd = 'CRO'
                    AND ((dft.oli_set_cd IN ('C', 'E')
                          AND dft.oli_svc_stat_cd = 'A'
                          AND dft.olo_stat_cd = 'C'
                          AND dft.oli_stat_cd = 'C')
                         OR (((dft.oli_svc_stat_cd = 'A'
                               AND dft.olo_stat_cd = 'C')
                              OR (dft.oli_svc_stat_cd = 'C'
                                  AND dft.olo_stat_cd = 'R'
                                  AND dft.oli_sub_rnw_ind = 'N'
                                  AND dft.oli_end_dt > sysdate))
                             AND (nvl(dft.olo_entr_typ_cd, 'Z') != 'F'
                                  AND dft.oli_tot_amt > 0.01
                                  AND dft.oli_term_mth_cnt > 0)))
                    AND dft.olo_hash_account_id = t2.cro_comb_acct_id
    THEN 'Y'
          ELSE 'N'
          END)                             cro_actv_flg
    /*max(CASE WHEN dft.oli_stat_cd = 'C'
            AND dft.oli_mag_cd IN ('NCPR','UCPR')
            AND dft.oli_crt_dt >= add_months(sysdate,-12)
           THEN 'Y'
           WHEN dft.oli_stat_cd = 'C'
            AND dft.oli_mag_cd IN ('NCBK','UCBK')
            AND dft.oli_strt_dt BETWEEN ADD_MONTHS(SYSDATE,-12) AND SYSDATE
           THEN 'Y'
           ELSE 'N'
      END) aps_actv_pd_flg,*/
    /*max(CASE WHEN dft.oli_stat_cd = 'C'
             AND dft.oli_mag_cd IN ('NCPR','UCPR')
             AND dft.oli_crt_dt >= add_months(sysdate,-12)
            THEN 'Y'
            ELSE 'N'
       END) aps_rpts_actv_pd_flg,*/
    /*MAX(CASE WHEN dft.oli_mag_cd = 'CRMG'
              AND ((dft.oli_svc_stat_cd = 'A'
                    AND dft.olo_stat_cd = 'C')
                   OR (dft.oli_svc_stat_cd = 'C'
                       AND dft.olo_stat_cd = 'R'
                       AND dft.oli_sub_rnw_ind = 'N'
                       AND dft.oli_end_dt > sysdate))
              AND nvl(dft.olo_entr_typ_cd,'Z') != 'F'
              AND dft.oli_sku_num > 5000000
              AND dft.oli_tot_amt > 0.01
              AND dft.oli_term_mth_cnt > 0
              AND dft.olo_hash_account_id = t2.crmg_comb_acct_id
             THEN 'Y'
             ELSE 'N'
        END) crmg_actv_pd_flg,*/
    , MAX(CASE WHEN dft.oli_mag_cd = 'CRO'
                    AND ((dft.oli_svc_stat_cd = 'A'
                          AND dft.olo_stat_cd = 'C')
                         OR (dft.oli_svc_stat_cd = 'C'
                             AND dft.olo_stat_cd = 'R'
                             AND dft.oli_sub_rnw_ind = 'N'
                             AND dft.oli_end_dt > sysdate))
                    AND nvl(dft.olo_entr_typ_cd, 'Z') != 'F'
                    AND dft.oli_tot_amt > 0.01
                    AND dft.oli_term_mth_cnt > 0
                    AND dft.olo_hash_account_id = t2.cro_comb_acct_id
    THEN 'Y'
          ELSE 'N'
          END)                             cro_actv_pd_flg
    , max(CASE WHEN dft.oli_stat_cd = 'C'
                    AND dft.oli_strt_dt BETWEEN ADD_MONTHS(SYSDATE, -12) AND SYSDATE
                    AND dft.oli_mag_cd = 'NCBK'
    THEN 'Y'
          ELSE 'N'
          END)                             ncbk_actv_pd_flg
    /*max(CASE WHEN dft.oli_stat_cd = 'C'
           AND dft.oli_strt_dt BETWEEN ADD_MONTHS(SYSDATE,-12) AND SYSDATE
            AND dft.oli_mag_cd = 'UCBK'
           THEN 'Y'
           ELSE 'N'
      END) ucbk_actv_pd_flg,*/
    /*MAX(CASE WHEN dft.oli_mag_cd = 'CRMG'
              AND ((dft.oli_svc_stat_cd = 'A'
                    AND dft.olo_stat_cd = 'C')
                   OR (dft.oli_svc_stat_cd = 'C'
                        AND dft.olo_stat_cd = 'R'
                        AND dft.oli_sub_rnw_ind = 'N'
                        AND dft.oli_end_dt > sysdate))
              AND ((sku.selection = 'A'
              AND sku.product = 'CRMG')
                   OR (sku.selection = 'X'
                       AND sku.product = 'CRMG'
                       AND dft.oli_term_mth_cnt > 1))
              AND dft.oli_sku_num > 5000000
              AND dft.oli_tot_amt > 0.01
              AND dft.oli_term_mth_cnt > 0
              AND nvl(dft.olo_entr_typ_cd,'Z') != 'F'
              AND dft.olo_hash_account_id = t2.crmg_comb_acct_id
        THEN 'Y'
        ELSE 'N'
        END) crmg_annual_flg,*/
    , MAX(CASE WHEN dft.oli_mag_cd = 'CRO'
                    AND ((dft.oli_svc_stat_cd = 'A'
                          AND dft.olo_stat_cd = 'C')
                         OR (dft.oli_svc_stat_cd = 'C'
                             AND dft.olo_stat_cd = 'R'
                             AND dft.oli_sub_rnw_ind = 'N'
                             AND dft.oli_end_dt > sysdate))
                    AND ((sku.selection = 'A'
                          AND sku.product = 'CRO')
                         OR (dft.oli_sku_num IS NULL
                             AND dft.oli_term_mth_cnt > 1)
                         OR (sku.selection = 'X'
                             AND sku.product = 'CRO'
                             AND dft.oli_term_mth_cnt > 1))
                    AND dft.oli_tot_amt > 0.01
                    AND dft.oli_term_mth_cnt > 0
                    AND nvl(dft.olo_entr_typ_cd, 'Z') != 'F'
                    AND dft.olo_hash_account_id = t2.cro_comb_acct_id
    THEN 'Y'
          ELSE 'N'
          END)                             cro_annual_flg
    , CASE WHEN substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
         nvl(dft.oli_crd_stat_cd, '')
                              ELSE NULL
                              END), 9) = 'F'
    THEN 'Y'
      ELSE 'N'
      END                                  crmg_canc_bad_dbt_flg
    , CASE
      WHEN substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
          THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
               nvl(dft.oli_CRD_STAT_CD, '')
                         ELSE NULL
                         END), 9) = 'F'
          THEN
              'Y'
      WHEN substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
          THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
               nvl(dft.oli_svc_stat_cd, '') ||
               nvl(dft.oli_canc_rsn_cd, '')
                         ELSE NULL
                         END), 9) IN ('C50', 'C06')
          THEN 'Y'
      ELSE 'N'
      END                                  cro_canc_bad_dbt_flg
    , CASE WHEN substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
         nvl(dft.oli_stat_cd, '')
                              ELSE NULL
                              END), 9, 1) = 'R'
    THEN 'Y'
      ELSE 'N'
      END                                  crmg_canc_cust_flg
    , CASE WHEN
    substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
        THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
             nvl(dft.oli_stat_cd, '')
                  ELSE NULL
                  END), 9, 1) = 'R'
    OR (substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
        THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
             nvl(dft.oli_svc_stat_cd, '') ||
             nvl(dft.oli_canc_rsn_cd, '')
                      ELSE NULL
                      END), 9, 1) = 'C'
        AND substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
        THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
             nvl(dft.oli_svc_stat_cd, '') ||
             nvl(dft.oli_canc_rsn_cd, 'xx')
                          ELSE NULL
                          END), 10, 2) NOT IN ('50', '06'))
    THEN 'Y'
      ELSE 'N'
      END                                  cro_canc_cust_flg
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDD'),'00010101') ||
                       nvl(dft.oli_crd_stat_cd,'')
                  ELSE null
             END),9) crmg_cr_stat_cd,*/
    , substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
         nvl(dft.oli_crd_stat_cd, '')
                    ELSE NULL
                    END), 9)               cro_cr_stat_cd
    /*max(CASE WHEN dft.oli_mag_cd = 'CRMG'
           THEN dft.oli_end_dt
           ELSE null
      END) crmg_exp_dt,*/
    , max(CASE WHEN dft.oli_mag_cd = 'CRO'
    THEN nvl(dft.oli_cro_acct_exp_dt, dft.oli_end_dt)
          ELSE NULL
          END)                             cro_exp_dt
    , cast(substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
                              AND dft.oli_crd_stat_cd = 'F'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
         to_char(dft.oli_canc_dt, 'YYYY-MM-DD HH24:MI:SS')
                    ELSE NULL
                    END), 9) AS TIMESTAMP)  crmg_lst_canc_bad_dbt_dt
    , cast(substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
                              AND dft.oli_stat_cd = 'R'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
         to_char(dft.oli_canc_dt, 'YYYY-MM-DD HH24:MI:SS')
                    ELSE NULL
                    END), 9) AS TIMESTAMP)  crmg_lst_canc_cust_dt
    , cast(substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
                              AND ((dft.oli_stat_cd = 'R')
                                   OR (dft.oli_svc_stat_cd = 'C'
                                       AND nvl(dft.oli_canc_rsn_cd, '00') NOT IN ('50', '06')))
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
         to_char(nvl(dft.oli_canc_dt, dft.oli_cro_acct_exp_dt), 'YYYY-MM-DD HH24:MI:SS')
                    ELSE NULL
                    END), 9) AS TIMESTAMP)  cro_lst_canc_cust_dt
    /*max(CASE WHEN dft.oli_mag_cd IN ('NCBK', 'UCBK', 'NCPR', 'UCPR')
           THEN NULLIF(GREATEST(nvl(ixo.dc_last_aps_ord_dt,to_date('19000101','YYYYMMDD')), nvl(dft.oli_crt_dt,to_date('19000101','YYYYMMDD'))),to_date('19000101','YYYYMMDD'))
           ELSE
            NULLIF(nvl(ixo.dc_last_aps_ord_dt,to_date('19000101','YYYYMMDD')),to_date('19000101','YYYYMMDD'))
      END) aps_lst_ord_dt,*/
    /*max(CASE WHEN dft.oli_mag_cd = 'NCPR'
             THEN dft.oli_crt_dt
             ELSE null
        END) nc_rpts_lst_ord_dt,*/
    , max(CASE WHEN dft.oli_mag_cd = 'NCBK'
    THEN dft.oli_crt_dt
          ELSE NULL
          END)                             ncbk_lst_ord_dt
    /*max(CASE WHEN dft.oli_mag_cd = 'UCPR'
           THEN dft.oli_crt_dt
           ELSE null
      END) uc_rpts_lst_ord_dt,*/
    /*max(CASE WHEN dft.oli_mag_cd = 'UCBK'
             THEN dft.oli_crt_dt
             ELSE null
        END) ucbk_lst_ord_dt,*/
    /*max(CASE WHEN dft.oli_mag_cd = 'CRMG'
             THEN dft.oli_pmt_dt
             ELSE null
        END) crmg_lst_pmt_dt,*/
    , max(CASE WHEN dft.oli_mag_cd = 'CRO'
    THEN dft.oli_pmt_dt
          ELSE NULL
          END)                             cro_lst_pmt_dt
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'NCPR'
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       nvl(dft.oli_shp_meth_cd,'')
                  ELSE null
             END),15) nc_rpts_lst_del_chnl_cd,*/
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'UCPR'
                    THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                         nvl(dft.oli_shp_meth_cd,'')
                    ELSE null
               END),15) uc_rpts_lst_del_chnl_cd,*/
    , max(CASE WHEN ixo.dc_last_aps_ord_dt IS NOT NULL
    THEN
        'Y'
          WHEN dft.oli_mag_cd IN ('NCPR', 'UCPR', 'NCBK', 'UCBK')
              THEN 'Y'
          ELSE 'N'
          END)                             aps_flg
    , max(CASE WHEN dft.oli_mag_cd = 'CRMG'
    THEN 'Y'
          ELSE 'N'
          END)                             crmg_flg
    , max(CASE WHEN dft.oli_mag_cd = 'CRO'
    THEN 'Y'
          ELSE 'N'
          END)                             cro_flg
    /*max(CASE WHEN dft.oli_mag_cd = 'NCPR'
           THEN 'Y'
           ELSE 'N'
      END) nc_rpts_flg,*/
    , max(CASE WHEN dft.oli_mag_cd = 'NCBK'
    THEN 'Y'
          ELSE 'N'
          END)                             ncbk_flg
    , max(CASE WHEN dft.oli_mag_cd = 'UCPR'
    THEN 'Y'
          ELSE 'N'
          END)                             uc_rpts_flg
    , max(CASE WHEN dft.oli_mag_cd = 'UCBK'
    THEN 'Y'
          ELSE 'N'
          END)                             ucbk_flg
    /*substring(min(CASE WHEN dft.oli_mag_cd = 'CRMG'
                   AND nvl(dft.oli_set_cd, 'Z') NOT IN ('B', 'D')
                   AND dft.oli_strt_dt > nvl(dft.t1_last_crmg_brk_dt,to_date('00010101','YYYYMMDD'))
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDD'),'99999999') ||
                       nvl(dft.oli_ext_keycd,dft.oli_int_keycd)
                  ELSE null
             END),9) crmg_curr_mbr_keycode,*/
    , substring(min(CASE WHEN dft.oli_mag_cd = 'CRO'
                              AND dft.oli_set_cd NOT IN ('B', 'D')
                              AND nvl(to_char(dft.oli_strt_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
                                  || nvl(to_char(dft.oli_end_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
                                  || lpad(dft.oli_itm_id, 21) > nvl(dft.t1_last_cro_acct_brk_dt, '0000000000000000000000000000')
                              AND dft.olo_hash_account_id = dft.t1_cro_curr_mbr_dt_acct_id
    THEN nvl(to_char(dft.oli_strt_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
         || nvl(to_char(dft.oli_end_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
         || lpad(dft.oli_itm_id, 21)
         || nvl(dft.oli_ext_keycd, dft.oli_int_keycd)
                    END), 50)              cro_curr_mbr_keycode
    /*substring(MAX(CASE WHEN dft.oli_mag_cd = 'CRMG'
                  AND dft.oli_svc_stat_cd = 'A'
                  AND dft.oli_sku_num > 5000000
                  AND dft.olo_stat_cd = 'C'
                  AND dft.olo_hash_account_id = t2.crmg_comb_acct_id
                 THEN NVL(TO_CHAR(dft.oli_crt_dt,'YYYYMMDD'),'00000000') ||
                               nvl(dft.oli_ext_keycd,dft.oli_int_keycd)
                 ELSE NULL
             END),9) crmg_curr_ord_keycode,*/
    , substring(MAX(CASE WHEN dft.oli_mag_cd = 'CRO'
                              AND dft.oli_svc_stat_cd = 'A'
                              AND dft.olo_stat_cd = 'C'
                              AND dft.olo_hash_account_id = t2.cro_comb_acct_id
    THEN NVL(TO_CHAR(dft.oli_crt_dt, 'YYYYMMDD'), '00000000') ||
         nvl(dft.oli_ext_keycd, dft.oli_int_keycd)
                    ELSE NULL
                    END), 9)               cro_curr_ord_keycode
    /*substring(min(CASE WHEN dft.oli_mag_cd = 'CRMG'
                  AND nvl(dft.oli_set_cd, 'Z') NOT IN ('B', 'D')
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDD'),'99999999') ||
                       nvl(dft.oli_ext_keycd,dft.oli_int_keycd)
                  ELSE null
             END),9) crmg_fst_mbr_keycode,*/
    , substring(min(CASE WHEN dft.oli_mag_cd = 'CRO'
                              AND dft.oli_set_cd NOT IN ('B', 'D')
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '99999999') ||
         nvl(dft.oli_ext_keycd, dft.oli_int_keycd)
                    ELSE NULL
                    END), 9)               cro_fst_mbr_keycode
    /*substring(min(CASE WHEN dft.oli_mag_cd IN ('NCPR', 'UCPR', 'NCBK', 'UCBK')
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       nvl(dft.oli_ext_keycd,dft.oli_int_keycd)
                  ELSE null
             END),15) aps_fst_ord_keycode,*/
    /*substring(min(CASE WHEN dft.oli_mag_cd = 'NCPR'
                    THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                         nvl(dft.oli_ext_keycd,dft.oli_int_keycd)
                    ELSE null
               END),15) nc_rpts_fst_ord_keycode,*/
    , substring(min(CASE WHEN dft.oli_mag_cd = 'NCBK'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDDHH24MISS'), '99999999010101') ||
         nvl(dft.oli_ext_keycd, dft.oli_int_keycd)
                    ELSE NULL
                    END), 15)              ncbk_fst_ord_keycode
    /*substring(min(CASE WHEN dft.oli_mag_cd = 'UCPR'
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       nvl(dft.oli_ext_keycd,dft.oli_int_keycd)
                  ELSE null
             END),15) uc_rpts_fst_ord_keycode,*/
    /*substring(min(CASE WHEN dft.oli_mag_cd = 'UCBK'
                    THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                         nvl(dft.oli_ext_keycd,dft.oli_int_keycd)
                    ELSE null
               END),15) ucbk_fst_ord_keycode,*/
    /*substring(max(CASE WHEN dft.oli_mag_cd IN ('NCPR', 'UCPR', 'NCBK', 'UCBK')
                    THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                         nvl(dft.oli_ext_keycd,dft.oli_int_keycd)
                    ELSE null
               END),15) aps_lst_ord_keycode,*/
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
                    THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDD'),'00010101') ||
                         nvl(dft.oli_ext_keycd,dft.oli_int_keycd)
                    ELSE null
               END),9) crmg_lst_ord_keycode,*/
    , substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
         nvl(dft.oli_ext_keycd, dft.oli_int_keycd)
                    ELSE NULL
                    END), 9)               cro_lst_ord_keycode
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'NCPR'
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       nvl(dft.oli_ext_keycd,dft.oli_int_keycd)
                  ELSE null
             END),15) nc_rpts_lst_ord_keycode,*/
    , substring(max(CASE WHEN dft.oli_mag_cd = 'NCBK'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDDHH24MISS'), '00010101010101') ||
         nvl(dft.oli_ext_keycd, dft.oli_int_keycd)
                    ELSE NULL
                    END), 15)              ncbk_lst_ord_keycode
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'UCPR'
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       nvl(dft.oli_ext_keycd,dft.oli_int_keycd)
                  ELSE null
             END),15) uc_rpts_lst_ord_keycode,*/
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'UCBK'
                    THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                         nvl(dft.oli_ext_keycd,dft.oli_int_keycd)
                    ELSE null
               END),15) ucbk_lst_ord_keycode,*/
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
                              THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDD'),'00010101') ||
                                   nvl(dft.oli_tot_gross_amt,'0')
                              ELSE null
                         END),9)  crmg_lst_sub_amt,*/
    , substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
         nvl(dft.oli_tot_gross_amt, '0')
                    ELSE NULL
                    END), 9)               cro_lst_sub_amt
    /*MAX(CASE WHEN dft.oli_mag_cd = 'CRMG'
            AND ((dft.oli_svc_stat_cd = 'A'
                  AND dft.olo_stat_cd = 'C')
                 OR (dft.oli_svc_stat_cd = 'C'
                      AND dft.olo_stat_cd = 'R'
                      AND dft.oli_sub_rnw_ind = 'N'
                      AND dft.oli_end_dt > sysdate))
            AND sku.product = 'CRMG'
            AND sku.selection = 'M'
            AND nvl(dft.olo_entr_typ_cd,'Z') != 'F'
            AND dft.oli_sku_num > 5000000
            AND dft.oli_tot_amt > 0.01
            AND dft.oli_term_mth_cnt > 0
            AND dft.olo_hash_account_id = t2.crmg_comb_acct_id
       THEN 'Y'
       ELSE 'N'
  END) crmg_monthly_flg,*/
    , MAX(CASE WHEN dft.oli_mag_cd = 'CRO'
                    AND ((dft.oli_svc_stat_cd = 'A'
                          AND dft.olo_stat_cd = 'C')
                         OR (dft.oli_svc_stat_cd = 'C'
                             AND dft.olo_stat_cd = 'R'
                             AND dft.oli_sub_rnw_ind = 'N'
                             AND dft.oli_end_dt > sysdate))
                    AND ((sku.selection = 'M'
                          AND sku.product = 'CRO')
                         OR (dft.oli_sku_num IS NULL
                             AND dft.oli_term_mth_cnt = 1))
                    AND nvl(dft.olo_entr_typ_cd, 'Z') != 'F'
                    AND dft.oli_tot_amt > 0.01
                    AND dft.oli_term_mth_cnt > 0
                    AND dft.olo_hash_account_id = t2.cro_comb_acct_id
    THEN 'Y'
          ELSE 'N'
          END)                             cro_monthly_flg
    , MAX(CASE WHEN dft.oli_mag_cd = 'CRO'
                    AND dft.olo_hash_account_id = t2.cro_comb_acct_id
                    AND dft.oli_svc_stat_cd = 'D'
                    AND dft.oli_set_cd IN ('B', 'D')
                    AND dft.oli_crt_dt > add_months(sysdate, -12)
    THEN 'Y'
          ELSE 'N'
          END)                             cro_non_sub_dnr_flg
    /*max(dft.t1_crmg_brks_cnt) crmg_brks_cnt,*/
    , max(dft.t1_cro_brks_cnt)             cro_brks_cnt
    /*trunc(count(CASE WHEN dft.oli_mag_cd = 'CRMG'
                    AND nvl(dft.oli_set_cd, 'Z') NOT IN ('B', 'D')
                    AND dft.oli_strt_dt > nvl(dft.t1_last_crmg_brk_dt,to_date('00010101','YYYYMMDD'))
                   THEN 'x'
                   ELSE null
              END) - .1) crmg_rnw_cnt,*/
    , trunc(count(CASE WHEN dft.oli_mag_cd = 'CRO'
                            AND dft.oli_set_cd NOT IN ('B', 'D')
                            AND nvl(dft.oli_crd_stat_cd, 'x') != 'F'
                            AND nvl(dft.oli_canc_rsn_cd, '00') NOT IN ('50', '06')
                            AND nvl(to_char(dft.oli_strt_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
                                || nvl(to_char(dft.oli_end_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
                                || lpad(dft.oli_itm_id, 21) > nvl(dft.t1_last_cro_acct_brk_dt, '0000000000000000000000000000')
                            AND dft.olo_hash_account_id = dft.t1_cro_curr_mbr_dt_acct_id
    THEN 'x'
                  ELSE NULL
                  END) - .1)               cro_rnw_cnt
    , MAX(CASE WHEN dft.oli_mag_cd = 'CRO'
                    AND dft.oli_svc_stat_cd = 'A'
                    AND dft.oli_set_cd IN ('C', 'E')
                    AND dft.oli_stat_cd = 'C'
                    AND dft.olo_stat_cd = 'C'
                    AND dft.olo_hash_account_id = t2.cro_comb_acct_id
    THEN 'Y'
          ELSE 'N'
          END)                             cro_rec_flg
    /*CASE WHEN ((MAX(nvl(an.crmg_lt_sub_flg,'N')) = 'Y') OR
       (MAX(CASE WHEN dft.oli_mag_cd = 'CRMG'
                      AND dft.oli_svc_stat_cd = 'A'
                      AND dft.oli_sku_num > 5000000
                      AND dft.olo_stat_cd = 'C'
                      AND dft.olo_hash_account_id = t2.crmg_comb_acct_id
                     THEN '1'
                     ELSE '0'
                END) = '1'))
       THEN 'A'
       ELSE substring(MAX(CASE  WHEN dft.oli_mag_cd = 'CRMG'
                              AND dft.olo_hash_account_id = t2.crmg_comb_acct_id
                             THEN NVL(TO_CHAR(dft.oli_crt_dt,'YYYYMMDD'),'00000000') ||
                                           nvl(decode(dft.oli_svc_stat_cd,'A','C',dft.oli_svc_stat_cd),'')
                       END),9)
  END crmg_svc_stat_cd,*/
    , CASE WHEN ((MAX(nvl(an.cro_lt_sub_flg, 'N')) = 'Y') OR
                 (MAX(CASE WHEN dft.oli_mag_cd = 'CRO'
                                AND dft.oli_svc_stat_cd = 'A'
                                AND dft.olo_stat_cd = 'C'
                                AND dft.olo_hash_account_id = t2.cro_comb_acct_id
                     THEN '1'
                      ELSE '0'
                      END) = '1'))
    THEN 'A'
      ELSE substring(MAX(CASE WHEN dft.oli_mag_cd = 'CRO'
                                   AND dft.olo_hash_account_id = t2.cro_comb_acct_id
          THEN NVL(TO_CHAR(dft.oli_crt_dt, 'YYYYMMDD'), '00000000') ||
               nvl(decode(dft.oli_svc_stat_cd, 'A', 'C', dft.oli_svc_stat_cd), '')
                         END), 9)
      END                                  cro_svc_stat_cd
    /*substring(min(CASE WHEN dft.oli_mag_cd = 'CRMG'
                   AND nvl(dft.oli_set_cd, 'Z') NOT IN ('B', 'D')
                   AND dft.oli_strt_dt > nvl(dft.t1_last_crmg_brk_dt,to_date('00010101','YYYYMMDD'))
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDD'),'99999999') ||
                       nvl(dft.oli_sub_src_cd,'')
                  ELSE null
             END),9) crmg_curr_mbr_src_cd,*/
    , substring(min(CASE WHEN dft.oli_mag_cd = 'CRO'
                              AND dft.oli_set_cd NOT IN ('B', 'D')
                              AND nvl(to_char(dft.oli_strt_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
                                  || nvl(to_char(dft.oli_end_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
                                  || lpad(nvl(dft.oli_itm_id, '0'), 21) > nvl(dft.t1_last_cro_acct_brk_dt, '0000000000000000000000000000')
                              AND dft.olo_hash_account_id = dft.t1_cro_curr_mbr_dt_acct_id
    THEN nvl(to_char(dft.oli_strt_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
         || nvl(to_char(dft.oli_end_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
         || lpad(nvl(dft.oli_itm_id, '0'), 21)
         || dft.oli_sub_src_cd
                    ELSE NULL
                    END), 50)              cro_curr_mbr_src_cd
    /*substring(MAX(CASE WHEN dft.oli_mag_cd = 'CRMG'
                  AND dft.oli_svc_stat_cd = 'A'
                  AND dft.oli_sku_num > 5000000
                  AND dft.olo_stat_cd = 'C'
                  AND dft.olo_hash_account_id = t2.crmg_comb_acct_id
                 THEN NVL(TO_CHAR(dft.oli_crt_dt,'YYYYMMDD'),'00000000') ||
                                           nvl(dft.oli_sub_src_cd,'')
                               ELSE NULL
            END),9) crmg_curr_ord_src_cd,*/
    , substring(MAX(CASE WHEN dft.oli_mag_cd = 'CRO'
                              AND dft.oli_svc_stat_cd = 'A'
                              AND dft.olo_stat_cd = 'C'
                              AND dft.olo_hash_account_id = t2.cro_comb_acct_id
    THEN NVL(TO_CHAR(dft.oli_crt_dt, 'YYYYMMDD'), '00000000') ||
         nvl(dft.oli_sub_src_cd, '')
                    ELSE NULL
                    END), 9)               cro_curr_ord_src_cd
    /*substring(min(CASE WHEN dft.oli_mag_cd IN ('NCPR', 'UCPR', 'NCBK', 'UCBK')
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       nvl(dft.oli_aps_src_cd,'')
                  ELSE null
             END),15) aps_fst_ord_src_cd,*/
    /*substring(min(CASE WHEN dft.oli_mag_cd = 'CRMG'
                    THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDD'),'99999999') ||
                         nvl(dft.oli_sub_src_cd,'')
                    ELSE null
               END),9) crmg_fst_ord_src_cd,*/
    , substring(min(CASE WHEN dft.oli_mag_cd = 'CRO'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '99999999') ||
         nvl(dft.oli_sub_src_cd, '')
                    ELSE NULL
                    END), 9)               cro_fst_ord_src_cd
    /*substring(min(CASE WHEN dft.oli_mag_cd = 'NCPR'
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       nvl(dft.oli_aps_src_cd,'')
                  ELSE null
             END),15) nc_rpts_fst_ord_src_cd,*/
    , substring(min(CASE WHEN dft.oli_mag_cd = 'NCBK'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDDHH24MISS'), '99999999010101') ||
         nvl(dft.oli_aps_src_cd, '')
                    ELSE NULL
                    END), 15)              ncbk_fst_ord_src_cd
    /*substring(min(CASE WHEN dft.oli_mag_cd = 'UCPR'
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       nvl(dft.oli_aps_src_cd,'')
                  ELSE null
             END),15) uc_rpts_fst_ord_src_cd,*/
    /*substring(min(CASE WHEN dft.oli_mag_cd = 'UCBK'
                    THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                         nvl(dft.oli_aps_src_cd,'')
                    ELSE null
               END),15) ucbk_fst_ord_src_cd,*/
    /*substring(max(CASE WHEN dft.oli_mag_cd IN ('NCPR', 'UCPR', 'NCBK', 'UCBK')
                    THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                         nvl(dft.oli_aps_src_cd,'')
                    ELSE null
               END),15) aps_lst_ord_src_cd,*/
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
                    THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDD'),'00010101') ||
                         nvl(dft.oli_sub_src_cd,'')
                    ELSE null
               END),9) crmg_lst_ord_src_cd,*/
    , substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00010101') ||
         nvl(dft.oli_sub_src_cd, '')
                    ELSE NULL
                    END), 9)               cro_lst_ord_src_cd
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'NCPR'
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       nvl(dft.oli_aps_src_cd,'')
                  ELSE null
             END),15) nc_rpts_lst_ord_src_cd,*/
    , substring(max(CASE WHEN dft.oli_mag_cd = 'NCBK'
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDDHH24MISS'), '00010101010101') ||
         nvl(dft.oli_aps_src_cd, '')
                    ELSE NULL
                    END), 15)              ncbk_lst_ord_src_cd
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'UCPR'
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       nvl(dft.oli_aps_src_cd,'')
                  ELSE null
             END),15) uc_rpts_lst_ord_src_cd,*/
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'UCBK'
                    THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                         nvl(dft.oli_aps_src_cd,'')
                    ELSE null
               END),15) ucbk_lst_ord_src_cd,*/
    /*substring(min(CASE WHEN dft.oli_mag_cd = 'CRMG'
                     AND nvl(dft.oli_set_cd, 'Z') NOT IN ('B', 'D')
                     AND dft.oli_crt_dt <= dft.t1_last_crmg_brk_dt
                     AND dft.oli_crt_dt > nvl(dft.t1_prior_crmg_brk_dt,to_date('00010101','YYYYMMDD'))
                    THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDD'),'99999999') ||
                         nvl(dft.oli_sub_src_cd,'')
                    ELSE null
               END),9) crmg_prior_mbr_src_cd,*/
    , substring(min(CASE WHEN dft.oli_mag_cd = 'CRO'
                              AND dft.oli_set_cd NOT IN ('B', 'D')
                              AND dft.oli_crt_dt <= dft.t1_last_cro_brk_dt
                              AND dft.oli_crt_dt > nvl(dft.t1_prior_cro_brk_dt, to_date('00010101', 'YYYYMMDD'))
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '99999999') ||
         nvl(dft.oli_sub_src_cd, '')
                    ELSE NULL
                    END), 9)               cro_prior_mbr_src_cd
    , MAX(CASE WHEN dft.oli_mag_cd = 'CRO'
                    AND dft.oli_svc_stat_cd != 'D'
                    AND dft.oli_set_cd IN ('B', 'D')
                    AND dft.oli_crt_dt > add_months(sysdate, -12)
                    AND dft.olo_hash_account_id = t2.cro_comb_acct_id
    THEN 'Y'
          ELSE 'N'
          END)                             cro_sub_dnr_flg
    /*substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
                             AND dft.oli_stat_cd = 'C'
                             AND dft.oli_svc_stat_cd = 'A'
                             AND dft.oli_sku_num > 5000000
                             AND dft.olo_stat_cd = 'C'
                             AND dft.olo_hash_account_id = t2.crmg_comb_acct_id
                            THEN nvl(to_char( dft.oli_crt_dt,'YYYYMMDD'),'00000000') ||
                                          nvl(dft.oli_term_mth_cnt,'0')
                       END),9)  crmg_curr_ord_term,*/
    , substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
                              AND dft.oli_stat_cd = 'C'
                              AND dft.oli_svc_stat_cd = 'A'
                              AND dft.olo_stat_cd = 'C'
                              AND dft.olo_hash_account_id = t2.cro_comb_acct_id
    THEN nvl(to_char(dft.oli_crt_dt, 'YYYYMMDD'), '00000000') ||
         nvl(dft.oli_term_mth_cnt, '0')
                    END), 9)               cro_curr_ord_term
    /*substring(max(CASE WHEN dft.oli_mag_cd IN ('NCPR', 'UCPR', 'NCBK', 'UCBK')
                  THEN nvl(to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       nvl(dft.oli_mag_cd,'')
                  WHEN nvl(dft.oli_mag_cd,'AAAA') NOT IN ('NCPR','UCPR','NCBK', 'UCBK') AND ixo.DC_LAST_APS_ORD_DT is not null
                  then '00010101010101' || 'REPORT'
                  ELSE null
             END),15) aps_lst_prod_type,*/
    , min(CASE WHEN dft.oli_mag_cd = 'CRO'
                    AND dft.oli_set_cd IN ('B', 'D')
    THEN dft.oli_crt_dt
          ELSE NULL
          END)                             cro_fst_dnr_dt
    , MAX(CASE WHEN dft.oli_mag_cd = 'CRO'
                    AND dft.oli_svc_stat_cd = 'A'
                    AND dft.oli_sub_rnw_ind = 'Y'
                    AND dft.olo_stat_cd = 'C'
                    AND dft.olo_hash_account_id = t2.cro_comb_acct_id
    THEN 'Y'
          ELSE 'N'
          END)                             cro_auto_rnw_flg
    /*MAX(CASE WHEN dft.oli_mag_cd = 'CRMG'
            AND dft.oli_svc_stat_cd = 'A'
            AND dft.oli_sub_rnw_ind = 'Y'
            AND dft.oli_sku_num > 5000000
            AND dft.olo_stat_cd = 'C'
            AND dft.olo_hash_account_id = t2.crmg_comb_acct_id
          THEN 'Y'
          ELSE 'N'
  END) crmg_auto_rnw_flg,*/
    , max(CASE WHEN dft.oli_mag_cd = 'CRO'
                    AND dft.oli_set_cd IN ('A', 'C', 'E')
    THEN 'Y'
          ELSE 'N'
          END)                             cro_non_dnr_flg
    /*min(CASE WHEN dft.oli_mag_cd = 'CRMG'
            AND nvl(dft.oli_set_cd, 'Z') NOT IN ('B','D')
            AND dft.oli_strt_dt > nvl(dft.t1_last_crmg_brk_dt,to_date('00010101','YYYYMMDD'))
           THEN dft.oli_crt_dt
           ELSE null
  END) crmg_curr_mbr_dt,*/
    , cast(substring(min(CASE WHEN dft.oli_mag_cd = 'CRO'
                              AND nvl(dft.oli_set_cd, 'Z') NOT IN ('B', 'D')
                              AND nvl(to_char(dft.oli_strt_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
                                  || nvl(to_char(dft.oli_end_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
                                  || lpad(nvl(dft.oli_itm_id, '0'), 21) > nvl(dft.t1_last_cro_acct_brk_dt, '0000000000000000000000000000')
                              AND dft.olo_hash_account_id = dft.t1_cro_curr_mbr_dt_acct_id
    THEN nvl(to_char(dft.oli_strt_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
         || nvl(to_char(dft.oli_end_dt, 'YYYYMMDDHH24MISS'), '00000000000000')
         || lpad(nvl(dft.oli_itm_id, '0'), 21)
         || to_char(dft.oli_crt_dt, 'YYYY-MM-DD HH24:MI:SS')
                    ELSE NULL
                    END), 50) AS TIMESTAMP) cro_curr_mbr_dt
    /*nvl(sum(CASE WHEN (dft.oli_mag_cd in ('NCBK','UCBK','NCPR','UCPR')
            AND ((dft.oli_sku_num < '5000000') OR (dft.oli_sku_num > '5000000' and dft.oli_stat_cd = 'C')))
           THEN dft.oli_tot_amt
      END),0) aps_ltd_pd_amt,*/
    /*nvl(sum(CASE WHEN (dft.oli_mag_cd = 'CRMG'
              AND ((dft.oli_sku_num < '5000000') OR (dft.oli_sku_num > '5000000' and dft.oli_stat_cd = 'C')))
             THEN dft.oli_tot_amt
        END),0) crmg_ltd_pd_amt,*/
    , nvl(sum(CASE WHEN (dft.oli_mag_cd = 'CRO'
                         AND ((nvl(dft.oli_sku_num, '1') < '5000000') OR (dft.oli_sku_num > '5000000' AND dft.oli_stat_cd = 'C')))
    THEN dft.oli_tot_amt
              END), 0)                     cro_ltd_pd_amt
    /*max(CASE WHEN dft.oli_svc_stat_cd = 'A'
            AND nvl(dft.oli_set_cd,'x') not in ('B','D')
            AND dft.oli_sku_num > 5000000
            AND dft.olo_stat_cd = 'C'
            AND dft.oli_mag_cd = 'CRMG'
           THEN dft.oli_crt_dt
      END) crmg_curr_ord_dt,*/
    , max(CASE WHEN dft.oli_svc_stat_cd = 'A'
                    AND nvl(dft.oli_set_cd, 'x') NOT IN ('B', 'D')
                    AND dft.olo_stat_cd = 'C'
                    AND dft.oli_mag_cd = 'CRO'
    THEN dft.oli_crt_dt
          END)                             cro_curr_ord_dt
    /*min(CASE WHEN dft.oli_mag_cd in ('NCPR','UCPR','NCBK','UCBK')
           THEN
           nullif(least(nvl(ixo.dc_last_aps_ord_dt,to_date('99991231','YYYYMMDD')),nvl(dft.oli_crt_dt,to_date('99991231','YYYYMMDD'))),to_date('99991231','YYYYMMDD'))
          else
            nullif(nvl(ixo.dc_last_aps_ord_dt, to_date('99991231','YYYYMMDD')),to_date('99991231','YYYYMMDD'))
      END) aps_fst_ord_dt,*/
    /*min(CASE WHEN dft.oli_mag_cd = 'CRMG'
              AND nvl(dft.oli_set_cd,'X') not in ('C','E')
             THEN dft.oli_crt_dt
        END) crmg_fst_ord_dt,*/
    , min(CASE WHEN dft.oli_mag_cd = 'CRO'
    THEN dft.oli_crt_dt
          END)                             cro_fst_ord_dt
    /*min(CASE WHEN dft.oli_mag_cd = 'NCPR'
           THEN dft.oli_crt_dt
      END) nc_rpts_fst_ord_dt,*/
    , min(CASE WHEN dft.oli_mag_cd = 'NCBK'
    THEN dft.oli_crt_dt
          END)                             ncbk_fst_ord_dt
    /*min(CASE WHEN dft.oli_mag_cd = 'UCPR'
           THEN dft.oli_crt_dt
      END) uc_rpts_fst_ord_dt,*/
    /*min(CASE WHEN dft.oli_mag_cd = 'UCBK'
             THEN dft.oli_crt_dt
        END) ucbk_fst_ord_dt,*/
    , max(CASE WHEN (dft.oli_crd_stat_cd = 'F'
                     OR (dft.oli_svc_stat_cd = 'C' AND dft.oli_canc_rsn_cd IN ('50', '06')))
                    AND dft.oli_mag_cd = 'CRO'
    THEN dft.oli_canc_dt
          END)                             cro_lst_canc_bad_dbt_dt
    /*max(CASE WHEN nvl(oli_set_cd ,'x') not in ('C','E')
            AND dft.oli_mag_cd = 'CRMG'
           THEN dft.oli_crt_dt
      END) crmg_lst_ord_dt,*/
    , max(CASE WHEN dft.oli_mag_cd = 'CRO'
    THEN dft.oli_crt_dt
          END)                             cro_lst_ord_dt
    , max(CASE WHEN dft.oli_set_cd IN ('B', 'D')
                    AND dft.oli_mag_cd = 'CRO'
    THEN dft.oli_crt_dt
          END)                             cro_lst_dnr_ord_dt
    , count(DISTINCT CASE WHEN dft.oli_mag_cd = 'CRMG'
                               AND dft.oli_canc_flg = 'Y'
    THEN dft.oli_ord_id
                     END)                  crmg_canc_cust_cnt
    , count(DISTINCT CASE WHEN dft.oli_mag_cd = 'CRO'
                               AND dft.oli_canc_flg = 'Y'
    THEN dft.oli_ord_id
                     END)                  cro_canc_cust_cnt
    /*count(distinct CASE WHEN dft.oli_mag_cd in ('NCBK','UCBK','NCPR','UCPR')
                      THEN dft.olo_individual_id || dft.olo_hash_account_id || nvl(dft.olo_ord_id,'0')
                 END) aps_ord_cnt,*/
    /*count(distinct CASE WHEN dft.oli_mag_cd = 'CRMG'
               THEN dft.oli_ord_id
          END) crmg_ord_cnt,*/
    , count(CASE WHEN dft.oli_mag_cd = 'CRO'
                      AND nvl(dft.oli_set_cd, 'x') NOT IN ('C', 'E')
    THEN 1
            END)                           cro_ord_cnt
    /*count(distinct CASE WHEN dft.oli_mag_cd = 'CRMG'
              AND dft.oli_sub_src_cd = 'D'
             THEN dft.oli_ord_id
        END) crmg_dm_ord_cnt,*/
    , count(CASE WHEN dft.oli_mag_cd = 'CRO'
                      AND nvl(dft.oli_set_cd, 'x') NOT IN ('C', 'E')
                      AND dft.oli_sub_src_cd = 'D'
    THEN 1
            END)                           cro_dm_ord_cnt
    /*count(distinct CASE WHEN dft.oli_mag_cd = 'CRMG'
              AND dft.oli_sub_src_cd in ('E','G','K','N','U')
             THEN dft.oli_ord_id
        END) crmg_em_ord_cnt,*/
    , count(CASE WHEN dft.oli_mag_cd = 'CRO'
                      AND nvl(dft.oli_set_cd, 'x') NOT IN ('C', 'E')
                      AND dft.oli_sub_src_cd IN ('E', 'G', 'K', 'N', 'U')
    THEN 1
            END)                           cro_em_ord_cnt
    , count(DISTINCT CASE WHEN dft.oli_mag_cd = 'CRO'
                               AND dft.oli_set_cd IN ('B', 'D')
    THEN dft.oli_ord_id
                     END)                  cro_dnr_ord_cnt
    /*count(CASE WHEN dft.oli_mag_cd in ('NCBK','UCBK','NCPR','UCPR')
              AND dft.oli_stat_cd = 'C'
             THEN 1
        END) aps_pd_prod_cnt,*/
    /*CASE WHEN substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
                              THEN to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS') || nvl(dft.oli_set_cd,'')
                         END),15) in ('C','E')
         THEN 'R'
         WHEN substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
                              THEN to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS') || nvl(dft.oli_set_cd,'A')
                         END),15) = 'A'
         THEN 'O'
         WHEN substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
                              THEN to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS') || nvl(dft.oli_set_cd,'') || nvl(dft.oli_svc_stat_cd,'')
                         END),15) in ('BD','DD')
         THEN 'N'
         WHEN substring(max(CASE WHEN dft.oli_mag_cd = 'CRMG'
                              THEN to_char(dft.oli_crt_dt,'YYYYMMDDHH24MISS') || nvl(dft.oli_set_cd,'')
                         END),15) in ('B','D')
         THEN 'D'
    END crmg_lst_sub_ord_role_cd,*/
    , CASE WHEN substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
    THEN to_char(dft.oli_crt_dt, 'YYYYMMDDHH24MISS') || nvl(dft.oli_set_cd, '')
                              END), 15) IN ('C', 'E')
    THEN 'R'
      WHEN substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
          THEN to_char(dft.oli_crt_dt, 'YYYYMMDDHH24MISS') || nvl(dft.oli_set_cd, '')
                         END), 15) = 'A'
          THEN 'O'
      WHEN substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
          THEN to_char(dft.oli_crt_dt, 'YYYYMMDDHH24MISS') || nvl(dft.oli_set_cd, '') || nvl(dft.oli_svc_stat_cd, '')
                         END), 15) IN ('BD', 'DD')
          THEN 'N'
      WHEN substring(max(CASE WHEN dft.oli_mag_cd = 'CRO'
          THEN to_char(dft.oli_crt_dt, 'YYYYMMDDHH24MISS') || nvl(dft.oli_set_cd, '')
                         END), 15) IN ('B', 'D')
          THEN 'D'
      END                                  cro_lst_sub_ord_role_cd
    , max(an.lst_logn_dt)                  lst_logn_dt
    , max(nvl(an.logn_cnt, 0))             logn_cnt
    , max(nvl(an.cro_lt_sub_flg, 'N'))     cro_lt_sub_flg
    /*max(nvl(an.crmg_lt_sub_flg,'N')) crmg_lt_sub_flg,*/
    , MAX(CASE WHEN dft.oli_mag_cd = 'CARP'
                    AND ((dft.oli_set_cd IN ('C', 'E')
                          AND dft.oli_svc_stat_cd = 'A'
                          AND dft.olo_stat_cd = 'C'
                          AND dft.oli_stat_cd = 'C')
                         OR (((dft.oli_svc_stat_cd = 'A'
                               AND dft.olo_stat_cd = 'C')
                              OR (dft.oli_svc_stat_cd = 'C'
                                  AND dft.olo_stat_cd = 'R'
                                  AND dft.oli_sub_rnw_ind = 'N'
                                  AND dft.oli_end_dt > sysdate))
                             AND (nvl(dft.olo_entr_typ_cd, 'Z') != 'F'
                                  AND dft.oli_tot_amt > 0.01
                                  AND dft.oli_term_mth_cnt > 0)))
                    AND dft.olo_hash_account_id = t2.carp_comb_acct_id
    THEN 'Y'
          ELSE 'N'
          END)                             carp_actv_flg
    , MAX(CASE WHEN dft.oli_mag_cd = 'CARP'
                    AND ((dft.oli_svc_stat_cd = 'A'
                          AND dft.olo_stat_cd = 'C')
                         OR (dft.oli_svc_stat_cd = 'C'
                             AND dft.olo_stat_cd = 'R'
                             AND dft.oli_sub_rnw_ind = 'N'
                             AND dft.oli_end_dt > sysdate))
                    AND nvl(dft.olo_entr_typ_cd, 'Z') != 'F'
                    AND dft.oli_tot_amt > 0.01
                    AND dft.oli_term_mth_cnt > 0
                    AND dft.olo_hash_account_id = t2.carp_comb_acct_id
    THEN 'Y'
          ELSE 'N'
          END)                             carp_actv_pd_flg
    , dft.household_id AS                  osl_hh_id
FROM agg.digital_full_transaction_temp_driver dft
    LEFT JOIN agg.digital_sku_temp_temp sku
        ON dft.oli_sku_num = sku.sku_num
    LEFT JOIN agg.digital_t2_temp t2
        ON dft.individual_id = t2.individual_id
    LEFT JOIN prod.agg_individual_xographic ixo
        ON dft.individual_id = ixo.individual_id
    LEFT JOIN agg.digital_an_temp an
        ON dft.individual_id = an.individual_id
GROUP BY dft.individual_id, dft.household_id;

analyse prod.agg_digital_summary;
