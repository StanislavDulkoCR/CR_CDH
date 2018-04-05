DROP TABLE IF EXISTS    cr_temp.indiv_oli_sku_temp;
CREATE TABLE            cr_temp.indiv_oli_sku_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(individual_id)
as
WITH cte_oli AS (SELECT
                     individual_id
                     , tot_amt
                     , pmt_dt
                     , set_cd
                     , stat_cd
                     , canc_dt
                     , crt_dt
                     , mag_cd
                     , crd_stat_cd
                     , sku_num
                     , canc_rsn_cd
                     , svc_stat_cd
                     , sub_src_cd
                     , end_dt
                     , sub_rnw_ind
                     , CASE WHEN substring(NVL(ext_keycd, int_keycd), 1, 2) = 'WF'
        THEN NULL
                       WHEN mag_cd = 'CRO' AND set_cd = 'A'
                            AND MONTHS_BETWEEN(strt_dt, MAX(end_dt)
                       OVER (
                           PARTITION BY individual_id, mag_cd, set_cd
                           ORDER BY strt_dt, NVL(ext_keycd, int_keycd) DESC
                           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) > 2
                           THEN 1 END AS cro_break_flg
                     , CASE WHEN substring(NVL(ext_keycd, int_keycd), 1, 2) = 'WF'
        THEN NULL
                       WHEN mag_cd = 'CARP' AND set_cd = 'A'
                            AND MONTHS_BETWEEN(strt_dt, MAX(end_dt)
                       OVER (
                           PARTITION BY individual_id, mag_cd, set_cd
                           ORDER BY strt_dt, NVL(ext_keycd, int_keycd) DESC
                           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) > 2
                           THEN 1 END AS carp_break_flg
                     , CASE WHEN substring(NVL(ext_keycd, int_keycd), 1, 2) = 'WF'
        THEN NULL
                       WHEN mag_cd = 'CRMG' AND NVL(set_cd, 'A') = 'A'
                            AND MONTHS_BETWEEN(strt_dt, MAX(end_dt)
                       OVER (
                           PARTITION BY individual_id, mag_cd, NVL(set_cd, 'A')
                           ORDER BY strt_dt, NVL(ext_keycd, int_keycd) DESC
                           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING )) > 2
                           THEN 1 END AS crmg_break_flg
                 FROM prod.agg_digital_item)
    , cte_skulkp AS (SELECT

                           sku_num       AS SKU_NUM
                         , sub_type_code AS SUB_TYP_CD
                         , sku_desc      AS SKU_DES
                         , product       AS PRODUCT
                         , value         AS VALUE
                         , unit_flag     AS UNIT_FLG
                         , selection     AS SELECTION
                         , comp_flag     AS COMP_FLG
                         , value_range   AS AMT
                         , update_date   AS MAINT_DT
                         , valid_from    AS VALID_FROM
                         , valid_to      AS VALID_TO

                     FROM prod.sku_lkup
)
SELECT
    t1.individual_id
    , SUM(CASE WHEN NVL(t1.sku_num, '1') < '5000000'
    THEN t1.tot_amt
          ELSE NULL END)                           AS oli_tot_amt
    , SUM(CASE WHEN t1.sku_num > '5000000' AND t1.stat_cd = 'C'
    THEN t1.tot_amt
          ELSE NULL END)                           AS oli_tot_amt2
    , MIN(CASE WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E')
    THEN t1.crt_dt
          ELSE NULL END)                           AS olo_prod_fst_ord_dt
    , MAX(CASE WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E')
    THEN t1.crt_dt
          ELSE NULL END)                           AS olo_prod_lst_ord_dt
    , MAX(t1.pmt_dt)                               AS oli_mx_pmt_dt
    , MIN(t1.crt_dt)                               AS oli_mn_crt_dt
    , MAX(t1.crt_dt)                               AS oli_mx_crt_dt
    , COUNT(CASE WHEN NVL(t1.set_cd, 'X') IN ('C', 'E')
    THEN 'x'
            ELSE NULL END)                         AS oli_ce_cnt
    , COUNT(CASE WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E')
    THEN 'x'
            ELSE NULL END)                         AS oli_xce_cnt
    , COUNT(CASE WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E') AND t1.sub_src_cd = 'D'
    THEN 'x'
            ELSE NULL END)                         AS oli_xce_d_cnt
    , COUNT(CASE WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E') AND t1.sub_src_cd IN ('E', 'G', 'K', 'N', 'U')
    THEN 'x'
            ELSE NULL END)                         AS oli_xce_egknu_cnt
    , MAX(CASE WHEN t1.stat_cd = 'R'
    THEN t1.canc_dt
          ELSE NULL END)                           AS oli_mx_canc_dt
    , MIN(CASE WHEN NVL(set_cd, 'X') IN ('C', 'E')
    THEN NVL(TO_CHAR(crt_dt, 'YYYYMMDD'), '99991231') || '5' ||
         decode(mag_cd, 'CRO', 0, 'CRMG', 1, 'HL', 2, 'NCBK', 3, 'UCBK', 4, 'NCPR', 5, 'UCPR', 6, 'CAPS', 7, 'CARP', 8,
                9) || mag_cd || ' REC'
          ELSE NVL(TO_CHAR(crt_dt, 'YYYYMMDD'), '99991231') || '4' ||
               decode(mag_cd, 'CRO', 0, 'CRMG', 1, 'HL', 2, 'NCBK', 3, 'UCBK', 4, 'NCPR', 5, 'UCPR', 6, 'CAPS', 7,
                      'CARP', 8, 9) || mag_cd END) AS oli_fst_prod_cd
    , MAX(CASE WHEN crd_stat_cd = 'F' OR (svc_stat_cd = 'C' AND canc_rsn_cd IN ('50', '06'))
    THEN canc_dt
          ELSE NULL END)                           AS oli_prod_lst_canc_bad_dbt_dt
    , COUNT(DISTINCT CASE WHEN mag_cd = 'CRO' AND set_cd = 'A'
    THEN 1 END)
      + COUNT(cro_break_flg)
      + COUNT(CASE WHEN mag_cd = 'CRO' AND set_cd IN ('B', 'D')
    THEN 1 END)                                    AS oli_cro_prod_cnt
    , COUNT(DISTINCT CASE WHEN mag_cd = 'CARP' AND set_cd = 'A'
    THEN 1 END)
      + COUNT(carp_break_flg)
      + COUNT(CASE WHEN mag_cd = 'CARP' AND set_cd IN ('B', 'D')
    THEN 1 END)                                    AS oli_carp_prod_cnt
    , COUNT(DISTINCT CASE WHEN mag_cd = 'CRMG' AND NVL(set_cd, 'A') = 'A'
    THEN 1 END)
      + COUNT(crmg_break_flg)
      + COUNT(CASE WHEN mag_cd = 'CRMG' AND set_cd IN ('B', 'D')
    THEN 1 END)                                    AS oli_crmg_prod_cnt
    , COUNT(CASE WHEN mag_cd IN ('NCBK', 'UCBK', 'NCPR', 'UCPR')
    THEN 1 END)                                    AS oli_aps_prod_cnt
    , COUNT(CASE WHEN mag_cd = 'CARP'
    THEN 1 END)                                    AS oli_carp_cnt
    , MAX(CASE WHEN mag_cd = 'CRO' AND set_cd = 'A'
    THEN 1
          ELSE 0 END)                              AS oli_cro_prod_cnta
    , COUNT(CASE WHEN set_cd IN ('B', 'D')
    THEN 1 END)                                    AS oli_prod_dnr_ord_cnt
    , MAX(CASE WHEN canc_dt IS NULL
    THEN end_dt
          WHEN canc_dt IS NOT NULL AND sub_rnw_ind IN ('Y', 'N')
              THEN end_dt END)                     AS oli_mx_end_dt
FROM cte_oli t1 left join cte_skulkp sku on t1.sku_num = sku.sku_num
where 1=1
    /*#TODO RM*/ and t1.individual_id in (  1216297895 , 1200000029 , 1200000039 , 1200000050 , 1200000058 , 1200000059 , 1200000060 , 1200000092 , 1200000100 , 1200000107 , 1200000204 , 1200000206 , 1200000244 , 1200000253 , 1200000265 , 1200000301 , 1200000089 , 1200000178 , 1200000270 , 1200000346 , 1200000405 , 1200000416 , 1200000433 , 1200000489 , 1200000536 , 1200000554 , 1200000602 , 1200000612 , 1200000629 , 1200000636 , 1200000640 , 1200000678 , 1200000002)
GROUP BY t1.individual_id