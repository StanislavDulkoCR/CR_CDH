create table cr_temp.mt_books_summary as
SELECT driver.INDIVIDUAL_ID,
       max(id.household_id) as hh_id
       ,MAX (
          CASE
             WHEN    io.dc_cdb_last_ord_dt IS NOT NULL
                  OR bo.individual_id IS NOT NULL
             THEN
                'Y'
             ELSE
                'N'
          END)
          AS BK_FLG,
       MAX (
          CASE
             WHEN     bi.cdr_flg = 'Y'
                  AND bo.ord_dt >= ADD_MONTHS (SYSDATE, -12)
                  AND NVL (bo.dnr_recip_cd, 'Z') != 'R'
             THEN
                'Y'
             ELSE
                'N'
          END)
          AS CDR_ACTV_FLG,
       MAX (
          CASE
             WHEN     bo.ord_dt >= ADD_MONTHS (SYSDATE, -12)
                  AND NVL (bo.dnr_recip_cd, 'Z') != 'R'
                  AND NVL (bo.ord_prem_flg, 'Z') != 'Y'
             THEN
                'Y'
             ELSE
                'N'
          END)
          AS BK_ACTV_FLG,
       MAX (
          CASE
             WHEN     bo.ord_dt >= ADD_MONTHS (SYSDATE, -12)
                  AND bo.pmt_amt > 0
                  AND bo.pmt_amt >= bo.ord_value_amt
                  AND NVL (bo.ord_prem_flg, 'Z') != 'Y'
             THEN
                'Y'
             ELSE
                'N'
          END)
          AS BK_ACTV_PD_FLG,
       MAX (bos.pmt_amt)
          AS BK_LTD_PD_AMT,
       MAX (
          NULLIF (
             GREATEST (
                NVL (io.dc_cdb_last_ord_dt, TO_DATE ('19000101', 'YYYYMMDD')),
                NVL (
                   CASE
                      WHEN bo.ord_num = bi.ord_num AND bi.cdr_flg = 'Y'
                      THEN
                         bo.ord_dt
                      ELSE
                         NULL
                   END,
                   TO_DATE ('19000101', 'YYYYMMDD'))),
             TO_DATE ('19000101', 'YYYYMMDD')))
          AS CDR_LST_ORD_DT,
       MAX (bo.LST_PMT_DT)
          AS BK_LST_PMT_DT,
       substring (
          MIN (
             CASE
                WHEN bi.cdr_flg = 'Y'
                THEN
                      NVL (TO_CHAR (bo.ord_dt, 'YYYYMMDD'), '99990101')
                   || bo.keycode
                ELSE
                   NULL
             END),
          9)
          AS CDR_FST_ORD_KEYCODE,
       substring (
          MAX (
             CASE
                WHEN bi.cdr_flg = 'Y'
                THEN
                      NVL (TO_CHAR (bo.ord_dt, 'YYYYMMDD'), '99990101')
                   || bo.keycode
                ELSE
                   NULL
             END),
          9)
          AS CDR_LST_ORD_KEYCODE,
         SUM (CASE WHEN bi.cdr_flg = 'Y' THEN 1 ELSE 0 END)
       + MAX (CASE WHEN io.dc_cdb_last_ord_dt IS NOT NULL THEN 1 ELSE 0 END)
          AS CDR_NUM_ORD
FROM ((((SELECT DISTINCT agg_books_order.INDIVIDUAL_ID
         FROM prod.agg_books_order agg_books_order
         UNION
         SELECT agg_individual_xographic.INDIVIDUAL_ID
         FROM prod.agg_individual_xographic agg_individual_xographic
         WHERE agg_individual_xographic.DC_CDB_LAST_ORD_DT IS NOT NULL) driver
        LEFT OUTER JOIN
        (SELECT agg_individual_xographic.INDIVIDUAL_ID,
                agg_individual_xographic.DC_CDB_LAST_ORD_DT
         FROM prod.agg_individual_xographic agg_individual_xographic
         WHERE agg_individual_xographic.DC_CDB_LAST_ORD_DT IS NOT NULL) io
           ON (driver.INDIVIDUAL_ID = io.INDIVIDUAL_ID))
       LEFT OUTER JOIN prod.agg_books_order bo
          ON (driver.INDIVIDUAL_ID = bo.INDIVIDUAL_ID))
      LEFT OUTER JOIN prod.agg_books_item bi
         ON (bo.ORD_NUM = bi.ORD_NUM))
     LEFT OUTER JOIN
     (SELECT agg_books_order.INDIVIDUAL_ID,
             SUM (pmt_amt) AS PMT_AMT
      FROM prod.agg_books_order agg_books_order
      GROUP BY agg_books_order.INDIVIDUAL_ID) bos
        ON (driver.INDIVIDUAL_ID = bos.INDIVIDUAL_ID)
        /*SD 9/7/17: addition to propagate HH_ID */
     left outer join prod.individual id
        on driver.individual_id = id.individual_id
        /*SD 9/7/17: end*/
GROUP BY driver.INDIVIDUAL_ID
;
