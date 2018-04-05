
/*Query for Non-Web Ad Server and Membership Feed - As of 11/08/2017*/

SELECT DISTINCT
      ltrim(n.acct_num, '0')             AS acct_num
    , (CASE WHEN f.fr_dnr_act_flg = 'Y'
    THEN '1'
       WHEN f.fr_dnr_lps_flg = 'Y'
           THEN '2'
       WHEN f.fr_dnr_inact_flg = 'Y'
           THEN '3'
       ELSE '0' END)                     AS fr_code
    , f.fr_mbr_gvng_lvl_cd
    , nvl(a.cr_actv_flg, 'N')            AS cr_actv_flg
    , nvl(b.hl_actv_flg, 'N')            AS hl_actv_flg
    , 'N'                                AS ma_actv_flg
    , 'N'                                AS shm_actv_flg
    , to_char(a.cr_exp_dt, 'MM/DD/YYYY') AS cr_exp_dt
    , to_char(b.hl_exp_dt, 'MM/DD/YYYY') AS hl_exp_dt
    , NULL                               AS ma_exp_dt
    , NULL                               AS shm_exp_dt
    , nvl(a.cr_auto_rnw_flg, 'N')        AS cr_auto_rnw_flg
    , nvl(b.hl_auto_rnw_flg, 'N')        AS hl_auto_rnw_flg
    , 'N'                                AS ma_auto_rnw_flg
    , 'N'                                AS shm_auto_rnw_flg
    , (CASE WHEN f.fr_dnr_act_flg = 'Y'
    THEN to_char(f.fr_lst_don_dt, 'MM/DD/YYYY')
       ELSE NULL END)                    AS fr_lst_don_dt
    , (CASE WHEN f.fr_dnr_act_flg = 'Y'
    THEN f.fr_lst_don_amt
       ELSE NULL END)                    AS fr_lst_don_amt
    , (CASE WHEN fr_ch_status = 'ACTIVE'
    THEN 'Y'
       ELSE 'N' END)                     AS fr_sustainer
    , a.cns_cds_acct_num
    , b.crh_cds_acct_num
FROM prod.agg_account_number n, prod.agg_fundraising_summary f,

    (/*CR Magazine*/

        SELECT DISTINCT
            m.individual_id
            , m.cr_actv_flg
            , m.cr_exp_dt
            , cns_cds_acct_num
            , (CASE WHEN m.cr_actv_flg = 'Y' AND a.keycode LIKE 'AUTO%'
            THEN 'Y'
               ELSE 'N' END) AS cr_auto_rnw_flg
        FROM prod.print_account_detail a, prod.agg_account_number c,

            /*CR Mag Actives*/
            (SELECT
                 n.individual_id
                 , 'Y'                         AS cr_actv_flg
                 , y.cr_exp_dt
                 , min(ltrim(n.acct_num, '0')) AS cns_cds_acct_num
             FROM prod.print_account_detail a, etl_proc.lookup_magazine i, prod.agg_account_number n,

                 (/*Latest expire date of active individuals*/
                     SELECT
                         n.individual_id
                         , max(last_day(i.pub_date)) AS cr_exp_dt
                     FROM prod.print_account_detail a, etl_proc.lookup_magazine i, prod.agg_account_number n
                     WHERE a.hash_account_id = n.hash_account_id
                           AND n.acct_prefix = 'CNS'
                           AND a.magazine_code = 'CNS'
                           AND i.magazine_code = 'CNS'
                           AND a.expr_iss_num = i.iss_num
                           AND ((a.svc_status_code IN ('A', 'E', 'C') AND last_day(i.pub_date) >= trunc(sysdate)) -- Active, credit-pending, or expired with a present or future expire date
                                OR (a.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(a.update_date)) <= 6)) -- Suspended within past 6 months
                           AND a.purged_ind IS NULL
                     GROUP BY n.individual_id) y

             WHERE n.individual_id = y.individual_id
                   AND a.hash_account_id = n.hash_account_id
                   AND n.acct_prefix = 'CNS'
                   AND a.magazine_code = 'CNS'
                   AND i.magazine_code = 'CNS'
                   AND a.expr_iss_num = i.iss_num
                   AND ((a.svc_status_code IN ('A', 'E', 'C') AND last_day(i.pub_date) >= trunc(sysdate)) -- Active, credit-pending, or expired with a present or future expire date
                        OR (a.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(a.update_date)) <= 6)) -- Suspended within past 6 months
                   AND a.purged_ind IS NULL
                   AND last_day(i.pub_date) = y.cr_exp_dt
             GROUP BY n.individual_id, y.cr_exp_dt

             UNION ALL

             /*CR Mag Inactives*/

             SELECT
                 v.individual_id
                 , v.cr_actv_flg
                 , v.cr_exp_dt
                 , min(v.cns_cds_acct_num) AS cns_cds_acct_num
             FROM

                 (SELECT
                      n.individual_id
                      , ltrim(n.acct_num, '0')                    AS cns_cds_acct_num
                      , 'N'                                       AS cr_actv_flg
                      , nvl(max(o.canc_dt), last_day(i.pub_date)) AS cr_exp_dt
                  FROM prod.print_account_detail a, prod.agg_print_order o, prod.agg_account_number n, etl_proc.lookup_magazine i
                  WHERE n.individual_id = o.individual_id
                        AND a.hash_account_id = n.hash_account_id
                        AND a.hash_account_id = o.hash_account_id
                        AND a.magazine_code = 'CNS'
                        AND o.mag_cd = 'CNS'
                        AND n.acct_prefix = 'CNS'
                        AND a.svc_status_code = 'B' -- canceled
                        AND a.purged_ind IS NULL
                        AND i.magazine_code = 'CNS'
                        AND a.expr_iss_num = i.iss_num
                  GROUP BY n.individual_id, ltrim(n.acct_num, '0'), last_day(i.pub_date)

                  UNION ALL

                  SELECT DISTINCT
                      n.individual_id
                      , ltrim(n.acct_num, '0') AS cns_cds_acct_num
                      , 'N'                    AS cr_actv_flg
                      , last_day(i.pub_date)   AS cr_exp_dt
                  FROM prod.print_account_detail a, etl_proc.lookup_magazine i, prod.agg_account_number n
                  WHERE a.hash_account_id = n.hash_account_id
                        AND n.acct_prefix = 'CNS'
                        AND a.magazine_code = 'CNS'
                        AND i.magazine_code = 'CNS'
                        AND a.expr_iss_num = i.iss_num
                        AND ((a.svc_status_code IN ('A', 'E', 'C') AND last_day(i.pub_date) < trunc(sysdate))-- Active, credit-pending, or expired with a past expire date
                             OR (a.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(a.update_date)) > 6) -- suspended more than 6 month ago
                             OR nvl(a.svc_status_code, 'X') NOT IN ('A', 'E', 'C', 'G', 'B', 'D')) -- Not: active, credit-pending, expired, suspended, canceled, or non-subscribing donor
                        AND a.purged_ind IS NULL) v,

                 (/*Latest expire date of inactive individuals*/
                     SELECT
                         w.individual_id
                         , max(w.cr_exp_dt) AS max_cr_exp_dt
                     FROM

                         (SELECT
                              n.individual_id
                              , ltrim(n.acct_num, '0')                    AS cns_cds_acct_num
                              , nvl(max(o.canc_dt), last_day(i.pub_date)) AS cr_exp_dt
                          FROM prod.print_account_detail a, prod.agg_print_order o, prod.agg_account_number n, etl_proc.lookup_magazine i
                          WHERE n.individual_id = o.individual_id
                                AND a.hash_account_id = n.hash_account_id
                                AND a.hash_account_id = o.hash_account_id
                                AND a.magazine_code = 'CNS'
                                AND o.mag_cd = 'CNS'
                                AND n.acct_prefix = 'CNS'
                                AND a.svc_status_code = 'B' -- canceled
                                AND a.purged_ind IS NULL
                                AND i.magazine_code = 'CNS'
                                AND a.expr_iss_num = i.iss_num
                          GROUP BY n.individual_id, ltrim(n.acct_num, '0'), last_day(i.pub_date)

                          UNION ALL

                          SELECT DISTINCT
                              n.individual_id
                              , ltrim(n.acct_num, '0') AS cns_cds_acct_num
                              , last_day(i.pub_date)   AS cr_exp_dt
                          FROM prod.print_account_detail a, etl_proc.lookup_magazine i, prod.agg_account_number n
                          WHERE a.hash_account_id = n.hash_account_id
                                AND n.acct_prefix = 'CNS'
                                AND a.magazine_code = 'CNS'
                                AND i.magazine_code = 'CNS'
                                AND a.expr_iss_num = i.iss_num
                                AND ((a.svc_status_code IN ('A', 'E', 'C') AND last_day(i.pub_date) < trunc(sysdate))-- Active, credit-pending, or expired with a past expire date
                                     OR (a.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(a.update_date)) > 6) -- suspended more than 6 month ago
                                     OR nvl(a.svc_status_code, 'X') NOT IN ('A', 'E', 'C', 'G', 'B', 'D')) -- Not: active, credit-pending, expired, suspended, canceled, or non-subscribing donor
                                AND a.purged_ind IS NULL) w,

                         (/*Excluding active CR mag individuals*/
                             SELECT DISTINCT n.individual_id
                             FROM prod.agg_account_number n

                             MINUS

                             SELECT DISTINCT n.individual_id
                             FROM prod.print_account_detail a, etl_proc.lookup_magazine i, prod.agg_account_number n
                             WHERE a.hash_account_id = n.hash_account_id
                                   AND n.acct_prefix = 'CNS'
                                   AND a.magazine_code = 'CNS'
                                   AND i.magazine_code = 'CNS'
                                   AND a.expr_iss_num = i.iss_num
                                   AND ((a.svc_status_code IN ('A', 'E', 'C') AND last_day(i.pub_date) >= trunc(sysdate)) -- Active, credit-pending, or expired with a present or future expire date
                                        OR (a.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(a.update_date)) <= 6)) -- Suspended within past 6 months
                                   AND a.purged_ind IS NULL) p

                     WHERE w.individual_id = p.individual_id
                     GROUP BY w.individual_id) z

             WHERE v.individual_id = z.individual_id
                   AND v.cr_exp_dt = z.max_cr_exp_dt
             GROUP BY v.individual_id, v.cr_actv_flg, v.cr_exp_dt) m

        WHERE m.individual_id = c.individual_id
              AND a.hash_account_id = c.hash_account_id
              AND m.cns_cds_acct_num = ltrim(c.acct_num, '0')
              AND c.acct_prefix = 'CNS'
              AND a.magazine_code = 'CNS') a,

    (/*CR on Health*/

        SELECT DISTINCT
            m.individual_id
            , m.hl_actv_flg
            , m.hl_exp_dt
            , crh_cds_acct_num
            , (CASE WHEN m.hl_actv_flg = 'Y' AND a.keycode LIKE 'AUTO%'
            THEN 'Y'
               ELSE 'N' END) AS hl_auto_rnw_flg
        FROM prod.print_account_detail a, prod.agg_account_number c,

            /*CR on Health Actives*/
            (SELECT
                 n.individual_id
                 , 'Y'                         AS hl_actv_flg
                 , y.hl_exp_dt
                 , min(ltrim(n.acct_num, '0')) AS crh_cds_acct_num
             FROM prod.print_account_detail a, etl_proc.lookup_magazine i, prod.agg_account_number n,

                 (/*Latest expire date of active individuals*/
                     SELECT
                         n.individual_id
                         , max(last_day(i.pub_date)) AS hl_exp_dt
                     FROM prod.print_account_detail a, etl_proc.lookup_magazine i, prod.agg_account_number n
                     WHERE a.hash_account_id = n.hash_account_id
                           AND n.acct_prefix = 'CRH'
                           AND a.magazine_code = 'CRH'
                           AND i.magazine_code = 'CRH'
                           AND a.expr_iss_num = i.iss_num
                           AND ((a.svc_status_code IN ('A', 'E', 'C') AND last_day(i.pub_date) >= trunc(sysdate)) -- Active, credit-pending, or expired with a present or future expire date
                                OR (a.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(a.update_date)) <= 6)) -- Suspended within past 6 months
                           AND a.purged_ind IS NULL
                     GROUP BY n.individual_id) y

             WHERE n.individual_id = y.individual_id
                   AND a.hash_account_id = n.hash_account_id
                   AND n.acct_prefix = 'CRH'
                   AND a.magazine_code = 'CRH'
                   AND i.magazine_code = 'CRH'
                   AND a.expr_iss_num = i.iss_num
                   AND ((a.svc_status_code IN ('A', 'E', 'C') AND last_day(i.pub_date) >= trunc(sysdate)) -- Active, credit-pending, or expired with a present or future expire date
                        OR (a.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(a.update_date)) <= 6)) -- Suspended within past 6 months
                   AND a.purged_ind IS NULL
                   AND last_day(i.pub_date) = y.hl_exp_dt
             GROUP BY n.individual_id, y.hl_exp_dt

             UNION ALL

             /*CR on Health Inactives*/

             SELECT
                 v.individual_id
                 , v.hl_actv_flg
                 , v.hl_exp_dt
                 , min(v.crh_cds_acct_num) AS crh_cds_acct_num
             FROM

                 (SELECT
                      n.individual_id
                      , ltrim(n.acct_num, '0')                    AS crh_cds_acct_num
                      , 'N'                                       AS hl_actv_flg
                      , nvl(max(o.canc_dt), last_day(i.pub_date)) AS hl_exp_dt
                  FROM prod.print_account_detail a, prod.agg_print_order o, prod.agg_account_number n, etl_proc.lookup_magazine i
                  WHERE n.individual_id = o.individual_id
                        AND a.hash_account_id = n.hash_account_id
                        AND a.hash_account_id = o.hash_account_id
                        AND a.magazine_code = 'CRH'
                        AND o.mag_cd = 'CRH'
                        AND n.acct_prefix = 'CRH'
                        AND a.svc_status_code = 'B' -- canceled
                        AND a.purged_ind IS NULL
                        AND i.magazine_code = 'CRH'
                        AND a.expr_iss_num = i.iss_num
                  GROUP BY n.individual_id, ltrim(n.acct_num, '0'), last_day(i.pub_date)

                  UNION ALL

                  SELECT DISTINCT
                      n.individual_id
                      , ltrim(n.acct_num, '0') AS crh_cds_acct_num
                      , 'N'                    AS hl_actv_flg
                      , last_day(i.pub_date)   AS hl_exp_dt
                  FROM prod.print_account_detail a, etl_proc.lookup_magazine i, prod.agg_account_number n
                  WHERE a.hash_account_id = n.hash_account_id
                        AND n.acct_prefix = 'CRH'
                        AND a.magazine_code = 'CRH'
                        AND i.magazine_code = 'CRH'
                        AND a.expr_iss_num = i.iss_num
                        AND ((a.svc_status_code IN ('A', 'E', 'C') AND last_day(i.pub_date) < trunc(sysdate))-- Active, credit-pending, or expired with a past expire date
                             OR (a.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(a.update_date)) > 6) -- suspended more than 6 month ago
                             OR nvl(a.svc_status_code, 'X') NOT IN ('A', 'E', 'C', 'G', 'B', 'D')) -- Not: active, credit-pending, expired, suspended, canceled, or non-subscribing donor
                        AND a.purged_ind IS NULL) v,

                 (/*Latest expire date of inactive individuals*/
                     SELECT
                         w.individual_id
                         , max(w.hl_exp_dt) AS max_hl_exp_dt
                     FROM

                         (SELECT
                              n.individual_id
                              , ltrim(n.acct_num, '0')                    AS crh_cds_acct_num
                              , nvl(max(o.canc_dt), last_day(i.pub_date)) AS hl_exp_dt
                          FROM prod.print_account_detail a, prod.agg_print_order o, prod.agg_account_number n, etl_proc.lookup_magazine i
                          WHERE n.individual_id = o.individual_id
                                AND a.hash_account_id = n.hash_account_id
                                AND a.hash_account_id = o.hash_account_id
                                AND a.magazine_code = 'CRH'
                                AND o.mag_cd = 'CRH'
                                AND n.acct_prefix = 'CRH'
                                AND a.svc_status_code = 'B' -- canceled
                                AND a.purged_ind IS NULL
                                AND i.magazine_code = 'CRH'
                                AND a.expr_iss_num = i.iss_num
                          GROUP BY n.individual_id, ltrim(n.acct_num, '0'), last_day(i.pub_date)

                          UNION ALL

                          SELECT DISTINCT
                              n.individual_id
                              , ltrim(n.acct_num, '0') AS crh_cds_acct_num
                              , last_day(i.pub_date)   AS hl_exp_dt
                          FROM prod.print_account_detail a, etl_proc.lookup_magazine i, prod.agg_account_number n
                          WHERE a.hash_account_id = n.hash_account_id
                                AND n.acct_prefix = 'CRH'
                                AND a.magazine_code = 'CRH'
                                AND i.magazine_code = 'CRH'
                                AND a.expr_iss_num = i.iss_num
                                AND ((a.svc_status_code IN ('A', 'E', 'C') AND last_day(i.pub_date) < trunc(sysdate))-- Active, credit-pending, or expired with a past expire date
                                     OR (a.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(a.update_date)) > 6) -- suspended more than 6 month ago
                                     OR nvl(a.svc_status_code, 'X') NOT IN ('A', 'E', 'C', 'G', 'B', 'D')) -- Not: active, credit-pending, expired, suspended, canceled, or non-subscribing donor
                                AND a.purged_ind IS NULL) w,

                         (/*Excluding active CR on Health individuals*/
                             SELECT DISTINCT n.individual_id
                             FROM prod.agg_account_number n

                             MINUS

                             SELECT DISTINCT n.individual_id
                             FROM prod.print_account_detail a, etl_proc.lookup_magazine i, prod.agg_account_number n
                             WHERE a.hash_account_id = n.hash_account_id
                                   AND n.acct_prefix = 'CRH'
                                   AND a.magazine_code = 'CRH'
                                   AND i.magazine_code = 'CRH'
                                   AND a.expr_iss_num = i.iss_num
                                   AND ((a.svc_status_code IN ('A', 'E', 'C') AND last_day(i.pub_date) >= trunc(sysdate)) -- Active, credit-pending, or expired with a present or future expire date
                                        OR (a.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(a.update_date)) <= 6)) -- Suspended within past 6 months
                                   AND a.purged_ind IS NULL) p

                     WHERE w.individual_id = p.individual_id
                     GROUP BY w.individual_id) z

             WHERE v.individual_id = z.individual_id
                   AND v.hl_exp_dt = z.max_hl_exp_dt
             GROUP BY v.individual_id, v.hl_actv_flg, v.hl_exp_dt) m

        WHERE m.individual_id = c.individual_id
              AND a.hash_account_id = c.hash_account_id
              AND m.crh_cds_acct_num = ltrim(c.acct_num, '0')
              AND c.acct_prefix = 'CRH'
              AND a.magazine_code = 'CRH') b

WHERE n.individual_id = a.individual_id ( + )
      AND n.individual_id = b.individual_id ( + )
      AND n.individual_id = f.individual_id ( + )
      AND n.acct_prefix = 'PWI'
      AND (a.cr_exp_dt > add_months(trunc(sysdate), -6)
           OR b.hl_exp_dt > add_months(trunc(sysdate), -6)
           OR f.fr_dnr_act_flg = 'Y')
ORDER BY cast(ltrim(n.acct_num, '0') AS BIGINT)
;