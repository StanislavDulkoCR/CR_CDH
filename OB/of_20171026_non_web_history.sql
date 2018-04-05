/***************************************************************************
*            (C) Copyright Consumer Reports 2017
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Outbound Feed
* Module Name:  17.22 NON_WEB_HISTORY.sql
* Date       :  2017/10/26
* Dev & QA   :  Stanislav Dulko
***************************************************************************/

drop table if exists    cr_temp.non_web_sd;
CREATE TABLE            cr_temp.non_web_sd
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (acct_num)
    AS
WITH cte_actv_flg AS (

    SELECT
        aan.individual_id
        , aan.hash_account_id
        , aan.acct_num
        , aan.acct_prefix
        , CASE
          WHEN pad.magazine_code = 'CNS'
               AND lmag.magazine_code = 'CNS'
               AND pad.purged_ind IS NULL
               AND pad.svc_status_code IN ('A', 'E', 'C')
               AND last_day(lmag.pub_date) >= trunc(sysdate)
              THEN 'Y'
          WHEN pad.magazine_code = 'CNS'
               AND lmag.magazine_code = 'CNS'
               AND pad.purged_ind IS NULL
               AND pad.svc_status_code = 'G'
               AND months_between(trunc(sysdate), trunc(pad.update_date)) <= 6
              THEN 'Y'
          ELSE 'N'
          END                     AS cr_actv_flg
        , CASE
          WHEN pad.magazine_code = 'CRH'
               AND lmag.magazine_code = 'CRH'
               AND pad.purged_ind IS NULL
               AND pad.svc_status_code IN ('A', 'E', 'C')
               AND last_day(lmag.pub_date) >= trunc(sysdate)
              THEN 'Y'
          WHEN pad.magazine_code = 'CRH'
               AND lmag.magazine_code = 'CRH'
               AND pad.purged_ind IS NULL
               AND pad.svc_status_code = 'G'
               AND months_between(trunc(sysdate), trunc(pad.update_date)) <= 6
              THEN 'Y'
          ELSE 'N'
          END                     AS hl_actv_flg

        , cast('N' AS VARCHAR(1)) AS ma_actv_flg
        , cast('N' AS VARCHAR(1)) AS shm_actv_flg

        , pad.magazine_code       AS pad_magazine_code
        , pad.svc_status_code     AS pad_svc_status_code
        , pad.update_date         AS pad_update_date
        , pad.keycode             AS pad_keycode
        , pad.purged_ind          AS pad_purged_ind

        , lmag.magazine_code      AS lmag_magazine_code
        , lmag.pub_date           AS lmag_pub_date



    FROM prod.agg_account_number aan
        LEFT JOIN prod.print_account_detail pad
            ON aan.hash_account_id = pad.hash_account_id
        LEFT JOIN etl_proc.lookup_magazine lmag
            ON pad.expr_iss_num = lmag.iss_num
               AND pad.magazine_code = lmag.magazine_code

    WHERE exists(SELECT NULL
                 FROM prod.agg_fundraising_summary ex0
                 WHERE 1 = 1 AND ex0.individual_id = aan.individual_id
                       AND ex0.fr_dnr_act_flg = 'Y')
          OR
          exists(SELECT NULL
                 FROM prod.agg_account_number ex1
                 WHERE 1 = 1
                       AND ex1.individual_id = aan.individual_id AND ex1.acct_prefix = 'PWI')
), cte_exp_dt AS (

    SELECT
        flg.individual_id
        , flg.hash_account_id
        , flg.acct_prefix
        , flg.acct_num
        , flg.cr_actv_flg
        , flg.hl_actv_flg
        , flg.ma_actv_flg
        , flg.shm_actv_flg
        , flg.pad_magazine_code
        , flg.pad_svc_status_code
        , flg.pad_update_date
        , flg.pad_keycode
        , flg.lmag_magazine_code
        , flg.lmag_pub_date

        /*CNS*/
        , CASE WHEN pad_magazine_code = 'CNS'
                    AND lmag_magazine_code = 'CNS'
                    AND apo.mag_cd = 'CNS'
                    AND flg.cr_actv_flg = 'Y'
                    AND pad_purged_ind IS NULL
        THEN last_day(lmag_pub_date) END              AS cns_b0_pub_date
        , CASE
          WHEN pad_svc_status_code = 'B'
               AND pad_magazine_code = 'CNS'
               AND lmag_magazine_code = 'CNS'
               AND apo.mag_cd = 'CNS'
               AND flg.cr_actv_flg = 'N'
               AND pad_purged_ind IS NULL
              THEN trunc(apo.canc_dt) END             AS cns_b1_canc_dt
        , CASE
          WHEN pad_svc_status_code = 'B'
               AND pad_magazine_code = 'CNS'
               AND lmag_magazine_code = 'CNS'
               AND apo.mag_cd = 'CNS'
               AND flg.cr_actv_flg = 'N'
               AND pad_purged_ind IS NULL
              THEN trunc(last_day(lmag_pub_date)) END AS cns_b2_pub_date
        , CASE
          WHEN ((pad_svc_status_code IN ('A', 'E', 'C') AND trunc(last_day(lmag_pub_date)) < trunc(sysdate))
                OR
                (pad_svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(pad_update_date)) > 6)
                OR
                (nvl(pad_svc_status_code, 'X') NOT IN ('A', 'E', 'C', 'G', 'B', 'D')))
               AND pad_magazine_code = 'CNS' AND lmag_magazine_code = 'CNS' AND apo.mag_cd = 'CNS' AND flg.cr_actv_flg = 'N' AND pad_purged_ind IS NULL
              THEN trunc(last_day(lmag_pub_date)) END AS cns_b3_pub_date

        /*CRH*/
        , CASE WHEN pad_magazine_code = 'CRH'
                    AND lmag_magazine_code = 'CRH'
                    AND apo.mag_cd = 'CRH'
                    AND flg.hl_actv_flg = 'Y'
                    AND pad_purged_ind IS NULL
        THEN last_day(lmag_pub_date) END              AS crh_b0_pub_date
        , CASE
          WHEN pad_svc_status_code = 'B'
               AND pad_magazine_code = 'CRH'
               AND lmag_magazine_code = 'CRH'
               AND apo.mag_cd = 'CRH'
               AND flg.hl_actv_flg = 'N'
               AND pad_purged_ind IS NULL
              THEN trunc(apo.canc_dt) END             AS crh_b1_canc_dt
        , CASE
          WHEN pad_svc_status_code = 'B'
               AND pad_magazine_code = 'CRH'
               AND lmag_magazine_code = 'CRH'
               AND apo.mag_cd = 'CRH'
               AND flg.hl_actv_flg = 'N'
               AND pad_purged_ind IS NULL
              THEN trunc(last_day(lmag_pub_date)) END AS crh_b2_pub_date
        , CASE
          WHEN ((pad_svc_status_code IN ('A', 'E', 'C') AND trunc(last_day(lmag_pub_date)) < trunc(sysdate))
                OR
                (pad_svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(pad_update_date)) > 6)
                OR
                (nvl(pad_svc_status_code, 'X') NOT IN ('A', 'E', 'C', 'G', 'B', 'D')))
               AND pad_magazine_code = 'CRH' AND lmag_magazine_code = 'CRH' AND apo.mag_cd = 'CRH' AND flg.hl_actv_flg = 'N' AND pad_purged_ind IS NULL
              THEN trunc(last_day(lmag_pub_date)) END AS crh_b3_pub_date

    FROM cte_actv_flg flg
        LEFT JOIN prod.agg_print_order apo
            ON flg.individual_id = apo.individual_id AND flg.hash_account_id = apo.hash_account_id

), cte_fin_calc AS (

    SELECT
        fdset.individual_id


        , CASE
          WHEN afs.fr_dnr_act_flg = 'Y'
              THEN 1
          WHEN afs.fr_dnr_lps_flg = 'Y'
              THEN 2
          WHEN afs.fr_dnr_inact_flg = 'Y'
              THEN 3
          ELSE 0 END                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                AS fr_code
        , afs.fr_mbr_gvng_lvl_cd                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    AS fr_mbr_gvng_lvl_cd
        , fdset.cr_actv_flg
        , fdset.hl_actv_flg
        , fdset.ma_actv_flg
        , fdset.shm_actv_flg
        --         , nvl(fdset.cns_b0_pub_date, coalesce( fdset.cns_b1_canc_dt, fdset.cns_b2_pub_date , fdset.cns_b3_pub_date)) AS cr_exp_dt


        , nvl(to_char(fdset.cns_b0_pub_date, 'YYYYMMDD') || nvl(substring(pad_keycode, 1, 4), '0000') || fdset.cr_actv_flg || ltrim(fdset.acct_num, 0)
            , greatest(nvl  (   to_char(fdset.cns_b1_canc_dt , 'YYYYMMDD') || nvl(substring(pad_keycode, 1, 4), '0000') || fdset.cr_actv_flg || ltrim(fdset.acct_num, 0)
                            ,   to_char(fdset.cns_b2_pub_date, 'YYYYMMDD') || nvl(substring(pad_keycode, 1, 4), '0000') || fdset.cr_actv_flg || ltrim(fdset.acct_num, 0)
                            )
                            ,   to_char(fdset.cns_b3_pub_date, 'YYYYMMDD') || nvl(substring(pad_keycode, 1, 4), '0000') || fdset.cr_actv_flg || ltrim(fdset.acct_num, 0))
          ) AS cr_exp_dt_conc

        --         , nvl(fdset.crh_b0_pub_date, coalesce( fdset.crh_b1_canc_dt, fdset.crh_b2_pub_date , fdset.crh_b3_pub_date)) AS hl_exp_dt
        , nvl(to_char(fdset.crh_b0_pub_date, 'YYYYMMDD') || nvl(substring(pad_keycode, 1, 4), '0000') || fdset.cr_actv_flg || ltrim(fdset.acct_num, 0)
            , greatest(nvl(to_char(fdset.crh_b1_canc_dt, 'YYYYMMDD') || nvl(substring(pad_keycode, 1, 4), '0000') || fdset.cr_actv_flg || ltrim(fdset.acct_num, 0), to_char(fdset.crh_b2_pub_date, 'YYYYMMDD') || nvl(substring(pad_keycode, 1, 4), '0000') || fdset.cr_actv_flg || ltrim(fdset.acct_num, 0)), to_char(fdset.crh_b3_pub_date, 'YYYYMMDD') || nvl(substring(pad_keycode, 1, 4), '0000') || fdset.cr_actv_flg || ltrim(fdset.acct_num, 0))) AS hl_exp_dt_conc
        , cast(NULL AS DATE)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AS ma_exp_dt
        , cast(NULL AS DATE)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AS shm_exp_dt
        --         , to_char(fdset.cns_b0_pub_date, 'YYYYMMDD') || pad_keycode                                                AS cns_datekeycode
        --         , to_char(fdset.crh_b0_pub_date, 'YYYYMMDD') || pad_keycode                                                AS crh_datekeycode
        , cast('N' AS VARCHAR(1))                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AS ma_auto_rnw_flg
        , cast('N' AS VARCHAR(1))                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AS shm_auto_rnw_flg



        , CASE
          WHEN afs.fr_dnr_act_flg = 'Y'
              THEN to_char(afs.fr_lst_don_dt, 'MM/DD/YYYY')
          ELSE NULL END                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             AS fr_lst_dont_dt
        , CASE
          WHEN afs.fr_dnr_act_flg = 'Y'
              THEN afs.fr_lst_don_amt
          ELSE NULL END                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             AS fr_lst_don_amt
        , CASE
          WHEN afs.fr_ch_status = 'ACTIVE'
              THEN 'Y'
          ELSE 'N' END                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              AS fr_sustainer

        --         , CASE
        --           WHEN fdset.acct_prefix = 'CNS'
        --                AND fdset.pad_magazine_code = 'CNS'
        --               THEN to_char(greatest(fdset.crh_b1_canc_dt, fdset.crh_b2_pub_date, fdset.crh_b3_pub_date), 'YYYYMMDD') || ltrim(fdset.acct_num, 0)
        --           ELSE NULL
        --           END                                                                                                         cns_cds_acct_num
        --         , fdset.crh_b1_canc_dt, fdset.crh_b1_canc_dt, fdset.crh_b2_pub_date, fdset.crh_b3_pub_date, ltrim(fdset.acct_num, 0)
        --         , CASE
        --           WHEN fdset.acct_prefix = 'CRH'
        --                AND fdset.pad_magazine_code = 'CRH'
        --               THEN to_char(greatest(fdset.crh_b1_canc_dt, fdset.crh_b2_pub_date, fdset.crh_b3_pub_date), 'YYYYMMDD') || ltrim(fdset.acct_num, 0)
        --           ELSE NULL
        --           END                                                                                                         crh_cds_acct_num



        , fdset.hash_account_id
    --         , fdset.acct_prefix
    --         , fdset.pad_magazine_code
    --         , fdset.pad_svc_status_code
    --         , fdset.pad_update_date
    --         , fdset.pad_pub_date
    --         , fdset.pad_keycode
    --         , fdset.lmag_magazine_code
    --         , fdset.lmag_pub_date

    FROM cte_exp_dt fdset
        LEFT JOIN prod.agg_fundraising_summary afs
            ON fdset.individual_id = afs.individual_id

), cte_fin_agg AS (

    SELECT
          calc.individual_id                                             AS individual_id
        , max(calc.fr_code)                                              AS fr_code
        , max(calc.fr_mbr_gvng_lvl_cd)                                   AS fr_mbr_gvng_lvl_cd
        , max(calc.cr_actv_flg)                                          AS cr_actv_flg
        , max(calc.hl_actv_flg)                                          AS hl_actv_flg
        , max(calc.ma_actv_flg)                                          AS ma_actv_flg
        , max(calc.shm_actv_flg)                                         AS shm_actv_flg
        , to_date(substring(max(calc.cr_exp_dt_conc), 1, 8), 'YYYYMMDD') AS cr_exp_dt
        , to_date(substring(max(calc.hl_exp_dt_conc), 1, 8), 'YYYYMMDD') AS hl_exp_dt
        , max(calc.ma_exp_dt)                                            AS ma_exp_dt
        , max(calc.shm_exp_dt)                                           AS shm_exp_dt
        , CASE
          WHEN substring(max(calc.cr_exp_dt_conc), 9, 5) = 'AUTO' || 'Y' /*SD Keycode of AUTO% and CR_ACTV_FLG = Y*/
              THEN 'Y'
          ELSE 'N' END                                                   AS cr_auto_rnw_flg
        , CASE
          WHEN substring(max(calc.hl_exp_dt_conc), 9, 5) = 'AUTO' || 'Y' /*SD Keycode of AUTO% and HL_ACTV_FLG = Y*/
              THEN 'Y'
          ELSE 'N' END                                                   AS hl_auto_rnw_flg
        , max(calc.ma_auto_rnw_flg)                                      AS ma_auto_rnw_flg
        , max(calc.shm_auto_rnw_flg)                                     AS shm_auto_rnw_flg
        , max(calc.fr_lst_dont_dt)                                       AS fr_lst_dont_dt
        , max(calc.fr_lst_don_amt)                                       AS fr_lst_don_amt
        , max(calc.fr_sustainer)                                         AS fr_sustainer
        , substring(max(calc.cr_exp_dt_conc), 14)                        AS cns_cds_acct_num
        , substring(max(calc.hl_exp_dt_conc), 14)                        AS crh_cds_acct_num

    FROM cte_fin_calc calc
    GROUP BY calc.individual_id

)
-- SELECT *
-- FROM cte_fin_agg
-- ;

SELECT


      ltrim(accnum.acct_num, 0)             AS acct_num

    , accnum.individual_id

    , fin.fr_code
    , fin.fr_mbr_gvng_lvl_cd
    , fin.cr_actv_flg
    , fin.hl_actv_flg
    , fin.ma_actv_flg
    , fin.shm_actv_flg
    , to_char(fin.cr_exp_dt, 'DD/MM/YYYY')  AS cr_exp_dt
    , to_char(fin.hl_exp_dt, 'DD/MM/YYYY')  AS hl_exp_dt
    , to_char(fin.ma_exp_dt, 'DD/MM/YYYY')  AS ma_exp_dt
    , to_char(fin.shm_exp_dt, 'DD/MM/YYYY') AS shm_exp_dt
    , fin.cr_auto_rnw_flg
    , fin.hl_auto_rnw_flg
    , fin.ma_auto_rnw_flg
    , fin.shm_auto_rnw_flg
    , fin.fr_lst_dont_dt
    , fin.fr_lst_don_amt
    , fin.fr_sustainer
    , fin.cns_cds_acct_num
    , fin.crh_cds_acct_num


FROM prod.agg_account_number accnum
    LEFT JOIN cte_fin_agg fin
        ON accnum.individual_id = fin.individual_id


WHERE 1 = 1
      AND accnum.acct_prefix = 'PWI'
      AND (1 = 2
           OR fin.cr_exp_dt > add_months(trunc(sysdate), -6)
           OR fin.hl_exp_dt > add_months(trunc(sysdate), -6)
           OR exists(SELECT NULL
                     FROM prod.agg_fundraising_summary ex0
                     WHERE ex0.individual_id = accnum.individual_id AND ex0.fr_dnr_act_flg = 'Y'))

ORDER BY ltrim(accnum.acct_num, 0) ASC
;


;

SELECT
    aan.individual_id
    , CASE WHEN pad.magazine_code = 'CNS'
                AND lmag.magazine_code = 'CNS'
                AND apo.mag_cd = 'CNS'
                AND pad.purged_ind IS NULL
    --                     AND flg.cr_actv_flg = 'Y'
    THEN last_day(lmag.pub_date) END              AS cns_b0_pub_date
    , CASE
      WHEN pad.svc_status_code = 'B'
           AND pad.magazine_code = 'CNS'
           AND lmag.magazine_code = 'CNS'
           AND apo.mag_cd = 'CNS'
           AND pad.purged_ind IS NULL
          --                AND flg.cr_actv_flg = 'N'
          THEN trunc(apo.canc_dt) END             AS cns_b1_canc_dt
    , CASE
      WHEN pad.svc_status_code = 'B'
           AND pad.magazine_code = 'CNS'
           AND lmag.magazine_code = 'CNS'
           AND apo.mag_cd = 'CNS'
           AND pad.purged_ind IS NULL
          --                AND flg.cr_actv_flg = 'N'
          THEN trunc(last_day(lmag.pub_date)) END AS cns_b2_pub_date
    , CASE
      WHEN ((pad.svc_status_code IN ('A', 'E', 'C') AND trunc(last_day(lmag.pub_date)) < trunc(sysdate))
            OR
            (pad.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(pad.update_date)) > 6)
            OR
            (nvl(pad.svc_status_code, 'X') NOT IN ('A', 'E', 'C', 'G', 'B', 'D')))
           AND pad.magazine_code = 'CNS' AND lmag.magazine_code = 'CNS' AND apo.mag_cd = 'CNS' AND pad.purged_ind IS NULL
          --                AND flg.cr_actv_flg = 'N'
          THEN trunc(last_day(lmag.pub_date)) END AS cns_b3_pub_date

FROM prod.agg_account_number aan
    LEFT JOIN prod.print_account_detail pad
        ON aan.hash_account_id = pad.hash_account_id
    LEFT JOIN etl_proc.lookup_magazine lmag
        ON pad.iss_num = lmag.iss_num
           AND pad.magazine_code = lmag.magazine_code
    LEFT JOIN prod.agg_print_order apo
        ON aan.individual_id = apo.individual_id AND aan.hash_account_id = apo.hash_account_id

WHERE aan.individual_id = 1217707268

;

, CASE
WHEN pad.magazine_code = 'CNS'
AND lmag.magazine_code = 'CNS'
AND pad.purged_ind IS NULL
AND pad.svc_status_code IN ('A', 'E', 'C')
AND last_day(lmag.pub_date) >= trunc(sysdate)
THEN 'Y'
WHEN pad.magazine_code = 'CNS'
AND lmag.magazine_code = 'CNS'
AND pad.purged_ind IS NULL
AND pad.svc_status_code = 'G'
AND months_between(trunc(sysdate), trunc(pad.update_date)) <= 6
THEN 'Y'
ELSE 'N'
END AS cr_actv_flg
;

SELECT
    /*CR_ACTV_FLG Comparison*/
    pad.svc_status_code /*IN ('A', 'E', 'C')*/
    , last_day(lmag.pub_date)
    , trunc(sysdate)

    , pad.svc_status_code /*= 'G'*/
    , months_between(trunc(sysdate), trunc(pad.update_date))

    /*CR_EXP_DT Comparison*/
    , last_day(lmag.pub_date)
    , apo.canc_dt

FROM prod.agg_account_number aan
    LEFT JOIN prod.print_account_detail pad
        ON aan.hash_account_id = pad.hash_account_id
    LEFT JOIN etl_proc.lookup_magazine lmag
        ON pad.expr_iss_num = lmag.iss_num
           AND pad.magazine_code = lmag.magazine_code
    LEFT JOIN prod.agg_print_order apo
        ON aan.individual_id = apo.individual_id AND aan.hash_account_id = apo.hash_account_id
WHERE aan.individual_id = 1217707268
      AND pad.magazine_code = 'CNS'
      AND lmag.magazine_code = 'CNS'
      AND aan.acct_prefix = 'CNS'
      AND pad.purged_ind IS NULL
LIMIT 100
;

;


;

SELECT ltrim(accnum.acct_num, 0) AS acct_num
FROM prod.agg_account_number accnum
    LEFT JOIN cte_fin_agg fin
        ON accnum.individual_id = fin.individual_id
WHERE accnum.acct_prefix = 'PWI'
LIMIT 1000
;


SELECT individual_id
FROM prod.agg_account_number aan
WHERE aan.acct_prefix = 'PWI'
      AND individual_id IN (1200639057, 1201054624, 1200437304, 1200920954, 1201077789, 1200579537, 1200645937, 1221691437, 1218386299, 1219463968, 1221837448, 1217460734, 1218586949, 1221615020, 1202681074, 1221661583, 1215919896, 1212706165, 1218842195, 1206108121, 1216672363, 1215667357, 1216625740, 1216680098, 1216682758, 1216638721, 1230280970, 1215831391, 1217026615, 1217026615, 1201050189, 1202390891, 1213955005, 1235098827, 1216752189, 1230280970, 1215831391, 1217026615, 1217026615, 1201050189, 1202390891, 1213955005, 1235098827, 1216752189, 1200031512, 1200020849, 1200050334, 1200006296, 1200023745, 1200031527, 1200044068, 1200059785, 1200089656, 1200059897, 1200058644, 1200061389, 1200228798, 1200245878, 1200126542, 1200220314, 1200151345, 1200042058, 1200059904, 1200024695, 1200206423, 1200091438, 1200227181, 1200154842, 1200195171, 1200208711, 1200239219, 1200110846, 1200224068, 1200218838, 1200087661, 1200222730, 1200225975, 1200178479, 1200141778, 1200142767, 1200065378, 1200150677, 1200089445, 1200176370, 1200060209, 1210130764, 1211609315, 1213107369, 1215359682, 1218324267)


LIMIT 100
;

;

SELECT *
FROM prod.print_account_detail
LIMIT 100
;

;

WITH tt1 (q, w, e, z) AS (
    SELECT
        1
        , 2
        , 3
        , 'a'
    UNION ALL SELECT
                  2
                  , 1
                  , 4
                  , 'c'
    UNION ALL SELECT
                  3
                  , 5
                  , 1
                  , 'b'

)

SELECT greatest(q, w, e) || z
FROM tt1
;

WITH cte_print_summ_ind AS (
    SELECT
        ps.individual_id
        , cr_actv_flg
        , hl_actv_flg
        , ma_actv_flg
        , shm_actv_flg
        , cr_exp_dt
        , hl_exp_dt
        , ma_exp_dt
        , shm_exp_dt
        , cr_auto_rnw_flg
        , hl_auto_rnw_flg
        , ma_auto_rnw_flg
        , shm_auto_rnw_flg
        , CASE WHEN trunc(ps.cr_exp_dt) > dateadd(DAY, -180, trunc(sysdate))
        THEN 1
          ELSE NULL END cr_exp_dt_fl
        , CASE WHEN trunc(ps.hl_exp_dt) > dateadd(DAY, -180, trunc(sysdate))
        THEN 1
          ELSE NULL END hl_exp_dt_fl
        , CASE WHEN trunc(ps.ma_exp_dt) > dateadd(DAY, -180, trunc(sysdate))
        THEN 1
          ELSE NULL END ma_exp_dt_fl
        , CASE WHEN trunc(ps.shm_exp_dt) > dateadd(DAY, -180, trunc(sysdate))
        THEN 1
          ELSE NULL END shm_exp_dt_fl
    FROM prod.agg_print_summary ps

    WHERE 1 = 1
          /*#TODO RMV*/ AND ps.individual_id IN (1216672363, 1215667357, 1216625740, 1216680098, 1216682758, 1216638721, 1230280970, 1215831391, 1217026615, 1217026615, 1201050189, 1202390891, 1213955005, 1235098827, 1216752189, 1230280970, 1215831391, 1217026615, 1217026615, 1201050189, 1202390891, 1213955005, 1235098827, 1216752189, 1200031512, 1200020849, 1200050334, 1200006296, 1200023745, 1200031527, 1200044068, 1200059785, 1200089656, 1200059897, 1200058644, 1200061389, 1200228798, 1200245878, 1200126542, 1200220314, 1200151345, 1200042058, 1200059904, 1200024695, 1200206423, 1200091438, 1200227181, 1200154842, 1200195171, 1200208711, 1200239219, 1200110846, 1200224068, 1200218838, 1200087661, 1200222730, 1200225975, 1200178479, 1200141778, 1200142767, 1200065378, 1200150677, 1200089445, 1200176370, 1200060209, 1210130764, 1211609315, 1213107369, 1215359682, 1218324267)

), cte_print_sum_valid_ind AS (

    SELECT
        individual_id
        , cr_actv_flg
        , hl_actv_flg
        , ma_actv_flg
        , shm_actv_flg
        , cr_exp_dt
        , hl_exp_dt
        , ma_exp_dt
        , shm_exp_dt
        , cr_auto_rnw_flg
        , hl_auto_rnw_flg
        , ma_auto_rnw_flg
        , shm_auto_rnw_flg
    FROM cte_print_summ_ind
    WHERE coalesce(cr_exp_dt_fl, hl_exp_dt_fl, ma_exp_dt_fl, shm_exp_dt_fl) = 1

), cte_fr_summ_ind AS (

    SELECT
        fs.individual_id
        , fs.fr_dnr_act_flg
        , fs.fr_dnr_lps_flg
        , fs.fr_dnr_inact_flg
        , fs.fr_mbr_gvng_lvl_cd
        , fs.fr_lst_don_dt
        , fs.fr_lst_don_amt
    FROM prod.agg_fundraising_summary fs
    WHERE 1 = 1
          /*#TODO RMV*/ AND fs.individual_id IN (1216672363, 1215667357, 1216625740, 1216680098, 1216682758, 1216638721, 1230280970, 1215831391, 1217026615, 1217026615, 1201050189, 1202390891, 1213955005, 1235098827, 1216752189, 1230280970, 1215831391, 1217026615, 1217026615, 1201050189, 1202390891, 1213955005, 1235098827, 1216752189, 1200031512, 1200020849, 1200050334, 1200006296, 1200023745, 1200031527, 1200044068, 1200059785, 1200089656, 1200059897, 1200058644, 1200061389, 1200228798, 1200245878, 1200126542, 1200220314, 1200151345, 1200042058, 1200059904, 1200024695, 1200206423, 1200091438, 1200227181, 1200154842, 1200195171, 1200208711, 1200239219, 1200110846, 1200224068, 1200218838, 1200087661, 1200222730, 1200225975, 1200178479, 1200141778, 1200142767, 1200065378, 1200150677, 1200089445, 1200176370, 1200060209, 1200021147, 1200021228, 1200021351, 1200144292, 1200144532, 1200144556, 1200147837)
          AND fr_dnr_act_flg = 'Y'


), cte_non_web_valid_ind AS (

    SELECT
        nvl(ps.individual_id, fs.individual_id) AS individual_id

        /*Print Summary*/
        , ps.cr_actv_flg
        , ps.hl_actv_flg
        , ps.ma_actv_flg
        , ps.shm_actv_flg
        , ps.cr_exp_dt
        , ps.hl_exp_dt
        , ps.ma_exp_dt
        , ps.shm_exp_dt
        , ps.cr_auto_rnw_flg
        , ps.hl_auto_rnw_flg
        , ps.ma_auto_rnw_flg
        , ps.shm_auto_rnw_flg

        /*Fundraising*/
        , fs.fr_dnr_act_flg
        , fs.fr_dnr_lps_flg
        , fs.fr_dnr_inact_flg
        , fs.fr_mbr_gvng_lvl_cd
        , fs.fr_lst_don_dt
        , fs.fr_lst_don_amt

    FROM cte_print_sum_valid_ind ps
        FULL JOIN cte_fr_summ_ind fs
            ON ps.individual_id = fs.individual_id

), cte_precalc AS (

    SELECT
          ltrim(an.acct_num, 0)             AS acct_num
        , CASE
          WHEN wvi.fr_dnr_act_flg = 'Y'
              THEN 1
          WHEN wvi.fr_dnr_lps_flg = 'Y'
              THEN 2
          WHEN wvi.fr_dnr_inact_flg = 'Y'
              THEN 3
          ELSE 0 END                        AS fr_code
        , nvl(wvi.fr_mbr_gvng_lvl_cd, NULL) AS fr_mbr_gvng_lvl_cd
        , CASE
          WHEN pad.magazine_code = 'CNS'
               AND lmag.magazine_code = 'CNS'
               AND pad.purged_ind IS NULL
               AND pad.svc_status_code IN ('A', 'E', 'C')
               AND last_day(lmag.pub_date) >= trunc(sysdate)
              THEN 'Y'
          WHEN pad.magazine_code = 'CNS'
               AND lmag.magazine_code = 'CNS'
               AND pad.purged_ind IS NULL
               AND pad.svc_status_code = 'G'
               AND months_between(trunc(sysdate), trunc(pad.update_date)) <= 6
              THEN 'Y'
          ELSE 'N'
          END                               AS cr_actv_flg
        , CASE
          WHEN pad.magazine_code = 'CRH'
               AND lmag.magazine_code = 'CRH'
               AND pad.purged_ind IS NULL
               AND pad.svc_status_code IN ('A', 'E', 'C')
               AND last_day(lmag.pub_date) >= trunc(sysdate)
              THEN 'Y'
          WHEN pad.magazine_code = 'CRH'
               AND lmag.magazine_code = 'CRH'
               AND pad.purged_ind IS NULL
               AND pad.svc_status_code = 'G'
               AND months_between(trunc(sysdate), trunc(pad.update_date)) <= 6
              THEN 'Y'
          ELSE 'N'
          END                               AS hl_actv_flg
        , 'N'                               AS ma_actv_flg
        , 'N'                               AS shm_actv_flg
        --     , nvl(to_char(wvi.cr_exp_dt, 'MM/DD/YYYY'), NULL)  AS cr_exp_dt
        --     , nvl(to_char(wvi.hl_exp_dt, 'MM/DD/YYYY'), NULL)  AS hl_exp_dt
        --     , nvl(to_char(wvi.ma_exp_dt, 'MM/DD/YYYY'), NULL)  AS ma_exp_dt
        --     , nvl(to_char(wvi.shm_exp_dt, 'MM/DD/YYYY'), NULL) AS shm_exp_dt
        --     , nvl(wvi.cr_auto_rnw_flg, 'N')                    AS cr_auto_rnw_flg
        --     , nvl(wvi.hl_auto_rnw_flg, 'N')                    AS hl_auto_rnw_flg
        --     , nvl(wvi.ma_auto_rnw_flg, 'N')                    AS ma_auto_rnw_flg
        --     , nvl(wvi.shm_auto_rnw_flg, 'N')                   AS shm_auto_rnw_flg
        , CASE
          WHEN fr_dnr_act_flg = 'Y'
              THEN to_char(wvi.fr_lst_don_dt, 'MM/DD/YYYY')
          ELSE NULL END                     AS fr_lst_don_dt
        , CASE
          WHEN fr_dnr_act_flg = 'Y'
              THEN wvi.fr_lst_don_amt
          ELSE NULL END                     AS fr_lst_don_amt
        , CAST('N' AS VARCHAR(1))           AS fr_sustainer

        , an.hash_account_id                AS an_hash_account_id
        , an.acct_prefix                    AS an_acct_prefix
        , pad.magazine_code                 AS pad_magazine_code
        , pad.expr_iss_num                  AS pad_expr_iss_num
        , pad.purged_ind                    AS pad_purged_ind
        , pad.svc_status_code               AS pad_svc_status_code
        , pad.hash_account_id               AS pad_hash_account_id
        , lmag.magazine_code                AS lmag_magazine_code
        , lmag.pub_date                     AS lmag_pub_date


    /*, an.individual_id*/
    FROM prod.agg_account_number an
        INNER JOIN cte_non_web_valid_ind wvi
            ON wvi.individual_id = an.individual_id
        LEFT JOIN prod.print_account_detail pad
            ON an.hash_account_id = pad.hash_account_id
               AND pad.purged_ind IS NULL
        LEFT JOIN etl_proc.lookup_magazine lmag
            ON pad.iss_num = lmag.iss_num
    WHERE 1 = 1
    --           AND an.acct_prefix = 'PWI'


)

SELECT
    acct_num
    , fr_code
    , fr_mbr_gvng_lvl_cd
    , cr_actv_flg
    , hl_actv_flg
    , ma_actv_flg
    , shm_actv_flg
    , an_hash_account_id
    , an_acct_prefix
    , pad_magazine_code
    , pad_expr_iss_num
    , pad_purged_ind
    , pad_svc_status_code
    , pad_hash_account_id
    , lmag_magazine_code
    , lmag_pub_date


FROM cte_precalc precalc


-- where exists (select null from prod.print_account_detail pad where pad.hash_account_id = precalc.pad_hash_account_id)

-- ORDER BY cast(precalc.acct_num AS BIGINT) ASC

LIMIT 100
;


;

WITH cte_exp_date_calc AS (

    SELECT
        apo.individual_id
        , CASE WHEN pad.magazine_code = 'CNS' AND lmag.magazine_code = 'CNS' AND apo.mag_cd = 'CNS'
        THEN last_day(lmag.pub_date) END              AS cns_b0_pub_date
        , CASE
          WHEN pad.svc_status_code = 'B' AND pad.magazine_code = 'CNS' AND lmag.magazine_code = 'CNS' AND apo.mag_cd = 'CNS'
              THEN trunc(apo.canc_dt) END             AS cns_b1_canc_dt
        , CASE
          WHEN pad.svc_status_code = 'B' AND pad.magazine_code = 'CNS' AND lmag.magazine_code = 'CNS' AND apo.mag_cd = 'CNS'
              THEN trunc(last_day(lmag.pub_date)) END AS cns_b2_pub_date
        , CASE
          WHEN ((pad.svc_status_code IN ('A', 'E', 'C') AND trunc(last_day(lmag.pub_date)) < trunc(sysdate))
                OR
                (pad.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(pad.update_date)) > 6)
                OR
                (nvl(pad.svc_status_code, 'X') NOT IN ('A', 'E', 'C', 'G', 'B', 'D')))
               AND pad.magazine_code = 'CNS' AND lmag.magazine_code = 'CNS' AND apo.mag_cd = 'CNS'
              THEN trunc(last_day(lmag.pub_date)) END AS cns_b3_pub_date
        , CASE WHEN pad.magazine_code = 'CRH' AND lmag.magazine_code = 'CRH' AND apo.mag_cd = 'CRH'
        THEN last_day(lmag.pub_date) END              AS crh_b0_pub_date
        , CASE
          WHEN pad.svc_status_code = 'B' AND pad.magazine_code = 'CRH' AND lmag.magazine_code = 'CRH' AND apo.mag_cd = 'CRH'
              THEN trunc(apo.canc_dt) END             AS crh_b1_canc_dt
        , CASE
          WHEN pad.svc_status_code = 'B' AND pad.magazine_code = 'CRH' AND lmag.magazine_code = 'CRH' AND apo.mag_cd = 'CRH'
              THEN trunc(last_day(lmag.pub_date)) END AS crh_b2_pub_date
        , CASE
          WHEN ((pad.svc_status_code IN ('A', 'E', 'C') AND trunc(last_day(lmag.pub_date)) < trunc(sysdate))
                OR
                (pad.svc_status_code = 'G' AND months_between(trunc(sysdate), trunc(pad.update_date)) > 6)
                OR
                (nvl(pad.svc_status_code, 'X') NOT IN ('A', 'E', 'C', 'G', 'B', 'D')))
               AND pad.magazine_code = 'CRH' AND lmag.magazine_code = 'CRH' AND apo.mag_cd = 'CRH'
              THEN trunc(last_day(lmag.pub_date)) END AS crh_b3_pub_date

    FROM prod.print_account_detail pad
        INNER JOIN etl_proc.lookup_magazine lmag
            ON pad.magazine_code = lmag.magazine_code
               AND pad.iss_num = lmag.iss_num
        INNER JOIN prod.agg_print_order apo
            ON pad.hash_account_id = apo.hash_account_id



    WHERE 1 = 1
          AND pad.purged_ind IS NULL

          AND individual_id IN (1216672363, 1215667357, 1216625740, 1216680098, 1216682758, 1216638721, 1230280970, 1215831391, 1217026615, 1217026615, 1201050189, 1202390891, 1213955005, 1235098827, 1216752189)
)


SELECT DISTINCT
    individual_id
    , (cns_b0_pub_date)                                          AS active_cr_exp_dt
    , greatest(cns_b1_canc_dt, cns_b2_pub_date, cns_b3_pub_date) AS inactive_cr_exp_dt
    , (crh_b0_pub_date)                                          AS active_hl_exp_dt
    , greatest(crh_b1_canc_dt, crh_b2_pub_date, crh_b3_pub_date) AS inactive_hl_exp_dt
FROM cte_exp_date_calc


LIMIT 100
;

SELECT
    '20181231AUTOY10969921'
    , substring('20181231AUTOY10969921', 1, 8)
    , substring('20181231AUTOY10969921', 9, 4)
    , substring('20181231AUTOY10969921', 13, 1)
    , substring('20181231AUTOY10969921', 14)

;

WITH tt1 (q, w) AS (
    SELECT
        1
        , 1
    UNION ALL SELECT
                  1
                  , 2
    UNION ALL SELECT
                  2
                  , 3)

SELECT
    q
    , greatest(w)
FROM tt1
GROUP BY q
;

SELECT *
FROM etl_proc.lookup_magazine
LIMIT 100
;


SELECT count(*)
FROM prod.preference_history ph
WHERE id_type = 'E'
      AND NOT exists(SELECT NULL
                     FROM prod.individual_email ie
                     WHERE ie.email_address = ph.id_value)
LIMIT 100
;

SELECT *
FROM prod.preference_history ph
WHERE id_type = 'E'
      AND NOT exists(SELECT NULL
                     FROM prod.individual_email ie
                     WHERE ie.email_address = ph.id_value)
LIMIT 100
;

SELECT *
FROM cr_temp.mt_individual
LIMIT 100
;


SELECT
    hash_account_id
    , individual_id
FROM prod.agg_account_number an
WHERE acct_prefix = 'PWI'
      AND exists(SELECT NULL
                 FROM prod.agg_print_summary pad
                 WHERE pad.individual_id = an.individual_id)
LIMIT 100
;