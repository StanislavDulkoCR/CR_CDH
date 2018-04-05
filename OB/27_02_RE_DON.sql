DROP TABLE IF EXISTS 	agg.${custom_var1};
CREATE TABLE agg.${custom_var1}
    DISTSTYLE KEY
    DISTKEY (cscid_indiv)
    SORTKEY (cscid_indiv)
    AS WITH cte_re_don_fin AS (
        SELECT

              ah.individual_id                      AS cscid_indiv
            , cast(' ' AS VARCHAR(1))               AS cntrlnum
            , to_char(trunc(ah.action_date), 'MM/DD/YYYY') AS rectdate
            , CASE
              WHEN fnc_code = 'REF'
                  THEN (abs(ah.action_amount) * (-1))
              ELSE ah.action_amount
              END                                   AS rectamnt
            , ah.keycode                            AS applcod
            , ah.order_entry_code                   AS frmofpay
            , cast(NULL AS VARCHAR(9))              AS csc_ind_urn

        FROM prod.action_header ah
            INNER JOIN prod.fundraising_transaction funt
                ON ah.hash_action_id = funt.hash_action_id

        WHERE 1 = 0
              OR (1 = 1
                  AND fnc_code IN ('ANH', 'AHS', 'REF', 'SCN')
                  AND exists(SELECT NULL
                             FROM prod.re_individuals ex0
                             WHERE 1 = 1
                                   AND ah.individual_id = ex0.individual_id
                                   AND ex0.don_last_extract_date IS NOT NULL
                                   AND ah.update_date > ex0.don_last_extract_date))

              OR (1 = 1
                  AND fnc_code IN ('ANH', 'AHS', 'REF', 'SCN')
                  AND NOT exists(SELECT NULL
                                 FROM prod.re_individuals ex1
                                 WHERE 1 = 1
                                       AND ah.individual_id = ex1.individual_id
                                       AND ex1.don_last_extract_date IS NOT NULL)
                  AND (1 = 0
                       OR exists(SELECT NULL
                                 FROM prod.agg_individual ex2
                                 WHERE ah.individual_id = ex2.individual_id AND ex2.cga_flg = 'Y')
                       OR exists(SELECT NULL
                                 FROM prod.agg_fundraising_donation ex3
                                 WHERE ah.individual_id = ex3.individual_id
                                       AND ex3.don_amt >= 100
                                       AND ex3.don_dt >= cast(to_date('20040601', 'YYYYMMDD') AS TIMESTAMP))
                       OR exists(SELECT NULL
                                 FROM prod.agg_fundraising_summary ex3
                                 WHERE ah.individual_id = ex3.individual_id
                                       AND ex3.fr_ltd_don_amt >= 5000)))

    )

    SELECT
        cscid_indiv
        , cntrlnum
        , rectdate
        , rectamnt
        , applcod
        , frmofpay
        , csc_ind_urn
    FROM cte_re_don_fin fin

    WHERE 1 = 1
          AND NOT exists(SELECT NULL
                         FROM prod.rm_icd_lookup excl1 /*Exclusions*/
                         WHERE fin.cscid_indiv = excl1.acx_rm_new_individual_id
                               AND source_indiv_id IN (1000200101269706, 10002001012697060, 10002000131940010, 10002000611371060, 10002000136474010))
    ORDER BY applcod ASC
;


/*SD Note: Inserting new individuals which were never sent previously into RE_INDIVIDUALS*/
INSERT INTO prod.re_individuals
SELECT distinct cscid_indiv as individual_id
FROM agg.${custom_var1} temp
where not exists (select null from prod.re_individuals rei where temp.cscid_indiv = rei.individual_id)
;
/*SD Note: Update RE_EMA-specific extract date in RE_INDIVIDUALS*/
UPDATE prod.re_individuals
SET don_last_extract_date = cast('${custom_var2}' AS TIMESTAMP)
FROM agg.${custom_var1} temp
    INNER JOIN prod.re_individuals rei
        ON rei.individual_id = temp.cscid_indiv
;