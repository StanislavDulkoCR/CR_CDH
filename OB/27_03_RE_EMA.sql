DROP TABLE IF EXISTS 	agg.${custom_var1};
CREATE TABLE 			agg.${custom_var1}
    DISTSTYLE KEY
    DISTKEY (csc_ind_id)
    SORTKEY (csc_ind_id)
    AS WITH cte_email_resp AS (

    SELECT
        individual_id
        , email
        , substring(max(to_char(update_date, 'YYYYMMDDHH24MISS') || to_char(create_date, 'YYYYMMDDHH24MISS')||catalog_type), 29, 1) AS catalog_type_max
    FROM prod.email_response
    GROUP BY individual_id, email

), cte_re_ema_fin AS (
    SELECT
          cast(NULL AS SMALLINT)                                         AS csc_email_id
        , aem.individual_id                                              AS csc_ind_id
        , aem.email_address                                              AS email_address
        , ' '                                                            AS email_format_pref
        , ' '                                                            AS email_undeliverable_count
        , decode(catalog_type_max, '', 'U', NULL, 'U', catalog_type_max) AS email_html_capability
        , ' '                                                            AS email_unsubscribe
        , ' '                                                            AS email_primary_flag
        , CASE
          WHEN aps.non_prom_em_flg = 'Y'
              THEN 'N'
          WHEN aps.fr_non_prom_em_flg = 'Y'
              THEN 'N'
          ELSE NULL
          END                                                            AS email_authoriz_flag
        , cast(NULL AS SMALLINT)                                         AS csc_ind_urn
        , aem.email_type_cd

    FROM prod.agg_email aem
        LEFT JOIN prod.re_individuals rei
            ON aem.individual_id = rei.individual_id
        LEFT JOIN cte_email_resp resp
            ON aem.individual_id = resp.individual_id
               AND aem.email_address = resp.email
        LEFT JOIN prod.agg_preference_summary aps
            ON aem.individual_id = aps.individual_id

    WHERE 1 = 0

          OR (1 = 1
              AND aem.email_type_cd = 'I'
              AND rei.ema_last_extract_date is not null /*SD Note: IS in RE_INDIVIDUALS and EMA was sent*/
              AND (1 = 0

                   OR (aem.eff_dt > rei.ema_last_extract_date)
                   OR (aem.fst_dt > rei.ema_last_extract_date)
                   OR EXISTS(SELECT NULL
                             FROM prod.individual_email ex0
                             WHERE ex0.individual_id = aem.individual_id
                                   AND ex0.update_date > rei.ema_last_extract_date)
                   OR EXISTS(SELECT NULL
                             FROM prod.preference_history ex1
                             WHERE ex1.id_value = aem.email_address
                                   AND ex1.id_type = 'E'
                                   AND ex1.preference_code = 'EMAIL'
                                   AND ex1.preference_scope IN ('FUN', 'FR', 'GLB')
                                   AND ex1.update_date > rei.ema_last_extract_date)
                   OR EXISTS(SELECT NULL
                             FROM prod.email_response ex3
                             WHERE ex3.individual_id = aem.individual_id
                                   AND ex3.email = aem.email_address
                                   AND ex3.create_date > rei.ema_last_extract_date)
                   OR EXISTS(SELECT NULL
                             FROM prod.id_xref ex4
                             WHERE aem.individual_id = ex4.former_id
                                   AND ex4.insert_date > rei.ema_last_extract_date)))

          OR (1 = 1
          	  AND aem.email_type_cd = 'I'
              AND rei.ema_last_extract_date is null /*SD Note: NOT in RE_INDIVIDUALS or EMA was NOT sent*/
              AND (1 = 0
                   OR exists(SELECT NULL
                             FROM prod.agg_individual ex1
                             WHERE aem.individual_id = ex1.individual_id
                                   AND ex1.cga_flg = 'Y')
                   OR exists(SELECT NULL
                             FROM prod.agg_fundraising_donation ex2
                             WHERE aem.individual_id = ex2.individual_id
                                   AND ex2.don_amt >= 100
                                   AND trunc(ex2.don_dt) >= cast(to_date('20040601', 'YYYYMMDD') AS TIMESTAMP))
                   OR exists(SELECT NULL
                             FROM prod.agg_fundraising_summary ex3
                             WHERE aem.individual_id = ex3.individual_id
                                   AND ex3.fr_ltd_don_amt >= 5000)

                   ))
)

SELECT
    csc_email_id
    , csc_ind_id
    , fin.email_address
    , email_format_pref
    , email_undeliverable_count
    , email_html_capability
    , email_unsubscribe
    , email_primary_flag
    , email_authoriz_flag
    , csc_ind_urn


FROM cte_re_ema_fin fin

WHERE 1 = 1
      AND fin.email_type_cd = 'I'
      AND NOT exists(SELECT NULL
                     FROM prod.rm_icd_lookup excl1 /*Exclusions*/
                     WHERE fin.csc_ind_id = excl1.acx_rm_new_individual_id
                           AND source_indiv_id IN (1000200101269706, 10002001012697060, 10002000131940010, 10002000611371060, 10002000136474010))
      AND fin.email_address <> 'nobody@nowhere.com'
ORDER BY cast(csc_ind_id AS BIGINT) ASC
;

/*SD Note: Inserting new individuals which were never sent previously into RE_INDIVIDUALS*/
INSERT INTO prod.re_individuals
SELECT distinct csc_ind_id as individual_id
FROM agg.${custom_var1} temp
where not exists (select null from prod.re_individuals rei where temp.csc_ind_id = rei.individual_id)
;
/*SD Note: Update RE_EMA-specific extract date in RE_INDIVIDUALS*/
UPDATE prod.re_individuals
SET ema_last_extract_date = cast('${custom_var2}' AS TIMESTAMP)
FROM agg.${custom_var1} temp
    INNER JOIN prod.re_individuals rei
        ON rei.individual_id = temp.csc_ind_id
;