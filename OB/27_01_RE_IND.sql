DROP TABLE IF EXISTS 	agg.${custom_var1};

CREATE TABLE agg.${custom_var1}
    DISTSTYLE KEY
    DISTKEY (csc_ind_id)
    SORTKEY (csc_ind_id)
    AS WITH cte_re_ind_upd AS (/*existing RE individuals*/
        SELECT ana.individual_id
        FROM prod.agg_name_address ana
            INNER JOIN prod.re_individuals rei
                ON ana.individual_id = rei.individual_id

        WHERE rei.ind_last_extract_date IS NOT NULL
              AND (1 = 0
                   OR exists(SELECT NULL
                             FROM prod.individual_address ex1
                             WHERE ana.individual_id = ex1.individual_id
                                   AND ex1.update_date > rei.ind_last_extract_date)
                   OR exists(SELECT NULL
                             FROM prod.preference_history ex2
                             WHERE ana.individual_id = ex2.id_value
                                   AND ex2.id_type = 'I'
                                   AND ex2.update_date > rei.ind_last_extract_date)
                   OR exists(SELECT NULL
                             FROM prod.individual_phone ex3
                             WHERE ana.individual_id = ex3.individual_id
                                   AND ex3.update_date > rei.ind_last_extract_date)
                   OR exists(SELECT NULL
                             FROM prod.id_xref ex4
                             WHERE ana.individual_id = ex4.former_id
                                   AND ex4.insert_date > rei.ind_last_extract_date))
    ), cte_re_ind_new AS (  /*new RE individuals*/
        SELECT ana.individual_id
        FROM prod.agg_name_address ana

        WHERE 1 = 1
              AND NOT exists(SELECT NULL
                             FROM prod.re_individuals rei
                             WHERE ana.individual_id = rei.individual_id
                                   AND rei.ind_last_extract_date IS NOT NULL)
              AND (1 = 0
                   OR exists(SELECT NULL
                             FROM prod.agg_individual ex1
                             WHERE ana.individual_id = ex1.individual_id
                                   AND ex1.cga_flg = 'Y')
                   OR exists(SELECT NULL
                             FROM prod.agg_fundraising_donation ex2
                             WHERE ana.individual_id = ex2.individual_id
                                   AND ex2.don_amt >= 100
                                   AND trunc(ex2.don_dt) >= cast(to_date('20040601', 'YYYYMMDD') AS TIMESTAMP))
                   OR exists(SELECT NULL
                             FROM prod.agg_fundraising_summary ex3
                             WHERE ana.individual_id = ex3.individual_id
                                   AND ex3.fr_ltd_don_amt >= 5000)
                   )
    ), cte_fin_calc AS (
        SELECT
              ana.individual_id                                                        AS csc_ind_id
            , TO_CHAR(cast('${custom_var2}' AS TIMESTAMP), 'MM/DD/YYYY')               AS date_modified
            , aps.cu_board_flg                                                         AS cic_cubord
            , TO_CHAR(agi.ind_fst_rel_dt, 'MM/DD/YYYY')                                AS cid_recdate
            ,    decode(ana.primary_number  , null, '', ana.primary_number  ||' ')
               ||decode(ana.pre_directional , null, '', ana.pre_directional ||' ')
               ||decode(ana.street          , null, '', ana.street          ||' ')
               ||decode(ana.street_suffix   , null, '', ana.street_suffix   ||' ')
               ||decode(ana.post_directional, null, '', ana.post_directional||' ')
               ||decode(ana.unit_designator , null, '', ana.unit_designator ||' ')
               ||decode(ana.secondary_number, null, '', ana.secondary_number)          AS civ_addr1
            , ana.city                                                                 AS civ_city
            , ana.state_province                                                       AS civ_state
            , ana.business_name                                                        AS civ_cmpnme
            , CASE
              WHEN afn.first_name IS NOT NULL AND afn.last_name IS NOT NULL
                  THEN afn.salutation
              WHEN ana.first_name IS NOT NULL AND ana.last_name IS NOT NULL
                  THEN ana.salutation
              ELSE afn.salutation
              END                                                                      AS civ_title1
            , CASE
              WHEN afn.first_name IS NOT NULL AND afn.last_name IS NOT NULL
                  THEN afn.first_name
              WHEN ana.first_name IS NOT NULL AND ana.last_name IS NOT NULL
                  THEN ana.first_name
              ELSE afn.first_name
              END                                                                      AS civ_fname
            , CASE
              WHEN afn.first_name IS NOT NULL AND afn.last_name IS NOT NULL
                  THEN afn.middle_name
              WHEN ana.first_name IS NOT NULL AND ana.last_name IS NOT NULL
                  THEN ana.middle_name
              ELSE afn.middle_name
              END                                                                      AS civ_mname
            , CASE
              WHEN afn.first_name IS NOT NULL AND afn.last_name IS NOT NULL
                  THEN afn.last_name
              WHEN ana.first_name IS NOT NULL AND ana.last_name IS NOT NULL
                  THEN ana.last_name
              ELSE afn.last_name
              END                                                                      AS civ_lname
            , CASE
              WHEN afn.first_name IS NOT NULL AND afn.last_name IS NOT NULL
                  THEN afn.suffix
              WHEN ana.first_name IS NOT NULL AND ana.last_name IS NOT NULL
                  THEN ana.suffix
              ELSE afn.suffix
              END                                                                      AS civ_suffix
            , CASE
              WHEN ana.country_id = 'USA'
                  THEN SUBSTRING(ana.postal_code, 1, 5)
              ELSE ana.postal_code
              END                                                                      AS civ_zip
            , CASE
              WHEN ana.country_id = 'USA'
                  THEN SUBSTRING(ana.postal_code, 6, 4)
              ELSE ana.postal_code
              END                                                                      AS civ_zip4
            , ana.mobile_telecom_nbr                                                   AS civ_hphone
            , DECODE(aps.fr_non_ack_flg, 'Y', 'X', 'U')                                AS fic_acknofq
            , cast(' ' as varchar(1))                                                  AS fic_anonym
            , CASE
              WHEN ana.country_id = 'USA'
                  THEN DECODE(ana.usa_deliverable_flg, 'N', 'X', 'U')
              WHEN ana.country_id != 'USA'
                  THEN DECODE(ana.for_deliverable_flg, 'N', 'X', 'U')
              END                                                                      AS fic_badaddr

            , DECODE(ana.tel_valid_flag, 'N', 'X', 'U')                                AS fic_badphn
            , DECODE(aps.dcd_flg, 'N', 'X', 'U')                                       AS fic_deceased
            , DECODE(aps.fr_mail_freq_cd, 'N', 'X', '1', '1', '2', '2', '3', '3', 'U') AS fic_mailfq
            , DECODE(aps.fr_non_prom_prem_flg, 'Y', 'X', 'U')                          AS fic_nopremium
            , DECODE(aps.non_prom_rfl_flg, 'Y', 'X', 'U')                              AS fic_noraffent
            , DECODE(aps.non_prom_rfl_flg, 'Y', 'X', 'U')                              AS fic_norafmail
            , DECODE(aps.fr_tm_freq_cd, 'N', 'X', '1', '1', 'U')                       AS fic_phonefq
            , DECODE(aps.fr_rnw_mail_freq_cd, 'N', 'X', '1', '1', 'U')                 AS fic_renewfq
            , DECODE(aps.fr_non_prom_tm_rem_flg, 'Y', 'X', 'U')                        AS fic_tmremindfq
            , DECODE(aps.fr_non_prom_tof_flg, 'Y', 'X', 'U')                           AS fic_toffq
            , apo.ord_dt                                                               AS pid_recdate_ord
            , adi.crt_dt                                                               AS pid_recdate_crt
            , CAST(NULL AS VARCHAR(10))                                                AS csc_ind_urn


        FROM prod.agg_name_address ana
            LEFT JOIN  prod.agg_preference_summary aps
                ON aps.individual_id = ana.individual_id
            LEFT JOIN prod.agg_individual agi
                ON ana.individual_id = agi.individual_id
            LEFT JOIN prod.agg_fundraising_name afn
                ON afn.individual_id = agi.individual_id
            LEFT JOIN prod.agg_digital_item adi
                ON adi.individual_id = agi.individual_id
            LEFT JOIN prod.agg_print_order apo
                ON apo.individual_id = agi.individual_id

        WHERE 1 = 1
              AND (exists(SELECT NULL
                          FROM cte_re_ind_upd reupd
                          WHERE ana.individual_id = reupd.individual_id)
                   OR exists(SELECT NULL
                             FROM cte_re_ind_new renew
                             WHERE ana.individual_id = renew.individual_id))


    ), cte_fin_agg AS (

        SELECT
            csc_ind_id
            , date_modified
            , cic_cubord
            , cid_recdate
            , civ_addr1
            , civ_city
            , civ_state
            , civ_cmpnme
            , civ_title1
            , civ_fname
            , civ_mname
            , civ_lname
            , civ_suffix
            , civ_zip
            , civ_zip4
            , civ_hphone
            , fic_acknofq
            , fic_anonym
            , fic_badaddr
            , fic_badphn
            , fic_deceased
            , fic_mailfq
            , fic_nopremium
            , fic_noraffent
            , fic_norafmail
            , fic_phonefq
            , fic_renewfq
            , fic_tmremindfq
            , fic_toffq
            , to_char(least(MIN(pid_recdate_ord), MIN(pid_recdate_crt)), 'MM/DD/YYYY') pid_recdate
            , csc_ind_urn

        FROM cte_fin_calc
        GROUP BY csc_ind_id, date_modified, cic_cubord, cid_recdate, civ_addr1, civ_city, civ_state, civ_cmpnme, civ_title1, civ_fname, civ_mname, civ_lname, civ_suffix, civ_zip, civ_zip4, civ_hphone, fic_acknofq, fic_anonym, fic_badaddr, fic_badphn, fic_deceased, fic_mailfq, fic_nopremium, fic_noraffent, fic_norafmail, fic_phonefq, fic_renewfq, fic_tmremindfq, fic_toffq, csc_ind_urn
    )

    SELECT
        csc_ind_id
        , date_modified
        , cic_cubord
        , cid_recdate
        , civ_addr1
        , civ_city
        , civ_state
        , civ_cmpnme
        , civ_title1
        , civ_fname
        , civ_mname
        , civ_lname
        , civ_suffix
        , civ_zip
        , civ_zip4
        , civ_hphone
        , fic_acknofq
        , fic_anonym
        , fic_badaddr
        , fic_badphn
        , fic_deceased
        , fic_mailfq
        , fic_nopremium
        , fic_noraffent
        , fic_norafmail
        , fic_phonefq
        , fic_renewfq
        , fic_tmremindfq
        , fic_toffq
        , pid_recdate
        , csc_ind_urn
    FROM cte_fin_agg fin
    WHERE 1 = 1
          AND NOT exists(SELECT NULL
                         FROM prod.rm_icd_lookup excl1 /*Exclusions*/
                         WHERE fin.csc_ind_id = excl1.acx_rm_new_individual_id
                               AND source_indiv_id IN (1000200101269706, 10002001012697060, 10002000131940010, 10002000611371060, 10002000136474010))

;

/*SD Note: Inserting new individuals which were never sent previously into RE_INDIVIDUALS*/
INSERT INTO prod.re_individuals
SELECT distinct csc_ind_id as individual_id
FROM agg.${custom_var1} temp
where not exists (select null from prod.re_individuals rei where temp.csc_ind_id = rei.individual_id)
;
/*SD Note: Update RE_EMA-specific extract date in RE_INDIVIDUALS*/
UPDATE prod.re_individuals
SET ind_last_extract_date = cast('${custom_var2}' AS TIMESTAMP)
FROM agg.${custom_var1} temp
    INNER JOIN prod.re_individuals rei
        ON rei.individual_id = temp.csc_ind_id
;