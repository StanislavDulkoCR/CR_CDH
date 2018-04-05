WITH cte_1614_donotrent_calc AS (
    SELECT
          15                                                 AS transaction_type
        , lpad(substring(acc.source_account_id, 1, 9), 9, 0) AS account_number
        , upper(cast(rpad(' ', 102, ' ') AS VARCHAR(102)))   AS filler_not_used1
        , upper(lpad(pad.magazine_code, 3, ' '))             AS magazine_abbreviation
        , 'C'                                                AS promo_restrict
        , upper(cast(rpad(' ', 158, ' ') AS VARCHAR(158)))   AS filler_not_used2

    FROM prod.account acc
        INNER JOIN prod.print_account_detail pad
            ON acc.hash_account_id = pad.hash_account_id

    WHERE 1 = 1
          AND acc.source_name = 'CDS'
          AND acc.account_subtype_code NOT IN ('CRE', 'CRT')
          AND pad.purged_ind IS NULL
          AND exists(SELECT NULL
                     FROM prod.preference_history ph
                     WHERE ph.id_value = acc.individual_id
                           AND ph.id_type = 'I'
                           AND ph.preference_code = 'RENT'
                           AND ph.preference_value = 'N'
                           AND ph.data_source <> 'CDS')
)

SELECT
    calc.transaction_type
    , calc.account_number
    , calc.filler_not_used1
    , 99999 || lpad(row_number()
                    OVER (
                        ORDER BY cast(calc.account_number as BIGINT) ASC, calc.magazine_abbreviation asc), 12, 0) AS audit_trail_number
    , calc.magazine_abbreviation
    , calc.promo_restrict
    , calc.filler_not_used2

FROM cte_1614_donotrent_calc calc
    order by cast(calc.account_number as BIGINT) ASC, calc.magazine_abbreviation asc
LIMIT 100;

/*acc.data_source = 'CDS' and ph.data_source <> 'CDS'*/