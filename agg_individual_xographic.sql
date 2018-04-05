-- UPDATE GENDER_CD FROM 10 DAY RANGE
UPDATE prod.agg_individual_xographic
SET gender_cd = t.ib8688_gender_input_flag
    , maint_dt = CURRENT_DATE
FROM prod.agg_individual_xographic xo
    JOIN (SELECT
              individual_id
              , ib8688_gender_input_flag
              , update_date
              , rank()
                OVER (
                    PARTITION BY individual_id
                    ORDER BY update_date DESC ) AS rank
          FROM prod.infobase_profile
          WHERE update_date >= DateAdd(DAY, -10, Current_Date)
                AND ib8688_gender_input_flag IS NOT NULL) t
        ON xo.individual_id = t.individual_id
WHERE t.rank = 1 AND xo.gender_cd != t.ib8688_gender_input_flag;


;
select cast(sysdate as timestamp)

;
with cte_dd as (
select xo.individual_id, gender_cd, ib8688_gender_input_flag
FROM prod.agg_individual_xographic xo
    JOIN (SELECT
              individual_id
              , ib8688_gender_input_flag
              , update_date
              , rank()
                OVER (
                    PARTITION BY individual_id
                    ORDER BY update_date DESC ) AS rank
          FROM prod.infobase_profile
          WHERE update_date >= DateAdd(DAY, -150, Current_Date)
                AND ib8688_gender_input_flag IS NOT NULL) t
        ON xo.individual_id = t.individual_id
WHERE t.rank = 1 AND xo.gender_cd != t.ib8688_gender_input_flag)
    , cte_sd as (

    SELECT
        ip.individual_id
        , gender_cd
        , ib8688_gender_input_flag
    FROM prod.agg_individual_xographic xo
        INNER JOIN prod.infobase_profile ip
            ON xo.individual_id = ip.individual_id
    WHERE update_date >= DateAdd(DAY, -150, Current_Date)
          AND ib8688_gender_input_flag IS NOT NULL
          AND xo.gender_cd != ip.ib8688_gender_input_flag
)

select 1,count(*) from cte_dd
union ALL
    select 2,count( *) from cte_sd
;
SELECT cast(getdate() as timestamp)
FROM prod.agg_individual_xographic
LIMIT 100;


SELECT *
FROM prod.account
LIMIT 100;





;



SELECT
    sysdate
    , sysdate :: TIMESTAMP
    , current_time
    , current_timestamp
    , CURRENT_DATE
    , localtimestamp
, current_timestamp
-- FROM prod.agg_individual_xographic
LIMIT 100;
;
select max(update_date) , min(update_date)
FROM prod.infobase_profile
          WHERE update_date >= DateAdd(DAY, -10, Current_Date)
and update_date is not null
;
SELECT DateAdd(DAY, -10, Current_Date), trunc(sysdate) - 10
FROM prod.infobase_profile
LIMIT 100;
;

select count(*) from prod.infobase_profile
;




with tt1 (q,w) as (


              select 1,2
    union all select 1,2--+
    union all select 2,4
    union all select 2,5
    union all select 2,6--+
)
select q, max(substring(w||q,1,1))
--row_number() over (partition by q order by w desc, q desc)
    from tt1
group by q













;;;
-- INSERT GENDER_CD FOR NEW INDIVIDUALS FROM 10 DAY RANGE
INSERT INTO prod.agg_individual_xographic (individual_id, gender_cd, acx_gender_cd, customer_typ, dc_online_first_dt, dc_offline_first_dt, dc_fund_first_dt, dc_corp_first_dt, cga_dob, maint_dt, dc_cdb_last_ord_dt, ad_apl_keycode, dc_last_aps_ord_dt, fr_ch_status)
    SELECT
        individual_id
        , ib8688_gender_input_flag
        , NULL
        , 'C'
        , NULL
        , NULL
        , NULL
        , NULL
        , NULL
        , CURRENT_DATE
        , NULL
        , NULL
        , NULL
        , NULL
    FROM (SELECT
              individual_id
              , ib8688_gender_input_flag
              , update_date
              , rank()
                OVER (
                    PARTITION BY individual_id
                    ORDER BY update_date DESC ) AS rank
          FROM prod.infobase_profile
          WHERE update_date >= DateAdd(DAY, -10, Current_Date)
                AND ib8688_gender_input_flag IS NOT NULL) t
    WHERE t.rank = 1
          AND individual_id NOT IN
              (SELECT individual_id
               FROM prod.agg_individual_xographic);

-- UPDATE ACX_GENDER FROM 3 DAY RANGE
UPDATE prod.agg_individual_xographic
SET acx_gender_cd = t.acx_gender, maint_dt = CURRENT_DATE
FROM prod.agg_individual_xographic xo
    JOIN (SELECT
              acx_rm_new_individual_id
              , acx_gender
              , create_date
              , rank()
                OVER (
                    PARTITION BY acx_rm_new_individual_id
                    ORDER BY create_date DESC ) AS rank
          FROM prod.match_instance
          WHERE create_date >= DateAdd(DAY, -3, Current_Date)
                AND acx_gender IS NOT NULL) t
        ON xo.individual_id = t.acx_rm_new_individual_id
WHERE t.rank = 1 AND xo.gender_cd != t.acx_gender;

-- INSERT GENDER_CD FOR NEW INDIVIDUALS FROM 3 DAY RANGE
INSERT INTO prod.agg_individual_xographic (individual_id, gender_cd, acx_gender_cd, customer_typ, dc_online_first_dt, dc_offline_first_dt, dc_fund_first_dt, dc_corp_first_dt, cga_dob, maint_dt, dc_cdb_last_ord_dt, ad_apl_keycode, dc_last_aps_ord_dt, fr_ch_status)
    SELECT
        acx_rm_new_individual_id
        , NULL
        , acx_gender
        , 'C'
        , NULL
        , NULL
        , NULL
        , NULL
        , NULL
        , CURRENT_DATE
        , NULL
        , NULL
        , NULL
        , NULL
    FROM (SELECT
              acx_rm_new_individual_id
              , acx_gender
              , create_date
              , rank()
                OVER (
                    PARTITION BY acx_rm_new_individual_id
                    ORDER BY create_date DESC ) AS rank
          FROM prod.match_instance
          WHERE create_date >= DateAdd(DAY, -3, Current_Date)
                AND acx_gender IS NOT NULL) t
    WHERE t.rank = 1
          AND acx_rm_new_individual_id NOT IN
              (SELECT individual_id
               FROM prod.agg_individual_xographic);

-- UPDATE FR_CH_STATUS
/* NEED ADDITIONAL INFORMATION IN ORDER TO CODE THIS */


-- UPDATE CUSTOMER_TYP
UPDATE prod.agg_individual_xographic
SET customer_typ = 'C'
WHERE customer_typ IN ('P', 'R', 'T')
      AND individual_id IN
          (SELECT DISTINCT (individual_id)
           FROM prod.action_header);

-- UPDATE GENDER_CD FROM 10 DAY RANGE
UPDATE prod.agg_individual_xographic
SET gender_cd = t.ib8688_gender_input_flag, maint_dt = CURRENT_DATE
FROM prod.agg_individual_xographic xo
    JOIN (SELECT
              individual_id
              , ib8688_gender_input_flag
              , update_date
              , rank()
                OVER (
                    PARTITION BY individual_id
                    ORDER BY update_date DESC ) AS rank
          FROM prod.infobase_profile
          WHERE update_date >= DateAdd(DAY, -10, Current_Date)
                AND ib8688_gender_input_flag IS NOT NULL) t
        ON xo.individual_id = t.individual_id
WHERE t.rank = 1 AND xo.gender_cd != t.ib8688_gender_input_flag;

-- INSERT GENDER_CD FOR NEW INDIVIDUALS FROM 10 DAY RANGE
INSERT INTO prod.agg_individual_xographic (individual_id, gender_cd, acx_gender_cd, customer_typ, dc_online_first_dt, dc_offline_first_dt, dc_fund_first_dt, dc_corp_first_dt, cga_dob, maint_dt, dc_cdb_last_ord_dt, ad_apl_keycode, dc_last_aps_ord_dt, fr_ch_status)
    SELECT
        individual_id
        , ib8688_gender_input_flag
        , NULL
        , 'C'
        , NULL
        , NULL
        , NULL
        , NULL
        , NULL
        , CURRENT_DATE
        , NULL
        , NULL
        , NULL
        , NULL
    FROM (SELECT
              individual_id
              , ib8688_gender_input_flag
              , update_date
              , rank()
                OVER (
                    PARTITION BY individual_id
                    ORDER BY update_date DESC ) AS rank
          FROM prod.infobase_profile
          WHERE update_date >= DateAdd(DAY, -10, Current_Date)
                AND ib8688_gender_input_flag IS NOT NULL) t
    WHERE t.rank = 1
          AND individual_id NOT IN
              (SELECT individual_id
               FROM prod.agg_individual_xographic);

-- UPDATE ACX_GENDER FROM 3 DAY RANGE
UPDATE prod.agg_individual_xographic
SET acx_gender_cd = t.acx_gender, maint_dt = CURRENT_DATE
FROM prod.agg_individual_xographic xo
    JOIN (SELECT
              acx_rm_new_individual_id
              , acx_gender
              , create_date
              , rank()
                OVER (
                    PARTITION BY acx_rm_new_individual_id
                    ORDER BY create_date DESC ) AS rank
          FROM prod.match_instance
          WHERE create_date >= DateAdd(DAY, -3, Current_Date)
                AND acx_gender IS NOT NULL) t
        ON xo.individual_id = t.acx_rm_new_individual_id
WHERE t.rank = 1 AND xo.gender_cd != t.acx_gender;

-- INSERT GENDER_CD FOR NEW INDIVIDUALS FROM 3 DAY RANGE
INSERT INTO prod.agg_individual_xographic (individual_id, gender_cd, acx_gender_cd, customer_typ, dc_online_first_dt, dc_offline_first_dt, dc_fund_first_dt, dc_corp_first_dt, cga_dob, maint_dt, dc_cdb_last_ord_dt, ad_apl_keycode, dc_last_aps_ord_dt, fr_ch_status)
    SELECT
        acx_rm_new_individual_id
        , NULL
        , acx_gender
        , 'C'
        , NULL
        , NULL
        , NULL
        , NULL
        , NULL
        , CURRENT_DATE
        , NULL
        , NULL
        , NULL
        , NULL
    FROM (SELECT
              acx_rm_new_individual_id
              , acx_gender
              , create_date
              , rank()
                OVER (
                    PARTITION BY acx_rm_new_individual_id
                    ORDER BY create_date DESC ) AS rank
          FROM prod.match_instance
          WHERE create_date >= DateAdd(DAY, -3, Current_Date)
                AND acx_gender IS NOT NULL) t
    WHERE t.rank = 1
          AND acx_rm_new_individual_id NOT IN
              (SELECT individual_id
               FROM prod.agg_individual_xographic);

-- UPDATE FR_CH_STATUS
/* NEED ADDITIONAL INFORMATION IN ORDER TO CODE THIS */


-- UPDATE CUSTOMER_TYP
UPDATE prod.agg_individual_xographic
SET customer_typ = 'C'
WHERE customer_typ IN ('P', 'R', 'T')
      AND individual_id IN
          (SELECT DISTINCT (individual_id)
           FROM prod.action_header);