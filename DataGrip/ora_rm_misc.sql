WITH RM_VALID_IDS (icd_id, cdh_id) AS
    (SELECT SRC_INDIV_ID
      , ACX_RM_NEW_INDIVIDUAL_ID
    FROM ACXIOM_TO_CR_DAILY_0710
    )
SELECT DISTINCT icd_id, cdh_id
FROM RM_VALID_IDS RM1
WHERE EXISTS
    (SELECT NULL
    FROM RM_VALID_IDS RM2
    WHERE 1=1
        and RM1.CDH_ID = RM2.CDH_ID
        and RM2.ICD_ID in (10002001100541010)
--        and RM2.cdh_id    in (       1200000002)
    )

    order by 2,1;




select COUNT(*)
from MIDDLE_TIER2.MT_PROMOTION tm
where exists (select null from ACXIOM_TO_CR_DAILY_0710_fltrd where icd_id = tm.individual_id)

--2,584,537,320
--2,604,128,646
;
SELECT *
FROM warehouse4.external_ref
;

SELECT *
FROM WAREHOUSE2.FUNDRAISING_TRANS
where INDIVIDUAL_ID = 10002001092787110
;
SELECT *
FROM MIDDLE_TIER2.MT_FR_DONATION
where INDIVIDUAL_ID =10002001092787110
;
SELECT *
FROM MIDDLE_TIER2.MT_FR_SUMMARY
where INDIVIDUAL_ID = 10002001092787110
;

WITH RM_VALID_IDS (icd_id, cdh_id) AS
    (SELECT SRC_INDIV_ID
      , ACX_RM_NEW_INDIVIDUAL_ID
    FROM ACXIOM_TO_CR_DAILY_0710
    )
SELECT DISTINCT icd_id, cdh_id

-- NAME_ADDRESS

    , na.HH_ID
    , na.ACCOUNT_NUM
    , na.PWI_ACCOUNT_NUM
    , na.ADDRESS_ID
    , na.SALUTATION
    , na.FIRST_NAME
    , na.MIDDLE_NAME
    , na.LAST_NAME
    , na.SUFFIX
    , na.BUSINESS_NAME
    , na.PRIMARY_NUMBER
    , na.PRE_DIRECTIONAL
    , na.STREET
    , na.STREET_SUFFIX
    , na.POST_DIRECTIONAL
    , na.UNIT_DESIGNATOR
    , na.SECONDARY_NUMBER
    , na.ADDITIONAL_ADDRESS_DATA
    , na.CITY
    , na.STATE_PROVINCE
    , na.POSTAL_CODE
    , na.SCF_FSA
    , na.COUNTRY_ID
    , na.CAN_FLG
    , na.USA_FLG
    , na.FOREIGN_FLG
    , na.USA_DELIVERABLE_FLG
    , na.FOR_DELIVERABLE_FLG
    , na.DPBC
    , na.CRC
    , na.LOT
    , na.DSF_DELIVERY_TYPE
    , na.DSF_DELIVERABILITY_CODE
    , na.GCDI_DELIVERABILITY_CODE
    , na.DSF_MATCH_FLAG
    , na.DSF_VACANCY_INDICATOR
    , na.DSF_SEASONAL_INDICATOR
    , na.DSF_DROP_POINT_INDICATOR
    , na.DSF_RECORD_TYPE
    , na.LACS_INDICATOR
    , na.EDIT_FAIL_FLG
    , na.COA_SOURCE
    , na.COA_DATE
    , na.NCOA_MOVE_TYPE
    , na.NCOA_CATEGORY
    , na.NCOA_FOOTNOTE
    , na.NCOA_MOVE_EFF_DT
    , na.ADDRESS_TYPE
    , na.TEL_VALID_FLAG
    , na.TELECOM_NBR
    , na.TEL_AREA_CODE
    , na.TEL_PREFIX
    , na.MOBILE_TELECOM_NBR
    , na.COA_CNT

--EMAIL
    , em.EMAIL_TYPE_CD
    , em.EMAIL_ADDRESS
    , em.EMAIL_ID
    , em.DATA_SOURCE
    , em.SRC_VALID_FLAG
    , em.SRC_DELV_IND


FROM RM_VALID_IDS RM1
    left join middle_tier2.MT_NAME_ADDRESS na
        on rm1.icd_id = na.INDIVIDUAL_ID
    left join MIDDLE_TIER2.MT_EMAIL em
        on rm1.icd_id = em.INDIVIDUAL_ID
WHERE EXISTS
    (SELECT NULL
    FROM RM_VALID_IDS RM2
    WHERE 1=1
        and RM1.CDH_ID != RM2.CDH_ID
        and rm1.icd_id = rm2.icd_id
    )

    order by 2,1;

;
create INDEX


WITH RM_VALID_IDS (icd_id, cdh_id) AS
    (SELECT SRC_INDIV_ID
      , ACX_RM_NEW_INDIVIDUAL_ID
    FROM ACXIOM_TO_CR_DAILY_0710
    )
SELECT DISTINCT icd_id, cdh_id
FROM RM_VALID_IDS RM1
WHERE EXISTS
    (SELECT NULL
    FROM RM_VALID_IDS RM2
    WHERE 1=1
        and RM1.CDH_ID <> RM2.CDH_ID
        and rm1.icd_id = rm2.icd_id


--         and RM.ICD_ID in (10002000000359110)
-- --        and cdh_id    in (       1216297895)
    )

    order by 1,2;

SELECT *
FROM MIDDLE_TIER2.MT_NAME_ADDRESS
;
SELECT *
FROM MIDDLE_TIER2.MT_EMAIL
;


SELECT *
FROM MIDDLE_TIER2.MT_INDIVIDUAL
;
select count(*)
    from MIDDLE_TIER2.MT_PROMO_SUMMARY
where exists(SELECT  null from ACXIOM_TO_CR_DAILY_0710_fltrd rm WHERE rm.icd_id = INDIVIDUAL_ID)
;
with tt1 as (
    SELECT mp.INDIVIDUAL_ID
    FROM middle_tier2.mt_promotion mp
    WHERE contact_type = 'P' AND rowid NOT IN (SELECT t1.rowid AS rowid_mt_promo
                                               FROM middle_tier2.mt_promotion t1, middle_tier2.mt_resp_summary t2
                                               WHERE t1.individual_id = t2.individual_id
                                                     AND t1.bus_unt = t2.bus_unt
                                                     AND t1.keycode = t2.keycode
                                                     AND t1.chnl_cd = 'E'
                                                     AND t1.contact_type = 'P'
                                                     AND (t2.ABQ_RESP_CD = 'W' OR t2.ADV_RESP_CD = 'W' OR t2.APS_RESP_CD = 'W' OR t2.FR_RESP_CD = 'W'
                                                          OR t2.SUBS_RESP_CD = 'W' OR t2.CRSCH_RESP_CD = 'W'))
)

select count(*)
    from tt1
where exists( select null from ACXIOM_TO_CR_DAILY_0710_fltrd rm WHERE rm.icd_id = INDIVIDUAL_ID)
;


2312402733

;

icd_id	cdh_id	non_prod_prom_cnt
10002001756251040	1213200413	4<>3

;

SELECT non_prod_prom_cnt
FROM MIDDLE_TIER2.MT_PROMO_SUMMARY
where INDIVIDUAL_ID = 10002001756251040
;
  count(case when ph.bus_unt in ('ABQ','ADV','FUN','CGA','MKT','PNL')
             then 1
        end) non_prod_prom_cnt
;
select * from middle_tier2.mt_promotion where contact_type = 'P' and rowid  in (
    select t2.*
            from middle_tier2.mt_promotion t1, middle_tier2.mt_resp_summary t2
            where t1.individual_id = t2.individual_id
                and t1.bus_unt = t2.bus_unt
                and t1.keycode = t2.keycode
                and t1.chnl_cd = 'E'
                and t1.contact_type = 'P'
                and t1.INDIVIDUAL_ID = 10002001756251040
                and t2.INDIVIDUAL_ID = 10002001756251040
                and (t2.ABQ_RESP_CD = 'W' or t2.ADV_RESP_CD = 'W' or t2.APS_RESP_CD = 'W' or t2.FR_RESP_CD = 'W'
                or t2.SUBS_RESP_CD = 'W' or t2.CRSCH_RESP_CD = 'W')

)

;

SELECT *
FROM WAREHOUSE4.EXTERNAL_REF
where 1=1
and
      ( external_key like '%16856582'
or external_key like '%16856803'
or external_key like '%16868616'
or external_key like '%16871715'
or external_key like '%16886667'
or external_key like '%16895248'
or external_key like '%16895340'
or external_key like '%16936978'
or external_key like '%16937103')
-- and INDIVIDUAL_ID = 10002000711865080
;


SELECT *
FROM RM_20171024_dups_non_dgi_dates rm1
WHERE (to_char(rm1.maintenance_date, 'YYYYMMDD')) = (SELECT min(to_char(maintenance_date, 'YYYYMMDD'))
                                                     FROM RM_20171024_dups_non_dgi_dates
                                                     WHERE rm1.individual_id = individual_id )
      AND rm1.individual_id = 10002000000048080

;


SELECT min(to_char(maintenance_date, 'YYYYMMDD')), INDIVIDUAL_ID, ADDRESS_ID
                                                     FROM RM_20171024_dups_non_dgi_dates
where individual_id = 10002000000048080
group by INDIVIDUAL_ID, ADDRESS_ID

;


SELECT rm1.*, dense_rank() over (PARTITION BY INDIVIDUAL_ID order by maintenance_date desc)
FROM RM_20171024_dups_non_dgi_dates rm1
where individual_id in ( 10002000000048080
, 10002000000000080
, 10002000000000080
, 10002000000001080
, 10002000000001080
, 10002000000002060
, 10002000000004010
, 10002000000004010
, 10002000000005100
, 10002000000005100
, 10002000000009050)
;


SELECT *
    from RM_20171024_DUPS_NON_DGI_DATES

;

SELECT
    t1.individual_id
    , COUNT(CASE WHEN NVL(t1.set_cd, 'X') NOT IN ('C', 'E') AND t1.mag_cd IN ('CNS', 'CRH', 'CRM', 'SHM')
                      AND ((SUBSTR(t1.keycode, 1, 1) IN ('A', 'G', 'X'))
                           OR (SUBSTR(t1.keycode, 1, 1) = 'D' AND NVL(SUBSTR(t1.keycode, 2, 1), 'X') != 'N')
                           OR (SUBSTR(t1.keycode, 1, 1) = 'B' AND NVL(SUBSTR(t1.keycode, 9, 1), 'X') NOT IN ('A', 'B', 'C', 'S'))
                           OR (SUBSTR(t1.keycode, 1, 1) = 'U' AND REGEXP_INSTR(NVL(SUBSTR(t1.keycode, 9, 1), '1'), '[^[:alpha:]]') = 1)
                           OR (SUBSTR(t1.keycode, 1, 1) = 'R' AND NVL(SUBSTR(t1.keycode, 9, 1), 'X') NOT IN ('B', 'C', 'D', 'E', 'F', 'T'))
                           OR (SUBSTR(t1.keycode, 1, 1) = 'U' AND LENGTH(t1.keycode) = 7 AND REGEXP_INSTR(SUBSTR(t1.keycode, 6, 1), '[^[:alpha:]]') = 1))
    THEN 1 END) AS ofo_prod_dm_ord_cnt
--     ,t1.acct_id
-- ,t1.mag_cd
--     ,t1.ord_id
-- ,SUBSTR(t1.keycode, 1, 1) q
--     ,REGEXP_INSTR(NVL(SUBSTR(t1.keycode, 9, 1), '1'), '[^[:alpha:]]') w
--     ,NVL(SUBSTR(t1.keycode, 9, 1), '1') e
--
-- ,case when (SUBSTR(t1.keycode, 1, 1) = 'U' AND REGEXP_INSTR(NVL(SUBSTR(t1.keycode, 9, 1), '1'), '[^[:alpha:]]') = 1) then 1 else null end zz
FROM middle_tier2.mt_offline_ord t1, warehouse2.offline_cancel_act oca
WHERE t1.ord_id = oca.ord_id (+)
      AND t1.acct_id = oca.acct_id (+)
      AND t1.individual_id = 10002000024802090
-- and NVL(t1.set_cd, 'X') NOT IN ('C', 'E') AND t1.mag_cd IN ('CNS', 'CRH', 'CRM', 'SHM')
GROUP BY individual_id
;

select substr('1234567890',-5) from dual