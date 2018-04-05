WITH RM_VALID_IDS (icd_id, cdh_id) AS
    (
SELECT source_indiv_id , acx_rm_new_individual_id 
    FROM prod.match_instance
    )
SELECT DISTINCT icd_id, cdh_id
FROM RM_VALID_IDS
WHERE EXISTS
    (
SELECT NULL
    FROM RM_VALID_IDS RM
    WHERE 1=1
        AND RM_VALID_IDS.cdh_id = rm.cdh_id
        and RM.ICD_ID 
in (10314300081258000
)
    );



ord_num, 

order by 1,2,3,4,5
;
;
SELECT individual_id, credit_card_expire, magazine_code, credit_card_type
FROM prod.credit_card cc inner join  prod.account acc on cc.hash_account_id = acc.hash_account_id
WHERE 1=1
and individual_id = 1221271173


;
;
SELECT count(*)
FROM prod.agg_TARGET_ANALYTICS
WHERE 1=1
union all
SELECT count(*)
FROM prod.TARGET_ANALYTICS
WHERE 1=1
;
select *
from information_schema.tables
where table_name like ''


;
;
;
SELECT count(*)
FROM prod.AGG_EMAIL_DEV_RESPONSE_DETAIL
WHERE 1=1
;
select *
from prod.AGG_EMAIL_DEV_RESPONSE_DETAIL
where individual_id = 1200273404 /*10002000000045050*/
and cell_name in ( 'CROMC_BrandAnnouncement_General_Remail_EH69BB1_20160926 - Unique 2'
,'CROMC_CRO_BuildBuyApril2015_EH54BR2_20150416'
,'CROMC_CRO_BuildBuyMarch2017Reminder_EH73BR1_20170323'
,'CROMC_Monthly_iPad_1_ET4BIPAD1_20141105'
,'CROMC_Monthly_iPad_April2014_ET43IPAD1_20140303'
,'CRONL_MCAFTER_1_EH46TBA_20140625'
,'CRRES_CFA_STUDY3_20130430_2013109Ba_Rem1'
,'CUAQB_2014_5004_Control_34_Rem3_20140520'
,'CUAQB_Auto_Survey_R2- 0000023'
,'FUNDR_Online_Nov_GivingTuesday_blast5_7033711510_20161129'
,'FUNDR_Online_Oct_Acquisition_eff1_6031710104_20151018'
,'SSMRT_DM_AFTER_DN28SCA1_20120829')

;
;
SELECT count(*)
FROM prod.individual_address
WHERE 1=1

;

SELECT TOP 100 acx_ef_record
FROM prod.individual_address WHERE individual_id = 1203179758;
;
SELECT TOP 100 *
FROM prod.agg_name_address
WHERE 1=1 and individual_id = 1200273404            
;
SELECT  count(*) FROM prod.agg_digital_item;
 


