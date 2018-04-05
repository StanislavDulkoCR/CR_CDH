;
create table cr_temp.GTT_MT_ONLINE_ORD as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
/*, acct_id*/
--, NVL2(ord_id, ord_id::text,'(NULL)') as ord_id
, NVL2(stat_cd, stat_cd::text,'(NULL)') as stat_cd
, NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)') as ord_dt
, NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)') as entr_typ_cd
--, NVL2(TO_CHAR(pd_dt,'DD-MON-YY'), TO_CHAR(pd_dt,'DD-MON-YY')::text,'(NULL)') as pd_dt
--, NVL2(trunc(nvl(pd_amt,'0')), trunc(nvl(pd_amt,'0'))::text,'(NULL)') as pd_amt
, NVL2(TO_CHAR(crt_dt,'DD-MON-YY'), TO_CHAR(crt_dt,'DD-MON-YY')::text,'(NULL)') as crt_dt
/*---END---*/
,md5(
ICD_ID||cdh_id
/*--START--*/
/*||individual_id*/
/*||acct_id*/
--||NVL2(ord_id, ord_id::text,'(NULL)')
||NVL2(stat_cd, stat_cd::text,'(NULL)')
||NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)')
--||NVL2(TO_CHAR(pd_dt,'DD-MON-YY'), TO_CHAR(pd_dt,'DD-MON-YY')::text,'(NULL)')
--||NVL2(trunc(nvl(pd_amt,'0')), trunc(nvl(pd_amt,'0'))::text,'(NULL)')
||NVL2(TO_CHAR(crt_dt,'DD-MON-YY'), TO_CHAR(crt_dt,'DD-MON-YY')::text,'(NULL)')
/*---END---*/
) as hash_value
from cr_temp.MT_ONLINE_ORD T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;
CREATE TABLE cr_temp.gtt_agg_digital_order AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
/*, acct_id*/
--, NVL2(ord_id, ord_id::text,'(NULL)') as ord_id
, NVL2(stat_cd, stat_cd::text,'(NULL)') as stat_cd
, NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)') as ord_dt
, NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)') as entr_typ_cd
--, NVL2(TO_CHAR(pd_dt,'DD-MON-YY'), TO_CHAR(pd_dt,'DD-MON-YY')::text,'(NULL)') as pd_dt
--, NVL2(trunc(nvl(pd_amt,'0')), trunc(nvl(pd_amt,'0'))::text,'(NULL)') as pd_amt
, NVL2(TO_CHAR(crt_dt,'DD-MON-YY'), TO_CHAR(crt_dt,'DD-MON-YY')::text,'(NULL)') as crt_dt
/*---END---*/
,md5(
ICD_ID||cdh_id
/*--START--*/
/*||individual_id*/
/*||acct_id*/
--||NVL2(ord_id, ord_id::text,'(NULL)')
||NVL2(stat_cd, stat_cd::text,'(NULL)')
||NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)')
--||NVL2(TO_CHAR(pd_dt,'DD-MON-YY'), TO_CHAR(pd_dt,'DD-MON-YY')::text,'(NULL)')
--||NVL2(trunc(nvl(pd_amt,'0')), trunc(nvl(pd_amt,'0'))::text,'(NULL)')
||NVL2(TO_CHAR(crt_dt,'DD-MON-YY'), TO_CHAR(crt_dt,'DD-MON-YY')::text,'(NULL)')
/*---END---*/
) as hash_value
from prod.agg_digital_order T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;
------------------------------------------------------------------------TABLE CREATION END\
with hash_diff as (
select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID, HASH_VALUE
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.GTT_MT_ONLINE_ORD
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.gtt_agg_digital_order
)
group by ICD_ID, CDH_ID, hash_value
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select top 50 * from hash_diff;
------------------------------------------------------------------------RUN ALL ABOVE
;
select * from   cr_temp.GTT_MT_ONLINE_ORD  where icd_id = 10002000000001040
;
select * from   cr_temp.gtt_agg_digital_order  where cdh_id = 1201301788
;
------------------------------------------------------------------------------------------------
select * from   cr_temp.GTT_MT_ONLINE_ORD  where icd_id = 10002000004933090
;
select * from   cr_temp.gtt_agg_digital_order  where cdh_id = 1205938146
;
--truncate table    cr_temp.GTT_MT_ONLINE_ORD;
drop table  cr_temp.GTT_MT_ONLINE_ORD;
--truncate table cr_temp.gtt_agg_digital_order;
drop table  cr_temp.gtt_agg_digital_order;

"Table Level rule(s):
All records in the Data Warehouse ACTION_HEADER table 
where ACTION_HEADER.ACTION_TYPE = 'ORDER' AND ACTION_HEADER.SOURCE_NAME IN ('PWI', 'DCK') or 
(where DIGITAL_TRANSACTION.ORDER_TYPE = 'RET' and ACTION_HEADER.SOURCE_NAME IN ('PWI', 'DCK')
and ACTION_HEADER.HASH_ACTION_ID = DIGITAL_TRANSACTION_DETAIL.HASH_ACTION_ID and DIGITAL_TRANSACTION_DETAIL.REFERENCE_ID = '0')

as well as individuals in the Data Warehouse PRINT_TRANSACTION table where ACTION_HEADER.HASH_ACCOUNT_ID = PRINT_ACCOUNT_DETAIL.HASH_ACCOUNT_ID AND PRINT_TRANSACTION.HASH_ACTION_ID = ACTION_HEADER.HASH_ACTION_ID and PRINT_ACCOUNT_DETAIL.MAGAZINE_CODE = 'CRE' and ACTION_HEADER.SOURCE_NAME = 'CDS' and INDIVIDUAL is in EXTERNAL_REF.INDIVIDUAL_ID and EXTERNAL_REF.INTERNAL_KEY = OFFLINE_ACCOUNT.ACCT_ID."						
;
"ONLINE_ORDER
If ONLINE_ITEM.SKU_NUM < 5000000 then 
If ONLINE_ORDER.PD_DT is null then sum the ONLINE_ITEM.TOT_AMT where ONLINE_ITEM.ORD_ID = ONLINE_ORDER.ORD_ID
Else ONLINE_ORDER.PD_AMT.
If ONLINE_ITEM.SKU_NUM > 5000000 then 
sum the ONLINE_ITEM.TOT_AMT where ONLINE_ITEM.ORD_ID = ONLINE_ORDER.ORD_ID and ONLINE_ITEM.STAT_CD = 'C'.
OFFLINE_ORDER
If OFFLINE_ORDER.SET_CD is 'A' then OFFLINE_ORDER.NET_VAL_AMT.
Else
If the OFFLINE_ORDER.SET_CD is 'B' or 'D' (donor) sum the OFFLINE_ORDER.NET_VAL_AMT plus all OFFLINE_ORDER.NET_VAL_AMT of the recipient records where OFFLINE_ORDER.ORD_NUM of the donor order equals OFFLINE_ORDER.ORD_NUM of the recipient order and OFFLINE_ORDER.SET_CD of the recipient is 'C' or 'E'.
Else set to zero."

;
"Online (Digital)
(SOURCE_NAME = 'PWI', ACTION_TYPE = 'ORDER')
If DIGITAL_TRANSACTION_DETAIL.SKU_NUM < 5000000 then 
If ACTION_HEADER.PAID_DATE is null then sum the DIGITAL_TRANSACTION_DETAIL.LINE_ITEM_TOTAL_AMOUNT where ACTION_HEADER.HASH_ACTION_ID = DIGITAL_TRANSACTION_DETAIL.HASH_ACTION_ID
Else ACTION_HEADER.ACTION_AMOUNT.
If DIGITAL_TRANSACTION_DETAIL.SKU_NUM > 5000000 then 
sum the DIGITAL_TRANSACTION_DETAIL.LINE_ITEM_TOTAL_AMOUNT where ACTION_HEADER.HASH_ACTION_ID = DIGITAL_TRANSACTION_DETAIL.HASH_ACTION_ID and DIGITAL_TRANSACTION_DETAIL.LINE_ITEM_STATUS = 'C'.

Offline (Print)
(SOURCE_NAME = 'CDS', ACTION_TYPE = 'ORDER')
If PRINT_TRANSACTION.SET_CD is 'A' then ACTION_HEADER.ACTION_AMOUNT.
Else
If the PRINT_TRANSACTION.SET_CD is 'B' or 'D' (donor) sum the ACTION_HEADER.ACTION_AMOUNT plus all TMP_PRINT_RECIPIENT_ORDER.SUM_ACTION_AMOUNT of the recipient records where ACTION_HEADER.HASH_ACTION_ID of the donor order equals PRINT_TRANSACTION.HASH_ACTION_ID of the recipient order and PRINT_TRANSACTION.SET_CD of the recipient is 'C' or 'E'.
Else set to zero."

;
14717692
19552153
;
SELECT TOP 100 action_id,  action_type, td.source_name, ah.source_name, SKU_NUM, LINE_ITEM_TOTAL_AMOUNT, ah.paid_date, ACTION_AMOUNT
,LINE_ITEM_STATUS
FROM prod.DIGITAL_TRANSACTION_DETAIL td inner join prod.action_header ah on td.hash_action_id = ah.hash_action_id
WHERE 1=1 and individual_id = 1205030309
;
;
SELECT TOP 100 *
FROM cr_temp.mt_online_ord
WHERE 1=1 and individual_id = 10639500015409050

;
SELECT TOP 100 *
FROM prod.agg_digital_order
WHERE 1=1 and individual_id = 1233153701
;

SELECT TOP 100 * FROM prod.agg_books_order
WHERE ord_cdr_flg is null;
;
"Table Level rule(s):
All records in the Data Warehouse ACTION_HEADER table 
where ACTION_HEADER.ACTION_TYPE = 'ORDER' AND ACTION_HEADER.SOURCE_NAME IN ('PWI', 'DCK') or 
(where DIGITAL_TRANSACTION.ORDER_TYPE = 'RET' and ACTION_HEADER.SOURCE_NAME IN ('PWI', 'DCK') 
and ACTION_HEADER.HASH_ACTION_ID = DIGITAL_TRANSACTION_DETAIL.HASH_ACTION_ID 
and DIGITAL_TRANSACTION_DETAIL.REFERENCE_ID = '0')

as well as individuals in the Data Warehouse PRINT_TRANSACTION table
where ACTION_HEADER.HASH_ACCOUNT_ID = PRINT_ACCOUNT_DETAIL.HASH_ACCOUNT_ID 
AND PRINT_TRANSACTION.HASH_ACTION_ID = ACTION_HEADER.HASH_ACTION_ID 
and PRINT_ACCOUNT_DETAIL.MAGAZINE_CODE = 'CRE' and ACTION_HEADER.SOURCE_NAME = 'CDS' 
and INDIVIDUAL is in EXTERNAL_REF.INDIVIDUAL_ID and EXTERNAL_REF.INTERNAL_KEY = OFFLINE_ACCOUNT.ACCT_ID."
;
;
SELECT TOP 100 *
FROM prod.action_header inner join  prod.PRINT_ACCOUNT_DETAIL on  ACTION_HEADER.HASH_ACCOUNT_ID = PRINT_ACCOUNT_DETAIL.HASH_ACCOUNT_ID
inner join prod.PRINT_TRANSACTION on PRINT_TRANSACTION.HASH_ACTION_ID = ACTION_HEADER.HASH_ACTION_ID 
WHERE 1=1 and individual_id = 1233153701
;

select * from   cr_temp.GTT_MT_ONLINE_ORD  where icd_id = 10639500015409050
;
select * from   cr_temp.gtt_agg_digital_order
;
;
SELECT TOP 100 *
FROM cr_temp.mt_online_ord
WHERE 1=1 and individual_id = 10639500015409050
;
;
SELECT TOP 100 *
FROM cr_temp.mt_online_ord
WHERE 1=1 and individual_id = 10639500015409050

;
SELECT TOP 100 *
FROM prod.agg_digital_order
WHERE 1=1 and individual_id = 1233153701
;
"Online (Digital)
(SOURCE_NAME = 'PWI', ACTION_TYPE = 'ORDER')
If ACTION_HEADER.ACTION_SUBTYPE_CODE is null and any DIGITAL_TRANSACTION_DETAIL.SKU_NUM = SKU_LKUP.SKU_NUM and SKU_LKP.COMP_FLG = 'C' then 'F'
Else ACTION_HEADER.ACTION_SUBTYPE_CODE

Offline (Print)
(SOURCE_NAME = 'CDS', ACTION_TYPE = 'ORDER')
If the PRINT_TRANSACTION.SOURCE_TRANSACTION_CODE is 'CA' or 'CC' and the first four bytes ACTION_HEADER.KEYCODE are not 'LIFE' then code as 'F'
Else ACTION_HEADER.ACTION_SUBTYPE_CODE"

;
SELECT TOP 100 *
FROM prod.action_header 
WHERE 1=1 and individual_id = 1221711070       


;
select * from prod.agg_digital_item
where individual_id = '1221711070';
;
SELECT TOP 100 *
FROM cr_temp.mt_online_item
WHERE 1=1 and individual_id = 10002000000000050
;

select count(*) from   cr_temp.GTT_MT_ONLINE_ORD ;
select count(*) from   cr_temp.gtt_agg_digital_order;

"Online (Digital)
(SOURCE_NAME = 'PWI', ACTION_TYPE = 'ORDER')
If ACTION_HEADER.PD_DT is null
then use the maximum
    DIGITAL_TRANSACTION_DETAIL.LINE_ITEM_ORDER_MODIFY_DATE
       from the DIGITAL_TRANSACTION_DETAIL entry
            where
                DIGITAL_TRANSACTION_DETAIL.LINE_ITEM_STATUS = 'C' or 'H'
                where
                  DIGITAL_TRANSACTION_DETAIL.HASH_ACTION_ID = 
                        ACTION_HEADER.HASH_ACTION_ID,
Else
    ACTION_HEADER.PD_DT

Offline (Print)
(SOURCE_NAME = 'PWI', ACTION_TYPE = 'ORDER')
If PRINT_TRANSACTION.SET_CD is 'A' then ACTION_HEADER.ACTION_DATE.
Else
Take the maximum
    Recipient Order ACTION_HEADER.ACTION_DATE
    and Donor Order  ACTION_HEADER.ACTION_DATE"
;
;
SELECT TOP 100 ah.SOURCE_NAME, dt.source_name, action_type, paid_date, LINE_ITEM_ORDER_MODIFY_DATE, LINE_ITEM_STATUS,pt.SET_CoDe, ACTION_DATE
FROM prod.action_header ah left join prod.digital_transaction_detail dt on ah.hash_action_id = dt.hash_action_id
inner join prod.PRINT_TRANSACTION pt on pt.hash_action_id = ah.hash_action_id
left join prod.PRINT_TRANSACTION pt2 on ah.addtl_action_id = pt.
WHERE 1=1 and individual_id = 1230617863
;

;
SELECT TOP 100 ah1.SOURCE_NAME,  ah1.action_type,  ah1.action_type, paid_date, LINE_ITEM_ORDER_MODIFY_DATE, LINE_ITEM_STATUS, pt.SET_CoDe, ACTION_DATE
,ah2.*
FROM prod.action_header ah1 inner join prod.action_header ah2 on ah1.addtl_action_id = ah2.addtl_action_id
left join prod.PRINT_TRANSACTION pt on pt.hash_action_id = ah1.hash_action_id or pt.hash_action_id = ah2.hash_action_id
WHERE 1=1 and ah1.individual_id = 1230617863
;
with donor as (select * from prod.PRINT_TRANSACTION pt inner join prod.action_header ah on pt.hash_action_id = ah.hash_action_id where set_code in ('C','E') )
, rec as (select * from prod.PRINT_TRANSACTION pt inner join prod.action_header ah on pt.hash_action_id = ah.hash_action_id where set_code in ('B','D') )

select --donor.AGCY_CODE, donor.AGCY_GRS_VAL_AMOUNT, rec.AGCY_GRS_VAL_AMOUNT, rec.individual_id, donor.individual_id, donor.addtl_action_id, rec.addtl_action_id, donor.action_id, rec.action_id
-- donor.SOURCE_NAME  ,rec.SOURCE_NAME
 donor.ACTION_TYPE  ,rec.ACTION_TYPE
,donor.set_code     ,rec.set_code
,donor.action_date  ,rec.action_date
,donor.*, rec.*
from donor inner join rec on donor.addtl_action_id = rec.addtl_action_id
--and rec.action_id = 1050950000059602015	
--and rec.addtl_action_id = 933800190018 
--and rec.individual_id = 1230617863
and donor.individual_id = 1230617863


;
;
SELECT TOP 100 * FROM prod.agg_credit_card
WHERE individual_id = 1206533762;
SELECT TOP 100 * FROM prod.credit_card cc inner join prod.account acc on cc.hash_account_id = acc.hash_account_id
WHERE individual_id = 1206533762;

SELECT 'NOT XXX' as addr_subtype, count(*)
FROM prod.individual_address
WHERE data_source is null and address_subtype != 'XXX'
union all
SELECT 'XXX' as addr_subtype, count(*)
FROM prod.individual_address
WHERE data_source is null and address_subtype = 'XXX'
union all
SELECT 'ALL' as addr_subtype, count(*)
FROM prod.individual_address
WHERE data_source is null;


SELECT TOP 100 * FROM prod.agg_name_address
WHERE 1=1 and individual_id = 1200273404      ;












