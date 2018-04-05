;
create table cr_temp.GTT_MT_CREDIT_CARD as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(cc_source, cc_source::text,'(NULL)') as cc_source
--, NVL2(lpad(right(cc_acct_num,4),4,0)::text, lpad(right(cc_acct_num,4),4,0)::text,'(NULL)') as cc_acct_num
--, NVL2(cc_type_cd, cc_type_cd::text,'(NULL)') as cc_type_cd
, cc_exp_dt as cc_exp_dt
/*---END---*/
,md5(
ICD_ID||cdh_id
/*--START--*/
/*||individual_id*/
||NVL2(cc_source, cc_source::text,'(NULL)')
--||NVL2(lpad(right(cc_acct_num,4),4,0)::text, lpad(right(cc_acct_num,4),4,0)::text,'(NULL)')
--||NVL2(cc_type_cd, cc_type_cd::text,'(NULL)')
||cc_exp_dt
/*---END---*/
) as hash_value
from cr_temp.MT_CREDIT_CARD T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;
CREATE TABLE cr_temp.gtt_agg_credit_card AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(cc_source, cc_source::text,'(NULL)') as cc_source
--, NVL2(cc_acct_num::text, cc_acct_num::text,'(NULL)') as cc_acct_num
--, NVL2(cc_type_cd, cc_type_cd::text,'(NULL)') as cc_type_cd
, right(cc_exp_dt,2)||right(left(cc_exp_dt,4),2) as cc_exp_dt
/*---END---*/
,md5(
ICD_ID||cdh_id
/*--START--*/
/*||individual_id*/
||NVL2(cc_source, cc_source::text,'(NULL)')
--||NVL2(cc_acct_num, cc_acct_num::text,'(NULL)')
--||NVL2(cc_type_cd, cc_type_cd::text,'(NULL)')
||right(cc_exp_dt,2)||right(left(cc_exp_dt,4),2)
/*---END---*/
) as hash_value
from prod.agg_credit_card T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;
------------------------------------------------------------------------TABLE CREATION END\
with hash_diff as (
select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID, HASH_VALUE
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.GTT_MT_CREDIT_CARD
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.gtt_agg_credit_card
)
group by ICD_ID, CDH_ID, hash_value
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select top 200 * from hash_diff
;
------------------------------------------------------------------------RUN ALL ABOVE
;

select * from   cr_temp.GTT_MT_CREDIT_CARD  where icd_id = 10002000002061080
;
select * from   cr_temp.gtt_agg_credit_card  where cdh_id = 1214980813
;
--truncate table    cr_temp.GTT_MT_CREDIT_CARD;
drop table  cr_temp.GTT_MT_CREDIT_CARD;
--truncate table cr_temp.gtt_agg_credit_card;
drop table  cr_temp.gtt_agg_credit_card;


;
SELECT TOP 100 cc.*
FROM prod.credit_card cc inner join prod.action_header ah on cc.hash_account_id = ah.hash_account_id
WHERE 1=1 and individual_id = 1214980813

;
SELECT TOP 100 right(cc_acct_num,4)
FROM cr_temp.mt_credit_card
WHERE 1=1 and individual_id = 10002000000007110
;
;
SELECT TOP 100 *
FROM prod.agg_credit_card
WHERE 1=1 and individual_id = 1222000478
;
;
SELECT TOP 100 *
FROM cr_temp.mt_credit_card
WHERE 1=1 and individual_id = 10002000000241060
;
select count(*) from  cr_temp.GTT_MT_CREDIT_CARD;
select count(*) from  cr_temp.gtt_agg_credit_card;
