create table cr_temp.manual_agg_account_number
	DISTSTYLE KEY DISTKEY(individual_id) 
	INTERLEAVED SORTKEY(individual_id) 
	as
with man_agg_account_number 
    (individual_id ,hash_account_id ,source_name ,account_subtype_code ,acct_id ,acct_prefix ,acct_num ,first_date ,acct_lst_logn_dt ,acct_logn_cnt ,group_id ,usr_src_ind ,purged_ind) 
as (
select acc.individual_id, acc.hash_account_id, acc.source_name, acc.account_subtype_code, acc.source_account_id, acc.account_subtype_code, acc.source_account_id, acc.first_account_date
	, account_last_login_date, account_login_cnt, group_id, USR_SRC_IND, PURGED_IND
from prod.account acc 
left join (select distinct account_last_login_date, account_login_cnt, hash_account_id , upper(external_user_source) as USR_SRC_IND from prod.digital_account_detail) dad 
													on acc.hash_account_id = dad.hash_account_id and acc.source_name = 'PWI'
left join prod.print_account_detail pad on acc.hash_account_id = pad.hash_account_id and acc.source_name = 'CDS'

left join (select substring(max(to_char(dlg.create_date,'YYYYMMDD')||GROUP_ID),9) as group_id, dlg.hash_account_id, acc.individual_id
from prod.digital_lic_group dlg inner join prod.account acc on dlg.hash_account_id = acc.hash_account_id and acc.account_subtype_code = 'PWI'
where 1=1 and dlg.delete_date is null 
group by  acc.individual_id, dlg.hash_account_id) dlg on dlg.hash_account_id = acc.hash_account_id
)
select *
from man_agg_account_number;
;
create table cr_temp.GTT_MT_ACCOUNT_NUMBER as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(acct_prefix, acct_prefix::text,'(NULL)') as acct_prefix
, NVL2(acct_num, acct_num::text,'(NULL)') as acct_num
, NVL2(first_date, first_date::text,'(NULL)') as first_date
, NVL2(TO_CHAR(acct_lst_logn_dt,'DD-MON-YY'), TO_CHAR(acct_lst_logn_dt,'DD-MON-YY')::text,'(NULL)') as acct_lst_logn_dt
, NVL2(trunc(nvl(acct_logn_cnt,'0')), trunc(nvl(acct_logn_cnt,'0'))::text,'(NULL)') as acct_logn_cnt
, NVL2(group_id, group_id::text,'(NULL)') as group_id
, NVL2(usr_src_ind, usr_src_ind::text,'(NULL)') as usr_src_ind
, NVL2(purged_ind, purged_ind::text,'(NULL)') as purged_ind
/*---END---*/

from cr_temp.MT_ACCOUNT_NUMBER T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;

CREATE TABLE cr_temp.gtt_agg_al_agg_account_numbe AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(acct_prefix, acct_prefix::text,'(NULL)') as acct_prefix
, NVL2(acct_num, acct_num::text,'(NULL)') as acct_num
, NVL2(first_date, first_date::text,'(NULL)') as first_date
, NVL2(TO_CHAR(acct_lst_logn_dt,'DD-MON-YY'), TO_CHAR(acct_lst_logn_dt,'DD-MON-YY')::text,'(NULL)') as acct_lst_logn_dt
, NVL2(trunc(nvl(acct_logn_cnt,'0')), trunc(nvl(acct_logn_cnt,'0'))::text,'(NULL)') as acct_logn_cnt
, NVL2(group_id, group_id::text,'(NULL)') as group_id
, NVL2(usr_src_ind, usr_src_ind::text,'(NULL)') as usr_src_ind
, NVL2(purged_ind, purged_ind::text,'(NULL)') as purged_ind
/*---END---*/
from cr_temp.manual_agg_account_number T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;



------------------------------------------------------------------------TABLE CREATION END\;
CREATE TABLE cr_temp.diff_MT_ACCOUNT_NUMBER AS
with hash_diff as (

select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID     /*, individual_id*/, acct_prefix, acct_num, first_date, acct_lst_logn_dt, acct_logn_cnt, group_id, usr_src_ind, purged_ind
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL /*, individual_id*/, acct_prefix, acct_num, first_date, acct_lst_logn_dt, acct_logn_cnt, group_id, usr_src_ind, purged_ind
from cr_temp.GTT_MT_ACCOUNT_NUMBER
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL /*, individual_id*/, acct_prefix, acct_num, first_date, acct_lst_logn_dt, acct_logn_cnt, group_id, usr_src_ind, purged_ind
from cr_temp.gtt_agg_al_agg_account_numbe
)

group by ICD_ID, CDH_ID                                                              /*, individual_id*/, acct_prefix, acct_num, first_date, acct_lst_logn_dt, acct_logn_cnt, group_id, usr_src_ind, purged_ind
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select * from hash_diff;

select top 300 * from cr_temp.diff_MT_ACCOUNT_NUMBER
 where 1=1
;


with icd as (select * from cr_temp.diff_MT_ACCOUNT_NUMBER where 1=1 and cnt_icd_fl = 1)
,    cdh as (select * from cr_temp.diff_MT_ACCOUNT_NUMBER where 1=1 and cnt_cdh_fl = 1 and acct_prefix not in ('DMP','KBL','ABQ','ANH','LSR','ANH'))
select top 300 icd.cnt_icd_fl||cdh.cnt_cdh_fl as match_fl, icd.icd_id ,cdh.cdh_id
--, case when icd.individual_id = cdh.individual_id then '' else icd.individual_id||'<>'||cdh.individual_id /*#*/end individual_id_diff
, case when icd.acct_prefix = cdh.acct_prefix then '' else icd.acct_prefix||'<>'||cdh.acct_prefix /*#*/end acct_prefix_diff
, case when icd.acct_num = cdh.acct_num then '' else icd.acct_num||'<>'||cdh.acct_num /*#*/end acct_num_diff
, case when icd.first_date = cdh.first_date then '' else icd.first_date||'<>'||cdh.first_date /*#*/end first_date_diff
, case when icd.acct_lst_logn_dt = cdh.acct_lst_logn_dt then '' else icd.acct_lst_logn_dt||'<>'||cdh.acct_lst_logn_dt /*#*/end acct_lst_logn_dt_diff
, case when icd.acct_logn_cnt = cdh.acct_logn_cnt then '' else icd.acct_logn_cnt||'<>'||cdh.acct_logn_cnt /*#*/end acct_logn_cnt_diff
, case when icd.group_id = cdh.group_id then '' else icd.group_id||'<>'||cdh.group_id /*#*/end group_id_diff
, case when icd.usr_src_ind = cdh.usr_src_ind then '' else icd.usr_src_ind||'<>'||cdh.usr_src_ind /*#*/end usr_src_ind_diff
, case when icd.purged_ind = cdh.purged_ind then '' else icd.purged_ind||'<>'||cdh.purged_ind /*#*/end purged_ind_diff

, '||                    ICD                    >>' as sep1, icd.*
, '||                    CDH                    >>' as sep2, cdh.*

from  icd full join cdh
on icd.icd_id = cdh.icd_id and icd.cdh_id = cdh.cdh_id
and icd.acct_num = cdh.acct_num
and icd.acct_prefix = cdh.acct_prefix


------------------------------------------------------------------------RUN ALL ABOVE
;
drop table  cr_temp.diff_MT_ACCOUNT_NUMBER
;
select * from   cr_temp.GTT_MT_ACCOUNT_NUMBER  where icd_id = xxxxxxxxxxxxxxxxxxxxxx
;
select * from   cr_temp.gtt_agg_al_agg_account_numbe  where cdh_id = xxxxxxxxxxxxxxxxxxxxxx
;
select count(*) from   cr_temp.GTT_MT_ACCOUNT_NUMBER
;
select count(*) from   cr_temp.gtt_agg_al_agg_account_numbe
;
--truncate table    cr_temp.GTT_MT_ACCOUNT_NUMBER;
drop table  cr_temp.GTT_MT_ACCOUNT_NUMBER;
--truncate table cr_temp.gtt_agg_al_agg_account_numbe;
drop table  cr_temp.gtt_agg_al_agg_account_numbe;

select *
from cr_temp.manual_agg_account_number
where 1=1 and individual_id  = 1202470523
limit 1000; 