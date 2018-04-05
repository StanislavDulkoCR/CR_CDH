drop table if exists cr_temp.manual_agg_account_number;
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
where 1=1 and dlg.delete_date is null and individual_id = 1201101565
group by  acc.individual_id, dlg.hash_account_id) dlg on dlg.hash_account_id = acc.hash_account_id
)
select *
from man_agg_account_number;

select count(i), count(c), individual_id ,hash_account_id ,source_name ,account_subtype_code ,acct_id ,acct_prefix ,acct_num ,first_date 
	,acct_lst_logn_dt ,acct_logn_cnt ,group_id ,usr_src_ind ,purged_ind
	
	from (
select 1 as i, null as c, individual_id ,hash_account_id ,source_name ,account_subtype_code ,acct_id ,acct_prefix ,acct_num ,first_date 
	,acct_lst_logn_dt ,acct_logn_cnt ,group_id ,usr_src_ind ,purged_ind
	from cr_temp.manual_agg_account_number
	union all
select null as i, 1 as c, individual_id ,hash_account_id ,source_name ,account_subtype_code ,acct_id ,acct_prefix ,acct_num ,first_date 
	,acct_lst_logn_dt ,acct_logn_cnt ,group_id ,usr_src_ind ,purged_ind
	from prod.agg_account_number
	)
	group by individual_id ,hash_account_id ,source_name ,account_subtype_code ,acct_id ,acct_prefix ,acct_num ,first_date 
	,acct_lst_logn_dt ,acct_logn_cnt ,group_id ,usr_src_ind ,purged_ind
	having count(i) != count(c)
	order by individual_id, COUNT(i) desc, COUNT(c) desc
	
	
	;
	
	
select *
from prod.account
where 1=1 and individual_id = 1200003672
limit 1000;
select *
from cr_temp.manual_agg_account_number
where 1=1 and individual_id = 1200003672
limit 1000;


select max(to_char(dlg.create_date,'YYYYMMDD')||GROUP_ID) as max_hash_group, dlg.hash_account_id, acc.individual_id
from prod.digital_lic_group dlg inner join prod.account acc on dlg.hash_account_id = acc.hash_account_id and acc.account_subtype_code = 'PWI'
where 1=1 and dlg.delete_date is null and individual_id = 1202470523
group by  acc.individual_id, dlg.hash_account_id
;
select *
from prod.account
where 1=1 and hash_account_id = 'FF9170EC1DF74984A04C229302C8D38A'
limit 1000;