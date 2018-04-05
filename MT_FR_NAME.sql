with fr_raw ( INDIVIDUAL_ID ,SALUTATION ,FIRST_NAME ,MIDDLE_NAME ,LAST_NAME ,SUFFIX) as (

SELECT nvl(fv.individual_id, anaf.individual_id)
, NVL(fv.fundraising_salutation_formal, anaf.salutation)
, nvl(fv.fundraising_first_name, anaf.first_name)
, nvl(fv.fundraising_middle_name, anaf.middle_name)
, nvl(fv.fundraising_last_name, anaf.last_name)
, nvl(fv.fundraising_suffix_name, anaf.suffix)
FROM prod.fundraising_vanity_name fv --5607581
full join (
select *
from prod.agg_name_address ana --1567785
where 1=1
and exists (SELECT null
			FROM prod.agg_fundraising_summary afs
			WHERE 1=1 and afs.individual_id = ana.individual_id
			and not exists (SELECT null
							FROM prod.fundraising_vanity_name fvn
							WHERE 1=1 and fvn.individual_id = afs.individual_id
						   )
		   )
) anaf on fv.individual_id = anaf.individual_id


), fr_fin as (
select *
from fr_raw
WHERE 1=1 
order by 1 asc)

, diff as (
select * from (
select * from fr_fin
union all
select * from prod.agg_fundraising_name
)
group by  INDIVIDUAL_ID ,SALUTATION ,FIRST_NAME ,MIDDLE_NAME ,LAST_NAME ,SUFFIX
having count(*) = 1 
order by 1)

--select top 20 * from diff;
select fin.*, afn.* from fr_fin fin inner join prod.agg_fundraising_name afn 
on fin.individual_id = afn.individual_id

and fin.individual_id in (  --exists in vanity_name
 1200344934
,1200817379
,1200435795
,1200844758)

;
;
SELECT TOP 100 *
FROM prod.agg_name_address
WHERE 1=1 and individual_id in (  --exists in vanity_name
 1200021282
,1200011456
,1200017867
,1200020822
,1200021154
,1200000059
,1200000405
,1200001863
,1200002043
,1200002570
--not exists
,1200224380
,1200027437
,1200015326
,1200197724
,1200201456
,1200305763
,1200367360
,1200018453
,1200038692
,1200000007)

;
;
SELECT TOP 100 *
FROM prod.agg_name_address
WHERE 1=1 and salutation like '%CAP%'

;
SELECT count(*)
FROM prod.agg_fundraising_name
																order by 1 asc
	;
	
SELECT count(*)--fv.individual_id
left join (
select *
from prod.agg_name_address ana --1567785
where 1=1
and exists (SELECT null
FROM prod.fundraising_vanity_name fvn
WHERE 1=1 and fvn.individual_id = ana.individual_id)  
and not exists (SELECT null
FROM prod.agg_fundraising_summary afs
WHERE 1=1 and afs.individual_id = ana.individual_id)
	
;
select count(*)
from prod.agg_name_address ana --1567785
where 1=1
and exists (SELECT null
			FROM prod.agg_fundraising_summary afs
			WHERE 1=1 and afs.individual_id = ana.individual_id
			and not exists (SELECT null
							FROM prod.fundraising_vanity_name fvn
							WHERE 1=1 and fvn.individual_id = afs.individual_id
						   ))
;

SELECT count(*)
FROM prod.agg_fundraising_summary  mfs --5102084
;
SELECT count(*)
FROM prod.agg_fundraising_summary  mfs --1567785
WHERE 1=1
and not exists ( select null from prod.fundraising_vanity_name fn where fn.INDIVIDUAL_ID = mfs.INDIVIDUAL_ID )

;
