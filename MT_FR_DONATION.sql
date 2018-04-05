;
create table cr_temp.GTT_MT_FR_DONATION as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(TO_CHAR(don_dt,'DD-MON-YY'), TO_CHAR(don_dt,'DD-MON-YY')::text,'(NULL)') as don_dt
, NVL2(trunc(nvl(don_gross_amt,'0')), trunc(nvl(don_gross_amt,'0'))::text,'(NULL)') as don_gross_amt
, NVL2(trunc(nvl(don_amt,'0')), trunc(nvl(don_amt,'0'))::text,'(NULL)') as don_amt
, NVL2(pmt_typ_cd, pmt_typ_cd::text,'(NULL)') as pmt_typ_cd
, NVL2(keycode, keycode::text,'(NULL)') as keycode
, NVL2(trunc(nvl(rf_amt,'0')), trunc(nvl(rf_amt,'0'))::text,'(NULL)') as rf_amt
, NVL2(don_ind, don_ind::text,'(NULL)') as don_ind
, NVL2(don_cd, don_cd::text,'(NULL)') as don_cd
, NVL2(recur_don, recur_don::text,'(NULL)') as recur_don

/*---END---*/

from cr_temp.MT_FR_DONATION T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;

CREATE TABLE cr_temp.gtt_agg_fundraising_donation AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(TO_CHAR(don_dt,'DD-MON-YY'), TO_CHAR(don_dt,'DD-MON-YY')::text,'(NULL)') as don_dt
, NVL2(trunc(nvl(don_gross_amt,'0')), trunc(nvl(don_gross_amt,'0'))::text,'(NULL)') as don_gross_amt
, NVL2(trunc(nvl(don_amt,'0')), trunc(nvl(don_amt,'0'))::text,'(NULL)') as don_amt
, NVL2(pmt_typ_cd, pmt_typ_cd::text,'(NULL)') as pmt_typ_cd
, NVL2(keycode, keycode::text,'(NULL)') as keycode
, NVL2(trunc(nvl(rf_amt,'0')), trunc(nvl(rf_amt,'0'))::text,'(NULL)') as rf_amt
, NVL2(don_ind, don_ind::text,'(NULL)') as don_ind
, NVL2(don_cd, don_cd::text,'(NULL)') as don_cd
, NVL2(recur_don, recur_don::text,'(NULL)') as recur_don

/*---END---*/
from prod.agg_fundraising_donation T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;



------------------------------------------------------------------------TABLE CREATION END\;
CREATE TABLE cr_temp.diff_mt_fr_donation AS
with hash_diff as (

select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID     /*, individual_id*/, don_dt, don_gross_amt, don_amt, pmt_typ_cd, keycode, rf_amt, don_ind, don_cd, recur_don
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL /*, individual_id*/, don_dt, don_gross_amt, don_amt, pmt_typ_cd, keycode, rf_amt, don_ind, don_cd, recur_don
from cr_temp.GTT_MT_FR_DONATION
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL /*, individual_id*/, don_dt, don_gross_amt, don_amt, pmt_typ_cd, keycode, rf_amt, don_ind, don_cd, recur_don
from cr_temp.gtt_agg_fundraising_donation
)

group by ICD_ID, CDH_ID                                                              /*, individual_id*/, don_dt, don_gross_amt, don_amt, pmt_typ_cd, keycode, rf_amt, don_ind, don_cd, recur_don
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select * from hash_diff;

select top 300 * from cr_temp.diff_mt_fr_donation
 where 1=1
;

create table cr_temp.fullmatch_id_agg_fundraising_don 
	DISTSTYLE KEY DISTKEY(cdh_id) 
	INTERLEAVED SORTKEY(cdh_id) 
as 
select distinct cdh_id 
from crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts rm
where not exists (select null from cr_temp.diff_mt_fr_donation ma where ma.cdh_id = rm.cdh_id)
and exists (select null from prod.agg_fundraising_donation ap where ap.individual_id = rm.cdh_id)

;
with icd as (select * from cr_temp.diff_mt_fr_donation where 1=1 and cnt_icd_fl = 1)
,    cdh as (select * from cr_temp.diff_mt_fr_donation where 1=1 and cnt_cdh_fl = 1)
select top 300 icd.cnt_icd_fl||cdh.cnt_cdh_fl as match_fl, icd.icd_id ,cdh.cdh_id
--, case when icd.individual_id = cdh.individual_id then '' else icd.individual_id||'<>'||cdh.individual_id /*#*/end individual_id_diff
, case when icd.don_dt = cdh.don_dt then '' else icd.don_dt||'<>'||cdh.don_dt /*#*/end don_dt_diff
, case when icd.don_gross_amt = cdh.don_gross_amt then '' else icd.don_gross_amt||'<>'||cdh.don_gross_amt /*#*/end don_gross_amt_diff
, case when icd.don_amt = cdh.don_amt then '' else icd.don_amt||'<>'||cdh.don_amt /*#*/end don_amt_diff
, case when icd.pmt_typ_cd = cdh.pmt_typ_cd then '' else icd.pmt_typ_cd||'<>'||cdh.pmt_typ_cd /*#*/end pmt_typ_cd_diff
, case when icd.keycode = cdh.keycode then '' else icd.keycode||'<>'||cdh.keycode /*#*/end keycode_diff
, case when icd.rf_amt = cdh.rf_amt then '' else icd.rf_amt||'<>'||cdh.rf_amt /*#*/end rf_amt_diff
, case when icd.don_ind = cdh.don_ind then '' else icd.don_ind||'<>'||cdh.don_ind /*#*/end don_ind_diff
, case when icd.don_cd = cdh.don_cd then '' else icd.don_cd||'<>'||cdh.don_cd /*#*/end don_cd_diff
, case when icd.recur_don = cdh.recur_don then '' else icd.recur_don||'<>'||cdh.recur_don /*#*/end recur_don_diff


, '||                    ICD                    >>' as sep1, icd.*
, '||                    CDH                    >>' as sep2, cdh.*

from  icd full join cdh
on icd.icd_id = cdh.icd_id and icd.cdh_id = cdh.cdh_id


------------------------------------------------------------------------RUN ALL ABOVE
;
drop table  cr_temp.diff_mt_fr_donation
;
select * from   cr_temp.GTT_MT_FR_DONATION  where icd_id = xxxxxxxxxxxxxxxxxxxxxx
;
select * from   cr_temp.gtt_agg_fundraising_donation  where cdh_id = xxxxxxxxxxxxxxxxxxxxxx
;
select count(*) from   cr_temp.GTT_MT_FR_DONATION
;
select count(*) from   cr_temp.gtt_agg_fundraising_donation
;
--truncate table    cr_temp.GTT_MT_FR_DONATION;
drop table  cr_temp.GTT_MT_FR_DONATION;
--truncate table cr_temp.gtt_agg_fundraising_donation;
drop table  cr_temp.gtt_agg_fundraising_donation;

select *
  from prod.agg_fundraising_donation
 where 1=1 and individual_id = 1218290916
 limit 1000;