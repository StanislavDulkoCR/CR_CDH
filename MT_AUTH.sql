;
create table cr_temp.GTT_MT_AUTHORIZATION as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(assoc_typ_cd, assoc_typ_cd::text,'(NULL)') as assoc_typ_cd
, NVL2(auth_cd, auth_cd::text,'(NULL)') as auth_cd
, NVL2(scp_cd, scp_cd::text,'(NULL)') as scp_cd
, NVL2(data_source, data_source::text,'(NULL)') as data_source
, NVL2(auth_flg, auth_flg::text,'(NULL)') as auth_flg
, NVL2(TO_CHAR(fst_dt,'DD-MON-YY'), TO_CHAR(fst_dt,'DD-MON-YY')::text,'(NULL)') as fst_dt
, NVL2(TO_CHAR(eff_dt,'DD-MON-YY'), TO_CHAR(eff_dt,'DD-MON-YY')::text,'(NULL)') as eff_dt

/*---END---*/

from cr_temp.MT_AUTHORIZATION_1 T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;

CREATE TABLE cr_temp.gtt_agg_preference1 AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(assoc_typ_cd, assoc_typ_cd::text,'(NULL)') as assoc_typ_cd
, NVL2(auth_cd, auth_cd::text,'(NULL)') as auth_cd
, NVL2(scp_cd, scp_cd::text,'(NULL)') as scp_cd
, NVL2(data_source, data_source::text,'(NULL)') as data_source
, NVL2(auth_flg, auth_flg::text,'(NULL)') as auth_flg
, NVL2(TO_CHAR(fst_dt,'DD-MON-YY'), TO_CHAR(fst_dt,'DD-MON-YY')::text,'(NULL)') as fst_dt
, NVL2(TO_CHAR(eff_dt,'DD-MON-YY'), TO_CHAR(eff_dt,'DD-MON-YY')::text,'(NULL)') as eff_dt

/*---END---*/
from prod.agg_preference T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;



------------------------------------------------------------------------TABLE CREATION END\;
CREATE TABLE cr_temp.diff_MT_AUTHORIZATION AS
with hash_diff as (

select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID     /*, individual_id*/, assoc_typ_cd, auth_cd, scp_cd, data_source, auth_flg, fst_dt, eff_dt
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL /*, individual_id*/, assoc_typ_cd, auth_cd, scp_cd, data_source, auth_flg, fst_dt, eff_dt
from cr_temp.GTT_MT_AUTHORIZATION
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL /*, individual_id*/, assoc_typ_cd, auth_cd, scp_cd, data_source, auth_flg, fst_dt, eff_dt
from cr_temp.gtt_agg_preference1
)

group by ICD_ID, CDH_ID                                                              /*, individual_id*/, assoc_typ_cd, auth_cd, scp_cd, data_source, auth_flg, fst_dt, eff_dt
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select * from hash_diff;

select top 300 * from cr_temp.diff_MT_AUTHORIZATION
 where 1=1
;


with icd as (select * from cr_temp.diff_MT_AUTHORIZATION where 1=1 and cnt_icd_fl = 1)
,    cdh as (select * from cr_temp.diff_MT_AUTHORIZATION where 1=1 and cnt_cdh_fl = 1)
select top 300 icd.cnt_icd_fl||cdh.cnt_cdh_fl as match_fl, nvl(icd.icd_id,cdh.icd_id) as icd_id ,cdh.cdh_id
--, case when icd.individual_id = cdh.individual_id then '' else icd.individual_id||'<>'||cdh.individual_id /*#*/end individual_id_diff
, case when icd.assoc_typ_cd = cdh.assoc_typ_cd then '' else icd.assoc_typ_cd||'<>'||cdh.assoc_typ_cd /*    #*/end assoc_typ_cd_diff
, case when icd.auth_cd = cdh.auth_cd then '' else icd.auth_cd||'<>'||cdh.auth_cd /*                        #*/end auth_cd_diff
, case when icd.scp_cd = cdh.scp_cd then '' else icd.scp_cd||'<>'||cdh.scp_cd /*                            #*/end scp_cd_diff
, case when icd.data_source = cdh.data_source then '' else icd.data_source||'<>'||cdh.data_source /*        #*/end data_source_diff
, case when icd.auth_flg = cdh.auth_flg then '' else icd.auth_flg||'<>'||cdh.auth_flg /*                    #*/end auth_flg_diff
, case when icd.fst_dt = cdh.fst_dt then '' else icd.fst_dt||'<>'||cdh.fst_dt /*                            #*/end fst_dt_diff
, case when icd.eff_dt = cdh.eff_dt then '' else icd.eff_dt||'<>'||cdh.eff_dt /*                            #*/end eff_dt_diff


, '||                    ICD                    >>' as sep1, icd.*
, '||                    CDH                    >>' as sep2, cdh.*

from  icd full join cdh
on icd.icd_id = cdh.icd_id and icd.cdh_id = cdh.cdh_id
and icd.auth_cd = cdh.auth_cd
and icd.scp_cd = cdh.scp_cd
and icd.data_source = cdh.data_source
-- one record per individual per authorization type code per authorization code per authorization scope code per data source where AUTH_FLG is not null.  
------------------------------------------------------------------------RUN ALL ABOVE
;
drop table  cr_temp.diff_MT_AUTHORIZATION
;
select * from   cr_temp.GTT_MT_AUTHORIZATION  where icd_id = 10002000000002070
;
select * from   cr_temp.gtt_agg_preference1  where cdh_id = 1219051292
;
select count(*) from   cr_temp.GTT_MT_AUTHORIZATION
;
select count(*) from   cr_temp.gtt_agg_preference1
;
--truncate table    cr_temp.GTT_MT_AUTHORIZATION;
drop table  cr_temp.GTT_MT_AUTHORIZATION;
--truncate table cr_temp.gtt_agg_preference1;
drop table  cr_temp.gtt_agg_preference1;

;
SELECT TOP 100 *
FROM cr_temp.mt_authorization inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID
WHERE 1=1 and individual_id = 10002000000002070
;
select count(*) from   cr_temp.MT_AUTHORIZATION
;
select count(*) from   cr_temp.MT_AUTHORIZATION_1


