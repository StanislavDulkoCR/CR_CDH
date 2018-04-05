;
drop table if exists  cr_temp.GTT_MT_RESP_SUMMARY_0;
create table cr_temp.GTT_MT_RESP_SUMMARY_0 as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/

, NVL2(bus_unt, bus_unt::text,'(NULL)') as bus_unt
, NVL2(keycode, keycode::text,'(NULL)') as keycode
, NVL2(TO_CHAR(prom_dt,'DD-MON-YY'), TO_CHAR(prom_dt,'DD-MON-YY')::text,'(NULL)') as prom_dt
, NVL2(abq_resp_cd, abq_resp_cd::text,'(NULL)') as abq_resp_cd
, NVL2(adv_resp_cd, adv_resp_cd::text,'(NULL)') as adv_resp_cd
, NVL2(TO_CHAR(resp_dt,'DD-MON-YY'), TO_CHAR(resp_dt,'DD-MON-YY')::text,'(NULL)') as resp_dt
, NVL2(aps_resp_cd, aps_resp_cd::text,'(NULL)') as aps_resp_cd
, NVL2(fr_resp_cd, fr_resp_cd::text,'(NULL)') as fr_resp_cd
, NVL2(subs_resp_cd, subs_resp_cd::text,'(NULL)') as subs_resp_cd
, NVL2(crsch_resp_cd, crsch_resp_cd::text,'(NULL)') as crsch_resp_cd
/*---END---*/

from cr_temp.MT_RESP_SUMMARY T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;

drop table if exists  cr_temp.gtt_agg_response_summary;
CREATE TABLE cr_temp.gtt_agg_response_summary AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/

, NVL2(bus_unt, bus_unt::text,'(NULL)') as bus_unt
, NVL2(keycode, keycode::text,'(NULL)') as keycode
, NVL2(TO_CHAR(prom_dt,'DD-MON-YY'), TO_CHAR(prom_dt,'DD-MON-YY')::text,'(NULL)') as prom_dt
, NVL2(abq_resp_cd, abq_resp_cd::text,'(NULL)') as abq_resp_cd
, NVL2(adv_resp_cd, adv_resp_cd::text,'(NULL)') as adv_resp_cd
, NVL2(TO_CHAR(resp_dt,'DD-MON-YY'), TO_CHAR(resp_dt,'DD-MON-YY')::text,'(NULL)') as resp_dt
, NVL2(aps_resp_cd, aps_resp_cd::text,'(NULL)') as aps_resp_cd
, NVL2(fr_resp_cd, fr_resp_cd::text,'(NULL)') as fr_resp_cd
, NVL2(subs_resp_cd, subs_resp_cd::text,'(NULL)') as subs_resp_cd
, NVL2(crsch_resp_cd, crsch_resp_cd::text,'(NULL)') as crsch_resp_cd
/*---END---*/
from prod.agg_response_summary T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID 
where exists (select null from cr_temp.GTT_MT_RESP_SUMMARY_0 mt where mt.cdh_id = T_MAIN.individual_id );



------------------------------------------------------------------------TABLE CREATION END\;
drop table if exists  cr_temp.diff_mt_resp_summary_0;
CREATE TABLE cr_temp.diff_mt_resp_summary_0 AS
with hash_diff as (

select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID     , bus_unt, keycode, prom_dt, abq_resp_cd, adv_resp_cd, resp_dt, aps_resp_cd, fr_resp_cd, subs_resp_cd, crsch_resp_cd
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL , bus_unt, keycode, prom_dt, abq_resp_cd, adv_resp_cd, resp_dt, aps_resp_cd, fr_resp_cd, subs_resp_cd, crsch_resp_cd
from cr_temp.GTT_MT_RESP_SUMMARY_0
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL , bus_unt, keycode, prom_dt, abq_resp_cd, adv_resp_cd, resp_dt, aps_resp_cd, fr_resp_cd, subs_resp_cd, crsch_resp_cd
from cr_temp.gtt_agg_response_summary
)

group by ICD_ID, CDH_ID                                                              , bus_unt, keycode, prom_dt, abq_resp_cd, adv_resp_cd, resp_dt, aps_resp_cd, fr_resp_cd, subs_resp_cd, crsch_resp_cd
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select * from hash_diff hd
-- where exists (select null from xxxxxxxxxxxxxxxxxxxxxxxx fullmatch where hd.cdh_id = fullmatch.cdh_id)
;
select to_char(count(distinct icd_id),'FM999,999,999,990') as unique_icd_id, to_char(count(distinct cdh_id),'FM999,999,999,990') as unique_cdh_id
, to_char(count(*),'FM999,999,999,990') as rows_total_count, to_char(count(*)/2,'FM999,999,999,990') as rowshalf_total_count
, to_char((select count(*) from cr_temp.GTT_MT_RESP_SUMMARY_0),'FM999,999,999,990') as total_ICD
, to_char((select count(*) from cr_temp.gtt_agg_response_summary),'FM999,999,999,990')  as total_CDH 
from cr_temp.diff_mt_resp_summary_0

;
drop table if exists cr_temp.comp_diff_mt_resp_summary_0;
CREATE TABLE cr_temp.comp_diff_mt_resp_summary_0 AS
with icd as (select * from cr_temp.diff_mt_resp_summary_0 where 1=1 and cnt_icd_fl = 1)
,    cdh as (select * from cr_temp.diff_mt_resp_summary_0 where 1=1 and cnt_cdh_fl = 1)
select icd.cnt_icd_fl||cdh.cnt_cdh_fl as match_fl, nvl(icd.icd_id,cdh.icd_id) as icd_id ,cdh.cdh_id

, case when icd.bus_unt = cdh.bus_unt then '' else icd.bus_unt||'<>'||cdh.bus_unt /*#*/end bus_unt_diff
, case when icd.keycode = cdh.keycode then '' else icd.keycode||'<>'||cdh.keycode /*#*/end keycode_diff
, case when icd.prom_dt = cdh.prom_dt then '' else icd.prom_dt||'<>'||cdh.prom_dt /*#*/end prom_dt_diff
, case when icd.abq_resp_cd = cdh.abq_resp_cd then '' else icd.abq_resp_cd||'<>'||cdh.abq_resp_cd /*#*/end abq_resp_cd_diff
, case when icd.adv_resp_cd = cdh.adv_resp_cd then '' else icd.adv_resp_cd||'<>'||cdh.adv_resp_cd /*#*/end adv_resp_cd_diff
, case when icd.resp_dt = cdh.resp_dt then '' else icd.resp_dt||'<>'||cdh.resp_dt /*#*/end resp_dt_diff
, case when icd.aps_resp_cd = cdh.aps_resp_cd then '' else icd.aps_resp_cd||'<>'||cdh.aps_resp_cd /*#*/end aps_resp_cd_diff
, case when icd.fr_resp_cd = cdh.fr_resp_cd then '' else icd.fr_resp_cd||'<>'||cdh.fr_resp_cd /*#*/end fr_resp_cd_diff
, case when icd.subs_resp_cd = cdh.subs_resp_cd then '' else icd.subs_resp_cd||'<>'||cdh.subs_resp_cd /*#*/end subs_resp_cd_diff
, case when icd.crsch_resp_cd = cdh.crsch_resp_cd then '' else icd.crsch_resp_cd||'<>'||cdh.crsch_resp_cd /*#*/end crsch_resp_cd_diff

/*, '||                    ICD                    >>' as sep1/*, icd.* */*/
/*, '||                    CDH                    >>' as sep2/*, cdh.**/*/

from  icd full join cdh
on icd.icd_id = cdh.icd_id and icd.cdh_id = cdh.cdh_id
and icd.bus_unt = cdh.bus_unt and icd.keycode = cdh.keycode
/*additional joins for PK*/
;
select 'MT_RESP_SUMMARY_0' as icd_table_name, 'agg_response_summary' as cdh_table_name

, to_char(sum( case when bus_unt_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/bus_unt
, to_char(sum( case when keycode_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/keycode
, to_char(sum( case when prom_dt_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/prom_dt
, to_char(sum( case when abq_resp_cd_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/abq_resp_cd
, to_char(sum( case when adv_resp_cd_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/adv_resp_cd
, to_char(sum( case when resp_dt_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/resp_dt
, to_char(sum( case when aps_resp_cd_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/aps_resp_cd
, to_char(sum( case when fr_resp_cd_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/fr_resp_cd
, to_char(sum( case when subs_resp_cd_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/subs_resp_cd
, to_char(sum( case when crsch_resp_cd_diff = '' then 0 else 1 end) -1,'FM999,999,999,990') as /*#*/crsch_resp_cd
from cr_temp.comp_diff_mt_resp_summary_0
;
select top 1000 * 


,  bus_unt_diff
,  keycode_diff
,  prom_dt_diff
,  abq_resp_cd_diff
,  adv_resp_cd_diff
,  resp_dt_diff
,  aps_resp_cd_diff
,  fr_resp_cd_diff
,  subs_resp_cd_diff
,  crsch_resp_cd_diff
from cr_temp.comp_diff_mt_resp_summary_0
 where 1=1 and ( 1=2

 or bus_unt_diff != ''
-- or keycode_diff != ''
-- or prom_dt_diff != ''
-- or abq_resp_cd_diff != ''
-- or adv_resp_cd_diff != ''
-- or resp_dt_diff != ''
-- or aps_resp_cd_diff != ''
-- or fr_resp_cd_diff != ''
-- or subs_resp_cd_diff != ''
-- or crsch_resp_cd_diff != ''
)

;
------------------------------------------------------------------------RUN ALL ABOVE


;
select * from   cr_temp.GTT_MT_RESP_SUMMARY_0  where icd_id = 00000000000000000000
;
select * from   cr_temp.gtt_agg_response_summary  where cdh_id = 00000000000000000000











												
