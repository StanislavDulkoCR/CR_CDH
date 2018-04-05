;
create table cr_temp.GTT_MT_NAME_ADDRESS as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
--, NVL2(hh_id, hh_id::text,'(NULL)') as hh_id
--, NVL2(osl_hh_id, osl_hh_id::text,'(NULL)') as osl_hh_id
--, NVL2(account_num, account_num::text,'(NULL)') as account_num
--, NVL2(pwi_account_num, pwi_account_num::text,'(NULL)') as pwi_account_num
--, NVL2(address_id, address_id::text,'(NULL)') as address_id
, NVL2(salutation, salutation::text,'(NULL)') as salutation
, NVL2(first_name, first_name::text,'(NULL)') as first_name
, NVL2(middle_name, middle_name::text,'(NULL)') as middle_name
, NVL2(last_name, last_name::text,'(NULL)') as last_name
--, NVL2(suffix, suffix::text,'(NULL)') as suffix
--, NVL2(business_name, business_name::text,'(NULL)') as business_name
--, NVL2(primary_number, primary_number::text,'(NULL)') as primary_number
--, NVL2(pre_directional, pre_directional::text,'(NULL)') as pre_directional
--, NVL2(street, street::text,'(NULL)') as street
--, NVL2(street_suffix, street_suffix::text,'(NULL)') as street_suffix
--, NVL2(post_directional, post_directional::text,'(NULL)') as post_directional
--, NVL2(unit_designator, unit_designator::text,'(NULL)') as unit_designator
--, NVL2(secondary_number, secondary_number::text,'(NULL)') as secondary_number
--, NVL2(additional_address_data, additional_address_data::text,'(NULL)') as additional_address_data
--, NVL2(city, city::text,'(NULL)') as city
--, NVL2(state_province, state_province::text,'(NULL)') as state_province
--, NVL2(postal_code, postal_code::text,'(NULL)') as postal_code
--, NVL2(scf_fsa, scf_fsa::text,'(NULL)') as scf_fsa
--, NVL2(country_id, country_id::text,'(NULL)') as country_id
--, NVL2(can_flg, can_flg::text,'(NULL)') as can_flg
--, NVL2(usa_flg, usa_flg::text,'(NULL)') as usa_flg
--, NVL2(foreign_flg, foreign_flg::text,'(NULL)') as foreign_flg
--, NVL2(usa_deliverable_flg, usa_deliverable_flg::text,'(NULL)') as usa_deliverable_flg
--, NVL2(for_deliverable_flg, for_deliverable_flg::text,'(NULL)') as for_deliverable_flg
--, NVL2(dpbc, dpbc::text,'(NULL)') as dpbc
--, NVL2(crc, crc::text,'(NULL)') as crc
--, NVL2(lot, lot::text,'(NULL)') as lot
--, NVL2(dsf_delivery_type, dsf_delivery_type::text,'(NULL)') as dsf_delivery_type
--, NVL2(dsf_deliverability_code, dsf_deliverability_code::text,'(NULL)') as dsf_deliverability_code
--, NVL2(gcdi_deliverability_code, gcdi_deliverability_code::text,'(NULL)') as gcdi_deliverability_code
--, NVL2(dsf_match_flag, dsf_match_flag::text,'(NULL)') as dsf_match_flag
--, NVL2(dsf_vacancy_indicator, dsf_vacancy_indicator::text,'(NULL)') as dsf_vacancy_indicator
--, NVL2(dsf_seasonal_indicator, dsf_seasonal_indicator::text,'(NULL)') as dsf_seasonal_indicator
--, NVL2(dsf_drop_point_indicator, dsf_drop_point_indicator::text,'(NULL)') as dsf_drop_point_indicator
--, NVL2(dsf_record_type, dsf_record_type::text,'(NULL)') as dsf_record_type
--, NVL2(lacs_indicator, lacs_indicator::text,'(NULL)') as lacs_indicator
--, NVL2(edit_fail_flg, edit_fail_flg::text,'(NULL)') as edit_fail_flg
--, NVL2(coa_source, coa_source::text,'(NULL)') as coa_source
--, NVL2(coa_date, coa_date::text,'(NULL)') as coa_date
--, NVL2(ncoa_move_type, ncoa_move_type::text,'(NULL)') as ncoa_move_type
--, NVL2(ncoa_category, ncoa_category::text,'(NULL)') as ncoa_category
--, NVL2(ncoa_footnote, ncoa_footnote::text,'(NULL)') as ncoa_footnote
--, NVL2(TO_CHAR(ncoa_move_eff_dt,'DD-MON-YY'), TO_CHAR(ncoa_move_eff_dt,'DD-MON-YY')::text,'(NULL)') as ncoa_move_eff_dt
--, NVL2(address_type, address_type::text,'(NULL)') as address_type
--, NVL2(tel_valid_flag, tel_valid_flag::text,'(NULL)') as tel_valid_flag
--, NVL2(telecom_nbr, telecom_nbr::text,'(NULL)') as telecom_nbr
--, NVL2(tel_area_code, tel_area_code::text,'(NULL)') as tel_area_code
--, NVL2(tel_prefix, tel_prefix::text,'(NULL)') as tel_prefix
--, NVL2(mobile_telecom_nbr, mobile_telecom_nbr::text,'(NULL)') as mobile_telecom_nbr
--, NVL2(trunc(nvl(coa_cnt,'0')), trunc(nvl(coa_cnt,'0'))::text,'(NULL)') as coa_cnt
/*---END---*/
,md5(
ICD_ID||cdh_id
/*--START--*/
/*||individual_id*/
--||NVL2(hh_id, hh_id::text,'(NULL)')
--||NVL2(osl_hh_id, osl_hh_id::text,'(NULL)')
--||NVL2(account_num, account_num::text,'(NULL)')
--||NVL2(pwi_account_num, pwi_account_num::text,'(NULL)')
--||NVL2(address_id, address_id::text,'(NULL)')
||NVL2(salutation, salutation::text,'(NULL)')
||NVL2(first_name, first_name::text,'(NULL)')
||NVL2(middle_name, middle_name::text,'(NULL)')
||NVL2(last_name, last_name::text,'(NULL)')
--||NVL2(suffix, suffix::text,'(NULL)')
--||NVL2(business_name, business_name::text,'(NULL)')
--||NVL2(primary_number, primary_number::text,'(NULL)')
--||NVL2(pre_directional, pre_directional::text,'(NULL)')
--||NVL2(street, street::text,'(NULL)')
--||NVL2(street_suffix, street_suffix::text,'(NULL)')
--||NVL2(post_directional, post_directional::text,'(NULL)')
--||NVL2(unit_designator, unit_designator::text,'(NULL)')
--||NVL2(secondary_number, secondary_number::text,'(NULL)')
--||NVL2(additional_address_data, additional_address_data::text,'(NULL)')
--||NVL2(city, city::text,'(NULL)')
--||NVL2(state_province, state_province::text,'(NULL)')
--||NVL2(postal_code, postal_code::text,'(NULL)')
--||NVL2(scf_fsa, scf_fsa::text,'(NULL)')
--||NVL2(country_id, country_id::text,'(NULL)')
--||NVL2(can_flg, can_flg::text,'(NULL)')
--||NVL2(usa_flg, usa_flg::text,'(NULL)')
--||NVL2(foreign_flg, foreign_flg::text,'(NULL)')
--||NVL2(usa_deliverable_flg, usa_deliverable_flg::text,'(NULL)')
--||NVL2(for_deliverable_flg, for_deliverable_flg::text,'(NULL)')
--||NVL2(dpbc, dpbc::text,'(NULL)')
--||NVL2(crc, crc::text,'(NULL)')
--||NVL2(lot, lot::text,'(NULL)')
--||NVL2(dsf_delivery_type, dsf_delivery_type::text,'(NULL)')
--||NVL2(dsf_deliverability_code, dsf_deliverability_code::text,'(NULL)')
--||NVL2(gcdi_deliverability_code, gcdi_deliverability_code::text,'(NULL)')
--||NVL2(dsf_match_flag, dsf_match_flag::text,'(NULL)')
--||NVL2(dsf_vacancy_indicator, dsf_vacancy_indicator::text,'(NULL)')
--||NVL2(dsf_seasonal_indicator, dsf_seasonal_indicator::text,'(NULL)')
--||NVL2(dsf_drop_point_indicator, dsf_drop_point_indicator::text,'(NULL)')
--||NVL2(dsf_record_type, dsf_record_type::text,'(NULL)')
--||NVL2(lacs_indicator, lacs_indicator::text,'(NULL)')
--||NVL2(edit_fail_flg, edit_fail_flg::text,'(NULL)')
--||NVL2(coa_source, coa_source::text,'(NULL)')
--||NVL2(coa_date, coa_date::text,'(NULL)')
--||NVL2(ncoa_move_type, ncoa_move_type::text,'(NULL)')
--||NVL2(ncoa_category, ncoa_category::text,'(NULL)')
--||NVL2(ncoa_footnote, ncoa_footnote::text,'(NULL)')
--||NVL2(TO_CHAR(ncoa_move_eff_dt,'DD-MON-YY'), TO_CHAR(ncoa_move_eff_dt,'DD-MON-YY')::text,'(NULL)')
--||NVL2(address_type, address_type::text,'(NULL)')
--||NVL2(tel_valid_flag, tel_valid_flag::text,'(NULL)')
--||NVL2(telecom_nbr, telecom_nbr::text,'(NULL)')
--||NVL2(tel_area_code, tel_area_code::text,'(NULL)')
--||NVL2(tel_prefix, tel_prefix::text,'(NULL)')
--||NVL2(mobile_telecom_nbr, mobile_telecom_nbr::text,'(NULL)')
--||NVL2(trunc(nvl(coa_cnt,'0')), trunc(nvl(coa_cnt,'0'))::text,'(NULL)')
/*---END---*/
) as hash_value
from cr_temp.MT_NAME_ADDRESS T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;
CREATE TABLE cr_temp.gtt_agg_name_address AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
--, NVL2(hh_id, hh_id::text,'(NULL)') as hh_id
--, NVL2(osl_hh_id, osl_hh_id::text,'(NULL)') as osl_hh_id
--, NVL2(account_num, account_num::text,'(NULL)') as account_num
--, NVL2(pwi_account_num, pwi_account_num::text,'(NULL)') as pwi_account_num
--, NVL2(address_id, address_id::text,'(NULL)') as address_id
, NVL2(salutation, salutation::text,'(NULL)') as salutation
, NVL2(first_name, first_name::text,'(NULL)') as first_name
, NVL2(middle_name, middle_name::text,'(NULL)') as middle_name
, NVL2(last_name, last_name::text,'(NULL)') as last_name
--, NVL2(suffix, suffix::text,'(NULL)') as suffix
--, NVL2(business_name, business_name::text,'(NULL)') as business_name
--, NVL2(primary_number, primary_number::text,'(NULL)') as primary_number
--, NVL2(pre_directional, pre_directional::text,'(NULL)') as pre_directional
--, NVL2(street, street::text,'(NULL)') as street
--, NVL2(street_suffix, street_suffix::text,'(NULL)') as street_suffix
--, NVL2(post_directional, post_directional::text,'(NULL)') as post_directional
--, NVL2(unit_designator, unit_designator::text,'(NULL)') as unit_designator
--, NVL2(secondary_number, secondary_number::text,'(NULL)') as secondary_number
--, NVL2(additional_address_data, additional_address_data::text,'(NULL)') as additional_address_data
--, NVL2(city, city::text,'(NULL)') as city
--, NVL2(state_province, state_province::text,'(NULL)') as state_province
--, NVL2(postal_code, postal_code::text,'(NULL)') as postal_code
--, NVL2(scf_fsa, scf_fsa::text,'(NULL)') as scf_fsa
--, NVL2(country_id, country_id::text,'(NULL)') as country_id
--, NVL2(can_flg, can_flg::text,'(NULL)') as can_flg
--, NVL2(usa_flg, usa_flg::text,'(NULL)') as usa_flg
--, NVL2(foreign_flg, foreign_flg::text,'(NULL)') as foreign_flg
--, NVL2(usa_deliverable_flg, usa_deliverable_flg::text,'(NULL)') as usa_deliverable_flg
--, NVL2(for_deliverable_flg, for_deliverable_flg::text,'(NULL)') as for_deliverable_flg
--, NVL2(dpbc, dpbc::text,'(NULL)') as dpbc
--, NVL2(crc, crc::text,'(NULL)') as crc
--, NVL2(lot, lot::text,'(NULL)') as lot
--, NVL2(dsf_delivery_type, dsf_delivery_type::text,'(NULL)') as dsf_delivery_type
--, NVL2(dsf_deliverability_code, dsf_deliverability_code::text,'(NULL)') as dsf_deliverability_code
--, NVL2(gcdi_deliverability_code, gcdi_deliverability_code::text,'(NULL)') as gcdi_deliverability_code
--, NVL2(dsf_match_flag, dsf_match_flag::text,'(NULL)') as dsf_match_flag
--, NVL2(dsf_vacancy_indicator, dsf_vacancy_indicator::text,'(NULL)') as dsf_vacancy_indicator
--, NVL2(dsf_seasonal_indicator, dsf_seasonal_indicator::text,'(NULL)') as dsf_seasonal_indicator
--, NVL2(dsf_drop_point_indicator, dsf_drop_point_indicator::text,'(NULL)') as dsf_drop_point_indicator
--, NVL2(dsf_record_type, dsf_record_type::text,'(NULL)') as dsf_record_type
--, NVL2(lacs_indicator, lacs_indicator::text,'(NULL)') as lacs_indicator
--, NVL2(edit_fail_flg, edit_fail_flg::text,'(NULL)') as edit_fail_flg
--, NVL2(coa_source, coa_source::text,'(NULL)') as coa_source
--, NVL2(coa_date, coa_date::text,'(NULL)') as coa_date
--, NVL2(ncoa_move_type, ncoa_move_type::text,'(NULL)') as ncoa_move_type
--, NVL2(ncoa_category, ncoa_category::text,'(NULL)') as ncoa_category
--, NVL2(ncoa_footnote, ncoa_footnote::text,'(NULL)') as ncoa_footnote
--, NVL2(TO_CHAR(ncoa_move_eff_dt,'DD-MON-YY'), TO_CHAR(ncoa_move_eff_dt,'DD-MON-YY')::text,'(NULL)') as ncoa_move_eff_dt
--, NVL2(address_type, address_type::text,'(NULL)') as address_type
--, NVL2(tel_valid_flag, tel_valid_flag::text,'(NULL)') as tel_valid_flag
--, NVL2(telecom_nbr, telecom_nbr::text,'(NULL)') as telecom_nbr
--, NVL2(tel_area_code, tel_area_code::text,'(NULL)') as tel_area_code
--, NVL2(tel_prefix, tel_prefix::text,'(NULL)') as tel_prefix
--, NVL2(mobile_telecom_nbr, mobile_telecom_nbr::text,'(NULL)') as mobile_telecom_nbr
--, NVL2(trunc(nvl(coa_cnt,'0')), trunc(nvl(coa_cnt,'0'))::text,'(NULL)') as coa_cnt
/*---END---*/
,md5(
ICD_ID||cdh_id
/*--START--*/
/*||individual_id*/
--||NVL2(hh_id, hh_id::text,'(NULL)')
--||NVL2(osl_hh_id, osl_hh_id::text,'(NULL)')
--||NVL2(account_num, account_num::text,'(NULL)')
--||NVL2(pwi_account_num, pwi_account_num::text,'(NULL)')
--||NVL2(address_id, address_id::text,'(NULL)')
||NVL2(salutation, salutation::text,'(NULL)')
||NVL2(first_name, first_name::text,'(NULL)')
||NVL2(middle_name, middle_name::text,'(NULL)')
||NVL2(last_name, last_name::text,'(NULL)')
--||NVL2(suffix, suffix::text,'(NULL)')
--||NVL2(business_name, business_name::text,'(NULL)')
--||NVL2(primary_number, primary_number::text,'(NULL)')
--||NVL2(pre_directional, pre_directional::text,'(NULL)')
--||NVL2(street, street::text,'(NULL)')
--||NVL2(street_suffix, street_suffix::text,'(NULL)')
--||NVL2(post_directional, post_directional::text,'(NULL)')
--||NVL2(unit_designator, unit_designator::text,'(NULL)')
--||NVL2(secondary_number, secondary_number::text,'(NULL)')
--||NVL2(additional_address_data, additional_address_data::text,'(NULL)')
--||NVL2(city, city::text,'(NULL)')
--||NVL2(state_province, state_province::text,'(NULL)')
--||NVL2(postal_code, postal_code::text,'(NULL)')
--||NVL2(scf_fsa, scf_fsa::text,'(NULL)')
--||NVL2(country_id, country_id::text,'(NULL)')
--||NVL2(can_flg, can_flg::text,'(NULL)')
--||NVL2(usa_flg, usa_flg::text,'(NULL)')
--||NVL2(foreign_flg, foreign_flg::text,'(NULL)')
--||NVL2(usa_deliverable_flg, usa_deliverable_flg::text,'(NULL)')
--||NVL2(for_deliverable_flg, for_deliverable_flg::text,'(NULL)')
--||NVL2(dpbc, dpbc::text,'(NULL)')
--||NVL2(crc, crc::text,'(NULL)')
--||NVL2(lot, lot::text,'(NULL)')
--||NVL2(dsf_delivery_type, dsf_delivery_type::text,'(NULL)')
--||NVL2(dsf_deliverability_code, dsf_deliverability_code::text,'(NULL)')
--||NVL2(gcdi_deliverability_code, gcdi_deliverability_code::text,'(NULL)')
--||NVL2(dsf_match_flag, dsf_match_flag::text,'(NULL)')
--||NVL2(dsf_vacancy_indicator, dsf_vacancy_indicator::text,'(NULL)')
--||NVL2(dsf_seasonal_indicator, dsf_seasonal_indicator::text,'(NULL)')
--||NVL2(dsf_drop_point_indicator, dsf_drop_point_indicator::text,'(NULL)')
--||NVL2(dsf_record_type, dsf_record_type::text,'(NULL)')
--||NVL2(lacs_indicator, lacs_indicator::text,'(NULL)')
--||NVL2(edit_fail_flg, edit_fail_flg::text,'(NULL)')
--||NVL2(coa_source, coa_source::text,'(NULL)')
--||NVL2(coa_date, coa_date::text,'(NULL)')
--||NVL2(ncoa_move_type, ncoa_move_type::text,'(NULL)')
--||NVL2(ncoa_category, ncoa_category::text,'(NULL)')
--||NVL2(ncoa_footnote, ncoa_footnote::text,'(NULL)')
--||NVL2(TO_CHAR(ncoa_move_eff_dt,'DD-MON-YY'), TO_CHAR(ncoa_move_eff_dt,'DD-MON-YY')::text,'(NULL)')
--||NVL2(address_type, address_type::text,'(NULL)')
--||NVL2(tel_valid_flag, tel_valid_flag::text,'(NULL)')
--||NVL2(telecom_nbr, telecom_nbr::text,'(NULL)')
--||NVL2(tel_area_code, tel_area_code::text,'(NULL)')
--||NVL2(tel_prefix, tel_prefix::text,'(NULL)')
--||NVL2(mobile_telecom_nbr, mobile_telecom_nbr::text,'(NULL)')
--||NVL2(trunc(nvl(coa_cnt,'0')), trunc(nvl(coa_cnt,'0'))::text,'(NULL)')
/*---END---*/
) as hash_value
from prod.agg_name_address T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;
------------------------------------------------------------------------TABLE CREATION END\
with hash_diff as (
select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID, HASH_VALUE
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.GTT_MT_NAME_ADDRESS
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.gtt_agg_name_address
)
group by ICD_ID, CDH_ID, hash_value
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select top 50 * from hash_diff;
------------------------------------------------------------------------RUN ALL ABOVE
;
select * from   cr_temp.GTT_MT_NAME_ADDRESS  where icd_id = 10002000000274080
;
select * from   cr_temp.gtt_agg_name_address  where cdh_id = 1200938550
;
SELECT * from prod.individual_address WHERE individual_id = 1202550522
;
--truncate table    cr_temp.GTT_MT_NAME_ADDRESS;
drop table  cr_temp.GTT_MT_NAME_ADDRESS;
--truncate table cr_temp.gtt_agg_name_address;
drop table  cr_temp.gtt_agg_name_address;

;
Pos. 1-3 of prod.individual_address.acx_zipcode where prod.individual_address.acx_country_code = 'USA' or 'CAN' 
;
SELECT TOP 100 *
FROM prod.agg_name_address
WHERE 1=1 and individual_id = 1203816315

;
SELECT acx_ef_record, *
--distinct TO_CHAR(update_date,'DD-MON-YY')
FROM prod.individual_address 
WHERE 1=1 and individual_id = 1219101386
;
select individual_id, acx_ef_record
from prod.individual_address
where individual_id IN (1203179758);
;
Table Level rule:  Records from the Data Warehouse NAME_ADDRESS_REF table 
where NAME_ADDRESS_REF.ADDRESS_ID = TRANS_CLASS_3.ADDRESS_ID and NAME_ADDRESS_REF.INDIVIDUAL_ID = TRANS_CLASS_3.INDIVIDUAL_ID 
keeping the NAME_ADDRESS_REF row 
where NAME_ADDRESS_REF.NAME_ADDRESS_DIFF is ‘CNS’, ‘CRH’, ‘CRM’, 'SHM', 'DMP', 'ENN', 'CCC', 'PWI', or 'CDB' 
as the highest priority using the maximum MAINTENANCE_DATE as a tiebreaker.  

CDS, CU, KBL, LSR, DCK, CGA, CVO, and old CDS CRE/CRT are all at the same level (2nd Highest), 
with maximum MAINTENANCE_DATE as a tiebreaker among them.  OSE is at the lowest level.									

;
SELECT TOP 100 *
FROM prod.agg_name_address an
WHERE 1=1 --and individual_id = 1220570993    
and exists (select null from prod.individual_address ia where an.individual_id = ia.individual_id and an.address_id = ia.address_id and ia.favorite_address_flag != 'Y')
;
;
SELECT TOP 100 *
FROM prod.agg_email 
WHERE 1=1
and email_type_cd = 'N'
--and individual_id =1220570993
;
;

select count(*) --, data_source
from prod.agg_EMAIL
WHERE 1=1 --group by data_source


;
;
SELECT TOP 100 *
FROM prod.coa
WHERE 1=1 and individual_id = 1230492231
;
SELECT TOP 100 ina.acx_full_name, acx_dsf_address_type FROM prod.individual ina inner join prod.individual_address ia on ina.individual_id = ia.individual_id
WHERE ia.individual_id = 1220021119;
;
SELECT TOP 100 * FROM prod.individual_address
WHERE individual_id = 1220021119;

SELECT TOP 100 * FROM prod.agg_name_address 
WHERE individual_id = 1232031188;
prod.individual	acx_full_name
;
;
SELECT TOP 100 acx_full_name
FROM prod.individual
WHERE 1=1 and acx_full_name is not null
;
if prod.individual_address.acx_dsf_address_type = 'B' then prod.individual.acx_full_name
;
;
SELECT TOP 100 acx_secondary_unit_designator, acx_secondary_num
FROM prod.match_instance mi
WHERE 1=1 and acx_rm_new_individual_id = 1210820356
;
select count(*) FROM MIDDLE_TIER2.mt_online_item;
--COUNT(*) = 91,657,601
SELECT  count(*), stat_cd, mag_cd FROM prod.agg_digital_item group by stat_cd, mag_cd;
--COUNT(*) = 51,447,594
;
SELECT  count(*) FROM prod.agg_digital_item;
;
SELECT TOP 100 *
FROM prod.agg_name_address
WHERE 1=1 and individual_id in (1225658174
,1226207731
,1223349439
,1226110355)
;
Example:
SELECT TOP 100 * FROM prod.individual_address
WHERE individual_id =1205450921;

SELECT count(*) 
FROM prod.individual_address ia
WHERE not exists (
					SELECT null
					FROM prod.agg_name_address ana
					WHERE 1=1 and ana.individual_id = ia.individual_id
)

;
"Code as 'N' where prod.agg_name_address.GCDI_DELIVERABILITY_CODE is '9' or 
where (prod.agg_name_address.COUNTRY_ID = 'CAN' 
and prod.agg_name_address.NCOA_CATEGORY = 'B')
Else if prod.agg_name_address.GCDI_DELIVERABILITY_CODE is not null code as 'Y'
ELSE Code as null."

;
SELECT TOP 100 acx_dsf_deliverability_indicator, acx_dsf_vacancy
, NCOA_CATEGORY
FROM prod.individual_address ia 
full join prod.coa  on ia.individual_id = coa.individual_id and ia.address_id = coa.address_id
WHERE 1=1 and ia.individual_id = 1200688761
;
;
SELECT TOP 100 ncoa_category

FROM prod.coa
WHERE 1=1 and individual_id = 1226181019
;

;
select count(*) from   cr_temp.GTT_MT_NAME_ADDRESS  icd
where not exists (
select  null from   cr_temp.gtt_agg_name_address cdh where icd.cdh_id = cdh.cdh_id)

;
;
SELECT TOP 100 ana.for_deliverable_flg, GCDI_DELIVERABILITY_CODE, COUNTRY_ID, NCOA_CATEGORY
FROM prod.agg_name_address ana
WHERE 1=1 and individual_id = 1200688761
;
;
SELECT 'NOT XXX' as addr_subtype, count(*)
FROM prod.individual_address
WHERE 1=1 
and data_source is null 
and address_subtype != 'XXX'
union all
SELECT 'XXX' as addr_subtype, count(*)
FROM prod.individual_address
WHERE 1=1 
and data_source is null 
and address_subtype = 'XXX'
union all
SELECT 'ALL' as addr_subtype, count(*)
FROM prod.individual_address
WHERE 1=1 
and data_source is null 


--and individual_id in (1200012952
--,1200008032
--,1200010656
--,1200005355)
;
;
SELECT TOP 100 *
FROM cr_temp.GTT_MT_NAME_ADDRESS
WHERE 1=1 and icd_id in ( 10002000694423020
,10002001364728010
,10004000454191030
,10004000695795040
,10004000882474020
,10004001079835050
,10004001370125040
,10247500003380010)

;
;
SELECT an.usa_deliverable_flg, an.dsf_deliverability_code, an.dsf_vacancy_indicator, an.ncoa_category
FROM prod.agg_name_address an
WHERE 1=1 and individual_id = 1226181019
;
;
SELECT TOP 100 acx_dsf_vacancy
FROM prod.match_instance
WHERE 1=1 and acx_rm_new_individual_id = 1226181019

;
;
SELECT TOP 100 *
FROM prod.individual_address
WHERE 1=1 and individual_id = 1200688761
;
;
SELECT TOP 100 GCDI_DELIVERABILITY_CODE, COUNTRY_ID, NCOA_CATEGORY
FROM prod.agg_name_address
WHERE 1=1 and individual_id = 1200688761
;
FOR_DELIVERABLE_FLG
"Code as 'N' where prod.agg_name_address.GCDI_DELIVERABILITY_CODE is '9' or 
where (prod.agg_name_address.COUNTRY_ID = 'CAN' and prod.agg_name_address.NCOA_CATEGORY = 'B')
Else if prod.agg_name_address.GCDI_DELIVERABILITY_CODE is not null code as 'Y'
ELSE Code as null."
;
"individual_address 
icd_id           |cdh_id    |acx_ef_record
10002000000000000|          |Y            
                 |1203179758|F            
"	"SD 8/3 3p:
Value is F (False) but for this field values are inversed in RM.

Flag values example:
   edit_fail_flg/acx_ef_record
ICD_WH:         Y
ICD_MT:         Y
match_instance: F
indiv_address:  F
name_address:   N 

USA_DELIVERABLE_FLG in agg_name_address depends on this field (EDIT_FAIL_FLG) and will need to be adjusted accordingly."
;
;
SELECT count(*)
FROM cr_temp.gtt_mt_name_address
WHERE 1=1 --and individual_id = 10002000000035040
;
;
SELECT count(*)
FROM cr_temp.gtt_agg_name_address
WHERE 1=1



;
SELECT TOP 100 *--individual_id, address_id, acx_ef_record
FROM prod.individual_address
WHERE 1=1 and individual_id = 1207016361
;
SELECT TOP 100 *-- individual_id, address_id, edit_fail_flg
FROM prod.agg_name_address
WHERE 1=1 and individual_id = 1207016361
;
SELECT TOP 100 * FROM prod.individual_address
WHERE individual_id =1205450921;
SELECT TOP 100 * FROM prod.agg_name_address 
WHERE individual_id =1205450921;
;
SELECT TOP 100 *
FROM prod.digital_transaction_detail dt inner join prod.action_header ah on dt.hash_action_id = ah.hash_action_id
WHERE 1=1 and individual_id = 1201300964




line_item_order_create_date
2004-07-27 19:25:33



