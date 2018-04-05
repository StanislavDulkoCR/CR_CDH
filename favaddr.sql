/*---------------Favourite Address logic only*/
Table Level rule:  Records from the Data Warehouse NAME_ADDRESS_REF table 
where NAME_ADDRESS_REF.ADDRESS_ID = TRANS_CLASS_3.ADDRESS_ID and NAME_ADDRESS_REF.INDIVIDUAL_ID = TRANS_CLASS_3.INDIVIDUAL_ID 
keeping the NAME_ADDRESS_REF row 
where NAME_ADDRESS_REF.NAME_ADDRESS_DIFF is ‘CNS’, ‘CRH’, ‘CRM’, 'SHM', 'DMP', 'ENN', 'CCC', 'PWI', or 'CDB' 
as the highest priority using the maximum MAINTENANCE_DATE as a tiebreaker.  

CDS, CU, KBL, LSR, DCK, CGA, CVO, and old CDS CRE/CRT are all at the same level (2nd Highest), 
with maximum MAINTENANCE_DATE as a tiebreaker among them.  

OSE is at the lowest level.									


;
with pre as (
SELECT *
,decode(address_subtype, 'CNS',1099, 'CRH',1098, 'CRM',1097, 'SHM',1096, 'DMP',1095, 'ENN',1094, 'CCC',1093, 'PWI',1092, 'CDB',1091           ) fav_grp1
,decode(address_subtype, 'CDS',1069, 'CU' ,1068, 'KBL',1067, 'LSR',1066, 'DCK',1065, 'CGA',1064, 'CVO',1063, 'CDS',1062, 'CRE',1061,'CRT',1060) fav_grp2
,decode(address_subtype, 'OSL',1039                                                                                                           ) fav_grp3

FROM prod.individual_address )
, fav_indaddr as (
select  individual_id
, substring(max(coalesce(update_date||fav_grp1, update_date||fav_grp2, update_date||fav_grp3)||address_id),1/*base*/+19/*update_date length*/+4/*fav_group length*/) as address_id
from pre
group by individual_id)

select top 10 *
from fav_indaddr
where individual_id = 1202550522
;/*---------------/*/

;
SELECT TOP 100 acx_zipcode, address_id
FROM prod.individual_address
WHERE 1=1 and individual_id = 1202550522

;
select max(qq) from (
select STRTOL('2017-08-09 11:20:30',22) qq union all select  STRTOL('2017-08-09 11:21:30',22))
;
select CONVERT('1234567',SIGNED INTEGER);

CONVERT(invoice_number ,SIGNED INTEGER)