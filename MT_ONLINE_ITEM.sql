;
create table cr_temp.GTT_MT_ONLINE_ITEM as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(itm_id, itm_id::text,'(NULL)') as itm_id
, NVL2(ord_id, ord_id::text,'(NULL)') as ord_id
, NVL2(seq_num, seq_num::text,'(NULL)') as seq_num
, NVL2(stat_cd, stat_cd::text,'(NULL)') as stat_cd
, NVL2(sku_num, sku_num::text,'(NULL)') as sku_num
, NVL2(trunc(nvl(tot_gross_amt,'0')), trunc(nvl(tot_gross_amt,'0'))::text,'(NULL)') as tot_gross_amt
--, NVL2(trunc(nvl(tot_amt,'0')), trunc(nvl(tot_amt,'0'))::text,'(NULL)') as tot_amt
--, NVL2(TO_CHAR(strt_dt,'DD-MON-YY'), TO_CHAR(strt_dt,'DD-MON-YY')::text,'(NULL)') as strt_dt
--, NVL2(TO_CHAR(end_dt,'DD-MON-YY'), TO_CHAR(end_dt,'DD-MON-YY')::text,'(NULL)') as end_dt
, NVL2(mag_cd, mag_cd::text,'(NULL)') as mag_cd
 , NVL2(crd_stat_cd, crd_stat_cd::text,'(NULL)') as crd_stat_cd
 , NVL2(canc_flg, canc_flg::text,'(NULL)') as canc_flg
 , NVL2(trunc(nvl(term_mth_cnt,'0')), trunc(nvl(term_mth_cnt,'0'))::text,'(NULL)') as term_mth_cnt
 , NVL2(rnw_cd, rnw_cd::text,'(NULL)') as rnw_cd
 , NVL2(set_cd, set_cd::text,'(NULL)') as set_cd
-- , NVL2(ext_keycd, ext_keycd::text,'(NULL)') as ext_keycd
-- , NVL2(int_keycd, int_keycd::text,'(NULL)') as int_keycd
-- , NVL2(canc_rsn_cd, canc_rsn_cd::text,'(NULL)') as canc_rsn_cd
 , NVL2(shp_meth_cd, shp_meth_cd::text,'(NULL)') as shp_meth_cd
 , NVL2(TO_CHAR(crt_dt,'DD-MON-YY'), TO_CHAR(crt_dt,'DD-MON-YY')::text,'(NULL)') as crt_dt
 , upper(NVL2(rpt_nam, rpt_nam::text,'(NULL)')) as rpt_nam
 , NVL2(sub_id, sub_id::text,'(NULL)') as sub_id
 , NVL2(sub_rnw_ind, sub_rnw_ind::text,'(NULL)') as sub_rnw_ind
 , NVL2(svc_stat_cd, svc_stat_cd::text,'(NULL)') as svc_stat_cd
 , NVL2(TO_CHAR(canc_dt,'DD-MON-YY'), TO_CHAR(canc_dt,'DD-MON-YY')::text,'(NULL)') as canc_dt
 --, NVL2(TO_CHAR(pmt_dt,'DD-MON-YY'), TO_CHAR(pmt_dt,'DD-MON-YY')::text,'(NULL)') as pmt_dt
 , NVL2(sub_src_cd, sub_src_cd::text,'(NULL)') as sub_src_cd
-- , NVL2(aps_src_cd, aps_src_cd::text,'(NULL)') as aps_src_cd
 , NVL2(TO_CHAR(cro_acct_exp_dt,'DD-MON-YY'), TO_CHAR(cro_acct_exp_dt,'DD-MON-YY')::text,'(NULL)') as cro_acct_exp_dt
 , NVL2(cancelled_by, cancelled_by::text,'(NULL)') as cancelled_by
/*---END---*/
,md5(
ICD_ID||cdh_id
/*--START--*/
/*||individual_id*/
-- ||NVL2(itm_id, itm_id::text,'(NULL)')
-- ||NVL2(ord_id, ord_id::text,'(NULL)')
||NVL2(seq_num, seq_num::text,'(NULL)')
||NVL2(stat_cd, stat_cd::text,'(NULL)')
||NVL2(sku_num, sku_num::text,'(NULL)')
||NVL2(trunc(nvl(tot_gross_amt,'0')), trunc(nvl(tot_gross_amt,'0'))::text,'(NULL)')
--||NVL2(trunc(nvl(tot_amt,'0')), trunc(nvl(tot_amt,'0'))::text,'(NULL)')
--||NVL2(TO_CHAR(strt_dt,'DD-MON-YY'), TO_CHAR(strt_dt,'DD-MON-YY')::text,'(NULL)')
--||NVL2(TO_CHAR(end_dt,'DD-MON-YY'), TO_CHAR(end_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(mag_cd, mag_cd::text,'(NULL)')
 ||NVL2(crd_stat_cd, crd_stat_cd::text,'(NULL)')
 ||NVL2(canc_flg, canc_flg::text,'(NULL)')
 ||NVL2(trunc(nvl(term_mth_cnt,'0')), trunc(nvl(term_mth_cnt,'0'))::text,'(NULL)')
 ||NVL2(rnw_cd, rnw_cd::text,'(NULL)')
 ||NVL2(set_cd, set_cd::text,'(NULL)')
-- ||NVL2(ext_keycd, ext_keycd::text,'(NULL)')
-- ||NVL2(int_keycd, int_keycd::text,'(NULL)')
-- ||NVL2(canc_rsn_cd, canc_rsn_cd::text,'(NULL)')
 ||NVL2(shp_meth_cd, shp_meth_cd::text,'(NULL)')
 ||NVL2(TO_CHAR(crt_dt,'DD-MON-YY'), TO_CHAR(crt_dt,'DD-MON-YY')::text,'(NULL)')
 ||upper(NVL2(rpt_nam, rpt_nam::text,'(NULL)'))
 ||NVL2(sub_id, sub_id::text,'(NULL)')
 ||NVL2(sub_rnw_ind, sub_rnw_ind::text,'(NULL)')
 ||NVL2(svc_stat_cd, svc_stat_cd::text,'(NULL)')
 ||NVL2(TO_CHAR(canc_dt,'DD-MON-YY'), TO_CHAR(canc_dt,'DD-MON-YY')::text,'(NULL)')
-- ||NVL2(TO_CHAR(pmt_dt,'DD-MON-YY'), TO_CHAR(pmt_dt,'DD-MON-YY')::text,'(NULL)')
 ||NVL2(sub_src_cd, sub_src_cd::text,'(NULL)')
-- ||NVL2(aps_src_cd, aps_src_cd::text,'(NULL)')
 ||NVL2(TO_CHAR(cro_acct_exp_dt,'DD-MON-YY'), TO_CHAR(cro_acct_exp_dt,'DD-MON-YY')::text,'(NULL)')
 ||NVL2(cancelled_by, cancelled_by::text,'(NULL)')
/*---END---*/
) as hash_value
from cr_temp.MT_ONLINE_ITEM T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;
CREATE TABLE cr_temp.gtt_agg_digital_item AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
, NVL2(itm_id, itm_id::text,'(NULL)') as itm_id
, NVL2(ord_id, ord_id::text,'(NULL)') as ord_id
, NVL2(seq_num, seq_num::text,'(NULL)') as seq_num
, NVL2(stat_cd, stat_cd::text,'(NULL)') as stat_cd
, NVL2(sku_num, sku_num::text,'(NULL)') as sku_num
, NVL2(trunc(nvl(tot_gross_amt,'0')), trunc(nvl(tot_gross_amt,'0'))::text,'(NULL)') as tot_gross_amt
--, NVL2(trunc(nvl(tot_amt,'0')), trunc(nvl(tot_amt,'0'))::text,'(NULL)') as tot_amt
--, NVL2(TO_CHAR(strt_dt,'DD-MON-YY'), TO_CHAR(strt_dt,'DD-MON-YY')::text,'(NULL)') as strt_dt
--, NVL2(TO_CHAR(end_dt,'DD-MON-YY'), TO_CHAR(end_dt,'DD-MON-YY')::text,'(NULL)') as end_dt
, NVL2(mag_cd, mag_cd::text,'(NULL)') as mag_cd
 , NVL2(crd_stat_cd, crd_stat_cd::text,'(NULL)') as crd_stat_cd
 , NVL2(canc_flg, canc_flg::text,'(NULL)') as canc_flg
 , NVL2(trunc(nvl(term_mth_cnt,'0')), trunc(nvl(term_mth_cnt,'0'))::text,'(NULL)') as term_mth_cnt
 , NVL2(rnw_cd, rnw_cd::text,'(NULL)') as rnw_cd
 , NVL2(set_cd, set_cd::text,'(NULL)') as set_cd
-- , NVL2(ext_keycd, ext_keycd::text,'(NULL)') as ext_keycd
-- , NVL2(int_keycd, int_keycd::text,'(NULL)') as int_keycd
-- , NVL2(canc_rsn_cd, canc_rsn_cd::text,'(NULL)') as canc_rsn_cd
 , NVL2(shp_meth_cd, shp_meth_cd::text,'(NULL)') as shp_meth_cd
 , NVL2(TO_CHAR(crt_dt,'DD-MON-YY'), TO_CHAR(crt_dt,'DD-MON-YY')::text,'(NULL)') as crt_dt
 , upper(NVL2(rpt_nam, rpt_nam::text,'(NULL)')) as rpt_nam
 , NVL2(sub_id, sub_id::text,'(NULL)') as sub_id
 , NVL2(sub_rnw_ind, sub_rnw_ind::text,'(NULL)') as sub_rnw_ind
 , NVL2(svc_stat_cd, svc_stat_cd::text,'(NULL)') as svc_stat_cd
 , NVL2(TO_CHAR(canc_dt,'DD-MON-YY'), TO_CHAR(canc_dt,'DD-MON-YY')::text,'(NULL)') as canc_dt
 --, NVL2(TO_CHAR(pmt_dt,'DD-MON-YY'), TO_CHAR(pmt_dt,'DD-MON-YY')::text,'(NULL)') as pmt_dt
 , NVL2(sub_src_cd, sub_src_cd::text,'(NULL)') as sub_src_cd
-- , NVL2(aps_src_cd, aps_src_cd::text,'(NULL)') as aps_src_cd
 , NVL2(TO_CHAR(cro_acct_exp_dt,'DD-MON-YY'), TO_CHAR(cro_acct_exp_dt,'DD-MON-YY')::text,'(NULL)') as cro_acct_exp_dt
 , NVL2(cancelled_by, cancelled_by::text,'(NULL)') as cancelled_by
/*---END---*/
,md5(
ICD_ID||cdh_id
/*--START--*/
/*||individual_id*/
-- ||NVL2(itm_id, itm_id::text,'(NULL)')
-- ||NVL2(ord_id, ord_id::text,'(NULL)')
||NVL2(seq_num, seq_num::text,'(NULL)')
||NVL2(stat_cd, stat_cd::text,'(NULL)')
||NVL2(sku_num, sku_num::text,'(NULL)')
||NVL2(trunc(nvl(tot_gross_amt,'0')), trunc(nvl(tot_gross_amt,'0'))::text,'(NULL)')
--||NVL2(trunc(nvl(tot_amt,'0')), trunc(nvl(tot_amt,'0'))::text,'(NULL)')
--||NVL2(TO_CHAR(strt_dt,'DD-MON-YY'), TO_CHAR(strt_dt,'DD-MON-YY')::text,'(NULL)')
--||NVL2(TO_CHAR(end_dt,'DD-MON-YY'), TO_CHAR(end_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(mag_cd, mag_cd::text,'(NULL)')
 ||NVL2(crd_stat_cd, crd_stat_cd::text,'(NULL)')
 ||NVL2(canc_flg, canc_flg::text,'(NULL)')
 ||NVL2(trunc(nvl(term_mth_cnt,'0')), trunc(nvl(term_mth_cnt,'0'))::text,'(NULL)')
 ||NVL2(rnw_cd, rnw_cd::text,'(NULL)')
 ||NVL2(set_cd, set_cd::text,'(NULL)')
-- ||NVL2(ext_keycd, ext_keycd::text,'(NULL)')
-- ||NVL2(int_keycd, int_keycd::text,'(NULL)')
-- ||NVL2(canc_rsn_cd, canc_rsn_cd::text,'(NULL)')
 ||NVL2(shp_meth_cd, shp_meth_cd::text,'(NULL)')
 ||NVL2(TO_CHAR(crt_dt,'DD-MON-YY'), TO_CHAR(crt_dt,'DD-MON-YY')::text,'(NULL)')
 ||upper(NVL2(rpt_nam, rpt_nam::text,'(NULL)'))
 ||NVL2(sub_id, sub_id::text,'(NULL)')
 ||NVL2(sub_rnw_ind, sub_rnw_ind::text,'(NULL)')
 ||NVL2(svc_stat_cd, svc_stat_cd::text,'(NULL)')
 ||NVL2(TO_CHAR(canc_dt,'DD-MON-YY'), TO_CHAR(canc_dt,'DD-MON-YY')::text,'(NULL)')
--||NVL2(TO_CHAR(pmt_dt,'DD-MON-YY'), TO_CHAR(pmt_dt,'DD-MON-YY')::text,'(NULL)')
 ||NVL2(sub_src_cd, sub_src_cd::text,'(NULL)')
-- ||NVL2(aps_src_cd, aps_src_cd::text,'(NULL)')
 ||NVL2(TO_CHAR(cro_acct_exp_dt,'DD-MON-YY'), TO_CHAR(cro_acct_exp_dt,'DD-MON-YY')::text,'(NULL)')
 ||NVL2(cancelled_by, cancelled_by::text,'(NULL)')
/*---END---*/
) as hash_value
from prod.agg_digital_item T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;
------------------------------------------------------------------------TABLE CREATION END\
with hash_diff as (
select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID, HASH_VALUE
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.GTT_MT_ONLINE_ITEM
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.gtt_agg_digital_item
)
group by ICD_ID, CDH_ID, hash_value
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select top 200 * from hash_diff;
------------------------------------------------------------------------RUN ALL ABOVE
;
select * from   cr_temp.GTT_MT_ONLINE_ITEM  where icd_id = 10002000000003110
;
select * from   cr_temp.gtt_agg_digital_item  where cdh_id = 1229376930
;
--truncate table    cr_temp.GTT_MT_ONLINE_ITEM;
drop table  cr_temp.GTT_MT_ONLINE_ITEM;
--truncate table cr_temp.gtt_agg_digital_item;
drop table  cr_temp.gtt_agg_digital_item;
;
SELECT TOP 100 ah.*
FROM prod.agg_digital_item ah 
WHERE 1=1 and individual_id = 1221711070
and ord_id in (90497532,102003772)
;
SELECT TOP 100 ah.*
FROM prod.ACTION_HEADER ah 
WHERE 1=1 and individual_id = 1229884790
and action_id in (90497532,102003772)
;
"Table Level rule: 
All Digital Orders from ACTION_HEADER (where SOURCE_NAME = 'PWI' and ACTION_TYPE = 'ORDER) where DIGITAL_TRANSACTION_DETAIL.REFERENCE_ID is null or zero and 
All Print Orders from ACTION_HEADER (where SOURCE_NAME = 'CDS' and ACTION_TYPE = 'ORDER) and PRINT_ACCOUNT_DETAIL.MAGAZINE_CODE is 'CRE'. 
Note: Refer to the tab REF_agg_print_order for details regarding Recipient and Cancel relations mentioned in the print transformations"		
;
;
SELECT TOP 100 *
FROM prod.agg_digital_item
WHERE 1=1 and individual_id = 1229674236
;
OFFLINE_CANCEL_ACT.CANC_RSN_CD 
where OFFLINE_ORDER.ACCT_ID = OFFLINE_CANCEL_ACT.ACCT_ID 
and OFFLINE_ORDER.ORD_ID = OFFLINE_CANCEL_ACT.ORD_ID from the entry with the maximum OFFLINE_CANCEL_ACT.ACT_DT.

;
INDIVIDUAL_ID    |ACCT_ID          |ORD_ID             |ORD_NUM      |ORD_DT             |STAT_CD|CPLT_DT            |ENTR_TYP_CD|CR_STAT_CD|CANC_FLG|CANC_RSN_CD|CANC_DT            |ISS_CANC_NUM
10002000222419060|10004000421141034|1000400170189101015|130492356344 |31-OCT-01          |C      |11-NOV-03          |A          |G         |N       |           |                   |
10002000222419060|10004000421141034|1000400170189501015|530704125516 |03-NOV-05          |B      |                   |E          |A         |Y       |06         |08-MAY-06          |11

1201511433       |0015416753       |1000400170189101015|130492356344 |2001-10-31 00:00:00|C      |2003-11-11 00:00:00|A          |G         |N       |05         |2002-08-01 00:00:00|11
1201511433       |0015416753       |1000400170189501015|530704125516 |2005-11-03 00:00:00|B      |NULL               |E          |A         |Y       |06         |2006-05-08 00:00:00|11
;
SELECT ah.action_id, ah.action_date, pc.cancel_reason_code, pc.iss_cancel_num, addtl_action_id
,
ROW_NUMBER() OVER (PARTITION BY ah.hash_account_id, ah.action_id ORDER BY ah.action_date DESC ) ranking
FROM prod.action_header ah JOIN prod.print_cancel pc 
  on ah.hash_action_id = pc.hash_action_id 
WHERE ah.source_name = 'CDS' AND ah.action_type = 'CANCEL' and ah.action_id = 1000400170189101015;



;
select count(*)
from prod.action_header ah
WHERE 1=1 and individual_id = 1201301788

;
select count(*) from  cr_temp.GTT_MT_ONLINE_ITEM;
select count(*) from  cr_temp.gtt_agg_digital_item;
select count(*) from  cr_temp.MT_CREDIT_CARD;
select count(*) from  prod.agg_credit_card;

;
SELECT TOP 100 *
FROM prod.agg_digital_item
WHERE 1=1 and individual_id = 1228060579
;
SELECT TOP 100 *
FROM prod.action_header ah inner join prod.digital_transaction_detail dtd on dtd.hash_action_id = ah.hash
WHERE 1=1 and individual_id = 1228060579
;
"Digital:
IF
    DIGITAL_TRANSACTION_DETAIL.LINE_ITEM_MAGAZINE_CODE is 'CRO' or 'CARP'
Then
    If
        DIGITAL_TRANSACTION_DETAIL.EXTERNAL_KEYCODE is null
        and DIGITAL_TRANSACTION_DETAIL.INTERNAL_KEYCODE is null
    Then “I”
    ELSE IF
        The first two positions of (DIGITAL_TRANSACTION_DETAIL.EXTERNAL_KEYCODE 
            or if null the DIGITAL_TRANSACTION_DETAIL.INTERNAL_KEYCODE) = “WF”
    Then “X”
    Else
        use the first position of (DIGITAL_TRANSACTION_DETAIL.EXTERNAL_KEYCODE
            or if null the DIGITAL_TRANSACTION_DETAIL.INTERNAL_KEYCODE)
Else If
    DIGITAL_TRANSACTION_DETAIL.LINE_ITEM_MAGAZINE_CODE is 'CRMG’
Then 
    IF
        DIGITAL_TRANSACTION_DETAIL.EXTERNAL_KEYCODE is null
        and DIGITAL_TRANSACTION_DETAIL.INTERNAL_KEYCODE is null
    THEN “I”
    Else If
        (DIGITAL_TRANSACTION_DETAIL.EXTERNAL_KEYCODE
            or if null then DIGITAL_TRANSACTION_DETAIL.INTERNAL_KEYCODE
        ) is “VOLUNTARY ORDER”
    Then “I”
    Else If
        the first position is ‘H’ for (DIGITAL_TRANSACTION_DETAIL.EXTERNAL_KEYCODE
            or if null the DIGITAL_TRANSACTION_DETAIL.INTERNAL_KEYCODE)
    Then
        Use the sixth position of (DIGITAL_TRANSACTION_DETAIL.EXTERNAL_KEYCODE or
            if null the DIGITAL_TRANSACTION_DETAIL.INTERNAL_KEYCODE)
    Else
        use the first position of (DIGITAL_TRANSACTION_DETAIL.EXTERNAL_KEYCODE
            or if null the DIGITAL_TRANSACTION_DETAIL.INTERNAL_KEYCODE)
Print:
NULL

Note:  this logic occurs after the keycodes have been converted to uppercase."

;
;
SELECT TOP 100 *-- LINE_ITEM_MAGAZINE_CODE, EXTERNAL_KEYCODE, INTERNAL_KEYCODE
FROM prod.action_header ah --inner join prod.digital_transaction_detail dt on dt.hash_action_id = ah.hash_action_id
WHERE 1=1 and individual_id = 1221713040 and ah.action_id = '1000430014700200015'
;
;
SELECT TOP 100 mag_cd, ext_keycd, int_keycd, sub_src_cd
FROM prod.agg_digital_item
WHERE 1=1 and individual_id = 1221713040 and itm_id = 1000430014700200015

;
"Digital:
DIGITAL_TRANSACTION_DETAIL.RENEWABLE_SUBSRC_FLAG

Print:
If the first four bytes of OFFLINE_ACCOUNT.KEYCODE = 'AUTO'
Then code as 'Y'
Else code as 'N'"

;
SELECT TOP 100 individual_id, action_id, keycode
FROM prod.action_header ah inner join prod.print_account_detail pa on pa.hash_account_id = ah.hash_account_id
WHERE 1=1 and individual_id = 1234864011 and action_id IN (1000430142526700015
,1000430142526800015
,1000430142526900015
,1000430142527000015)
;
;
SELECT TOP 100  individual_id, itm_id, ord_id, sub_rnw_ind
FROM prod.agg_digital_item
WHERE 1=1 and individual_id = 1234864011
and itm_id IN (1000430142526700015
,1000430142526800015
,1000430142526900015
,1000430142527000015)

