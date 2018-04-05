;
create table cr_temp.GTT_MT_BOOKS_ORD as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
/*, acct_id*/
, NVL2(ord_num, ord_num::text,'(NULL)') as ord_num
, NVL2(dnr_recip_cd, dnr_recip_cd::text,'(NULL)') as dnr_recip_cd
--, NVL2(ord_prem_flg, ord_prem_flg::text,'N') as ord_prem_flg
--, NVL2(ord_cdr_flg, ord_cdr_flg::text,'N') as ord_cdr_flg
, NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)') as entr_typ_cd
, NVL2(TO_CHAR(trn_dt,'DD-MON-YY'), TO_CHAR(trn_dt,'DD-MON-YY')::text,'(NULL)') as trn_dt
, NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)') as ord_dt
--, NVL2(ord_stat_cd, ord_stat_cd::text,'(NULL)') as ord_stat_cd
, NVL2(keycode, keycode::text,'(NULL)') as keycode
, NVL2(src_cd, src_cd::text,'(NULL)') as src_cd
, NVL2(trunc(nvl(ord_prod_amt,'0')), trunc(nvl(ord_prod_amt,'0'))::text,'(NULL)') as ord_prod_amt
, NVL2(trunc(nvl(ord_ship_amt,'0')), trunc(nvl(ord_ship_amt,'0'))::text,'(NULL)') as ord_ship_amt
, NVL2(trunc(nvl(ord_tax_amt,'0')), trunc(nvl(ord_tax_amt,'0'))::text,'(NULL)') as ord_tax_amt
, NVL2(trunc(nvl(ord_misc_amt,'0')), trunc(nvl(ord_misc_amt,'0'))::text,'(NULL)') as ord_misc_amt
, NVL2(trunc(nvl(ord_gft_cert_amt,'0')), trunc(nvl(ord_gft_cert_amt,'0'))::text,'(NULL)') as ord_gft_cert_amt
, NVL2(trunc(nvl(ord_coupn_amt,'0')), trunc(nvl(ord_coupn_amt,'0'))::text,'(NULL)') as ord_coupn_amt
, NVL2(trunc(nvl(ord_disc_amt,'0')), trunc(nvl(ord_disc_amt,'0'))::text,'(NULL)') as ord_disc_amt
, NVL2(trunc(nvl(ord_value_amt,'0')), trunc(nvl(ord_value_amt,'0'))::text,'(NULL)') as ord_value_amt
, NVL2(istlmt_cd, istlmt_cd::text,'(NULL)') as istlmt_cd
, NVL2(gift_ord_cd, gift_ord_cd::text,'(NULL)') as gift_ord_cd
, NVL2(currncy_typ_cd, currncy_typ_cd::text,'(NULL)') as currncy_typ_cd
, NVL2(crd_stat_cd, crd_stat_cd::text,'(NULL)') as crd_stat_cd
, NVL2(trunc(nvl(pmt_amt,'0')), trunc(nvl(pmt_amt,'0'))::text,'(NULL)') as pmt_amt
, NVL2(TO_CHAR(lst_pmt_dt,'DD-MON-YY'), TO_CHAR(lst_pmt_dt,'DD-MON-YY')::text,'(NULL)') as lst_pmt_dt
, NVL2(TO_CHAR(lst_pmt_ret_dt,'DD-MON-YY'), TO_CHAR(lst_pmt_ret_dt,'DD-MON-YY')::text,'(NULL)') as lst_pmt_ret_dt
, NVL2(trunc(nvl(wr_off_amt,'0')), trunc(nvl(wr_off_amt,'0'))::text,'(NULL)') as wr_off_amt
, NVL2(TO_CHAR(lst_wr_off_dt,'DD-MON-YY'), TO_CHAR(lst_wr_off_dt,'DD-MON-YY')::text,'(NULL)') as lst_wr_off_dt
, NVL2(trunc(nvl(coll_amt,'0')), trunc(nvl(coll_amt,'0'))::text,'(NULL)') as coll_amt
, NVL2(TO_CHAR(lst_coll_dt,'DD-MON-YY'), TO_CHAR(lst_coll_dt,'DD-MON-YY')::text,'(NULL)') as lst_coll_dt
/*---END---*/
,md5(
ICD_ID||cdh_id
/*--START--*/
/*||individual_id*/
/*||acct_id*/
||NVL2(ord_num, ord_num::text,'(NULL)')
||NVL2(dnr_recip_cd, dnr_recip_cd::text,'(NULL)')
--||NVL2(ord_prem_flg, ord_prem_flg::text,'(NULL)')
--||NVL2(ord_cdr_flg, ord_cdr_flg::text,'(NULL)')
||NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)')
||NVL2(TO_CHAR(trn_dt,'DD-MON-YY'), TO_CHAR(trn_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)')
--||NVL2(ord_stat_cd, ord_stat_cd::text,'(NULL)')
||NVL2(keycode, keycode::text,'(NULL)')
||NVL2(src_cd, src_cd::text,'(NULL)')
||NVL2(trunc(nvl(ord_prod_amt,'0')), trunc(nvl(ord_prod_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_ship_amt,'0')), trunc(nvl(ord_ship_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_tax_amt,'0')), trunc(nvl(ord_tax_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_misc_amt,'0')), trunc(nvl(ord_misc_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_gft_cert_amt,'0')), trunc(nvl(ord_gft_cert_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_coupn_amt,'0')), trunc(nvl(ord_coupn_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_disc_amt,'0')), trunc(nvl(ord_disc_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_value_amt,'0')), trunc(nvl(ord_value_amt,'0'))::text,'(NULL)')
||NVL2(istlmt_cd, istlmt_cd::text,'(NULL)')
||NVL2(gift_ord_cd, gift_ord_cd::text,'(NULL)')
||NVL2(currncy_typ_cd, currncy_typ_cd::text,'(NULL)')
||NVL2(crd_stat_cd, crd_stat_cd::text,'(NULL)')
||NVL2(trunc(nvl(pmt_amt,'0')), trunc(nvl(pmt_amt,'0'))::text,'(NULL)')
||NVL2(TO_CHAR(lst_pmt_dt,'DD-MON-YY'), TO_CHAR(lst_pmt_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(TO_CHAR(lst_pmt_ret_dt,'DD-MON-YY'), TO_CHAR(lst_pmt_ret_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(trunc(nvl(wr_off_amt,'0')), trunc(nvl(wr_off_amt,'0'))::text,'(NULL)')
||NVL2(TO_CHAR(lst_wr_off_dt,'DD-MON-YY'), TO_CHAR(lst_wr_off_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(trunc(nvl(coll_amt,'0')), trunc(nvl(coll_amt,'0'))::text,'(NULL)')
||NVL2(TO_CHAR(lst_coll_dt,'DD-MON-YY'), TO_CHAR(lst_coll_dt,'DD-MON-YY')::text,'(NULL)')
/*---END---*/
) as hash_value
from cr_temp.MT_BOOKS_ORD T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;
CREATE TABLE cr_temp.gtt_agg_books_order AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
/*, individual_id*/
/*, acct_id*/
, NVL2(ord_num, ord_num::text,'(NULL)') as ord_num
, NVL2(dnr_recip_cd, dnr_recip_cd::text,'(NULL)') as dnr_recip_cd
--, NVL2(ord_prem_flg, ord_prem_flg::text,'(NULL)') as ord_prem_flg
--, NVL2(ord_cdr_flg, ord_cdr_flg::text,'(NULL)') as ord_cdr_flg
, NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)') as entr_typ_cd
, NVL2(TO_CHAR(trn_dt,'DD-MON-YY'), TO_CHAR(trn_dt,'DD-MON-YY')::text,'(NULL)') as trn_dt
, NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)') as ord_dt
--, NVL2(ord_stat_cd, ord_stat_cd::text,'(NULL)') as ord_stat_cd
, NVL2(keycode, keycode::text,'(NULL)') as keycode
, NVL2(src_cd, src_cd::text,'(NULL)') as src_cd
, NVL2(trunc(nvl(ord_prod_amt,'0')), trunc(nvl(ord_prod_amt,'0'))::text,'(NULL)') as ord_prod_amt
, NVL2(trunc(nvl(ord_ship_amt,'0')), trunc(nvl(ord_ship_amt,'0'))::text,'(NULL)') as ord_ship_amt
, NVL2(trunc(nvl(ord_tax_amt,'0')), trunc(nvl(ord_tax_amt,'0'))::text,'(NULL)') as ord_tax_amt
, NVL2(trunc(nvl(ord_misc_amt,'0')), trunc(nvl(ord_misc_amt,'0'))::text,'(NULL)') as ord_misc_amt
, NVL2(trunc(nvl(ord_gft_cert_amt,'0')), trunc(nvl(ord_gft_cert_amt,'0'))::text,'(NULL)') as ord_gft_cert_amt
, NVL2(trunc(nvl(ord_coupn_amt,'0')), trunc(nvl(ord_coupn_amt,'0'))::text,'(NULL)') as ord_coupn_amt
, NVL2(trunc(nvl(ord_disc_amt,'0')), trunc(nvl(ord_disc_amt,'0'))::text,'(NULL)') as ord_disc_amt
, NVL2(trunc(nvl(ord_value_amt,'0')), trunc(nvl(ord_value_amt,'0'))::text,'(NULL)') as ord_value_amt
, NVL2(istlmt_cd, istlmt_cd::text,'(NULL)') as istlmt_cd
, NVL2(gift_ord_cd, gift_ord_cd::text,'(NULL)') as gift_ord_cd
, NVL2(currncy_typ_cd, currncy_typ_cd::text,'(NULL)') as currncy_typ_cd
, NVL2(crd_stat_cd, crd_stat_cd::text,'(NULL)') as crd_stat_cd
, NVL2(trunc(nvl(pmt_amt,'0')), trunc(nvl(pmt_amt,'0'))::text,'(NULL)') as pmt_amt
, NVL2(TO_CHAR(lst_pmt_dt,'DD-MON-YY'), TO_CHAR(lst_pmt_dt,'DD-MON-YY')::text,'(NULL)') as lst_pmt_dt
, NVL2(TO_CHAR(lst_pmt_ret_dt,'DD-MON-YY'), TO_CHAR(lst_pmt_ret_dt,'DD-MON-YY')::text,'(NULL)') as lst_pmt_ret_dt
, NVL2(trunc(nvl(wr_off_amt,'0')), trunc(nvl(wr_off_amt,'0'))::text,'(NULL)') as wr_off_amt
, NVL2(TO_CHAR(lst_wr_off_dt,'DD-MON-YY'), TO_CHAR(lst_wr_off_dt,'DD-MON-YY')::text,'(NULL)') as lst_wr_off_dt
, NVL2(trunc(nvl(coll_amt,'0')), trunc(nvl(coll_amt,'0'))::text,'(NULL)') as coll_amt
, NVL2(TO_CHAR(lst_coll_dt,'DD-MON-YY'), TO_CHAR(lst_coll_dt,'DD-MON-YY')::text,'(NULL)') as lst_coll_dt
/*---END---*/
,md5(
ICD_ID||cdh_id
/*--START--*/
/*||individual_id*/
/*||acct_id*/
||NVL2(ord_num, ord_num::text,'(NULL)')
||NVL2(dnr_recip_cd, dnr_recip_cd::text,'(NULL)')
--||NVL2(ord_prem_flg, ord_prem_flg::text,'(NULL)')
--||NVL2(ord_cdr_flg, ord_cdr_flg::text,'(NULL)')
||NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)')
||NVL2(TO_CHAR(trn_dt,'DD-MON-YY'), TO_CHAR(trn_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)')
--||NVL2(ord_stat_cd, ord_stat_cd::text,'(NULL)')
||NVL2(keycode, keycode::text,'(NULL)')
||NVL2(src_cd, src_cd::text,'(NULL)')
||NVL2(trunc(nvl(ord_prod_amt,'0')), trunc(nvl(ord_prod_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_ship_amt,'0')), trunc(nvl(ord_ship_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_tax_amt,'0')), trunc(nvl(ord_tax_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_misc_amt,'0')), trunc(nvl(ord_misc_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_gft_cert_amt,'0')), trunc(nvl(ord_gft_cert_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_coupn_amt,'0')), trunc(nvl(ord_coupn_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_disc_amt,'0')), trunc(nvl(ord_disc_amt,'0'))::text,'(NULL)')
||NVL2(trunc(nvl(ord_value_amt,'0')), trunc(nvl(ord_value_amt,'0'))::text,'(NULL)')
||NVL2(istlmt_cd, istlmt_cd::text,'(NULL)')
||NVL2(gift_ord_cd, gift_ord_cd::text,'(NULL)')
||NVL2(currncy_typ_cd, currncy_typ_cd::text,'(NULL)')
||NVL2(crd_stat_cd, crd_stat_cd::text,'(NULL)')
||NVL2(trunc(nvl(pmt_amt,'0')), trunc(nvl(pmt_amt,'0'))::text,'(NULL)')
||NVL2(TO_CHAR(lst_pmt_dt,'DD-MON-YY'), TO_CHAR(lst_pmt_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(TO_CHAR(lst_pmt_ret_dt,'DD-MON-YY'), TO_CHAR(lst_pmt_ret_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(trunc(nvl(wr_off_amt,'0')), trunc(nvl(wr_off_amt,'0'))::text,'(NULL)')
||NVL2(TO_CHAR(lst_wr_off_dt,'DD-MON-YY'), TO_CHAR(lst_wr_off_dt,'DD-MON-YY')::text,'(NULL)')
||NVL2(trunc(nvl(coll_amt,'0')), trunc(nvl(coll_amt,'0'))::text,'(NULL)')
||NVL2(TO_CHAR(lst_coll_dt,'DD-MON-YY'), TO_CHAR(lst_coll_dt,'DD-MON-YY')::text,'(NULL)')
/*---END---*/
) as hash_value
from prod.agg_books_order T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;
------------------------------------------------------------------------TABLE CREATION END\
with hash_diff as (
select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID, HASH_VALUE
from (
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.GTT_MT_BOOKS_ORD
union all
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL, HASH_VALUE
from cr_temp.gtt_agg_books_order
)
group by ICD_ID, CDH_ID, hash_value
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select top 50 * from hash_diff;
------------------------------------------------------------------------RUN ALL ABOVE
;
select * from   cr_temp.GTT_MT_BOOKS_ORD  where icd_id = 10002000027354080
;
select * from   cr_temp.gtt_agg_books_order  where cdh_id = 1221769933
;
--truncate table    cr_temp.GTT_MT_BOOKS_ORD;
drop table  cr_temp.GTT_MT_BOOKS_ORD;
--truncate table cr_temp.gtt_agg_books_order;
drop table  cr_temp.gtt_agg_books_order;			

;
SELECT TOP 100 *
FROM cr_temp.mt_books_ord
WHERE 1=1 and individual_id = '10002000012922020'

;
SELECT TOP 100 *
FROM prod.agg_books_order
WHERE 1=1 and individual_id = 1200592595



;
												
"for all action_header records where source_name = 'CDB':
take count of all return records - return_count
take count of all order records - order_count
If order_count = return_count and  action_header.action_subtype_code = '63' 
then code as 'F', 
If order_count = return_count and  action_header.action_subtype_code = '64'
 then code as 'B', 
If order_count != return_count and  action_header.action_subtype_code = '63'
 then code as 'G', 
If order_count != return_count and  action_header.action_subtype_code = '64'
 then code as 'E', 
Else if there are no return entries 
then books_transaction.order_status"	"A = Order active
B = Order canceled
D = Order pending
E = Order partial cancel
F = Order returned
G = Order partial return
H = Order deleted"	BOOKS_TRANSACTION	order_status

;
;
SELECT TOP 100 item_type_code, cdr_flag, *
FROM prod.books_transaction  bt full join prod.action_header ah on bt.hash_action_id = ah.hash_action_id
full join prod.books_transaction_detail btd on bt.hash_action_id = btd.hash_action_id
WHERE 1=1 and individual_id = 1206845844
and ah.source_name = 'CDB'

;
SELECT TOP 100 *--item_type_code, cdr_flag

FROM prod.books_transaction_detail btd full join prod.action_header ah on btd.hash_action_id = ah.hash_action_id
WHERE 1=1 
--and individual_id = 1206845844
and action_id =  103268094523001
;
SELECT TOP 100 action_subtype_code, ah.*, bt.*
FROM prod.action_header ah inner join prod.books_transaction bt on ah.hash_action_id = bt.hash_action_id
WHERE 1=1
and individual_id = 1203053841
;
"If all books_transaction_detail.ITM_TYP_CD = 'C' in the order
then code as 'Y'
Else code as 'N'."
"If any books_transaction_detail.CDR_FLG = 'Y' 
then code as 'Y'.
Else code as 'N'."

;
;
SELECT TOP 100 action_type, LINE_ITEM_ORDER_MODIFY_DATE, LINE_ITEM_STATUS, paid_date, *
FROM prod.action_header ah full join prod.digital_transaction_detail dt on dt.hash_action_id = ah.hash_action_id
WHERE 1=1 and individual_id = 1229674236    --source_name, and stat_code = 'R'
;

individual_id|acct_id                               |ord_id  |stat_cd|ord_dt             |entr_typ_cd|pd_dt
1229674236   |00000000000000000000000000000015411566|22323278|C      |2015-02-17 22:10:14|D          |2015-02-17 22:10:14
1229674236   |00000000000000000000000000000009381818|25243428|R      |2002-03-01 00:00:00|NULL       |1600-01-01 00:00:00
;
SELECT TOP 100 *
FROM prod.action_header
WHERE 1=1 and individual_id = 1229674236
;
"Online (Digital)
(SOURCE_NAME = 'PWI', ACTION_TYPE = 'ORDER')
If ACTION_HEADER.PD_DT is null
then use the maximum
    DIGITAL_TRANSACTION_DETAIL.LINE_ITEM_ORDER_MODIFY_DATE
       from the DIGITAL_TRANSACTION_DETAIL entry
            where
                DIGITAL_TRANSACTION_DETAIL.LINE_ITEM_STATUS = 'C' or 'H'
                where
                  DIGITAL_TRANSACTION_DETAIL.HASH_ACTION_ID = 
                        ACTION_HEADER.HASH_ACTION_ID,
Else
    ACTION_HEADER.PD_DT

Offline (Print)
(SOURCE_NAME = 'PWI', ACTION_TYPE = 'ORDER')
If PRINT_TRANSACTION.SET_CD is 'A' then ACTION_HEADER.ACTION_DATE.
Else
Take the maximum
    Recipient Order ACTION_HEADER.ACTION_DATE
    and Donor Order  ACTION_HEADER.ACTION_DATE"

;
;
SELECT TOP 100 *
FROM cr_temp.mt_online_ord
WHERE 1=1 and individual_id = 10002000000006110
