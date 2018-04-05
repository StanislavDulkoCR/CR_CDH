;
create table cr_temp.GTT_MT_BOOKS_ORD as
select 1 as icd_fl, null as cdh_fl,ICD_ID, cdh_id
/*--START--*/
, NVL2(ord_num, ord_num::text,'(NULL)') as ord_num
, NVL2(dnr_recip_cd, dnr_recip_cd::text,'(NULL)') as dnr_recip_cd
, NVL2(ord_prem_flg, ord_prem_flg::text,'(NULL)') as ord_prem_flg
, NVL2(ord_cdr_flg, ord_cdr_flg::text,'(NULL)') as ord_cdr_flg
, NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)') as entr_typ_cd
, NVL2(TO_CHAR(trn_dt,'DD-MON-YY'), TO_CHAR(trn_dt,'DD-MON-YY')::text,'(NULL)') as trn_dt
, NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)') as ord_dt
, NVL2(ord_stat_cd, ord_stat_cd::text,'(NULL)') as ord_stat_cd
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

from cr_temp.MT_BOOKS_ORD T_MAIN  inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = ICD_ID ;

CREATE TABLE cr_temp.gtt_agg_books_order AS
select null as icd_fl, 1 as cdh_fl,ICD_ID, cdh_id
/*--START--*/
, NVL2(ord_num, ord_num::text,'(NULL)') as ord_num
, NVL2(dnr_recip_cd, dnr_recip_cd::text,'(NULL)') as dnr_recip_cd
, NVL2(ord_prem_flg, ord_prem_flg::text,'(NULL)') as ord_prem_flg
, NVL2(ord_cdr_flg, ord_cdr_flg::text,'(NULL)') as ord_cdr_flg
, NVL2(entr_typ_cd, entr_typ_cd::text,'(NULL)') as entr_typ_cd
, NVL2(TO_CHAR(trn_dt,'DD-MON-YY'), TO_CHAR(trn_dt,'DD-MON-YY')::text,'(NULL)') as trn_dt
, NVL2(TO_CHAR(ord_dt,'DD-MON-YY'), TO_CHAR(ord_dt,'DD-MON-YY')::text,'(NULL)') as ord_dt
, NVL2(ord_stat_cd, ord_stat_cd::text,'(NULL)') as ord_stat_cd
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
from prod.agg_books_order T_MAIN               inner join crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts on INDIVIDUAL_ID = CDH_ID ;



------------------------------------------------------------------------TABLE CREATION END\;
CREATE TABLE cr_temp.diff_MT_BOOKS_ORD AS
with hash_diff as (

select  COUNT(ICD_FL) as CNT_ICD_FL, COUNT(CDH_FL) as CNT_CDH_FL, ICD_ID, CDH_ID     , ord_num, dnr_recip_cd/*, ord_prem_flg, ord_cdr_flg*/, entr_typ_cd, trn_dt, ord_dt/*, ord_stat_cd*/, keycode, src_cd, ord_prod_amt, ord_ship_amt, ord_tax_amt, ord_misc_amt, ord_gft_cert_amt, ord_coupn_amt, ord_disc_amt, ord_value_amt, istlmt_cd, gift_ord_cd, currncy_typ_cd, crd_stat_cd, pmt_amt, lst_pmt_dt, lst_pmt_ret_dt, wr_off_amt, lst_wr_off_dt, coll_amt, lst_coll_dt
from (                                                                                                      /*                           */                             /*             */
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL , ord_num, dnr_recip_cd/*, ord_prem_flg, ord_cdr_flg*/, entr_typ_cd, trn_dt, ord_dt/*, ord_stat_cd*/, keycode, src_cd, ord_prod_amt, ord_ship_amt, ord_tax_amt, ord_misc_amt, ord_gft_cert_amt, ord_coupn_amt, ord_disc_amt, ord_value_amt, istlmt_cd, gift_ord_cd, currncy_typ_cd, crd_stat_cd, pmt_amt, lst_pmt_dt, lst_pmt_ret_dt, wr_off_amt, lst_wr_off_dt, coll_amt, lst_coll_dt
from cr_temp.GTT_MT_BOOKS_ORD                                                                               /*                           */                             /*             */
union all                                                                                                   /*                           */                             /*             */
select  ICD_ID, cdh_id, to_number(ICD_FL,9) as ICD_FL, to_number(CDH_FL,9) as CDH_FL , ord_num, dnr_recip_cd/*, ord_prem_flg, ord_cdr_flg*/, entr_typ_cd, trn_dt, ord_dt/*, ord_stat_cd*/, keycode, src_cd, ord_prod_amt, ord_ship_amt, ord_tax_amt, ord_misc_amt, ord_gft_cert_amt, ord_coupn_amt, ord_disc_amt, ord_value_amt, istlmt_cd, gift_ord_cd, currncy_typ_cd, crd_stat_cd, pmt_amt, lst_pmt_dt, lst_pmt_ret_dt, wr_off_amt, lst_wr_off_dt, coll_amt, lst_coll_dt
from cr_temp.gtt_agg_books_order                                                                            /*                           */                             /*             */
)                                                                                                           /*                           */                             /*             */
                                                                                                            /*                           */                             /*             */
group by ICD_ID, CDH_ID                                                              , ord_num, dnr_recip_cd/*, ord_prem_flg, ord_cdr_flg*/, entr_typ_cd, trn_dt, ord_dt/*, ord_stat_cd*/, keycode, src_cd, ord_prod_amt, ord_ship_amt, ord_tax_amt, ord_misc_amt, ord_gft_cert_amt, ord_coupn_amt, ord_disc_amt, ord_value_amt, istlmt_cd, gift_ord_cd, currncy_typ_cd, crd_stat_cd, pmt_amt, lst_pmt_dt, lst_pmt_ret_dt, wr_off_amt, lst_wr_off_dt, coll_amt, lst_coll_dt
having COUNT(ICD_FL)!=COUNT(CDH_FL)
order by ICD_ID, CDH_ID, COUNT(ICD_FL) desc, COUNT(CDH_FL) desc
)
select * from hash_diff;

select top 300 * from cr_temp.diff_MT_BOOKS_ORD
 where 1=1
;


with icd as (select * from cr_temp.diff_MT_BOOKS_ORD where 1=1 and cnt_icd_fl = 1)
,    cdh as (select * from cr_temp.diff_MT_BOOKS_ORD where 1=1 and cnt_cdh_fl = 1)
select top 300 icd.cnt_icd_fl||cdh.cnt_cdh_fl as match_fl, icd.icd_id ,cdh.cdh_id, icd.ord_num
, case when icd.ord_num = cdh.ord_num then '' else icd.ord_num||'<>'||cdh.ord_num /*                                    #*/ end ord_num_diff
, case when icd.dnr_recip_cd = cdh.dnr_recip_cd then '' else icd.dnr_recip_cd||'<>'||cdh.dnr_recip_cd /*                #*/ end dnr_recip_cd_diff
--, case when icd.ord_prem_flg = cdh.ord_prem_flg then '' else icd.ord_prem_flg||'<>'||cdh.ord_prem_flg /*                #*/ end ord_prem_flg_diff
--, case when icd.ord_cdr_flg = cdh.ord_cdr_flg then '' else icd.ord_cdr_flg||'<>'||cdh.ord_cdr_flg /*                    #*/ end ord_cdr_flg_diff
, case when icd.entr_typ_cd = cdh.entr_typ_cd then '' else icd.entr_typ_cd||'<>'||cdh.entr_typ_cd /*                    #*/ end entr_typ_cd_diff
, case when icd.trn_dt = cdh.trn_dt then '' else icd.trn_dt||'<>'||cdh.trn_dt /*                                        #*/ end trn_dt_diff
, case when icd.ord_dt = cdh.ord_dt then '' else icd.ord_dt||'<>'||cdh.ord_dt /*                                        #*/ end ord_dt_diff
--, case when icd.ord_stat_cd = cdh.ord_stat_cd then '' else icd.ord_stat_cd||'<>'||cdh.ord_stat_cd /*                    #*/ end ord_stat_cd_diff
, case when icd.keycode = cdh.keycode then '' else icd.keycode||'<>'||cdh.keycode /*                                    #*/ end keycode_diff
, case when icd.src_cd = cdh.src_cd then '' else icd.src_cd||'<>'||cdh.src_cd /*                                        #*/ end src_cd_diff
, case when icd.ord_prod_amt = cdh.ord_prod_amt then '' else icd.ord_prod_amt||'<>'||cdh.ord_prod_amt /*                #*/ end ord_prod_amt_diff
, case when icd.ord_ship_amt = cdh.ord_ship_amt then '' else icd.ord_ship_amt||'<>'||cdh.ord_ship_amt /*                #*/ end ord_ship_amt_diff
, case when icd.ord_tax_amt = cdh.ord_tax_amt then '' else icd.ord_tax_amt||'<>'||cdh.ord_tax_amt /*                    #*/ end ord_tax_amt_diff
, case when icd.ord_misc_amt = cdh.ord_misc_amt then '' else icd.ord_misc_amt||'<>'||cdh.ord_misc_amt /*                #*/ end ord_misc_amt_diff
, case when icd.ord_gft_cert_amt = cdh.ord_gft_cert_amt then '' else icd.ord_gft_cert_amt||'<>'||cdh.ord_gft_cert_amt /*#*/ end ord_gft_cert_amt_diff
, case when icd.ord_coupn_amt = cdh.ord_coupn_amt then '' else icd.ord_coupn_amt||'<>'||cdh.ord_coupn_amt /*            #*/ end ord_coupn_amt_diff
, case when icd.ord_disc_amt = cdh.ord_disc_amt then '' else icd.ord_disc_amt||'<>'||cdh.ord_disc_amt /*                #*/ end ord_disc_amt_diff
, case when icd.ord_value_amt = cdh.ord_value_amt then '' else icd.ord_value_amt||'<>'||cdh.ord_value_amt /*            #*/ end ord_value_amt_diff
, case when icd.istlmt_cd = cdh.istlmt_cd then '' else icd.istlmt_cd||'<>'||cdh.istlmt_cd /*                            #*/ end istlmt_cd_diff
, case when icd.gift_ord_cd = cdh.gift_ord_cd then '' else icd.gift_ord_cd||'<>'||cdh.gift_ord_cd /*                    #*/ end gift_ord_cd_diff
, case when icd.currncy_typ_cd = cdh.currncy_typ_cd then '' else icd.currncy_typ_cd||'<>'||cdh.currncy_typ_cd /*        #*/ end currncy_typ_cd_diff
, case when icd.crd_stat_cd = cdh.crd_stat_cd then '' else icd.crd_stat_cd||'<>'||cdh.crd_stat_cd /*                    #*/ end crd_stat_cd_diff
, case when icd.pmt_amt = cdh.pmt_amt then '' else icd.pmt_amt||'<>'||cdh.pmt_amt /*                                    #*/ end pmt_amt_diff
, case when icd.lst_pmt_dt = cdh.lst_pmt_dt then '' else icd.lst_pmt_dt||'<>'||cdh.lst_pmt_dt /*                        #*/ end lst_pmt_dt_diff
, case when icd.lst_pmt_ret_dt = cdh.lst_pmt_ret_dt then '' else icd.lst_pmt_ret_dt||'<>'||cdh.lst_pmt_ret_dt /*        #*/ end lst_pmt_ret_dt_diff
, case when icd.wr_off_amt = cdh.wr_off_amt then '' else icd.wr_off_amt||'<>'||cdh.wr_off_amt /*                        #*/ end wr_off_amt_diff
, case when icd.lst_wr_off_dt = cdh.lst_wr_off_dt then '' else icd.lst_wr_off_dt||'<>'||cdh.lst_wr_off_dt /*            #*/ end lst_wr_off_dt_diff
, case when icd.coll_amt = cdh.coll_amt then '' else icd.coll_amt||'<>'||cdh.coll_amt /*                                #*/ end coll_amt_diff
, case when icd.lst_coll_dt = cdh.lst_coll_dt then '' else icd.lst_coll_dt||'<>'||cdh.lst_coll_dt /*                    #*/ end lst_coll_dt_diff

, '||                    ICD                    >>' as sep1, icd.*
, '||                    CDH                    >>' as sep2, cdh.*

from  icd full join cdh
on icd.icd_id = cdh.icd_id and icd.cdh_id = cdh.cdh_id
and icd.ord_num = cdh.ord_num

------------------------------------------------------------------------RUN ALL ABOVE
;
drop table  cr_temp.diff_MT_BOOKS_ORD
;
select * from   cr_temp.GTT_MT_BOOKS_ORD  where icd_id = xxxxxxxxxxxxxxxxxxxxxx
;
select * from   cr_temp.gtt_agg_books_order  where cdh_id = xxxxxxxxxxxxxxxxxxxxxx
;
select count(*) from   cr_temp.GTT_MT_BOOKS_ORD
;
select count(*) from   cr_temp.gtt_agg_books_order
;
--truncate table    cr_temp.GTT_MT_BOOKS_ORD;
drop table  cr_temp.GTT_MT_BOOKS_ORD;
--truncate table cr_temp.gtt_agg_books_order;
drop table  cr_temp.gtt_agg_books_order;


