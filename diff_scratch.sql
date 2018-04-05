with tt1 (cnt_icd_fl, cnt_cdh_fl, icd_id, cdh_id, ord_num, dnr_recip_cd, ord_prem_flg, ord_cdr_flg, entr_typ_cd, trn_dt, ord_dt, keycode, src_cd, ord_prod_amt, ord_ship_amt, ord_tax_amt, ord_misc_amt, ord_gft_cert_amt, ord_coupn_amt, ord_disc_amt, ord_value_amt, istlmt_cd, gift_ord_cd, currncy_typ_cd, crd_stat_cd, pmt_amt, lst_pmt_dt, lst_pmt_ret_dt, wr_off_amt, lst_wr_off_dt, coll_amt, lst_coll_dt) as (
select '1', '0', '10002000020166100', '1201171715', '106038160640001', '(NULL)', 'N', 'Y', 'B', '07-FEB-06', '07-FEB-06', 'DA5CB102', '1', '44', '3', '0', '0', '0', '0', '0', '48', 'N', 'A', '(NULL)', 'E', '0', '19-JUL-06', '01-AUG-06', '48', '30-AUG-06', '0', '(NULL)'  union all
select '0', '1', '10002000020166100', '1201171715', '106038160640001', '(NULL)', 'N', 'Y', 'B', '07-FEB-06', '07-FEB-06', 'DA5CB102', '1', '44', '3', '0', '0', '0', '0', '0', '0', 'N', 'A', '(NULL)', 'E', '0', '19-JUL-06', '01-AUG-06', '48', '30-AUG-06', '0', '(NULL)' union all
select '0', '1', '10002000020166100', '1201171715', '106038160655555', '(NULL)', 'N', 'Y', 'B', '07-FEB-06', '07-FEB-06', 'DA5CB102', '1', '44', '3', '0', '0', '0', '0', '0', '0', 'N', 'A', '(NULL)', 'E', '0', '19-JUL-06', '01-AUG-06', '48', '30-AUG-06', '0', '(NULL)' 
)


select * 

from (
select * from tt1 where 1=1 and cnt_icd_fl = 1) full join (select * from tt1 
where 1=1 and cnt_cdh_fl = 1