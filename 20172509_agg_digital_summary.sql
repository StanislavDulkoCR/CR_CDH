DROP TABLE IF EXISTS cr_temp.digital_external_ref_temp;
CREATE TABLE cr_temp.digital_external_ref_temp
	DISTSTYLE KEY DISTKEY(individual_id) 
	INTERLEAVED SORTKEY(individual_id)  
as 
select distinct INDIVIDUAL_ID, HASH_ACCOUNT_ID                             
from PROD.account 
where INDIVIDUAL_ID is not null
;
DROP TABLE IF EXISTS cr_temp.ind_xref;
CREATE TABLE cr_temp.ind_xref
	DISTSTYLE KEY DISTKEY(individual_id) 
	INTERLEAVED SORTKEY(individual_id) 
as 
select distinct INDIVIDUAL_ID                   
from PROD.individual 
where INDIVIDUAL_ID is not null
;


CREATE TABLE cr_temp.mt_onl_sum_temp1
AS
SELECT
     t2.individual_id,
     t2.hash_account_id,
     t2.acct_id,
     t2.itm_id,
     t2.mag_cd,
     t2.set_cd,
     t2.strt_dt,
     t2.end_dt,
     t2.crt_dt,
     first_value(crmg_brk_dt) OVER (PARTITION BY t2.individual_id
                                        ORDER BY crmg_brk_dt desc nulls last
                                        rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_crmg_brk_dt,
     nth_value(crmg_brk_dt,2) OVER (PARTITION BY t2.individual_id
                                        ORDER BY crmg_brk_dt desc nulls last
                                    rows between unbounded preceding and unbounded following) prior_crmg_brk_dt,
     first_value(cro_brk_dt ) OVER (PARTITION BY t2.individual_id
                                        ORDER BY cro_brk_dt desc nulls last rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_cro_brk_dt,
     nth_value(cro_brk_dt ,2) OVER (PARTITION BY t2.individual_id
                                        ORDER BY cro_brk_dt desc nulls last
                                    rows between unbounded preceding and unbounded following) prior_cro_brk_dt,
     first_value(cro_acct_brk_dt ) OVER (PARTITION BY t2.individual_id, t2.hash_account_id
                                        ORDER BY cro_acct_brk_dt desc nulls last rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_cro_acct_brk_dt,
     count(crmg_brk_dt) over (partition by individual_id) crmg_brks_cnt,
     count(cro_brk_dt) over (partition by individual_id) cro_brks_cnt,
     cro_actv
   FROM
     (SELECT
        t1.individual_id,
        t1.hash_account_id,
        t1.itm_id,
		    t1.mag_cd,
		    t1.set_cd,
		    t1.strt_dt,
		    t1.end_dt,
		    t1.crt_dt,
        CASE WHEN t1.mag_cd = 'CRMG'
              AND nvl(t1.set_cd, 'Z') NOT IN ('B', 'D')
              AND substring(nvl(t1.ext_keycd, nvl(t1.int_keycd, 'ZZ')),1,2) != 'WF'
             THEN CASE WHEN months_between(t1.strt_dt,
                                           nvl(max(t1.end_dt) over (PARTITION BY
                                                              t1.individual_id,
                                                              t1.mag_cd,
                                                              case when nvl(t1.set_cd, 'Z') NOT IN ('B', 'D') then 1 else 0 end
                                                            ORDER BY t1.strt_dt, nvl(t1.ext_keycd, t1.int_keycd)
                                                            rows BETWEEN unbounded preceding and 1 PRECEDING)
                                               ,t1.strt_dt)) > 2
                       THEN max(t1.end_dt) over (PARTITION BY
                                                              t1.individual_id,
                                                              t1.mag_cd,
                                                              case when nvl(t1.set_cd, 'Z') NOT IN ('B', 'D') then 1 else 0 end
                                                            ORDER BY t1.strt_dt, nvl(t1.ext_keycd, t1.int_keycd)
                                                            rows BETWEEN unbounded preceding and 1 PRECEDING)
                       ELSE null
                  END
             ELSE null
        END crmg_brk_dt,
        CASE WHEN t1.mag_cd = 'CRO'
              AND t1.set_cd NOT IN ('B', 'D')
             THEN CASE WHEN decode(t1.term_mth_cnt,1,1,2)
                              != lead(decode(t1.term_mth_cnt,1,1,2)) over (partition by t1.individual_id
                                                                                      , t1.mag_cd
                                                                                      , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
                                                                               order by t1.strt_dt, t1.end_dt, t1.itm_id)
                       THEN t1.end_dt
                       WHEN t1.term_mth_cnt > 1
                        AND lead(t1.strt_dt) over (partition by t1.individual_id
	                                                            , t1.mag_cd
	                                                            , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
	                                                     order by t1.strt_dt, t1.end_dt, t1.itm_id)
	                            - t1.end_dt > 49
                       THEN t1.end_dt
                       WHEN t1.term_mth_cnt = 1
                        AND lead(t1.strt_dt) over (partition by t1.individual_id
	                                                            , t1.mag_cd
	                                                            , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
	                                                     order by t1.strt_dt, t1.end_dt, t1.itm_id)
	                            - t1.end_dt > 25
                       THEN t1.end_dt
                  END
        END cro_brk_dt,
        CASE WHEN t1.mag_cd = 'CRO'
              AND t1.set_cd NOT IN ('B', 'D')
             THEN CASE WHEN decode(t1.term_mth_cnt,1,1,2)
                              != lead(decode(t1.term_mth_cnt,1,1,2)) over (partition by t1.individual_id
                                                                                      , t1.hash_account_id
                                                                                      , t1.mag_cd
                                                                                      , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
                                                                               order by t1.strt_dt, t1.end_dt, t1.itm_id)
                       THEN nvl(to_char(t1.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || nvl(to_char(t1.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || NVL(lpad(t1.itm_id,21),'0')
                       WHEN t1.term_mth_cnt > 1
                        AND lead(t1.strt_dt) over (partition by t1.individual_id
                                                              , t1.hash_account_id
	                                                            , t1.mag_cd
	                                                            , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
	                                                     order by t1.strt_dt, t1.end_dt, t1.itm_id)
	                            - t1.end_dt > 49
                       THEN nvl(to_char(t1.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || nvl(to_char(t1.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || NVL(lpad(t1.itm_id,21),'0')
                       WHEN t1.term_mth_cnt = 1
                        AND lead(t1.strt_dt) over (partition by t1.individual_id
                                                              , t1.hash_account_id
	                                                            , t1.mag_cd
	                                                            , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
	                                                     order by t1.strt_dt, t1.end_dt, t1.itm_id)
	                            - t1.end_dt > 25
                       THEN nvl(to_char(t1.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || nvl(to_char(t1.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || NVL(lpad(t1.itm_id,21),'0')
                  END
        END cro_acct_brk_dt,
        case when ((t1.svc_stat_cd = 'A'
                       AND olo.stat_cd = 'C')
                     OR (t1.svc_stat_cd = 'C'
                         AND olo.stat_cd = 'R'
                         AND t1.sub_rnw_ind = 'N'
                         AND t1.end_dt > sysdate))
	            AND t1.mag_cd = 'CRO'
	            AND nvl(olo.entr_typ_cd,'Z') != 'F'
	            AND t1.tot_amt > 0.01
	            AND t1.term_mth_cnt > 0
	           then 'Y'
             else 'N'            
        end cro_actv
      FROM prod.agg_digital_item t1 
      left join prod.agg_digital_order olo on t1.hash_account_id = olo.hash_account_id and t1.ord_id = olo.ord_id
      ) t2

      
;
CREATE TABLE middle_tier2.mt_online_summary
AS
SELECT
  ix.individual_id,
  ix.household_id,
  MAX(CASE WHEN oli.mag_cd = 'CRMG'
            AND ((oli.svc_stat_cd = 'A'
                  AND olo.stat_cd = 'C')
                 OR (oli.svc_stat_cd = 'C'
                     AND olo.stat_cd = 'R'
                     AND oli.sub_rnw_ind = 'N'
                     AND oli.end_dt > sysdate))
            AND nvl(olo.entr_typ_cd,'Z') != 'F'
            AND oli.sku_num > 5000000
            AND oli.tot_amt > 0.01
            AND oli.term_mth_cnt > 0
            AND oli.acct_id = t2.crmg_comb_acct_id
           THEN 'Y'
           ELSE 'N'            
      END) crmg_actv_flg,
  MAX(CASE WHEN oli.mag_cd = 'CRO'
            AND ((oli.set_cd in ('C','E')
                  AND oli.svc_stat_cd = 'A'
                  AND olo.stat_cd = 'C'
                  AND oli.stat_cd = 'C')
                 OR (((oli.svc_stat_cd = 'A'
                       AND olo.stat_cd = 'C')
                     OR (oli.svc_stat_cd = 'C'
                         AND olo.stat_cd = 'R'
                         AND oli.sub_rnw_ind = 'N'
                         AND oli.end_dt > sysdate))
                     AND (nvl(olo.entr_typ_cd,'Z') != 'F'
                     AND oli.tot_amt > 0.01
                     AND oli.term_mth_cnt > 0)))
            AND oli.acct_id = t2.cro_comb_acct_id
           THEN 'Y'
           ELSE 'N'            
      END) cro_actv_flg,
  max(CASE WHEN oli.stat_cd = 'C'
            AND oli.mag_cd IN ('NCPR','UCPR')
            AND oli.crt_dt >= add_months(sysdate,-12)
           THEN 'Y'
           WHEN oli.stat_cd = 'C'
            AND oli.mag_cd IN ('NCBK','UCBK')
            AND oli.strt_dt BETWEEN ADD_MONTHS(SYSDATE,-12) AND SYSDATE
           THEN 'Y'
           ELSE 'N'
      END) aps_actv_pd_flg,
   max(CASE WHEN oli.stat_cd = 'C'
            AND oli.mag_cd IN ('NCPR','UCPR')
            AND oli.crt_dt >= add_months(sysdate,-12)
           THEN 'Y'
           ELSE 'N'
      END) aps_rpts_actv_pd_flg,
  MAX(CASE WHEN oli.mag_cd = 'CRMG'
            AND ((oli.svc_stat_cd = 'A'
                  AND olo.stat_cd = 'C')
                 OR (oli.svc_stat_cd = 'C'
                     AND olo.stat_cd = 'R'
                     AND oli.sub_rnw_ind = 'N'
                     AND oli.end_dt > sysdate))
            AND nvl(olo.entr_typ_cd,'Z') != 'F'
            AND oli.sku_num > 5000000
            AND oli.tot_amt > 0.01
            AND oli.term_mth_cnt > 0
            AND oli.acct_id = t2.crmg_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) crmg_actv_pd_flg,
  MAX(CASE WHEN oli.mag_cd = 'CRO'
            AND ((oli.svc_stat_cd = 'A'
                 AND olo.stat_cd = 'C')
                OR (oli.svc_stat_cd = 'C'
                     AND olo.stat_cd = 'R'
                     AND oli.sub_rnw_ind = 'N'
                     AND oli.end_dt > sysdate))
            AND nvl(olo.entr_typ_cd,'Z') != 'F'
            AND oli.tot_amt > 0.01
            AND oli.term_mth_cnt > 0
            AND oli.acct_id = t2.cro_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) cro_actv_pd_flg,
  max(CASE WHEN oli.stat_cd = 'C'
            AND oli.strt_dt BETWEEN ADD_MONTHS(SYSDATE,-12) AND SYSDATE
            AND oli.mag_cd = 'NCBK'
           THEN 'Y'
           ELSE 'N'
      END) ncbk_actv_pd_flg,
  max(CASE WHEN oli.stat_cd = 'C'
           AND oli.strt_dt BETWEEN ADD_MONTHS(SYSDATE,-12) AND SYSDATE
            AND oli.mag_cd = 'UCBK'
           THEN 'Y'
           ELSE 'N'
      END) ucbk_actv_pd_flg,
  MAX(CASE WHEN oli.mag_cd = 'CRMG'
            AND ((oli.svc_stat_cd = 'A'
                  AND olo.stat_cd = 'C')
                 OR (oli.svc_stat_cd = 'C'
                      AND olo.stat_cd = 'R'
                      AND oli.sub_rnw_ind = 'N'
                      AND oli.end_dt > sysdate))
            AND ((sku.selection = 'A'
            AND sku.product = 'CRMG')
                 OR (sku.selection = 'X'
                     AND sku.product = 'CRMG'
                     AND oli.term_mth_cnt > 1))
            AND oli.sku_num > 5000000
            AND oli.tot_amt > 0.01
            AND oli.term_mth_cnt > 0
            AND nvl(olo.entr_typ_cd,'Z') != 'F'
            AND oli.acct_id = t2.crmg_comb_acct_id
      THEN 'Y'
      ELSE 'N'
      END) crmg_annual_flg,
  MAX(CASE WHEN oli.mag_cd = 'CRO'
            AND ((oli.svc_stat_cd = 'A'
                  AND olo.stat_cd = 'C')
                 OR (oli.svc_stat_cd = 'C'
                      AND olo.stat_cd = 'R'
                      AND oli.sub_rnw_ind = 'N'
                      AND oli.end_dt > sysdate))
            AND ((sku.selection = 'A'
                  AND sku.product = 'CRO')
                 OR (oli.sku_num is null
                     AND oli.term_mth_cnt > 1)
                 OR (sku.selection = 'X'
                     AND sku.product = 'CRO'
                     AND oli.term_mth_cnt > 1))
            AND oli.tot_amt > 0.01
            AND oli.term_mth_cnt > 0
            AND nvl(olo.entr_typ_cd,'Z') != 'F'
            AND oli.acct_id = t2.cro_comb_acct_id
      THEN 'Y'
      ELSE 'N'
      END) cro_annual_flg,
  CASE WHEN substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                            THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                                         oli.crd_stat_cd
                            ELSE null
                       END),9) = 'F'
       THEN 'Y'
       ELSE 'N'
  END crmg_canc_bad_dbt_flg,
  CASE         
      WHEN substr(max(CASE WHEN oli.mag_cd = 'CRO' 
                           THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                            oli.CRD_STAT_CD
                           else null
                           end),9) = 'F'
      THEN
        'Y'  
      WHEN substr(max(CASE WHEN oli.mag_cd = 'CRO'
                            THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                                 oli.svc_stat_cd ||
                                 oli.canc_rsn_cd
                            ELSE null
                       END),9) IN ('C50','C06')
       THEN 'Y'
       ELSE 'N'
  END cro_canc_bad_dbt_flg,  
  CASE WHEN substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                            THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                                 oli.stat_cd
                            ELSE null
                       END),9,1) = 'R'
       THEN 'Y'
       ELSE 'N'
  END crmg_canc_cust_flg,
  CASE WHEN
            substr(max(CASE WHEN oli.mag_cd = 'CRO' 
                       THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                             oli.stat_cd
                       ELSE null
                       END),9,1) = 'R'
            OR (          
            substr(max(CASE WHEN oli.mag_cd = 'CRO'
                            THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                                 oli.svc_stat_cd ||
                                 oli.canc_rsn_cd
                            ELSE null
                       END),9,1) = 'C'
        AND substr(max(CASE WHEN oli.mag_cd = 'CRO'
                            THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                                 oli.svc_stat_cd ||
                                 nvl(oli.canc_rsn_cd,'xx')
                            ELSE null
                       END),10,2) NOT IN ('50','06'))
       THEN 'Y'
       ELSE 'N'
  END cro_canc_cust_flg,
  substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                       oli.crd_stat_cd
                  ELSE null
             END),9) crmg_cr_stat_cd,
  substr(max(CASE WHEN oli.mag_cd = 'CRO'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                       oli.crd_stat_cd
                  ELSE null
             END),9) cro_cr_stat_cd,
  max(CASE WHEN oli.mag_cd = 'CRMG'
           THEN oli.end_dt
           ELSE null
      END) crmg_exp_dt,
  max(CASE WHEN oli.mag_cd = 'CRO'
           THEN nvl(oli.cro_acct_exp_dt, oli.end_dt)
           ELSE null
      END) cro_exp_dt,
  to_date(substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                           AND oli.crd_stat_cd = 'F'
                          THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                               to_char(oli.canc_dt,'YYYYMMDD')
                          ELSE null
                     END),9),'YYYYMMDD') crmg_lst_canc_bad_dbt_dt,
  to_date(substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                           AND oli.stat_cd = 'R'
                          THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                               to_char(oli.canc_dt,'YYYYMMDD')
                          ELSE null
                     END),9),'YYYYMMDD') crmg_lst_canc_cust_dt,                   
  to_date(substr(max(CASE WHEN oli.mag_cd = 'CRO'
                           AND ((oli.stat_cd = 'R')
                           OR  (oli.svc_stat_cd = 'C'
                           AND nvl(oli.canc_rsn_cd,'00') NOT IN ('50','06')))
                          THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                               to_char(nvl(oli.canc_dt,oli.cro_acct_exp_dt),'YYYYMMDD')
                          ELSE null
                     END),9),'YYYYMMDD') cro_lst_canc_cust_dt,  
  max(CASE WHEN oli.mag_cd IN ('NCBK', 'UCBK', 'NCPR', 'UCPR')
           THEN NULLIF(GREATEST(nvl(ixo.dc_last_aps_ord_dt,to_date('19000101','YYYYMMDD')), nvl(oli.crt_dt,to_date('19000101','YYYYMMDD'))),to_date('19000101','YYYYMMDD'))
           ELSE
            NULLIF(nvl(ixo.dc_last_aps_ord_dt,to_date('19000101','YYYYMMDD')),to_date('19000101','YYYYMMDD'))
      END) aps_lst_ord_dt,
  max(CASE WHEN oli.mag_cd = 'NCPR'
           THEN oli.crt_dt
           ELSE null
      END) nc_rpts_lst_ord_dt,
  max(CASE WHEN oli.mag_cd = 'NCBK'
           THEN oli.crt_dt
           ELSE null
      END) ncbk_lst_ord_dt,
  max(CASE WHEN oli.mag_cd = 'UCPR'
           THEN oli.crt_dt
           ELSE null
      END) uc_rpts_lst_ord_dt,
  max(CASE WHEN oli.mag_cd = 'UCBK'
           THEN oli.crt_dt
           ELSE null
      END) ucbk_lst_ord_dt,
  max(CASE WHEN oli.mag_cd = 'CRMG'
           THEN oli.pmt_dt
           ELSE null
      END) crmg_lst_pmt_dt,
  max(CASE WHEN oli.mag_cd = 'CRO'
           THEN oli.pmt_dt
           ELSE null
      END) cro_lst_pmt_dt,
  substr(max(CASE WHEN oli.mag_cd = 'NCPR'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       oli.shp_meth_cd
                  ELSE null
             END),15) nc_rpts_lst_del_chnl_cd,
  substr(max(CASE WHEN oli.mag_cd = 'UCPR'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       oli.shp_meth_cd
                  ELSE null
             END),15) uc_rpts_lst_del_chnl_cd,
  max(
      CASE WHEN ixo.dc_last_aps_ord_dt is not null then
        'Y'
          WHEN oli.mag_cd IN ('NCPR', 'UCPR', 'NCBK', 'UCBK')
           THEN 'Y'
           ELSE 'N'
      END) aps_flg,
  max(CASE WHEN oli.mag_cd = 'CRMG'
           THEN 'Y'
           ELSE 'N'
      END) crmg_flg,
  max(CASE WHEN oli.mag_cd = 'CRO'
           THEN 'Y'
           ELSE 'N'
      END) cro_flg,
  max(CASE WHEN oli.mag_cd = 'NCPR'
           THEN 'Y'
           ELSE 'N'
      END) nc_rpts_flg,
  max(CASE WHEN oli.mag_cd = 'NCBK'
           THEN 'Y'
           ELSE 'N'
      END) ncbk_flg,
  max(CASE WHEN oli.mag_cd = 'UCPR'
           THEN 'Y'
           ELSE 'N'
      END) uc_rpts_flg,
  max(CASE WHEN oli.mag_cd = 'UCBK'
           THEN 'Y'
           ELSE 'N'
      END) ucbk_flg,
  substr(min(CASE WHEN oli.mag_cd = 'CRMG'
                   AND nvl(oli.set_cd, 'Z') NOT IN ('B', 'D')
                   AND oli.strt_dt > nvl(t1.last_crmg_brk_dt,to_date('00010101','YYYYMMDD'))                
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'99999999') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),9) crmg_curr_mbr_keycode,
  substr(min(CASE WHEN oli.mag_cd = 'CRO'
                   AND oli.set_cd NOT IN ('B', 'D')
                   AND nvl(to_char(oli.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                    || nvl(to_char(oli.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                    || lpad(oli.itm_id,21) > nvl(t1.last_cro_acct_brk_dt,'0000000000000000000000000000')
                   AND oli.acct_id = t1.cro_curr_mbr_dt_acct_id
                  THEN nvl(to_char(oli.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                    || nvl(to_char(oli.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                    || lpad(oli.itm_id,21)
                    || nvl(oli.ext_keycd,oli.int_keycd)
             END),50) cro_curr_mbr_keycode,
  SUBSTR(MAX(CASE WHEN oli.mag_cd = 'CRMG'
                  AND oli.svc_stat_cd = 'A'
                  AND oli.sku_num > 5000000
                  AND olo.stat_cd = 'C'
                  AND oli.acct_id = t2.crmg_comb_acct_id
                 THEN NVL(TO_CHAR(oli.crt_dt,'YYYYMMDD'),'00000000') || 
                               nvl(oli.ext_keycd,oli.int_keycd)
                 ELSE NULL
             END),9) crmg_curr_ord_keycode,
  SUBSTR(MAX(CASE WHEN oli.mag_cd = 'CRO'
                  AND oli.svc_stat_cd = 'A'
                  AND olo.stat_cd = 'C'
                  AND oli.acct_id = t2.cro_comb_acct_id
                 THEN NVL(TO_CHAR(oli.crt_dt,'YYYYMMDD'),'00000000') || 
                               nvl(oli.ext_keycd,oli.int_keycd)
                 ELSE NULL
             END),9) cro_curr_ord_keycode,
  substr(min(CASE WHEN oli.mag_cd = 'CRMG'   
                  AND nvl(oli.set_cd, 'Z') NOT IN ('B', 'D')
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'99999999') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),9) crmg_fst_mbr_keycode,
  substr(min(CASE WHEN oli.mag_cd = 'CRO'
                  AND oli.set_cd NOT IN ('B', 'D')
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'99999999') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),9) cro_fst_mbr_keycode,
  substr(min(CASE WHEN oli.mag_cd IN ('NCPR', 'UCPR', 'NCBK', 'UCBK')
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),15) aps_fst_ord_keycode,
  substr(min(CASE WHEN oli.mag_cd = 'NCPR'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),15) nc_rpts_fst_ord_keycode,
  substr(min(CASE WHEN oli.mag_cd = 'NCBK'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),15) ncbk_fst_ord_keycode,
  substr(min(CASE WHEN oli.mag_cd = 'UCPR'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),15) uc_rpts_fst_ord_keycode,
  substr(min(CASE WHEN oli.mag_cd = 'UCBK'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),15) ucbk_fst_ord_keycode,
  substr(max(CASE WHEN oli.mag_cd IN ('NCPR', 'UCPR', 'NCBK', 'UCBK')
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),15) aps_lst_ord_keycode,
  substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),9) crmg_lst_ord_keycode,
  substr(max(CASE WHEN oli.mag_cd = 'CRO'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),9) cro_lst_ord_keycode,
  substr(max(CASE WHEN oli.mag_cd = 'NCPR'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),15) nc_rpts_lst_ord_keycode,
  substr(max(CASE WHEN oli.mag_cd = 'NCBK'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),15) ncbk_lst_ord_keycode,
  substr(max(CASE WHEN oli.mag_cd = 'UCPR'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),15) uc_rpts_lst_ord_keycode,
  substr(max(CASE WHEN oli.mag_cd = 'UCBK'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       nvl(oli.ext_keycd,oli.int_keycd)
                  ELSE null
             END),15) ucbk_lst_ord_keycode,
  to_number(substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                            THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                                 oli.tot_gross_amt
                            ELSE null
                       END),9)) crmg_lst_sub_amt,
  to_number(substr(max(CASE WHEN oli.mag_cd = 'CRO'
                            THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                                 oli.tot_gross_amt
                            ELSE null
                       END),9)) cro_lst_sub_amt,
  MAX(CASE WHEN oli.mag_cd = 'CRMG'
            AND ((oli.svc_stat_cd = 'A'
                  AND olo.stat_cd = 'C')
                 OR (oli.svc_stat_cd = 'C'
                      AND olo.stat_cd = 'R'
                      AND oli.sub_rnw_ind = 'N'
                      AND oli.end_dt > sysdate))
            AND sku.product = 'CRMG'
            AND sku.selection = 'M'
            AND nvl(olo.entr_typ_cd,'Z') != 'F'
            AND oli.sku_num > 5000000
            AND oli.tot_amt > 0.01
            AND oli.term_mth_cnt > 0
            AND oli.acct_id = t2.crmg_comb_acct_id
       THEN 'Y'
       ELSE 'N'
  END) crmg_monthly_flg,
 MAX(CASE WHEN oli.mag_cd = 'CRO'
            AND ((oli.svc_stat_cd = 'A'
                  AND olo.stat_cd = 'C')
                 OR (oli.svc_stat_cd = 'C'
                      AND olo.stat_cd = 'R'
                      AND oli.sub_rnw_ind = 'N'
                      AND oli.end_dt > sysdate))
            AND ((sku.selection = 'M'
                  AND sku.product = 'CRO')
                 OR (oli.sku_num is null
                     AND oli.term_mth_cnt = 1))
            AND nvl(olo.entr_typ_cd,'Z') != 'F'
            AND oli.tot_amt > 0.01
            AND oli.term_mth_cnt > 0
            AND oli.acct_id = t2.cro_comb_acct_id
       THEN 'Y'
       ELSE 'N'
  END) cro_monthly_flg,
  MAX(CASE WHEN oli.mag_cd = 'CRO'
            AND oli.acct_id = t2.cro_comb_acct_id
            AND oli.svc_stat_cd = 'D'
            AND oli.set_cd IN ('B','D')
            AND oli.crt_dt > add_months(sysdate,-12)
       THEN 'Y'
       ELSE 'N'
  END) cro_non_sub_dnr_flg,
  max(t1.crmg_brks_cnt) crmg_brks_cnt,
  max(t1.cro_brks_cnt) cro_brks_cnt,
  trunc(count(CASE WHEN oli.mag_cd = 'CRMG'
                    AND nvl(oli.set_cd, 'Z') NOT IN ('B', 'D')
                    AND oli.strt_dt > nvl(t1.last_crmg_brk_dt,to_date('00010101','YYYYMMDD'))
                   THEN 'x'
                   ELSE null
              END) - .1) crmg_rnw_cnt,
  trunc(count(CASE WHEN oli.mag_cd = 'CRO'
                    AND oli.set_cd NOT IN ('B', 'D')
                    AND nvl(oli.crd_stat_cd,'x') != 'F'
                    AND nvl(oli.canc_rsn_cd,'00') NOT IN ('50','06')
                    AND nvl(to_char(oli.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                     || nvl(to_char(oli.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                     || lpad(oli.itm_id,21) > nvl(t1.last_cro_acct_brk_dt,'0000000000000000000000000000')
                    AND oli.acct_id = t1.cro_curr_mbr_dt_acct_id
                   THEN 'x'
                   ELSE null
              END) - .1) cro_rnw_cnt,
  MAX(CASE WHEN oli.mag_cd = 'CRO'
            AND oli.svc_stat_cd = 'A'
            AND oli.set_cd IN ('C','E')
            AND oli.stat_cd = 'C'
            AND olo.stat_cd = 'C'
            AND oli.acct_id = t2.cro_comb_acct_id
       THEN 'Y'
       ELSE 'N'
  END) cro_rec_flg,
  CASE WHEN ((MAX(nvl(an.crmg_lt_sub_flg,'N')) = 'Y') OR
       (MAX(CASE WHEN oli.mag_cd = 'CRMG'
                      AND oli.svc_stat_cd = 'A'
                      AND oli.sku_num > 5000000
                      AND olo.stat_cd = 'C'
                      AND oli.acct_id = t2.crmg_comb_acct_id
                     THEN '1'
                     ELSE '0'
                END) = '1'))
       THEN 'A'
       ELSE SUBSTR(MAX(CASE  WHEN oli.mag_cd = 'CRMG'
                              AND oli.acct_id = t2.crmg_comb_acct_id
                             THEN NVL(TO_CHAR(oli.crt_dt,'YYYYMMDD'),'00000000') ||
                                           decode(oli.svc_stat_cd,'A','C',oli.svc_stat_cd)            
                       END),9)
  END crmg_svc_stat_cd,
 CASE WHEN ((MAX(nvl(an.cro_lt_sub_flg,'N')) = 'Y') OR
       (MAX(CASE WHEN oli.mag_cd = 'CRO'
                      AND oli.svc_stat_cd = 'A'
                      AND olo.stat_cd = 'C'
                      AND oli.acct_id = t2.cro_comb_acct_id
                     THEN '1'
                     ELSE '0'
                END) = '1'))
       THEN 'A'
       ELSE SUBSTR(MAX(CASE  WHEN oli.mag_cd = 'CRO'
                              AND oli.acct_id = t2.cro_comb_acct_id
                             THEN NVL(TO_CHAR(oli.crt_dt,'YYYYMMDD'),'00000000') ||
                                           decode(oli.svc_stat_cd,'A','C',oli.svc_stat_cd)            
                       END),9)
  END cro_svc_stat_cd,
  substr(min(CASE WHEN oli.mag_cd = 'CRMG'
                   AND nvl(oli.set_cd, 'Z') NOT IN ('B', 'D')
                   AND oli.strt_dt > nvl(t1.last_crmg_brk_dt,to_date('00010101','YYYYMMDD'))
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'99999999') ||
                       oli.sub_src_cd
                  ELSE null
             END),9) crmg_curr_mbr_src_cd,
  substr(min(CASE WHEN oli.mag_cd = 'CRO'
                   AND oli.set_cd NOT IN ('B', 'D')
                   AND nvl(to_char(oli.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                    || nvl(to_char(oli.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                    || lpad(oli.itm_id,21) > nvl(t1.last_cro_acct_brk_dt,'0000000000000000000000000000')
                   AND oli.acct_id = t1.cro_curr_mbr_dt_acct_id
                  THEN nvl(to_char(oli.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                    || nvl(to_char(oli.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                    || lpad(oli.itm_id,21)
                    || oli.sub_src_cd
                  ELSE null
             END),50) cro_curr_mbr_src_cd,
 SUBSTR(MAX(CASE WHEN oli.mag_cd = 'CRMG'
                  AND oli.svc_stat_cd = 'A'
                  AND oli.sku_num > 5000000
                  AND olo.stat_cd = 'C'
                  AND oli.acct_id = t2.crmg_comb_acct_id
                 THEN NVL(TO_CHAR(oli.crt_dt,'YYYYMMDD'),'00000000') ||
                                           oli.sub_src_cd
                               ELSE NULL            
            END),9) crmg_curr_ord_src_cd,
  SUBSTR(MAX(CASE WHEN oli.mag_cd = 'CRO'
                  AND oli.svc_stat_cd = 'A'
                  AND olo.stat_cd = 'C'
                  AND oli.acct_id = t2.cro_comb_acct_id
                 THEN NVL(TO_CHAR(oli.crt_dt,'YYYYMMDD'),'00000000') ||
                                           oli.sub_src_cd
                               ELSE NULL            
            END),9) cro_curr_ord_src_cd,
  substr(min(CASE WHEN oli.mag_cd IN ('NCPR', 'UCPR', 'NCBK', 'UCBK')
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       oli.aps_src_cd
                  ELSE null
             END),15) aps_fst_ord_src_cd,
  substr(min(CASE WHEN oli.mag_cd = 'CRMG'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'99999999') ||
                       oli.sub_src_cd
                  ELSE null
             END),9) crmg_fst_ord_src_cd,
  substr(min(CASE WHEN oli.mag_cd = 'CRO'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'99999999') ||
                       oli.sub_src_cd
                  ELSE null
             END),9) cro_fst_ord_src_cd,
  substr(min(CASE WHEN oli.mag_cd = 'NCPR'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       oli.aps_src_cd
                  ELSE null
             END),15) nc_rpts_fst_ord_src_cd,
  substr(min(CASE WHEN oli.mag_cd = 'NCBK'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       oli.aps_src_cd
                  ELSE null
             END),15) ncbk_fst_ord_src_cd,
  substr(min(CASE WHEN oli.mag_cd = 'UCPR'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       oli.aps_src_cd
                  ELSE null
             END),15) uc_rpts_fst_ord_src_cd,
  substr(min(CASE WHEN oli.mag_cd = 'UCBK'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'99999999010101') ||
                       oli.aps_src_cd
                  ELSE null
             END),15) ucbk_fst_ord_src_cd,
  substr(max(CASE WHEN oli.mag_cd IN ('NCPR', 'UCPR', 'NCBK', 'UCBK')
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       oli.aps_src_cd
                  ELSE null
             END),15) aps_lst_ord_src_cd,
  substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                       oli.sub_src_cd
                  ELSE null
             END),9) crmg_lst_ord_src_cd,
  substr(max(CASE WHEN oli.mag_cd = 'CRO'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'00010101') ||
                       oli.sub_src_cd
                  ELSE null
             END),9) cro_lst_ord_src_cd,
  substr(max(CASE WHEN oli.mag_cd = 'NCPR'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       oli.aps_src_cd
                  ELSE null
             END),15) nc_rpts_lst_ord_src_cd,
  substr(max(CASE WHEN oli.mag_cd = 'NCBK'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       oli.aps_src_cd
                  ELSE null
             END),15) ncbk_lst_ord_src_cd,
  substr(max(CASE WHEN oli.mag_cd = 'UCPR'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       oli.aps_src_cd
                  ELSE null
             END),15) uc_rpts_lst_ord_src_cd,
  substr(max(CASE WHEN oli.mag_cd = 'UCBK'
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       oli.aps_src_cd
                  ELSE null
             END),15) ucbk_lst_ord_src_cd,
  substr(min(CASE WHEN oli.mag_cd = 'CRMG'
                   AND nvl(oli.set_cd, 'Z') NOT IN ('B', 'D')
                   AND oli.crt_dt <= t1.last_crmg_brk_dt
                   AND oli.crt_dt > nvl(t1.prior_crmg_brk_dt,to_date('00010101','YYYYMMDD'))
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'99999999') ||
                       oli.sub_src_cd
                  ELSE null
             END),9) crmg_prior_mbr_src_cd,
  substr(min(CASE WHEN oli.mag_cd = 'CRO'
                   AND oli.set_cd NOT IN ('B', 'D')
                   AND oli.crt_dt <= t1.last_cro_brk_dt
                   AND oli.crt_dt > nvl(t1.prior_cro_brk_dt,to_date('00010101','YYYYMMDD'))
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDD'),'99999999') ||
                       oli.sub_src_cd
                  ELSE null
             END),9) cro_prior_mbr_src_cd,
  MAX(CASE WHEN oli.mag_cd = 'CRO'
            AND oli.svc_stat_cd != 'D'
            AND oli.set_cd in ('B', 'D')
            AND oli.crt_dt > add_months(sysdate,-12)
            AND oli.acct_id = t2.cro_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) cro_sub_dnr_flg,
  to_number(substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                             AND oli.stat_cd = 'C'
                             AND oli.svc_stat_cd = 'A'
                             AND oli.sku_num > 5000000
                             AND olo.stat_cd = 'C'
                             AND oli.acct_id = t2.crmg_comb_acct_id
                            THEN nvl(to_char( oli.crt_dt,'YYYYMMDD'),'00000000') ||
                                          oli.term_mth_cnt
                       END),9)) crmg_curr_ord_term,
  to_number(substr(max(CASE WHEN oli.mag_cd = 'CRO'
                             AND oli.stat_cd = 'C'
                             AND oli.svc_stat_cd = 'A'
                             AND olo.stat_cd = 'C'
                             AND oli.acct_id = t2.cro_comb_acct_id
                            THEN nvl(to_char( oli.crt_dt,'YYYYMMDD'),'00000000') ||
                                          oli.term_mth_cnt
                       END),9)) cro_curr_ord_term,
  substr(max(CASE WHEN oli.mag_cd IN ('NCPR', 'UCPR', 'NCBK', 'UCBK')
                  THEN nvl(to_char(oli.crt_dt,'YYYYMMDDHH24MISS'),'00010101010101') ||
                       oli.mag_cd
                  WHEN nvl(oli.mag_cd,'AAAA') NOT IN ('NCPR','UCPR','NCBK', 'UCBK') AND ixo.DC_LAST_APS_ORD_DT is not null 
                  then '00010101010101' || 'REPORT'
                  ELSE null
             END),15) aps_lst_prod_type,
  min(CASE WHEN oli.mag_cd = 'CRO'
            AND oli.set_cd IN ('B', 'D')
           THEN oli.crt_dt
           ELSE null
      END) cro_fst_dnr_dt,
  MAX(CASE WHEN oli.mag_cd = 'CRO'
            AND oli.svc_stat_cd = 'A'
            AND oli.sub_rnw_ind = 'Y'
            AND olo.stat_cd = 'C'
            AND oli.acct_id = t2.cro_comb_acct_id
          THEN 'Y' 
          ELSE 'N'
  END) cro_auto_rnw_flg,
  MAX(CASE WHEN oli.mag_cd = 'CRMG'
            AND oli.svc_stat_cd = 'A'
            AND oli.sub_rnw_ind = 'Y'
            AND oli.sku_num > 5000000
            AND olo.stat_cd = 'C'
            AND oli.acct_id = t2.crmg_comb_acct_id
          THEN 'Y' 
          ELSE 'N'
  END) crmg_auto_rnw_flg,
  max(CASE WHEN oli.mag_cd = 'CRO'
            AND oli.set_cd IN ('A', 'C', 'E')
           THEN 'Y'
           ELSE 'N'
      END) cro_non_dnr_flg,
  min(CASE WHEN oli.mag_cd = 'CRMG'
            AND nvl(oli.set_cd, 'Z') NOT IN ('B','D')
            AND oli.strt_dt > nvl(t1.last_crmg_brk_dt,to_date('00010101','YYYYMMDD'))
           THEN oli.crt_dt
           ELSE null
  END) crmg_curr_mbr_dt,
  to_date(substr(min(CASE WHEN oli.mag_cd = 'CRO'
                           AND nvl(oli.set_cd, 'Z') NOT IN ('B','D')
                           AND nvl(to_char(oli.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                            || nvl(to_char(oli.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                            || lpad(oli.itm_id,21) > nvl(t1.last_cro_acct_brk_dt,'0000000000000000000000000000')
                           AND oli.acct_id = t1.cro_curr_mbr_dt_acct_id
                          THEN nvl(to_char(oli.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                            || nvl(to_char(oli.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                            || lpad(oli.itm_id,21)
                            || to_char(oli.crt_dt,'YYYYMMDDHH24MISS')
                          ELSE null
                     END),50),'YYYYMMDDHH24MISS') cro_curr_mbr_dt,
  nvl(sum(CASE WHEN (oli.mag_cd in ('NCBK','UCBK','NCPR','UCPR')
            AND ((oli.sku_num < '5000000') OR (oli.sku_num > '5000000' and oli.stat_cd = 'C')))
           THEN oli.tot_amt
      END),0) aps_ltd_pd_amt,
  nvl(sum(CASE WHEN (oli.mag_cd = 'CRMG'
            AND ((oli.sku_num < '5000000') OR (oli.sku_num > '5000000' and oli.stat_cd = 'C')))
           THEN oli.tot_amt
      END),0) crmg_ltd_pd_amt,
  nvl(sum(CASE WHEN (oli.mag_cd = 'CRO'
            AND ((nvl(oli.sku_num,'1') < '5000000') OR (oli.sku_num > '5000000' and oli.stat_cd = 'C')))
           THEN oli.tot_amt
      END),0) cro_ltd_pd_amt,
  max(CASE WHEN oli.svc_stat_cd = 'A'
            AND nvl(oli.set_cd,'x') not in ('B','D')
            AND oli.sku_num > 5000000
            AND olo.stat_cd = 'C'
            AND oli.mag_cd = 'CRMG'
           THEN oli.crt_dt
      END) crmg_curr_ord_dt,
  max(CASE WHEN oli.svc_stat_cd = 'A'
            AND nvl(oli.set_cd,'x') not in ('B','D')
            AND olo.stat_cd = 'C'
            AND oli.mag_cd = 'CRO'
           THEN oli.crt_dt
      END) cro_curr_ord_dt,
  min(CASE WHEN oli.mag_cd in ('NCPR','UCPR','NCBK','UCBK')
           THEN 
           nullif(least(nvl(ixo.dc_last_aps_ord_dt,to_date('99991231','YYYYMMDD')),nvl(oli.crt_dt,to_date('99991231','YYYYMMDD'))),to_date('99991231','YYYYMMDD'))
          else
            nullif(nvl(ixo.dc_last_aps_ord_dt, to_date('99991231','YYYYMMDD')),to_date('99991231','YYYYMMDD'))  
      END) aps_fst_ord_dt,
  min(CASE WHEN oli.mag_cd = 'CRMG'
            AND nvl(oli.set_cd,'X') not in ('C','E')
           THEN oli.crt_dt
      END) crmg_fst_ord_dt,
  min(CASE WHEN oli.mag_cd = 'CRO'
           THEN oli.crt_dt
      END) cro_fst_ord_dt,
  min(CASE WHEN oli.mag_cd = 'NCPR'
           THEN oli.crt_dt
      END) nc_rpts_fst_ord_dt,
  min(CASE WHEN oli.mag_cd = 'NCBK'
           THEN oli.crt_dt
      END) ncbk_fst_ord_dt,
  min(CASE WHEN oli.mag_cd = 'UCPR'
           THEN oli.crt_dt
      END) uc_rpts_fst_ord_dt,
  min(CASE WHEN oli.mag_cd = 'UCBK'
           THEN oli.crt_dt
      END) ucbk_fst_ord_dt,
  max(CASE WHEN (oli.crd_stat_cd = 'F'
                 OR (oli.svc_stat_cd = 'C' AND oli.canc_rsn_cd in ('50','06')))
            AND oli.mag_cd = 'CRO'
           THEN oli.canc_dt
      END) cro_lst_canc_bad_dbt_dt,
  max(CASE WHEN nvl(set_cd ,'x') not in ('C','E')
            AND oli.mag_cd = 'CRMG'
           THEN oli.crt_dt
      END) crmg_lst_ord_dt,
  max(CASE WHEN oli.mag_cd = 'CRO'
           THEN oli.crt_dt
      END) cro_lst_ord_dt,
  max(CASE WHEN oli.set_cd in ('B','D')
            AND oli.mag_cd = 'CRO'
           THEN oli.crt_dt
      END) cro_lst_dnr_ord_dt,
  count(distinct CASE WHEN oli.mag_cd = 'CRMG'
                       AND oli.canc_flg = 'Y'
                      THEN oli.ord_id
                 END) crmg_canc_cust_cnt,
  count(distinct CASE WHEN oli.mag_cd = 'CRO'
                       AND oli.canc_flg = 'Y'
                      THEN oli.ord_id
                 END) cro_canc_cust_cnt,
  count(distinct CASE WHEN oli.mag_cd in ('NCBK','UCBK','NCPR','UCPR')
                      THEN olo.individual_id || olo.acct_id || olo.ord_id
                 END) aps_ord_cnt,
  count(distinct CASE WHEN oli.mag_cd = 'CRMG'
             THEN oli.ord_id
        END) crmg_ord_cnt,
  count(CASE WHEN oli.mag_cd = 'CRO'
              AND nvl(oli.set_cd,'x') not in ('C', 'E')
             THEN 1
        END) cro_ord_cnt,
  count(distinct CASE WHEN oli.mag_cd = 'CRMG'
              AND oli.sub_src_cd = 'D'
             THEN oli.ord_id
        END) crmg_dm_ord_cnt,
  count(CASE WHEN oli.mag_cd = 'CRO'
              AND nvl(oli.set_cd,'x') not in ('C', 'E')
              AND oli.sub_src_cd = 'D'
             THEN 1
        END) cro_dm_ord_cnt,
  count(distinct CASE WHEN oli.mag_cd = 'CRMG'
              AND oli.sub_src_cd in ('E','G','K','N','U')
             THEN oli.ord_id
        END) crmg_em_ord_cnt,
  count(CASE WHEN oli.mag_cd = 'CRO'
              AND nvl(oli.set_cd,'x') not in ('C', 'E')
              AND oli.sub_src_cd in ('E','G','K','N','U')
             THEN 1
        END) cro_em_ord_cnt,
  count(distinct CASE WHEN oli.mag_cd = 'CRO'
                       AND oli.set_cd in ('B','D')
                      THEN oli.ord_id
                 END) cro_dnr_ord_cnt,
  count(CASE WHEN oli.mag_cd in ('NCBK','UCBK','NCPR','UCPR')
              AND oli.stat_cd = 'C'
             THEN 1
        END) aps_pd_prod_cnt,
  CASE WHEN substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                            THEN to_char(oli.crt_dt,'YYYYMMDDHH24MISS') || oli.set_cd
                       END),15) in ('C','E')
       THEN 'R'
       WHEN substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                            THEN to_char(oli.crt_dt,'YYYYMMDDHH24MISS') || nvl(oli.set_cd,'A')
                       END),15) = 'A'
       THEN 'O'
       WHEN substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                            THEN to_char(oli.crt_dt,'YYYYMMDDHH24MISS') || oli.set_cd || oli.svc_stat_cd
                       END),15) in ('BD','DD')
       THEN 'N'
       WHEN substr(max(CASE WHEN oli.mag_cd = 'CRMG'
                            THEN to_char(oli.crt_dt,'YYYYMMDDHH24MISS') || oli.set_cd
                       END),15) in ('B','D')
       THEN 'D'
  END crmg_lst_sub_ord_role_cd,
  CASE WHEN substr(max(CASE WHEN oli.mag_cd = 'CRO'
                            THEN to_char(oli.crt_dt,'YYYYMMDDHH24MISS') || oli.set_cd
                       END),15) in ('C','E')
       THEN 'R'
       WHEN substr(max(CASE WHEN oli.mag_cd = 'CRO'
                            THEN to_char(oli.crt_dt,'YYYYMMDDHH24MISS') || oli.set_cd
                       END),15) = 'A'
       THEN 'O'
       WHEN substr(max(CASE WHEN oli.mag_cd = 'CRO'
                            THEN to_char(oli.crt_dt,'YYYYMMDDHH24MISS') || oli.set_cd || oli.svc_stat_cd
                       END),15) in ('BD','DD')
       THEN 'N'
       WHEN substr(max(CASE WHEN oli.mag_cd = 'CRO'
                            THEN to_char(oli.crt_dt,'YYYYMMDDHH24MISS') || oli.set_cd
                       END),15) in ('B','D')
       THEN 'D'
  END cro_lst_sub_ord_role_cd,
  max(an.lst_logn_dt) lst_logn_dt,
  max(nvl(an.logn_cnt,0)) logn_cnt,
  max(nvl(an.cro_lt_sub_flg,'N')) cro_lt_sub_flg,
  max(nvl(an.crmg_lt_sub_flg,'N')) crmg_lt_sub_flg,
  MAX(CASE WHEN oli.mag_cd = 'CARP'
            AND ((oli.set_cd in ('C','E')
                  AND oli.svc_stat_cd = 'A'
                  AND olo.stat_cd = 'C'
                  AND oli.stat_cd = 'C')
                 OR (((oli.svc_stat_cd = 'A'
                       AND olo.stat_cd = 'C')
                     OR (oli.svc_stat_cd = 'C'
                         AND olo.stat_cd = 'R'
                         AND oli.sub_rnw_ind = 'N'
                         AND oli.end_dt > sysdate))
                     AND (nvl(olo.entr_typ_cd,'Z') != 'F'
                     AND oli.tot_amt > 0.01
                     AND oli.term_mth_cnt > 0)))
            AND oli.acct_id = t2.carp_comb_acct_id
           THEN 'Y'
           ELSE 'N'            
      END) carp_actv_flg,
  MAX(CASE WHEN oli.mag_cd = 'CARP'
            AND ((oli.svc_stat_cd = 'A'
                  AND olo.stat_cd = 'C')
                OR (oli.svc_stat_cd = 'C'
                    AND olo.stat_cd = 'R'
                    AND oli.sub_rnw_ind = 'N'
                    AND oli.end_dt > sysdate))
            AND nvl(olo.entr_typ_cd,'Z') != 'F'
            AND oli.tot_amt > 0.01
            AND oli.term_mth_cnt > 0
            AND oli.acct_id = t2.carp_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) carp_actv_pd_flg,
 ix.household_id   
FROM
    ( select
                distinct individual_id
               from middle_tier2.mt_online_ord
               union 
              select
                individual_id
              from warehouse2.individual_xographic
              where dc_last_aps_ord_dt is not null
               union
              select
                distinct individual_id
              from middle_tier2.mt_account_number
              where acct_prefix = 'PWI'
              and group_id in ('310','26401','26402','26403')) driver, 
     ind_xref ix,
     (select distinct individual_id, internal_key from warehouse2.external_ref) er,
     middle_tier2.mt_online_ord olo,
     middle_tier2.mt_online_item oli,
     mt_onl_sum_temp1 t1,
     warehouse2.sku_lkp sku,
     (SELECT individual_id,
             substr(max(CASE WHEN mag_cd = 'CRMG'
                             THEN nvl(to_char(end_dt,'YYYYMMDD'),'0000000') || acct_id
                             ELSE NULL
                        END),9) crmg_comb_acct_id,
             substr(max(CASE WHEN mag_cd = 'CRO'
                             THEN nvl(to_char(nvl(cro_acct_exp_dt,end_dt),'YYYYMMDD'),'0000000') || 
							               nvl(lpad(sku_num,16,'0'),'0000000000000000')||acct_id
                             ELSE NULL
                        END),25) cro_comb_acct_id,
             substr(max(CASE WHEN mag_cd = 'CARP'
                             THEN nvl(to_char(end_dt,'YYYYMMDD'),'0000000') || acct_id
                             ELSE NULL
                        END),9) carp_comb_acct_id
      FROM middle_tier2.mt_online_item
      GROUP BY individual_id) t2,
     warehouse2.individual_xographic ixo,
     (select individual_id,
      max(CASE WHEN acct_prefix = 'PWI'
           THEN acct_lst_logn_dt
      END) lst_logn_dt,
      nvl(sum(CASE WHEN acct_prefix = 'PWI'
           THEN acct_logn_cnt
      END),0) logn_cnt,
      MIN(CASE WHEN acct_prefix = 'PWI'
            AND group_id = '311'
           THEN 'N'
           WHEN acct_prefix = 'PWI'
            AND group_id in ('310', '26401', '26403')
           THEN 'Y'
           ELSE null
           END) cro_lt_sub_flg,
      MIN(CASE WHEN acct_prefix = 'PWI'
            AND group_id = '311'
           THEN 'N'
           WHEN acct_prefix = 'PWI'
            AND group_id in ('310', '26402', '26403')
           THEN 'Y'
           ELSE null
           END) crmg_lt_sub_flg    
      from middle_tier2.mt_account_number
      group by individual_id) an
WHERE
        driver.individual_id = ix.individual_id 
    and    ix.individual_id    = er.individual_id (+)
  AND er.internal_key = olo.acct_id (+)
  AND olo.acct_id = oli.acct_id (+)
  AND olo.ord_id = oli.ord_id (+)
  AND oli.individual_id = t1.individual_id(+)
  AND oli.acct_id = t1.acct_id(+)
  AND oli.sku_num = sku.sku_num(+)
  AND ix.individual_id = t2.individual_id (+)
  AND ix.individual_id = ixo.individual_id(+)
  AND ix.individual_id = an.individual_id(+)
  AND ix.active_flag = 'A'
GROUP BY ix.individual_id, ix.household_id
;