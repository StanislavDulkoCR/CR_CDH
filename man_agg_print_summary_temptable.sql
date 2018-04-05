
DROP TABLE IF EXISTS cr_temp.mt_ofl_sum_temp;
CREATE TABLE cr_temp.mt_ofl_sum_temp
AS
SELECT 
  er.individual_id,
  MAX(CASE WHEN oa.magazine_code = 'CNS'
            AND oa.svc_status_code = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (substring(ofo.keycode,1,4) = 'LIFE'))
            AND oa.hash_account_id = ct.cr_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) cr_actv_flg,
  MAX(CASE WHEN oa.magazine_code = 'CRH'
            AND oa.svc_status_code = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (substring(ofo.keycode,1,4) = 'LIFE'))
            AND oa.hash_account_id = ct.hl_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) hl_actv_flg,  
  MAX(CASE WHEN oa.magazine_code = 'CRM'
            AND oa.svc_status_code = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (substring(ofo.keycode,1,4) = 'LIFE'))
            AND oa.hash_account_id = ct.ma_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) ma_actv_flg,
  MAX(CASE WHEN oa.magazine_code = 'CNS'
            AND oa.svc_status_code in ('A','E')
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (substring(ofo.keycode,1,4) = 'LIFE'))
            AND oa.hash_account_id = ct.cr_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) cr_actv_rptg_flg,
  MAX(CASE WHEN oa.magazine_code = 'CRH'
            AND oa.svc_status_code in ('A','E')
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (substring(ofo.keycode,1,4) = 'LIFE'))
            AND oa.hash_account_id = ct.hl_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) hl_actv_rptg_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRM'
            AND oa.svc_status_code in ('A','E')
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (substring(ofo.keycode,1,4) = 'LIFE'))
            AND oa.hash_account_id = ct.ma_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) ma_actv_rptg_flg,
  MAX(CASE WHEN oa.magazine_code = 'CNS'
            AND oa.svc_status_code = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND ofo.set_cd in ('A','B')
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (substring(ofo.keycode,1,4) = 'LIFE'))
            AND ofo.cr_stat_cd in ('C', 'D', 'E', 'F', 'G', 'I')
            AND oa.hash_account_id = ct.cr_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) cr_actv_pd_flg,
  MAX(CASE WHEN oa.magazine_code = 'CRH'
            AND oa.svc_status_code = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND ofo.set_cd in ('A','B')
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (substring(ofo.keycode,1,4) = 'LIFE'))
            AND ofo.cr_stat_cd in ('C', 'D', 'E', 'F', 'G', 'I')
            AND oa.hash_account_id = ct.hl_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) hl_actv_pd_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRM'
            AND oa.svc_status_code = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND ofo.set_cd in ('A','B')
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (substring(ofo.keycode,1,4) = 'LIFE'))
            AND ofo.cr_stat_cd in ('C', 'D', 'E', 'F', 'G', 'I')
            AND oa.hash_account_id = ct.ma_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) ma_actv_pd_flg, 
  CASE WHEN substring(MAX(CASE WHEN oa.magazine_code = 'CNS' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.cancel_reason_code 
                            ELSE NULL 
                       END),9) IN ('06','50')
       THEN 'Y' 
       ELSE 'N' 
  END cr_canc_bad_dbt_flg, 
  CASE WHEN substring(MAX(CASE WHEN oa.magazine_code = 'CRH' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.cancel_reason_code 
                            ELSE NULL 
                       END),9) IN ('06','50') 
       THEN 'Y' 
       ELSE 'N' 
  END hl_canc_bad_dbt_flg, 
  CASE WHEN substring(MAX(CASE WHEN oa.magazine_code = 'CRM' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.cancel_reason_code 
                            ELSE NULL 
                       END),9) IN ('06','50') 
       THEN 'Y' 
       ELSE 'N' 
  END ma_canc_bad_dbt_flg, 
  CASE WHEN substring(MAX(CASE WHEN oa.magazine_code = 'CNS' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.cancel_reason_code 
                            ELSE NULL 
                       END),9) NOT IN ('06','14','50') 
       THEN 'Y' 
       ELSE 'N' 
  END cr_canc_cust_flg, 
  CASE WHEN substring(MAX(CASE WHEN oa.magazine_code = 'CRH' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.cancel_reason_code 
                            ELSE NULL 
                       END),9) NOT IN ('06','14','50') 
       THEN 'Y' 
       ELSE 'N' 
  END hl_canc_cust_flg, 
  CASE WHEN substring(MAX(CASE WHEN oa.magazine_code = 'CRM' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.cancel_reason_code 
                            ELSE NULL 
                       END),9) NOT IN ('06','14','50') 
       THEN 'Y' 
       ELSE 'N' 
  END ma_canc_cust_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CNS'
            AND oa.svc_status_code = 'E'
            AND oa.hash_account_id = ct.cr_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) cr_crd_pend_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRH'
            AND oa.svc_status_code = 'E'
            AND oa.hash_account_id = ct.hl_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) hl_crd_pend_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRM'
            AND oa.svc_status_code = 'E'
            AND oa.hash_account_id = ct.ma_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) ma_crd_pend_flg,
  DECODE(substring(MAX(CASE WHEN oa.magazine_code = 'CNS' 
                         THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                              ofo.cr_stat_cd 
                         ELSE NULL 
                    END),9), 
         'C','P', 
         'D','P', 
         'E','P', 
         'F','P', 
         'G','P', 
         'I','P', 
         'A','U', 
         'H','U', 
         'B','W', 
         'J','W',NULL) cr_crd_stat_cd, 
  DECODE(substring(MAX(CASE WHEN oa.magazine_code = 'CRH' 
                         THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                              ofo.cr_stat_cd 
                         ELSE NULL 
                    END),9), 
         'C','P', 
         'D','P', 
         'E','P', 
         'F','P', 
         'G','P', 
         'I','P', 
         'A','U', 
         'H','U', 
         'B','W', 
         'J','W',NULL) hl_crd_stat_cd, 
  DECODE(substring(MAX(CASE WHEN oa.magazine_code = 'CRM' 
                         THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                              ofo.cr_stat_cd 
                         ELSE NULL 
                    END),9), 
         'C','P', 
         'D','P', 
         'E','P', 
         'F','P', 
         'G','P', 
         'I','P', 
         'A','U', 
         'H','U', 
         'B','W', 
         'J','W',NULL) ma_crd_stat_cd, 
  MAX(CASE WHEN oa.magazine_code = 'CNS' 
           THEN iss.pub_date 
           WHEN de.magazine_code = 'CNS' 
           THEN de.exp_date 
           ELSE NULL 
      END) cr_exp_dt, 
  MAX(CASE WHEN oa.magazine_code = 'CRH' 
           THEN iss.pub_date 
           WHEN de.magazine_code = 'CRH' 
           THEN de.exp_date 
           ELSE NULL 
      END) hl_exp_dt, 
  MAX(CASE WHEN oa.magazine_code = 'CRM' 
           THEN iss.pub_date 
           ELSE NULL 
      END) ma_exp_dt, 
  MAX(CASE WHEN ofo.canc_rsn_cd not in ('06','14','50')
           AND ofo.mag_cd = 'CNS'
           THEN ofo.canc_dt
      END) cr_lst_canc_dt,
  MAX(CASE WHEN ofo.canc_rsn_cd not in ('06','14','50')
           AND ofo.mag_cd = 'CRH'
           THEN ofo.canc_dt
      END) hl_lst_canc_dt,
  MAX(CASE WHEN ofo.canc_rsn_cd not in ('06','14','50')
           AND ofo.mag_cd = 'CRM'
           THEN ofo.canc_dt
      END) ma_lst_canc_dt, 
  MAX(CASE WHEN oa.magazine_code = 'CRT' 
           THEN ofo.ord_dt 
           ELSE NULL 
      END) tl_lst_ord_dt, 
  MAX(CASE WHEN oa.magazine_code = 'CNS' 
           THEN ofo.pmt_dt 
           ELSE NULL 
      END) cr_lst_pmt_dt, 
  MAX(CASE WHEN oa.magazine_code = 'CRH' 
           THEN ofo.pmt_dt 
           ELSE NULL 
      END) hl_lst_pmt_dt, 
  MAX(CASE WHEN oa.magazine_code = 'CRM' 
           THEN ofo.pmt_dt 
           ELSE NULL 
      END) ma_lst_pmt_dt, 
  MAX(CASE WHEN oa.magazine_code = 'CNS' 
           THEN 'Y' 
           WHEN de.magazine_code = 'CNS' 
           THEN 'Y' 
           ELSE 'N' 
      END) cr_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRH' 
           THEN 'Y' 
           WHEN de.magazine_code = 'CRH' 
           THEN 'Y' 
           ELSE 'N' 
      END) hl_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRM' 
           THEN 'Y' 
           ELSE 'N' 
      END) ma_flg, 
  CASE WHEN MAX(CASE WHEN oa.magazine_code = 'CNS'
                      AND oa.svc_status_code = 'C'
                      AND oa.hash_account_id = ct.cr_comb_acct_id
                     THEN 'Y'
                     ELSE 'N'
                END) = 'Y'
       THEN 'Y'
       WHEN MAX(CASE WHEN oa.magazine_code = 'CNS'
                     THEN '2'
                     WHEN de.magazine_code = 'CNS'
                     THEN '1'
                END) = '1'
       THEN 'Y'
       ELSE 'N'
  END cr_exp_flg, 
  CASE WHEN MAX(CASE WHEN oa.magazine_code = 'CRH'
                      AND oa.svc_status_code = 'C'
                      AND oa.hash_account_id = ct.hl_comb_acct_id
                     THEN 'Y'
                     ELSE 'N'
                END) = 'Y'
       THEN 'Y'
       WHEN MAX(CASE WHEN oa.magazine_code = 'CRH'
                     THEN '2'
                     WHEN de.magazine_code = 'CRH'
                     THEN '1'
                END) = '1'
       THEN 'Y'
       ELSE 'N'
  END hl_exp_flg, 
  CASE WHEN MAX(CASE WHEN oa.magazine_code = 'CRM'
                      AND oa.svc_status_code = 'C'
                      AND oa.hash_account_id = ct.ma_comb_acct_id
                     THEN 'Y'
                     ELSE 'N'
                END) = 'Y'
       THEN 'Y'
       ELSE 'N'
  END ma_exp_flg,
  substring(MAX(CASE WHEN oa.magazine_code = 'CNS'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.cr_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') ||
                         ofo.keycode
             END),9) cr_curr_ord_keycode, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRH'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.hl_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') ||
                         ofo.keycode
             END),9) hl_curr_ord_keycode, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.ma_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') ||
                         ofo.keycode
             END),9) ma_curr_ord_keycode, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CNS' 
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) cr_fst_mbr_keycode, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRH' 
                  AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) hl_fst_mbr_keycode, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRM' 
                  AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) ma_fst_mbr_keycode, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CNS' 
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) cr_lst_ord_keycode, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRH' 
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) hl_lst_ord_keycode, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRM' 
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) ma_lst_ord_keycode, 
  MAX(CASE WHEN oa.magazine_code = 'CNS'
            AND substring(ofo.keycode,1,4) = 'LIFE'
            AND oa.svc_status_code = 'A'
            AND oa.hash_account_id = ct.cr_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) cr_lt_sub_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRH'
            AND substring(ofo.keycode,1,4) = 'LIFE'
            AND oa.svc_status_code = 'A'
            AND oa.hash_account_id = ct.hl_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) hl_lt_sub_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRM'
            AND substring(ofo.keycode,1,4) = 'LIFE'
            AND oa.svc_status_code = 'A'
            AND oa.hash_account_id = ct.ma_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) ma_lt_sub_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CNS'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_status_code = 'D'
            AND oa.hash_account_id = ct.cr_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) cr_non_sub_dnr_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRH'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_status_code = 'D'
            AND oa.hash_account_id = ct.hl_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) hl_non_sub_dnr_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRM'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_status_code = 'D'
            AND oa.hash_account_id = ct.ma_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) ma_non_sub_dnr_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CNS'
            AND oa.svc_status_code = 'A'
            AND ofo.set_cd IN ('C','E')
            AND ofo.stat_cd = 'B'
            AND oa.hash_account_id = ct.cr_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) cr_rec_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRH'
            AND oa.svc_status_code = 'A'
            AND ofo.set_cd IN ('C','E')
            AND ofo.stat_cd = 'B'
            AND oa.hash_account_id = ct.hl_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) hl_rec_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRM'
            AND oa.svc_status_code = 'A'
            AND ofo.set_cd IN ('C','E')
            AND ofo.stat_cd = 'B'
            AND oa.hash_account_id = ct.ma_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) ma_rec_flg, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CNS'
                   AND oa.hash_account_id = ct.cr_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       oa.svc_status_code
                  WHEN de.magazine_code = 'CNS'
                  THEN '-1000000'||
                       'C'
             END),9) cr_svc_stat_cd, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRH'
                   AND oa.hash_account_id = ct.hl_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       oa.svc_status_code
                  WHEN de.magazine_code = 'CRH'
                  THEN '-1000000'||
                       'C'
             END),9) hl_svc_stat_cd, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRM'
                   AND oa.hash_account_id = ct.ma_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       oa.svc_status_code
             END),9) ma_svc_stat_cd,   
  substring(MAX(CASE WHEN oa.magazine_code = 'CNS'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.cr_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                   DECODE(ofo.src_cd,                                                     
                                          'AF','1',                                                       
                                          'BA','2',                                                       
                                          'BB','3',                                                       
                                          'BE','4',                                                       
                                          'BF','5',                                                       
                                          'BH','6',                                                       
                                          'CA','7',                                                       
                                          'CC','7',                                                       
                                          substring(ofo.keycode,1,1))
             END),9) cr_curr_ord_src_cd,  
  substring(MAX(CASE WHEN oa.magazine_code = 'CRH'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.hl_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                   DECODE(ofo.src_cd,                                                     
                                          'AF','1',                                                       
                                          'BA','2',                                                       
                                          'BB','3',                                                       
                                          'BE','4',                                                       
                                          'BF','5',                                                       
                                          'BH','6',                                                       
                                          'CA','7',                                                       
                                          'CC','7',                                                       
                                          substring(ofo.keycode,1,1))
             END),9) hl_curr_ord_src_cd,
     substring(MAX(CASE WHEN oa.magazine_code = 'CRM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.ma_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                   DECODE(ofo.src_cd,                                                     
                                          'AF','1',                                                       
                                          'BA','2',                                                       
                                          'BB','3',                                                       
                                          'BE','4',                                                       
                                          'BF','5',                                                       
                                          'BH','6',                                                       
                                          'CA','7',                                                       
                                          'CC','7',                                                       
                                          substring(ofo.keycode,1,1))
             END),9) ma_curr_ord_src_cd,   
  substring(MIN(CASE WHEN oa.magazine_code = 'CNS' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) cr_fst_ord_src_cd, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRH' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) hl_fst_ord_src_cd, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRM' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) ma_fst_ord_src_cd, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CNS' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) cr_lst_ord_src_cd, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRH' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) hl_lst_ord_src_cd, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRM' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) ma_lst_ord_src_cd,  
  MAX(CASE WHEN oa.magazine_code = 'CNS'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_status_code != 'D'
            AND oa.hash_account_id = ct.cr_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) cr_sub_dnr_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRH'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_status_code != 'D'
            AND oa.hash_account_id = ct.hl_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) hl_sub_dnr_flg,
  MAX(CASE WHEN oa.magazine_code = 'CRM'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_status_code != 'D'
            AND oa.hash_account_id = ct.ma_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) ma_sub_dnr_flg,
  substring(MAX(CASE WHEN oa.magazine_code = 'CNS'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.cr_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.term_mth_cnt
             END),9) cr_curr_ord_term, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRH'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.hl_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.term_mth_cnt
             END),9) hl_curr_ord_term, 
 substring(MAX(CASE WHEN oa.magazine_code = 'CRM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.ma_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.term_mth_cnt
             END),9) ma_curr_ord_term, 
  MIN(CASE WHEN oa.magazine_code = 'CNS' 
            AND ofo.set_cd IN ('B','D') 
           THEN ofo.ord_dt 
           ELSE NULL 
      END) cr_fst_dnr_dt, 
  MIN(CASE WHEN oa.magazine_code = 'CRH' 
            AND ofo.set_cd IN ('B','D') 
           THEN ofo.ord_dt 
           ELSE NULL 
      END) hl_fst_dnr_dt, 
  MIN(CASE WHEN oa.magazine_code = 'CRM' 
            AND ofo.set_cd IN ('B','D') 
           THEN ofo.ord_dt 
           ELSE NULL 
      END) ma_fst_dnr_dt, 
  MAX(CASE WHEN de.magazine_code = 'CNS' 
           THEN de.eds_lst_src_cd 
           ELSE NULL 
      END) eds_lst_src_cd, 
  MAX(CASE WHEN de.magazine_code = 'CNS' 
           THEN de.morbank_mtch_code 
           ELSE NULL 
      END) morbank_mtch_cd, 
  MAX(CASE WHEN oa.magazine_code = 'CNS'
            AND oa.svc_status_code = 'A'
            AND substring(oa.keycode,1,4) = 'AUTO'
            AND oa.hash_account_id = ct.cr_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) cr_auto_rnw_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRH'
            AND oa.svc_status_code = 'A'
            AND substring(oa.keycode,1,4) = 'AUTO'
            AND oa.hash_account_id = ct.hl_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END)  hl_auto_rnw_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRM'
            AND oa.svc_status_code = 'A'
            AND substring(oa.keycode,1,4) = 'AUTO'
            AND oa.hash_account_id = ct.ma_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) ma_auto_rnw_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CNS' 
            AND ( 
                  ofo.set_cd IN ('A','C','E')  
                  or 
                  ( ofo.SET_CD in ('B', 'D') and ofo.STAT_CD not in ('D') )
                )
           THEN 'Y' 
           ELSE 'N' 
      END) cr_non_dnr_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRH' 
            AND ( 
                  ofo.set_cd IN ('A','C','E')  
                  or 
                  ( ofo.SET_CD in ('B', 'D') and ofo.STAT_CD not in ('D') )
                )
           THEN 'Y' 
           ELSE 'N' 
      END) hl_non_dnr_flg, 
  MAX(CASE WHEN oa.magazine_code = 'CRM' 
            AND ( 
                  ofo.set_cd IN ('A','C','E')  
                  or 
                  ( ofo.SET_CD in ('B', 'D') and ofo.STAT_CD not in ('D') )
                )
           THEN 'Y' 
           ELSE 'N' 
      END) ma_non_dnr_flg,
  substring(MIN(CASE WHEN oa.magazine_code = 'CNS' 
                   AND ofo.orig_strt_iss_num > 0
                   AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_cro_brk_dt, STRPOS(obt.last_cro_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num)
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) cr_curr_mbr_keycode, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRH' 
                   AND ofo.orig_strt_iss_num > 0
                   AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_hl_brk_dt, STRPOS(obt.last_hl_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num)
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) hl_curr_mbr_keycode, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRM'
                   AND ofo.orig_strt_iss_num > 0 
                   AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_ma_brk_dt, STRPOS(obt.last_ma_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num)
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) ma_curr_mbr_keycode, 
  MAX(CASE WHEN oa.magazine_code = 'CNS' 
            AND ( ofo.set_cd NOT IN('B','D') or ofo.stat_cd not in ('D') )
           THEN obt.cro_brks_cnt 
           ELSE null 
  END) cro_brks_cnt,
  MAX(CASE WHEN de_cnt.magazine_code = 'CNS'
           THEN de_cnt.de_cr_brks_cnt                  
           ELSE null                  
  END) de_cro_brks_cnt, 
  MAX(CASE WHEN oa.magazine_code = 'CRH' 
            AND ( ofo.set_cd NOT IN('B','D') or ofo.stat_cd not in ('D') )
           THEN obt.hl_brks_cnt 
           ELSE NULL 
  END) hl_brks_cnt,
  MAX(CASE WHEN de_cnt.magazine_code = 'CRH' 
           THEN de_cnt.de_hl_brks_cnt                  
           ELSE null                  
  END) de_hl_brks_cnt, 
  MAX(CASE WHEN oa.magazine_code = 'CRM' 
            AND ( ofo.set_cd NOT IN('B','D') or ofo.stat_cd not in ('D') )
           THEN obt.ma_brks_cnt 
           ELSE NULL 
  END) ma_brks_cnt,
  substring(MIN(CASE WHEN oa.magazine_code = 'CNS' 
                   AND ofo.orig_strt_iss_num > 0
                   AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_cro_brk_dt, STRPOS(obt.last_cro_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num) 
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) cr_curr_mbr_src_cd, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRH' 
                   AND ofo.orig_strt_iss_num > 0
                   AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_hl_brk_dt, STRPOS(obt.last_hl_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num) 
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) hl_curr_mbr_src_cd, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRM' 
                   AND ofo.orig_strt_iss_num > 0                  
                   AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_ma_brk_dt, STRPOS(obt.last_ma_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num) 
AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) ma_curr_mbr_src_cd, 
    substring(MIN(CASE WHEN oa.magazine_code = 'CNS'
                     AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                     AND cro_brks_cnt = 1
                     AND ofo.orig_strt_iss_num >= obt.cns_orig_strt_iss_num
                     AND ofo.orig_strt_iss_num < cast(substring(obt.last_cro_brk_dt, STRPOS(obt.last_cro_brk_dt, '|')+1) as integer)
                    THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  WHEN oa.magazine_code = 'CNS' 
                   AND cro_brks_cnt > 1
                   AND ofo.orig_strt_iss_num < cast(substring(obt.last_cro_brk_dt, STRPOS(obt.last_cro_brk_dt, '|')+1) as integer)
                   AND ofo.orig_strt_iss_num >= cast(substring(obt.prior_cro_brk_dt, STRPOS(obt.prior_cro_brk_dt, '|')+1) as integer)
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) cr_prior_mbr_src_cd, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRH' 
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   AND hl_brks_cnt = 1
                   AND ofo.orig_strt_iss_num >= obt.crh_orig_strt_iss_num
                   AND ofo.orig_strt_iss_num < cast(substring(obt.last_hl_brk_dt, STRPOS(obt.last_hl_brk_dt, '|')+1) as integer)                   
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  WHEN oa.magazine_code = 'CRH' 
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   AND hl_brks_cnt > 1
                   AND ofo.orig_strt_iss_num < cast(substring(obt.last_hl_brk_dt, STRPOS(obt.last_hl_brk_dt, '|')+1) as integer)
                   AND ofo.orig_strt_iss_num >= cast(substring(obt.prior_hl_brk_dt, STRPOS(obt.prior_hl_brk_dt, '|')+1) as integer)
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) hl_prior_mbr_src_cd, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRM' 
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   AND ma_brks_cnt = 1
                   AND ofo.orig_strt_iss_num >= obt.crm_orig_strt_iss_num
                   AND ofo.orig_strt_iss_num < cast(substring(obt.last_ma_brk_dt, STRPOS(obt.last_ma_brk_dt, '|')+1) as integer)                 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  WHEN oa.magazine_code = 'CRM' 
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   AND ma_brks_cnt > 1
                   AND ofo.orig_strt_iss_num < cast(substring(obt.last_ma_brk_dt, STRPOS(obt.last_ma_brk_dt, '|')+1) as integer)
                   AND ofo.orig_strt_iss_num >= cast(substring(obt.prior_ma_brk_dt, STRPOS(obt.prior_ma_brk_dt, '|')+1) as integer)
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) ma_prior_mbr_src_cd,
    substring(MAX(CASE WHEN oa.magazine_code = 'CNS'
                     AND oa.svc_status_code <> 'D' 
                    THEN lpad(oa.expr_iss_num, 5, 0)||oa.renewal_cnt
               END), 6) cr_rnw_cnt,
    substring(MAX(CASE WHEN oa.magazine_code = 'CRH'
                     AND oa.svc_status_code <> 'D' 
                    THEN lpad(oa.expr_iss_num, 5, 0)||oa.renewal_cnt
               END), 6) hl_rnw_cnt,        
    substring(MAX(CASE WHEN oa.magazine_code = 'CRM'
                     AND oa.svc_status_code <> 'D' 
                    THEN lpad(oa.expr_iss_num, 5, 0)||oa.renewal_cnt
               END), 6) ma_rnw_cnt,
    MIN(CASE WHEN oa.magazine_code = 'CNS'                
              AND ofo.orig_strt_iss_num > 0
              AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_cro_brk_dt, STRPOS(obt.last_cro_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num) 
              AND ( nvl(ofo.set_cd, 'Z') not in ('B','D') or ofo.stat_cd not in ('D') )
             THEN ofo.ord_dt
             ELSE null
    END) cr_curr_mbr_dt,                         -- mribbi 11/06/2006
    MIN(CASE WHEN oa.magazine_code = 'CRH' 
              AND ofo.orig_strt_iss_num > 0
              AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_hl_brk_dt, STRPOS(obt.last_hl_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num) 
              AND ( nvl(ofo.set_cd, 'Z') not in ('B','D') or ofo.stat_cd not in ('D') )
             THEN ofo.ord_dt
             ELSE null
    END) hl_curr_mbr_dt,                         -- mribbi 11/06/2006
    MIN(CASE WHEN oa.magazine_code = 'CRM' 
              AND ofo.orig_strt_iss_num > 0
              AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_ma_brk_dt, STRPOS(obt.last_ma_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num) 
              AND ( nvl(ofo.set_cd, 'Z') not in ('B','D') or ofo.stat_cd not in ('D') )
             THEN ofo.ord_dt
             ELSE null
    END) ma_curr_mbr_dt,                           -- mribbi 11/06/2006
    nvl(max(ofo2.cr_ltd_pd_amt),0) cr_ltd_pd_amt,
    nvl(max(ofo2.hl_ltd_pd_amt),0) hl_ltd_pd_amt,
    nvl(max(ofo2.ma_ltd_pd_amt),0) ma_ltd_pd_amt,
    max(CASE WHEN ofo.stat_cd = 'B'
              AND oa.magazine_code = 'CNS'
              AND oa.svc_status_code = 'A'
              AND oa.hash_account_id = ct.cr_comb_acct_id
             THEN ofo.ord_dt
      END) cr_curr_ord_dt,
    max(CASE WHEN ofo.stat_cd = 'B'
              AND oa.magazine_code = 'CRH'
              AND oa.svc_status_code = 'A'
              AND oa.hash_account_id = ct.hl_comb_acct_id
         THEN ofo.ord_dt
    END) hl_curr_ord_dt,
    max(CASE WHEN ofo.stat_cd = 'B'
              AND oa.magazine_code = 'CRM'
              AND oa.svc_status_code = 'A'
              AND oa.hash_account_id = ct.ma_comb_acct_id
             THEN ofo.ord_dt
        END) ma_curr_ord_dt,
    min(CASE WHEN ofo.mag_cd = 'CNS'  --ddbarr nvl(ofo.set_cd,'X') not in ('C','E')
             THEN ofo.ord_dt
        END) cr_fst_ord_dt,
    min(CASE WHEN ofo.mag_cd = 'CRH'  --ddbarr nvl(ofo.set_cd,'X') not in ('C','E')
             THEN ofo.ord_dt
        END) hl_fst_ord_dt,
    min(CASE WHEN ofo.mag_cd = 'CRM'  --ddbarr nvl(ofo.set_cd,'X') not in ('C','E')
             THEN ofo.ord_dt
        END) ma_fst_ord_dt,
    max(CASE WHEN ofo.canc_rsn_cd in ('06','50')
              AND ofo.mag_cd = 'CNS'
             THEN ofo.canc_dt
        END) cr_lst_canc_bad_dbt_dt,
    max(CASE WHEN ofo.canc_rsn_cd in ('06','50')
              AND ofo.mag_cd = 'CRH'
             THEN ofo.canc_dt
        END) hl_lst_canc_bad_dbt_dt,
    max(CASE WHEN ofo.canc_rsn_cd in ('06','50')
              AND ofo.mag_cd = 'CRM'
             THEN ofo.canc_dt
        END) ma_lst_canc_bad_dbt_dt,
    max(CASE WHEN ofo.set_cd in ('B','D')
              AND ofo.mag_cd = 'CNS'
             THEN ofo.ord_dt
        END) cr_lst_dnr_ord_dt,
    max(CASE WHEN ofo.set_cd in ('B','D')
              AND ofo.mag_cd = 'CRH'
             THEN ofo.ord_dt
        END) hl_lst_dnr_ord_dt,
    max(CASE WHEN ofo.set_cd in ('B','D')
              AND ofo.mag_cd = 'CRM'
             THEN ofo.ord_dt
        END) ma_lst_dnr_ord_dt,
    max(CASE WHEN ofo.mag_cd = 'CNS'  --ddbarr nvl(ofo.set_cd,'X') not in ('C','E')
             THEN ofo.ord_dt
        END) cr_lst_ord_dt,
    max(CASE WHEN ofo.mag_cd = 'CRH'  --ddbarr nvl(ofo.set_cd,'X') not in ('C','E')
             THEN ofo.ord_dt
        END) hl_lst_ord_dt,
    max(CASE WHEN ofo.mag_cd = 'CRM'  --ddbarr nvl(ofo.set_cd,'X') not in ('C','E')
             THEN ofo.ord_dt
        END) ma_lst_ord_dt,
    nvl(max(ofo2.cr_canc_bad_dbt_cnt),0) cr_canc_bad_dbt_cnt,
    nvl(max(ofo2.hl_canc_bad_dbt_cnt),0) hl_canc_bad_dbt_cnt,
    nvl(max(ofo2.ma_canc_bad_dbt_cnt),0) ma_canc_bad_dbt_cnt,
    nvl(max(ofo2.cr_canc_cust_cnt),0) cr_canc_cust_cnt,
    nvl(max(ofo2.hl_canc_cust_cnt),0) hl_canc_cust_cnt,
    nvl(max(ofo2.ma_canc_cust_cnt),0) ma_canc_cust_cnt,
    nvl(max(ofo2.cr_dm_ord_cnt),0) cr_dm_ord_cnt,
    nvl(max(ofo2.hl_dm_ord_cnt),0) hl_dm_ord_cnt,
    nvl(max(ofo2.ma_dm_ord_cnt),0) ma_dm_ord_cnt,
    nvl(max(ofo2.cr_em_ord_cnt),0) cr_em_ord_cnt,
    nvl(max(ofo2.hl_em_ord_cnt),0) hl_em_ord_cnt,
    nvl(max(ofo2.ma_em_ord_cnt),0) ma_em_ord_cnt,
    nvl(max(ofo2.cr_dnr_ord_cnt),0) cr_dnr_ord_cnt,
    nvl(max(ofo2.hl_dnr_ord_cnt),0) hl_dnr_ord_cnt,
    nvl(max(ofo2.ma_dnr_ord_cnt),0) ma_dnr_ord_cnt,
    nvl(max(ofo2.cr_ord_cnt),0) cr_ord_cnt,
    nvl(max(ofo2.hl_ord_cnt),0) hl_ord_cnt,
    nvl(max(ofo2.ma_ord_cnt),0) ma_ord_cnt,
    nvl(max(ofo2.cr_wr_off_cnt),0) cr_wr_off_cnt,
    nvl(max(ofo2.hl_wr_off_cnt),0) hl_wr_off_cnt,
    nvl(max(ofo2.ma_wr_off_cnt),0) ma_wr_off_cnt,
    CASE WHEN substring(max(CASE WHEN ofo.mag_cd = 'CNS'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('C','E')
         THEN 'R'
         WHEN substring(max(CASE WHEN ofo.mag_cd = 'CNS'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) = 'A'
         THEN 'O'
         WHEN substring(max(CASE WHEN ofo.mag_cd = 'CNS'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || nvl(ofo.set_cd,'X') || oa.svc_status_code
                         END),9) in ('BD','DD')
         THEN 'N'
         WHEN substring(max(CASE WHEN ofo.mag_cd = 'CNS'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('B','D')
         THEN 'D'
    END cr_lst_sub_ord_role_cd,
    CASE WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRH'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('C','E')
         THEN 'R'
         WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRH'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) = 'A'
         THEN 'O'
         WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRH'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || nvl(ofo.set_cd,'X') || oa.svc_status_code
                         END),9) in ('BD','DD')
         THEN 'N'
         WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRH'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('B','D')
         THEN 'D'
    END hl_lst_sub_ord_role_cd,
    CASE WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('C','E')
         THEN 'R'
         WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) = 'A'
         THEN 'O'
         WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd || oa.svc_status_code
                         END),9) in ('BD','DD')
         THEN 'N'
         WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('B','D')
         THEN 'D'
    END ma_lst_sub_ord_role_cd,
    nvl(max(ofo2.shm_ltd_pd_amt),0) shm_ltd_pd_amt,
    nvl(max(ofo2.shm_canc_bad_dbt_cnt),0) shm_canc_bad_dbt_cnt,
    nvl(max(ofo2.shm_canc_cust_cnt),0) shm_canc_cust_cnt,
    nvl(max(ofo2.shm_dm_ord_cnt),0) shm_dm_ord_cnt,
    nvl(max(ofo2.shm_em_ord_cnt),0) shm_em_ord_cnt,
    nvl(max(ofo2.shm_dnr_ord_cnt),0) shm_dnr_ord_cnt,
    nvl(max(ofo2.shm_ord_cnt),0) shm_ord_cnt,
    nvl(max(ofo2.shm_wr_off_cnt),0) shm_wr_off_cnt,
    MAX(CASE WHEN oa.magazine_code = 'SHM'
           AND oa.svc_status_code = 'A'
           AND ofo.entr_typ_cd != 'F' 
           AND ofo.stat_cd = 'B'
           AND (ofo.src_cd NOT IN ('CA','CC')
                OR (substring(ofo.keycode,1,4) = 'LIFE'))
           AND oa.hash_account_id = ct.shm_comb_acct_id
          THEN 'Y' 
          ELSE 'N' 
        END) shm_actv_flg,
    MAX(CASE WHEN oa.magazine_code = 'SHM'
            AND oa.svc_status_code in ('A','E')
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (substring(ofo.keycode,1,4) = 'LIFE'))
            AND oa.hash_account_id = ct.shm_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) shm_actv_rptg_flg,
     MAX(CASE WHEN oa.magazine_code = 'SHM'
            AND oa.svc_status_code = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND ofo.set_cd in ('A','B')
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (substring(ofo.keycode,1,4) = 'LIFE'))
            AND ofo.cr_stat_cd in ('C', 'D', 'E', 'F', 'G', 'I')
            AND oa.hash_account_id = ct.shm_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) shm_actv_pd_flg,
      CASE WHEN substring(MAX(CASE WHEN oa.magazine_code = 'SHM' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.cancel_reason_code 
                            ELSE NULL 
                       END),9) IN ('06','50')
       THEN 'Y' 
       ELSE 'N' 
  END shm_canc_bad_dbt_flg, 
    CASE WHEN substring(MAX(CASE WHEN oa.magazine_code = 'SHM' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.cancel_reason_code 
                            ELSE NULL 
                       END),9) NOT IN ('06','14','50') 
       THEN 'Y' 
       ELSE 'N' 
  END shm_canc_cust_flg, 
   MAX(CASE WHEN oa.magazine_code = 'SHM'
            AND oa.svc_status_code = 'E'
            AND oa.hash_account_id = ct.shm_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) shm_crd_pend_flg,
    DECODE(substring(MAX(CASE WHEN oa.magazine_code = 'SHM' 
                         THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                              ofo.cr_stat_cd 
                         ELSE NULL 
                    END),9), 
         'C','P', 
         'D','P', 
         'E','P', 
         'F','P', 
         'G','P', 
         'I','P', 
         'A','U', 
         'H','U', 
         'B','W', 
         'J','W',NULL) shm_crd_stat_cd, 
     MAX(CASE WHEN oa.magazine_code = 'SHM' 
           THEN iss.pub_date 
           ELSE NULL 
      END) shm_exp_dt, 
     MAX(CASE WHEN ofo.canc_rsn_cd not in ('06','14','50')
           AND ofo.mag_cd = 'SHM'
           THEN ofo.canc_dt
      END) shm_lst_canc_dt,
     MAX(CASE WHEN oa.magazine_code = 'SHM' 
           THEN ofo.pmt_dt 
           ELSE NULL 
      END) shm_lst_pmt_dt, 
       MAX(CASE WHEN oa.magazine_code = 'SHM' 
           THEN 'Y' 
           ELSE 'N' 
      END) shm_flg,
      CASE WHEN MAX(CASE WHEN oa.magazine_code = 'SHM'
                      AND oa.svc_status_code = 'C'
                      AND oa.hash_account_id = ct.shm_comb_acct_id
                     THEN 'Y'
                     ELSE 'N'
                END) = 'Y'
       THEN 'Y'
       ELSE 'N'
      END shm_exp_flg,
        substring(MIN(CASE WHEN oa.magazine_code = 'SHM' 
                   AND ofo.orig_strt_iss_num > 0
                   AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_shm_brk_dt, STRPOS(obt.last_shm_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num)
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) shm_curr_mbr_keycode,
      substring(MAX(CASE WHEN oa.magazine_code = 'SHM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.shm_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') ||
                         ofo.keycode
             END),9) shm_curr_ord_keycode, 
      substring(MIN(CASE WHEN oa.magazine_code = 'SHM' 
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) shm_fst_mbr_keycode, 
       substring(MAX(CASE WHEN oa.magazine_code = 'SHM' 
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) shm_lst_ord_keycode,
      MAX(CASE WHEN oa.magazine_code = 'SHM'
            AND substring(ofo.keycode,1,4) = 'LIFE'
            AND oa.svc_status_code = 'A'
            AND oa.hash_account_id = ct.shm_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) shm_lt_sub_flg, 
      MAX(CASE WHEN oa.magazine_code = 'SHM'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_status_code = 'D'
            AND oa.hash_account_id = ct.shm_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) shm_non_sub_dnr_flg,
       MAX(CASE WHEN oa.magazine_code = 'SHM' AND (ofo.set_cd NOT IN('B','D') or ofo.stat_cd not in ('D') )
           THEN obt.shm_brks_cnt 
           ELSE NULL 
  END) shm_brks_cnt, 
      substring(MAX(CASE WHEN oa.magazine_code = 'SHM'
                     AND oa.svc_status_code <> 'D' 
                    THEN lpad(oa.expr_iss_num, 5, 0)||oa.renewal_cnt
               END), 6) shm_rnw_cnt,
    MAX(CASE WHEN oa.magazine_code = 'SHM'
            AND oa.svc_status_code = 'A'
            AND ofo.set_cd IN ('C','E')
            AND ofo.stat_cd = 'B'
            AND oa.hash_account_id = ct.shm_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) shm_rec_flg, 
     substring(MAX(CASE WHEN oa.magazine_code = 'SHM'
                   AND oa.hash_account_id = ct.shm_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       oa.svc_status_code
             END),9) shm_svc_stat_cd, 
     substring(MIN(CASE WHEN oa.magazine_code = 'SHM' 
                   AND ofo.orig_strt_iss_num > 0
                   AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_shm_brk_dt, STRPOS(obt.last_shm_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num) 
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) shm_curr_mbr_src_cd,   
     substring(MAX(CASE WHEN oa.magazine_code = 'SHM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.shm_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                   DECODE(ofo.src_cd,                                                     
                                          'AF','1',                                                       
                                          'BA','2',                                                       
                                          'BB','3',                                                       
                                          'BE','4',                                                       
                                          'BF','5',                                                       
                                          'BH','6',                                                       
                                          'CA','7',                                                       
                                          'CC','7',                                                       
                                          substring(ofo.keycode,1,1))
             END),9) shm_curr_ord_src_cd, 
     substring(MIN(CASE WHEN oa.magazine_code = 'SHM' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) shm_fst_ord_src_cd, 
       substring(MAX(CASE WHEN oa.magazine_code = 'SHM' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) shm_lst_ord_src_cd, 
          substring(MIN(CASE WHEN oa.magazine_code = 'SHM'
             AND ofo.set_cd not in ('B','D') 
             AND shm_brks_cnt = 1
             AND ofo.orig_strt_iss_num >= obt.shm_orig_strt_iss_num
             AND ofo.orig_strt_iss_num < cast(substring(obt.last_shm_brk_dt, STRPOS(obt.last_shm_brk_dt, '|')+1) as integer)
            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  WHEN oa.magazine_code = 'SHM' 
                   AND shm_brks_cnt > 1
           AND ofo.orig_strt_iss_num < cast(substring(obt.last_shm_brk_dt, STRPOS(obt.last_shm_brk_dt, '|')+1) as integer)
           AND ofo.orig_strt_iss_num >= cast(substring(obt.prior_shm_brk_dt, STRPOS(obt.prior_shm_brk_dt, '|')+1) as integer)
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) shm_prior_mbr_src_cd, 
      MAX(CASE WHEN oa.magazine_code = 'SHM'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_status_code != 'D'
            AND oa.hash_account_id = ct.shm_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) shm_sub_dnr_flg,
      substring(MAX(CASE WHEN oa.magazine_code = 'SHM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.shm_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.term_mth_cnt
             END),9) shm_curr_ord_term, 
      MIN(CASE WHEN oa.magazine_code = 'SHM' 
            AND ofo.set_cd IN ('B','D') 
           THEN ofo.ord_dt 
           ELSE NULL 
      END) shm_fst_dnr_dt, 
       MAX(CASE WHEN oa.magazine_code = 'SHM'
            AND oa.svc_status_code = 'A'
            AND substring(oa.keycode,1,4) = 'AUTO'
            AND oa.hash_account_id = ct.shm_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) shm_auto_rnw_flg, 
      MAX(CASE WHEN oa.magazine_code = 'SHM' 
            AND (
                  ofo.set_cd IN ('A','C','E')
                  or
                  (ofo.set_cd in ('B', 'D') and ofo.stat_cd not in ('D'))
                ) 
           THEN 'Y' 
           ELSE 'N' 
      END) shm_non_dnr_flg, 
        MIN(CASE WHEN oa.magazine_code = 'SHM'                
              AND ofo.orig_strt_iss_num > 0
              AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_shm_brk_dt, STRPOS(obt.last_shm_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num) 
              AND ( nvl(ofo.set_cd, 'Z') not in ('B','D') or ofo.stat_cd not in ('D') )
             THEN ofo.ord_dt
             ELSE null
    END) shm_curr_mbr_dt,  
       max(CASE WHEN ofo.stat_cd = 'B'
              AND oa.magazine_code = 'SHM'
              AND oa.svc_status_code = 'A'
              AND oa.hash_account_id = ct.shm_comb_acct_id
             THEN ofo.ord_dt
      END) shm_curr_ord_dt,
        min(CASE WHEN ofo.mag_cd = 'SHM'  --ddbarr nvl(ofo.set_cd,'X') not in ('C','E')
             THEN ofo.ord_dt
        END) shm_fst_ord_dt,
        max(CASE WHEN ofo.canc_rsn_cd in ('06','50')
              AND ofo.mag_cd = 'SHM'
             THEN ofo.canc_dt
        END) shm_lst_canc_bad_dbt_dt,
        max(CASE WHEN ofo.set_cd in ('B','D')
              AND ofo.mag_cd = 'SHM'
             THEN ofo.ord_dt
        END) shm_lst_dnr_ord_dt,
          max(CASE WHEN ofo.mag_cd = 'SHM'  --ddbarr nvl(ofo.set_cd,'X') not in ('C','E')
             THEN ofo.ord_dt
        END) shm_lst_ord_dt,
        CASE WHEN substring(max(CASE WHEN ofo.mag_cd = 'SHM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('C','E')
         THEN 'R'
         WHEN substring(max(CASE WHEN ofo.mag_cd = 'SHM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) = 'A'
         THEN 'O'
         WHEN substring(max(CASE WHEN ofo.mag_cd = 'SHM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || nvl(ofo.set_cd,'X') || oa.svc_status_code
                         END),9) in ('BD','DD')
         THEN 'N'
         WHEN substring(max(CASE WHEN ofo.mag_cd = 'SHM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('B','D')
         THEN 'D'
        END shm_lst_sub_ord_role_cd
	FROM                cr_temp.print_external_ref_temp     er      
        left join       cr_temp.offline_account_tmp4        oa      on er.hash_account_id  = oa.hash_account_id
        left join       cr_temp.agg_print_order_tmp3        ofo     on oa.hash_account_id  = ofo.hash_account_id
        left join       cr_temp.print_cancel_act            OCA     on ofo.ord_id          = oca.action_id      
        left join       cr_temp.mt_ofl_brks_temp            obt     on er.individual_id    = obt.individual_id              
        left join       etl_proc.lookup_magazine            iss     on oa.magazine_code    = iss.magazine_code  and oa.expr_iss_num = iss.iss_num
        left join       cr_temp.mt_ofl_de_temp              de      on er.individual_id    = de.individual_id
        left join       cr_temp.mt_ofl_combine_temp         ct      on er.individual_id    = ct.individual_id
        left join       cr_temp.mt_ofl_sum_tmp1             de_cnt  on er.individual_id    = de_cnt.individual_id
        left join       cr_temp.mt_ofl_sum_tmp2             ofo2    on er.individual_id    = ofo2.individual_id

WHERE 
    NOT (oa.hash_account_id IS NULL AND oa.source_account_id IS NULL AND de.individual_id IS NULL)
    
GROUP BY er.individual_id

;