----------------------------------
/* CREATE TEMP MT OFL BRKS TEMP */
----------------------------------

DROP TABLE IF EXISTS agg.mt_ofl_brks_temp;
CREATE TABLE agg.mt_ofl_brks_temp DISTKEY(individual_id)
as 
select
  individual_id,
  min(cns_orig_strt_iss_num) cns_orig_strt_iss_num,
  min(crh_orig_strt_iss_num) crh_orig_strt_iss_num,
  min(crm_orig_strt_iss_num) crm_orig_strt_iss_num,
  min(shm_orig_strt_iss_num) shm_orig_strt_iss_num,
    max(case when cro_brk_dt_rnk = 1
           then nvl2(cro_brk_dt, cro_brk_dt||'|'||nvl(orig_strt_iss_num,'0'), null)
           else null
      end) last_cro_brk_dt,
  max(case when cro_brk_dt_rnk = 2
           then nvl2(cro_brk_dt, cro_brk_dt||'|'||nvl(orig_strt_iss_num,'0'), null)
           else null
      end) prior_cro_brk_dt,
  count(cro_brk_dt) cro_brks_cnt,
  max(case when hl_brk_dt_rnk = 1
           then nvl2(hl_brk_dt, hl_brk_dt||'|'||nvl(orig_strt_iss_num,'0'), null)
           else null
      end) last_hl_brk_dt,
  max(case when hl_brk_dt_rnk = 2
           then nvl2(hl_brk_dt, hl_brk_dt||'|'||nvl(orig_strt_iss_num,'0'), null)
           else null
      end) prior_hl_brk_dt,
  count(hl_brk_dt) hl_brks_cnt,
  max(case when ma_brk_dt_rnk = 1
           then nvl2(ma_brk_dt, ma_brk_dt||'|'||nvl(orig_strt_iss_num,'0'), null)
           else null
      end) last_ma_brk_dt,
  max(case when ma_brk_dt_rnk = 2
           then nvl2(ma_brk_dt, ma_brk_dt||'|'||nvl(orig_strt_iss_num,'0'), null)
           else null
      end) prior_ma_brk_dt,
  count(ma_brk_dt) ma_brks_cnt,
  max(case when shm_brk_dt_rnk = 1
           then nvl2(shm_brk_dt, shm_brk_dt||'|'||nvl(orig_strt_iss_num,'0'), null)
           else null
      end) last_shm_brk_dt,
  max(case when shm_brk_dt_rnk = 2
           then nvl2(shm_brk_dt, shm_brk_dt||'|'||nvl(orig_strt_iss_num,'0'), null)
           else null
      end) prior_shm_brk_dt,
  count(shm_brk_dt) shm_brks_cnt
from 
  (select
     t2.individual_id,
     cro_brk_dt,
     hl_brk_dt, 
     ma_brk_dt,
     shm_brk_dt,
     row_number() over (partition by t2.individual_id
                        order by cro_brk_dt desc nulls last) cro_brk_dt_rnk,
     row_number() over (partition by t2.individual_id
                        order by hl_brk_dt desc nulls last) hl_brk_dt_rnk,
     row_number() over (partition by t2.individual_id
                        order by ma_brk_dt desc nulls last) ma_brk_dt_rnk,
     row_number() over (partition by t2.individual_id
                        order by shm_brk_dt desc nulls last) shm_brk_dt_rnk,
    orig_strt_iss_num,
    cns_orig_strt_iss_num,
    crh_orig_strt_iss_num,
    crm_orig_strt_iss_num,
    shm_orig_strt_iss_num
   from
     (select 
        oo.individual_id,
        case when oa.magazine_code = 'CNS'
             then case when months_between(STRT_LKP.pub_date,
                                           nvl(max(end_lkp.pub_date) over (partition by
                                                              oo.individual_id,
                                                              oa.magazine_code
                                                            order by STRT_LKP.pub_date
                                                            rows between unbounded preceding and 1 preceding)
                                               ,STRT_LKP.pub_date)) > 6
                       then max(end_lkp.pub_date) over (partition by
                                                              oo.individual_id,
                                                              oa.magazine_code
                                                            order by STRT_LKP.pub_date
                                                            rows between unbounded preceding and 1 preceding)
                       else null
                  end
             else null
        end cro_brk_dt,
        case when oa.magazine_code = 'CNS'
             then min(orig_strt_iss_num) over (partition by  oo.individual_id, oa.magazine_code) 
    end cns_orig_strt_iss_num,
    case when oa.magazine_code = 'CRH'
             then case when months_between(STRT_LKP.pub_date,
                                           nvl(max(end_lkp.pub_date) over (partition by
                                                              oo.individual_id,
                                                              oa.magazine_code
                                                            order by STRT_LKP.pub_date
                                                            rows between unbounded preceding and 1 preceding)
                                               ,STRT_LKP.pub_date)) > 6
                       then max(end_lkp.pub_date) over (partition by
                                                              oo.individual_id,
                                                              oa.magazine_code
                                                            order by STRT_LKP.pub_date
                                                            rows between unbounded preceding and 1 preceding)
                       else null
                  end
             else null
        end hl_brk_dt,
        case when oa.magazine_code = 'CRH'
             then min(orig_strt_iss_num) over (partition by  oo.individual_id, oa.magazine_code) 
    end crh_orig_strt_iss_num,
    case when oa.magazine_code = 'CRM'
             then case when months_between(STRT_LKP.pub_date,
                                           nvl(max(end_lkp.pub_date) over (partition by
                                                              oo.individual_id,
                                                              oa.magazine_code
                                                            order by STRT_LKP.pub_date
                                                            rows between unbounded preceding and 1 preceding)
                                               ,STRT_LKP.pub_date)) > 6
                       then max(end_lkp.pub_date) over (partition by
                                                              oo.individual_id,
                                                              oa.magazine_code
                                                            order by STRT_LKP.pub_date
                                                            rows between unbounded preceding and 1 preceding)
                       else null
                  end
             else null
        end ma_brk_dt,
        case when oa.magazine_code = 'CRM'
             then min(orig_strt_iss_num) over (partition by  oo.individual_id, oa.magazine_code) 
    end crm_orig_strt_iss_num,
  case when oa.magazine_code = 'SHM'
             then case when months_between(STRT_LKP.pub_date,
                                           nvl(max(end_lkp.pub_date) over (partition by
                                                              oo.individual_id,
                                                              oa.magazine_code
                                                            order by STRT_LKP.pub_date
                                                            rows between unbounded preceding and 1 preceding)
                                               ,STRT_LKP.pub_date)) > 6
                       then max(end_lkp.pub_date) over (partition by
                                                              oo.individual_id,
                                                              oa.magazine_code
                                                            order by STRT_LKP.pub_date
                                                            rows between unbounded preceding and 1 preceding)
                       else null
                  end
             else null
        end shm_brk_dt,
  case when oa.magazine_code = 'SHM'
             then min(orig_strt_iss_num) over (partition by  oo.individual_id, oa.magazine_code) 
    end shm_orig_strt_iss_num,
    OO.ORIG_STRT_ISS_NUM   
    
from prod.agg_print_order OO 
        inner join      (select acc.hash_account_id, magazine_code
                        FROM prod.account acc
                        inner JOIN prod.print_account_detail pad
                        ON  pad.hash_account_id = acc.hash_account_id 
                            and ACC.SOURCE_NAME = 'CDS' ) OA                  
                                                          on oo.hash_account_id = oa.hash_account_id

        inner join      etl_proc.lookup_magazine STRT_LKP on STRT_LKP.magazine_code = oa.magazine_code and STRT_LKP.iss_num = oo.orig_strt_iss_num
        inner join      etl_proc.lookup_magazine end_lkp  on end_lkp.magazine_code = oa.magazine_code and end_lkp.iss_num = (oo.orig_strt_iss_num + oo.term_mth_cnt)

where 1=1
  and ( set_cd not in('B','D') or stat_cd not in ('D') )
  and oa.magazine_code in ('CNS', 'CRM', 'CRH', 'SHM'))
       t2)
group by individual_id;

DROP TABLE IF EXISTS agg.mt_ofl_combine_temp_temp;
CREATE TABLE agg.mt_ofl_combine_temp_temp
AS
SELECT  individual_id,
        CASE WHEN oa.magazine_code = 'CNS'
                        THEN nvl(to_char(iss.pub_date,'YYYYMMDD'),'00000000') || oa.hash_account_id
                        ELSE NULL
                   END cr_comb_acct_id,
        CASE WHEN oa.magazine_code = 'CRH'
                        THEN nvl(to_char(iss.pub_date,'YYYYMMDD'),'00000000') || oa.hash_account_id
                        ELSE NULL
                   END hl_comb_acct_id,
        CASE WHEN oa.magazine_code = 'CRM'
                        THEN nvl(to_char(iss.pub_date,'YYYYMMDD'),'00000000') || oa.hash_account_id
                        ELSE NULL
                   END ma_comb_acct_id,
        CASE WHEN oa.magazine_code = 'SHM'
                        THEN nvl(to_char(iss.pub_date,'YYYYMMDD'),'00000000') || oa.hash_account_id
                        ELSE NULL
                   END shm_comb_acct_id
from    (select acc.hash_account_id, magazine_code, expr_iss_num, individual_id 
                        FROM prod.account acc
                        LEFT JOIN prod.print_account_detail pad
                        ON  pad.hash_account_id = acc.hash_account_id 
                            and ACC.SOURCE_NAME = 'CDS' ) oa                  
        left join      ETL_PROC.LOOKUP_MAGAZINE ISS on oa.magazine_code = iss.magazine_code and oa.expr_iss_num = iss.iss_num
        where 1=1 and oa.magazine_code in ('CNS','CRH','CRM','SHM');
        
DROP TABLE IF EXISTS agg.mt_ofl_combine_temp;
CREATE TABLE agg.mt_ofl_combine_temp DISTKEY(individual_id)
AS
SELECT individual_id,
       substring(max(cr_comb_acct_id),9) cr_comb_acct_id,
       substring(max(hl_comb_acct_id),9) hl_comb_acct_id,
       substring(max(ma_comb_acct_id),9) ma_comb_acct_id,
       substring(max(shm_comb_acct_id),9) shm_comb_acct_id
FROM agg.mt_ofl_combine_temp_temp ct
GROUP BY individual_id;
        
DROP TABLE IF EXISTS agg.mt_ofl_de_temp;
CREATE TABLE agg.mt_ofl_de_temp DISTKEY(individual_id)
AS
SELECT 
        det.individual_id,
        det.magazine_code,
        max(det.exp_date) exp_date,
        count(*) de_cnt,
        substring(max(CASE WHEN det.magazine_code = 'CNS'
                        THEN nvl(to_char(det.exp_date,'YYYYMMDD'),'00010101') ||
                             det.last_source_code
                        ELSE null
                   END),9) eds_lst_src_cd,
        substring(max(CASE WHEN det.magazine_code = 'CNS'
                        THEN nvl(to_char(det.exp_date,'YYYYMMDD'),'00010101') ||
                             det.morbank_mtch_code
                        ELSE null
                   END),9) morbank_mtch_code
      FROM prod.legacy_dead_expires det
      WHERE 1=1
      GROUP BY det.individual_id,det.magazine_code;

DROP TABLE IF EXISTS agg.mt_ofl_sum_tmp1;
CREATE TABLE agg.mt_ofl_sum_tmp1 DISTKEY(individual_id)
AS
SELECT DE.INDIVIDUAL_ID, 
       de.magazine_code,
       CASE WHEN de.magazine_code = 'CNS'
            THEN DECODE(max(cds.INDIVIDUAL_ID),null ,COUNT(*)-1,COUNT(*)) 
            ELSE NULL
       END DE_CR_BRKS_CNT,
       CASE WHEN de.magazine_code = 'CRH'
            THEN DECODE(max(cds.INDIVIDUAL_ID),null ,COUNT(*)-1,COUNT(*)) 
            ELSE NULL
       END DE_HL_BRKS_CNT
FROM prod.legacy_dead_expires DE left join 
     (SELECT DISTINCT INDIVIDUAL_ID, MAG_CD
       FROM prod.agg_print_order
      WHERE MAG_CD in ('CNS','CRH')
        and (KEYCODE not in ('VFREE','ZFREE')
          or KEYCODE is null)) CDS on DE.INDIVIDUAL_ID = CDS.INDIVIDUAL_ID and de.magazine_code = CDS.MAG_CD
where 1=1
    and de.magazine_code in ('CNS', 'CRH')
GROUP BY DE.INDIVIDUAL_ID, de.magazine_code;
 
DROP TABLE IF EXISTS agg.mt_ofl_sum_tmp2;
CREATE TABLE agg.mt_ofl_sum_tmp2 DISTKEY(individual_id)
AS
SELECT
  individual_id,
  sum(CASE WHEN ofo.mag_cd = 'CNS'
           THEN ofo.pd_amt
      END) cr_ltd_pd_amt,
  sum(CASE WHEN ofo.mag_cd = 'CRH'
           THEN ofo.pd_amt
      END) hl_ltd_pd_amt,
  sum(CASE WHEN ofo.mag_cd = 'CRM'
           THEN ofo.pd_amt
      END) ma_ltd_pd_amt,
  sum(CASE WHEN ofo.mag_cd = 'SHM'
           THEN ofo.pd_amt
      END) shm_ltd_pd_amt,
  count(CASE WHEN ofo.canc_rsn_cd in ('06','50')
              AND ofo.mag_cd = 'CNS'
             THEN 1
        END) cr_canc_bad_dbt_cnt,
  count(CASE WHEN ofo.canc_rsn_cd in ('06','50')
              AND ofo.mag_cd = 'CRH'
             THEN 1
        END) hl_canc_bad_dbt_cnt,
  count(CASE WHEN ofo.canc_rsn_cd in ('06','50')
              AND ofo.mag_cd = 'CRM'
             THEN 1
        END) ma_canc_bad_dbt_cnt,
  count(CASE WHEN ofo.canc_rsn_cd in ('06','50')
              AND ofo.mag_cd = 'SHM'
             THEN 1
        END) shm_canc_bad_dbt_cnt,
  count(CASE WHEN ofo.canc_rsn_cd not in ('06','14','50')
              AND ofo.mag_cd = 'CNS'
             THEN 1
        END) cr_canc_cust_cnt,
  count(CASE WHEN ofo.canc_rsn_cd not in ('06','14','50')
              AND ofo.mag_cd = 'CRH'
             THEN 1
        END) hl_canc_cust_cnt,
  count(CASE WHEN ofo.canc_rsn_cd not in ('06','14','50')
              AND ofo.mag_cd = 'CRM'
             THEN 1
        END) ma_canc_cust_cnt,
  count(CASE WHEN ofo.canc_rsn_cd not in ('06','14','50')
              AND ofo.mag_cd = 'SHM'
             THEN 1
        END) shm_canc_cust_cnt,
  count(CASE WHEN ofo.mag_cd = 'CNS'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND ((substring(ofo.keycode,1,1) in ('A','G','X'))
                   OR (substring(ofo.keycode,1,1) = 'D' 
                     AND nvl(substring(ofo.keycode,2,1),'X') != 'N')
                   OR (substring(ofo.keycode,1,1) in ('B') 
                     AND nvl(substring(ofo.keycode,9,1),'X') not in ('A','B','C','S'))
                   OR (substring(ofo.keycode,1,1) in ('U') 
                     AND regexp_instr(nvl(substring(ofo.keycode,9,1),'1'),'[^[:alpha:]]') = 1)
                   OR (substring(ofo.keycode,1,1) = 'R' 
                     AND nvl(substring(ofo.keycode,9,1),'X') not in ('B','C','D','E','F','T'))
                   OR (substring(ofo.keycode,1,1) in ('U') 
                     AND length(ofo.keycode) = 7
                     AND regexp_instr(substring(ofo.keycode,6,1),'[^[:alpha:]]') = 1))
             THEN 1
        END) cr_dm_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CRH'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND ((substring(ofo.keycode,1,1) in ('A','G','X'))
                   OR (substring(ofo.keycode,1,1) = 'D' 
                     AND nvl(substring(ofo.keycode,2,1),'X') != 'N')
                   OR (substring(ofo.keycode,1,1) = 'B' 
                     AND nvl(substring(ofo.keycode,9,1),'X') not in ('A','B','C','S'))
                   OR (substring(ofo.keycode,1,1) = 'U' 
                     AND regexp_instr(nvl(substring(ofo.keycode,9,1),'1'),'[^[:alpha:]]') = 1)
                   OR (substring(ofo.keycode,1,1) = 'R' 
                     AND nvl(substring(ofo.keycode,9,1),'X') not in ('B','C','D','E','F','T'))
                   OR (substring(ofo.keycode,1,1) in ('U') 
                     AND length(ofo.keycode) = 7
                     AND regexp_instr(substring(ofo.keycode,6,1),'[^[:alpha:]]') = 1))
             THEN 1
        END) hl_dm_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CRM'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND ((substring(ofo.keycode,1,1) in ('A','G','X'))
                   OR (substring(ofo.keycode,1,1) = 'D' AND nvl(substring(ofo.keycode,2,1),'X') != 'N')
                   OR (substring(ofo.keycode,1,1) = 'B' AND nvl(substring(ofo.keycode,9,1),'X') not in ('A','B','C','S'))
                   OR (substring(ofo.keycode,1,1) = 'U' 
                            AND regexp_instr(nvl(substring(ofo.keycode,9,1),'1'),'[^[:alpha:]]') = 1)
                   OR (substring(ofo.keycode,1,1) = 'R' AND nvl(substring(ofo.keycode,9,1),'X') not in ('B','C','D','E','F','T')) 
                   OR (substring(ofo.keycode,1,1) = 'U' 
                         AND length(ofo.keycode) = 7
                         AND regexp_instr(substring(ofo.keycode,6,1),'[^[:alpha:]]') = 1 ))
             THEN 1
        END) ma_dm_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'SHM'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND ((substring(ofo.keycode,1,1) in ('A','G','X'))
                   OR (substring(ofo.keycode,1,1) = 'D' 
                     AND nvl(substring(ofo.keycode,2,1),'X') != 'N')
                   OR (substring(ofo.keycode,1,1) = 'B' 
                     AND nvl(substring(ofo.keycode,9,1),'X') not in ('A','B','C','S'))
                   OR (substring(ofo.keycode,1,1) = 'U' 
                         AND regexp_instr(nvl(substring(ofo.keycode,9,1),'1'),'[^[:alpha:]]') = 1)
                   OR (substring(ofo.keycode,1,1) = 'R' 
                     AND nvl(substring(ofo.keycode,9,1),'X') not in ('B','C','D','E','F','T'))
                   OR (substring(ofo.keycode,1,1) = 'U' 
                     AND length(ofo.keycode) = 7
                     AND regexp_instr(substring(ofo.keycode,6,1),'[^[:alpha:]]') = 1))
             THEN 1
        END) shm_dm_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CNS'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND (   (substring(ofo.keycode,1,1) = 'E')
                   OR (substring(ofo.keycode,1,1) = 'D' AND substring(ofo.keycode,2,1) = 'N')
                   OR (substring(ofo.keycode,1,1) = 'B' AND substring(ofo.keycode,9,1) in ('A','B','C','S'))
                   OR (substring(ofo.keycode,1,1) = 'U' AND regexp_instr(substring(ofo.keycode,9,1),'[[:alpha:]]') = 1)
                   OR (substring(ofo.keycode,1,1) = 'R' AND substring(ofo.keycode,9,1) in ('B','C','D','E','F'))
                   OR (substring(ofo.keycode,4,6) = 'FAILCC')     
                   OR (substring(ofo.keycode,1,1) = 'U' 
                         AND length(ofo.keycode) = 7
                         AND substring(ofo.keycode,6,1) in ('A','B','C','D','E','F'))
                   )
             THEN 1
        END) cr_em_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CRH'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND (   (substring(ofo.keycode,1,1) = 'E')
                   OR (substring(ofo.keycode,1,1) = 'D' AND substring(ofo.keycode,2,1) = 'N')
                   OR (substring(ofo.keycode,1,1) = 'B' AND substring(ofo.keycode,9,1) in ('A','B','C','S'))
                   OR (substring(ofo.keycode,1,1) = 'U' AND regexp_instr(substring(ofo.keycode,9,1),'[[:alpha:]]') = 1)
                   OR (substring(ofo.keycode,1,1) = 'R' AND substring(ofo.keycode,9,1) in ('B','C','D','E','F'))
                   OR (substring(ofo.keycode,4,6) = 'FAILCC')     
                   OR (substring(ofo.keycode,1,1) = 'U' 
                         AND length(ofo.keycode) = 7
                         AND substring(ofo.keycode,6,1) in ('A','B','C','D','E','F'))
                  )
             THEN 1
        END) hl_em_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CRM'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND (   (substring(ofo.keycode,1,1) = 'E')
                   OR (substring(ofo.keycode,1,1) = 'D' AND substring(ofo.keycode,2,1) = 'N')
                   OR (substring(ofo.keycode,1,1) = 'B' AND substring(ofo.keycode,9,1) in ('A','B','C','S'))
                   OR (substring(ofo.keycode,1,1) = 'U' AND regexp_instr(substring(ofo.keycode,9,1),'[[:alpha:]]') = 1)
                   OR (substring(ofo.keycode,1,1) = 'R' AND substring(ofo.keycode,9,1) in ('B','C','D','E','F'))
                   OR (substring(ofo.keycode,4,6) = 'FAILCC')     
                   OR (substring(ofo.keycode,1,1) = 'U' 
                         AND length(ofo.keycode) = 7
                         AND substring(ofo.keycode,6,1) in ('A','B','C','D','E','F'))
                  )
             THEN 1
        END) ma_em_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'SHM'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND (   (substring(ofo.keycode,1,1) = 'E')
                   OR (substring(ofo.keycode,1,1) = 'D' AND substring(ofo.keycode,2,1) = 'N')
                   OR (substring(ofo.keycode,1,1) = 'B' AND substring(ofo.keycode,9,1) in ('A','B','C','S'))
                   OR (substring(ofo.keycode,1,1) = 'U' AND regexp_instr(substring(ofo.keycode,9,1),'[[:alpha:]]') = 1)
                   OR (substring(ofo.keycode,1,1) = 'R' AND substring(ofo.keycode,9,1) in ('B','C','D','E','F'))
                   OR (substring(ofo.keycode,4,6) = 'FAILCC')     
                   OR (substring(ofo.keycode,1,1) = 'U' 
                         AND length(ofo.keycode) = 7
                         AND substring(ofo.keycode,6,1) in ('A','B','C','D','E','F'))
                   )
             THEN 1
        END) shm_em_ord_cnt,
  count(CASE WHEN ofo.set_cd in ('B','D')
              AND ofo.mag_cd = 'CNS'
             THEN 1
        END) cr_dnr_ord_cnt,
  count(CASE WHEN ofo.set_cd in ('B','D')
              AND ofo.mag_cd = 'CRH'
             THEN 1
        END) hl_dnr_ord_cnt,
  count(CASE WHEN ofo.set_cd in ('B','D')
              AND ofo.mag_cd = 'CRM'
             THEN 1
        END) ma_dnr_ord_cnt,
  count(CASE WHEN ofo.set_cd in ('B','D')
              AND ofo.mag_cd = 'SHM'
             THEN 1
        END) shm_dnr_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CNS'
              AND nvl(ofo.set_cd,'X') not in ('C','E')
             THEN 1
        END) cr_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CRH'
              AND nvl(ofo.set_cd,'X') not in ('C','E')
             THEN 1
        END) hl_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CRM'
              AND nvl(ofo.set_cd,'X') not in ('C','E')
             THEN 1
        END) ma_ord_cnt,
   count(CASE WHEN ofo.mag_cd = 'SHM'
              AND nvl(ofo.set_cd,'X') not in ('C','E')
             THEN 1
        END) shm_ord_cnt,
  count(CASE WHEN ofo.cr_stat_cd in ('B','J')
              AND ofo.mag_cd = 'CNS'
              AND nvl(ofo.set_cd,'X') not in ('C','E')
             THEN 1
        END) cr_wr_off_cnt,
  count(CASE WHEN ofo.cr_stat_cd in ('B','J')
              AND ofo.mag_cd = 'CRH'
              AND nvl(ofo.set_cd,'X') not in ('C','E')
             THEN 1
        END) hl_wr_off_cnt,
  count(CASE WHEN ofo.cr_stat_cd in ('B','J')
              AND ofo.mag_cd = 'CRM'
              AND nvl(ofo.set_cd,'X') not in ('C','E')
             THEN 1
        END) ma_wr_off_cnt,
  count(CASE WHEN ofo.cr_stat_cd in ('B','J')
              AND ofo.mag_cd = 'SHM'
              AND nvl(ofo.set_cd,'X') not in ('C','E')
             THEN 1
        END) shm_wr_off_cnt
FROM prod.agg_print_order ofo
WHERE mag_cd in ('CNS','CRH','CRM','SHM')
  AND (KEYCODE not in ('VFREE','ZFREE') or KEYCODE is null)
GROUP BY individual_id
;

DROP TABLE IF EXISTS agg.agg_print_order_tmp3;
CREATE TABLE agg.agg_print_order_tmp3 DISTKEY(hash_account_id)
AS
select 
            set_cd
           ,ord_dt
           ,mag_cd
           ,canc_dt
           ,canc_rsn_cd
           ,orig_strt_iss_num
           ,term_mth_cnt
           ,keycode
           ,ord_id
           ,hash_account_id
           ,stat_cd
           ,src_cd
           ,pmt_dt
           ,cr_stat_cd
           ,entr_typ_cd
      from prod.agg_print_order where (keycode not in ('VFREE','ZFREE') or KEYCODE is null);

DROP TABLE IF EXISTS agg.offline_account_tmp4;
CREATE TABLE agg.offline_account_tmp4 DISTKEY(hash_account_id)
AS
select  
            oa.svc_status_code
           ,oa.hash_account_id
           ,oa.source_account_id
           ,oa.magazine_code
           ,oa.keycode
           ,oa.renewal_cnt
           ,oa.expr_iss_num 
      from (select distinct pad.svc_status_code,	acc.hash_account_id, pad.magazine_code, pad.keycode, pad.renewal_cnt, pad.expr_iss_num, acc.source_account_id
                        FROM prod.account acc
                        inner JOIN prod.print_account_detail pad
                        ON  pad.hash_account_id = acc.hash_account_id 
                            and ACC.SOURCE_NAME = 'CDS' ) OA
      where nvl(oa.magazine_code,'XXX') IN ('CNS', 'CRH', 'CRM', 'SHM', 'CRT','XXX');

;
DROP TABLE IF EXISTS agg.print_external_ref_temp;
CREATE TABLE agg.print_external_ref_temp
	DISTSTYLE KEY DISTKEY(individual_id) 
	INTERLEAVED SORTKEY(individual_id) 
as 
select INDIVIDUAL_ID, HASH_ACCOUNT_ID                             
from PROD.account 
where INDIVIDUAL_ID is not null
;
DROP TABLE IF EXISTS agg.print_cancel_act;
CREATE TABLE agg.print_cancel_act
	DISTSTYLE KEY DISTKEY(ACTION_ID) 
	INTERLEAVED SORTKEY(ACTION_ID) 
as
select ACTION_ID, CANCEL_REASON_CODE                              
from PROD.ACTION_HEADER AH                                        
inner join PROD.PRINT_CANCEL PC on AH.HASH_ACTION_ID = PC.HASH_ACTION_ID
;

----------------------------------
/* CREATE TEMP MT OFL SUM TEMP */
----------------------------------
DROP TABLE IF EXISTS agg.mt_ofl_sum_temp;
CREATE TABLE agg.mt_ofl_sum_temp
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
                              NVL(ofo.cr_stat_cd ,'')
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
                              NVL(ofo.cr_stat_cd ,'')
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
                              NVL(ofo.cr_stat_cd ,'')
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
                         NVL(ofo.keycode,'')
             END),9) cr_curr_ord_keycode, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRH'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.hl_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') ||
                         NVL(ofo.keycode,'')
             END),9) hl_curr_ord_keycode, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.ma_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') ||
                         NVL(ofo.keycode,'')
             END),9) ma_curr_ord_keycode, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CNS' 
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'99999999') || 
                       NVL(ofo.keycode ,'')
                  ELSE NULL 
             END),9) cr_fst_mbr_keycode, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRH' 
                  AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'99999999') || 
                       NVL(ofo.keycode ,'')
                  ELSE NULL 
             END),9) hl_fst_mbr_keycode, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRM' 
                  AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'99999999') || 
                       NVL(ofo.keycode ,'')
                  ELSE NULL 
             END),9) ma_fst_mbr_keycode, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CNS' 
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'00000000') || 
                       NVL(ofo.keycode ,'')
                  ELSE NULL 
             END),9) cr_lst_ord_keycode, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRH' 
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'00000000') || 
                       NVL(ofo.keycode ,'')
                  ELSE NULL 
             END),9) hl_lst_ord_keycode, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRM' 
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'00000000') || 
                       NVL(ofo.keycode ,'')
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
                       NVL(oa.svc_status_code,'')
                  WHEN de.magazine_code = 'CNS'
                  THEN '-1000000'||
                       'C'
             END),9) cr_svc_stat_cd, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRH'
                   AND oa.hash_account_id = ct.hl_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       NVL(oa.svc_status_code,'')
                  WHEN de.magazine_code = 'CRH'
                  THEN '-1000000'||
                       'C'
             END),9) hl_svc_stat_cd, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRM'
                   AND oa.hash_account_id = ct.ma_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       NVL(oa.svc_status_code,'')
             END),9) ma_svc_stat_cd,   
  substring(MAX(CASE WHEN oa.magazine_code = 'CNS'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.cr_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                   NVL(DECODE(ofo.src_cd,                                                     
                                          'AF','1',                                                       
                                          'BA','2',                                                       
                                          'BB','3',                                                       
                                          'BE','4',                                                       
                                          'BF','5',                                                       
                                          'BH','6',                                                       
                                          'CA','7',                                                       
                                          'CC','7',                                                       
                                          substring(ofo.keycode,1,1)),'')
             END),9) cr_curr_ord_src_cd,  
  substring(MAX(CASE WHEN oa.magazine_code = 'CRH'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.hl_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                   NVL(DECODE(ofo.src_cd,                                                     
                                          'AF','1',                                                       
                                          'BA','2',                                                       
                                          'BB','3',                                                       
                                          'BE','4',                                                       
                                          'BF','5',                                                       
                                          'BH','6',                                                       
                                          'CA','7',                                                       
                                          'CC','7',                                                       
                                          substring(ofo.keycode,1,1)),'')
             END),9) hl_curr_ord_src_cd,
     substring(MAX(CASE WHEN oa.magazine_code = 'CRM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_status_code = 'A'
                   AND oa.hash_account_id = ct.ma_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                   NVL(DECODE(ofo.src_cd,                                                     
                                          'AF','1',                                                       
                                          'BA','2',                                                       
                                          'BB','3',                                                       
                                          'BE','4',                                                       
                                          'BF','5',                                                       
                                          'BH','6',                                                       
                                          'CA','7',                                                       
                                          'CC','7',                                                       
                                          substring(ofo.keycode,1,1)),'')
             END),9) ma_curr_ord_src_cd,   
  substring(MIN(CASE WHEN oa.magazine_code = 'CNS' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       NVL(DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) ,'')
                  ELSE NULL 
             END),9) cr_fst_ord_src_cd, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRH' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       NVL(DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) ,'')
                  ELSE NULL 
             END),9) hl_fst_ord_src_cd, 
  substring(MIN(CASE WHEN oa.magazine_code = 'CRM' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       NVL(DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) ,'')
                  ELSE NULL 
             END),9) ma_fst_ord_src_cd, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CNS' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       NVL(DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) ,'')
                  ELSE NULL 
             END),9) cr_lst_ord_src_cd, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRH' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       NVL(DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) ,'')
                  ELSE NULL 
             END),9) hl_lst_ord_src_cd, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRM' 
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       NVL(DECODE(ofo.src_cd, 
                              'AF','1', 
                              'BA','2', 
                              'BB','3', 
                              'BE','4', 
                              'BF','5', 
                              'BH','6', 
                              'CA','7', 
                              'CC','7', 
                              substring(ofo.keycode,1,1)) ,'')
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
                       NVL(ofo.term_mth_cnt,'0')
             END),9) cr_curr_ord_term, 
   substring(MAX(CASE WHEN oa.magazine_code = 'CRH'
                    AND ofo.stat_cd = 'B'
                    AND oa.svc_status_code = 'A'
                    AND oa.hash_account_id = ct.hl_comb_acct_id
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                        NVL(ofo.term_mth_cnt,'0')
              END),9) hl_curr_ord_term, 
  substring(MAX(CASE WHEN oa.magazine_code = 'CRM'
                    AND ofo.stat_cd = 'B'
                    AND oa.svc_status_code = 'A'
                    AND oa.hash_account_id = ct.ma_comb_acct_id
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                        NVL(ofo.term_mth_cnt,'0')
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
                        NVL(ofo.keycode ,'')
                   ELSE NULL 
              END),9) cr_curr_mbr_keycode, 
   substring(MIN(CASE WHEN oa.magazine_code = 'CRH' 
                    AND ofo.orig_strt_iss_num > 0
                    AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_hl_brk_dt, STRPOS(obt.last_hl_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num)
                    AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(ofo.keycode ,'')
                   ELSE NULL 
              END),9) hl_curr_mbr_keycode, 
   substring(MIN(CASE WHEN oa.magazine_code = 'CRM'
                    AND ofo.orig_strt_iss_num > 0 
                    AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_ma_brk_dt, STRPOS(obt.last_ma_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num)
                    AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(ofo.keycode ,'')
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
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
                   ELSE NULL 
              END),9) cr_curr_mbr_src_cd, 
   substring(MIN(CASE WHEN oa.magazine_code = 'CRH' 
                    AND ofo.orig_strt_iss_num > 0
                    AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_hl_brk_dt, STRPOS(obt.last_hl_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num) 
                    AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'')
                   ELSE NULL 
              END),9) hl_curr_mbr_src_cd, 
   substring(MIN(CASE WHEN oa.magazine_code = 'CRM' 
                    AND ofo.orig_strt_iss_num > 0                  
                    AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_ma_brk_dt, STRPOS(obt.last_ma_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num) 
 AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
                   ELSE NULL 
              END),9) ma_curr_mbr_src_cd, 
     substring(MIN(CASE WHEN oa.magazine_code = 'CNS'
                      AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                      AND cro_brks_cnt = 1
                      AND ofo.orig_strt_iss_num >= obt.cns_orig_strt_iss_num
                      AND ofo.orig_strt_iss_num < cast(substring(obt.last_cro_brk_dt, STRPOS(obt.last_cro_brk_dt, '|')+1) as integer)
                     THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
                   WHEN oa.magazine_code = 'CNS' 
                    AND cro_brks_cnt > 1
                    AND ofo.orig_strt_iss_num < cast(substring(obt.last_cro_brk_dt, STRPOS(obt.last_cro_brk_dt, '|')+1) as integer)
                    AND ofo.orig_strt_iss_num >= cast(substring(obt.prior_cro_brk_dt, STRPOS(obt.prior_cro_brk_dt, '|')+1) as integer)
                    AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
                   ELSE NULL 
              END),9) cr_prior_mbr_src_cd, 
   substring(MIN(CASE WHEN oa.magazine_code = 'CRH' 
                    AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                    AND hl_brks_cnt = 1
                    AND ofo.orig_strt_iss_num >= obt.crh_orig_strt_iss_num
                    AND ofo.orig_strt_iss_num < cast(substring(obt.last_hl_brk_dt, STRPOS(obt.last_hl_brk_dt, '|')+1) as integer)                   
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
                   WHEN oa.magazine_code = 'CRH' 
                    AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                    AND hl_brks_cnt > 1
                    AND ofo.orig_strt_iss_num < cast(substring(obt.last_hl_brk_dt, STRPOS(obt.last_hl_brk_dt, '|')+1) as integer)
                    AND ofo.orig_strt_iss_num >= cast(substring(obt.prior_hl_brk_dt, STRPOS(obt.prior_hl_brk_dt, '|')+1) as integer)
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
                   ELSE NULL 
              END),9) hl_prior_mbr_src_cd, 
   substring(MIN(CASE WHEN oa.magazine_code = 'CRM' 
                    AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                    AND ma_brks_cnt = 1
                    AND ofo.orig_strt_iss_num >= obt.crm_orig_strt_iss_num
                    AND ofo.orig_strt_iss_num < cast(substring(obt.last_ma_brk_dt, STRPOS(obt.last_ma_brk_dt, '|')+1) as integer)                 
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
                   WHEN oa.magazine_code = 'CRM' 
                    AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                    AND ma_brks_cnt > 1
                    AND ofo.orig_strt_iss_num < cast(substring(obt.last_ma_brk_dt, STRPOS(obt.last_ma_brk_dt, '|')+1) as integer)
                    AND ofo.orig_strt_iss_num >= cast(substring(obt.prior_ma_brk_dt, STRPOS(obt.prior_ma_brk_dt, '|')+1) as integer)
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
                   ELSE NULL 
              END),9) ma_prior_mbr_src_cd,
     substring(MAX(CASE WHEN oa.magazine_code = 'CNS'
                      AND oa.svc_status_code <> 'D' 
                     THEN lpad(oa.expr_iss_num, 5, 0)||NVL(oa.renewal_cnt,'0')
                END), 6) cr_rnw_cnt,
     substring(MAX(CASE WHEN oa.magazine_code = 'CRH'
                      AND oa.svc_status_code <> 'D' 
                     THEN lpad(oa.expr_iss_num, 5, 0)||NVL(oa.renewal_cnt,'0')
                END), 6) hl_rnw_cnt,        
     substring(MAX(CASE WHEN oa.magazine_code = 'CRM'
                      AND oa.svc_status_code <> 'D' 
                     THEN lpad(oa.expr_iss_num, 5, 0)||NVL(oa.renewal_cnt,'0')
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
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'')
                          END),9) in ('C','E')
          THEN 'R'
          WHEN substring(max(CASE WHEN ofo.mag_cd = 'CNS'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'')
                          END),9) = 'A'
          THEN 'O'
          WHEN substring(max(CASE WHEN ofo.mag_cd = 'CNS'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || nvl(ofo.set_cd,'X') || NVL(oa.svc_status_code,'')
                          END),9) in ('BD','DD')
          THEN 'N'
          WHEN substring(max(CASE WHEN ofo.mag_cd = 'CNS'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'')
                          END),9) in ('B','D')
          THEN 'D'
     END cr_lst_sub_ord_role_cd,
     CASE WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRH'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'')
                          END),9) in ('C','E')
          THEN 'R'
          WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRH'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'')
                          END),9) = 'A'
          THEN 'O'
          WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRH'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || nvl(ofo.set_cd,'X') || NVL(oa.svc_status_code,'')
                          END),9) in ('BD','DD')
          THEN 'N'
          WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRH'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'')
                          END),9) in ('B','D')
          THEN 'D'
     END hl_lst_sub_ord_role_cd,
     CASE WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRM'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'')
                          END),9) in ('C','E')
          THEN 'R'
          WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRM'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'')
                          END),9) = 'A'
          THEN 'O'
          WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRM'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'') || NVL(oa.svc_status_code,'')
                          END),9) in ('BD','DD')
          THEN 'N'
          WHEN substring(max(CASE WHEN ofo.mag_cd = 'CRM'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'')
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
                               NVL(ofo.cr_stat_cd ,'')
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
                        NVL(ofo.keycode ,'')
                   ELSE NULL 
              END),9) shm_curr_mbr_keycode,
       substring(MAX(CASE WHEN oa.magazine_code = 'SHM'
                    AND ofo.stat_cd = 'B'
                    AND oa.svc_status_code = 'A'
                    AND oa.hash_account_id = ct.shm_comb_acct_id
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') ||
                          NVL(ofo.keycode,'')
              END),9) shm_curr_ord_keycode, 
       substring(MIN(CASE WHEN oa.magazine_code = 'SHM' 
                    AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                   THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(ofo.keycode ,'')
                   ELSE NULL 
              END),9) shm_fst_mbr_keycode, 
        substring(MAX(CASE WHEN oa.magazine_code = 'SHM' 
                   THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'00000000') || 
                        NVL(ofo.keycode ,'')
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
                     THEN lpad(oa.expr_iss_num, 5, 0)||NVL(oa.renewal_cnt,'0')
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
                        NVL(oa.svc_status_code,'')
              END),9) shm_svc_stat_cd, 
      substring(MIN(CASE WHEN oa.magazine_code = 'SHM' 
                    AND ofo.orig_strt_iss_num > 0
                    AND ofo.orig_strt_iss_num >= nvl(cast(substring(obt.last_shm_brk_dt, STRPOS(obt.last_shm_brk_dt, '|')+1) as integer), ofo.orig_strt_iss_num) 
                    AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
                   ELSE NULL 
              END),9) shm_curr_mbr_src_cd,   
      substring(MAX(CASE WHEN oa.magazine_code = 'SHM'
                    AND ofo.stat_cd = 'B'
                    AND oa.svc_status_code = 'A'
                    AND oa.hash_account_id = ct.shm_comb_acct_id
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                    NVL(DECODE(ofo.src_cd,                                                     
                                           'AF','1',                                                       
                                           'BA','2',                                                       
                                           'BB','3',                                                       
                                           'BE','4',                                                       
                                           'BF','5',                                                       
                                           'BH','6',                                                       
                                           'CA','7',                                                       
                                           'CC','7',                                                       
                                           substring(ofo.keycode,1,1)),'')
              END),9) shm_curr_ord_src_cd, 
      substring(MIN(CASE WHEN oa.magazine_code = 'SHM' 
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
                   ELSE NULL 
              END),9) shm_fst_ord_src_cd, 
        substring(MAX(CASE WHEN oa.magazine_code = 'SHM' 
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
                   ELSE NULL 
              END),9) shm_lst_ord_src_cd, 
           substring(MIN(CASE WHEN oa.magazine_code = 'SHM'
              AND ofo.set_cd not in ('B','D') 
              AND shm_brks_cnt = 1
              AND ofo.orig_strt_iss_num >= obt.shm_orig_strt_iss_num
              AND ofo.orig_strt_iss_num < cast(substring(obt.last_shm_brk_dt, STRPOS(obt.last_shm_brk_dt, '|')+1) as integer)
             THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
                   WHEN oa.magazine_code = 'SHM' 
                    AND shm_brks_cnt > 1
            AND ofo.orig_strt_iss_num < cast(substring(obt.last_shm_brk_dt, STRPOS(obt.last_shm_brk_dt, '|')+1) as integer)
            AND ofo.orig_strt_iss_num >= cast(substring(obt.prior_shm_brk_dt, STRPOS(obt.prior_shm_brk_dt, '|')+1) as integer)
                    AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                        NVL(DECODE(ofo.src_cd, 
                               'AF','1', 
                               'BA','2', 
                               'BB','3', 
                               'BE','4', 
                               'BF','5', 
                               'BH','6', 
                               'CA','7', 
                               'CC','7', 
                               substring(ofo.keycode,1,1)),'') 
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
                        NVL(ofo.term_mth_cnt,'0')
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
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'')
                          END),9) in ('C','E')
          THEN 'R'
          WHEN substring(max(CASE WHEN ofo.mag_cd = 'SHM'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'')
                          END),9) = 'A'
          THEN 'O'
          WHEN substring(max(CASE WHEN ofo.mag_cd = 'SHM'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || nvl(ofo.set_cd,'X') || NVL(oa.svc_status_code,'')
                          END),9) in ('BD','DD')
          THEN 'N'
          WHEN substring(max(CASE WHEN ofo.mag_cd = 'SHM'
                               THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || NVL(ofo.set_cd,'')
                          END),9) in ('B','D')
          THEN 'D'
         END shm_lst_sub_ord_role_cd
	FROM                agg.print_external_ref_temp     er      
        left join       agg.offline_account_tmp4        oa      on er.hash_account_id  = oa.hash_account_id
        left join       agg.agg_print_order_tmp3        ofo     on oa.hash_account_id  = ofo.hash_account_id
        left join       agg.print_cancel_act            OCA     on ofo.ord_id          = oca.action_id      
        left join       agg.mt_ofl_brks_temp            obt     on er.individual_id    = obt.individual_id              
        left join       etl_proc.lookup_magazine        iss     on oa.magazine_code    = iss.magazine_code  and oa.expr_iss_num = iss.iss_num
        left join       agg.mt_ofl_de_temp              de      on er.individual_id    = de.individual_id
        left join       agg.mt_ofl_combine_temp         ct      on er.individual_id    = ct.individual_id
        left join       agg.mt_ofl_sum_tmp1             de_cnt  on er.individual_id    = de_cnt.individual_id
        left join       agg.mt_ofl_sum_tmp2             ofo2    on er.individual_id    = ofo2.individual_id
WHERE 
    NOT (oa.hash_account_id IS NULL AND oa.source_account_id IS NULL AND de.individual_id IS NULL)
GROUP BY er.individual_id;


------------------------------
/* AGG PRINT SUMMARY CREATE */
------------------------------


DROP TABLE IF EXISTS    prod.agg_print_summary;
CREATE TABLE            prod.agg_print_summary 
	DISTSTYLE KEY DISTKEY(individual_id) 
	INTERLEAVED SORTKEY(individual_id) 
as
SELECT
 ix.individual_id                      
,ix.household_id as hh_id              
--V--SD: Here and below - Substr over max characters is NULL in Oracle and '' in RedShift. Decode workaround.
,decode(ost.cr_actv_flg                ,'',null ,ost.cr_actv_flg            ) as cr_actv_flg              
,decode(ost.hl_actv_flg                ,'',null ,ost.hl_actv_flg            ) as hl_actv_flg             
,decode(ost.ma_actv_flg                ,'',null ,ost.ma_actv_flg            ) as ma_actv_flg             
,decode(ost.cr_actv_rptg_flg           ,'',null ,ost.cr_actv_rptg_flg       ) as cr_actv_rptg_flg        
,decode(ost.hl_actv_rptg_flg           ,'',null ,ost.hl_actv_rptg_flg       ) as hl_actv_rptg_flg        
,decode(ost.ma_actv_rptg_flg           ,'',null ,ost.ma_actv_rptg_flg       ) as ma_actv_rptg_flg        
,decode(ost.cr_actv_pd_flg             ,'',null ,ost.cr_actv_pd_flg         ) as cr_actv_pd_flg          
,decode(ost.hl_actv_pd_flg             ,'',null ,ost.hl_actv_pd_flg         ) as hl_actv_pd_flg          
,decode(ost.ma_actv_pd_flg             ,'',null ,ost.ma_actv_pd_flg         ) as ma_actv_pd_flg          
,decode(ost.cr_canc_bad_dbt_flg        ,'',null ,ost.cr_canc_bad_dbt_flg    ) as cr_canc_bad_dbt_flg     
,decode(ost.hl_canc_bad_dbt_flg        ,'',null ,ost.hl_canc_bad_dbt_flg    ) as hl_canc_bad_dbt_flg     
,decode(ost.ma_canc_bad_dbt_flg        ,'',null ,ost.ma_canc_bad_dbt_flg    ) as ma_canc_bad_dbt_flg     
,decode(ost.cr_canc_cust_flg           ,'',null ,ost.cr_canc_cust_flg       ) as cr_canc_cust_flg        
,decode(ost.hl_canc_cust_flg           ,'',null ,ost.hl_canc_cust_flg       ) as hl_canc_cust_flg        
,decode(ost.ma_canc_cust_flg           ,'',null ,ost.ma_canc_cust_flg       ) as ma_canc_cust_flg        
,decode(ost.cr_crd_pend_flg            ,'',null ,ost.cr_crd_pend_flg        ) as cr_crd_pend_flg         
,decode(ost.hl_crd_pend_flg            ,'',null ,ost.hl_crd_pend_flg        ) as hl_crd_pend_flg         
,decode(ost.ma_crd_pend_flg            ,'',null ,ost.ma_crd_pend_flg        ) as ma_crd_pend_flg         
,decode(ost.cr_crd_stat_cd             ,'',null ,ost.cr_crd_stat_cd         ) as cr_crd_stat_cd           
,decode(ost.hl_crd_stat_cd             ,'',null ,ost.hl_crd_stat_cd         ) as hl_crd_stat_cd           
,decode(ost.ma_crd_stat_cd             ,'',null ,ost.ma_crd_stat_cd         ) as ma_crd_stat_cd           
,nullif(greatest(nvl(ost.cr_crd_stat_cd,'A'),
                 nvl(ost.hl_crd_stat_cd,'A'),
                 nvl(ost.ma_crd_stat_cd,'A'),
                 nvl(ost.shm_crd_stat_cd,'A')),'A')                              ofo_crd_stat_cd
,ost.cr_exp_dt             
,ost.hl_exp_dt             
,ost.ma_exp_dt             
,ost.cr_lst_canc_dt        
,ost.hl_lst_canc_dt        
,ost.ma_lst_canc_dt        
,ost.tl_lst_ord_dt         
,ost.cr_lst_pmt_dt         
,ost.hl_lst_pmt_dt         
,ost.ma_lst_pmt_dt         
,decode(ost.cr_flg                     ,'',null ,ost.cr_flg                  ) as cr_flg    
,decode(ost.hl_flg                     ,'',null ,ost.hl_flg                  ) as hl_flg    
,decode(ost.ma_flg                     ,'',null ,ost.ma_flg                  ) as ma_flg    
,decode(ost.cr_exp_flg                 ,'',null ,ost.cr_exp_flg              ) as cr_exp_flg
,decode(ost.hl_exp_flg                 ,'',null ,ost.hl_exp_flg              ) as hl_exp_flg
,decode(ost.ma_exp_flg                 ,'',null ,ost.ma_exp_flg              ) as ma_exp_flg
,decode(ost.cr_curr_mbr_keycode        ,'',null ,ost.cr_curr_mbr_keycode     ) as cr_curr_mbr_keycode     
,decode(ost.hl_curr_mbr_keycode        ,'',null ,ost.hl_curr_mbr_keycode     ) as hl_curr_mbr_keycode     
,decode(ost.ma_curr_mbr_keycode        ,'',null ,ost.ma_curr_mbr_keycode     ) as ma_curr_mbr_keycode     
,decode(ost.cr_curr_ord_keycode        ,'',null ,ost.cr_curr_ord_keycode     ) as cr_curr_ord_keycode     
,decode(ost.hl_curr_ord_keycode        ,'',null ,ost.hl_curr_ord_keycode     ) as hl_curr_ord_keycode     
,decode(ost.ma_curr_ord_keycode        ,'',null ,ost.ma_curr_ord_keycode     ) as ma_curr_ord_keycode     
,decode(ost.cr_fst_mbr_keycode         ,'',null ,ost.cr_fst_mbr_keycode      ) as cr_fst_mbr_keycode      
,decode(ost.hl_fst_mbr_keycode         ,'',null ,ost.hl_fst_mbr_keycode      ) as hl_fst_mbr_keycode      
,decode(ost.ma_fst_mbr_keycode         ,'',null ,ost.ma_fst_mbr_keycode      ) as ma_fst_mbr_keycode      
,decode(ost.cr_lst_ord_keycode         ,'',null ,ost.cr_lst_ord_keycode      ) as cr_lst_ord_keycode      
,decode(ost.hl_lst_ord_keycode         ,'',null ,ost.hl_lst_ord_keycode      ) as hl_lst_ord_keycode      
,decode(ost.ma_lst_ord_keycode         ,'',null ,ost.ma_lst_ord_keycode      ) as ma_lst_ord_keycode      
,decode(ost.cr_lt_sub_flg              ,'',null ,ost.cr_lt_sub_flg           ) as cr_lt_sub_flg           
,decode(ost.hl_lt_sub_flg              ,'',null ,ost.hl_lt_sub_flg           ) as hl_lt_sub_flg           
,decode(ost.ma_lt_sub_flg              ,'',null ,ost.ma_lt_sub_flg           ) as ma_lt_sub_flg           
,decode(ost.cr_non_sub_dnr_flg         ,'',null ,ost.cr_non_sub_dnr_flg      ) as cr_non_sub_dnr_flg       
,decode(ost.hl_non_sub_dnr_flg         ,'',null ,ost.hl_non_sub_dnr_flg      ) as hl_non_sub_dnr_flg       
,decode(ost.ma_non_sub_dnr_flg         ,'',null ,ost.ma_non_sub_dnr_flg      ) as ma_non_sub_dnr_flg       
,nvl(ost.cro_brks_cnt,0) + nvl(ost.de_cro_brks_cnt,0)                             cr_brks_cnt      
,nvl(ost.hl_brks_cnt,0) + nvl(ost.de_hl_brks_cnt,0)                               hl_brks_cnt      
,nvl(ost.ma_brks_cnt, 0)                                                          ma_brks_cnt              
,ost.cr_rnw_cnt               
,ost.hl_rnw_cnt               
,ost.ma_rnw_cnt               
,decode(ost.cr_rec_flg                 ,'',null ,ost.cr_rec_flg              ) as cr_rec_flg               
,decode(ost.hl_rec_flg                 ,'',null ,ost.hl_rec_flg              ) as hl_rec_flg               
,decode(ost.ma_rec_flg                 ,'',null ,ost.ma_rec_flg              ) as ma_rec_flg                
,decode(ost.cr_svc_stat_cd             ,'',null ,ost.cr_svc_stat_cd          ) as cr_svc_stat_cd            
,decode(ost.hl_svc_stat_cd             ,'',null ,ost.hl_svc_stat_cd          ) as hl_svc_stat_cd            
,decode(ost.ma_svc_stat_cd             ,'',null ,ost.ma_svc_stat_cd          ) as ma_svc_stat_cd            
,decode(ost.cr_curr_mbr_src_cd         ,'',null ,ost.cr_curr_mbr_src_cd      ) as cr_curr_mbr_src_cd        
,decode(ost.hl_curr_mbr_src_cd         ,'',null ,ost.hl_curr_mbr_src_cd      ) as hl_curr_mbr_src_cd        
,decode(ost.ma_curr_mbr_src_cd         ,'',null ,ost.ma_curr_mbr_src_cd      ) as ma_curr_mbr_src_cd        
,decode(ost.cr_curr_ord_src_cd         ,'',null ,ost.cr_curr_ord_src_cd      ) as cr_curr_ord_src_cd        
,decode(ost.hl_curr_ord_src_cd         ,'',null ,ost.hl_curr_ord_src_cd      ) as hl_curr_ord_src_cd        
,decode(ost.ma_curr_ord_src_cd         ,'',null ,ost.ma_curr_ord_src_cd      ) as ma_curr_ord_src_cd        
,decode(ost.cr_fst_ord_src_cd          ,'',null ,ost.cr_fst_ord_src_cd       ) as cr_fst_ord_src_cd         
,decode(ost.hl_fst_ord_src_cd          ,'',null ,ost.hl_fst_ord_src_cd       ) as hl_fst_ord_src_cd         
,decode(ost.ma_fst_ord_src_cd          ,'',null ,ost.ma_fst_ord_src_cd       ) as ma_fst_ord_src_cd         
,decode(ost.cr_lst_ord_src_cd          ,'',null ,ost.cr_lst_ord_src_cd       ) as cr_lst_ord_src_cd         
,decode(ost.hl_lst_ord_src_cd          ,'',null ,ost.hl_lst_ord_src_cd       ) as hl_lst_ord_src_cd         
,decode(ost.ma_lst_ord_src_cd          ,'',null ,ost.ma_lst_ord_src_cd       ) as ma_lst_ord_src_cd         
,decode(ost.cr_prior_mbr_src_cd        ,'',null ,ost.cr_prior_mbr_src_cd     ) as cr_prior_mbr_src_cd       
,decode(ost.hl_prior_mbr_src_cd        ,'',null ,ost.hl_prior_mbr_src_cd     ) as hl_prior_mbr_src_cd       
,decode(ost.ma_prior_mbr_src_cd        ,'',null ,ost.ma_prior_mbr_src_cd     ) as ma_prior_mbr_src_cd       
,decode(ost.cr_sub_dnr_flg             ,'',null ,ost.cr_sub_dnr_flg          ) as cr_sub_dnr_flg            
,decode(ost.hl_sub_dnr_flg             ,'',null ,ost.hl_sub_dnr_flg          ) as hl_sub_dnr_flg            
,decode(ost.ma_sub_dnr_flg             ,'',null ,ost.ma_sub_dnr_flg          ) as ma_sub_dnr_flg            
,decode(ost.cr_curr_ord_term           ,'',null ,ost.cr_curr_ord_term        ) as cr_curr_ord_term          
,decode(ost.hl_curr_ord_term           ,'',null ,ost.hl_curr_ord_term        ) as hl_curr_ord_term          
,decode(ost.ma_curr_ord_term           ,'',null ,ost.ma_curr_ord_term        ) as ma_curr_ord_term          
,ost.cr_fst_dnr_dt            
,ost.hl_fst_dnr_dt            
,ost.ma_fst_dnr_dt            
,decode(ost.eds_lst_src_cd             ,'',null ,ost.eds_lst_src_cd          ) as eds_lst_src_cd            
,decode(ost.morbank_mtch_cd            ,'',null ,ost.morbank_mtch_cd         ) as morbank_mtch_cd           
,decode(ost.cr_auto_rnw_flg            ,'',null ,ost.cr_auto_rnw_flg         ) as cr_auto_rnw_flg           
,decode(ost.hl_auto_rnw_flg            ,'',null ,ost.hl_auto_rnw_flg         ) as hl_auto_rnw_flg           
,decode(ost.ma_auto_rnw_flg            ,'',null ,ost.ma_auto_rnw_flg         ) as ma_auto_rnw_flg           
,decode(ost.cr_non_dnr_flg             ,'',null ,ost.cr_non_dnr_flg          ) as cr_non_dnr_flg            
,decode(ost.hl_non_dnr_flg             ,'',null ,ost.hl_non_dnr_flg          ) as hl_non_dnr_flg            
,decode(ost.ma_non_dnr_flg             ,'',null ,ost.ma_non_dnr_flg          ) as ma_non_dnr_flg            
,ost.cr_curr_mbr_dt           
,ost.hl_curr_mbr_dt           
,ost.ma_curr_mbr_dt           
,ost.cr_ltd_pd_amt           
,ost.hl_ltd_pd_amt           
,ost.ma_ltd_pd_amt           
,ost.cr_curr_ord_dt           
,ost.hl_curr_ord_dt           
,ost.ma_curr_ord_dt           
,ost.cr_fst_ord_dt            
,ost.hl_fst_ord_dt            
,ost.ma_fst_ord_dt            
,ost.cr_lst_canc_bad_dbt_dt 
,ost.hl_lst_canc_bad_dbt_dt 
,ost.ma_lst_canc_bad_dbt_dt 
,ost.cr_lst_dnr_ord_dt       
,ost.hl_lst_dnr_ord_dt       
,ost.ma_lst_dnr_ord_dt       
,ost.cr_lst_ord_dt           
,ost.hl_lst_ord_dt           
,ost.ma_lst_ord_dt           
,ost.cr_canc_bad_dbt_cnt  
,ost.hl_canc_bad_dbt_cnt  
,ost.ma_canc_bad_dbt_cnt  
,ost.cr_canc_cust_cnt     
,ost.hl_canc_cust_cnt     
,ost.ma_canc_cust_cnt     
,ost.cr_dm_ord_cnt        
,ost.hl_dm_ord_cnt        
,ost.ma_dm_ord_cnt        
,ost.cr_em_ord_cnt        
,ost.hl_em_ord_cnt        
,ost.ma_em_ord_cnt        
,ost.cr_dnr_ord_cnt       
,ost.hl_dnr_ord_cnt       
,ost.ma_dnr_ord_cnt       
,ost.cr_ord_cnt           
,ost.hl_ord_cnt           
,ost.ma_ord_cnt           
,ost.cr_wr_off_cnt        
,ost.hl_wr_off_cnt        
,ost.ma_wr_off_cnt        
,decode(ost.cr_lst_sub_ord_role_cd     ,'',null ,ost.cr_lst_sub_ord_role_cd  ) as cr_lst_sub_ord_role_cd  
,decode(ost.hl_lst_sub_ord_role_cd     ,'',null ,ost.hl_lst_sub_ord_role_cd  ) as hl_lst_sub_ord_role_cd  
,decode(ost.ma_lst_sub_ord_role_cd     ,'',null ,ost.ma_lst_sub_ord_role_cd  ) as ma_lst_sub_ord_role_cd  
,decode(ost.shm_actv_flg               ,'',null ,ost.shm_actv_flg            ) as shm_actv_flg              
,decode(ost.shm_actv_rptg_flg          ,'',null ,ost.shm_actv_rptg_flg       ) as shm_actv_rptg_flg         
,decode(ost.shm_actv_pd_flg            ,'',null ,ost.shm_actv_pd_flg         ) as shm_actv_pd_flg           
,decode(ost.shm_canc_bad_dbt_flg       ,'',null ,ost.shm_canc_bad_dbt_flg    ) as shm_canc_bad_dbt_flg      
,decode(ost.shm_canc_cust_flg          ,'',null ,ost.shm_canc_cust_flg       ) as shm_canc_cust_flg         
,decode(ost.shm_crd_pend_flg           ,'',null ,ost.shm_crd_pend_flg        ) as shm_crd_pend_flg          
,decode(ost.shm_crd_stat_cd            ,'',null ,ost.shm_crd_stat_cd         ) as shm_crd_stat_cd           
,ost.shm_exp_dt             
,ost.shm_lst_canc_dt        
,ost.shm_lst_pmt_dt         
,decode(ost.shm_flg                    ,'',null ,ost.shm_flg                 ) as shm_flg                   
,decode(ost.shm_exp_flg                ,'',null ,ost.shm_exp_flg             ) as shm_exp_flg               
,decode(ost.shm_curr_mbr_keycode       ,'',null ,ost.shm_curr_mbr_keycode    ) as shm_curr_mbr_keycode      
,decode(ost.shm_curr_ord_keycode       ,'',null ,ost.shm_curr_ord_keycode    ) as shm_curr_ord_keycode      
,decode(ost.shm_fst_mbr_keycode        ,'',null ,ost.shm_fst_mbr_keycode     ) as shm_fst_mbr_keycode       
,decode(ost.shm_lst_ord_keycode        ,'',null ,ost.shm_lst_ord_keycode     ) as shm_lst_ord_keycode       
,decode(ost.shm_lt_sub_flg             ,'',null ,ost.shm_lt_sub_flg          ) as shm_lt_sub_flg            
,decode(ost.shm_non_sub_dnr_flg        ,'',null ,ost.shm_non_sub_dnr_flg     ) as shm_non_sub_dnr_flg       
,nvl(ost.shm_brks_cnt, 0)                                                         shm_brks_cnt              
,ost.shm_rnw_cnt       
,decode(ost.shm_rec_flg                ,'',null ,ost.shm_rec_flg             ) as shm_rec_flg               
,decode(ost.shm_svc_stat_cd            ,'',null ,ost.shm_svc_stat_cd         ) as shm_svc_stat_cd           
,decode(ost.shm_curr_mbr_src_cd        ,'',null ,ost.shm_curr_mbr_src_cd     ) as shm_curr_mbr_src_cd       
,decode(ost.shm_curr_ord_src_cd        ,'',null ,ost.shm_curr_ord_src_cd     ) as shm_curr_ord_src_cd       
,decode(ost.shm_fst_ord_src_cd         ,'',null ,ost.shm_fst_ord_src_cd      ) as shm_fst_ord_src_cd        
,decode(ost.shm_lst_ord_src_cd         ,'',null ,ost.shm_lst_ord_src_cd      ) as shm_lst_ord_src_cd        
,decode(ost.shm_prior_mbr_src_cd       ,'',null ,ost.shm_prior_mbr_src_cd    ) as shm_prior_mbr_src_cd      
,decode(ost.shm_sub_dnr_flg            ,'',null ,ost.shm_sub_dnr_flg         ) as shm_sub_dnr_flg           
,decode(ost.shm_curr_ord_term          ,'',null ,ost.shm_curr_ord_term       ) as shm_curr_ord_term         
,ost.shm_fst_dnr_dt   
,decode(ost.shm_auto_rnw_flg           ,'',null ,ost.shm_auto_rnw_flg        ) as shm_auto_rnw_flg          
,decode(ost.shm_non_dnr_flg            ,'',null ,ost.shm_non_dnr_flg         ) as shm_non_dnr_flg           
,ost.shm_curr_mbr_dt 
,ost.shm_ltd_pd_amt        
,ost.shm_curr_ord_dt         
,ost.shm_fst_ord_dt          
,ost.shm_lst_canc_bad_dbt_dt 
,ost.shm_lst_dnr_ord_dt      
,ost.shm_lst_ord_dt          
,ost.shm_canc_bad_dbt_cnt       
,ost.shm_canc_cust_cnt          
,ost.shm_dm_ord_cnt             
,ost.shm_em_ord_cnt             
,ost.shm_dnr_ord_cnt            
,ost.shm_ord_cnt                
,ost.shm_wr_off_cnt             
,decode(ost.shm_lst_sub_ord_role_cd    ,'',null ,ost.shm_lst_sub_ord_role_cd ) as shm_lst_sub_ord_role_cd   
,ix.household_id as osl_hh_id
FROM prod.individual ix 
inner join agg.mt_ofl_sum_temp ost on ix.individual_id = ost.individual_id

;