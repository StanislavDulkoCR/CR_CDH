/***************************************************************************
*            (C) Copyright Acxiom Corporation 2006
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project:      Consumers Union Acxiom Database Solution - Middle Tier ETL
* Module Name:  middle_tier2.mt_offline_summary.sql
* Date:         4/13/2006
***************************************************************************/
CREATE TABLE mt_ofl_brks_temp
as 
select
  individual_id,
  min(cns_orig_strt_iss_num) cns_orig_strt_iss_num,
  min(crh_orig_strt_iss_num) crh_orig_strt_iss_num,
  min(crm_orig_strt_iss_num) crm_orig_strt_iss_num,
  min(shm_orig_strt_iss_num) shm_orig_strt_iss_num,
  max(case when cro_brk_dt_rnk = 1
           then nvl2(cro_brk_dt, cro_brk_dt||'|'||orig_strt_iss_num, null)
           else null
      end) last_cro_brk_dt,
  max(case when cro_brk_dt_rnk = 2
           then nvl2(cro_brk_dt, cro_brk_dt||'|'||orig_strt_iss_num, null)
           else null
      end) prior_cro_brk_dt,
  count(cro_brk_dt) cro_brks_cnt,
  max(case when hl_brk_dt_rnk = 1
           then nvl2(hl_brk_dt, hl_brk_dt||'|'||orig_strt_iss_num, null)
           else null
      end) last_hl_brk_dt,
  max(case when hl_brk_dt_rnk = 2
           then nvl2(hl_brk_dt, hl_brk_dt||'|'||orig_strt_iss_num, null)
           else null
      end) prior_hl_brk_dt,
  count(hl_brk_dt) hl_brks_cnt,
  max(case when ma_brk_dt_rnk = 1
           then nvl2(ma_brk_dt, ma_brk_dt||'|'||orig_strt_iss_num, null)
           else null
      end) last_ma_brk_dt,
  max(case when ma_brk_dt_rnk = 2
           then nvl2(ma_brk_dt, ma_brk_dt||'|'||orig_strt_iss_num, null)
           else null
      end) prior_ma_brk_dt,
  count(ma_brk_dt) ma_brks_cnt,
  max(case when shm_brk_dt_rnk = 1
           then nvl2(shm_brk_dt, shm_brk_dt||'|'||orig_strt_iss_num, null)
           else null
      end) last_shm_brk_dt,
  max(case when shm_brk_dt_rnk = 2
           then nvl2(shm_brk_dt, shm_brk_dt||'|'||orig_strt_iss_num, null)
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
        er.individual_id,
        case when oa.mag_cd = 'CNS'
             then case when months_between(strt_lkp.pub_dt,
                                           nvl(max(end_lkp.pub_dt) over (partition by
                                                              er.individual_id,
                                                              oa.mag_cd
                                                            order by strt_lkp.pub_dt
                                                            rows between unbounded preceding and 1 preceding)
                                               ,strt_lkp.pub_dt)) > 6
                       then max(end_lkp.pub_dt) over (partition by
                                                              er.individual_id,
                                                              oa.mag_cd
                                                            order by strt_lkp.pub_dt
                                                            rows between unbounded preceding and 1 preceding)
                       else null
                  end
             else null
        end cro_brk_dt,
        case when oa.mag_cd = 'CNS'
             then min(orig_strt_iss_num) over (partition by  er.individual_id, oa.mag_cd) 
    end cns_orig_strt_iss_num,
    case when oa.mag_cd = 'CRH'
             then case when months_between(strt_lkp.pub_dt,
                                           nvl(max(end_lkp.pub_dt) over (partition by
                                                              er.individual_id,
                                                              oa.mag_cd
                                                            order by strt_lkp.pub_dt
                                                            rows between unbounded preceding and 1 preceding)
                                               ,strt_lkp.pub_dt)) > 6
                       then max(end_lkp.pub_dt) over (partition by
                                                              er.individual_id,
                                                              oa.mag_cd
                                                            order by strt_lkp.pub_dt
                                                            rows between unbounded preceding and 1 preceding)
                       else null
                  end
             else null
        end hl_brk_dt,
        case when oa.mag_cd = 'CRH'
             then min(orig_strt_iss_num) over (partition by  er.individual_id, oa.mag_cd) 
    end crh_orig_strt_iss_num,
    case when oa.mag_cd = 'CRM'
             then case when months_between(strt_lkp.pub_dt,
                                           nvl(max(end_lkp.pub_dt) over (partition by
                                                              er.individual_id,
                                                              oa.mag_cd
                                                            order by strt_lkp.pub_dt
                                                            rows between unbounded preceding and 1 preceding)
                                               ,strt_lkp.pub_dt)) > 6
                       then max(end_lkp.pub_dt) over (partition by
                                                              er.individual_id,
                                                              oa.mag_cd
                                                            order by strt_lkp.pub_dt
                                                            rows between unbounded preceding and 1 preceding)
                       else null
                  end
             else null
        end ma_brk_dt,
        case when oa.mag_cd = 'CRM'
             then min(orig_strt_iss_num) over (partition by  er.individual_id, oa.mag_cd) 
    end crm_orig_strt_iss_num,
  case when oa.mag_cd = 'SHM'
             then case when months_between(strt_lkp.pub_dt,
                                           nvl(max(end_lkp.pub_dt) over (partition by
                                                              er.individual_id,
                                                              oa.mag_cd
                                                            order by strt_lkp.pub_dt
                                                            rows between unbounded preceding and 1 preceding)
                                               ,strt_lkp.pub_dt)) > 6
                       then max(end_lkp.pub_dt) over (partition by
                                                              er.individual_id,
                                                              oa.mag_cd
                                                            order by strt_lkp.pub_dt
                                                            rows between unbounded preceding and 1 preceding)
                       else null
                  end
             else null
        end shm_brk_dt,
  case when oa.mag_cd = 'SHM'
             then min(orig_strt_iss_num) over (partition by  er.individual_id, oa.mag_cd) 
    end shm_orig_strt_iss_num,
    oo.orig_strt_iss_num         
from middle_tier2.mt_offline_ord oo, 
     warehouse2.offline_account oa,
     warehouse2.external_ref er, 
     warehouse2.issue_lkp strt_lkp,  
     warehouse2.issue_lkp end_lkp  
where oa.acct_id = oo.acct_id
  and to_char(er.internal_key) = oa.acct_id
  and end_lkp.mag_cd = oa.mag_cd
  and end_lkp.iss_num = (oo.orig_strt_iss_num + oo.term_mth_cnt)
  and strt_lkp.mag_cd = oa.mag_cd
  and strt_lkp.iss_num = oo.orig_strt_iss_num
  and ( set_cd not in('B','D') or stat_cd not in ('D') )
  and oa.mag_cd in ('CNS', 'CRM', 'CRH', 'SHM'))
       t2)
group by individual_id;

CREATE TABLE mt_ofl_combine_temp_temp
AS
SELECT /*+ no_parallel(er) no_parallel(oa) no_parallel(iss) */ individual_id,
        CASE WHEN oa.mag_cd = 'CNS'
                        THEN nvl(to_char(iss.pub_dt,'YYYYMMDD'),'00000000') || oa.acct_id
                        ELSE NULL
                   END cr_comb_acct_id,
        CASE WHEN oa.mag_cd = 'CRH'
                        THEN nvl(to_char(iss.pub_dt,'YYYYMMDD'),'00000000') || oa.acct_id
                        ELSE NULL
                   END hl_comb_acct_id,
        CASE WHEN oa.mag_cd = 'CRM'
                        THEN nvl(to_char(iss.pub_dt,'YYYYMMDD'),'00000000') || oa.acct_id
                        ELSE NULL
                   END ma_comb_acct_id,
        CASE WHEN oa.mag_cd = 'SHM'
                        THEN nvl(to_char(iss.pub_dt,'YYYYMMDD'),'00000000') || oa.acct_id
                        ELSE NULL
                   END shm_comb_acct_id
FROM warehouse2.external_ref er,
     warehouse2.offline_account oa,
     warehouse2.issue_lkp iss
WHERE er.internal_key = oa.acct_id
  AND oa.mag_cd = iss.mag_cd (+)
  AND oa.expr_iss_num = iss.iss_num (+)
  AND oa.mag_cd in ('CNS','CRH','CRM','SHM');		

CREATE TABLE mt_ofl_combine_temp
AS
SELECT /*+ no_parallel(ct) */ individual_id,
       substr(max(cr_comb_acct_id),9) cr_comb_acct_id,
       substr(max(hl_comb_acct_id),9) hl_comb_acct_id,
       substr(max(ma_comb_acct_id),9) ma_comb_acct_id,
       substr(max(shm_comb_acct_id),9) shm_comb_acct_id
FROM mt_ofl_combine_temp_temp ct
GROUP BY individual_id;

CREATE TABLE mt_ofl_de_temp
AS
SELECT /*+ no_parallel(ert) no_parallel(det)*/
        ert.individual_id,
        det.mag_cd,
        max(det.exp_dt) exp_dt,
        count(*) de_cnt,
        substr(max(CASE WHEN det.mag_cd = 'CNS'
                        THEN nvl(to_char(det.exp_dt,'YYYYMMDD'),'00010101') ||
                             det.lst_src_cd
                        ELSE null
                   END),9) eds_lst_src_cd,
        substr(max(CASE WHEN det.mag_cd = 'CNS'
                        THEN nvl(to_char(det.exp_dt,'YYYYMMDD'),'00010101') ||
                             det.morbank_mtch_cd
                        ELSE null
                   END),9) morbank_mtch_cd
      FROM warehouse2.external_ref ert,
           warehouse2.dead_expires det
      WHERE ert.individual_id = det.individual_id
      GROUP BY ert.individual_id,det.mag_cd;

CREATE TABLE mt_ofl_sum_tmp1
AS
SELECT DE.INDIVIDUAL_ID, 
       DE.MAG_CD,
       CASE WHEN DE.MAG_CD = 'CNS'
            THEN DECODE(max(cds.INDIVIDUAL_ID),NULL,COUNT(*)-1,COUNT(*)) 
            ELSE NULL
       END DE_CR_BRKS_CNT,
       CASE WHEN DE.MAG_CD = 'CRH'
            THEN DECODE(max(cds.INDIVIDUAL_ID),NULL,COUNT(*)-1,COUNT(*)) 
            ELSE NULL
       END DE_HL_BRKS_CNT
FROM warehouse2.DEAD_EXPIRES DE left join 
     (SELECT DISTINCT INDIVIDUAL_ID, MAG_CD
       FROM middle_tier2.mt_OFFLINE_ORD
      WHERE MAG_CD in ('CNS','CRH')
        and (KEYCODE not in ('VFREE','ZFREE')
          or KEYCODE is null)) CDS on DE.INDIVIDUAL_ID = CDS.INDIVIDUAL_ID and DE.MAG_CD = CDS.MAG_CD
where 1=1
    and DE.MAG_CD in ('CNS', 'CRH')
GROUP BY DE.INDIVIDUAL_ID, DE.MAG_CD
;

CREATE TABLE mt_ofl_sum_tmp2
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
              AND ((substr(ofo.keycode,1,1) in ('A','G','X'))
                   OR (substr(ofo.keycode,1,1) = 'D' 
                     AND nvl(substr(ofo.keycode,2,1),'X') != 'N')
                   OR (substr(ofo.keycode,1,1) in ('B') 
                     AND nvl(substr(ofo.keycode,9,1),'X') not in ('A','B','C','S'))
                   OR (substr(ofo.keycode,1,1) in ('U') 
                     AND regexp_instr(nvl(substr(ofo.keycode,9,1),'1'),'[^[:alpha:]]') = 1)
                   OR (substr(ofo.keycode,1,1) = 'R' 
                     AND nvl(substr(ofo.keycode,9,1),'X') not in ('B','C','D','E','F','T'))
                   OR (substr(ofo.keycode,1,1) in ('U') 
                     AND length(ofo.keycode) = 7
                     AND regexp_instr(substr(ofo.keycode,6,1),'[^[:alpha:]]') = 1))
             THEN 1
        END) cr_dm_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CRH'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND ((substr(ofo.keycode,1,1) in ('A','G','X'))
                   OR (substr(ofo.keycode,1,1) = 'D' 
                     AND nvl(substr(ofo.keycode,2,1),'X') != 'N')
                   OR (substr(ofo.keycode,1,1) = 'B' 
                     AND nvl(substr(ofo.keycode,9,1),'X') not in ('A','B','C','S'))
                   OR (substr(ofo.keycode,1,1) = 'U' 
                     AND regexp_instr(nvl(substr(ofo.keycode,9,1),'1'),'[^[:alpha:]]') = 1)
                   OR (substr(ofo.keycode,1,1) = 'R' 
                     AND nvl(substr(ofo.keycode,9,1),'X') not in ('B','C','D','E','F','T'))
                   OR (substr(ofo.keycode,1,1) in ('U') 
                     AND length(ofo.keycode) = 7
                     AND regexp_instr(substr(ofo.keycode,6,1),'[^[:alpha:]]') = 1))
             THEN 1
        END) hl_dm_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CRM'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND ((substr(ofo.keycode,1,1) in ('A','G','X'))
                   OR (substr(ofo.keycode,1,1) = 'D' AND nvl(substr(ofo.keycode,2,1),'X') != 'N')
                   OR (substr(ofo.keycode,1,1) = 'B' AND nvl(substr(ofo.keycode,9,1),'X') not in ('A','B','C','S'))
                   OR (substr(ofo.keycode,1,1) = 'U' 
                            AND regexp_instr(nvl(substr(ofo.keycode,9,1),'1'),'[^[:alpha:]]') = 1)
                   OR (substr(ofo.keycode,1,1) = 'R' AND nvl(substr(ofo.keycode,9,1),'X') not in ('B','C','D','E','F','T')) 
                   OR (substr(ofo.keycode,1,1) = 'U' 
                         AND length(ofo.keycode) = 7
                         AND regexp_instr(substr(ofo.keycode,6,1),'[^[:alpha:]]') = 1 ))
             THEN 1
        END) ma_dm_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'SHM'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND ((substr(ofo.keycode,1,1) in ('A','G','X'))
                   OR (substr(ofo.keycode,1,1) = 'D' 
                     AND nvl(substr(ofo.keycode,2,1),'X') != 'N')
                   OR (substr(ofo.keycode,1,1) = 'B' 
                     AND nvl(substr(ofo.keycode,9,1),'X') not in ('A','B','C','S'))
                   OR (substr(ofo.keycode,1,1) = 'U' 
                         AND regexp_instr(nvl(substr(ofo.keycode,9,1),'1'),'[^[:alpha:]]') = 1)
                   OR (substr(ofo.keycode,1,1) = 'R' 
                     AND nvl(substr(ofo.keycode,9,1),'X') not in ('B','C','D','E','F','T'))
                   OR (substr(ofo.keycode,1,1) = 'U' 
                     AND length(ofo.keycode) = 7
                     AND regexp_instr(substr(ofo.keycode,6,1),'[^[:alpha:]]') = 1))
             THEN 1
        END) shm_dm_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CNS'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND (   (substr(ofo.keycode,1,1) = 'E')
                   OR (substr(ofo.keycode,1,1) = 'D' AND substr(ofo.keycode,2,1) = 'N')
                   OR (substr(ofo.keycode,1,1) = 'B' AND substr(ofo.keycode,9,1) in ('A','B','C','S'))
                   OR (substr(ofo.keycode,1,1) = 'U' AND regexp_instr(substr(ofo.keycode,9,1),'[[:alpha:]]') = 1)
                   OR (substr(ofo.keycode,1,1) = 'R' AND substr(ofo.keycode,9,1) in ('B','C','D','E','F'))
                   OR (substr(ofo.keycode,4,6) = 'FAILCC')     
                   OR (substr(ofo.keycode,1,1) = 'U' 
                         AND length(ofo.keycode) = 7
                         AND substr(ofo.keycode,6,1) in ('A','B','C','D','E','F'))
                   )
             THEN 1
        END) cr_em_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CRH'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND (   (substr(ofo.keycode,1,1) = 'E')
                   OR (substr(ofo.keycode,1,1) = 'D' AND substr(ofo.keycode,2,1) = 'N')
                   OR (substr(ofo.keycode,1,1) = 'B' AND substr(ofo.keycode,9,1) in ('A','B','C','S'))
                   OR (substr(ofo.keycode,1,1) = 'U' AND regexp_instr(substr(ofo.keycode,9,1),'[[:alpha:]]') = 1)
                   OR (substr(ofo.keycode,1,1) = 'R' AND substr(ofo.keycode,9,1) in ('B','C','D','E','F'))
                   OR (substr(ofo.keycode,4,6) = 'FAILCC')     
                   OR (substr(ofo.keycode,1,1) = 'U' 
                         AND length(ofo.keycode) = 7
                         AND substr(ofo.keycode,6,1) in ('A','B','C','D','E','F'))
                  )
             THEN 1
        END) hl_em_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'CRM'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND (   (substr(ofo.keycode,1,1) = 'E')
                   OR (substr(ofo.keycode,1,1) = 'D' AND substr(ofo.keycode,2,1) = 'N')
                   OR (substr(ofo.keycode,1,1) = 'B' AND substr(ofo.keycode,9,1) in ('A','B','C','S'))
                   OR (substr(ofo.keycode,1,1) = 'U' AND regexp_instr(substr(ofo.keycode,9,1),'[[:alpha:]]') = 1)
                   OR (substr(ofo.keycode,1,1) = 'R' AND substr(ofo.keycode,9,1) in ('B','C','D','E','F'))
                   OR (substr(ofo.keycode,4,6) = 'FAILCC')     
                   OR (substr(ofo.keycode,1,1) = 'U' 
                         AND length(ofo.keycode) = 7
                         AND substr(ofo.keycode,6,1) in ('A','B','C','D','E','F'))
                  )
             THEN 1
        END) ma_em_ord_cnt,
  count(CASE WHEN ofo.mag_cd = 'SHM'
              AND nvl(ofo.set_cd,'X') not in ('C','E')  --ddbarr
              AND (   (substr(ofo.keycode,1,1) = 'E')
                   OR (substr(ofo.keycode,1,1) = 'D' AND substr(ofo.keycode,2,1) = 'N')
                   OR (substr(ofo.keycode,1,1) = 'B' AND substr(ofo.keycode,9,1) in ('A','B','C','S'))
                   OR (substr(ofo.keycode,1,1) = 'U' AND regexp_instr(substr(ofo.keycode,9,1),'[[:alpha:]]') = 1)
                   OR (substr(ofo.keycode,1,1) = 'R' AND substr(ofo.keycode,9,1) in ('B','C','D','E','F'))
                   OR (substr(ofo.keycode,4,6) = 'FAILCC')     
                   OR (substr(ofo.keycode,1,1) = 'U' 
                         AND length(ofo.keycode) = 7
                         AND substr(ofo.keycode,6,1) in ('A','B','C','D','E','F'))
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
FROM middle_tier2.mt_offline_ord ofo
WHERE mag_cd in ('CNS','CRH','CRM','SHM')
  AND (KEYCODE not in ('VFREE','ZFREE') or KEYCODE is null)
GROUP BY individual_id
;

CREATE TABLE mt_offline_ord_tmp3
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
           ,acct_id
           ,stat_cd
           ,src_cd
           ,pmt_dt
           ,cr_stat_cd
           ,entr_typ_cd
      from middle_tier2.mt_offline_ord where (keycode not in ('VFREE','ZFREE') or KEYCODE is null);

CREATE TABLE offline_account_tmp4
AS
select  
            svc_stat_cd
           ,acct_id
           ,mag_cd
           ,keycd
           ,rnw_cnt
           ,expr_iss_num 
      from warehouse2.offline_account
      where nvl(mag_cd,'XXX') IN ('CNS', 'CRH', 'CRM', 'SHM', 'CRT','XXX');

CREATE TABLE mt_ofl_sum_temp
AS
SELECT 
  er.individual_id,
  MAX(CASE WHEN oa.mag_cd = 'CNS'
            AND oa.svc_stat_cd = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (SUBSTR(ofo.keycode,1,4) = 'LIFE'))
            AND oa.acct_id = ct.cr_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) cr_actv_flg,
  MAX(CASE WHEN oa.mag_cd = 'CRH'
            AND oa.svc_stat_cd = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (SUBSTR(ofo.keycode,1,4) = 'LIFE'))
            AND oa.acct_id = ct.hl_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) hl_actv_flg,  
  MAX(CASE WHEN oa.mag_cd = 'CRM'
            AND oa.svc_stat_cd = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (SUBSTR(ofo.keycode,1,4) = 'LIFE'))
            AND oa.acct_id = ct.ma_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) ma_actv_flg,
  MAX(CASE WHEN oa.mag_cd = 'CNS'
            AND oa.svc_stat_cd in ('A','E')
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (SUBSTR(ofo.keycode,1,4) = 'LIFE'))
            AND oa.acct_id = ct.cr_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) cr_actv_rptg_flg,
  MAX(CASE WHEN oa.mag_cd = 'CRH'
            AND oa.svc_stat_cd in ('A','E')
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (SUBSTR(ofo.keycode,1,4) = 'LIFE'))
            AND oa.acct_id = ct.hl_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) hl_actv_rptg_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRM'
            AND oa.svc_stat_cd in ('A','E')
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (SUBSTR(ofo.keycode,1,4) = 'LIFE'))
            AND oa.acct_id = ct.ma_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) ma_actv_rptg_flg,
  MAX(CASE WHEN oa.mag_cd = 'CNS'
            AND oa.svc_stat_cd = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND ofo.set_cd in ('A','B')
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (SUBSTR(ofo.keycode,1,4) = 'LIFE'))
            AND ofo.cr_stat_cd in ('C', 'D', 'E', 'F', 'G', 'I')
            AND oa.acct_id = ct.cr_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) cr_actv_pd_flg,
  MAX(CASE WHEN oa.mag_cd = 'CRH'
            AND oa.svc_stat_cd = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND ofo.set_cd in ('A','B')
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (SUBSTR(ofo.keycode,1,4) = 'LIFE'))
            AND ofo.cr_stat_cd in ('C', 'D', 'E', 'F', 'G', 'I')
            AND oa.acct_id = ct.hl_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) hl_actv_pd_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRM'
            AND oa.svc_stat_cd = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND ofo.set_cd in ('A','B')
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (SUBSTR(ofo.keycode,1,4) = 'LIFE'))
            AND ofo.cr_stat_cd in ('C', 'D', 'E', 'F', 'G', 'I')
            AND oa.acct_id = ct.ma_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) ma_actv_pd_flg, 
  CASE WHEN SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CNS' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.canc_rsn_cd 
                            ELSE NULL 
                       END),9) IN ('06','50')
       THEN 'Y' 
       ELSE 'N' 
  END cr_canc_bad_dbt_flg, 
  CASE WHEN SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRH' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.canc_rsn_cd 
                            ELSE NULL 
                       END),9) IN ('06','50') 
       THEN 'Y' 
       ELSE 'N' 
  END hl_canc_bad_dbt_flg, 
  CASE WHEN SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRM' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.canc_rsn_cd 
                            ELSE NULL 
                       END),9) IN ('06','50') 
       THEN 'Y' 
       ELSE 'N' 
  END ma_canc_bad_dbt_flg, 
  CASE WHEN SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CNS' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.canc_rsn_cd 
                            ELSE NULL 
                       END),9) NOT IN ('06','14','50') 
       THEN 'Y' 
       ELSE 'N' 
  END cr_canc_cust_flg, 
  CASE WHEN SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRH' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.canc_rsn_cd 
                            ELSE NULL 
                       END),9) NOT IN ('06','14','50') 
       THEN 'Y' 
       ELSE 'N' 
  END hl_canc_cust_flg, 
  CASE WHEN SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRM' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.canc_rsn_cd 
                            ELSE NULL 
                       END),9) NOT IN ('06','14','50') 
       THEN 'Y' 
       ELSE 'N' 
  END ma_canc_cust_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CNS'
            AND oa.svc_stat_cd = 'E'
            AND oa.acct_id = ct.cr_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) cr_crd_pend_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRH'
            AND oa.svc_stat_cd = 'E'
            AND oa.acct_id = ct.hl_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) hl_crd_pend_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRM'
            AND oa.svc_stat_cd = 'E'
            AND oa.acct_id = ct.ma_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) ma_crd_pend_flg,
  DECODE(SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CNS' 
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
  DECODE(SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRH' 
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
  DECODE(SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRM' 
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
  MAX(CASE WHEN oa.mag_cd = 'CNS' 
           THEN iss.pub_dt 
           WHEN de.mag_cd = 'CNS' 
           THEN de.exp_dt 
           ELSE NULL 
      END) cr_exp_dt, 
  MAX(CASE WHEN oa.mag_cd = 'CRH' 
           THEN iss.pub_dt 
           WHEN de.mag_cd = 'CRH' 
           THEN de.exp_dt 
           ELSE NULL 
      END) hl_exp_dt, 
  MAX(CASE WHEN oa.mag_cd = 'CRM' 
           THEN iss.pub_dt 
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
  MAX(CASE WHEN oa.mag_cd = 'CRT' 
           THEN ofo.ord_dt 
           ELSE NULL 
      END) tl_lst_ord_dt, 
  MAX(CASE WHEN oa.mag_cd = 'CNS' 
           THEN ofo.pmt_dt 
           ELSE NULL 
      END) cr_lst_pmt_dt, 
  MAX(CASE WHEN oa.mag_cd = 'CRH' 
           THEN ofo.pmt_dt 
           ELSE NULL 
      END) hl_lst_pmt_dt, 
  MAX(CASE WHEN oa.mag_cd = 'CRM' 
           THEN ofo.pmt_dt 
           ELSE NULL 
      END) ma_lst_pmt_dt, 
  MAX(CASE WHEN oa.mag_cd = 'CNS' 
           THEN 'Y' 
           WHEN de.mag_cd = 'CNS' 
           THEN 'Y' 
           ELSE 'N' 
      END) cr_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRH' 
           THEN 'Y' 
           WHEN de.mag_cd = 'CRH' 
           THEN 'Y' 
           ELSE 'N' 
      END) hl_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRM' 
           THEN 'Y' 
           ELSE 'N' 
      END) ma_flg, 
  CASE WHEN MAX(CASE WHEN oa.mag_cd = 'CNS'
                      AND oa.svc_stat_cd = 'C'
                      AND oa.acct_id = ct.cr_comb_acct_id
                     THEN 'Y'
                     ELSE 'N'
                END) = 'Y'
       THEN 'Y'
       WHEN MAX(CASE WHEN oa.mag_cd = 'CNS'
                     THEN '2'
                     WHEN de.mag_cd = 'CNS'
                     THEN '1'
                END) = '1'
       THEN 'Y'
       ELSE 'N'
  END cr_exp_flg, 
  CASE WHEN MAX(CASE WHEN oa.mag_cd = 'CRH'
                      AND oa.svc_stat_cd = 'C'
                      AND oa.acct_id = ct.hl_comb_acct_id
                     THEN 'Y'
                     ELSE 'N'
                END) = 'Y'
       THEN 'Y'
       WHEN MAX(CASE WHEN oa.mag_cd = 'CRH'
                     THEN '2'
                     WHEN de.mag_cd = 'CRH'
                     THEN '1'
                END) = '1'
       THEN 'Y'
       ELSE 'N'
  END hl_exp_flg, 
  CASE WHEN MAX(CASE WHEN oa.mag_cd = 'CRM'
                      AND oa.svc_stat_cd = 'C'
                      AND oa.acct_id = ct.ma_comb_acct_id
                     THEN 'Y'
                     ELSE 'N'
                END) = 'Y'
       THEN 'Y'
       ELSE 'N'
  END ma_exp_flg,
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CNS'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_stat_cd = 'A'
                   AND oa.acct_id = ct.cr_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') ||
                         ofo.keycode
             END),9) cr_curr_ord_keycode, 
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRH'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_stat_cd = 'A'
                   AND oa.acct_id = ct.hl_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') ||
                         ofo.keycode
             END),9) hl_curr_ord_keycode, 
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_stat_cd = 'A'
                   AND oa.acct_id = ct.ma_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') ||
                         ofo.keycode
             END),9) ma_curr_ord_keycode, 
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CNS' 
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) cr_fst_mbr_keycode, 
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CRH' 
                  AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) hl_fst_mbr_keycode, 
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CRM' 
                  AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) ma_fst_mbr_keycode, 
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CNS' 
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) cr_lst_ord_keycode, 
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRH' 
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) hl_lst_ord_keycode, 
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRM' 
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) ma_lst_ord_keycode, 
  MAX(CASE WHEN oa.mag_cd = 'CNS'
            AND SUBSTR(ofo.keycode,1,4) = 'LIFE'
            AND oa.svc_stat_cd = 'A'
            AND oa.acct_id = ct.cr_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) cr_lt_sub_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRH'
            AND SUBSTR(ofo.keycode,1,4) = 'LIFE'
            AND oa.svc_stat_cd = 'A'
            AND oa.acct_id = ct.hl_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) hl_lt_sub_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRM'
            AND SUBSTR(ofo.keycode,1,4) = 'LIFE'
            AND oa.svc_stat_cd = 'A'
            AND oa.acct_id = ct.ma_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) ma_lt_sub_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CNS'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_stat_cd = 'D'
            AND oa.acct_id = ct.cr_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) cr_non_sub_dnr_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRH'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_stat_cd = 'D'
            AND oa.acct_id = ct.hl_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) hl_non_sub_dnr_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRM'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_stat_cd = 'D'
            AND oa.acct_id = ct.ma_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) ma_non_sub_dnr_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CNS'
            AND oa.svc_stat_cd = 'A'
            AND ofo.set_cd IN ('C','E')
            AND ofo.stat_cd = 'B'
            AND oa.acct_id = ct.cr_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) cr_rec_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRH'
            AND oa.svc_stat_cd = 'A'
            AND ofo.set_cd IN ('C','E')
            AND ofo.stat_cd = 'B'
            AND oa.acct_id = ct.hl_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) hl_rec_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRM'
            AND oa.svc_stat_cd = 'A'
            AND ofo.set_cd IN ('C','E')
            AND ofo.stat_cd = 'B'
            AND oa.acct_id = ct.ma_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) ma_rec_flg, 
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CNS'
                   AND oa.acct_id = ct.cr_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       oa.svc_stat_cd
                  WHEN de.mag_cd = 'CNS'
                  THEN '-1000000'||
                       'C'
             END),9) cr_svc_stat_cd, 
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRH'
                   AND oa.acct_id = ct.hl_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       oa.svc_stat_cd
                  WHEN de.mag_cd = 'CRH'
                  THEN '-1000000'||
                       'C'
             END),9) hl_svc_stat_cd, 
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRM'
                   AND oa.acct_id = ct.ma_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       oa.svc_stat_cd
             END),9) ma_svc_stat_cd,   
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CNS'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_stat_cd = 'A'
                   AND oa.acct_id = ct.cr_comb_acct_id
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
                                          SUBSTR(ofo.keycode,1,1))
             END),9) cr_curr_ord_src_cd,  
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRH'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_stat_cd = 'A'
                   AND oa.acct_id = ct.hl_comb_acct_id
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
                                          SUBSTR(ofo.keycode,1,1))
             END),9) hl_curr_ord_src_cd,
     SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_stat_cd = 'A'
                   AND oa.acct_id = ct.ma_comb_acct_id
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
                                          SUBSTR(ofo.keycode,1,1))
             END),9) ma_curr_ord_src_cd,   
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CNS' 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) cr_fst_ord_src_cd, 
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CRH' 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) hl_fst_ord_src_cd, 
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CRM' 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) ma_fst_ord_src_cd, 
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CNS' 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) cr_lst_ord_src_cd, 
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRH' 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) hl_lst_ord_src_cd, 
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRM' 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) ma_lst_ord_src_cd,  
  MAX(CASE WHEN oa.mag_cd = 'CNS'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_stat_cd != 'D'
            AND oa.acct_id = ct.cr_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) cr_sub_dnr_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRH'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_stat_cd != 'D'
            AND oa.acct_id = ct.hl_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) hl_sub_dnr_flg,
  MAX(CASE WHEN oa.mag_cd = 'CRM'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_stat_cd != 'D'
            AND oa.acct_id = ct.ma_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) ma_sub_dnr_flg,
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CNS'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_stat_cd = 'A'
                   AND oa.acct_id = ct.cr_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.term_mth_cnt
             END),9) cr_curr_ord_term, 
  SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRH'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_stat_cd = 'A'
                   AND oa.acct_id = ct.hl_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.term_mth_cnt
             END),9) hl_curr_ord_term, 
 SUBSTR(MAX(CASE WHEN oa.mag_cd = 'CRM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_stat_cd = 'A'
                   AND oa.acct_id = ct.ma_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.term_mth_cnt
             END),9) ma_curr_ord_term, 
  MIN(CASE WHEN oa.mag_cd = 'CNS' 
            AND ofo.set_cd IN ('B','D') 
           THEN ofo.ord_dt 
           ELSE NULL 
      END) cr_fst_dnr_dt, 
  MIN(CASE WHEN oa.mag_cd = 'CRH' 
            AND ofo.set_cd IN ('B','D') 
           THEN ofo.ord_dt 
           ELSE NULL 
      END) hl_fst_dnr_dt, 
  MIN(CASE WHEN oa.mag_cd = 'CRM' 
            AND ofo.set_cd IN ('B','D') 
           THEN ofo.ord_dt 
           ELSE NULL 
      END) ma_fst_dnr_dt, 
  MAX(CASE WHEN de.mag_cd = 'CNS' 
           THEN de.eds_lst_src_cd 
           ELSE NULL 
      END) eds_lst_src_cd, 
  MAX(CASE WHEN de.mag_cd = 'CNS' 
           THEN de.morbank_mtch_cd 
           ELSE NULL 
      END) morbank_mtch_cd, 
  MAX(CASE WHEN oa.mag_cd = 'CNS'
            AND oa.svc_stat_cd = 'A'
            AND SUBSTR(oa.keycd,1,4) = 'AUTO'
            AND oa.acct_id = ct.cr_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) cr_auto_rnw_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRH'
            AND oa.svc_stat_cd = 'A'
            AND SUBSTR(oa.keycd,1,4) = 'AUTO'
            AND oa.acct_id = ct.hl_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END)  hl_auto_rnw_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRM'
            AND oa.svc_stat_cd = 'A'
            AND SUBSTR(oa.keycd,1,4) = 'AUTO'
            AND oa.acct_id = ct.ma_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) ma_auto_rnw_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CNS' 
            AND ( 
                  ofo.set_cd IN ('A','C','E')  
                  or 
                  ( ofo.SET_CD in ('B', 'D') and ofo.STAT_CD not in ('D') )
                )
           THEN 'Y' 
           ELSE 'N' 
      END) cr_non_dnr_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRH' 
            AND ( 
                  ofo.set_cd IN ('A','C','E')  
                  or 
                  ( ofo.SET_CD in ('B', 'D') and ofo.STAT_CD not in ('D') )
                )
           THEN 'Y' 
           ELSE 'N' 
      END) hl_non_dnr_flg, 
  MAX(CASE WHEN oa.mag_cd = 'CRM' 
            AND ( 
                  ofo.set_cd IN ('A','C','E')  
                  or 
                  ( ofo.SET_CD in ('B', 'D') and ofo.STAT_CD not in ('D') )
                )
           THEN 'Y' 
           ELSE 'N' 
      END) ma_non_dnr_flg,
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CNS' 
                   AND ofo.orig_strt_iss_num > 0
                   AND ofo.orig_strt_iss_num >= nvl(substr(obt.last_cro_brk_dt, instr(obt.last_cro_brk_dt, '|', 1)+1), ofo.orig_strt_iss_num)
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) cr_curr_mbr_keycode, 
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CRH' 
                   AND ofo.orig_strt_iss_num > 0
                   AND ofo.orig_strt_iss_num >= nvl(substr(obt.last_hl_brk_dt, instr(obt.last_hl_brk_dt, '|', 1)+1), ofo.orig_strt_iss_num)
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) hl_curr_mbr_keycode, 
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CRM'
                   AND ofo.orig_strt_iss_num > 0 
                   AND ofo.orig_strt_iss_num >= nvl(substr(obt.last_ma_brk_dt, instr(obt.last_ma_brk_dt, '|', 1)+1), ofo.orig_strt_iss_num)
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) ma_curr_mbr_keycode, 
  MAX(CASE WHEN oa.mag_cd = 'CNS' 
            AND ( ofo.set_cd NOT IN('B','D') or ofo.stat_cd not in ('D') )
           THEN obt.cro_brks_cnt 
           ELSE null 
  END) cro_brks_cnt,
  MAX(CASE WHEN de_cnt.mag_cd = 'CNS'
           THEN de_cnt.de_cr_brks_cnt                  
           ELSE null                  
  END) de_cro_brks_cnt, 
  MAX(CASE WHEN oa.mag_cd = 'CRH' 
            AND ( ofo.set_cd NOT IN('B','D') or ofo.stat_cd not in ('D') )
           THEN obt.hl_brks_cnt 
           ELSE NULL 
  END) hl_brks_cnt,
  MAX(CASE WHEN de_cnt.mag_cd = 'CRH' 
           THEN de_cnt.de_hl_brks_cnt                  
           ELSE null                  
  END) de_hl_brks_cnt, 
  MAX(CASE WHEN oa.mag_cd = 'CRM' 
            AND ( ofo.set_cd NOT IN('B','D') or ofo.stat_cd not in ('D') )
           THEN obt.ma_brks_cnt 
           ELSE NULL 
  END) ma_brks_cnt,
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CNS' 
                   AND ofo.orig_strt_iss_num > 0
                   AND ofo.orig_strt_iss_num >= nvl(substr(obt.last_cro_brk_dt, instr(obt.last_cro_brk_dt, '|', 1)+1), ofo.orig_strt_iss_num) 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) cr_curr_mbr_src_cd, 
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CRH' 
                   AND ofo.orig_strt_iss_num > 0
                   AND ofo.orig_strt_iss_num >= nvl(substr(obt.last_hl_brk_dt, instr(obt.last_hl_brk_dt, '|', 1)+1), ofo.orig_strt_iss_num) 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) hl_curr_mbr_src_cd, 
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CRM' 
                   AND ofo.orig_strt_iss_num > 0                  
                   AND ofo.orig_strt_iss_num >= nvl(substr(obt.last_ma_brk_dt, instr(obt.last_ma_brk_dt, '|', 1)+1), ofo.orig_strt_iss_num) 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) ma_curr_mbr_src_cd, 
    SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CNS'
                     AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                     AND cro_brks_cnt = 1
                     AND ofo.orig_strt_iss_num >= obt.cns_orig_strt_iss_num
                     AND ofo.orig_strt_iss_num < substr(obt.last_cro_brk_dt, instr(obt.last_cro_brk_dt, '|', 1)+1)
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
                              SUBSTR(ofo.keycode,1,1)) 
                  WHEN oa.mag_cd = 'CNS' 
                   AND cro_brks_cnt > 1
                   AND ofo.orig_strt_iss_num < substr(obt.last_cro_brk_dt, instr(obt.last_cro_brk_dt, '|', 1)+1)
                   AND ofo.orig_strt_iss_num >= substr(obt.prior_cro_brk_dt, instr(obt.prior_cro_brk_dt, '|', 1)+1)
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) cr_prior_mbr_src_cd, 
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CRH' 
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   AND hl_brks_cnt = 1
                   AND ofo.orig_strt_iss_num >= obt.crh_orig_strt_iss_num
                   AND ofo.orig_strt_iss_num < substr(obt.last_hl_brk_dt, instr(obt.last_hl_brk_dt, '|', 1)+1)                   
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
                              SUBSTR(ofo.keycode,1,1)) 
                  WHEN oa.mag_cd = 'CRH' 
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   AND hl_brks_cnt > 1
                   AND ofo.orig_strt_iss_num < substr(obt.last_hl_brk_dt, instr(obt.last_hl_brk_dt, '|', 1)+1)
                   AND ofo.orig_strt_iss_num >= substr(obt.prior_hl_brk_dt, instr(obt.prior_hl_brk_dt, '|', 1)+1)
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) hl_prior_mbr_src_cd, 
  SUBSTR(MIN(CASE WHEN oa.mag_cd = 'CRM' 
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   AND ma_brks_cnt = 1
                   AND ofo.orig_strt_iss_num >= obt.crm_orig_strt_iss_num
                   AND ofo.orig_strt_iss_num < substr(obt.last_ma_brk_dt, instr(obt.last_ma_brk_dt, '|', 1)+1)                 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  WHEN oa.mag_cd = 'CRM' 
                   AND ( ofo.set_cd not in ('B','D') or ofo.stat_cd not in ('D') )
                   AND ma_brks_cnt > 1
                   AND ofo.orig_strt_iss_num < substr(obt.last_ma_brk_dt, instr(obt.last_ma_brk_dt, '|', 1)+1)
                   AND ofo.orig_strt_iss_num >= substr(obt.prior_ma_brk_dt, instr(obt.prior_ma_brk_dt, '|', 1)+1)
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) ma_prior_mbr_src_cd,
    substr(MAX(CASE WHEN oa.mag_cd = 'CNS'
                     AND oa.svc_stat_cd <> 'D' 
                    THEN lpad(oa.expr_iss_num, 5, 0)||oa.rnw_cnt
               END), 6) cr_rnw_cnt,
    substr(MAX(CASE WHEN oa.mag_cd = 'CRH'
                     AND oa.svc_stat_cd <> 'D' 
                    THEN lpad(oa.expr_iss_num, 5, 0)||oa.rnw_cnt
               END), 6) hl_rnw_cnt,        
    substr(MAX(CASE WHEN oa.mag_cd = 'CRM'
                     AND oa.svc_stat_cd <> 'D' 
                    THEN lpad(oa.expr_iss_num, 5, 0)||oa.rnw_cnt
               END), 6) ma_rnw_cnt,
    MIN(CASE WHEN oa.mag_cd = 'CNS'                
              AND ofo.orig_strt_iss_num > 0
              AND ofo.orig_strt_iss_num >= nvl(substr(obt.last_cro_brk_dt, instr(obt.last_cro_brk_dt, '|', 1)+1), ofo.orig_strt_iss_num) 
              AND ( nvl(ofo.set_cd, 'Z') not in ('B','D') or ofo.stat_cd not in ('D') )
             THEN ofo.ord_dt
             ELSE null
    END) cr_curr_mbr_dt,                         -- mribbi 11/06/2006
    MIN(CASE WHEN oa.mag_cd = 'CRH' 
              AND ofo.orig_strt_iss_num > 0
              AND ofo.orig_strt_iss_num >= nvl(substr(obt.last_hl_brk_dt, instr(obt.last_hl_brk_dt, '|', 1)+1), ofo.orig_strt_iss_num) 
              AND ( nvl(ofo.set_cd, 'Z') not in ('B','D') or ofo.stat_cd not in ('D') )
             THEN ofo.ord_dt
             ELSE null
    END) hl_curr_mbr_dt,                         -- mribbi 11/06/2006
    MIN(CASE WHEN oa.mag_cd = 'CRM' 
              AND ofo.orig_strt_iss_num > 0
              AND ofo.orig_strt_iss_num >= nvl(substr(obt.last_ma_brk_dt, instr(obt.last_ma_brk_dt, '|', 1)+1), ofo.orig_strt_iss_num) 
              AND ( nvl(ofo.set_cd, 'Z') not in ('B','D') or ofo.stat_cd not in ('D') )
             THEN ofo.ord_dt
             ELSE null
    END) ma_curr_mbr_dt,                           -- mribbi 11/06/2006
    nvl(max(ofo2.cr_ltd_pd_amt),0) cr_ltd_pd_amt,
    nvl(max(ofo2.hl_ltd_pd_amt),0) hl_ltd_pd_amt,
    nvl(max(ofo2.ma_ltd_pd_amt),0) ma_ltd_pd_amt,
    max(CASE WHEN ofo.stat_cd = 'B'
              AND oa.mag_cd = 'CNS'
              AND oa.svc_stat_cd = 'A'
              AND oa.acct_id = ct.cr_comb_acct_id
             THEN ofo.ord_dt
      END) cr_curr_ord_dt,
    max(CASE WHEN ofo.stat_cd = 'B'
              AND oa.mag_cd = 'CRH'
              AND oa.svc_stat_cd = 'A'
              AND oa.acct_id = ct.hl_comb_acct_id
         THEN ofo.ord_dt
    END) hl_curr_ord_dt,
    max(CASE WHEN ofo.stat_cd = 'B'
              AND oa.mag_cd = 'CRM'
              AND oa.svc_stat_cd = 'A'
              AND oa.acct_id = ct.ma_comb_acct_id
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
    CASE WHEN substr(max(CASE WHEN ofo.mag_cd = 'CNS'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('C','E')
         THEN 'R'
         WHEN substr(max(CASE WHEN ofo.mag_cd = 'CNS'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) = 'A'
         THEN 'O'
         WHEN substr(max(CASE WHEN ofo.mag_cd = 'CNS'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || nvl(ofo.set_cd,'X') || oa.svc_stat_cd
                         END),9) in ('BD','DD')
         THEN 'N'
         WHEN substr(max(CASE WHEN ofo.mag_cd = 'CNS'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('B','D')
         THEN 'D'
    END cr_lst_sub_ord_role_cd,
    CASE WHEN substr(max(CASE WHEN ofo.mag_cd = 'CRH'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('C','E')
         THEN 'R'
         WHEN substr(max(CASE WHEN ofo.mag_cd = 'CRH'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) = 'A'
         THEN 'O'
         WHEN substr(max(CASE WHEN ofo.mag_cd = 'CRH'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || nvl(ofo.set_cd,'X') || oa.svc_stat_cd
                         END),9) in ('BD','DD')
         THEN 'N'
         WHEN substr(max(CASE WHEN ofo.mag_cd = 'CRH'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('B','D')
         THEN 'D'
    END hl_lst_sub_ord_role_cd,
    CASE WHEN substr(max(CASE WHEN ofo.mag_cd = 'CRM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('C','E')
         THEN 'R'
         WHEN substr(max(CASE WHEN ofo.mag_cd = 'CRM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) = 'A'
         THEN 'O'
         WHEN substr(max(CASE WHEN ofo.mag_cd = 'CRM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd || oa.svc_stat_cd
                         END),9) in ('BD','DD')
         THEN 'N'
         WHEN substr(max(CASE WHEN ofo.mag_cd = 'CRM'
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
    MAX(CASE WHEN oa.mag_cd = 'SHM'
           AND oa.svc_stat_cd = 'A'
           AND ofo.entr_typ_cd != 'F' 
           AND ofo.stat_cd = 'B'
           AND (ofo.src_cd NOT IN ('CA','CC')
                OR (SUBSTR(ofo.keycode,1,4) = 'LIFE'))
           AND oa.acct_id = ct.shm_comb_acct_id
          THEN 'Y' 
          ELSE 'N' 
        END) shm_actv_flg,
    MAX(CASE WHEN oa.mag_cd = 'SHM'
            AND oa.svc_stat_cd in ('A','E')
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (SUBSTR(ofo.keycode,1,4) = 'LIFE'))
            AND oa.acct_id = ct.shm_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) shm_actv_rptg_flg,
     MAX(CASE WHEN oa.mag_cd = 'SHM'
            AND oa.svc_stat_cd = 'A'
            AND ofo.entr_typ_cd != 'F' 
            AND ofo.stat_cd = 'B'
            AND ofo.set_cd in ('A','B')
            AND (ofo.src_cd NOT IN ('CA','CC')
                 OR (SUBSTR(ofo.keycode,1,4) = 'LIFE'))
            AND ofo.cr_stat_cd in ('C', 'D', 'E', 'F', 'G', 'I')
            AND oa.acct_id = ct.shm_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) shm_actv_pd_flg,
      CASE WHEN SUBSTR(MAX(CASE WHEN oa.mag_cd = 'SHM' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.canc_rsn_cd 
                            ELSE NULL 
                       END),9) IN ('06','50')
       THEN 'Y' 
       ELSE 'N' 
  END shm_canc_bad_dbt_flg, 
    CASE WHEN SUBSTR(MAX(CASE WHEN oa.mag_cd = 'SHM' 
                            THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                                 oca.canc_rsn_cd 
                            ELSE NULL 
                       END),9) NOT IN ('06','14','50') 
       THEN 'Y' 
       ELSE 'N' 
  END shm_canc_cust_flg, 
   MAX(CASE WHEN oa.mag_cd = 'SHM'
            AND oa.svc_stat_cd = 'E'
            AND oa.acct_id = ct.shm_comb_acct_id
           THEN 'Y' 
           ELSE 'N' 
      END) shm_crd_pend_flg,
    DECODE(SUBSTR(MAX(CASE WHEN oa.mag_cd = 'SHM' 
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
     MAX(CASE WHEN oa.mag_cd = 'SHM' 
           THEN iss.pub_dt 
           ELSE NULL 
      END) shm_exp_dt, 
     MAX(CASE WHEN ofo.canc_rsn_cd not in ('06','14','50')
           AND ofo.mag_cd = 'SHM'
           THEN ofo.canc_dt
      END) shm_lst_canc_dt,
     MAX(CASE WHEN oa.mag_cd = 'SHM' 
           THEN ofo.pmt_dt 
           ELSE NULL 
      END) shm_lst_pmt_dt, 
       MAX(CASE WHEN oa.mag_cd = 'SHM' 
           THEN 'Y' 
           ELSE 'N' 
      END) shm_flg,
      CASE WHEN MAX(CASE WHEN oa.mag_cd = 'SHM'
                      AND oa.svc_stat_cd = 'C'
                      AND oa.acct_id = ct.shm_comb_acct_id
                     THEN 'Y'
                     ELSE 'N'
                END) = 'Y'
       THEN 'Y'
       ELSE 'N'
      END shm_exp_flg,
        SUBSTR(MIN(CASE WHEN oa.mag_cd = 'SHM' 
                   AND ofo.orig_strt_iss_num > 0
                   AND ofo.orig_strt_iss_num >= nvl(substr(obt.last_shm_brk_dt, instr(obt.last_shm_brk_dt, '|', 1)+1), ofo.orig_strt_iss_num)
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                   THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) shm_curr_mbr_keycode,
      SUBSTR(MAX(CASE WHEN oa.mag_cd = 'SHM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_stat_cd = 'A'
                   AND oa.acct_id = ct.shm_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') ||
                         ofo.keycode
             END),9) shm_curr_ord_keycode, 
      SUBSTR(MIN(CASE WHEN oa.mag_cd = 'SHM' 
                   AND ( ofo.set_cd not in('B','D') or ofo.stat_cd not in ('D') )
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'99999999') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) shm_fst_mbr_keycode, 
       SUBSTR(MAX(CASE WHEN oa.mag_cd = 'SHM' 
                  THEN NVL(TO_CHAR(ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.keycode 
                  ELSE NULL 
             END),9) shm_lst_ord_keycode,
      MAX(CASE WHEN oa.mag_cd = 'SHM'
            AND SUBSTR(ofo.keycode,1,4) = 'LIFE'
            AND oa.svc_stat_cd = 'A'
            AND oa.acct_id = ct.shm_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) shm_lt_sub_flg, 
      MAX(CASE WHEN oa.mag_cd = 'SHM'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_stat_cd = 'D'
            AND oa.acct_id = ct.shm_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) shm_non_sub_dnr_flg,
       MAX(CASE WHEN oa.mag_cd = 'SHM' AND (ofo.set_cd NOT IN('B','D') or ofo.stat_cd not in ('D') )
           THEN obt.shm_brks_cnt 
           ELSE NULL 
  END) shm_brks_cnt, 
      substr(MAX(CASE WHEN oa.mag_cd = 'SHM'
                     AND oa.svc_stat_cd <> 'D' 
                    THEN lpad(oa.expr_iss_num, 5, 0)||oa.rnw_cnt
               END), 6) shm_rnw_cnt,
    MAX(CASE WHEN oa.mag_cd = 'SHM'
            AND oa.svc_stat_cd = 'A'
            AND ofo.set_cd IN ('C','E')
            AND ofo.stat_cd = 'B'
            AND oa.acct_id = ct.shm_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) shm_rec_flg, 
     SUBSTR(MAX(CASE WHEN oa.mag_cd = 'SHM'
                   AND oa.acct_id = ct.shm_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       oa.svc_stat_cd
             END),9) shm_svc_stat_cd, 
     SUBSTR(MIN(CASE WHEN oa.mag_cd = 'SHM' 
                   AND ofo.orig_strt_iss_num > 0
                   AND ofo.orig_strt_iss_num >= nvl(substr(obt.last_shm_brk_dt, instr(obt.last_shm_brk_dt, '|', 1)+1), ofo.orig_strt_iss_num) 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) shm_curr_mbr_src_cd,   
     SUBSTR(MAX(CASE WHEN oa.mag_cd = 'SHM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_stat_cd = 'A'
                   AND oa.acct_id = ct.shm_comb_acct_id
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
                                          SUBSTR(ofo.keycode,1,1))
             END),9) shm_curr_ord_src_cd, 
     SUBSTR(MIN(CASE WHEN oa.mag_cd = 'SHM' 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) shm_fst_ord_src_cd, 
       SUBSTR(MAX(CASE WHEN oa.mag_cd = 'SHM' 
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) shm_lst_ord_src_cd, 
          SUBSTR(MIN(CASE WHEN oa.mag_cd = 'SHM'
             AND ofo.set_cd not in ('B','D') 
             AND shm_brks_cnt = 1
             AND ofo.orig_strt_iss_num >= obt.shm_orig_strt_iss_num
             AND ofo.orig_strt_iss_num < substr(obt.last_shm_brk_dt, instr(obt.last_shm_brk_dt, '|', 1)+1)
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
                              SUBSTR(ofo.keycode,1,1)) 
                  WHEN oa.mag_cd = 'SHM' 
                   AND shm_brks_cnt > 1
           AND ofo.orig_strt_iss_num < substr(obt.last_shm_brk_dt, instr(obt.last_shm_brk_dt, '|', 1)+1)
           AND ofo.orig_strt_iss_num >= substr(obt.prior_shm_brk_dt, instr(obt.prior_shm_brk_dt, '|', 1)+1)
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
                              SUBSTR(ofo.keycode,1,1)) 
                  ELSE NULL 
             END),9) shm_prior_mbr_src_cd, 
      MAX(CASE WHEN oa.mag_cd = 'SHM'
            AND ofo.set_cd IN ('B','D')
            AND ofo.ord_dt > ADD_MONTHS(SYSDATE,-12)
            AND oa.svc_stat_cd != 'D'
            AND oa.acct_id = ct.shm_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) shm_sub_dnr_flg,
      SUBSTR(MAX(CASE WHEN oa.mag_cd = 'SHM'
                   AND ofo.stat_cd = 'B'
                   AND oa.svc_stat_cd = 'A'
                   AND oa.acct_id = ct.shm_comb_acct_id
                  THEN NVL(TO_CHAR(ofo.ord_dt,'YYYYMMDD'),'00000000') || 
                       ofo.term_mth_cnt
             END),9) shm_curr_ord_term, 
      MIN(CASE WHEN oa.mag_cd = 'SHM' 
            AND ofo.set_cd IN ('B','D') 
           THEN ofo.ord_dt 
           ELSE NULL 
      END) shm_fst_dnr_dt, 
       MAX(CASE WHEN oa.mag_cd = 'SHM'
            AND oa.svc_stat_cd = 'A'
            AND SUBSTR(oa.keycd,1,4) = 'AUTO'
            AND oa.acct_id = ct.shm_comb_acct_id
           THEN 'Y'
           ELSE 'N'
      END) shm_auto_rnw_flg, 
      MAX(CASE WHEN oa.mag_cd = 'SHM' 
            AND (
                  ofo.set_cd IN ('A','C','E')
                  or
                  (ofo.set_cd in ('B', 'D') and ofo.stat_cd not in ('D'))
                ) 
           THEN 'Y' 
           ELSE 'N' 
      END) shm_non_dnr_flg, 
        MIN(CASE WHEN oa.mag_cd = 'SHM'                
              AND ofo.orig_strt_iss_num > 0
              AND ofo.orig_strt_iss_num >= nvl(substr(obt.last_shm_brk_dt, instr(obt.last_shm_brk_dt, '|', 1)+1), ofo.orig_strt_iss_num) 
              AND ( nvl(ofo.set_cd, 'Z') not in ('B','D') or ofo.stat_cd not in ('D') )
             THEN ofo.ord_dt
             ELSE null
    END) shm_curr_mbr_dt,  
       max(CASE WHEN ofo.stat_cd = 'B'
              AND oa.mag_cd = 'SHM'
              AND oa.svc_stat_cd = 'A'
              AND oa.acct_id = ct.shm_comb_acct_id
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
        CASE WHEN substr(max(CASE WHEN ofo.mag_cd = 'SHM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('C','E')
         THEN 'R'
         WHEN substr(max(CASE WHEN ofo.mag_cd = 'SHM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) = 'A'
         THEN 'O'
         WHEN substr(max(CASE WHEN ofo.mag_cd = 'SHM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || nvl(ofo.set_cd,'X') || oa.svc_stat_cd
                         END),9) in ('BD','DD')
         THEN 'N'
         WHEN substr(max(CASE WHEN ofo.mag_cd = 'SHM'
                              THEN nvl(to_char(ofo.ord_dt,'YYYYMMDD'),'00000000') || ofo.set_cd
                         END),9) in ('B','D')
         THEN 'D'
        END shm_lst_sub_ord_role_cd
FROM 
     offline_account_tmp4 oa,
     mt_offline_ord_tmp3 ofo,
     warehouse2.offline_cancel_act oca,
     mt_ofl_brks_temp obt,
     warehouse2.issue_lkp iss,
     mt_ofl_de_temp de,
     mt_ofl_combine_temp ct,
     mt_ofl_sum_tmp1 de_cnt,
     mt_ofl_sum_tmp2 ofo2,
     warehouse2.external_ref er
WHERE er.internal_key = oa.acct_id(+)
  AND oa.acct_id = ofo.acct_id(+)
  AND ofo.ord_id = oca.ord_id(+)
  AND er.individual_id = obt.individual_id(+)
  AND er.individual_id = de.individual_id(+)
  AND er.individual_id = ct.individual_id(+)
  AND er.individual_id = de_cnt.individual_id (+)
  AND er.individual_id = ofo2.individual_id (+)
  AND oa.mag_cd = iss.mag_cd(+)
  AND oa.expr_iss_num = iss.iss_num(+)
  AND nvl(oa.acct_id,de.individual_id) IS NOT NULL
GROUP BY er.individual_id
;

CREATE TABLE middle_tier2.mt_offline_summary
  as
SELECT
  ix.individual_id,
  ix.ind_urn,
  ix.hh_id,
  ost.cr_actv_flg,
  ost.hl_actv_flg,
  ost.ma_actv_flg,
  ost.cr_actv_rptg_flg,
  ost.hl_actv_rptg_flg,
  ost.ma_actv_rptg_flg,
  ost.cr_actv_pd_flg,
  ost.hl_actv_pd_flg,
  ost.ma_actv_pd_flg,
  ost.cr_canc_bad_dbt_flg,
  ost.hl_canc_bad_dbt_flg,
  ost.ma_canc_bad_dbt_flg,
  ost.cr_canc_cust_flg,
  ost.hl_canc_cust_flg,
  ost.ma_canc_cust_flg,
  ost.cr_crd_pend_flg,
  ost.hl_crd_pend_flg,
  ost.ma_crd_pend_flg,
  ost.cr_crd_stat_cd,
  ost.hl_crd_stat_cd,
  ost.ma_crd_stat_cd,
  nullif(greatest(nvl(ost.cr_crd_stat_cd,'A'),
                  nvl(ost.hl_crd_stat_cd,'A'),
                  nvl(ost.ma_crd_stat_cd,'A'),
                  nvl(ost.shm_crd_stat_cd,'A')),'A') ofo_crd_stat_cd,
  ost.cr_exp_dt,
  ost.hl_exp_dt,
  ost.ma_exp_dt,
  ost.cr_lst_canc_dt,
  ost.hl_lst_canc_dt,
  ost.ma_lst_canc_dt,
  ost.tl_lst_ord_dt,
  ost.cr_lst_pmt_dt,
  ost.hl_lst_pmt_dt,
  ost.ma_lst_pmt_dt,
  ost.cr_flg,
  ost.hl_flg,
  ost.ma_flg,
  ost.cr_exp_flg,
  ost.hl_exp_flg,
  ost.ma_exp_flg,
  ost.cr_curr_mbr_keycode,
  ost.hl_curr_mbr_keycode,
  ost.ma_curr_mbr_keycode,
  ost.cr_curr_ord_keycode,
  ost.hl_curr_ord_keycode,
  ost.ma_curr_ord_keycode,
  ost.cr_fst_mbr_keycode,
  ost.hl_fst_mbr_keycode,
  ost.ma_fst_mbr_keycode,
  ost.cr_lst_ord_keycode,
  ost.hl_lst_ord_keycode,
  ost.ma_lst_ord_keycode,
  ost.cr_lt_sub_flg,
  ost.hl_lt_sub_flg,
  ost.ma_lt_sub_flg,
  ost.cr_non_sub_dnr_flg,
  ost.hl_non_sub_dnr_flg,
  ost.ma_non_sub_dnr_flg,
  nvl(ost.cro_brks_cnt,0) + nvl(ost.de_cro_brks_cnt,0) cr_brks_cnt,
  nvl(ost.hl_brks_cnt,0) + nvl(ost.de_hl_brks_cnt,0) hl_brks_cnt,
  nvl(ost.ma_brks_cnt, 0) ma_brks_cnt,
  ost.cr_rnw_cnt,
  ost.hl_rnw_cnt,
  ost.ma_rnw_cnt,
  ost.cr_rec_flg,
  ost.hl_rec_flg,
  ost.ma_rec_flg,
  ost.cr_svc_stat_cd,
  ost.hl_svc_stat_cd,
  ost.ma_svc_stat_cd,
  ost.cr_curr_mbr_src_cd,
  ost.hl_curr_mbr_src_cd,
  ost.ma_curr_mbr_src_cd,
  ost.cr_curr_ord_src_cd,
  ost.hl_curr_ord_src_cd,
  ost.ma_curr_ord_src_cd,
  ost.cr_fst_ord_src_cd,
  ost.hl_fst_ord_src_cd,
  ost.ma_fst_ord_src_cd,
  ost.cr_lst_ord_src_cd,
  ost.hl_lst_ord_src_cd,
  ost.ma_lst_ord_src_cd,
  ost.cr_prior_mbr_src_cd,
  ost.hl_prior_mbr_src_cd,
  ost.ma_prior_mbr_src_cd,
  ost.cr_sub_dnr_flg,
  ost.hl_sub_dnr_flg,
  ost.ma_sub_dnr_flg,
  ost.cr_curr_ord_term,
  ost.hl_curr_ord_term,
  ost.ma_curr_ord_term,
  ost.cr_fst_dnr_dt,
  ost.hl_fst_dnr_dt,
  ost.ma_fst_dnr_dt,
  ost.eds_lst_src_cd,
  ost.morbank_mtch_cd,
  ost.cr_auto_rnw_flg,
  ost.hl_auto_rnw_flg,
  ost.ma_auto_rnw_flg,
  ost.cr_non_dnr_flg,
  ost.hl_non_dnr_flg,
  ost.ma_non_dnr_flg,
  ost.cr_curr_mbr_dt,
  ost.hl_curr_mbr_dt,
  ost.ma_curr_mbr_dt,
  ost.cr_ltd_pd_amt,
  ost.hl_ltd_pd_amt,
  ost.ma_ltd_pd_amt,
  ost.cr_curr_ord_dt,
  ost.hl_curr_ord_dt,
  ost.ma_curr_ord_dt,
  ost.cr_fst_ord_dt,
  ost.hl_fst_ord_dt,
  ost.ma_fst_ord_dt,
  ost.cr_lst_canc_bad_dbt_dt,
  ost.hl_lst_canc_bad_dbt_dt,
  ost.ma_lst_canc_bad_dbt_dt,
  ost.cr_lst_dnr_ord_dt,
  ost.hl_lst_dnr_ord_dt,
  ost.ma_lst_dnr_ord_dt,
  ost.cr_lst_ord_dt,
  ost.hl_lst_ord_dt,
  ost.ma_lst_ord_dt,
  ost.cr_canc_bad_dbt_cnt,
  ost.hl_canc_bad_dbt_cnt,
  ost.ma_canc_bad_dbt_cnt,
  ost.cr_canc_cust_cnt,
  ost.hl_canc_cust_cnt,
  ost.ma_canc_cust_cnt,
  ost.cr_dm_ord_cnt,
  ost.hl_dm_ord_cnt,
  ost.ma_dm_ord_cnt,
  ost.cr_em_ord_cnt,
  ost.hl_em_ord_cnt,
  ost.ma_em_ord_cnt,
  ost.cr_dnr_ord_cnt,
  ost.hl_dnr_ord_cnt,
  ost.ma_dnr_ord_cnt,
  ost.cr_ord_cnt,
  ost.hl_ord_cnt,
  ost.ma_ord_cnt,
  ost.cr_wr_off_cnt,
  ost.hl_wr_off_cnt,
  ost.ma_wr_off_cnt,
  ost.cr_lst_sub_ord_role_cd,
  ost.hl_lst_sub_ord_role_cd,
  ost.ma_lst_sub_ord_role_cd,
  ost.shm_actv_flg,
  ost.shm_actv_rptg_flg,
  ost.shm_actv_pd_flg,
  ost.shm_canc_bad_dbt_flg,
  ost.shm_canc_cust_flg,
  ost.shm_crd_pend_flg,
  ost.shm_crd_stat_cd,
  ost.shm_exp_dt,
  ost.shm_lst_canc_dt,
  ost.shm_lst_pmt_dt,
  ost.shm_flg,
  ost.shm_exp_flg,
  ost.shm_curr_mbr_keycode,
  ost.shm_curr_ord_keycode,
  ost.shm_fst_mbr_keycode,
  ost.shm_lst_ord_keycode,
  ost.shm_lt_sub_flg,
  ost.shm_non_sub_dnr_flg,
  nvl(ost.shm_brks_cnt, 0) shm_brks_cnt,
  ost.shm_rnw_cnt,
  ost.shm_rec_flg,
  ost.shm_svc_stat_cd,
  ost.shm_curr_mbr_src_cd,
  ost.shm_curr_ord_src_cd,
  ost.shm_fst_ord_src_cd,
  ost.shm_lst_ord_src_cd,
  ost.shm_prior_mbr_src_cd,
  ost.shm_sub_dnr_flg,
  ost.shm_curr_ord_term,
  ost.shm_fst_dnr_dt,
  ost.shm_auto_rnw_flg,
  ost.shm_non_dnr_flg,
  ost.shm_curr_mbr_dt,
  ost.shm_ltd_pd_amt,
  ost.shm_curr_ord_dt,
  ost.shm_fst_ord_dt,
  ost.shm_lst_canc_bad_dbt_dt,
  ost.shm_lst_dnr_ord_dt,
  ost.shm_lst_ord_dt,
  ost.shm_canc_bad_dbt_cnt,
  ost.shm_canc_cust_cnt,
  ost.shm_dm_ord_cnt,
  ost.shm_em_ord_cnt,
  ost.shm_dnr_ord_cnt,
  ost.shm_ord_cnt,
  ost.shm_wr_off_cnt,
  ost.shm_lst_sub_ord_role_cd,
  ix.osl_hh_id
FROM middle_tier.ind_xref ix,
     middle_tier.mt_ofl_sum_temp ost
WHERE ix.individual_id = ost.individual_id
  AND ix.active_flag = 'A';
