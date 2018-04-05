
CREATE TABLE cr_temp.mt_onl_sum_temp1
AS
SELECT
  individual_id,
  hash_account_id,
  max(last_crmg_brk_dt) last_crmg_brk_dt,
  max(prior_crmg_brk_dt) prior_crmg_brk_dt,
  max(last_cro_brk_dt) last_cro_brk_dt,
  max(prior_cro_brk_dt) prior_cro_brk_dt,
  max(last_cro_acct_brk_dt) last_cro_acct_brk_dt,
  max(crmg_brks_cnt) crmg_brks_cnt,
  max(cro_brks_cnt) cro_brks_cnt,
  case when max(max(cro_actv)) over (partition by individual_id) = 'Y'
       then first_value(hash_account_id) over (partition by individual_id
                                           order by max(cro_actv) desc nulls last
                                                  , to_date(SUBSTRING(min(case when mag_cd = 'CRO'
                                                                             and nvl(set_cd, 'Z') not in ('B','D')
                                                                             and nvl(to_char(strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || nvl(to_char(end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || lpad(itm_id,21) > nvl(last_cro_acct_brk_dt,'0000000000000000000000000000')
                                                                            then nvl(to_char(strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || nvl(to_char(end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || lpad(itm_id,21) || to_char(crt_dt,'YYYYMMDDHH24MISS')
                                                                            else null
                                                                       end),50),'YYYYMMDDHH24MISS') rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
       else first_value(hash_account_id) over (partition by individual_id
                                           order by max(cro_actv) desc nulls last
                                                  , to_date(SUBSTRING(min(case when mag_cd = 'CRO'
                                                                             and nvl(set_cd, 'Z') not in ('B','D')
                                                                             and nvl(to_char(strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || nvl(to_char(end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || lpad(itm_id,21) > nvl(last_cro_acct_brk_dt,'0000000000000000000000000000')
                                                                            then nvl(to_char(strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || nvl(to_char(end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                                                                              || lpad(itm_id,21) || to_char(crt_dt,'YYYYMMDDHH24MISS')
                                                                            else null
                                                                       end),50),'YYYYMMDDHH24MISS') desc nulls last rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  end cro_curr_mbr_dt_hash_account_id
FROM  
  (SELECT
     t2.individual_id,
     t2.hash_account_id,
     t2.itm_id,
     t2.mag_cd,
     t2.set_cd,
     t2.strt_dt,
     t2.end_dt,
     t2.crt_dt,
     first_value(crmg_brk_dt) OVER (PARTITION BY t2.individual_id
                                        ORDER BY crmg_brk_dt desc nulls last rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_crmg_brk_dt,
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
              AND SUBSTRING(nvl(t1.ext_keycd, nvl(t1.int_keycd, 'ZZ')),1,2) != 'WF'
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
                         || lpad(t1.itm_id,21)
                       WHEN t1.term_mth_cnt > 1
                        AND lead(t1.strt_dt) over (partition by t1.individual_id
                                                              , t1.hash_account_id
	                                                            , t1.mag_cd
	                                                            , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
	                                                     order by t1.strt_dt, t1.end_dt, t1.itm_id)
	                            - t1.end_dt > 49
                       THEN nvl(to_char(t1.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || nvl(to_char(t1.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || lpad(t1.itm_id,21)
                       WHEN t1.term_mth_cnt = 1
                        AND lead(t1.strt_dt) over (partition by t1.individual_id
                                                              , t1.hash_account_id
	                                                            , t1.mag_cd
	                                                            , case when t1.set_cd NOT IN ('B', 'D') then 1 else 0 end
	                                                     order by t1.strt_dt, t1.end_dt, t1.itm_id)
	                            - t1.end_dt > 25
                       THEN nvl(to_char(t1.strt_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || nvl(to_char(t1.end_dt,'YYYYMMDDHH24MISS'),'00000000000000')
                         || lpad(t1.itm_id,21)
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
      left join prod.agg_digital_order olo on t1.acct_id = olo.acct_id and t1.ord_id = olo.ord_id) t2)
GROUP BY individual_id, hash_account_id
;