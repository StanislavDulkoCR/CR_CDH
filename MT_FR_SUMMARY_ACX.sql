/***************************************************************************
*            (C) Copyright Acxiom Corporation 2006
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project:      Consumers Union Acxiom Database Solution - Middle Tier ETL
* Module Name:  middle_tier2.mt_fr_summary.sql
* Date:         3/30/2006
***************************************************************************/

create table temp_mt_fr_donation 
as
(select individual_id
            ,keycode
            ,don_dt
      from  middle_tier2.mt_fr_donation
      where don_amt > 0
        and don_cd = 'C3');

create table temp_mt_promotion 
as
select individual_id
            ,prom_dt
            ,keycode
            ,chnl_cd
            ,bus_unt 
     from  middle_tier2.mt_promotion
     where bus_unt = 'FUN';
     
CREATE TABLE mt_fs_don_temp
AS
SELECT
  fd.individual_id,
  round(avg(CASE WHEN fd.don_amt = 0
                 THEN null
                 ELSE fd.don_amt
            END),2) fr_avg_don_amt,
  round(avg(CASE WHEN fd.don_gross_amt = 0
                 THEN null
                 ELSE fd.don_gross_amt
            END),2) fr_avg_gross_don_amt,
  to_number(substr(max(CASE WHEN fd.don_amt > 0
                            THEN nvl(to_char(fd.don_dt,'YYYYMMDD'),'00010101') ||
                                 fd.don_amt
                            ELSE null
                       END),9)) fr_lst_don_amt,
  sum(fd.don_amt) fr_ltd_don_amt,
  max(fd.don_amt) fr_max_don_amt,
  sum(CASE WHEN fd.don_dt >= add_months(sysdate,-6)
           THEN fd.don_amt
           ELSE null
      END) fr_1_6_mth_don_amt,
  sum(CASE WHEN fd.don_dt >= add_months(sysdate,-24)
            AND fd.don_dt < add_months(sysdate,-12)
           THEN fd.don_amt
           ELSE null
      END) fr_13_24_mth_don_amt,
  sum(CASE WHEN fd.don_dt >= add_months(sysdate,-36)
            AND fd.don_dt < add_months(sysdate,-24)
           THEN fd.don_amt
           ELSE null
      END) fr_25_36_mth_don_amt,
  sum(CASE WHEN fd.don_dt < add_months(sysdate,-36)
           THEN fd.don_amt
           ELSE null
      END) fr_37_plus_mth_don_amt,
  sum(CASE WHEN fd.don_dt >= add_months(sysdate,-12)
            AND fd.don_dt < add_months(sysdate,-6)
           THEN fd.don_amt
           ELSE null
      END) fr_7_12_mth_don_amt,
  max(fd_amt_gt0.fr_prior_don_amt) fr_prior_don_amt,
  min(case when fd.don_amt > 0 then fd.don_dt end) fr_fst_don_dt,
  max(case when fd.don_amt > 0 then fd.don_dt end) fr_lst_don_dt,
  max(fd_amt_gt0.fr_prior_don_dt) fr_prior_don_dt,
  max(CASE WHEN fd.don_dt >= add_months(sysdate,-12)
            AND fd.don_amt > 0
           THEN 'Y'
           ELSE 'N'
      END) fr_dnr_act_flg,
  CASE WHEN max(CASE WHEN fd.don_amt > 0
                     THEN fd.don_dt
                     ELSE null
                END) < add_months(sysdate,-24)
       THEN 'Y'
       ELSE 'N'
  END fr_dnr_inact_flg,
  CASE WHEN max(CASE WHEN fd.don_amt > 0
                     THEN fd.don_dt
                     ELSE null
                END) BETWEEN add_months(sysdate,-24)
                         AND add_months(sysdate,-12)
       THEN 'Y'
       ELSE 'N'
  END fr_dnr_lps_flg,
  decode(max(CASE WHEN fd.don_dt > add_months(sysdate,-12)
                  THEN CASE WHEN fd.don_amt >= 1000
                            THEN '4'
                            WHEN fd.don_amt >= 500
                            THEN '3'
                            WHEN fd.don_amt >= 250
                            THEN '2'
                            WHEN fd.don_amt >= 100
                            THEN '1'
                            ELSE null
                       END
                  ELSE null
             END),
         '4','C',
         '3','P',
         '2','G',
         '1','S',
         null) fr_mbr_gvng_lvl_cd,
  add_months(
     to_date(
        substr(
           max(
               CASE WHEN fd.don_dt > add_months(sysdate,-12)
                     AND fd.don_amt >= 100
                    THEN lpad(to_char(trunc(100*fd.don_amt)),10,'0')||
                         to_char(fd.don_dt,'YYYYMMDD')
                    ELSE null
                END)
              ,11)
            ,'YYYYMMDD')
           ,12) fr_mbr_exp_dt,
  min(CASE WHEN nvl(fd.keycode,'0000000000') != '0000000000'
            AND fd.don_amt > 0
           THEN nvl(to_char(fd.don_dt,'YYYYMMDD'),'99999999') ||
                fd.keycode
           ELSE null
      END) fst_don_dt_keycode,
  max(CASE WHEN nvl(fd.keycode,'0000000000') != '0000000000'
            AND fd.don_amt > 0
           THEN nvl(to_char(fd.don_dt,'YYYYMMDD'),'00010101') ||
                fd.keycode
           ELSE null
      END) lst_don_dt_keycode,
  count(CASE WHEN fd.don_amt > 0
             THEN 1
             ELSE null
        END) fr_ltd_don_cnt,
  count(CASE WHEN fd.rf_amt > 0
             THEN 1
             ELSE null
        END) fr_don_ref_cnt,
  max(fd_amt_gt0.fr_times_ren_cnt) fr_times_ren_cnt,
  substr(max(CASE WHEN fd.don_amt > 0
                   AND nvl(fd.keycode,'0000000000') != '0000000000'
                  THEN nvl(to_char(fd.don_dt,'YYYYMMDD'),'00010101') || fd.keycode
                  ELSE null
              END),10,1) fr_lst_don_src_cd,
  substr(max(case when don_gross_amt > 0 and don_amt > 0 and substr(fd.keycode, 2, 1) in ('2','3','6') then
                      nvl(to_char(don_dt, 'YYYYMMDD'), '00010101') ||  don_amt
                    end) 
  , 9)
    as fr_lst_rfl_don_amt,
  substr(max(case when don_gross_amt > 0 and don_amt > 0 
                   and substr(fd.keycode, 2, 1) in ('0','1','4','5','7','8','9')
                   and fd.keycode != '0000000000' 
            then nvl(to_char(don_dt, 'YYYYMMDD'), '00010101') ||  don_amt
                 end)
  , 9)
    as fr_lst_non_rfl_don_amt,
  CASE WHEN max(don_dt) > add_months(sysdate,-36)
         THEN CASE WHEN sum(don_amt) between 225 and 999
                        or (sum(don_amt) between 150 and 225
                            and max(don_amt) >= 50)
                        or (sum(don_amt) between 125 and 225
                            and max(don_amt) >= 75)
                        or (sum(don_amt) between 100 and 225
                            and max(don_amt) >= 100)
                     THEN 'Y'
                   ELSE 'N'
              END
       ELSE 'N'
  END fr_tof_cd,
  CASE WHEN sum(case when don_dt <= to_date('20050531','YYYYMMDD')
                       then don_amt else 0 end) >= 1000
            and nvl(max(fm.flg),'X') != 'D' 
         THEN 'Y'
       WHEN max(fm.flg) = 'A' 
         THEN 'Y'
       ELSE 'N'
  END FR_LT_DNR_FLG,
  substr(min(case when fd.KEYCODE is not null and fd.KEYCODE != '0000000000' and DON_AMT > 0 
    then nvl(to_char(don_dt, 'YYYYMMDD'), '99999999') || fd.keycode
       end)
  , 9)
    as FR_FST_DON_KEYCODE,
  max(case when months_between(sysdate, fd.don_dt) <= 12 then fd.don_amt end) FR_MAX_DON_AMT_12_MTH,
  max(case when months_between(sysdate, fd.don_dt) <= 24 then fd.don_amt end) FR_MAX_DON_AMT_24_MTH,
  max(case when months_between(sysdate, fd.don_dt) <= 36 then fd.don_amt end) FR_MAX_DON_AMT_36_MTH,
  max(case when gk.keycode is not null then fd.don_dt end) FR_LST_GUARDIAN_DON_DT,
  case when max(case when months_between(sysdate, fd.don_dt) <= 36 then fd.don_amt end) between 50 and 99.99 
    then 'Y' else 'N' end FR_BRIDGE_PROGRAM_FLG,
  case when max(au.NON_PROM_CU_FR_RFL_FLG)  = 'Y' 
       then '1'
       when count(case when SUBSTR(fd.KEYCODE,2,1) in ('1','5','7') and months_between(sysdate, fd.don_dt) <= 36 and fd.DON_AMT > 0 then 1 end) > 0
         AND count(case when SUBSTR(fd.KEYCODE,2,1) in ('0','2','3','4','6','8','9') and months_between(sysdate, fd.don_dt) <= 36 
                   and fd.DON_AMT > 0 then 1 end) = 0
         AND max(fd_don_ind0.individual_id) is null
         AND months_between(sysdate, min(case when fd.don_amt > 0 then fd.don_dt end)) > 12
       then '2'
       when count(case when SUBSTR(fd.KEYCODE,2,1) in ('1','5','7') and months_between(sysdate, fd.don_dt) <= 36 and fd.DON_AMT > 0 then 1 end) >= 2
         OR count(case when SUBSTR(fd.KEYCODE,2,1) in ('1','7') and months_between(sysdate, fd.don_dt) <= 12 and fd.DON_AMT > 0 then 1 end) > 0
       then '3'
       when count(case when SUBSTR(fd.KEYCODE,2,1) in ('2','3') and months_between(sysdate, fd.don_dt) <= 36 and fd.DON_AMT > 0 then 1 end) > 0
         AND count(case when SUBSTR(fd.KEYCODE,2,1) in ('0','1','4','5','6','7','8','9') and months_between(sysdate, fd.don_dt) <= 36 
                   and fd.DON_AMT > 0 then 1 end) = 0
         AND months_between(sysdate, min(case when fd.don_amt > 0 then fd.don_dt end)) > 12
       then '4'
       when count(case when months_between(sysdate, fd.don_dt) <= 36 and fd.DON_AMT > 0 then 1 end) > 0
       then '5'
    end FR_TRACK_NUMBER,
  'qQ'     FR_CONV_TAG_RSP_FLG,
  to_number(substr(min( case when fd.don_amt > 0 
                             then nvl(to_char(fd.don_dt,'yyyymmdd'),'99991231') ||
                                 fd.don_amt end),9)) FR_FST_DON_AMT
FROM 
    (SELECT
        individual_id,
        don_dt,
        don_gross_amt,
        don_amt,
        keycode,
        rf_amt
      FROM middle_tier2.mt_fr_donation
      WHERE don_cd = 'C3' and
         (don_gross_amt !=0
         OR don_amt != 0)
    ) fd,
    (SELECT
        individual_id,
        max(CASE WHEN rec_nbr = 2
                 THEN don_amt
                 ELSE null
            END) fr_prior_don_amt,
        max(CASE WHEN rec_nbr = 2
                 THEN don_dt
                 ELSE null
            END) fr_prior_don_dt,
        trunc((max(don_dt) - nvl(max(don_brk),min(don_dt)))/365) fr_times_ren_cnt
      FROM
      ( SELECT
          individual_id,
          don_dt,
          don_amt,
          row_number() OVER (PARTITION BY individual_id ORDER BY don_dt DESC nulls last) rec_nbr,
          CASE WHEN (add_months(don_dt,-13) > lead(don_dt,1)
                                                   OVER (PARTITION BY individual_id
                                                         ORDER BY don_dt DESC nulls last))
               THEN don_dt
               ELSE null
          END don_brk
        FROM middle_tier2.mt_fr_donation
        WHERE don_cd = 'C3' and
           (don_gross_amt !=0
           OR don_amt != 0)
           AND don_amt > 0
      )
      group by individual_id
    ) fd_amt_gt0,
    (SELECT
        individual_id
      FROM middle_tier2.mt_fr_donation
      WHERE
        SUBSTR(KEYCODE,2,1) in ('2','3')
        AND months_between(sysdate, don_dt) <= 36
        AND don_ind = 0
      group by individual_id
    ) fd_don_ind0,
    -- (select mfd.individual_id
     -- from temp_mt_fr_donation         mfd
         -- ,temp_mt_promotion           mp
         -- ,fr_conv_tag_hist            fcth
     -- where 
       -- (
            -- mfd.individual_id = mp.individual_id
        -- and mp.prom_dt < to_date('20120601','yyyymmdd')
        -- and mp.keycode = fcth.keycode
        -- and mp.chnl_cd = fcth.chnl_cd
        -- and mfd.keycode = mp.keycode 
        -- and mfd.don_dt >= mp.prom_dt
       -- )
     -- union 
     -- select mfd.individual_id
     -- from temp_mt_fr_donation         mfd
         -- ,temp_mt_promotion           mp
         -- ,mt_target_analytics         mta
     -- where 
       -- (
        -- (    regexp_substr(mta.tag_scr, '[A-Za-z]+') = mta.tag_scr
         -- and mfd.individual_id = mta.individual_id
        -- )
        -- and
        -- (    mfd.individual_id = mp.individual_id 
         -- and mp.prom_dt >= to_date('20120601','yyyymmdd') 
         -- and 
         -- (
          -- (    mp.chnl_cd = 'D' 
           -- and substr(mfd.keycode,2,1) = '7'
          -- ) 
          -- or 
          -- (    mp.chnl_cd = 'E' 
           -- and substr(mp.keycode,2,1) = '0' 
           -- and substr(mp.keycode,3,1) = '6'
          -- )
         -- ) 
         -- and mfd.keycode = mp.keycode 
         -- and mfd.don_dt >= mp.prom_dt
        -- )
       -- )    
    -- group by mfd.individual_id)    fctr,
    MIDDLE_TIER2.FR_GUARDIAN_KEYCODES gk,
    middle_tier2.mt_auth_summary au,
    warehouse2.fr_membership fm
WHERE fd.individual_id = fm.individual_id (+)
  AND fd.individual_id = fd_amt_gt0.individual_id (+)
  AND fd.individual_id = fd_don_ind0.individual_id (+)
  AND fd.individual_id = au.individual_id (+)
  AND substr(fd.keycode, 1, 9) = gk.keycode (+)
  -- AND fd.individual_id = fctr.individual_id(+)
GROUP BY fd.individual_id
;


CREATE TABLE mt_fs_lt_pldg_temp AS
SELECT CAST( NULL AS NUMBER) AS individual_id 
  , 0                        AS fr_lst_lt_pldg_amt
  , 0                        AS lt_pldg_amt
  , 0                        AS lt_1_6_pldg_amt
  , 0                        AS lt_13_24_pldg_amt
  , 0                        AS lt_25_36_pldg_amt
  , 0                        AS lt_37_pldg_amt
  , 0                        AS lt_7_12_pldg_amt
  , CAST( NULL AS DATE )     AS fr_lst_lt_pldg_pmt_dt
  , CAST( NULL AS DATE )     AS fr_lst_lt_pldg_dt
  , CAST(NULL as varchar2(10) )    AS fst_lt_pldg_dt_keycode
  , cast( null as varchar2(10) )    AS lst_lt_pldg_dt_keycode
  , cast( null as varchar2(10) )    AS fr_lt_pldg_opn_flg
  , 0                        AS lt_pldg_cnt
  , CAST(NULL AS DATE)       AS fr_fst_lt_pldg_dt
FROM dual ;


CREATE TABLE mt_fs_prior_keycode_temp
AS
SELECT
  individual_id,
  keycode fr_prior_act_keycode
FROM (SELECT
        individual_id,
        keycode,
        row_number() OVER (PARTITION BY individual_id
                           ORDER BY don_dt DESC nulls last) rec_nbr
      FROM (SELECT
              individual_id,
              don_dt,
              keycode
            FROM middle_tier2.mt_fr_donation
            WHERE nvl(keycode,'0000000000') != '0000000000'
              AND don_amt > 0
              AND don_cd = 'C3'
            -- UNION ALL
            -- SELECT
            --   individual_id,
            --   pldg_dt don_dt,
            --   keycode
            -- FROM middle_tier2.mt_fr_pledge
            -- WHERE keycode is not null
            -- UNION ALL
            -- SELECT
            --   individual_id,
            --   pldg_dt don_dt,
            --   keycode
            -- FROM middle_tier2.mt_fr_lt_pledge
            -- WHERE keycode is not null)
            )
      )
WHERE rec_nbr = 2
;

create table fr_prom_temp
as               
select individual_id,
       keycode,
       prom_dt
from middle_tier2.mt_promotion
where bus_unt = 'FUN'
  and prom_dt > '31-MAY-2013' 
  and individual_id in (select distinct individual_id from middle_tier2.mt_fr_donation where don_dt > '31-MAY-2013')
  and SUBSTR(keycode,2,3) = '032';

CREATE TABLE mt_fr_summary_temp
AS
WITH fundraising_trans_starts AS
(SELECT
        individual_id,
        MAX (trn_dt) fr_ch_status_active_dt
        FROM (
           SELECT
           individual_id,
           trn_dt,
           CASE WHEN LAG(individual_id) OVER (ORDER BY individual_id, trn_dt) = individual_id THEN
              LAG(trn_dt) OVER (ORDER BY individual_id, trn_dt)
           ELSE
              NULL
           END AS prev_dt,
           CASE WHEN LEAD(individual_id) OVER (ORDER BY individual_id, trn_dt) = individual_id THEN
             LEAD(trn_dt) OVER (ORDER BY individual_id, trn_dt)
           ELSE
             NULL
           END AS next_dt
           FROM warehouse2.fundraising_trans
           WHERE
             trans_freq = 'R'
           ORDER BY individual_id, trn_dt
        ) WHERE (trn_dt >= ADD_MONTHS(prev_dt, 2) OR prev_dt IS NULL)
        AND (trn_dt >= ADD_MONTHS(next_dt, -2) OR next_dt IS NULL)
       GROUP BY individual_id
  )
SELECT
  ix.individual_id,
  ix.ind_urn,
  ix.hh_id,
  dn.fr_avg_don_amt,
  dn.fr_avg_gross_don_amt,
  dn.fr_lst_don_amt,
  dn.fr_ltd_don_amt,
  dn.fr_max_don_amt,
  dn.fr_1_6_mth_don_amt,
  dn.fr_13_24_mth_don_amt,
  dn.fr_25_36_mth_don_amt,
  dn.fr_37_plus_mth_don_amt,
  dn.fr_7_12_mth_don_amt,
  dn.fr_prior_don_amt,
  lt_pl.fr_lst_lt_pldg_amt,
  pldg.fr_lst_tm_pldg_amt,
  (nvl(pldg.pl_pldg_amt,0) + nvl(lt_pl.lt_pldg_amt,0)) fr_ltd_pldg_amt,
  (nvl(pldg.pl_1_6_pldg_amt,0) + nvl(lt_pl.lt_1_6_pldg_amt,0)) 
fr_1_6_mth_pldg_amt,
  (nvl(pldg.pl_13_24_pldg_amt,0) + nvl(lt_pl.lt_13_24_pldg_amt,0)) 
fr_13_24_mth_pldg_amt,
  (nvl(pldg.pl_25_36_pldg_amt,0) + nvl(lt_pl.lt_25_36_pldg_amt,0)) 
fr_25_36_mth_pldg_amt,
  (nvl(pldg.pl_37_pldg_amt,0) + nvl(lt_pl.lt_37_pldg_amt,0)) 
fr_37_plus_mth_pldg_amt,
  (nvl(pldg.pl_7_12_pldg_amt,0) + nvl(lt_pl.lt_7_12_pldg_amt,0)) 
fr_7_12_mth_pldg_amt,
  dn.fr_fst_don_dt,
  dn.fr_lst_don_dt,
  lt_pl.fr_lst_lt_pldg_pmt_dt,
  pldg.fr_lst_tm_pldg_dt,
  lt_pl.fr_lst_lt_pldg_dt,
  dn.fr_prior_don_dt,
  nvl2(lt_pl.individual_id,'Y','N') fr_lt_pldg_flg,
  dn.fr_dnr_act_flg,
  dn.fr_dnr_inact_flg,
  dn.fr_dnr_lps_flg,
  cast(null as varchar2(2)) fr_mbr_gvng_lvl_cd,
  cast(null as date) fr_mbr_exp_dt,
  substr(least(nvl(dn.fst_don_dt_keycode,'99999999'),
               nvl(pldg.fst_pldg_dt_keycode,'99999999'),
               nvl(lt_pl.fst_lt_pldg_dt_keycode,'99999999')),9) 
fr_fst_act_keycode,
  substr(greatest(nvl(dn.lst_don_dt_keycode,'00000000'),
                  nvl(pldg.lst_pldg_dt_keycode,'00000000'),
                  nvl(lt_pl.lst_lt_pldg_dt_keycode,'00000000')),9) 
fr_lst_act_keycode,
  pr_kc.fr_prior_act_keycode,
  lt_pl.fr_lt_pldg_opn_flg,
  dn.fr_ltd_don_cnt,
  dn.fr_don_ref_cnt,
  (nvl(pldg.pl_pldg_cnt,0) + nvl(lt_pl.lt_pldg_cnt,0)) fr_ltd_pldg_cnt,
  dn.fr_times_ren_cnt,
  dn.fr_lst_don_src_cd,
  CASE WHEN nvl(to_char(dn.fr_lst_don_dt,'YYYYMMDD'),'00010101') < to_char
(pldg.fr_lst_tm_pldg_dt,'YYYYMMDD')
       THEN 'Y'
       ELSE 'N'
  END fr_tm_pldg_opn_flg,
  pldg.fr_fst_pldg_dt,
  lt_pl.fr_fst_lt_pldg_dt,
  dn.fr_lst_rfl_don_amt,
  dn.fr_lst_non_rfl_don_amt ,
  cast(null as date) fr_mbr_frst_strt_dt,
  cast(null as varchar2(10)) fr_mbr_lst_keycode,
  cast(null as number(12,2)) fr_mbr_lst_ren_don_amt,
  nvl(dn.fr_tof_cd,'N') as fr_tof_cd,
  nvl(dn.fr_lt_dnr_flg,'N') as fr_lt_dnr_flg,
  cast(null as number(12,2)) as fr_mbr_lst_add_don_amt,
  cast(null as date) as fr_mbr_lst_add_don_dt,
  cast(null as date) as fr_mbr_pres_cir_frst_strt_dt,
  dn.FR_FST_DON_KEYCODE,
  dn.FR_MAX_DON_AMT_12_MTH,
  dn.FR_MAX_DON_AMT_24_MTH,
  dn.FR_MAX_DON_AMT_36_MTH,
  dn.FR_LST_GUARDIAN_DON_DT,
  dn.FR_BRIDGE_PROGRAM_FLG,
  dn.FR_TRACK_NUMBER,
  nvl(dn.FR_CONV_TAG_RSP_FLG,'N') FR_CONV_TAG_RSP_FLG,
  dn.FR_FST_DON_AMT,
  TO_DATE(fd.dt_lst_rfl_gift,'YYYYMMDD') as FR_LST_RFL_DON_DT,
  TO_DATE(fd.dt_lst_non_rfl_gift,'YYYYMMDD') as FR_LST_NON_RFL_DON_DT,
  fd.FR_LST_EML_DON_AMT,
  fd.FR_LST_EML_DON_DT,
  fd.fr_lst_dm_don_amt,
  fd.fr_lst_dm_don_dt,
  fd.fr_lst_org_onl_don_amt,
  fd.fr_lst_org_onl_don_dt,
  fd.fr_lst_ecomm_don_amt,
  fd.fr_lst_ecomm_don_dt,
  fd.fr_1_6_mth_don_cnt,
  fd.fr_7_12_mth_don_cnt,
  fd.fr_13_24_mth_don_cnt,
  fd.fr_25_36_mth_don_cnt,
  fd.fr_37_plus_mth_don_cnt,
  fd.fr_dm_pros_mdl_rsp_flg,
  fd.fr_mbr_basic_fst_dt,
  fd.fr_mbr_basic_fst_amt,
  fd.fr_mbr_basic_lst_dt,
  fd.fr_mbr_basic_lst_amt,
  fd.fr_mbr_basic_lst_keycode,
  fs.fr_ch_status,
  CASE WHEN fs.fr_ch_status = 'ACTIVE' THEN
    ctda.fr_ch_curr_ttl_don_amt
  ELSE
    NULL
  END fr_ch_curr_ttl_don_amt,
  ft.fr_ch_lst_don_dt,
  CASE WHEN fs.fr_ch_status = 'ACTIVE' THEN
    fts.fr_ch_status_active_dt
  ELSE
    NULL
  END fr_ch_curr_strt_dt,
  (SELECT
    MAX(ft3.trn_amt)
    FROM warehouse2.fundraising_trans ft3
    WHERE ft3.trn_dt = ft.fr_ch_lst_don_dt
    AND ft3.individual_id = ix.individual_id
  ) fr_ch_lst_don_amt,
  CASE WHEN fs.fr_ch_status = 'ACTIVE' THEN
    (SELECT
      COUNT(*)
      FROM warehouse2.fundraising_trans ft4
      WHERE ft4.individual_id = ix.individual_id
        AND ft4.fnc_cd = 'ANH'
        AND ft4.trans_freq = 'R'
        AND ft4.trn_dt >= fts.fr_ch_status_active_dt
    )
  ELSE
    NULL
  END fr_ch_curr_don_cnt,
  (SELECT
    MAX(ft5.apl_keycd)
    FROM warehouse2.fundraising_trans ft5
    WHERE ft5.trn_dt = ft.fr_ch_lst_don_dt
      AND ft5.individual_id = ix.individual_id
  ) fr_ch_lst_don_keycd,
  fts.fr_ch_status_active_dt,
  ix.osl_hh_id
FROM middle_tier2.ind_xref ix,
     mt_fs_don_temp dn,
     warehouse2.individual_xographic io,
     fundraising_trans_starts fts,
     (SELECT
        tft.individual_id,
        MAX(tft.trn_dt) fr_ch_lst_don_dt
        FROM warehouse2.fundraising_trans tft
        WHERE tft.trans_freq = 'R'
      GROUP BY tft.individual_id) ft,
     (SELECT
      ft.individual_id,
      SUM(ft.trn_amt) fr_ch_curr_ttl_don_amt
      FROM warehouse2.fundraising_trans ft,
           fundraising_trans_starts fts
      WHERE ft.trans_freq = 'R'
        AND ft.trn_dt >= fts.fr_ch_status_active_dt
        AND ft.individual_id = fts.individual_id (+)
      GROUP BY ft.individual_id
     ) ctda,
     (SELECT
        CAST( NULL AS NUMBER) as individual_id,
        0 fr_lst_tm_pldg_amt,
        0 pl_pldg_amt,
        0 pl_1_6_pldg_amt,
        0 pl_13_24_pldg_amt,
        0 pl_25_36_pldg_amt,
        0 pl_37_pldg_amt,
        0 pl_7_12_pldg_amt,
        CAST( NULL AS DATE ) fr_lst_tm_pldg_dt,
        CAST( NULL AS DATE ) fr_fst_pldg_dt,
        CAST(NULL as varchar2(10) ) fst_pldg_dt_keycode,
        CAST(NULL as varchar2(10) ) lst_pldg_dt_keycode,
        0 pl_pldg_cnt
      FROM dual) pldg,
     mt_fs_lt_pldg_temp lt_pl,
     mt_fs_prior_keycode_temp pr_kc,
     (SELECT fr.individual_id,
             MAX(case when DON_GROSS_AMT > 0
                       and SUBSTR(KEYCODE,2,1) in ('2','3','6') 
                      then NVL(TO_CHAR(don_dt,'YYYYMMDD'),'19000101')
                 end) AS DT_LST_RFL_GIFT,
             MAX(case when DON_GROSS_AMT > 0
                       and SUBSTR(KEYCODE,2,1) in ('0','1','4','5','7','8','9')
                       and KEYCODE != '0000000000'
                      then NVL(TO_CHAR(don_dt,'YYYYMMDD'),'19000101')
                 end) AS DT_LST_NON_RFL_GIFT,
             to_number(substr(max(case when substr(keycode,2,2) in ('03','06') 
              then nvl(to_char(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||don_amt                         
         end),15)) AS fr_lst_eml_don_amt,
             MAX(case when substr(keycode,2,2) in ('03','06') 
                      then don_dt
                 end) AS fr_lst_eml_don_dt,
             TO_NUMBER(SUBSTR(MAX(CASE WHEN    (SUBSTR(keycode,2,1) IN ('2','3','5','7','8','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'))
                                            OR (SUBSTR(keycode,2,1) = '4' AND SUBSTR(keycode,4,1) <> '2')
                                            OR (SUBSTR(keycode,2,1) = '4' AND SUBSTR(keycode,4,1) = '2' AND don_dt < '1-APR-2008')
                                            OR (SUBSTR(keycode,2,1) = '0' AND don_dt < '1-JAN-2002')
                                            OR (SUBSTR(keycode,2,1) = '1' AND don_dt >= '1-JUN-2012')
                                       THEN NVL(TO_CHAR(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||don_amt END),15)) fr_lst_dm_don_amt,
             MAX(CASE WHEN    (SUBSTR(keycode,2,1) IN ('2','3','5','7','8','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'))
                           OR (SUBSTR(keycode,2,1) = '4' AND SUBSTR(keycode,4,1) <> '2')
                           OR (SUBSTR(keycode,2,1) = '4' AND SUBSTR(keycode,4,1) = '2' AND don_dt < '1-APR-2008') 
                           OR (SUBSTR(keycode,2,1) = '0' AND don_dt < '1-JAN-2002')
                           OR (SUBSTR(keycode,2,1) = '1' AND don_dt >= '1-JUN-2012')
                      THEN don_dt END) fr_lst_dm_don_dt,
             TO_NUMBER(SUBSTR(MAX(CASE WHEN SUBSTR(keycode,2,2) IN ('02','00')
                                       THEN NVL(TO_CHAR(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||don_amt END),15)) fr_lst_org_onl_don_amt,
             MAX(CASE WHEN SUBSTR(keycode,2,2) IN ('02','00')
                      THEN don_dt END) fr_lst_org_onl_don_dt,
             TO_NUMBER(SUBSTR(MAX(CASE WHEN SUBSTR(keycode,2,2) = '05' AND don_dt >= '1-JAN-2013'
                                       THEN NVL(TO_CHAR(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||don_amt END),15)) fr_lst_ecomm_don_amt,
             MAX(CASE WHEN SUBSTR(keycode,2,2)  = '05' AND don_dt >= '1-JAN-2013'
                      THEN don_dt END) fr_lst_ecomm_don_dt,
             COUNT(CASE WHEN don_dt >= add_months(sysdate,-6) THEN 1 END) fr_1_6_mth_don_cnt,
             COUNT(CASE WHEN don_dt >= add_months(sysdate,-12) AND don_dt < add_months(sysdate,-6) THEN 1 END) fr_7_12_mth_don_cnt,
             COUNT(CASE WHEN don_dt >= add_months(sysdate,-24) AND don_dt < add_months(sysdate,-12) THEN 1 END) fr_13_24_mth_don_cnt,
             COUNT(CASE WHEN don_dt >= add_months(sysdate,-36) AND don_dt < add_months(sysdate,-24) THEN 1 END) fr_25_36_mth_don_cnt,
             COUNT(CASE WHEN don_dt < add_months(sysdate,-36) THEN 1 END) fr_37_plus_mth_don_cnt,
             MAX(CASE WHEN SUBSTR(keycode,2,1) = '1' AND don_dt >= '1-JUN-2012'
                      THEN 'Y'
                      ELSE 'N'
                  END)fr_dm_pros_mdl_rsp_flg,
             MIN(CASE WHEN  fr_pr.individual_id IS NOT NULL
                      THEN fr_pr.min_dt END) fr_mbr_basic_fst_dt,
             TO_NUMBER(SUBSTR(MIN(CASE WHEN fr_pr.individual_id IS NOT NULL
                                       THEN NVL(TO_CHAR(fr_pr.min_dt,'YYYYMMDDHH24MISS'),'00000000000000')||fr_pr.min_amt END),15)) fr_mbr_basic_fst_amt,
             MAX(CASE WHEN  fr_pr.individual_id IS NOT NULL
                      THEN fr_pr.max_dt END) fr_mbr_basic_lst_dt,
             TO_NUMBER(SUBSTR(MAX(CASE WHEN fr_pr.individual_id IS NOT NULL
                                       THEN NVL(TO_CHAR(fr_pr.max_dt,'YYYYMMDDHH24MISS'),'00000000000000')||fr_pr.max_amt END),15)) fr_mbr_basic_lst_amt,
             SUBSTR(MAX(CASE WHEN  fr_pr.individual_id IS NOT NULL
                                       THEN NVL(TO_CHAR(fr_pr.max_dt,'YYYYMMDDHH24MISS'),'00000000000000')||fr_pr.pr_key END),15) fr_mbr_basic_lst_keycode
      FROM middle_tier2.mt_fr_donation fr,
           (select fr.individual_id,
            SUBSTR(MAX(NVL(TO_CHAR(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||fr.keycode),15) pr_key,
            SUBSTR(MAX(NVL(TO_CHAR(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||fr.don_amt),15) max_amt,
            SUBSTR(MIN(NVL(TO_CHAR(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||fr.don_amt),15) min_amt,
            min(fr.don_dt) min_dt,
            max(fr.don_dt) max_dt
              from middle_tier2.mt_fr_donation fr,
                   fr_prom_temp pr
             where fr.individual_id = pr.individual_id (+)
               and fr.don_amt > 0
               and fr.don_cd = 'C3'
               and ((SUBSTR(fr.keycode,2,1) = '8' AND don_dt > '31-MAY-2009')
               OR   (SUBSTR(fr.keycode,2,3) = '032' AND don_dt > '31-MAY-2013')
               OR   (SUBSTR(fr.keycode,1,4) = '4062') 
               OR   ((SUBSTR(fr.keycode,2,2) = '02'
               and    fr.don_dt between pr.prom_dt + 6 and pr.prom_dt + 28)))
               group by fr.individual_id) fr_pr
      WHERE fr.individual_id = fr_pr.individual_id (+)
        AND DON_AMT > 0 
        AND don_cd = 'C3'
      GROUP BY fr.individual_id) fd,
     (
        SELECT
             io.individual_id,
             CASE WHEN io.fr_ch_status = 'C' THEN 'CANCELLED'
                WHEN io.fr_ch_status = 'A' AND ft.fr_ch_lst_don_dt > (sysdate - 31) 
             THEN 'ACTIVE'
                WHEN io.fr_ch_status = 'A' AND ft.fr_ch_lst_don_dt > (sysdate - 121) 
             THEN 'SUSPENDED'
                WHEN io.fr_ch_status = 'A' THEN 'EXPIRED' -- >= 122 days
             ELSE 'NONE'
             END fr_ch_status
        FROM
             warehouse2.individual_xographic io,
             (SELECT
                 tft.individual_id,
                 MAX(tft.trn_dt) fr_ch_lst_don_dt
                 FROM warehouse2.fundraising_trans tft
                 WHERE tft.trans_freq = 'R'
               GROUP BY tft.individual_id
             ) ft
        WHERE
             io.individual_id = ft.individual_id (+)
        ) fs
WHERE ix.individual_id = dn.individual_id(+)
  AND ix.individual_id = pldg.individual_id(+)
  AND ix.individual_id = lt_pl.individual_id(+)
  AND ix.individual_id = pr_kc.individual_id(+)
  AND ix.individual_id = fd.individual_id (+)
  AND ix.individual_id = io.individual_id (+)
  AND ix.individual_id = ft.individual_id (+)
  AND ix.individual_id = fts.individual_id (+)
  AND ix.individual_id = ctda.individual_id (+)
  AND ix.individual_id = fs.individual_id (+)
  AND ix.active_flag = 'A'
  AND nvl(dn.individual_id,
          nvl(pldg.individual_id,
              lt_pl.individual_id)) IS NOT NULL
;


-- ---ADD MEMBERSHIP DATA

-- DEFINE V_WAREHOUSE_SCHEMA = 'WAREHOUSE2'
-- DEFINE V_MIDDLE_TIER_SCHEMA = 'MIDDLE_TIER2'

-- DECLARE
-- --
-- cursor c_fr_donation is
-- select fs.rowid row_id,
--        fs.individual_id,
--        case when fd.don_dt >= to_date('20050601','YYYYMMDD')
--               then fd.don_dt
--        end don_dt,
--        case when fd.don_dt >=  greatest(to_date('20050601','YYYYMMDD'),nvl(pce.qualifying_date,to_date('20050601','YYYYMMDD')) )
--               then fd.don_amt
--        end don_amt,
--        case when fd.don_dt >= to_date('20050601','YYYYMMDD')
--               then fd.keycode
--        end keycode,
--        max(decode(fm.flg,'C',1,0))
--          over (partition by fs.individual_id) as flg_c_exists,
--        min(case when fd.don_amt >= 1000 then fd.don_dt end)
--          over (partition by fs.individual_id) as mbr_pres_cir_frst_strt_dt_tmp,
--       pce.individual_id pce_individual_id,
--       pce.qualifying_date pce_qualifying_date,
--       fs.fr_mbr_gvng_lvl_cd gvng_lvl_cd
-- from &V_MIDDLE_TIER_SCHEMA..mt_fr_summary_temp fs,
--      &V_MIDDLE_TIER_SCHEMA..mt_fr_donation fd,
--      &V_WAREHOUSE_SCHEMA..fr_membership fm,
--      &V_MIDDLE_TIER_SCHEMA..president_circle_exclude pce
-- where fs.individual_id = fd.individual_id
--       and fs.individual_id = fm.individual_id (+)
--       and fs.individual_id = pce.individual_id (+)
--       and ((fd.don_dt >= to_date('20050601','YYYYMMDD') 
--            and fd.don_cd = 'C3') or (fm.flg = 'C'))
-- order by individual_id, don_dt desc nulls last, keycode;
-- --
-- v_row c_fr_donation%rowtype;
-- --
-- row_count number;
-- ind_id number := 0;
-- v_last_upd_ind_id number :=0;
-- first_rec number := 1;
-- row_id rowid;
-- mbr_pres_cir_frst_strt_dt_tmp date;
-- pce_exclude_flg number := 0;
-- --
-- mbr_gvng_lvl_cd &V_MIDDLE_TIER_SCHEMA..mt_fr_summary_temp.fr_mbr_gvng_lvl_cd%type;
-- mbr_exp_dt &V_MIDDLE_TIER_SCHEMA..mt_fr_summary_temp.fr_mbr_exp_dt%type;
-- mbr_frst_strt_dt &V_MIDDLE_TIER_SCHEMA..mt_fr_summary_temp.fr_mbr_frst_strt_dt%type;
-- mbr_lst_keycode &V_MIDDLE_TIER_SCHEMA..mt_fr_summary_temp.fr_mbr_lst_keycode%type;
-- mbr_lst_ren_don_amt &V_MIDDLE_TIER_SCHEMA..mt_fr_summary_temp.fr_mbr_lst_ren_don_amt%type;
-- mbr_lst_add_don_amt &V_MIDDLE_TIER_SCHEMA..mt_fr_summary_temp.fr_mbr_lst_add_don_amt%type;
-- mbr_lst_add_don_dt &V_MIDDLE_TIER_SCHEMA..mt_fr_summary_temp.fr_mbr_lst_add_don_dt%type;
-- mbr_pres_cir_frst_strt_dt &V_MIDDLE_TIER_SCHEMA..mt_fr_summary_temp.fr_mbr_pres_cir_frst_strt_dt%type;
-- --
-- BEGIN
-- --
-- open c_fr_donation;
-- --
-- <<loop1>>
-- fetch c_fr_donation into v_row;
-- while not c_fr_donation%notfound loop
-- row_count := c_fr_donation%rowcount;
-- if mod(row_count,5000) = 0 then
--   commit;
-- end if;
-- --
-- if v_row.individual_id != ind_id or first_rec = 1 then
--   if first_rec = 0 and ind_id!=0 then
--     update &V_MIDDLE_TIER_SCHEMA..mt_fr_summary_temp
--     set  fr_mbr_gvng_lvl_cd = mbr_gvng_lvl_cd,
--          fr_mbr_exp_dt = mbr_exp_dt,
--          fr_mbr_frst_strt_dt = mbr_frst_strt_dt,
--          fr_mbr_lst_keycode = mbr_lst_keycode,
--          fr_mbr_lst_ren_don_amt = mbr_lst_ren_don_amt,
--          fr_mbr_lst_add_don_amt = mbr_lst_add_don_amt,
--          fr_mbr_lst_add_don_dt = mbr_lst_add_don_dt,
--          fr_mbr_pres_cir_frst_strt_dt = case when mbr_pres_cir_frst_strt_dt is not null
--                                              then mbr_pres_cir_frst_strt_dt
--                                              when mbr_gvng_lvl_cd = 'C'
--                                              then mbr_pres_cir_frst_strt_dt_tmp
--                                         end
--     where rowid = row_id;
--    ind_id:=0;
    
--   end if;
--   if v_row.gvng_lvl_cd = 'C' and  v_row.don_dt <= v_row.pce_qualifying_date then

--     mbr_gvng_lvl_cd := null;
--     mbr_exp_dt := to_date('20061231','YYYYMMDD');
--     mbr_frst_strt_dt := to_date('20050601','YYYYMMDD');
--     mbr_lst_keycode := null;
--     mbr_lst_ren_don_amt := null;
--     mbr_lst_add_don_amt := null;
--     mbr_lst_add_don_dt := null;
--     mbr_pres_cir_frst_strt_dt := to_date('20050601','YYYYMMDD');
-- --
--     row_id := v_row.row_id;
--     ind_id := v_row.individual_id;
--     mbr_pres_cir_frst_strt_dt_tmp := v_row.mbr_pres_cir_frst_strt_dt_tmp;
--     first_rec := 0;
--   elsif v_row.flg_c_exists = 1 and v_row.pce_individual_id is null then

--     mbr_gvng_lvl_cd := 'C'; 
--     mbr_exp_dt := to_date('20061231','YYYYMMDD');
--     mbr_frst_strt_dt := to_date('20050601','YYYYMMDD');
--     mbr_lst_keycode := null;
--     mbr_lst_ren_don_amt := null;
--     mbr_lst_add_don_amt := null;
--     mbr_lst_add_don_dt := null;
--     mbr_pres_cir_frst_strt_dt := to_date('20050601','YYYYMMDD');
-- --
--     row_id := v_row.row_id;
--     ind_id := v_row.individual_id;
--     mbr_pres_cir_frst_strt_dt_tmp := v_row.mbr_pres_cir_frst_strt_dt_tmp;
--     first_rec := 0;
--   elsif v_row.don_amt >= 100 then
--     mbr_gvng_lvl_cd := case when v_row.don_amt >= 100 and v_row.don_amt < 250
--                              then 'S'
--                            when v_row.don_amt >= 250 and v_row.don_amt < 500
--                              then 'G'
--                            when v_row.don_amt >= 500 and v_row.don_amt < 1000
--                              then 'P'
--                            when v_row.don_amt >= 1000
--                              then 'C'
--                       end;
--     mbr_exp_dt := case when v_row.don_dt <= to_date('20051231','YYYYMMDD')
--                         then to_date('20061231','YYYYMMDD')
--                       else last_day(add_months(v_row.don_dt,12))
--                   end;
--     mbr_frst_strt_dt := v_row.don_dt;
--     mbr_lst_keycode := v_row.keycode;
--     mbr_lst_ren_don_amt := null;
--     mbr_lst_add_don_amt := null;
--     mbr_lst_add_don_dt := null;
--     mbr_pres_cir_frst_strt_dt := null;
-- --
--     row_id := v_row.row_id;
--     ind_id := v_row.individual_id;
--     mbr_pres_cir_frst_strt_dt_tmp := v_row.mbr_pres_cir_frst_strt_dt_tmp;
--     first_rec := 0;
  
--     goto loop1;
--   else
--     goto loop1;
--   end if;
-- end if;
-- --
-- --3a. Renewal
-- if v_row.pce_qualifying_date is null or v_row.don_dt > v_row.pce_qualifying_date then
--     if v_row.don_dt <= mbr_exp_dt 
--          and ( (mbr_gvng_lvl_cd = 'S' and v_row.don_amt >= 100)
--                or (mbr_gvng_lvl_cd = 'G' and v_row.don_amt >= 250)
--                or (mbr_gvng_lvl_cd = 'P' and v_row.don_amt >= 500)
--                or (mbr_gvng_lvl_cd = 'C' and v_row.don_amt >= 1000) )
--          and ( (substr(v_row.keycode,2,3) = '414' and v_row.don_dt <= to_date('20061031','YYYYMMDD'))
--                 or (substr(v_row.keycode,2,2) in ('43', '47', '48'))
--                 or (substr(v_row.keycode,2,2) = '17' and v_row.don_dt
--                       between to_date('20061101','YYYYMMDD') and to_date('20080430','YYYYMMDD')) ) then
--       mbr_gvng_lvl_cd := case when v_row.don_amt >= 100 and v_row.don_amt < 250
--                                  then 'S'
--                                when v_row.don_amt >= 250 and v_row.don_amt < 500
--                                  then 'G'
--                                when v_row.don_amt >= 500 and v_row.don_amt < 1000
--                                  then 'P'
--                                when v_row.don_amt >= 1000
--                                  then 'C'
--                           end;
--       mbr_exp_dt := last_day(add_months(mbr_exp_dt,12));
--       mbr_lst_keycode := v_row.keycode;
--       mbr_lst_ren_don_amt := v_row.don_amt;
--     --3b. Renewal
--     elsif v_row.don_dt > add_months(mbr_exp_dt,-6) and v_row.don_dt <= mbr_exp_dt
--             and (substr(v_row.keycode,2,2) in ('47','48')
--                   or (substr(v_row.keycode,2,2) = '17' and v_row.don_dt
--                       between to_date('20061101','YYYYMMDD') and to_date('20080430','YYYYMMDD'))  )
--             and v_row.don_amt >= 100
--             and ( (mbr_gvng_lvl_cd = 'G' and v_row.don_amt < 250)
--                   or (mbr_gvng_lvl_cd = 'P' and v_row.don_amt < 500) ) then   
--       mbr_gvng_lvl_cd := case when v_row.don_amt >= 100 and v_row.don_amt < 250
--                                  then 'S'
--                                when v_row.don_amt >= 250 and v_row.don_amt < 500
--                                  then 'G'
--                                when v_row.don_amt >= 500 and v_row.don_amt < 1000
--                                  then 'P'
--                                when v_row.don_amt >= 1000
--                                  then 'C'
--                           end;
--       mbr_exp_dt := last_day(add_months(mbr_exp_dt,12));
--       mbr_lst_keycode := v_row.keycode;
--       mbr_lst_ren_don_amt := v_row.don_amt;
--     --3c. Renewal
--     elsif v_row.don_dt > add_months(mbr_exp_dt,-4) and v_row.don_dt <= mbr_exp_dt
--           and nvl(substr(v_row.keycode,2,1),'X') != '4'
--           and not (substr(v_row.keycode,2,2) = '17' and v_row.don_dt
--                       between to_date('20061101','YYYYMMDD') and to_date('20080430','YYYYMMDD'))
--           and ( (mbr_gvng_lvl_cd = 'S' and v_row.don_amt >= 100)
--                or (mbr_gvng_lvl_cd = 'G' and v_row.don_amt >= 250)
--                or (mbr_gvng_lvl_cd = 'P' and v_row.don_amt >= 500)
--                or (mbr_gvng_lvl_cd = 'C' and v_row.don_amt >= 1000) ) then
--       mbr_gvng_lvl_cd := case when v_row.don_amt >= 100 and v_row.don_amt < 250
--                                  then 'S'
--                                when v_row.don_amt >= 250 and v_row.don_amt < 500
--                                  then 'G'
--                                when v_row.don_amt >= 500 and v_row.don_amt < 1000
--                                  then 'P'
--                                when v_row.don_amt >= 1000
--                                  then 'C'
--                           end;
--       mbr_exp_dt := last_day(add_months(mbr_exp_dt,12));
--       mbr_lst_keycode := v_row.keycode;
--     --3d. Renewal
--     elsif v_row.don_dt <= mbr_exp_dt
--           and ( (mbr_gvng_lvl_cd = 'S' and v_row.don_amt >= 250)
--                 or (mbr_gvng_lvl_cd = 'G' and v_row.don_amt >= 500)
--                 or (mbr_gvng_lvl_cd = 'P' and v_row.don_amt >= 1000) ) then
                
--       mbr_gvng_lvl_cd := case when v_row.don_amt >= 100 and v_row.don_amt < 250
--                                  then 'S'
--                                when v_row.don_amt >= 250 and v_row.don_amt < 500
--                                  then 'G'
--                                when v_row.don_amt >= 500 and v_row.don_amt < 1000
--                                  then 'P'
--                                when v_row.don_amt >= 1000
--                                  then 'C'
--                           end;

--       mbr_exp_dt := last_day(add_months(mbr_exp_dt,12));
--       mbr_lst_keycode := v_row.keycode;
--       mbr_lst_ren_don_amt := case when (substr(v_row.keycode,2,3) = '414'
--                                           and v_row.don_dt <= to_date('20061031','YYYYMMDD'))
--                                        or (substr(v_row.keycode,2,2) in ('43','47','48'))
--                                        or (substr(v_row.keycode,2,2) = '17' and v_row.don_dt
--                                              between to_date('20061101','YYYYMMDD') and to_date('20080430','YYYYMMDD'))
--                                    then v_row.don_amt
--                                  else
--                                    mbr_lst_ren_don_amt
--                             end;
--     elsif v_row.don_dt > mbr_exp_dt
--           and v_row.don_amt >= 100
--           and ( (mbr_gvng_lvl_cd = 'C' and v_row.don_amt >= 1000)
--                 or mbr_gvng_lvl_cd != 'C') then
                
                
--       mbr_gvng_lvl_cd := case when v_row.don_amt >= 100 and v_row.don_amt < 250
--                                  then 'S'
--                                when v_row.don_amt >= 250 and v_row.don_amt < 500
--                                  then 'G'
--                                when v_row.don_amt >= 500 and v_row.don_amt < 1000
--                                  then 'P'
--                                when v_row.don_amt >= 1000
--                                  then 'C'
--                           end;
                           
--       mbr_exp_dt := last_day(add_months(v_row.don_dt,12));
--       mbr_lst_keycode := v_row.keycode;
--     elsif v_row.don_amt > 0 and nvl(substr(v_row.keycode,2,1),'X') not in ('2','3') then
--       mbr_lst_add_don_amt := v_row.don_amt;
--       mbr_lst_add_don_dt := v_row.don_dt;
--     end if;
-- end if;
-- --
-- fetch c_fr_donation into v_row;
-- --
-- end loop;
-- --
-- if first_rec = 0 and ind_id!=0 then
--   update &V_MIDDLE_TIER_SCHEMA..mt_fr_summary_temp
--   set  fr_mbr_gvng_lvl_cd = mbr_gvng_lvl_cd,
--        fr_mbr_exp_dt = mbr_exp_dt,
--        fr_mbr_frst_strt_dt = mbr_frst_strt_dt,
--        fr_mbr_lst_keycode = mbr_lst_keycode,
--        fr_mbr_lst_ren_don_amt = mbr_lst_ren_don_amt,
--        fr_mbr_lst_add_don_amt = mbr_lst_add_don_amt,
--        fr_mbr_lst_add_don_dt = mbr_lst_add_don_dt,
--        fr_mbr_pres_cir_frst_strt_dt = mbr_pres_cir_frst_strt_dt
--   where rowid = row_id;
-- end if;
-- --
-- commit;
-- --
-- close c_fr_donation;
-- --
-- END;

CREATE TABLE fr_sum_basic_temp
AS
select
individual_id,
case when substr(fr_mbr_basic_lst_keycode,2,2) not in ('88') and substr(fr_mbr_basic_lst_keycode,1,4) not in ('6808') and to_char(fr_mbr_basic_lst_dt,'MM') in ('01','02','03','04','05','06','07','08','09')
     then cast(DECODE(to_char(fr_mbr_basic_lst_dt,'YYYYMMDD'),NULL, NULL , to_date(to_char(fr_mbr_basic_lst_dt,'YYYY')||'0831','YYYYMMDD')) as DATE)
   else cast(DECODE(to_char(fr_mbr_basic_lst_dt,'YYYYMMDD'),NULL, NULL , to_date(to_char(fr_mbr_basic_lst_dt + 365,'YYYY')||'0831','YYYYMMDD')) as DATE) end as fr_mbr_basic_exp_dt
from mt_fr_summary_temp;

CREATE TABLE middle_tier2.mt_fr_summary_sep17
AS
SELECT
  fr.individual_id,
  fr.ind_urn,
  fr.hh_id,
  fr.fr_avg_don_amt,
  fr.fr_avg_gross_don_amt,
  fr.fr_lst_don_amt,
  fr.fr_ltd_don_amt,
  fr.fr_max_don_amt,
  fr.fr_1_6_mth_don_amt,
  fr.fr_13_24_mth_don_amt,
  fr.fr_25_36_mth_don_amt,
  fr.fr_37_plus_mth_don_amt,
  fr.fr_7_12_mth_don_amt,
  fr.fr_prior_don_amt,
  fr.fr_lst_lt_pldg_amt,
  fr.fr_lst_tm_pldg_amt,
  fr.fr_ltd_pldg_amt,
  fr.fr_1_6_mth_pldg_amt,
  fr.fr_13_24_mth_pldg_amt,
  fr.fr_25_36_mth_pldg_amt,
  fr.fr_37_plus_mth_pldg_amt,
  fr.fr_7_12_mth_pldg_amt,
  fr.fr_fst_don_dt,
  fr.fr_lst_don_dt,
  fr.fr_lst_lt_pldg_pmt_dt,
  fr.fr_lst_tm_pldg_dt,
  fr.fr_lst_lt_pldg_dt,
  fr.fr_prior_don_dt,
  fr.fr_lt_pldg_flg,
  fr.fr_dnr_act_flg,
  fr.fr_dnr_inact_flg,
  fr.fr_dnr_lps_flg,
  fr.fr_mbr_gvng_lvl_cd,
  fr.fr_mbr_exp_dt,
  fr.fr_fst_act_keycode,
  fr.fr_lst_act_keycode,
  fr.fr_prior_act_keycode,
  fr.fr_lt_pldg_opn_flg,
  fr.fr_ltd_don_cnt,
  fr.fr_don_ref_cnt,
  fr.fr_ltd_pldg_cnt,
  fr.fr_times_ren_cnt,
  fr.fr_lst_don_src_cd,
  fr.fr_tm_pldg_opn_flg,
  fr.fr_fst_pldg_dt,
  fr.fr_fst_lt_pldg_dt,
  fr.fr_lst_rfl_don_amt,
  fr.fr_lst_non_rfl_don_amt ,
  fr.fr_mbr_frst_strt_dt,
  fr.fr_mbr_lst_keycode,
  fr.fr_mbr_lst_ren_don_amt,
  fr.fr_tof_cd,
  fr.fr_lt_dnr_flg,
  fr.fr_mbr_lst_add_don_amt,
  fr.fr_mbr_lst_add_don_dt,
  fr.fr_mbr_pres_cir_frst_strt_dt,
  fr.FR_FST_DON_KEYCODE,
  fr.FR_MAX_DON_AMT_12_MTH,
  fr.FR_MAX_DON_AMT_24_MTH,
  fr.FR_MAX_DON_AMT_36_MTH,
  fr.FR_LST_GUARDIAN_DON_DT,
  fr.FR_BRIDGE_PROGRAM_FLG,
  fr.FR_TRACK_NUMBER,
  fr.FR_CONV_TAG_RSP_FLG,
  fr.FR_FST_DON_AMT,
  fr.FR_LST_RFL_DON_DT,
  fr.FR_LST_NON_RFL_DON_DT,
  fr.FR_LST_EML_DON_AMT,
  fr.FR_LST_EML_DON_DT,
  CASE WHEN ind.ind_fst_rel_dt >= '3-JAN-2013'
        AND ind.ind_fst_rel_dt < fr.fr_fst_don_dt
        AND fr.fr_fst_don_dt >= '3-JAN-2013'
        AND fr.fr_mbr_pres_cir_frst_strt_dt is null
                 THEN 'Y'
                 ELSE 'N'
            END AS FR_COOP_ELIGIBLE_FLG,
  fr.fr_lst_dm_don_amt,
  fr.fr_lst_dm_don_dt,
  fr.fr_lst_org_onl_don_amt,
  fr.fr_lst_org_onl_don_dt,
  fr.fr_lst_ecomm_don_amt,
  fr.fr_lst_ecomm_don_dt,
  fr.fr_1_6_mth_don_cnt,
  fr.fr_7_12_mth_don_cnt,
  fr.fr_13_24_mth_don_cnt,
  fr.fr_25_36_mth_don_cnt,
  fr.fr_37_plus_mth_don_cnt,
  fr.fr_dm_pros_mdl_rsp_flg,
  CASE WHEN fr.fr_mbr_gvng_lvl_cd = 'C'
       THEN 'C'
       WHEN fr.fr_mbr_gvng_lvl_cd IN ('S','G','P') AND fr.fr_mbr_exp_dt >= ADD_MONTHS(sysdate,-24)
       THEN 'L'
       WHEN cast(tmp.fr_mbr_basic_exp_dt as date) >= sysdate
       THEN 'B'
       ELSE 'N'
   END fr_mbr_comb_level,
  CASE WHEN fr.fr_mbr_gvng_lvl_cd = 'C'
       THEN TO_DATE('01/01/2050','MM/DD/YYYY')
       WHEN fr.fr_mbr_gvng_lvl_cd IN ('S','G','P') AND fr.fr_mbr_exp_dt >= ADD_MONTHS(sysdate,-24)
       THEN ADD_MONTHS(fr.fr_mbr_exp_dt,24)
       ELSE cast(tmp.fr_mbr_basic_exp_dt as date)
   END fr_mbr_comb_exp_dt,
  fr.fr_mbr_basic_fst_dt,
  fr.fr_mbr_basic_fst_amt,
  fr.fr_mbr_basic_lst_dt,
  fr.fr_mbr_basic_lst_amt,
  fr.fr_ch_status,
  fr.fr_ch_curr_ttl_don_amt,
  fr.fr_ch_lst_don_dt,
  fr.fr_ch_curr_strt_dt,
  fr.fr_ch_lst_don_amt,
  fr.fr_ch_curr_don_cnt,
  fr.fr_ch_lst_don_keycd,
  fr.fr_ch_status_active_dt,
  tmp.fr_mbr_basic_exp_dt,
  fr.fr_mbr_basic_lst_keycode,
  fr.osl_hh_id
FROM
mt_fr_summary_temp fr,
fr_sum_basic_temp tmp,
  middle_tier2.mt_individual ind
  WHERE fr.individual_id = ind.individual_id (+)
    and fr.individual_id = tmp.individual_id (+)
;