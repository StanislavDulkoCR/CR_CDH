/***************************************************************************
*            (C) Copyright Consumer Reports 2017
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  agg_fundraising_summary.sql
* Date       :  2017/10/09
* Dev & QA   :  Stanislav Dulko
***************************************************************************/

DROP TABLE IF EXISTS    agg.fundraising_donation_raw_temp;
CREATE TABLE            agg.fundraising_donation_raw_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
(select individual_id
            ,keycode
            ,don_dt
      from  prod.agg_fundraising_donation
      where don_amt > 0
        and don_cd = 'C3');


DROP TABLE IF EXISTS    agg.fundraising_promo_raw_temp;
CREATE TABLE            agg.fundraising_promo_raw_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
select individual_id
            ,prom_dt
            ,keycode
            ,chnl_cd
            ,bus_unt
     from  prod.agg_promotion
     where bus_unt = 'FUN';

DROP TABLE IF EXISTS    agg.fundraising_conv_tag_hist_temp;
CREATE TABLE            agg.fundraising_conv_tag_hist_temp
AS
with cte_fr_conv_tag_hist 
(      KEYCODE      , BUS_UNT , CHNL_CD)
as (
select '2030000406' , 'FUN'   , 'E' union all
select '2030000422' , 'FUN'   , 'E' union all
select '2032000248' , 'FUN'   , 'E' union all
select '2032000800' , 'FUN'   , 'E' union all
select '1781010028' , 'FUN'   , 'D' union all
select '1781020027' , 'FUN'   , 'D' union all
select '1781030026' , 'FUN'   , 'D' union all
select '1781040025' , 'FUN'   , 'D' union all
select '1781050024' , 'FUN'   , 'D' union all
select '1781060023' , 'FUN'   , 'D' union all
select '1781070022' , 'FUN'   , 'D' union all
select '1781080021' , 'FUN'   , 'D' union all
select '1781090020' , 'FUN'   , 'D' union all
select '2701010072' , 'FUN'   , 'D' union all
select '2701020089' , 'FUN'   , 'D' union all
select '2701030054' , 'FUN'   , 'D' union all
select '2701040061' , 'FUN'   , 'D' union all
select '2721010052' , 'FUN'   , 'D' union all
select '2721010144' , 'FUN'   , 'D' union all
select '2721020069' , 'FUN'   , 'D' union all
select '2721030076' , 'FUN'   , 'D' union all
select '2721040083' , 'FUN'   , 'D' union all
select '2721050090' , 'FUN'   , 'D' union all
select '2721060107' , 'FUN'   , 'D' union all
select '2721070114' , 'FUN'   , 'D' union all
select '2721080121' , 'FUN'   , 'D' union all
select '2721090138' , 'FUN'   , 'D' union all
select '2722010150' , 'FUN'   , 'D' union all
select '2722020159' , 'FUN'   , 'D' union all
select '2722030158' , 'FUN'   , 'D' union all
select '2722040157' , 'FUN'   , 'D' union all
select '2722050156' , 'FUN'   , 'D' union all
select '2722060155' , 'FUN'   , 'D' union all
select '2722070154' , 'FUN'   , 'D' union all
select '2722080153' , 'FUN'   , 'D' union all
select '2722090152' , 'FUN'   , 'D' union all
select '2723010159' , 'FUN'   , 'D' union all
select '2723020158' , 'FUN'   , 'D' union all
select '2723030157' , 'FUN'   , 'D' union all
select '2723040156' , 'FUN'   , 'D' union all
select '2723050155' , 'FUN'   , 'D' union all
select '2723060154' , 'FUN'   , 'D' union all
select '2723070153' , 'FUN'   , 'D' union all
select '2723080152' , 'FUN'   , 'D' union all
select '2723090151' , 'FUN'   , 'D' union all
select '2743010023' , 'FUN'   , 'D' union all
select '2743020030' , 'FUN'   , 'D' union all
select '2743030047' , 'FUN'   , 'D' union all
select '2743040053' , 'FUN'   , 'D' union all
select '2743050060' , 'FUN'   , 'D' union all
select '2743051068' , 'FUN'   , 'D' union all
select '2743052066' , 'FUN'   , 'D' union all
select '2743053064' , 'FUN'   , 'D' union all
select '2743054062' , 'FUN'   , 'D' union all
select '2743055069' , 'FUN'   , 'D' union all
select '2743056067' , 'FUN'   , 'D' union all
select '2743057065' , 'FUN'   , 'D' union all
select '2743058063' , 'FUN'   , 'D' union all
select '2743059061' , 'FUN'   , 'D' union all
select '2743060382' , 'FUN'   , 'D' union all
select '2743061067' , 'FUN'   , 'D' union all
select '2743062065' , 'FUN'   , 'D' union all
select '2743063063' , 'FUN'   , 'D' union all
select '2743064061' , 'FUN'   , 'D' union all
select '2743065068' , 'FUN'   , 'D' union all
select '2743070399' , 'FUN'   , 'D' union all
select '2781010117' , 'FUN'   , 'D' union all
select '2781020124' , 'FUN'   , 'D' union all
select '2781030131' , 'FUN'   , 'D' union all
select '2781040148' , 'FUN'   , 'D' union all
select '2781050154' , 'FUN'   , 'D' union all
select '2781060161' , 'FUN'   , 'D' union all
select '2781070087' , 'FUN'   , 'D' union all
select '2781080094' , 'FUN'   , 'D' union all
select '2781090101' , 'FUN'   , 'D' )

select KEYCODE, BUS_UNT, CHNL_CD 
from cte_fr_conv_tag_hist;

DROP TABLE IF EXISTS    agg.fundraising_don_agg1_temp;
CREATE TABLE            agg.fundraising_don_agg1_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
with fd as (SELECT
        individual_id,
        don_dt,
        don_gross_amt,
        don_amt,
        keycode,
        rf_amt
      FROM prod.agg_fundraising_donation
      WHERE don_cd = 'C3' and
         (don_gross_amt !=0
         OR don_amt != 0)
    )
,   fd_amt_gt0 as (SELECT
        individual_id,
        max(CASE WHEN rec_nbr = 2
                 THEN don_amt
                 ELSE null
            END) fr_prior_don_amt,
        max(CASE WHEN rec_nbr = 2
                 THEN don_dt
                 ELSE null
            END) fr_prior_don_dt,
        extract(days from ((max(don_dt) - nvl(max(don_brk),min(don_dt)))/365)) fr_times_ren_cnt
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
        FROM prod.agg_fundraising_donation
        WHERE don_cd = 'C3' and
           (don_gross_amt !=0
           OR don_amt != 0)
           AND don_amt > 0
      )
      group by individual_id
    )
, fd_don_ind0 as (SELECT
        individual_id
      FROM prod.agg_fundraising_donation
      WHERE
        SUBSTRING(KEYCODE,2,1) in ('2','3')
        AND months_between(sysdate, don_dt) <= 36
        AND don_ind = 0
      group by individual_id
    )
, fctr as (select mfd.individual_id
     from agg.fundraising_donation_raw_temp         mfd
         ,agg.fundraising_promo_raw_temp            mp
         ,agg.fundraising_conv_tag_hist_temp        fcth          
     where
       (
        mfd.individual_id = mp.individual_id
         and mp.prom_dt  <  to_date('20120601','yyyymmdd')
         and mp.keycode   = fcth.keycode
         and mp.chnl_cd   = fcth.chnl_cd
         and mfd.keycode  = mp.keycode
         and mfd.don_dt  >= mp.prom_dt
       )
     union
     select mfd.individual_id
     from agg.fundraising_donation_raw_temp         mfd
         ,agg.fundraising_promo_raw_temp            mp
         ,prod.agg_target_analytics                     mta
     where
       (
        (    REGEXP_SUBSTR(mta.tag_scr, '[A-Za-z]+') = mta.tag_scr
         and mfd.individual_id = mta.individual_id
        )
        and
        (    mfd.individual_id = mp.individual_id
         and mp.prom_dt >= to_date('20120601','yyyymmdd')
         and
         (
          (    mp.chnl_cd = 'D'
           and SUBSTRING(mfd.keycode,2,1) = '7'
          )
          or
          (    mp.chnl_cd = 'E'
           and SUBSTRING(mp.keycode,2,1) = '0'
           and SUBSTRING(mp.keycode,3,1) = '6'
          )
         )
         and mfd.keycode = mp.keycode
         and mfd.don_dt >= mp.prom_dt
        )
       )
    group by mfd.individual_id)

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
  (SUBSTRING(max(CASE WHEN fd.don_amt > 0
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
        SUBSTRING(
           max(
               CASE WHEN fd.don_dt > add_months(sysdate,-12)
                     AND fd.don_amt >= 100
                    THEN lpad(cast(trunc(100*fd.don_amt) as text),10,'0')||
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
  SUBSTRING(max(CASE WHEN fd.don_amt > 0
                   AND nvl(fd.keycode,'0000000000') != '0000000000'
                  THEN nvl(to_char(fd.don_dt,'YYYYMMDD'),'00010101') || fd.keycode
                  ELSE null
              END),10,1) fr_lst_don_src_cd,
  SUBSTRING(max(case when don_gross_amt > 0 and don_amt > 0 and SUBSTRING(fd.keycode, 2, 1) in ('2','3','6') then
                      nvl(to_char(don_dt, 'YYYYMMDD'), '00010101') ||  don_amt
                    end)
  , 9)
    as fr_lst_rfl_don_amt,
  SUBSTRING(max(case when don_gross_amt > 0 and don_amt > 0
                   and SUBSTRING(fd.keycode, 2, 1) in ('0','1','4','5','7','8','9')
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
            and nvl(max(fm.membership_flag),'X') != 'D'
         THEN 'Y'
       WHEN max(fm.membership_flag) = 'A'
         THEN 'Y'
       ELSE 'N'
  END FR_LT_DNR_FLG,
  SUBSTRING(min(case when fd.KEYCODE is not null and fd.KEYCODE != '0000000000' and DON_AMT > 0
    then nvl(to_char(don_dt, 'YYYYMMDD'), '99999999') || fd.keycode
       end)
  , 9)
    as FR_FST_DON_KEYCODE,
  max(case when months_between(sysdate, fd.don_dt) <= 12 then fd.don_amt end) FR_MAX_DON_AMT_12_MTH,
  max(case when months_between(sysdate, fd.don_dt) <= 24 then fd.don_amt end) FR_MAX_DON_AMT_24_MTH,
  max(case when months_between(sysdate, fd.don_dt) <= 36 then fd.don_amt end) FR_MAX_DON_AMT_36_MTH,
  NULL as FR_LST_GUARDIAN_DON_DT,
  case when max(case when months_between(sysdate, fd.don_dt) <= 36 then fd.don_amt end) between 50 and 99.99
    then 'Y' else 'N' end FR_BRIDGE_PROGRAM_FLG,
  case when max(au.NON_PROM_CU_FR_RFL_FLG)  = 'Y'
       then '1'
       when count(case when SUBSTRING(fd.KEYCODE,2,1) in ('1','5','7') and months_between(sysdate, fd.don_dt) <= 36 and fd.DON_AMT > 0 then 1 end) > 0
         AND count(case when SUBSTRING(fd.KEYCODE,2,1) in ('0','2','3','4','6','8','9') and months_between(sysdate, fd.don_dt) <= 36
                   and fd.DON_AMT > 0 then 1 end) = 0
         AND max(fd_don_ind0.individual_id) is null
         AND months_between(sysdate, min(case when fd.don_amt > 0 then fd.don_dt end)) > 12
       then '2'
       when count(case when SUBSTRING(fd.KEYCODE,2,1) in ('1','5','7') and months_between(sysdate, fd.don_dt) <= 36 and fd.DON_AMT > 0 then 1 end) >= 2
         OR count(case when SUBSTRING(fd.KEYCODE,2,1) in ('1','7') and months_between(sysdate, fd.don_dt) <= 12 and fd.DON_AMT > 0 then 1 end) > 0
       then '3'
       when count(case when SUBSTRING(fd.KEYCODE,2,1) in ('2','3') and months_between(sysdate, fd.don_dt) <= 36 and fd.DON_AMT > 0 then 1 end) > 0
         AND count(case when SUBSTRING(fd.KEYCODE,2,1) in ('0','1','4','5','6','7','8','9') and months_between(sysdate, fd.don_dt) <= 36
                   and fd.DON_AMT > 0 then 1 end) = 0
         AND months_between(sysdate, min(case when fd.don_amt > 0 then fd.don_dt end)) > 12
       then '4'
       when count(case when months_between(sysdate, fd.don_dt) <= 36 and fd.DON_AMT > 0 then 1 end) > 0
       then '5'
    end FR_TRACK_NUMBER,
  max(case when fctr.individual_id is null then 'N' else 'Y' end)     FR_CONV_TAG_RSP_FLG,
  SUBSTRING(min( case when fd.don_amt > 0
                             then nvl(to_char(fd.don_dt,'yyyymmdd'),'99991231') ||
                                 fd.don_amt end),9) FR_FST_DON_AMT
FROM
                                                                fd 
    left join prod.fundraising_membership                       fm 
        on fd.individual_id = fm.individual_id
    left join                                                   fd_amt_gt0
        on fd.individual_id = fd_amt_gt0.individual_id
    left join                                                   fd_don_ind0
        on fd.individual_id = fd_don_ind0.individual_id
    left join prod.agg_preference_summary                       au
        on fd.individual_id = au.individual_id
    left join                                                   fctr
        on fd.individual_id = fctr.individual_id
    /*left join FR_GUARDIAN_KEYCODES                            gk
        on SUBSTRING(fd.keycode, 1, 9) = gk.keycode  --SD: table is deprecated*/

GROUP BY fd.individual_id
;

DROP TABLE IF EXISTS    agg.mt_fs_lt_pldg_temp;
CREATE TABLE            agg.mt_fs_lt_pldg_temp AS
SELECT   null as individual_id                   --SD 20171006: Pledges are deprecated
       , 0    as fr_lst_lt_pldg_amt
       , 0    as lt_pldg_amt
       , 0    as lt_1_6_pldg_amt
       , 0    as lt_13_24_pldg_amt
       , 0    as lt_25_36_pldg_amt
       , 0    as lt_37_pldg_amt
       , 0    as lt_7_12_pldg_amt
       , null as fr_lst_lt_pldg_pmt_dt
       , null as fr_lst_lt_pldg_dt
       , null as fst_lt_pldg_dt_keycode
       , null as lst_lt_pldg_dt_keycode
       , null as fr_lt_pldg_opn_flg
       , 0    as lt_pldg_cnt
       , null as fr_fst_lt_pldg_dt
;


DROP TABLE IF EXISTS    agg.mt_fs_prior_keycode_temp;
CREATE TABLE            agg.mt_fs_prior_keycode_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
SELECT individual_id,
       keycode fr_prior_act_keycode
FROM
    (SELECT individual_id,
            keycode,
            row_number() OVER (PARTITION BY individual_id
                               ORDER BY don_dt DESC nulls LAST) rec_nbr
     FROM
         (SELECT individual_id,
                 don_dt,
                 keycode
          FROM prod.agg_fundraising_donation
          WHERE nvl(keycode,'0000000000') != '0000000000'
              AND don_amt > 0
              AND don_cd = 'C3'
          /*UNION ALL                      --SD 20171006: Pledges are deprecated
          SELECT
            individual_id,
            pldg_dt don_dt,
            keycode
          FROM mt_fr_pledge
          WHERE keycode is not null
          UNION ALL
          SELECT
            individual_id,
            pldg_dt don_dt,
            keycode
          FROM mt_fr_lt_pledge
          WHERE keycode is not null*/
 ))
WHERE rec_nbr = 2 ;

DROP TABLE IF EXISTS    agg.fundraising_promo_fun_temp;
CREATE TABLE            agg.fundraising_promo_fun_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
select individual_id,
       keycode,
       prom_dt
from prod.agg_promotion
where bus_unt = 'FUN'
  and prom_dt > '31-MAY-2013'
  and individual_id in (select distinct individual_id from prod.agg_fundraising_donation where don_dt > '31-MAY-2013')
  and substring(keycode,2,3) = '032';


DROP TABLE IF EXISTS    agg.fundraising_transaction_temp;
CREATE TABLE            agg.fundraising_transaction_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
select ah.individual_id as individual_id 
,   ah.action_amount    as trn_amt
,   ah.action_date      as trn_dt
,   ft.fnc_code         as fnc_cd
,   ah.action_frequency as trans_freq
,   ah.keycode          as apl_keycd

from prod.action_header ah inner join prod.fundraising_transaction ft on ah.hash_action_id = ft.hash_action_id
where 1=1 and action_type = 'FUNDRAISING';

DROP TABLE IF EXISTS    agg.fundraising_indx_temp;
CREATE TABLE            agg.fundraising_indx_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
select distinct individual_id     as individual_id 
            ,   null              as ind_urn 
            ,   household_id      as hh_id
            ,   household_id      as osl_hh_id
from prod.individual;



DROP TABLE IF EXISTS    agg.fundraising_trans_start_temp;
CREATE TABLE            agg.fundraising_trans_start_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
select    individual_id as individual_id 
        , MAX(trn_dt) fr_ch_status_active_dt
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
           FROM agg.fundraising_transaction_temp
           WHERE
             trans_freq = 'R'
           ORDER BY individual_id, trn_dt
        ) WHERE (trn_dt >= ADD_MONTHS(prev_dt, 2) OR prev_dt IS NULL)
        AND (trn_dt >= ADD_MONTHS(next_dt, -2) OR next_dt IS NULL)
       GROUP BY individual_id;

DROP TABLE IF EXISTS    agg.fundraising_last_don_dt_temp;
CREATE TABLE            agg.fundraising_last_don_dt_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
select  tft.individual_id
        , MAX(tft.trn_dt) fr_ch_lst_don_dt
        FROM  agg.fundraising_transaction_temp tft
        WHERE tft.trans_freq = 'R'
      GROUP BY tft.individual_id;

DROP TABLE IF EXISTS    agg.fundraising_donation_agg_temp;
CREATE TABLE            agg.fundraising_donation_agg_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
SELECT fr.individual_id,
             MAX(case when DON_GROSS_AMT > 0
                       and substring(KEYCODE,2,1) in ('2','3','6')
                      then NVL(TO_CHAR(don_dt,'YYYYMMDD'),'19000101')
                 end) AS DT_LST_RFL_GIFT,
             MAX(case when DON_GROSS_AMT > 0
                       and substring(KEYCODE,2,1) in ('0','1','4','5','7','8','9')
                       and KEYCODE != '0000000000'
                      then NVL(TO_CHAR(don_dt,'YYYYMMDD'),'19000101')
                 end) AS DT_LST_NON_RFL_GIFT,
             (substring(max(case when substring(keycode,2,2) in ('03','06')
              then nvl(to_char(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||don_amt
         end),15)) AS fr_lst_eml_don_amt,
             MAX(case when substring(keycode,2,2) in ('03','06')
                      then don_dt
                 end) AS fr_lst_eml_don_dt,
             (substring(MAX(CASE WHEN    (substring(keycode,2,1) IN ('2','3','5','7','8','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'))
                                            OR (substring(keycode,2,1) = '4' AND substring(keycode,4,1) <> '2')
                                            OR (substring(keycode,2,1) = '4' AND substring(keycode,4,1) = '2' AND don_dt < '1-APR-2008')
                                            OR (substring(keycode,2,1) = '0' AND don_dt < '1-JAN-2002')
                                            OR (substring(keycode,2,1) = '1' AND don_dt >= '1-JUN-2012')
                                       THEN NVL(TO_CHAR(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||don_amt END),15)) fr_lst_dm_don_amt,
             MAX(CASE WHEN    (substring(keycode,2,1) IN ('2','3','5','7','8','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'))
                           OR (substring(keycode,2,1) = '4' AND substring(keycode,4,1) <> '2')
                           OR (substring(keycode,2,1) = '4' AND substring(keycode,4,1) = '2' AND don_dt < '1-APR-2008')
                           OR (substring(keycode,2,1) = '0' AND don_dt < '1-JAN-2002')
                           OR (substring(keycode,2,1) = '1' AND don_dt >= '1-JUN-2012')
                      THEN don_dt END) fr_lst_dm_don_dt,
             (substring(MAX(CASE WHEN substring(keycode,2,2) IN ('02','00')
                                       THEN NVL(TO_CHAR(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||don_amt END),15)) fr_lst_org_onl_don_amt,
             MAX(CASE WHEN substring(keycode,2,2) IN ('02','00')
                      THEN don_dt END) fr_lst_org_onl_don_dt,
             (substring(MAX(CASE WHEN substring(keycode,2,2) = '05' AND don_dt >= '1-JAN-2013'
                                       THEN NVL(TO_CHAR(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||don_amt END),15)) fr_lst_ecomm_don_amt,
             MAX(CASE WHEN substring(keycode,2,2)  = '05' AND don_dt >= '1-JAN-2013'
                      THEN don_dt END) fr_lst_ecomm_don_dt,
             COUNT(CASE WHEN don_dt >= add_months(sysdate,-6) THEN 1 END) fr_1_6_mth_don_cnt,
             COUNT(CASE WHEN don_dt >= add_months(sysdate,-12) AND don_dt < add_months(sysdate,-6) THEN 1 END) fr_7_12_mth_don_cnt,
             COUNT(CASE WHEN don_dt >= add_months(sysdate,-24) AND don_dt < add_months(sysdate,-12) THEN 1 END) fr_13_24_mth_don_cnt,
             COUNT(CASE WHEN don_dt >= add_months(sysdate,-36) AND don_dt < add_months(sysdate,-24) THEN 1 END) fr_25_36_mth_don_cnt,
             COUNT(CASE WHEN don_dt < add_months(sysdate,-36) THEN 1 END) fr_37_plus_mth_don_cnt,
             MAX(CASE WHEN substring(keycode,2,1) = '1' AND don_dt >= '1-JUN-2012'
                      THEN 'Y'
                      ELSE 'N'
                  END)fr_dm_pros_mdl_rsp_flg,
             MIN(CASE WHEN  fr_pr.individual_id IS NOT NULL
                      THEN fr_pr.min_dt END) fr_mbr_basic_fst_dt,
             (substring(MIN(CASE WHEN fr_pr.individual_id IS NOT NULL
                                       THEN NVL(TO_CHAR(fr_pr.min_dt,'YYYYMMDDHH24MISS'),'00000000000000')||fr_pr.min_amt END),15)) fr_mbr_basic_fst_amt,
             MAX(CASE WHEN  fr_pr.individual_id IS NOT NULL
                      THEN fr_pr.max_dt END) fr_mbr_basic_lst_dt,
             (substring(MAX(CASE WHEN fr_pr.individual_id IS NOT NULL
                                       THEN NVL(TO_CHAR(fr_pr.max_dt,'YYYYMMDDHH24MISS'),'00000000000000')||fr_pr.max_amt END),15)) fr_mbr_basic_lst_amt,
             substring(MAX(CASE WHEN  fr_pr.individual_id IS NOT NULL
                                       THEN NVL(TO_CHAR(fr_pr.max_dt,'YYYYMMDDHH24MISS'),'00000000000000')||fr_pr.pr_key END),15) fr_mbr_basic_lst_keycode
      FROM prod.agg_fundraising_donation fr left join 
           (select fr.individual_id,
            substring(MAX(NVL(TO_CHAR(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||fr.keycode),15) pr_key,
            substring(MAX(NVL(TO_CHAR(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||fr.don_amt),15) max_amt,
            substring(MIN(NVL(TO_CHAR(don_dt,'YYYYMMDDHH24MISS'),'00000000000000')||fr.don_amt),15) min_amt,
            min(fr.don_dt) min_dt,
            max(fr.don_dt) max_dt
              from prod.agg_fundraising_donation fr left join agg.fundraising_promo_fun_temp pr on fr.individual_id = pr.individual_id
             where 1=1
               and fr.don_amt > 0
               and fr.don_cd = 'C3'
               and ((substring(fr.keycode,2,1)  = '8' AND don_dt > '31-MAY-2009')
               OR   (substring(fr.keycode,2,3)  = '032' AND don_dt > '31-MAY-2013')
               OR   (substring(fr.keycode,1,4)  = '4062')
               OR   ((substring(fr.keycode,2,2) = '02'
               and    fr.don_dt between pr.prom_dt + 6 and pr.prom_dt + 28)))
               group by fr.individual_id) fr_pr on  fr.individual_id = fr_pr.individual_id
      WHERE  1=1
        AND DON_AMT > 0
        AND don_cd = 'C3'
      GROUP BY fr.individual_id;

DROP TABLE IF EXISTS    agg.fundraising_xogr_status_temp;
CREATE TABLE            agg.fundraising_xogr_status_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
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
           prod.agg_individual_xographic io  left join 
           (SELECT
                 tft.individual_id,
                 MAX(tft.trn_dt) fr_ch_lst_don_dt
                 FROM agg.fundraising_transaction_temp tft
                 WHERE tft.trans_freq = 'R'
               GROUP BY tft.individual_id
             ) ft on io.individual_id = ft.individual_id;

DROP TABLE IF EXISTS    agg.fundraising_maxtranamt_temp;
CREATE TABLE            agg.fundraising_maxtranamt_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
SELECT ft3.individual_id
     , MAX(ft3.trn_amt) as trn_amt_max
FROM agg.fundraising_transaction_temp ft3
INNER JOIN agg.fundraising_last_don_dt_temp ft
ON  ft3.trn_dt            = ft.fr_ch_lst_don_dt
    AND ft3.individual_id = ft.individual_id
WHERE EXISTS (SELECT NULL FROM agg.fundraising_indx_temp ix WHERE ft.individual_id = ix.individual_id)
GROUP BY ft3.individual_id;


DROP TABLE IF EXISTS    agg.fundraising_cnt_anh_temp;
CREATE TABLE            agg.fundraising_cnt_anh_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
SELECT
    ft4.individual_id 
    , COUNT(*) as cnt_anh_rows
FROM agg.fundraising_transaction_temp ft4
    inner join agg.fundraising_trans_start_temp fts 
        on ft4.individual_id = fts.individual_id 
            and ft4.trn_dt  >= fts.fr_ch_status_active_dt
WHERE exists (select null from agg.fundraising_indx_temp ix where fts.individual_id = ix.individual_id )
    AND ft4.fnc_cd      = 'ANH'
    AND ft4.trans_freq  = 'R'   
group by ft4.individual_id ;

DROP TABLE IF EXISTS    agg.fundraising_maxdondt_keycd_temp;
CREATE TABLE            agg.fundraising_maxdondt_keycd_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
SELECT
    ft5.individual_id
,   MAX(ft5.apl_keycd) as apl_keycd_max
FROM agg.fundraising_transaction_temp ft5 
inner join agg.fundraising_last_don_dt_temp ft 
    on ft5.trn_dt = ft.fr_ch_lst_don_dt and ft5.individual_id = ft.individual_id
where exists (select null from agg.fundraising_indx_temp ix where ft.individual_id = ix.individual_id )
group by ft5.individual_id;


DROP TABLE IF EXISTS    agg.fundraising_ft3ft4ft5_temp;
CREATE TABLE            agg.fundraising_ft3ft4ft5_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
    select distinct
    /*SD: ft3 and ft4 and ft5 are group by from the same table*/
        ft3.individual_id as individual_id
    ,   ft3.trn_amt_max   as ft3_trn_amt_max
    ,   ft4.cnt_anh_rows  as ft4_cnt_anh_rows
    ,   ft5.apl_keycd_max as ft5_apl_keycd_max
    from agg.fundraising_maxtranamt_temp ft3 
        full join agg.fundraising_cnt_anh_temp ft4 
            on ft3.individual_id = ft4.individual_id
        full join agg.fundraising_maxdondt_keycd_temp ft5
            on ft4.individual_id = ft5.individual_id;


DROP TABLE IF EXISTS    agg.mt_fr_summary_temp;
CREATE TABLE            agg.mt_fr_summary_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
WITH ctda AS
    (SELECT ft.individual_id
      , SUM(ft.trn_amt) fr_ch_curr_ttl_don_amt
    FROM agg.fundraising_transaction_temp ft
    LEFT JOIN agg.fundraising_trans_start_temp fts
    ON  ft.individual_id  = fts.individual_id
    WHERE 1               =1
        AND ft.trans_freq = 'R'
        AND ft.trn_dt    >= fts.fr_ch_status_active_dt
    GROUP BY ft.individual_id
    )
  ,pldg AS
    (SELECT CAST( NULL AS INTEGER) AS individual_id
      , 0 fr_lst_tm_pldg_amt
      , 0 pl_pldg_amt
      , 0 pl_1_6_pldg_amt
      , 0 pl_13_24_pldg_amt
      , 0 pl_25_36_pldg_amt
      , 0 pl_37_pldg_amt
      , 0 pl_7_12_pldg_amt
      , CAST( NULL AS DATE ) fr_lst_tm_pldg_dt
      , CAST( NULL AS DATE ) fr_fst_pldg_dt
      , CAST( NULL AS VARCHAR(10) ) fst_pldg_dt_keycode
      , CAST( NULL AS VARCHAR(10) ) lst_pldg_dt_keycode
      , 0 pl_pldg_cnt
    )
SELECT ix.individual_id
  , ix.ind_urn
  , ix.hh_id
  , dn.fr_avg_don_amt
  , dn.fr_avg_gross_don_amt
  , dn.fr_lst_don_amt
  , dn.fr_ltd_don_amt
  , dn.fr_max_don_amt
  , dn.fr_1_6_mth_don_amt
  , dn.fr_13_24_mth_don_amt
  , dn.fr_25_36_mth_don_amt
  , dn.fr_37_plus_mth_don_amt
  , dn.fr_7_12_mth_don_amt
  , dn.fr_prior_don_amt
  , lt_pl.fr_lst_lt_pldg_amt
  , pldg.fr_lst_tm_pldg_amt
  , 0 fr_ltd_pldg_amt
  , 0 fr_1_6_mth_pldg_amt
  , 0 fr_13_24_mth_pldg_amt
  , 0 fr_25_36_mth_pldg_amt
  , 0 fr_37_plus_mth_pldg_amt
  , 0 fr_7_12_mth_pldg_amt
  , dn.fr_fst_don_dt
  , dn.fr_lst_don_dt
  , lt_pl.fr_lst_lt_pldg_pmt_dt
  , pldg.fr_lst_tm_pldg_dt
  , lt_pl.fr_lst_lt_pldg_dt
  , dn.fr_prior_don_dt
  , nvl2(lt_pl.individual_id,'Y','N') fr_lt_pldg_flg
  , dn.fr_dnr_act_flg
  , dn.fr_dnr_inact_flg
  , dn.fr_dnr_lps_flg
  , CAST(NULL AS VARCHAR(4)) fr_mbr_gvng_lvl_cd
  , CAST(NULL AS DATE) fr_mbr_exp_dt
  , substring(NVL(dn.fst_don_dt_keycode,'99999999'),9) fr_fst_act_keycode
  , substring(NVL(dn.lst_don_dt_keycode,'00000000'),9) fr_lst_act_keycode
  , pr_kc.fr_prior_act_keycode
  , lt_pl.fr_lt_pldg_opn_flg
  , dn.fr_ltd_don_cnt
  , dn.fr_don_ref_cnt
  , 0 fr_ltd_pldg_cnt
  , dn.fr_times_ren_cnt
  , dn.fr_lst_don_src_cd
  , null fr_tm_pldg_opn_flg
  , pldg.fr_fst_pldg_dt
  , lt_pl.fr_fst_lt_pldg_dt
  , dn.fr_lst_rfl_don_amt
  , dn.fr_lst_non_rfl_don_amt
  , CAST(NULL AS DATE) fr_mbr_frst_strt_dt
  , CAST(NULL AS VARCHAR(10)) fr_mbr_lst_keycode
  , CAST(NULL AS INTEGER) fr_mbr_lst_ren_don_amt
  , NVL(dn.fr_tof_cd,'N')     AS fr_tof_cd
  , NVL(dn.fr_lt_dnr_flg,'N') AS fr_lt_dnr_flg
  , CAST(NULL AS INTEGER)     AS fr_mbr_lst_add_don_amt
  , CAST(NULL AS DATE)        AS fr_mbr_lst_add_don_dt
  , CAST(NULL AS DATE)        AS fr_mbr_pres_cir_frst_strt_dt
  , dn.FR_FST_DON_KEYCODE
  , dn.FR_MAX_DON_AMT_12_MTH
  , dn.FR_MAX_DON_AMT_24_MTH
  , dn.FR_MAX_DON_AMT_36_MTH
  , dn.FR_LST_GUARDIAN_DON_DT
  , dn.FR_BRIDGE_PROGRAM_FLG
  , dn.FR_TRACK_NUMBER
  , NVL(dn.FR_CONV_TAG_RSP_FLG,'N') FR_CONV_TAG_RSP_FLG
  , dn.FR_FST_DON_AMT
  , TO_DATE(fd.dt_lst_rfl_gift,'YYYYMMDD')     AS FR_LST_RFL_DON_DT
  , TO_DATE(fd.dt_lst_non_rfl_gift,'YYYYMMDD') AS FR_LST_NON_RFL_DON_DT
  , fd.FR_LST_EML_DON_AMT
  , fd.FR_LST_EML_DON_DT
  , fd.fr_lst_dm_don_amt
  , fd.fr_lst_dm_don_dt
  , fd.fr_lst_org_onl_don_amt
  , fd.fr_lst_org_onl_don_dt
  , fd.fr_lst_ecomm_don_amt
  , fd.fr_lst_ecomm_don_dt
  , fd.fr_1_6_mth_don_cnt
  , fd.fr_7_12_mth_don_cnt
  , fd.fr_13_24_mth_don_cnt
  , fd.fr_25_36_mth_don_cnt
  , fd.fr_37_plus_mth_don_cnt
  , fd.fr_dm_pros_mdl_rsp_flg
  , fd.fr_mbr_basic_fst_dt
  , fd.fr_mbr_basic_fst_amt
  , fd.fr_mbr_basic_lst_dt
  , fd.fr_mbr_basic_lst_amt
  , fd.fr_mbr_basic_lst_keycode
  , fs.fr_ch_status
  , CASE WHEN fs.fr_ch_status = 'ACTIVE'
        THEN ctda.fr_ch_curr_ttl_don_amt
        ELSE NULL
    END fr_ch_curr_ttl_don_amt
  , ft.fr_ch_lst_don_dt
  , CASE WHEN fs.fr_ch_status = 'ACTIVE'
        THEN fts.fr_ch_status_active_dt
        ELSE NULL
    END fr_ch_curr_strt_dt
  , fta.ft3_trn_amt_max fr_ch_lst_don_amt
  , CASE WHEN fs.fr_ch_status = 'ACTIVE'
        THEN fta.ft4_cnt_anh_rows
        ELSE NULL
    END fr_ch_curr_don_cnt
  , fta.ft5_apl_keycd_max AS fr_ch_lst_don_keycd
  , fts.fr_ch_status_active_dt
  , ix.osl_hh_id
FROM agg.fundraising_indx_temp ix 
        left join agg.fundraising_don_agg1_temp         dn 
            on ix.individual_id = dn.individual_id
        left join                                           pldg 
            on ix.individual_id = pldg.individual_id
        left join agg.mt_fs_lt_pldg_temp                lt_pl 
            on ix.individual_id = lt_pl.individual_id
        left join agg.mt_fs_prior_keycode_temp          pr_kc 
            on ix.individual_id = pr_kc.individual_id
        left join agg.fundraising_donation_agg_temp     fd 
            on ix.individual_id = fd.individual_id
        left join prod.agg_individual_xographic             io 
            on ix.individual_id = io.individual_id
        left join agg.fundraising_last_don_dt_temp      ft 
            on ix.individual_id = ft.individual_id
        left join agg.fundraising_trans_start_temp      fts 
            on ix.individual_id = fts.individual_id
        left join                                           ctda 
            on ix.individual_id = ctda.individual_id
        left join agg.fundraising_xogr_status_temp      fs 
            on ix.individual_id = fs.individual_id
        left join agg.fundraising_ft3ft4ft5_temp        fta
            on ix.individual_id = fta.individual_id

WHERE dn.individual_id IS NOT NULL
;


DROP TABLE IF EXISTS    agg.fr_sum_basic_temp;
CREATE TABLE            agg.fr_sum_basic_temp
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
SELECT individual_id
  , CASE WHEN substring(fr_mbr_basic_lst_keycode,2,2) NOT   IN ('88')
            AND substring(fr_mbr_basic_lst_keycode,1,4) NOT IN ('6808')
            AND TO_CHAR(fr_mbr_basic_lst_dt,'MM')           IN ('01','02','03','04','05','06','07','08','09')
        THEN CAST(DECODE(TO_CHAR(fr_mbr_basic_lst_dt,'YYYYMMDD'),NULL, NULL , to_date(TO_CHAR(fr_mbr_basic_lst_dt,'YYYY')||'0831','YYYYMMDD')) AS
            DATE)
        ELSE CAST(DECODE(TO_CHAR(fr_mbr_basic_lst_dt,'YYYYMMDD'),NULL, NULL , to_date(TO_CHAR(fr_mbr_basic_lst_dt + 365,'YYYY')||'0831','YYYYMMDD'))
            AS DATE)
    END AS fr_mbr_basic_exp_dt
FROM agg.mt_fr_summary_temp;


DROP TABLE IF EXISTS    prod.agg_fundraising_summary;
CREATE TABLE            prod.agg_fundraising_summary
    DISTKEY(individual_id)
    sortkey(individual_id)
AS
SELECT fr.individual_id
  -- , fr.ind_urn
  , fr.hh_id
  , NVL(fr.fr_avg_don_amt,'0')          AS fr_avg_don_amt
  , NVL(fr.fr_avg_gross_don_amt,'0')    AS fr_avg_gross_don_amt
  , NVL(fr.fr_lst_don_amt,'0')          AS fr_lst_don_amt
  , NVL(fr.fr_ltd_don_amt,'0')          AS fr_ltd_don_amt
  , NVL(fr.fr_max_don_amt,'0')          AS fr_max_don_amt
  , NVL(fr.fr_1_6_mth_don_amt,'0')      AS fr_1_6_mth_don_amt
  , NVL(fr.fr_13_24_mth_don_amt,'0')    AS fr_13_24_mth_don_amt
  , NVL(fr.fr_25_36_mth_don_amt,'0')    AS fr_25_36_mth_don_amt
  , NVL(fr.fr_37_plus_mth_don_amt,'0')  AS fr_37_plus_mth_don_amt
  , NVL(fr.fr_7_12_mth_don_amt,'0')     AS fr_7_12_mth_don_amt
  , NVL(fr.fr_prior_don_amt,'0')        AS fr_prior_don_amt
  /*SD 20171009: all pledges are deprecated and dropped*/
  -- , NVL(fr.fr_lst_tm_pldg_amt,'0')      AS fr_lst_tm_pldg_amt
  -- , NVL(fr.fr_lst_lt_pldg_amt,'0')      AS fr_lst_lt_pldg_amt        
  -- , NVL(fr.fr_ltd_pldg_amt,'0')         AS fr_ltd_pldg_amt
  -- , NVL(fr.fr_1_6_mth_pldg_amt,'0')     AS fr_1_6_mth_pldg_amt
  -- , NVL(fr.fr_13_24_mth_pldg_amt,'0')   AS fr_13_24_mth_pldg_amt
  -- , NVL(fr.fr_25_36_mth_pldg_amt,'0')   AS fr_25_36_mth_pldg_amt
  -- , NVL(fr.fr_37_plus_mth_pldg_amt,'0') AS fr_37_plus_mth_pldg_amt
  -- , NVL(fr.fr_7_12_mth_pldg_amt,'0')    AS fr_7_12_mth_pldg_amt
  , fr.fr_fst_don_dt
  , fr.fr_lst_don_dt
  -- , fr.fr_lst_lt_pldg_pmt_dt
  -- , fr.fr_lst_tm_pldg_dt
  -- , fr.fr_lst_lt_pldg_dt
  , fr.fr_prior_don_dt
  -- , fr.fr_lt_pldg_flg
  , fr.fr_dnr_act_flg
  , fr.fr_dnr_inact_flg
  , fr.fr_dnr_lps_flg
  , fr.fr_mbr_gvng_lvl_cd
  , fr.fr_mbr_exp_dt
  , fr.fr_fst_act_keycode
  , fr.fr_lst_act_keycode
  , fr.fr_prior_act_keycode
  -- , fr.fr_lt_pldg_opn_flg
  , NVL(fr.fr_ltd_don_cnt,'0')          AS fr_ltd_don_cnt
  , NVL(fr.fr_don_ref_cnt,'0')          AS fr_don_ref_cnt
  -- , NVL(fr.fr_ltd_pldg_cnt,'0')         AS fr_ltd_pldg_cnt
  , NVL(fr.fr_times_ren_cnt,'0')        AS fr_times_ren_cnt
  , fr.fr_lst_don_src_cd
  -- , fr.fr_tm_pldg_opn_flg
  -- , fr.fr_fst_pldg_dt
  -- , fr.fr_fst_lt_pldg_dt
  , NVL(fr.fr_lst_rfl_don_amt,'0')      AS fr_lst_rfl_don_amt
  , NVL(fr.fr_lst_non_rfl_don_amt,'0')  AS fr_lst_non_rfl_don_amt
  , fr.fr_mbr_frst_strt_dt
  , fr.fr_mbr_lst_keycode
  , NVL(fr.fr_mbr_lst_ren_don_amt,'0')  AS fr_mbr_lst_ren_don_amt
  , fr.fr_tof_cd
  , fr.fr_lt_dnr_flg
  , NVL(fr.fr_mbr_lst_add_don_amt,'0')  AS fr_mbr_lst_add_don_amt
  , fr.fr_mbr_lst_add_don_dt
  , fr.fr_mbr_pres_cir_frst_strt_dt
  , fr.FR_FST_DON_KEYCODE
  , fr.FR_MAX_DON_AMT_12_MTH
  , fr.FR_MAX_DON_AMT_24_MTH
  , fr.FR_MAX_DON_AMT_36_MTH
  -- , fr.FR_LST_GUARDIAN_DON_DT
  -- , fr.FR_BRIDGE_PROGRAM_FLG
  , fr.FR_TRACK_NUMBER
  , fr.FR_CONV_TAG_RSP_FLG
  , fr.FR_FST_DON_AMT
  , fr.FR_LST_RFL_DON_DT
  , fr.FR_LST_NON_RFL_DON_DT
  , fr.FR_LST_EML_DON_AMT
  , fr.FR_LST_EML_DON_DT
  , CASE WHEN ind.ind_fst_rel_dt   >= '3-JAN-2013'
            AND ind.ind_fst_rel_dt <  fr.fr_fst_don_dt
            AND fr.fr_fst_don_dt   >= '3-JAN-2013'
            AND fr.fr_mbr_pres_cir_frst_strt_dt IS NULL
        THEN 'Y'
        ELSE 'N'
    END                                 AS FR_COOP_ELIGIBLE_FLG
  , NVL(fr.fr_lst_dm_don_amt,'0')       AS fr_lst_dm_don_amt
  , fr.fr_lst_dm_don_dt
  , NVL(fr.fr_lst_org_onl_don_amt,'0')  AS fr_lst_org_onl_don_amt
  , fr.fr_lst_org_onl_don_dt
  , NVL(fr.fr_lst_ecomm_don_amt,'0')    AS fr_lst_ecomm_don_amt
  , fr.fr_lst_ecomm_don_dt
  , NVL(fr.fr_1_6_mth_don_cnt,'0')      AS fr_1_6_mth_don_cnt
  , NVL(fr.fr_7_12_mth_don_cnt,'0')     AS fr_7_12_mth_don_cnt
  , NVL(fr.fr_13_24_mth_don_cnt,'0')    AS fr_13_24_mth_don_cnt
  , NVL(fr.fr_25_36_mth_don_cnt,'0')    AS fr_25_36_mth_don_cnt
  , NVL(fr.fr_37_plus_mth_don_cnt,'0')  AS fr_37_plus_mth_don_cnt
  , fr.fr_dm_pros_mdl_rsp_flg
  , CASE         WHEN fr.fr_mbr_gvng_lvl_cd   = 'C'
        THEN 'C' WHEN fr.fr_mbr_gvng_lvl_cd  IN ('S','G','P')
            AND fr.fr_mbr_exp_dt             >= ADD_MONTHS(sysdate,-24)
        THEN 'L' WHEN CAST(tmp.fr_mbr_basic_exp_dt AS DATE) >= sysdate
        THEN 'B'
        ELSE 'N'
    END fr_mbr_comb_level
  , CASE                                        WHEN fr.fr_mbr_gvng_lvl_cd  = 'C'
        THEN TO_DATE('01/01/2050','MM/DD/YYYY') WHEN fr.fr_mbr_gvng_lvl_cd  IN ('S','G','P')
            AND fr.fr_mbr_exp_dt  >= ADD_MONTHS(sysdate,-24)
        THEN ADD_MONTHS(fr.fr_mbr_exp_dt,24)
        ELSE CAST(tmp.fr_mbr_basic_exp_dt AS DATE)
    END fr_mbr_comb_exp_dt
  , fr.fr_mbr_basic_fst_dt
  , NVL(fr.fr_mbr_basic_fst_amt,'0')    AS fr_mbr_basic_fst_amt
  , fr.fr_mbr_basic_lst_dt
  , NVL(fr.fr_mbr_basic_lst_amt,'0')    AS fr_mbr_basic_lst_amt
  , fr.fr_ch_status
  , NVL(fr.fr_ch_curr_ttl_don_amt,'0')  AS fr_ch_curr_ttl_don_amt
  , fr.fr_ch_lst_don_dt
  , fr.fr_ch_curr_strt_dt
  , NVL(fr.fr_ch_lst_don_amt,'0')       AS fr_ch_lst_don_amt
  , NVL(fr.fr_ch_curr_don_cnt,'0')      AS fr_ch_curr_don_cnt
  , fr.fr_ch_lst_don_keycd
  , fr.fr_ch_status_active_dt
  , tmp.fr_mbr_basic_exp_dt
  , fr.fr_mbr_basic_lst_keycode
  , fr.osl_hh_id
FROM agg.mt_fr_summary_temp fr
    LEFT JOIN agg.fr_sum_basic_temp tmp
        ON  fr.individual_id = tmp.individual_id
    LEFT JOIN prod.agg_individual ind
        ON  fr.individual_id = ind.individual_id;
