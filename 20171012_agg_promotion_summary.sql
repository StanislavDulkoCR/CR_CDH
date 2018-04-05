/***************************************************************************
*            (C) Copyright Consumer Reports 2017
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  agg_promotion_summary.sql
* Date       :  2017/10/13
* Dev & QA   :  Stanislav Dulko
***************************************************************************/


SET wlm_query_slot_count = 5;

DROP TABLE IF EXISTS    agg.promsum_individual_temp;
CREATE TABLE            agg.promsum_individual_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(individual_id)
as
select distinct 
  ind.individual_id         as individual_id
, ind.household_id          as hh_id
, ind.household_id          as osl_hh_id
from PROD.individual ind
where INDIVIDUAL_ID is not null
and  exists (
            select null 
            from prod.agg_promotion ap 
            where ap.contact_type = 'P' 
              and ap.individual_id = ind.individual_id
            )   
;

analyse agg.promsum_individual_temp;

DROP TABLE IF EXISTS    agg.promsum_oli_summary_temp;
CREATE TABLE            agg.promsum_oli_summary_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(individual_id)
as
SELECT individual_id
  , MAX(
    CASE
        WHEN NVL(mag_cd,'x') != 'CRT'
        THEN crt_dt
    END)                                                as oli_crt_dt
  , MAX(
    CASE
        WHEN mag_cd IN ('NCBK','UCBK','NCPR','UCPR')
        THEN crt_dt
    END)                                                as oli_aps_slo_prom_dt
  , MAX(
    CASE
        WHEN mag_cd = 'CRMG'
        THEN crt_dt
        ELSE NULL
    END)                                                as oli_crmg_slo_prom_dt
  , MAX(
    CASE
        WHEN mag_cd = 'CRO'
        THEN crt_dt
    END)                                                as oli_cro_slo_prom_dt
  , MAX(crt_dt)                                         as oli_online_slo_prom_dt
FROM prod.agg_digital_item
WHERE NVL(set_cd,'x') NOT IN ('C','E')
GROUP BY individual_id;

analyse agg.promsum_oli_summary_temp;

DROP TABLE IF EXISTS    agg.promsum_ofo_summary_temp;
CREATE TABLE            agg.promsum_ofo_summary_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(individual_id)
as
SELECT individual_id
  , MAX(
    CASE
        WHEN NVL(mag_cd,'x') != 'CRT'
        THEN ord_dt
    END)                        as ofo_ord_dt
  , MAX(
    CASE
        WHEN mag_cd = 'CNS'
        THEN ord_dt
    END)                        as ofo_cr_slo_prom_dt
  , MAX(
    CASE
        WHEN mag_cd = 'CRH'
        THEN ord_dt
    END)                        as ofo_hl_slo_prom_dt
  , MAX(
    CASE
        WHEN mag_cd = 'CRM'
        THEN ord_dt
    END)                        as ofo_ma_slo_prom_dt
FROM prod.agg_print_order
WHERE NVL(set_cd,'x') NOT IN ('C','E')
GROUP BY individual_id;
      
analyse agg.promsum_ofo_summary_temp;

DROP TABLE IF EXISTS    agg.promsum_de_summary_temp;      
CREATE TABLE            agg.promsum_de_summary_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(individual_id)
as
SELECT individual_id
  , MAX(exp_date)                   as de_exp_dt
  , MAX(
    CASE
        WHEN magazine_code = 'CNS'
        THEN exp_date
  END)                              as de_cr_slo_prom_dt
  , MAX(
    CASE
        WHEN magazine_code = 'CRH'
        THEN exp_date
  END)                              as de_hl_slo_prom_dt
FROM prod.legacy_dead_expires
GROUP BY individual_id;

analyse agg.promsum_de_summary_temp;

DROP TABLE IF EXISTS    agg.promsum_promoresp_temp;
CREATE TABLE            agg.promsum_promoresp_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(individual_id)
as
select 
  pro.individual_id
, pro.prom_dt           as ph_prom_dt
, pro.chnl_cd           as ph_chnl_cd
, pro.keycode           as ph_keycode
, pro.bus_unt           as ph_bus_unt

from prod.agg_promotion pro
where pro.contact_type = 'P' 
and  not exists (   select null 
                    from prod.agg_promotion t1
                        inner join prod.agg_response_summary t2
                            on t1.individual_id = t2.individual_id
                                and t1.bus_unt      = t2.bus_unt
                                and t1.keycode      = t2.keycode
                    where 1=1
                        and t1.chnl_cd      = 'E'
                        and t1.contact_type = 'P'
                        and 'W' in (T2.ABQ_RESP_CD , T2.ADV_RESP_CD , T2.APS_RESP_CD , T2.FR_RESP_CD , T2.SUBS_RESP_CD , T2.CRSCH_RESP_CD )
                        /*SD: PK instead of ROWID*/
                        and (   pro.individual_id = t1.individual_id
                                and pro.bus_unt   = t1.bus_unt
                                and pro.keycode   = t1.keycode
                                and pro.prom_dt   = t1.prom_dt)
                )
;

analyse agg.promsum_promoresp_temp;

DROP TABLE IF EXISTS    agg.promsum_maxdt_temp;
CREATE TABLE            agg.promsum_maxdt_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(individual_id)
as
WITH cte_ar AS
    (SELECT individual_id
      , MAX(last_response_date)                             as ar_last_resp_dt
    FROM prod.legacy_constituent_response
    WHERE keycode IS NOT NULL
    GROUP BY individual_id
    )
  , cte_ad AS
    (SELECT individual_id
        , MAX(abq_yr)                                       as ad_abq_yr 
    FROM prod.agg_abq_donation 
    GROUP BY individual_id
    )
  , cte_abqr AS
    (SELECT individual_id
      , MAX(abq_yr)                                         as abqr_abq_yr
    FROM prod.agg_abq_response
    WHERE keycode IS NOT NULL
        AND mbr_flg = 'Y'
    GROUP BY individual_id
    )
  , cte_fd AS
    (SELECT individual_id
      , MAX(don_dt)                                         as fd_don_dt
    FROM prod.agg_fundraising_donation
    WHERE keycode IS NOT NULL
        AND don_amt > 0
    GROUP BY individual_id
    )
  , cte_fp AS
    (SELECT  /*SD 20171012 Pledges are deprecated*/
          CAST(NULL AS bigint)                             as individual_id
        , CAST(NULL AS date)                               as fp_pldg_dt
    )
  , cte_flp AS
    (SELECT 
          CAST(NULL AS bigint)                             as individual_id
        , CAST(NULL AS date)                               as flp_pldg_dt
    )
  , cte_bo AS   /*SD 20171012 Pledges are deprecated*/
    (SELECT individual_id
      , MAX(ord_dt)                                         as bo_ord_dt
    FROM prod.agg_books_order
    WHERE NVL(dnr_recip_cd,'x') != 'R'
    GROUP BY individual_id
    ) 


select 
  ix.individual_id
, ix.hh_id
, ix.osl_hh_id
, cte_ar.ar_last_resp_dt
, cte_ad.ad_abq_yr 
, cte_abqr.abqr_abq_yr
, cte_fd.fd_don_dt
, cte_fp.fp_pldg_dt
, cte_flp.flp_pldg_dt
, cte_bo.bo_ord_dt

from agg.promsum_individual_temp                    ix
    left join                                           cte_ar 
        on  ix.individual_id = cte_ar.individual_id     
    left join                                           cte_ad 
        on ix.individual_id = cte_ad.individual_id  
    left join                                           cte_abqr 
        on ix.individual_id = cte_abqr.individual_id     
    left join                                           cte_fd 
        on ix.individual_id = cte_fd.individual_id       
    left join                                           cte_fp 
        on ix.individual_id = cte_fp.individual_id       
    left join                                           cte_flp 
        on ix.individual_id = cte_flp.individual_id      
    left join                                           cte_bo 
        on ix.individual_id = cte_bo.individual_id   
;


analyse agg.promsum_maxdt_temp;

DROP TABLE IF EXISTS    agg.promsum_fullraw_temp;
CREATE TABLE            agg.promsum_fullraw_temp
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(individual_id)
as
select 

-- Max Dates temporary table 
  ixm.individual_id               as individual_id
, ixm.hh_id                       as hh_id
, ixm.osl_hh_id                   as osl_hh_id
, ixm.ar_last_resp_dt             as ar_last_resp_dt
, ixm.ad_abq_yr                   as ad_abq_yr
, ixm.abqr_abq_yr                 as abqr_abq_yr
, ixm.fd_don_dt                   as fd_don_dt
, ixm.fp_pldg_dt                  as fp_pldg_dt
, ixm.flp_pldg_dt                 as flp_pldg_dt
, ixm.bo_ord_dt                   as bo_ord_dt

-- Online Item / Digital Item
, oli.oli_crt_dt                  as oli_crt_dt
, oli.oli_aps_slo_prom_dt         as oli_aps_slo_prom_dt
, oli.oli_crmg_slo_prom_dt        as oli_crmg_slo_prom_dt
, oli.oli_cro_slo_prom_dt         as oli_cro_slo_prom_dt
, oli.oli_online_slo_prom_dt      as oli_online_slo_prom_dt

--Ofline Order / Print Order
, ofo.ofo_ord_dt                  as ofo_ord_dt
, ofo.ofo_cr_slo_prom_dt          as ofo_cr_slo_prom_dt
, ofo.ofo_hl_slo_prom_dt          as ofo_hl_slo_prom_dt
, ofo.ofo_ma_slo_prom_dt          as ofo_ma_slo_prom_dt

-- Dead Expires
, de.de_exp_dt                    as de_exp_dt
, de.de_cr_slo_prom_dt            as de_cr_slo_prom_dt
, de.de_hl_slo_prom_dt            as de_hl_slo_prom_dt

-- Individual Xographic
, ixo.dc_cdb_last_ord_dt          as ixo_dc_cdb_last_ord_dt

-- Promotion Summary
, ph.ph_prom_dt                   as ph_prom_dt
, ph.ph_chnl_cd                   as ph_chnl_cd
, ph.ph_keycode                   as ph_keycode
, ph.ph_bus_unt                   as ph_bus_unt


from agg.promsum_maxdt_temp                     ixm
    left join agg.promsum_oli_summary_temp      oli         
        on ixm.individual_id = oli.individual_id         
    left join agg.promsum_ofo_summary_temp      ofo         
        on ixm.individual_id = ofo.individual_id         
    left join agg.promsum_de_summary_temp       de          
        on ixm.individual_id = de.individual_id          
    left join prod.agg_individual_xographic         ixo         
        on ixm.individual_id = ixo.individual_id         
    left join agg.promsum_promoresp_temp        ph          
        on ixm.individual_id = ph.individual_id    
;

analyse agg.promsum_fullraw_temp;

DROP TABLE IF EXISTS    prod.agg_promotion_summary;
CREATE TABLE            prod.agg_promotion_summary
       DISTSTYLE KEY DISTKEY(individual_id)
       SORTKEY(individual_id)
as
SELECT 
  promo_temp.individual_id,
  promo_temp.hh_id,
  max(CASE WHEN promo_temp.ph_bus_unt = 'CNS'
           THEN promo_temp.ph_prom_dt
           ELSE null
      END) cr_lst_prom_dt,
  max(CASE WHEN promo_temp.ph_bus_unt IN ('MKT','PNL')
            AND promo_temp.ph_chnl_cd = 'E'
           THEN promo_temp.ph_prom_dt
           ELSE null
      END) crsch_em_lst_prom_dt,
  max(CASE WHEN promo_temp.ph_bus_unt = 'CRH'
           THEN promo_temp.ph_prom_dt
           ELSE null
      END) hl_lst_prom_dt,
  /*max(CASE WHEN promo_temp.ph_bus_unt = 'CRM'
           THEN promo_temp.ph_prom_dt
           ELSE null
      END) ma_lst_prom_dt,*/
  max(CASE WHEN promo_temp.ph_bus_unt = 'FUN'
            AND promo_temp.ph_chnl_cd = 'T'
           THEN promo_temp.ph_prom_dt
           ELSE null
      END) fr_tm_lst_prom_dt,      
  substring(max(CASE WHEN promo_temp.ph_bus_unt = 'ABQ'
                  THEN to_char(promo_temp.ph_prom_dt,'YYYYMMDD') ||
                       promo_temp.ph_keycode
                  ELSE null
             END),9) abq_lst_keycode,
  /*sum(case when promo_temp.ph_bus_unt = 'ABQ' and promo_temp.ph_chnl_cd = 'D' then 1 else 0 end) abq_dm_promo_cnt,*/
  /*sum(case when promo_temp.ph_bus_unt = 'ABQ' and promo_temp.ph_chnl_cd = 'E' then 1 else 0 end) abq_em_promo_cnt,*/
  max(case when promo_temp.ph_bus_unt = 'ADV' and promo_temp.ph_chnl_cd = 'E' then promo_temp.ph_prom_dt else null end) adv_em_lst_prom_dt,
  sum(case when promo_temp.ph_bus_unt = 'ADV' and promo_temp.ph_chnl_cd = 'E' then 1 else 0 end) adv_em_promo_cnt,  
  case when (sum(case when promo_temp.ph_bus_unt = 'ADV' and promo_temp.ph_chnl_cd = 'E' and promo_temp.ph_prom_dt > promo_temp.ar_last_resp_dt then 1 ELSE 0 end)) = 0 
          THEN (CASE WHEN max(promo_temp.ar_last_resp_dt) IS NOT NULL THEN 0 ELSE NULL END)
      ELSE sum(case when promo_temp.ph_bus_unt = 'ADV' and promo_temp.ph_chnl_cd = 'E' and promo_temp.ph_prom_dt > promo_temp.ar_last_resp_dt then 1 ELSE 0 end) end adv_sla_em_promo_cnt,
  max(case when promo_temp.ph_bus_unt in ('CNS','CRE','CRH','CRM','MGD','APS','NCB','UCB','NCR','UCR','BKS','SHM')
           then promo_temp.ph_prom_dt
      end) prod_lst_prom_dt,
  max(case when promo_temp.ph_bus_unt in ('ABQ','ADV','FUN','CGA','MKT','PNL')
           then promo_temp.ph_prom_dt
      end) non_prod_lst_prom_dt,
  /*max(case when promo_temp.ph_bus_unt in ('APS','NCR','UCR','NCB','UCB')
           then promo_temp.ph_prom_dt
      end) aps_lst_prom_dt,*/
  /*max(case when promo_temp.ph_bus_unt = 'MGD'
           then promo_temp.ph_prom_dt
      end) crmg_lst_prom_dt,*/
  max(case when promo_temp.ph_bus_unt = 'CRE'
           then promo_temp.ph_prom_dt
      end) cro_lst_prom_dt,
  /*max(case when promo_temp.ph_bus_unt = 'ABQ'
            and promo_temp.ph_chnl_cd = 'D'
           then promo_temp.ph_prom_dt
      end) abq_dm_lst_prom_dt,*/
  max(case when promo_temp.ph_bus_unt in ('CNS','CRE','CRH','CRM','MGD','APS','NCR','UCR','NCB','UCB','BKS','SHM')
            and promo_temp.ph_chnl_cd = 'D'
           then promo_temp.ph_prom_dt
      end) prod_dm_lst_prom_dt,
  max(case when promo_temp.ph_bus_unt = 'CNS'
            and promo_temp.ph_chnl_cd = 'D'
           then promo_temp.ph_prom_dt
      end) cr_dm_lst_prom_dt,
  max(case when promo_temp.ph_bus_unt = 'CRE'
            and promo_temp.ph_chnl_cd = 'D'
           then promo_temp.ph_prom_dt
      end) cro_dm_lst_prom_dt,
  max(case when promo_temp.ph_bus_unt = 'FUN'
            and promo_temp.ph_chnl_cd = 'D'
           then promo_temp.ph_prom_dt
      end) fr_dm_lst_prom_dt,
  max(case when promo_temp.ph_bus_unt = 'CRH'
            and promo_temp.ph_chnl_cd = 'D'
           then promo_temp.ph_prom_dt
      end) hl_dm_lst_prom_dt,
  max(case when promo_temp.ph_chnl_cd = 'D'
           then promo_temp.ph_prom_dt
      end) ind_dm_lst_prom_dt,
  /*max(case when promo_temp.ph_bus_unt = 'CRM'
            and promo_temp.ph_chnl_cd = 'D'
           then promo_temp.ph_prom_dt
      end) ma_dm_lst_prom_dt,*/
  /*max(case when promo_temp.ph_bus_unt = 'ABQ'
            and promo_temp.ph_chnl_cd = 'E'
           then promo_temp.ph_prom_dt
      end) abq_em_lst_prom_dt,*/
  max(case when promo_temp.ph_bus_unt in ('CNS','CRE','CRH','CRM','MGD','APS','NCR','UCR','NCB','UCB','BKS','SHM')
            and promo_temp.ph_chnl_cd = 'E'
           then promo_temp.ph_prom_dt
      end) prod_em_lst_prom_dt,
  max(case when promo_temp.ph_bus_unt = 'CNS'
            and promo_temp.ph_chnl_cd = 'E'
           then promo_temp.ph_prom_dt
      end) cr_em_lst_prom_dt,
  max(case when promo_temp.ph_bus_unt = 'CRE'
            and promo_temp.ph_chnl_cd = 'E'
           then promo_temp.ph_prom_dt
      end) cro_em_lst_prom_dt,
  max(case when promo_temp.ph_bus_unt = 'CRH'
            and promo_temp.ph_chnl_cd = 'E'
           then promo_temp.ph_prom_dt
      end) hl_em_lst_prom_dt,
  max(case when promo_temp.ph_chnl_cd = 'E'
           then promo_temp.ph_prom_dt
      end) ind_em_lst_prom_dt,
  /*max(case when promo_temp.ph_bus_unt = 'CRM'
            and promo_temp.ph_chnl_cd = 'E'
           then promo_temp.ph_prom_dt
      end) ma_em_lst_prom_dt,*/
  max(case when promo_temp.ph_bus_unt = 'FUN'
           then promo_temp.ph_prom_dt
      end) fr_lst_prom_dt,
  max(promo_temp.ph_prom_dt) ind_lst_prom_dt,
  /*max(case when promo_temp.ph_bus_unt = 'NCR'
           then promo_temp.ph_prom_dt
      end) nc_rpts_lst_prom_dt,*/
  /*max(case when promo_temp.ph_bus_unt = 'NCB'
           then promo_temp.ph_prom_dt
      end) ncbk_lst_prom_dt,*/
  /*max(case when promo_temp.ph_bus_unt = 'UCR'
           then promo_temp.ph_prom_dt
      end) uc_rpts_lst_prom_dt,*/
  /*max(case when promo_temp.ph_bus_unt = 'UCB'
           then promo_temp.ph_prom_dt
      end) ucbk_lst_prom_dt,*/
  count(case when promo_temp.ph_bus_unt in ('CNS','CRE','CRH','CRM','MGD','APS','NCR','UCR','NCB','UCB','BKS','SHM')
             then 1
        end) prod_prom_cnt,
  count(case when promo_temp.ph_bus_unt in ('ABQ','ADV','FUN','CGA','MKT','PNL')
             then 1
        end) non_prod_prom_cnt,
  /*count(case when promo_temp.ph_bus_unt in ( 'APS','NCR','UCR','NCB','UCB')
             then 1
        end) aps_prom_cnt,*/
  count(case when promo_temp.ph_bus_unt = 'CNS'
             then 1
        end) cr_prom_cnt,
  /*count(case when promo_temp.ph_bus_unt = 'MGD'
             then 1
        end) crmg_prom_cnt,*/
  count(case when promo_temp.ph_bus_unt = 'CRE'
             then 1
        end) cro_prom_cnt,
  count(case when promo_temp.ph_bus_unt in ('CNS','CRE','CRH','CRM','MGD','APS','NCR','UCR','NCB','UCB','BKS','SHM')
              and promo_temp.ph_chnl_cd = 'D'
             then 1
        end) prod_dm_prom_cnt,
  count(case when promo_temp.ph_bus_unt = 'CNS'
              and promo_temp.ph_chnl_cd = 'D'
             then 1
        end) cr_dm_prom_cnt,
  count(case when promo_temp.ph_bus_unt = 'CRE'
              and promo_temp.ph_chnl_cd = 'D'
             then 1
        end) cro_dm_prom_cnt,
  count(case when promo_temp.ph_bus_unt = 'FUN'
              and promo_temp.ph_chnl_cd = 'D'
             then 1
        end) fr_dm_prom_cnt,
  count(case when promo_temp.ph_bus_unt = 'CRH'
              and promo_temp.ph_chnl_cd = 'D'
             then 1
        end) hl_dm_prom_cnt,
  /*count(case when promo_temp.ph_bus_unt = 'CRM'
              and promo_temp.ph_chnl_cd = 'D'
             then 1
        end) ma_dm_prom_cnt,*/
  count(case when promo_temp.ph_bus_unt in ('CNS','CRE','CRH','CRM','MGD','APS','NCR','UCR','NCB','UCB','BKS','SHM')
              and promo_temp.ph_chnl_cd = 'E'
             then 1
        end) prod_em_prom_cnt,
  count(case when promo_temp.ph_bus_unt = 'CNS'
              and promo_temp.ph_chnl_cd = 'E'
             then 1
        end) cr_em_prom_cnt,
  count(case when promo_temp.ph_bus_unt = 'CRE'
              and promo_temp.ph_chnl_cd = 'E'
             then 1
        end) cro_em_prom_cnt,
  count(case when promo_temp.ph_bus_unt in ('MKT','PNL')
              and promo_temp.ph_chnl_cd = 'E'
             then 1
        end) crsch_em_prom_cnt,
  count(case when promo_temp.ph_bus_unt = 'FUN'
             then 1
        end) fr_prom_cnt,
  count(case when promo_temp.ph_bus_unt = 'CNS'
              and ((substring(promo_temp.ph_keycode,1,1) in ('G','U','X') and promo_temp.ph_keycode != 'UNKNOWN')
                   or substring(promo_temp.ph_keycode,1,2) = 'IG'
                   or (substring(promo_temp.ph_keycode,1,1) in ('J','K')
                       and substring(promo_temp.ph_keycode,7,1) in ('G','X')))
             then 1
        end) cr_dnr_prom_cnt,
  count(case when promo_temp.ph_bus_unt = 'CRE'
              and substring(promo_temp.ph_keycode,1,1) in ('G','U') and promo_temp.ph_keycode != 'UNKNOWN'           
             then 1
        end) cro_dnr_prom_cnt,
  count(case when promo_temp.ph_bus_unt = 'CRH'
             then 1
        end) hl_prom_cnt,
  count(promo_temp.ph_bus_unt) ind_prom_cnt,
  /*count(case when promo_temp.ph_bus_unt = 'CRM'
             then 1
        end) ma_prom_cnt,*/
  /*count(case when promo_temp.ph_bus_unt = 'NCR'
             then 1
        end) nc_rpts_prom_cnt,*/
  /*count(case when promo_temp.ph_bus_unt = 'NCB'
             then 1
        end) ncbk_prom_cnt,*/
   case WHEN (count(case when promo_temp.ph_bus_unt in ('ABQ','ADV','FUN','CGA','MKT','PNL')
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.ar_last_resp_dt,to_date('00010601','YYYYMMDD'))
                                         ,to_date(nvl(promo_temp.ad_abq_yr,'0001') || '0601','YYYYMMDD')
                                         ,to_date(nvl(promo_temp.abqr_abq_yr,'0001') || '0601','YYYYMMDD')
                                         ,nvl(promo_temp.fd_don_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.fp_pldg_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.flp_pldg_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end)) = 0 
         THEN (case WHEN ((count(promo_temp.ar_last_resp_dt) > 0) or
         (count(promo_temp.ad_abq_yr) > 0) or (count(promo_temp.abqr_abq_yr) > 0) or (count(promo_temp.fd_don_dt) > 0) or (count(promo_temp.fp_pldg_dt) > 0)
         or (count(promo_temp.flp_pldg_dt) > 0))
                   THEN 0 
                   ELSE NULL END)
         ELSE (count(case when promo_temp.ph_bus_unt in ('ABQ','ADV','FUN','CGA','MKT','PNL')
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.ar_last_resp_dt,to_date('00010601','YYYYMMDD'))
                                         ,to_date(nvl(promo_temp.ad_abq_yr,'0001') || '0601','YYYYMMDD')
                                         ,to_date(nvl(promo_temp.abqr_abq_yr,'0001') || '0601','YYYYMMDD')
                                         ,nvl(promo_temp.fd_don_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.fp_pldg_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.flp_pldg_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end)) END non_prod_sla_prom_cnt,
   CASE WHEN (count(case when promo_temp.ph_bus_unt = 'FUN'
               and promo_temp.ph_chnl_cd = 'D'
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.fd_don_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.fp_pldg_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.flp_pldg_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end)) = 0 
         THEN (CASE WHEN ((count(promo_temp.fd_don_dt) > 0) or (count(promo_temp.fp_pldg_dt) > 0) or (count(promo_temp.flp_pldg_dt) > 0))
                   THEN 0
                   ELSE NULL END)
         ELSE (count(case when promo_temp.ph_bus_unt = 'FUN'
               and promo_temp.ph_chnl_cd = 'D'
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.fd_don_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.fp_pldg_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.flp_pldg_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end)) END fr_sla_dm_prom_cnt,
   CASE WHEN (count(case when promo_temp.ph_bus_unt = 'FUN'
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.fd_don_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.fp_pldg_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.flp_pldg_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end)) = 0
         THEN (CASE WHEN ((count(promo_temp.fd_don_dt) > 0) or (count(promo_temp.fp_pldg_dt) > 0) or (count(promo_temp.flp_pldg_dt) > 0))
                   THEN 0
                   ELSE NULL END)
         ELSE (count(case when promo_temp.ph_bus_unt = 'FUN'
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.fd_don_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.fp_pldg_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.flp_pldg_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end)) END fr_sla_prom_cnt,
   CASE WHEN count(case when promo_temp.ph_bus_unt = 'FUN'
               and promo_temp.ph_chnl_cd = 'T'
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.fd_don_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(cast(promo_temp.fp_pldg_dt as date),to_date('00010601','YYYYMMDD'))
                                         ,nvl(cast(promo_temp.flp_pldg_dt as date),to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end) = 0 
         THEN (CASE WHEN ((count(promo_temp.fd_don_dt) > 0) or (count(promo_temp.fp_pldg_dt) > 0) or (count(promo_temp.flp_pldg_dt) > 0))
                   THEN 0
                   ELSE NULL END)
         ELSE count(case when promo_temp.ph_bus_unt = 'FUN'
               and promo_temp.ph_chnl_cd = 'T'
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.fd_don_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.fp_pldg_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.flp_pldg_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end) END fr_sla_tm_prom_cnt,
   CASE WHEN count(case when promo_temp.ph_bus_unt in ('CNS','CRE','CRH','CRM','MGD','APS','NCR','UCR','NCB','UCB','BKS','SHM')
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.ixo_dc_cdb_last_ord_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.bo_ord_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.ofo_ord_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.oli_crt_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.de_exp_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end) = 0 
         THEN (case WHEN ((count(promo_temp.ixo_dc_cdb_last_ord_dt) > 0) or
         (count(promo_temp.bo_ord_dt) > 0) or (count(promo_temp.ofo_ord_dt) > 0) or (count(promo_temp.oli_crt_dt) > 0) or (count(promo_temp.de_exp_dt) > 0))
                   THEN 0 
                   ELSE NULL END)
         ELSE count(case when promo_temp.ph_bus_unt in ('CNS','CRE','CRH','CRM','MGD','APS','NCR','UCR','NCB','UCB','BKS','SHM')
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.ixo_dc_cdb_last_ord_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.bo_ord_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.ofo_ord_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.oli_crt_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.de_exp_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end) END prod_slo_prom_cnt,
   /*CASE WHEN count(case when promo_temp.ph_bus_unt in ('APS','NCR','UCR','NCB','UCB')
               and promo_temp.ph_prom_dt > promo_temp.oli_aps_slo_prom_dt
              then 1
         end) = 0
         THEN (CASE WHEN (count(promo_temp.oli_aps_slo_prom_dt) > 0) 
                   THEN 0
                   ELSE NULL END)
         ELSE count(case when promo_temp.ph_bus_unt in ('APS','NCR','UCR','NCB','UCB')
               and promo_temp.ph_prom_dt > promo_temp.oli_aps_slo_prom_dt
              then 1
         end) END aps_slo_prom_cnt,        */
   CASE WHEN count(case when promo_temp.ph_bus_unt = 'CNS'
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.ofo_cr_slo_prom_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.de_cr_slo_prom_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end) = 0
         THEN (CASE WHEN ((count(promo_temp.ofo_cr_slo_prom_dt) > 0)
                                     OR (count(promo_temp.de_cr_slo_prom_dt) > 0))
                             THEN 0 
                             ELSE NULL END)
         ELSE count(case when promo_temp.ph_bus_unt = 'CNS'
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.ofo_cr_slo_prom_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.de_cr_slo_prom_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end) END cr_slo_prom_cnt,     
   /*CASE WHEN count(case when promo_temp.ph_bus_unt = 'MGD'
               and promo_temp.ph_prom_dt > promo_temp.oli_crmg_slo_prom_dt
              then 1
         end) = 0 
         THEN (CASE WHEN (count(promo_temp.oli_crmg_slo_prom_dt) > 0) 
                   THEN 0
                   ELSE NULL END)
         ELSE count(case when promo_temp.ph_bus_unt = 'MGD'
               and promo_temp.ph_prom_dt > promo_temp.oli_crmg_slo_prom_dt
              then 1
         end) END crmg_slo_prom_cnt,*/
   CASE WHEN count(case when promo_temp.ph_bus_unt = 'CRE'
               and promo_temp.ph_prom_dt > promo_temp.oli_cro_slo_prom_dt
              then 1
         end) = 0
         THEN (CASE WHEN (count(promo_temp.oli_cro_slo_prom_dt) > 0) 
                   THEN 0
                   ELSE NULL END)
         ELSE count(case when promo_temp.ph_bus_unt = 'CRE'
               and promo_temp.ph_prom_dt > promo_temp.oli_cro_slo_prom_dt
              then 1
         end) END cro_slo_prom_cnt,
   CASE WHEN count(case when promo_temp.ph_bus_unt = 'CNS'
               and promo_temp.ph_chnl_cd = 'D'
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.ofo_cr_slo_prom_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.de_cr_slo_prom_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end) = 0
         THEN (CASE WHEN ((count(promo_temp.ofo_cr_slo_prom_dt) > 0) OR (COUNT(promo_temp.de_cr_slo_prom_dt) > 0)) THEN 0 ELSE NULL END)
         ELSE count(case when promo_temp.ph_bus_unt = 'CNS'
               and promo_temp.ph_chnl_cd = 'D'
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.ofo_cr_slo_prom_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.de_cr_slo_prom_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end) END cr_slo_dm_prom_cnt,  
  CASE WHEN count(case when promo_temp.ph_bus_unt = 'CRE'
              and promo_temp.ph_chnl_cd = 'D'
              and promo_temp.ph_prom_dt > promo_temp.oli_cro_slo_prom_dt
             then 1
        end) = 0
        THEN (CASE WHEN (count(promo_temp.oli_cro_slo_prom_dt) > 0) THEN 0 ELSE NULL END)
        ELSE count(case when promo_temp.ph_bus_unt = 'CRE'
              and promo_temp.ph_chnl_cd = 'D'
              and promo_temp.ph_prom_dt > promo_temp.oli_cro_slo_prom_dt
             then 1
        end) END cro_slo_dm_prom_cnt,      
  CASE WHEN count(case when promo_temp.ph_bus_unt = 'CRE'
              and promo_temp.ph_chnl_cd = 'E'
              and promo_temp.ph_prom_dt > promo_temp.oli_cro_slo_prom_dt
             then 1
        end) = 0
        THEN (CASE WHEN (count(promo_temp.oli_cro_slo_prom_dt) > 0) THEN 0 ELSE NULL END)
        ELSE count(case when promo_temp.ph_bus_unt = 'CRE'
              and promo_temp.ph_chnl_cd = 'E'
              and promo_temp.ph_prom_dt > promo_temp.oli_cro_slo_prom_dt
             then 1
        end) END cro_slo_em_prom_cnt,   
   CASE WHEN count(case when promo_temp.ph_bus_unt = 'CRH'
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.ofo_hl_slo_prom_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.de_hl_slo_prom_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end) = 0
         THEN (CASE WHEN ((count(promo_temp.ofo_hl_slo_prom_dt) > 0) OR (count(promo_temp.de_hl_slo_prom_dt) > 0)) THEN 0 ELSE NULL END)
         ELSE count(case when promo_temp.ph_bus_unt = 'CRH'
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.ofo_hl_slo_prom_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.de_hl_slo_prom_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end) END hl_slo_prom_cnt,
   /*CASE WHEN count(case when promo_temp.ph_bus_unt = 'CRM'
               and promo_temp.ph_prom_dt > promo_temp.ofo_ma_slo_prom_dt
              then 1
         end) = 0
         THEN (CASE WHEN (count(promo_temp.ofo_ma_slo_prom_dt) > 0) THEN 0 ELSE NULL END)
         ELSE count(case when promo_temp.ph_bus_unt = 'CRM'
               and promo_temp.ph_prom_dt > promo_temp.ofo_ma_slo_prom_dt
              then 1
         end) END ma_slo_prom_cnt,*/
   CASE WHEN count(case when promo_temp.ph_bus_unt in ('CNS','CRM','CRH','BKS','SHM')
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.ofo_ord_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.de_exp_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.ixo_dc_cdb_last_ord_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.bo_ord_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end) = 0
         THEN (CASE WHEN ((count(promo_temp.ofo_ord_dt) > 0) or (count(promo_temp.de_exp_dt) > 0) or (count(promo_temp.ixo_dc_cdb_last_ord_dt) > 0) or
         (count(promo_temp.bo_ord_dt) > 0))
                   THEN 0
                   ELSE NULL END)
         ELSE count(case when promo_temp.ph_bus_unt in ('CNS','CRM','CRH','BKS','SHM')
               and promo_temp.ph_prom_dt > nullif(greatest(nvl(promo_temp.ofo_ord_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.de_exp_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.ixo_dc_cdb_last_ord_dt,to_date('00010601','YYYYMMDD'))
                                         ,nvl(promo_temp.bo_ord_dt,to_date('00010601','YYYYMMDD'))),to_date('00010601','YYYYMMDD'))
              then 1
         end) END offline_slo_prom_cnt,  
  CASE WHEN count(case when promo_temp.ph_bus_unt in ('CRE','MGD','APS','NCR','UCR','NCB','UCB')
              and promo_temp.ph_prom_dt > promo_temp.oli_online_slo_prom_dt
             then 1
        end) = 0 
        THEN (CASE WHEN (count(promo_temp.oli_online_slo_prom_dt) > 0) THEN 0 ELSE NULL END)
        ELSE count(case when promo_temp.ph_bus_unt in ('CRE','MGD','APS','NCR','UCR','NCB','UCB')
              and promo_temp.ph_prom_dt > promo_temp.oli_online_slo_prom_dt
             then 1
        end) END online_slo_prom_cnt,
  count(case when promo_temp.ph_bus_unt = 'FUN'
              and promo_temp.ph_chnl_cd = 'T'
             then 1
        end) fr_tm_prom_cnt,
  /*count(case when promo_temp.ph_bus_unt = 'UCR'
             then 1
        end) uc_rpts_prom_cnt,*/
  /*count(case when promo_temp.ph_bus_unt = 'UCB'
             then 1
        end) ucbk_prom_cnt,*/
  /* SHM bus_unt
  max(CASE WHEN promo_temp.ph_bus_unt = 'SHM'
           THEN promo_temp.ph_prom_dt
           ELSE null
      END) shm_lst_prom_dt,*/
  /*max(case when promo_temp.ph_bus_unt = 'SHM'
            and promo_temp.ph_chnl_cd = 'D'
           then promo_temp.ph_prom_dt
      end) shm_dm_lst_prom_dt,*/
  /*max(case when promo_temp.ph_bus_unt = 'SHM'
            and promo_temp.ph_chnl_cd = 'E'
           then promo_temp.ph_prom_dt
      end) shm_em_lst_prom_dt,*/
  /*count(case when promo_temp.ph_bus_unt = 'SHM'
             then 1
        end) shm_prom_cnt,*/
  /*count(case when promo_temp.ph_bus_unt = 'SHM'
              and promo_temp.ph_chnl_cd = 'D'
             then 1
        end) shm_dm_prom_cnt,*/
  /*count(case when promo_temp.ph_bus_unt = 'SHM'
              and promo_temp.ph_chnl_cd = 'E'
             then 1
        end) shm_em_prom_cnt,*/
  /*count(case when promo_temp.ph_bus_unt = 'SHM'
              and ((substring(promo_temp.ph_keycode,1,1) in ('G','U','X') and promo_temp.ph_keycode != 'UNKNOWN')
                   or substring(promo_temp.ph_keycode,1,2) = 'IG'
                   or (substring(promo_temp.ph_keycode,1,1) in ('J','K')
                       and substring(promo_temp.ph_keycode,7,1) in ('G','X')))
             then 1
        end) shm_dnr_prom_cnt,*/
  promo_temp.osl_hh_id
FROM agg.promsum_fullraw_temp promo_temp
GROUP BY promo_temp.individual_id, promo_temp.hh_id, promo_temp.osl_hh_id
;

analyse prod.agg_promotion_summary;