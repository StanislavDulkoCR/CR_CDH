
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
FROM cr_temp.promsum_fullraw_temp promo_temp
where individual_id = 1216297895
GROUP BY promo_temp.individual_id, promo_temp.hh_id, promo_temp.osl_hh_id
limit 100;


;
