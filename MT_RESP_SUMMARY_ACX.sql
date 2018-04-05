/**************************************************************************
*            (C) Copyright Acxiom Corporation 2006
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project:      CU Middle Tier Extract
* Module Name:     mt_resp_summary.sql
* Date:            11/28/2006     
*
* Level:  1 per individual_id per bus_unit per keycode
*
***************************************************************************/

-- create table resp_ind_xref_ph/*NOTICEME*/
-- as select t1.individual_id,t2.ind_urn
-- from warehouse.promotion t1, ind_xref t2
-- where t1.individual_id = t2.individual_id and t2.active_flag = 'A'
-- and nvl(t1.contact_type,'P') != 'T'
-- order by t1.individual_id,t2.ind_urn;

*

-- create table resp_mt_online_ord 
-- as select t1.* from mt_online_ord t1, 
-- resp_ind_xref_ph/*NOTICEME*/ t2 where t1.individual_id = t2.individual_id;

-- create table resp_mt_online_item 
-- as select t1.* from mt_online_item t1, 
-- resp_ind_xref_ph/*NOTICEME*/ t2 where t1.individual_id = t2.individual_id;

-- create table resp_mt_offline_ord 
-- as select t1.* from mt_offline_ord t1, 
-- resp_ind_xref_ph/*NOTICEME*/ t2 where t1.individual_id = t2.individual_id;

-- create table resp_email_response 
-- as select t1.* from warehouse.email_response t1, 
-- resp_ind_xref_ph/*NOTICEME*/ t2 where t1.individual_id = t2.individual_id
-- and t1.list_id not in ('10242','11367','11368') and t1.keycd is not null;

create table mt_resp_summary_temp
as
select
  tc.individual_id,
  tc.ind_urn,
  ph.bus_unt,
  ph.keycode,
  min(ph.prom_dt) prom_dt,
  substr(min(case when ph.bus_unt = 'ABQ' then
               case when abq.individual_id || abq.keycode is not null and abq.mbr_flg = 'Y' then '1M'
                    when abq.individual_id || abq.keycode is not null and nvl(abq.mbr_flg, 'Z') != 'Y' then '2Q' 
                    when er.campaign_optout = '0' then '3N'
                    when er.clicked = '1' then '4S'
                    when er.opened = '1' then '5U'
                    when er.bounced = '1' then '6W'
               else
                 '7Y'
               end
             else
               null
             end), 2) abq_resp_cd,
  substr(min(case when ph.bus_unt = 'ADV' then
               case when adv.individual_id || adv.keycode is not null then '1P'
                    when er.campaign_optout = '0' then '2N'
                    when er.clicked = '1' then '3S'
                    when er.opened = '1' then '4U'
                    when er.bounced = '1' then '5W'
               else 
                 '6Y'
               end
             else 
               null
             end), 2) adv_resp_cd,
  case when max(case when ph.bus_unt in ('APS','NCB','UCB','NCR','UCR')
                     then 1
                end) = 1
       then substr(min(case when ph.bus_unt in ('APS','NCB','UCB','NCR','UCR')
                            then case when (oi.crd_stat_cd = 'F'
                                        or (oi.svc_stat_cd = 'C' and oi.canc_rsn_cd in ('50','06')) )
                                       and oi.mag_cd in ('NCBK','UCBK','NCPR','UCPR')
                                      then '1A'
                                      when  oi.svc_stat_cd = 'C' and oi.canc_rsn_cd not in ('50','06','14')
                                       and oi.mag_cd in ('NCBK','UCBK','NCPR','UCPR')
                                      then '2C'
                                      when oi.stat_cd = 'C'
                                       and oi.mag_cd in ('NCBK','UCBK','NCPR','UCPR')
                                      then '3E'
                                      when oi.stat_cd != 'C'
                                       and oi.mag_cd in ('NCBK','UCBK','NCPR','UCPR')
                                      then '4G'
                                      when er.campaign_optout = '0' 
                                      then '5N'
                                      when er.clicked = '1'
                                      then '6S'
                                      when er.opened = '1'
                                      then '7U'
                                      when er.bounced = '1'
                                      then '8W'
                                      else '9Y'
                                 end
                            else null
                           end), 2)
       else null
  end aps_resp_cd,
  substr(min(case when ph.bus_unt = 'FUN'
                  then case when fd.don_ind = '1'
                            then '1I'
                            when fp.individual_id is not null
                            then '2K'
                            when fd.don_ind = '0'
                            then '3O'
                            when er.campaign_optout = '0' 
                            then '4N'
                            when er.clicked = '1'
                            then '5S'
                            when er.opened = '1'
                            then '6U'
                            when er.bounced = '1'
                            then '7W'
                            else '8Y'
                       end
                  else null
                 end), 2) fr_resp_cd,
  case when max(case when ph.bus_unt in ('CNS','CRH','CRM','SHM')
                     then 1
                end) = 1
       then substr(min(case when ph.bus_unt in ('CNS','CRH','CRM','SHM')
                            then case when ofo.canc_rsn_cd in ('06','50')
                                       and ofo.mag_cd in ('CNS','CRH','CRM','SHM')
                                      then '1A'
                                      when ofo.canc_rsn_cd not in ('06','50','14')
                                       and ofo.mag_cd in ('CNS','CRH','CRM','SHM')
                                      then '2C'
                                      when ofo.cr_stat_cd in ('C','D','E','F','G','I')
                                       and ofo.mag_cd in ('CNS','CRH','CRM','SHM')
                                      then '3E'
                                      when ofo.mag_cd in ('CNS','CRH','CRM','SHM')
                                       and ofo.mag_cd in ('CNS','CRH','CRM','SHM')
                                      then '4G'
                                      when er.campaign_optout = '0' 
                                      then '5N'
                                      when er.clicked = '1'
                                      then '6S'
                                      when er.opened = '1'
                                      then '7U'
                                      when er.bounced = '1'
                                      then '8W'
                                      else '9Y'
                                 end
                            else null
                           end), 2)
       when max(case when ph.bus_unt = 'CRE'
                     then 1
                end) = 1
       then substr(min(case when (oi.crd_stat_cd = 'F'
                              or (oi.svc_stat_cd = 'C' and oi.canc_rsn_cd in ('50','06')) )
                             and oi.mag_cd = 'CRO'
                            then '1A'
                            when oi.svc_stat_cd = 'C'
                             and oi.canc_rsn_cd not in ('50','06','14')
                             and oi.mag_cd = 'CRO'
                            then '2C'
                            when oi.stat_cd = 'C'
                             and oi.mag_cd = 'CRO'
                            then '3E'
                            when oi.stat_cd != 'C'
                             and oi.mag_cd = 'CRO'
                            then '4G'
                            when er.campaign_optout = '0' 
                            then '5N'
                            when er.clicked = '1'
                            then '6S'
                            when er.opened = '1'
                            then '7U'
                            when er.bounced = '1'
                            then '8W'
                            else '9Y'
                           end), 2)
       when max(case when ph.bus_unt = 'MGD'
                     then 1
                end) = 1
       then substr(min(case when (oi.crd_stat_cd = 'F'
                              or (oi.svc_stat_cd = 'C' and oi.canc_rsn_cd in ('50','06')) )
                             and oi.mag_cd = 'CRMG'
                            then '1A'
                            when oi.stat_cd = 'R'
                             and oi.mag_cd = 'CRMG'
                            then '2C'
                            when oi.stat_cd = 'C'
                             and oi.mag_cd = 'CRMG'
                            then '3E'
                            when OI.stat_cd != 'C'
                             and oi.mag_cd = 'CRMG'
                            then '4G'
                            when er.campaign_optout = '0' 
                            then '5N'
                            when er.clicked = '1'
                            then '6S'
                            when er.opened = '1'
                            then '7U'
                            when er.bounced = '1'
                            then '8W'
                            else '9Y'
                           end), 2)
  end subs_resp_cd,
  substr(min(case when ph.bus_unt in ('MKT','PNL') then
               case when er.campaign_optout = '0' then '1N'
                    when er.clicked = '1' then '2S'
                    when er.opened = '1' then '3U'
                    when er.bounced = '1' then '4W'
               else 
                 '5Y'
               end
             else 
               null
             end), 2) crsch_resp_cd,
  max(oi.ord_dt) olo_ord_dt,
  max(er.maint_dt) er_maint_dt,
  max(ph.prom_dt) ph_prom_dt,
  max(fd.don_dt) fd_don_dt,
  max(fp.pldg_dt) fp_pldg_dt,
  max(ofo.canc_dt) ofo_canc_dt,
  max(oi.canc_dt) oli_canc_dt,
  max(ofo.pmt_dt) ofo_pmt_dt,
  max(oi.pd_dt) olo_pd_dt,
  max(ofo.ord_dt) ofo_ord_dt,
  max(abq.abq_yr) abq_abq_yr,
  max(adv.last_resp_dt) adv_last_resp_dt
from
    resp_ind_xref_ph/*NOTICEME*/ tc,
    resp_ph ph,
    mt_abq_response abq,
    mt_adv_response adv,
    mt_fr_donation fd,
    resp_mt_offline_ord ofo,
    mt_fr_pledge fp,
    (select
       ph.individual_id,
       ph.keycode,
       er.campaign_optout,
       er.clicked,
       er.opened,
       er.bounced,
       er.maint_dt
     from resp_ind_xref_ph/*NOTICEME*/ tc,
       resp_ph ph,
       resp_email_response er,
       warehouse.di_list_id_lkp lk
     where tc.individual_id = er.individual_id
       and er.individual_id = ph.individual_id
       and ph.individual_id = tc.individual_id
       and ph.keycode = er.keycd
       and ph.bus_unt = lk.bus_unt
       and lk.list_id = er.list_id
       and ph.bus_unt in ('ABQ','ADV','APS','NCB','UCB','NCR','UCR','FUN','CNS','CRH','CRM','CRE','MGD','MKT','PNL','SHM')) er,
    (select
       olo.individual_id,
       nvl(oli.ext_keycd,oli.int_keycd) keycode,
       olo.pd_dt,
       oli.canc_dt,
       olo.ord_dt,
       oli.mag_cd,
       oli.crd_stat_cd,
       oli.svc_stat_cd,
       oli.canc_rsn_cd,
       oli.stat_cd
     from
       resp_mt_online_ord olo,
       resp_mt_online_item oli
     where
       olo.ord_id = oli.ord_id (+)) oi
where tc.individual_id = ph.individual_id
  and ph.individual_id = abq.individual_id (+)
  and ph.keycode = abq.abq_yr (+) || abq.keycode (+)
  and ph.individual_id = er.individual_id (+)
  and ph.keycode = er.keycode (+)
  and ph.individual_id = adv.individual_id (+)
  and ph.keycode = adv.keycode (+)
  and ph.bus_unt in ('ABQ','ADV','APS','NCB','UCB','NCR','UCR','FUN','CNS','CRH','CRM','CRE','MGD','MKT','PNL','SHM')
  and ph.individual_id =  oi.individual_id (+)
  and upper(ph.keycode) = oi.keycode (+)
  and ph.individual_id = fd.individual_id (+)
  and ph.keycode = fd.keycode (+)
  and ph.individual_id = ofo.individual_id (+)
  and ph.keycode = ofo.keycode (+)
  and ph.bus_unt = ofo.mag_cd (+)
  and ph.individual_id = fp.individual_id (+)
  and ph.keycode = fp.keycode (+)
group by tc.individual_id, tc.ind_urn, ph.bus_unt, ph.keycode;


create table mt_resp_summary
as
select
  individual_id,
  ind_urn,
  bus_unt,
  keycode,
  prom_dt,
  abq_resp_cd,
  adv_resp_cd,
  case when aps_resp_cd in ('E','G')
       then olo_ord_dt
       when aps_resp_cd in ('A','C')
       then oli_canc_dt
       when aps_resp_cd in ('N','S','U','W')
       then er_maint_dt
       when aps_resp_cd = 'Y'
       then ph_prom_dt
       when fr_resp_cd in ('I','O')
       then fd_don_dt
       when fr_resp_cd = 'K'
       then fp_pldg_dt
       when fr_resp_cd in ('N','S','U','W')
       then er_maint_dt
       when fr_resp_cd = 'Y'
       then ph_prom_dt
       when subs_resp_cd in ('A','C')
       then nvl(ofo_canc_dt,oli_canc_dt)
       when subs_resp_cd = 'E'
       then nvl(ofo_pmt_dt,olo_pd_dt)
       when subs_resp_cd = 'G'
       then nvl(ofo_ord_dt,olo_ord_dt)
       when subs_resp_cd in ('N','S','U','W')
       then er_maint_dt
       when subs_resp_cd = 'Y'
       then ph_prom_dt
       when abq_resp_cd in ('M','Q')
       then to_date(abq_abq_yr || '0601','YYYYMMDD')
       when abq_resp_cd in ('N','S','U','W')
       then er_maint_dt
       when abq_resp_cd = 'Y'
       then ph_prom_dt
       when adv_resp_cd = 'P'
       then adv_last_resp_dt
       when adv_resp_cd in ('N','S','U','W')
       then er_maint_dt
       when adv_resp_cd = 'Y'
       then ph_prom_dt
       when crsch_resp_cd in ('N','S','U','W')
       then er_maint_dt
       when crsch_resp_cd = 'Y'
       then ph_prom_dt
  end resp_dt,
  aps_resp_cd,
  fr_resp_cd,
  subs_resp_cd,
  crsch_resp_cd
  from middle_tier.mt_resp_summary_temp;
  
