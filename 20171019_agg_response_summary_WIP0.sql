CREATE TABLE mt_resp_summary_temp
    AS;
SELECT
    ph.individual_id
    , ph.bus_unt
    , ph.keycode
    , min(ph.prom_dt)             AS prom_dt
    , substring(min(CASE WHEN ph.bus_unt = 'ABQ'
    THEN CASE
                  WHEN abq.individual_id IS NOT NULL AND abq.keycode IS NOT NULL AND abq.mbr_flg = 'Y'
                     THEN '1M'
                 WHEN abq.individual_id IS NOT NULL AND abq.keycode IS NOT NULL AND nvl(abq.mbr_flg, 'Z') != 'Y'
                     THEN '2Q'
         WHEN er.campaign_optout = '0'
             THEN '3N'
         WHEN er.clicked = '1'
             THEN '4S'
         WHEN er.opened = '1'
             THEN '5U'
         WHEN er.bounced = '1'
             THEN '6W'
         ELSE
             '7Y'
         END
                 ELSE
                     NULL
                 END), 2)            abq_resp_cd
    , substring(min(CASE WHEN ph.bus_unt = 'ADV'
    THEN CASE
                  WHEN adv.individual_id is not null and adv.keycode IS NOT NULL
                     THEN '1P'
         WHEN er.campaign_optout = '0'
             THEN '2N'
         WHEN er.clicked = '1'
             THEN '3S'
         WHEN er.opened = '1'
             THEN '4U'
         WHEN er.bounced = '1'
             THEN '5W'
         ELSE
             '6Y'
         END
                 ELSE
                     NULL
                 END), 2)            adv_resp_cd
    , CASE WHEN max(CASE WHEN ph.bus_unt IN ('APS', 'NCB', 'UCB', 'NCR', 'UCR')
    THEN 1
                    END) = 1
    THEN substring(min(CASE WHEN ph.bus_unt IN ('APS', 'NCB', 'UCB', 'NCR', 'UCR')
        THEN CASE WHEN (oi.crd_stat_cd = 'F'
                        OR (oi.svc_stat_cd = 'C' AND oi.canc_rsn_cd IN ('50', '06')))
                       AND oi.mag_cd IN ('NCBK', 'UCBK', 'NCPR', 'UCPR')
            THEN '1A'
             WHEN oi.svc_stat_cd = 'C' AND oi.canc_rsn_cd NOT IN ('50', '06', '14')
                  AND oi.mag_cd IN ('NCBK', 'UCBK', 'NCPR', 'UCPR')
                 THEN '2C'
             WHEN oi.stat_cd = 'C'
                  AND oi.mag_cd IN ('NCBK', 'UCBK', 'NCPR', 'UCPR')
                 THEN '3E'
             WHEN oi.stat_cd != 'C'
                  AND oi.mag_cd IN ('NCBK', 'UCBK', 'NCPR', 'UCPR')
                 THEN '4G'
             WHEN er.campaign_optout = '0'
                 THEN '5N'
             WHEN er.clicked = '1'
                 THEN '6S'
             WHEN er.opened = '1'
                 THEN '7U'
             WHEN er.bounced = '1'
                 THEN '8W'
             ELSE '9Y'
             END
                    ELSE NULL
                    END), 2)
      ELSE NULL
      END                            aps_resp_cd
    , substring(min(CASE WHEN ph.bus_unt = 'FUN'
    THEN CASE WHEN fd.don_ind = '1'
        THEN '1I'
         WHEN fp.individual_id IS NOT NULL
             THEN '2K'
         WHEN fd.don_ind = '0'
             THEN '3O'
         WHEN er.campaign_optout = '0'
             THEN '4N'
         WHEN er.clicked = '1'
             THEN '5S'
         WHEN er.opened = '1'
             THEN '6U'
         WHEN er.bounced = '1'
             THEN '7W'
         ELSE '8Y'
         END
                 ELSE NULL
                 END), 2)            fr_resp_cd
    , CASE WHEN max(CASE WHEN ph.bus_unt IN ('CNS', 'CRH', 'CRM', 'SHM')
    THEN 1
                    END) = 1
    THEN substring(min(CASE WHEN ph.bus_unt IN ('CNS', 'CRH', 'CRM', 'SHM')
        THEN CASE WHEN ofo.canc_rsn_cd IN ('06', '50')
                       AND ofo.mag_cd IN ('CNS', 'CRH', 'CRM', 'SHM')
            THEN '1A'
             WHEN ofo.canc_rsn_cd NOT IN ('06', '50', '14')
                  AND ofo.mag_cd IN ('CNS', 'CRH', 'CRM', 'SHM')
                 THEN '2C'
             WHEN ofo.cr_stat_cd IN ('C', 'D', 'E', 'F', 'G', 'I')
                  AND ofo.mag_cd IN ('CNS', 'CRH', 'CRM', 'SHM')
                 THEN '3E'
             WHEN ofo.mag_cd IN ('CNS', 'CRH', 'CRM', 'SHM')
                  AND ofo.mag_cd IN ('CNS', 'CRH', 'CRM', 'SHM')
                 THEN '4G'
             WHEN er.campaign_optout = '0'
                 THEN '5N'
             WHEN er.clicked = '1'
                 THEN '6S'
             WHEN er.opened = '1'
                 THEN '7U'
             WHEN er.bounced = '1'
                 THEN '8W'
             ELSE '9Y'
             END
                    ELSE NULL
                    END), 2)
      WHEN max(CASE WHEN ph.bus_unt = 'CRE'
          THEN 1
               END) = 1
          THEN substring(min(CASE WHEN (oi.crd_stat_cd = 'F'
                                     OR (oi.svc_stat_cd = 'C' AND oi.canc_rsn_cd IN ('50', '06')))
                                    AND oi.mag_cd = 'CRO'
              THEN '1A'
                          WHEN oi.svc_stat_cd = 'C'
                               AND oi.canc_rsn_cd NOT IN ('50', '06', '14')
                               AND oi.mag_cd = 'CRO'
                              THEN '2C'
                          WHEN oi.stat_cd = 'C'
                               AND oi.mag_cd = 'CRO'
                              THEN '3E'
                          WHEN oi.stat_cd != 'C'
                               AND oi.mag_cd = 'CRO'
                              THEN '4G'
                          WHEN er.campaign_optout = '0'
                              THEN '5N'
                          WHEN er.clicked = '1'
                              THEN '6S'
                          WHEN er.opened = '1'
                              THEN '7U'
                          WHEN er.bounced = '1'
                              THEN '8W'
                          ELSE '9Y'
                          END), 2)
      WHEN max(CASE WHEN ph.bus_unt = 'MGD'
          THEN 1
               END) = 1
          THEN substring(min(CASE WHEN (oi.crd_stat_cd = 'F'
                                     OR (oi.svc_stat_cd = 'C' AND oi.canc_rsn_cd IN ('50', '06')))
                                    AND oi.mag_cd = 'CRMG'
              THEN '1A'
                          WHEN oi.stat_cd = 'R'
                               AND oi.mag_cd = 'CRMG'
                              THEN '2C'
                          WHEN oi.stat_cd = 'C'
                               AND oi.mag_cd = 'CRMG'
                              THEN '3E'
                          WHEN OI.stat_cd != 'C'
                               AND oi.mag_cd = 'CRMG'
                              THEN '4G'
                          WHEN er.campaign_optout = '0'
                              THEN '5N'
                          WHEN er.clicked = '1'
                              THEN '6S'
                          WHEN er.opened = '1'
                              THEN '7U'
                          WHEN er.bounced = '1'
                              THEN '8W'
                          ELSE '9Y'
                          END), 2)
      END                            subs_resp_cd
    , substring(min(CASE WHEN ph.bus_unt IN ('MKT', 'PNL')
    THEN
        CASE WHEN er.campaign_optout = '0'
            THEN '1N'
        WHEN er.clicked = '1'
            THEN '2S'
        WHEN er.opened = '1'
            THEN '3U'
        WHEN er.bounced = '1'
            THEN '4W'
        ELSE
            '5Y'
        END
                 ELSE
                     NULL
                 END), 2)         AS crsch_resp_cd
    , max(oi.ord_dt)              AS olo_ord_dt
    , max(er.maint_dt)            AS er_maint_dt
    , max(ph.prom_dt)             AS ph_prom_dt
    , max(fd.don_dt)              AS fd_don_dt
    , max(fp.pldg_dt)             AS fp_pldg_dt
    , max(ofo.canc_dt)            AS ofo_canc_dt
    , max(oi.canc_dt)             AS oli_canc_dt
    , max(ofo.pmt_dt)             AS ofo_pmt_dt
    , max(oi.pd_dt)               AS olo_pd_dt
    , max(ofo.ord_dt)             AS ofo_ord_dt
    , max(abq.abq_yr)             AS abq_abq_yr
    , max(adv.last_response_date) AS adv_last_resp_dt
FROM cr_temp.respsumm_promo_p_temp ph
    LEFT JOIN prod.agg_abq_response abq
        ON ph.individual_id = abq.individual_id AND ph.keycode = abq.abq_yr || abq.keycode
    LEFT JOIN cr_temp.respsumm_ph_ij_er_temp er
        ON ph.individual_id = er.individual_id AND ph.keycode = er.keycode
    LEFT JOIN prod.legacy_constituent_response adv
        ON ph.individual_id = adv.individual_id AND ph.keycode = adv.keycode
    LEFT JOIN prod.agg_fundraising_donation fd
        ON ph.individual_id = fd.individual_id AND ph.keycode = fd.keycode
    LEFT JOIN cr_temp.respsumm_ofo_temp ofo
        ON ph.individual_id = ofo.individual_id AND ph.keycode = ofo.keycode AND ph.bus_unt = ofo.mag_cd
    LEFT JOIN (SELECT
                     cast(NULL AS BIGINT) AS individual_id
                   , cast(NULL AS TEXT)      keycode
                   , cast(NULL AS DATE)      pldg_dt) fp /*SD 20171019: Pledges are deprecated*/
        ON ph.individual_id = fp.individual_id AND ph.keycode = fp.keycode
    LEFT JOIN cr_temp.respsumm_olo_lj_oli_temp oi
        ON ph.individual_id = oi.individual_id AND upper(ph.keycode) = oi.keycode
WHERE 1 = 1
      AND ph.bus_unt IN ('ABQ', 'ADV', 'APS', 'NCB', 'UCB', 'NCR', 'UCR', 'FUN', 'CNS', 'CRH', 'CRM', 'CRE', 'MGD', 'MKT', 'PNL', 'SHM')
      AND exists(SELECT NULL
           FROM cr_temp.respsumm_promo_nt_temp nt
           WHERE nt.individual_id = ph.individual_id)

GROUP BY ph.individual_id, ph.bus_unt, ph.keycode;