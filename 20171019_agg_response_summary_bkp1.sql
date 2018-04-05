SET wlm_query_slot_count TO 15;

DROP TABLE IF EXISTS cr_temp.respsumm_promo_nt_temp; --resp_ind_xref_ph
CREATE TABLE cr_temp.respsumm_promo_nt_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT DISTINCT
            /*#CNFRM*/ t1.individual_id
        FROM prod.promotion t1
        WHERE nvl(t1.contact_type, 'P') != 'T'
              /*#TODO RM*/ AND t1.individual_id IN (1200010439, 1200015826, 1200016266, 1200018306, 1200019235, 1200020139, 1200020145, 1200020315, 1200020915, 1200020920, 1200020946, 1200020948, 1200020967, 1200020989, 1200020997, 1200021009, 1200021045, 1200021047, 1200021052, 1200021068, 1235932034, 1235932033, 1235932032, 1235932031, 1235932030, 1235932029, 1235932028, 1235932027, 1235932026, 1235932025, 1235932024, 1235932023, 1235932022, 1235932021, 1235932020, 1235932019, 1235932018, 1235932017, 1235932016, 1235932015, 1235932014, 1235932013, 1235932012, 1235932011, 1235932010, 1235932009, 1235932008, 1235932007, 1235932006, 1235932005, 1235932004, 1235932003, 1235932002, 1235932001, 1235932000, 1235931999, 1235931998, 1235931997, 1235931996, 1235931995, 1235931994, 1235931993, 1235931992, 1235931991, 1235931990, 1235931989, 1235931988, 1235931987, 1235931986, 1235931985, 1235931984, 1235931983, 1235931982, 1235931981, 1235931980, 1235931979, 1235931978, 1235931977, 1235931976, 1235931975, 1235931974, 1235931973, 1235931972, 1235931971, 1235931970, 1235931969, 1235931968, 1235931967, 1235931966, 1235931965, 1235931964, 1235931963, 1235931962, 1235931961, 1235931960, 1235931959, 1235931958, 1235931957, 1235931956, 1235931955)
        ORDER BY t1.individual_id;


DROP TABLE IF EXISTS cr_temp.respsumm_promo_p_temp; --resp_ph
CREATE TABLE cr_temp.respsumm_promo_p_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT DISTINCT
            /*#CNFRM*/
            t1.individual_id
            , t1.Business_Unit  AS bus_unt
            , t1.keycode
            , t1.Promotion_Date AS prom_dt
        FROM prod.promotion t1
        WHERE t1.contact_type = 'P'
              AND exists(SELECT NULL
                         FROM cr_temp.respsumm_promo_nt_temp nt
                         WHERE nt.individual_id = t1.individual_id)
              /*#TODO RM*/ AND t1.individual_id IN (1200010439, 1200015826, 1200016266, 1200018306, 1200019235, 1200020139, 1200020145, 1200020315, 1200020915, 1200020920, 1200020946, 1200020948, 1200020967, 1200020989, 1200020997, 1200021009, 1200021045, 1200021047, 1200021052, 1200021068, 1235932034, 1235932033, 1235932032, 1235932031, 1235932030, 1235932029, 1235932028, 1235932027, 1235932026, 1235932025, 1235932024, 1235932023, 1235932022, 1235932021, 1235932020, 1235932019, 1235932018, 1235932017, 1235932016, 1235932015, 1235932014, 1235932013, 1235932012, 1235932011, 1235932010, 1235932009, 1235932008, 1235932007, 1235932006, 1235932005, 1235932004, 1235932003, 1235932002, 1235932001, 1235932000, 1235931999, 1235931998, 1235931997, 1235931996, 1235931995, 1235931994, 1235931993, 1235931992, 1235931991, 1235931990, 1235931989, 1235931988, 1235931987, 1235931986, 1235931985, 1235931984, 1235931983, 1235931982, 1235931981, 1235931980, 1235931979, 1235931978, 1235931977, 1235931976, 1235931975, 1235931974, 1235931973, 1235931972, 1235931971, 1235931970, 1235931969, 1235931968, 1235931967, 1235931966, 1235931965, 1235931964, 1235931963, 1235931962, 1235931961, 1235931960, 1235931959, 1235931958, 1235931957, 1235931956, 1235931955)
        ORDER BY t1.individual_id;


DROP TABLE IF EXISTS cr_temp.respsumm_olo_temp; --resp_mt_online_ord
CREATE TABLE cr_temp.respsumm_olo_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT
            olo.individual_id
            , olo.pd_dt
            , olo.ord_dt
            , olo.ord_id

        FROM prod.agg_digital_order olo
        WHERE 1 = 1
              AND exists(SELECT NULL
                         FROM cr_temp.respsumm_promo_nt_temp nt
                         WHERE nt.individual_id = olo.individual_id)
              /*#TODO RM*/ AND olo.individual_id IN (1200010439, 1200015826, 1200016266, 1200018306, 1200019235, 1200020139, 1200020145, 1200020315, 1200020915, 1200020920, 1200020946, 1200020948, 1200020967, 1200020989, 1200020997, 1200021009, 1200021045, 1200021047, 1200021052, 1200021068, 1235932034, 1235932033, 1235932032, 1235932031, 1235932030, 1235932029, 1235932028, 1235932027, 1235932026, 1235932025, 1235932024, 1235932023, 1235932022, 1235932021, 1235932020, 1235932019, 1235932018, 1235932017, 1235932016, 1235932015, 1235932014, 1235932013, 1235932012, 1235932011, 1235932010, 1235932009, 1235932008, 1235932007, 1235932006, 1235932005, 1235932004, 1235932003, 1235932002, 1235932001, 1235932000, 1235931999, 1235931998, 1235931997, 1235931996, 1235931995, 1235931994, 1235931993, 1235931992, 1235931991, 1235931990, 1235931989, 1235931988, 1235931987, 1235931986, 1235931985, 1235931984, 1235931983, 1235931982, 1235931981, 1235931980, 1235931979, 1235931978, 1235931977, 1235931976, 1235931975, 1235931974, 1235931973, 1235931972, 1235931971, 1235931970, 1235931969, 1235931968, 1235931967, 1235931966, 1235931965, 1235931964, 1235931963, 1235931962, 1235931961, 1235931960, 1235931959, 1235931958, 1235931957, 1235931956, 1235931955);



DROP TABLE IF EXISTS cr_temp.respsumm_oli_temp; --resp_mt_online_item
CREATE TABLE cr_temp.respsumm_oli_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT
            oli.individual_id
            , oli.ext_keycd
            , oli.int_keycd
            , oli.canc_dt
            , oli.mag_cd
            , oli.crd_stat_cd
            , oli.svc_stat_cd
            , oli.canc_rsn_cd
            , oli.stat_cd
            , oli.ord_id

        FROM prod.agg_digital_item oli
        WHERE 1 = 1
              AND exists(SELECT NULL
                         FROM cr_temp.respsumm_promo_nt_temp nt
                         WHERE nt.individual_id = oli.individual_id)
              /*#TODO RM*/ AND oli.individual_id IN (1200010439, 1200015826, 1200016266, 1200018306, 1200019235, 1200020139, 1200020145, 1200020315, 1200020915, 1200020920, 1200020946, 1200020948, 1200020967, 1200020989, 1200020997, 1200021009, 1200021045, 1200021047, 1200021052, 1200021068, 1235932034, 1235932033, 1235932032, 1235932031, 1235932030, 1235932029, 1235932028, 1235932027, 1235932026, 1235932025, 1235932024, 1235932023, 1235932022, 1235932021, 1235932020, 1235932019, 1235932018, 1235932017, 1235932016, 1235932015, 1235932014, 1235932013, 1235932012, 1235932011, 1235932010, 1235932009, 1235932008, 1235932007, 1235932006, 1235932005, 1235932004, 1235932003, 1235932002, 1235932001, 1235932000, 1235931999, 1235931998, 1235931997, 1235931996, 1235931995, 1235931994, 1235931993, 1235931992, 1235931991, 1235931990, 1235931989, 1235931988, 1235931987, 1235931986, 1235931985, 1235931984, 1235931983, 1235931982, 1235931981, 1235931980, 1235931979, 1235931978, 1235931977, 1235931976, 1235931975, 1235931974, 1235931973, 1235931972, 1235931971, 1235931970, 1235931969, 1235931968, 1235931967, 1235931966, 1235931965, 1235931964, 1235931963, 1235931962, 1235931961, 1235931960, 1235931959, 1235931958, 1235931957, 1235931956, 1235931955);

DROP TABLE IF EXISTS cr_temp.respsumm_ofo_temp; --resp_mt_offline_ord
CREATE TABLE cr_temp.respsumm_ofo_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT
            ofo.individual_id
            , ofo.canc_rsn_cd
            , ofo.mag_cd
            , ofo.cr_stat_cd
            , ofo.canc_dt
            , ofo.pmt_dt
            , ofo.ord_dt
            , ofo.keycode


        FROM prod.agg_print_order ofo
        WHERE 1 = 1
              AND exists(SELECT NULL
                         FROM cr_temp.respsumm_promo_nt_temp nt
                         WHERE nt.individual_id = ofo.individual_id)
              /*#TODO RM*/ AND ofo.individual_id IN (1200010439, 1200015826, 1200016266, 1200018306, 1200019235, 1200020139, 1200020145, 1200020315, 1200020915, 1200020920, 1200020946, 1200020948, 1200020967, 1200020989, 1200020997, 1200021009, 1200021045, 1200021047, 1200021052, 1200021068, 1235932034, 1235932033, 1235932032, 1235932031, 1235932030, 1235932029, 1235932028, 1235932027, 1235932026, 1235932025, 1235932024, 1235932023, 1235932022, 1235932021, 1235932020, 1235932019, 1235932018, 1235932017, 1235932016, 1235932015, 1235932014, 1235932013, 1235932012, 1235932011, 1235932010, 1235932009, 1235932008, 1235932007, 1235932006, 1235932005, 1235932004, 1235932003, 1235932002, 1235932001, 1235932000, 1235931999, 1235931998, 1235931997, 1235931996, 1235931995, 1235931994, 1235931993, 1235931992, 1235931991, 1235931990, 1235931989, 1235931988, 1235931987, 1235931986, 1235931985, 1235931984, 1235931983, 1235931982, 1235931981, 1235931980, 1235931979, 1235931978, 1235931977, 1235931976, 1235931975, 1235931974, 1235931973, 1235931972, 1235931971, 1235931970, 1235931969, 1235931968, 1235931967, 1235931966, 1235931965, 1235931964, 1235931963, 1235931962, 1235931961, 1235931960, 1235931959, 1235931958, 1235931957, 1235931956, 1235931955);

DROP TABLE IF EXISTS    cr_temp.respsumm_emresp_temp; --resp_email_response
CREATE TABLE            cr_temp.respsumm_emresp_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT

            emr.individual_id
            , emr.campaign_optout
            , emr.clicked
            , emr.Opened_Flag  AS opened
            , emr.Bounced_Flag AS bounced
            , emr.create_date  AS maint_dt
            , emr.keycode      AS keycd
            , emr.list_id

        FROM prod.email_response emr
        WHERE 1 = 1
              AND emr.list_id NOT IN ('10242', '11367', '11368')
              AND emr.keycode IS NOT NULL
              AND exists(SELECT NULL
                         FROM cr_temp.respsumm_promo_nt_temp nt
                         WHERE nt.individual_id = emr.individual_id)
              /*#TODO RM*/ AND emr.individual_id IN (1200010439, 1200015826, 1200016266, 1200018306, 1200019235, 1200020139, 1200020145, 1200020315, 1200020915, 1200020920, 1200020946, 1200020948, 1200020967, 1200020989, 1200020997, 1200021009, 1200021045, 1200021047, 1200021052, 1200021068, 1235932034, 1235932033, 1235932032, 1235932031, 1235932030, 1235932029, 1235932028, 1235932027, 1235932026, 1235932025, 1235932024, 1235932023, 1235932022, 1235932021, 1235932020, 1235932019, 1235932018, 1235932017, 1235932016, 1235932015, 1235932014, 1235932013, 1235932012, 1235932011, 1235932010, 1235932009, 1235932008, 1235932007, 1235932006, 1235932005, 1235932004, 1235932003, 1235932002, 1235932001, 1235932000, 1235931999, 1235931998, 1235931997, 1235931996, 1235931995, 1235931994, 1235931993, 1235931992, 1235931991, 1235931990, 1235931989, 1235931988, 1235931987, 1235931986, 1235931985, 1235931984, 1235931983, 1235931982, 1235931981, 1235931980, 1235931979, 1235931978, 1235931977, 1235931976, 1235931975, 1235931974, 1235931973, 1235931972, 1235931971, 1235931970, 1235931969, 1235931968, 1235931967, 1235931966, 1235931965, 1235931964, 1235931963, 1235931962, 1235931961, 1235931960, 1235931959, 1235931958, 1235931957, 1235931956, 1235931955);

;


DROP TABLE IF EXISTS    cr_temp.respsumm_olo_lj_oli_temp; --oi
CREATE TABLE            cr_temp.respsumm_olo_lj_oli_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT
            olo.individual_id
            , nvl(oli.ext_keycd, oli.int_keycd) keycode
            , olo.pd_dt
            , oli.canc_dt
            , olo.ord_dt
            , oli.mag_cd
            , oli.crd_stat_cd
            , oli.svc_stat_cd
            , oli.canc_rsn_cd
            , oli.stat_cd
        FROM cr_temp.respsumm_olo_temp olo
            LEFT JOIN cr_temp.respsumm_oli_temp oli
                ON olo.ord_id = oli.ord_id;


DROP TABLE IF EXISTS    cr_temp.respsumm_ph_ij_er_temp; --er
CREATE TABLE            cr_temp.respsumm_ph_ij_er_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
SELECT
    ph.individual_id
    , ph.keycode
    , er.campaign_optout
    , er.clicked
    , er.opened
    , er.bounced
    , er.maint_dt
FROM cr_temp.respsumm_promo_p_temp ph 
        INNER JOIN cr_temp.respsumm_emresp_temp er
            ON er.individual_id = ph.individual_id
                AND ph.keycode = er.keycd
WHERE 1 = 1
      AND ph.bus_unt IN ('ABQ', 'ADV', 'APS', 'NCB', 'UCB', 'NCR', 'UCR', 'FUN', 'CNS', 'CRH', 'CRM', 'CRE', 'MGD', 'MKT', 'PNL', 'SHM')
      AND EXISTS(SELECT NULL
                 FROM cr_temp.respsumm_promo_nt_temp nt
                 WHERE nt.individual_id = er.individual_id
                       AND nt.individual_id = ph.individual_id)
      AND EXISTS(SELECT NULL
                 FROM etl_proc.lookup_di_list_id lk
                 WHERE ph.bus_unt = lk.bus_unit AND lk.list_id = er.list_id);


DROP TABLE IF EXISTS cr_temp.respsumm_aggregation_done_temp; --mt_resp_summary_temp
CREATE TABLE cr_temp.respsumm_aggregation_done_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS WITH cte_pldg AS (SELECT
                             /*SD 20171019: Pledges are deprecated*/
                               cast(NULL AS BIGINT) AS individual_id
                             , cast(NULL AS TEXT)   AS keycode
                             , cast(NULL AS DATE)   AS pldg_dt)
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
                        END), 2)         abq_resp_cd
        , substring(min(CASE WHEN ph.bus_unt = 'ADV'
        THEN CASE
             WHEN adv.individual_id IS NOT NULL AND adv.keycode IS NOT NULL
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
                        END), 2)         adv_resp_cd
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
                        END), 2)         fr_resp_cd
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
                        END), 2)      AS crsch_resp_cd
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
        LEFT JOIN cte_pldg fp
            ON ph.individual_id = fp.individual_id AND ph.keycode = fp.keycode
        LEFT JOIN cr_temp.respsumm_olo_lj_oli_temp oi
            ON ph.individual_id = oi.individual_id AND upper(ph.keycode) = oi.keycode
    WHERE 1 = 1
          AND ph.bus_unt IN ('ABQ', 'ADV', 'APS', 'NCB', 'UCB', 'NCR', 'UCR', 'FUN', 'CNS', 'CRH', 'CRM', 'CRE', 'MGD', 'MKT', 'PNL', 'SHM')
          AND exists(SELECT NULL
                     FROM cr_temp.respsumm_promo_nt_temp nt
                     WHERE nt.individual_id = ph.individual_id)

    GROUP BY ph.individual_id, ph.bus_unt, ph.keycode;


DROP TABLE IF EXISTS    cr_temp.agg_response_summary_acx_encode;
CREATE TABLE 			cr_temp.agg_response_summary_acx_encode
(
    individual_id   bigint      ENCODE DELTA32K
    , bus_unt       varchar(30) ENCODE BYTEDICT /*a column contains a limited number of unique values*/
    , keycode       varchar(30) ENCODE Text255
    , prom_dt       timestamp   ENCODE DELTA32K
    , abq_resp_cd   char        ENCODE BYTEDICT
    , adv_resp_cd   char        ENCODE BYTEDICT
    , resp_dt       timestamp   ENCODE DELTA32K
    , aps_resp_cd   char        ENCODE BYTEDICT
    , fr_resp_cd    char        ENCODE BYTEDICT
    , subs_resp_cd  char        ENCODE BYTEDICT
    , crsch_resp_cd char        ENCODE BYTEDICT
)
    DISTKEY (individual_id)
    INTERLEAVED SORTKEY  (bus_unt, keycode);
;

/*DROP TABLE IF EXISTS    cr_temp.agg_response_summary_acx;
CREATE TABLE            cr_temp.agg_response_summary_acx
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS*/

truncate table cr_temp.agg_response_summary_acx_encode;

INSERT INTO cr_temp.agg_response_summary_acx_encode
SELECT
    individual_id
    , bus_unt
    , keycode
    , prom_dt
    , abq_resp_cd
    , adv_resp_cd
    , CASE WHEN aps_resp_cd IN ('E', 'G')
    THEN olo_ord_dt
      WHEN aps_resp_cd IN ('A', 'C')
          THEN oli_canc_dt
      WHEN aps_resp_cd IN ('N', 'S', 'U', 'W')
          THEN er_maint_dt
      WHEN aps_resp_cd = 'Y'
          THEN ph_prom_dt
      WHEN fr_resp_cd IN ('I', 'O')
          THEN fd_don_dt
      WHEN fr_resp_cd = 'K'
          THEN fp_pldg_dt
      WHEN fr_resp_cd IN ('N', 'S', 'U', 'W')
          THEN er_maint_dt
      WHEN fr_resp_cd = 'Y'
          THEN ph_prom_dt
      WHEN subs_resp_cd IN ('A', 'C')
          THEN nvl(ofo_canc_dt, oli_canc_dt)
      WHEN subs_resp_cd = 'E'
          THEN nvl(ofo_pmt_dt, olo_pd_dt)
      WHEN subs_resp_cd = 'G'
          THEN nvl(ofo_ord_dt, olo_ord_dt)
      WHEN subs_resp_cd IN ('N', 'S', 'U', 'W')
          THEN er_maint_dt
      WHEN subs_resp_cd = 'Y'
          THEN ph_prom_dt
      WHEN abq_resp_cd IN ('M', 'Q')
          THEN to_date(abq_abq_yr || '0601', 'YYYYMMDD')
      WHEN abq_resp_cd IN ('N', 'S', 'U', 'W')
          THEN er_maint_dt
      WHEN abq_resp_cd = 'Y'
          THEN ph_prom_dt
      WHEN adv_resp_cd = 'P'
          THEN adv_last_resp_dt
      WHEN adv_resp_cd IN ('N', 'S', 'U', 'W')
          THEN er_maint_dt
      WHEN adv_resp_cd = 'Y'
          THEN ph_prom_dt
      WHEN crsch_resp_cd IN ('N', 'S', 'U', 'W')
          THEN er_maint_dt
      WHEN crsch_resp_cd = 'Y'
          THEN ph_prom_dt
      END resp_dt
    , aps_resp_cd
    , fr_resp_cd
    , subs_resp_cd
    , crsch_resp_cd
FROM cr_temp.respsumm_aggregation_done_temp
order by individual_id
;