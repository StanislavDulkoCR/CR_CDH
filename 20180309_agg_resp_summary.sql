
/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  agg_response_summary.sql
* Date       :  2018/02/23
* Dev & QA   :  Stanislav Dulko
***************************************************************************/

/* respsumm_promo_nt_temp */
DROP TABLE IF EXISTS agg.respsumm_promo_nt_temp;
CREATE TABLE agg.respsumm_promo_nt_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS
        SELECT DISTINCT t1.individual_id
        FROM prod.promotion t1
        WHERE t1.contact_type != 'T' /*SD OLD: WHERE nvl(t1.contact_type,'P') != 'T'*/

        ORDER BY t1.individual_id
-- XN Sort  (cost=1000040799479.49..1000040824255.16 rows=9910269 width=8)
-- XN Unique  (cost=0.00..39,647,881.81 rows= 9910269 width=8)
 ;
ANALYZE agg.respsumm_promo_nt_temp;


/* respsumm_promo_p_temp */
/*completed in 13m 17s 376ms*/
DROP TABLE IF EXISTS agg.respsumm_promo_p_temp;
CREATE TABLE agg.respsumm_promo_p_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id, keycode)
    AS
        SELECT DISTINCT
            t1.individual_id
            , t1.Business_Unit  AS bus_unt
            , t1.keycode
            , t1.Promotion_Date AS prom_dt
        FROM prod.promotion t1
        WHERE t1.contact_type = 'P'
              AND exists(SELECT NULL
                         FROM agg.respsumm_promo_nt_temp nt
                         WHERE nt.individual_id = t1.individual_id)
        ORDER BY t1.individual_id
--  XN Sort  (cost=1000040799479.49..1000040824255.16 rows=9910269 width=8)
--  XN Unique  (cost=404798.93..878384150.10 rows=239525537 width=35)
 ;
ANALYZE agg.respsumm_promo_p_temp;


/* respsumm_olo_temp */
/*completed in 10s 351ms*/
DROP TABLE IF EXISTS agg.respsumm_olo_temp;
CREATE TABLE agg.respsumm_olo_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id, ord_id)
    AS
        SELECT
            olo.individual_id
            , olo.pd_dt
            , olo.ord_dt
            , olo.ord_id
        FROM prod.agg_digital_order olo
        WHERE exists
			(SELECT NULL
            FROM agg.respsumm_promo_nt_temp nt
            WHERE nt.individual_id = olo.individual_id);
ANALYZE agg.respsumm_olo_temp;


analyze prod.agg_digital_item;
/* respsumm_oli_temp */
/*completed in 7s 478ms*/
DROP TABLE IF EXISTS agg.respsumm_oli_temp;
CREATE TABLE agg.respsumm_oli_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id, ord_id)
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
        WHERE exists
			(SELECT NULL
			FROM agg.respsumm_promo_nt_temp nt
			WHERE nt.individual_id = oli.individual_id);
ANALYZE agg.respsumm_oli_temp;


/* respsumm_ofo_temp */
DROP TABLE IF EXISTS    agg.respsumm_ofo_temp;
CREATE TABLE agg.respsumm_ofo_temp
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
        WHERE exists
			(SELECT NULL
			FROM agg.respsumm_promo_nt_temp nt
			WHERE nt.individual_id = ofo.individual_id);
ANALYZE agg.respsumm_ofo_temp;  /*1 hour 06 minutes*/


/* respsumm_emresp_temp */
/*completed in 6m 6s 627ms*/
DROP TABLE IF EXISTS agg.respsumm_emresp_temp;
CREATE TABLE agg.respsumm_emresp_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id, keycd)
    AS SELECT
            emr.individual_id
            , emr.campaign_optout
            , emr.clicked
            , emr.Opened_Flag  AS opened
            , emr.Bounced_Flag AS bounced
            , emr.create_date  AS maint_dt
            , emr.keycode      AS keycd
            , emr.list_id
        FROM prod.email_response emr
        WHERE emr.list_id NOT IN ('10242', '11367', '11368')
              AND emr.keycode IS NOT NULL
              AND exists
			  	(SELECT NULL
				FROM agg.respsumm_promo_nt_temp nt
				WHERE nt.individual_id = emr.individual_id)
        order by emr.individual_id
-- XN Sort  (cost=1000583658550.69..1000586489206.16 rows=1132262189 width=61)
-- XN Hash IN Join DS_DIST_NONE  (cost=404798.93..413385787.14 rows=1132262189 width=61)
 ;
ANALYZE agg.respsumm_emresp_temp;


/* respsumm_olo_lj_oli_temp */
 /*completed in 10s 654ms*/
DROP TABLE IF EXISTS agg.respsumm_olo_lj_oli_temp;
CREATE TABLE agg.respsumm_olo_lj_oli_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS SELECT
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
        FROM agg.respsumm_olo_temp olo		 /* potential optmization point */
        LEFT JOIN agg.respsumm_oli_temp oli  /* potential optmization point */
            ON olo.ord_id = oli.ord_id; 	 /* potential optmization point */
ANALYZE agg.respsumm_olo_lj_oli_temp;


/* respsumm_ph_ij_er_temp */
DROP TABLE IF EXISTS agg.respsumm_ph_ij_er_temp;
CREATE TABLE agg.respsumm_ph_ij_er_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id, keycode)
    AS
SELECT
--     ph.individual_id
--     , ph.keycode
--     , er.campaign_optout
--     , er.clicked
--     , er.opened
--     , er.bounced
--     , er.maint_dt
    count(*)
FROM agg.respsumm_promo_p_temp ph
        INNER JOIN agg.respsumm_emresp_temp er
            ON er.individual_id = ph.individual_id
                AND ph.keycode = er.keycd
WHERE ph.bus_unt IN ('ABQ', 'ADV', 'APS', 'NCB', 'UCB', 'NCR', 'UCR', 'FUN', 'CNS', 'CRH', 'CRM', 'CRE', 'MGD', 'MKT', 'PNL', 'SHM')
      AND EXISTS(SELECT NULL
                 FROM agg.respsumm_promo_nt_temp nt
                 WHERE nt.individual_id = er.individual_id
--                       AND nt.individual_id = ph.individual_id /*SD It's an inner join anyway. Explain plan is much better without; count w: 3m 14s 406ms; count w/o: 2m 10s 447ms*/
      )
      AND EXISTS(SELECT NULL
                 FROM etl_proc.lookup_di_list_id lk
                 WHERE ph.bus_unt = lk.bus_unit AND lk.list_id = er.list_id)
-- XN Merge IN Join DS_DIST_NONE  (cost=170929017.87..   484786569.76 rows=6730202043 width=57)
-- XN Hash Join DS_BCAST_INNER  (cost=3774307946.85..7616143989702.89 rows=4494773232 width=57)
-- XN Hash Join DS_DIST_NONE  (cost=769981968.12..   7580202832432.70 rows=11824475760 width=57)
-- XN Merge Join DS_DIST_NONE  (cost=0.00..             1834035054.85 rows=17859576492 width=57)
 ;
ANALYZE agg.respsumm_ph_ij_er_temp; -- 3hours


/* respsumm_aggregation_done_temp */
DROP TABLE IF EXISTS agg.respsumm_aggregation_done_temp;
CREATE TABLE agg.respsumm_aggregation_done_temp
    DISTSTYLE KEY
    DISTKEY (individual_id)
    SORTKEY (individual_id)
    AS WITH cte_pldg AS
		(SELECT /*SD 20171019: Pledges are deprecated*/
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
    FROM agg.respsumm_promo_p_temp ph
        LEFT JOIN prod.agg_abq_response abq
            ON ph.individual_id = abq.individual_id AND ph.keycode = abq.abq_yr || abq.keycode
        LEFT JOIN agg.respsumm_ph_ij_er_temp er
            ON ph.individual_id = er.individual_id AND ph.keycode = er.keycode
        LEFT JOIN prod.legacy_constituent_response adv
            ON ph.individual_id = adv.individual_id AND ph.keycode = adv.keycode
        LEFT JOIN prod.agg_fundraising_donation fd
            ON ph.individual_id = fd.individual_id AND ph.keycode = fd.keycode
        LEFT JOIN agg.respsumm_ofo_temp ofo
            ON ph.individual_id = ofo.individual_id AND ph.keycode = ofo.keycode AND ph.bus_unt = ofo.mag_cd
        LEFT JOIN cte_pldg fp
            ON ph.individual_id = fp.individual_id AND ph.keycode = fp.keycode
        LEFT JOIN agg.respsumm_olo_lj_oli_temp oi
            ON ph.individual_id = oi.individual_id AND upper(ph.keycode) = oi.keycode
    WHERE ph.bus_unt IN ('ABQ', 'ADV', 'APS', 'NCB', 'UCB', 'NCR', 'UCR', 'FUN', 'CNS', 'CRH', 'CRM', 'CRE', 'MGD', 'MKT', 'PNL', 'SHM')
          -- AND exists(SELECT NULL
          --            FROM agg.respsumm_promo_nt_temp nt
          --            WHERE nt.individual_id = ph.individual_id)  SD 2017/10/20 22:06: agg.respsumm_promo_p_temp ph is already filtered by the same exists
    GROUP BY ph.individual_id, ph.bus_unt, ph.keycode;
ANALYZE agg.respsumm_aggregation_done_temp;


-- /* ztemp_agg_response_summary */
-- DROP TABLE IF EXISTS prod.ztemp_agg_response_summary;
-- CREATE TABLE prod.ztemp_agg_response_summary
-- (
--     individual_id   bigint      NOT NULL ENCODE DELTA32K DISTKEY
--     , bus_unt       varchar(30) NOT NULL ENCODE BYTEDICT /*a column contains a limited number of unique values*/
--     , keycode       varchar(30) NOT NULL ENCODE Text255
--     , prom_dt       timestamp   ENCODE DELTA32K
--     , abq_resp_cd   char        ENCODE BYTEDICT
--     , adv_resp_cd   char        ENCODE BYTEDICT
--     , resp_dt       timestamp   ENCODE DELTA32K
--     , aps_resp_cd   char        ENCODE BYTEDICT
--     , fr_resp_cd    char        ENCODE BYTEDICT
--     , subs_resp_cd  char        ENCODE BYTEDICT
--     , crsch_resp_cd char        ENCODE BYTEDICT
-- 	, key_response_summary varchar(64) NOT NULL ENCODE lzo
-- )
--     INTERLEAVED SORTKEY  (bus_unt, keycode);


-- INSERT INTO prod.ztemp_agg_response_summary
INSERT INTO prod.agg_response_summary
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
	, MD5(individual_id||bus_unt||keycode)
FROM agg.respsumm_aggregation_done_temp
order by individual_id;


/* SWAP TEMP FOR AGG */
-- DROP TABLE IF EXISTS prod.agg_response_summary;
-- ALTER TABLE prod.ztemp_agg_response_summary
-- RENAME TO agg_response_summary;
--
-- ALTER TABLE prod.agg_response_summary
-- 		ADD CONSTRAINT agg_response_summary_pkey
-- 		PRIMARY KEY (individual_id, bus_unt, keycode);

ANALYZE prod.agg_response_summary;

