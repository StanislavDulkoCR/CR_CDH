create table crprod_cdh.cr_temp.cdh_id_xref_noconsbaparts as
SELECT distinct ACX_RM_NEW_INDIVIDUAL_ID as cdh_id, source_indiv_id as icd_id
                FROM prod.match_instance rm1
                WHERE NOT EXISTS
                    (SELECT NULL
                    FROM prod.match_instance RM2
                    WHERE RM1.ACX_RM_NEW_INDIVIDUAL_ID = RM2.ACX_RM_NEW_INDIVIDUAL_ID
                        and RM1.source_indiv_id != RM2.source_indiv_id
                ) and NOT EXISTS
                    (SELECT NULL
                    FROM prod.match_instance RM3
                    WHERE RM1.ACX_RM_NEW_INDIVIDUAL_ID != RM3.ACX_RM_NEW_INDIVIDUAL_ID
                        and RM1.source_indiv_id = RM3.source_indiv_id
                )
				order by ACX_RM_NEW_INDIVIDUAL_ID asc, source_indiv_id asc

				;
;
SELECT TOP 100 *
FROM cr_temp.cdh_id_xref_noconsbaparts
WHERE 1=1
;
SELECT /*TOP 1000*/ icd_id, individual_id as CDH_INDIVIDUAL_ID, don_amt, to_char(don_dt, 'YYYY-MM-DD HH24:MI:SS') as don_dt
from prod.agg_cga t_main  inner join cr_temp.cdh_id_xref_noconsbaparts rm on t_main.individual_id = rm.cdh_id where 1=1  and 2=2 /*and exists (select null from ACXIOM_TO_CR_DAILY_0710_FLTRD rm
                    where rm.cdh_id = t_main.individual_id)*/ order by 1 asc, 2 asc, 3 asc

					;

					select distinct INDIVIDUAL_ID, DON_DT, DON_AMT
from PROD.AGG_CGA CGA
where 1=1
and INDIVIDUAL_ID  = 1200415208

;
;
select  INDIVIDUAL_ID
,SURVEY_ID
,TO_CHAR(SUBMIT_DT, 'YYYY-MM-DD HH24:MI:SS')

FROM prod.agg_constituent_survey_response
where 1=1
and INDIVIDUAL_ID = 1205974859
order by 1 asc, SURVEY_ID::int asc, 3

;
;
;
select top 100 individual_id as CDH_INDIVIDUAL_ID,alert_response_id,to_char(response_dt, 'YYYY-MM-DD HH24:MI:SS.MS') as response_dt, response_dt

from prod.agg_constituent_alert_response
