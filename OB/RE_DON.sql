

Ongoing, for files to be output weekly, immediately after MT weekend build completes, per PCR 041:
For updates to existing RE individualss:

Extract all FUNDRAISING_TRANS entries 
where   (
            FUNDRAISING_TRANS.INDIVIDUAL_ID IS in current RE_INDIVIDUALS table And 
           (
                FUNDRAISING_TRANS.MAINT_DT > LAST_EXTRACT_DATE 
                OR  (  FUNDRAISING_TRANS.INDIVIDUAL_ID
                    ,  FUNDRAISING_TRANS.ORIG_BPID
                    ) IN 
                    (
                        SELECT FUNDRAISING_COMBINATION.PRIM_INDIV_ID, FUNDRAISING_TRANS_PRECOMBINE.ORIG_BPID 
                        FROM FUNDRAISING_COMBINATION, FUNDRAISING_TRANS_COMBINE 
                        WHERE FUNDRAISING_COMBINATION.SECD_INDIV_ID = FUNDRAISING_TRANS_PRECOMBINE.INDIVIDUAL_ID 
                        AND FUNDRAISING_TRANS_PRECOMBINE.COMBINED_DT > LAST_EXTRACT_DATE
                     )  
                And FUNDRAISING_TRANS.FNC_CD in ('ANH','AHS','REF','SCN')
            )
        )


For new RE individualss:
Extract all FUNDRAISING_TRANS entries 
where   (
            FUNDRAISING_TRANS.INDIVIDUAL_ID is NOT in current RE_INDIVIDUALS table 
            And FUNDRAISING_TRANS.FNC_CD in ('ANH','AHS','REF','SCN') 
            And (
                        MT_INDIVIDUAL.CGA_FLG = 'Y'
                    Or (MT_FR_DONATION.DON_AMT >= 100 AND MT_FR_DONATION.DON_DT >= '20040601' on the same donation row for a given individuals)
                    Or MT_FR_SUMMARY.FR_LTD_DON_AMT >= 5000 
                )
        )


ORDER BY APPLCOD ASC 


Special request for ongoing, per update to PCR 041 and effective as of 12/8/2006:  
exclude individual ids 1000200101269706, 10002000131940010, 10002000611371060, 10002000136474010 from the actual output file.
SD note: 17-digit are legacy individuals and are handled through prod.rm_icd_lookup table



SELECT
--     ache.individual_id
--     , funt.fnc_code
--     , funt.update_date

  ache.INDIVIDUAL_ID as CSCID_INDIV
, cast(' ' as varchar(1)) as CNTRLNUM
, to_char(ache.action_date,'MM/DD/YYYY') as RECTDATE
, ache.action_amount as 

-- RECTAMNT FUNDRAISING_TRANS.TRN_AMT (note:  if fnc_cd = REF make sure to output trn_amt as negative value)
-- APPLCOD  FUNDRAISING_TRANS.APL_KEYCD
-- FRMOFPAY FUNDRAISING_TRANS.PMT_TYP_CD
-- CSC_IND_URN  MT_INDIVIDUAL.IND_URN



FROM prod.action_header ache
    INNER JOIN prod.fundraising_transaction funt
        ON ache.hash_action_id = funt.hash_action_id

where fnc_code in ('ANH','AHS','REF','SCN')
LIMIT 100
;