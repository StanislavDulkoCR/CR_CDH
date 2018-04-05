DROP TABLE IF EXISTS cr_temp.acx_agg_preference;
CREATE TABLE cr_temp.acx_agg_preference
	DISTSTYLE KEY DISTKEY(individual_id) 
	INTERLEAVED SORTKEY(individual_id) 
AS
WITH ACX_AGG_PREFERENCE (individual_id, assoc_typ_cd, auth_cd, scp_cd, data_source, auth_flg, fst_dt, eff_dt) as (
SELECT cast(a.id_value as bigint) as individual_id
  , a.id_type
  , a.preference_code
  , a.preference_scope
  , a.data_source
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN MIN(a.preference_value)
        WHEN a.data_source IN ('KBL','LSR','DCK')
        THEN MAX(a.preference_value)
        ELSE NULL
    END auth_flg
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN NVL(MIN(DECODE(a.preference_value,'N',a.start_date,NULL)),MIN(a.start_date))
        WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
        THEN NVL(MIN(DECODE(a.preference_value,'Y',a.start_date,NULL)),MIN(a.start_date))
        ELSE NULL
    END fst_dt
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN NVL(MIN(DECODE(a.preference_value,'N',a.effective_date,NULL)),MIN(a.effective_date))
        WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
        THEN NVL(MIN(DECODE(a.preference_value,'Y',a.effective_date,NULL)),MIN(a.effective_date))
        ELSE NULL
    END eff_dt
FROM  prod.preference_history a
WHERE ( (a.id_type = 'I')
    AND a.preference_value IS NOT NULL)
GROUP BY a.id_value
  , a.id_type
  , a.preference_code
  , a.preference_scope
  , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
SELECT me.individual_id
  , a.id_type
  , a.preference_code
  , a.preference_scope
  , a.data_source
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN MIN(a.preference_value)
        WHEN a.data_source IN ('KBL', 'LSR', 'DCK')
        THEN MAX(a.preference_value)
        ELSE NULL
    END auth_flg
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN NVL(MIN(DECODE(a.preference_value,'N',a.start_date,NULL)),MIN(a.start_date))
        WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
        THEN NVL(MIN(DECODE(a.preference_value,'Y',a.start_date,NULL)),MIN(a.start_date))
        ELSE NULL
    END fst_dt
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN NVL(MIN(DECODE(a.preference_value,'N',a.effective_date,NULL)),MIN(a.effective_date))
        WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
        THEN NVL(MIN(DECODE(a.preference_value,'Y',a.effective_date,NULL)),MIN(a.effective_date))
        ELSE NULL
    END eff_dt
FROM (prod.agg_email me
INNER JOIN prod.preference_history a
ON (me.email_address = a.id_value))
WHERE ( ( ( ( (me.EMAIL_TYPE_CD = 'I')
    AND a.id_type = 'E')
    AND a.preference_scope <> 'PNL')
    AND a.preference_scope NOT LIKE 'NEW%')
    AND a.preference_value IS NOT NULL)
GROUP BY me.individual_id
  , a.id_type
  , a.preference_code
  , a.preference_scope
  , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
SELECT me.individual_id
  , DECODE(a.id_type,'E','M')
  , a.preference_code
  , a.preference_scope
  , a.data_source
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN MIN(a.preference_value)
        WHEN a.data_source IN ('KBL', 'LSR', 'DCK')
        THEN MAX(a.preference_value)
        ELSE NULL
    END auth_flg
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN NVL(MIN(DECODE(a.preference_value,'N',a.start_date,NULL)),MIN(a.start_date))
        WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
        THEN NVL(MIN(DECODE(a.preference_value,'Y',a.start_date,NULL)),MIN(a.start_date))
        ELSE NULL
    END fst_dt
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN NVL(MIN(DECODE(a.preference_value,'N',a.effective_date,NULL)),MIN(a.effective_date))
        WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
        THEN NVL(MIN(DECODE(a.preference_value,'Y',a.effective_date,NULL)),MIN(a.effective_date))
        ELSE NULL
    END eff_dt
FROM (prod.agg_email me
INNER JOIN prod.preference_history a
ON (me.email_address = a.id_value))
WHERE ( ( ( ( (me.EMAIL_TYPE_CD = 'M')
    AND a.id_type = 'E')
    AND a.preference_code = 'EMAIL')
    AND a.preference_scope = 'PNL')
    AND a.preference_value IS NOT NULL)
GROUP BY me.individual_id
  , a.id_type
  , a.preference_code
  , a.preference_scope
  , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
SELECT me.individual_id
  , DECODE(a.id_type,'E','N')
  , a.preference_code
  , a.preference_scope
  , a.data_source
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN MIN(a.preference_value)
        WHEN a.data_source IN ('KBL', 'LSR', 'DCK')
        THEN MAX(a.preference_value)
        ELSE NULL
    END auth_flg
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN NVL(MIN(DECODE(a.preference_value,'N',a.start_date,NULL)),MIN(a.start_date))
        WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
        THEN NVL(MIN(DECODE(a.preference_value,'Y',a.start_date,NULL)),MIN(a.start_date))
        ELSE NULL
    END fst_dt
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN NVL(MIN(DECODE(a.preference_value,'N',a.effective_date,NULL)),MIN(a.effective_date))
        WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
        THEN NVL(MIN(DECODE(a.preference_value,'Y',a.effective_date,NULL)),MIN(a.effective_date))
        ELSE NULL
    END eff_dt
FROM (prod.agg_email me
INNER JOIN prod.preference_history a
ON (me.email_address = a.id_value))
WHERE ( ( ( ( (me.EMAIL_TYPE_CD = 'N')
    AND a.id_type = 'E')
    AND a.preference_code = 'EMAIL')
    AND a.preference_scope LIKE 'NEW%')
    AND a.preference_value IS NOT NULL)
GROUP BY me.individual_id
  , a.id_type
  , a.preference_code
  , a.preference_scope
  , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
SELECT me.individual_id
  , DECODE(a.id_type,'E','C')
  , a.preference_code
  , a.preference_scope
  , a.data_source
  , CASE
        WHEN a.data_source IN ('CVO')
        THEN MIN(a.preference_value)
        ELSE NULL
    END auth_flg
  , CASE
        WHEN a.data_source IN ('CVO')
        THEN NVL(MIN(DECODE(a.preference_value,'N',a.start_date,NULL)),MIN(a.start_date))
        ELSE NULL
    END fst_dt
  , CASE
        WHEN a.data_source IN ('CVO')
        THEN NVL(MIN(DECODE(a.preference_value,'N',a.effective_date,NULL)),MIN(a.effective_date))
        ELSE NULL
    END eff_dt
FROM (prod.agg_email me
INNER JOIN prod.preference_history a
ON (me.email_address = a.id_value))
WHERE ( ( ( ( ( ( me.EMAIL_TYPE_CD = 'A')
    AND a.id_type = 'E')
    AND a.DATA_SOURCE = 'CVO')
    AND a.preference_code = 'EMAIL')
    AND a.preference_scope IN ('ADV', 'CVONEWS', 'CVOAA', 'CVOFR'))
    AND a.preference_value IS NOT NULL)
GROUP BY me.individual_id
  , a.id_type
  , a.preference_code
  , a.preference_scope
  , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
SELECT ia.individual_id
  , a.id_type
  , a.preference_code
  , a.preference_scope
  , a.data_source
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN MIN(a.preference_value)
        WHEN a.data_source IN ('KBL', 'LSR', 'DCK')
        THEN MAX(a.preference_value)
        ELSE NULL
    END auth_flg
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN NVL(MIN(DECODE(a.preference_value,'N',a.start_date,NULL)),MIN(a.start_date))
        WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
        THEN NVL(MIN(DECODE(a.preference_value,'Y',a.start_date,NULL)),MIN(a.start_date))
        ELSE NULL
    END fst_dt
  , CASE
        WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
        THEN NVL(MIN(DECODE(a.preference_value,'N',a.effective_date,NULL)),MIN(a.effective_date))
        WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
        THEN NVL(MIN(DECODE(a.preference_value,'Y',a.effective_date,NULL)),MIN(a.effective_date))
        ELSE NULL
    END eff_dt
FROM (prod.individual_address ia
INNER JOIN prod.preference_history a
ON (ia.ADDRESS_ID = a.id_value))
WHERE ( (a.id_type = 'A')
    AND a.preference_value IS NOT NULL)
GROUP BY ia.individual_id
  , a.id_type
  , a.preference_code
  , a.preference_scope
  , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
-- --V--Stanis:Telecom/Individual Phone are being commented out at Ed's request. We are assuming Telephones will be deprecated.
-- UNION ALL
-- -------------------------------------------------------------------------------------------,
-- SELECT
-- ix.acx_rm_new_individual_id,
-- a.id_type,
-- a.preference_code,
-- a.preference_scope,
-- a.data_source,
-- CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
-- THEN min(a.preference_value)
-- WHEN a.data_source IN ('KBL', 'LSR', 'DCK')
-- THEN max(a.preference_value)
-- ELSE null
-- END auth_flg,
-- CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
-- THEN nvl(min(decode(a.preference_value,'N',a.start_date,null)),min(a.start_date))
-- WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
-- THEN nvl(min(decode(a.preference_value,'Y',a.start_date,null)),min(a.start_date))
-- ELSE null
-- END fst_dt,
-- CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
-- THEN nvl(min(decode(a.preference_value,'N',a.effective_date,null)),min(a.effective_date))
-- WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
-- THEN nvl(min(decode(a.preference_value,'Y',a.effective_date,null)),min(a.effective_date))
-- ELSE null
-- END eff_dt
-- FROM (prod.individual_phone t
-- INNER JOIN prod.preference_history a
-- ON (t.acx_valid_phone = a.id_value))
-- INNER JOIN prod.rm_icd_lookup ix
-- ON (ix.acx_rm_new_individual_id = t.INDIVIDUAL_ID)
-- WHERE ( ( a.id_type = 'T')
-- AND a.preference_value IS NOT NULL)
-- GROUP BY ix.acx_rm_new_individual_id
-- , a.id_type
-- , a.preference_code
-- , a.preference_scope
-- , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
SELECT acc.individual_id
  , 'I' assoc_typ_cd
  , 'GFTREN' auth_cd
  , 'GLB' scp_cd
  , 'CDS' data_source
  , 'N' auth_flg
  , sysdate fst_dt
  , sysdate eff_dt
FROM prod.account acc
INNER JOIN prod.print_account_detail pad
ON  pad.hash_account_id = acc.hash_account_id
WHERE 1=1
    --V--Stanis: Offline/Print accounts only. ICD.OFFLINE_ACCOUNT
    AND acc.source_name = 'CDS'
    AND acc.account_subtype_code IN ('CNS', 'CRH', 'CRM', 'SHM')
    AND NVL (substring(pad.gift_keycode, 1, 1), 'X') NOT IN ('*', '/', '&', 'X')
    --V--Stanis: excluding books for good measure
    AND NOT EXISTS
    (SELECT NULL FROM prod.books_account ba WHERE ba.hash_account_id = acc.hash_account_id
    )
GROUP BY acc.individual_id
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
SELECT ACC.INDIVIDUAL_ID
  , 'I' assoc_typ_cd
  , 'GFTOPT' auth_cd
  , 'GLB' scp_cd
  , 'CDS' data_source
  , 'N' auth_flg
  , sysdate fst_dt
  , sysdate eff_dt
FROM prod.account acc
inner JOIN prod.print_account_detail pad
ON  pad.hash_account_id = acc.hash_account_id
WHERE 1=1
    --V--Stanis: Offline/Print accounts only. ICD.OFFLINE_ACCOUNT
    AND acc.source_name = 'CDS'
    AND acc.account_subtype_code IN ( 'CNS', 'CRH', 'CRM', 'CRT', 'CRE', 'SHM' )
    AND substring(pad.gift_keycode, 1, 1) IN ( '*', '/', '&' )
    --V--Stanis: excluding books for good measure
    AND NOT EXISTS
    (SELECT NULL FROM prod.books_account ba WHERE ba.hash_account_id = acc.hash_account_id
    )
group by ACC.INDIVIDUAL_ID
)

select *
from acx_agg_preference
;