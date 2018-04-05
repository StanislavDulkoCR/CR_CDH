SELECT
  ix.individual_id,
  a.assoc_typ_cd,
  a.auth_cd,
  a.scp_cd,
  a.data_source,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN min(a.auth_flg)
       WHEN a.data_source IN ('KBL','LSR','DCK')
       THEN max(a.auth_flg)
       ELSE null
  END auth_flg,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN nvl(min(decode(a.auth_flg,'N',a.fst_dt,null)),min(a.fst_dt))
       WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
       THEN nvl(min(decode(a.auth_flg,'Y',a.fst_dt,null)),min(a.fst_dt))
       ELSE null
  END fst_dt,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN nvl(min(decode(a.auth_flg,'N',a.eff_dt,null)),min(a.eff_dt))
       WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
       THEN nvl(min(decode(a.auth_flg,'Y',a.eff_dt,null)),min(a.eff_dt))
       ELSE null
  END eff_dt
FROM MIDDLE_TIER2.IND_XREF ix
INNER JOIN WAREHOUSE2.AUTHORIZATION a
ON (ix.INDIVIDUAL_ID = a.ASSOC_ID)
WHERE ( (ix.ACTIVE_FLAG = 'A'
    AND a.ASSOC_TYP_CD = 'I')
    AND a.AUTH_FLG IS NOT NULL)
GROUP BY ix.INDIVIDUAL_ID
  , a.ASSOC_TYP_CD
  , a.AUTH_CD
  , a.SCP_CD
  , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
SELECT
  ix.individual_id,
  a.assoc_typ_cd,
  a.auth_cd,
  a.scp_cd,
  a.data_source,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN min(a.auth_flg)
       WHEN a.data_source IN ('KBL', 'LSR', 'DCK')
       THEN max(a.auth_flg)
       ELSE null
  END auth_flg,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN nvl(min(decode(a.auth_flg,'N',a.fst_dt,null)),min(a.fst_dt))
       WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
       THEN nvl(min(decode(a.auth_flg,'Y',a.fst_dt,null)),min(a.fst_dt))
       ELSE null
  END fst_dt,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN nvl(min(decode(a.auth_flg,'N',a.eff_dt,null)),min(a.eff_dt))
       WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
       THEN nvl(min(decode(a.auth_flg,'Y',a.eff_dt,null)),min(a.eff_dt))
       ELSE null
  END eff_dt
FROM (MIDDLE_TIER2.MT_EMAIL me
INNER JOIN WAREHOUSE2.AUTHORIZATION a
ON (me.EMAIL_ID = a.ASSOC_ID))
INNER JOIN MIDDLE_TIER2.IND_XREF ix
ON (ix.INDIVIDUAL_ID = me.INDIVIDUAL_ID)
WHERE ( ( ( ( (ix.ACTIVE_FLAG = 'A'
    AND me.EMAIL_TYPE_CD = 'I')
    AND a.ASSOC_TYP_CD = 'E')
    AND a.SCP_CD <> 'PNL')
    AND a.SCP_CD NOT LIKE 'NEW%')
    AND a.AUTH_FLG IS NOT NULL)
GROUP BY ix.INDIVIDUAL_ID
  , a.ASSOC_TYP_CD
  , a.AUTH_CD
  , a.SCP_CD
  , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
SELECT
  ix.individual_id,
  decode(a.assoc_typ_cd,'E','M'),
  a.auth_cd,
  a.scp_cd,
  a.data_source,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN min(a.auth_flg)
       WHEN a.data_source IN ('KBL', 'LSR', 'DCK')
       THEN max(a.auth_flg)
       ELSE null
  END auth_flg,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN nvl(min(decode(a.auth_flg,'N',a.fst_dt,null)),min(a.fst_dt))
       WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
       THEN nvl(min(decode(a.auth_flg,'Y',a.fst_dt,null)),min(a.fst_dt))
       ELSE null
  END fst_dt,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN nvl(min(decode(a.auth_flg,'N',a.eff_dt,null)),min(a.eff_dt))
       WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
       THEN nvl(min(decode(a.auth_flg,'Y',a.eff_dt,null)),min(a.eff_dt))
       ELSE null
  END eff_dt
FROM (MIDDLE_TIER2.MT_EMAIL me
INNER JOIN WAREHOUSE2.AUTHORIZATION a
ON (me.EMAIL_ID = a.ASSOC_ID))
INNER JOIN MIDDLE_TIER2.IND_XREF ix
ON (ix.INDIVIDUAL_ID = me.INDIVIDUAL_ID)
WHERE ( ( ( ( (ix.ACTIVE_FLAG = 'A'
    AND me.EMAIL_TYPE_CD = 'M')
    AND a.ASSOC_TYP_CD = 'E')
    AND a.AUTH_CD = 'EMAIL')
    AND a.SCP_CD = 'PNL')
    AND a.AUTH_FLG IS NOT NULL)
GROUP BY ix.INDIVIDUAL_ID
  , a.ASSOC_TYP_CD
  , a.AUTH_CD
  , a.SCP_CD
  , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
SELECT
  ix.individual_id,
  decode(a.assoc_typ_cd,'E','N'),
  a.auth_cd,
  a.scp_cd,
  a.data_source,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN min(a.auth_flg)
       WHEN a.data_source IN ('KBL', 'LSR', 'DCK')
       THEN max(a.auth_flg)
       ELSE null
  END auth_flg,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN nvl(min(decode(a.auth_flg,'N',a.fst_dt,null)),min(a.fst_dt))
       WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
       THEN nvl(min(decode(a.auth_flg,'Y',a.fst_dt,null)),min(a.fst_dt))
       ELSE null
  END fst_dt,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN nvl(min(decode(a.auth_flg,'N',a.eff_dt,null)),min(a.eff_dt))
       WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
       THEN nvl(min(decode(a.auth_flg,'Y',a.eff_dt,null)),min(a.eff_dt))
       ELSE null
  END eff_dt
FROM (MIDDLE_TIER2.MT_EMAIL me
INNER JOIN WAREHOUSE2.AUTHORIZATION a
ON (me.EMAIL_ID = a.ASSOC_ID))
INNER JOIN MIDDLE_TIER2.IND_XREF ix
ON (ix.INDIVIDUAL_ID = me.INDIVIDUAL_ID)
WHERE ( ( ( ( (ix.ACTIVE_FLAG = 'A'
    AND me.EMAIL_TYPE_CD = 'N')
    AND a.ASSOC_TYP_CD = 'E')
    AND a.AUTH_CD = 'EMAIL')
    AND a.SCP_CD LIKE 'NEW%')
    AND a.AUTH_FLG IS NOT NULL)
GROUP BY ix.INDIVIDUAL_ID
  , a.ASSOC_TYP_CD
  , a.AUTH_CD
  , a.SCP_CD
  , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
SELECT
  ix.individual_id,
  decode(a.assoc_typ_cd,'E','C'),
  a.auth_cd,
  a.scp_cd,
  a.data_source,
  CASE WHEN a.data_source IN ('CVO')
       THEN min(a.auth_flg)
       ELSE null
  END auth_flg,
  CASE WHEN a.data_source IN ('CVO')
       THEN nvl(min(decode(a.auth_flg,'N',a.fst_dt,null)),min(a.fst_dt))
       ELSE null
  END fst_dt,
  CASE WHEN a.data_source IN ('CVO')
       THEN nvl(min(decode(a.auth_flg,'N',a.eff_dt,null)),min(a.eff_dt))
       ELSE null
  END eff_dt
FROM (MIDDLE_TIER2.MT_EMAIL me
INNER JOIN WAREHOUSE2.AUTHORIZATION a
ON (me.EMAIL_ID = a.ASSOC_ID))
INNER JOIN MIDDLE_TIER2.IND_XREF ix
ON (ix.INDIVIDUAL_ID = me.INDIVIDUAL_ID)
WHERE ( ( ( ( ( ( ix.ACTIVE_FLAG = 'A'
    AND me.EMAIL_TYPE_CD = 'A')
    AND a.ASSOC_TYP_CD = 'E')
    AND a.DATA_SOURCE = 'CVO')
    AND a.AUTH_CD = 'EMAIL')
    AND a.SCP_CD IN ('ADV', 'CVONEWS', 'CVOAA', 'CVOFR'))
    AND a.AUTH_FLG IS NOT NULL)
GROUP BY ix.INDIVIDUAL_ID
  , a.ASSOC_TYP_CD
  , a.AUTH_CD
  , a.SCP_CD
  , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
SELECT
  ix.individual_id,
  a.assoc_typ_cd,
  a.auth_cd,
  a.scp_cd,
  a.data_source,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN min(a.auth_flg)
       WHEN a.data_source IN ('KBL', 'LSR', 'DCK')
       THEN max(a.auth_flg)
       ELSE null
  END auth_flg,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN nvl(min(decode(a.auth_flg,'N',a.fst_dt,null)),min(a.fst_dt))
       WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
       THEN nvl(min(decode(a.auth_flg,'Y',a.fst_dt,null)),min(a.fst_dt))
       ELSE null
  END fst_dt,
  CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       THEN nvl(min(decode(a.auth_flg,'N',a.eff_dt,null)),min(a.eff_dt))
       WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
       THEN nvl(min(decode(a.auth_flg,'Y',a.eff_dt,null)),min(a.eff_dt))
       ELSE null
  END eff_dt
FROM (WAREHOUSE2.INDIVIDUAL_ADDRESS ia
INNER JOIN WAREHOUSE2.AUTHORIZATION a
ON (ia.ADDRESS_ID = a.ASSOC_ID))
INNER JOIN MIDDLE_TIER2.IND_XREF ix
ON (ix.INDIVIDUAL_ID = ia.INDIVIDUAL_ID)
WHERE ( (ix.ACTIVE_FLAG = 'A'
    AND a.ASSOC_TYP_CD = 'A')
    AND a.AUTH_FLG IS NOT NULL)
GROUP BY ix.INDIVIDUAL_ID
  , a.ASSOC_TYP_CD
  , a.AUTH_CD
  , a.SCP_CD
  , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
--Telecom/Individual Phone are being commented out at Ed's request. We are assuming Telephones will be deprecated.
-- UNION ALL
-- -------------------------------------------------------------------------------------------,
-- SELECT
  -- ix.individual_id,
  -- a.assoc_typ_cd,
  -- a.auth_cd,
  -- a.scp_cd,
  -- a.data_source,
  -- CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       -- THEN min(a.auth_flg)
       -- WHEN a.data_source IN ('KBL', 'LSR', 'DCK')
       -- THEN max(a.auth_flg)
       -- ELSE null
  -- END auth_flg,
  -- CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       -- THEN nvl(min(decode(a.auth_flg,'N',a.fst_dt,null)),min(a.fst_dt))
       -- WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
       -- THEN nvl(min(decode(a.auth_flg,'Y',a.fst_dt,null)),min(a.fst_dt))
       -- ELSE null
  -- END fst_dt,
  -- CASE WHEN a.data_source IN ('TLS','DM','AX','AG','FC','IB','CDS','PWI','BMO','DGI','CDB','DMP','CCC','CU','CVO','AMP','CA','FAD')
       -- THEN nvl(min(decode(a.auth_flg,'N',a.eff_dt,null)),min(a.eff_dt))
       -- WHEN a.data_source IN ('KBL', 'DCK', 'LSR')
       -- THEN nvl(min(decode(a.auth_flg,'Y',a.eff_dt,null)),min(a.eff_dt))
       -- ELSE null
  -- END eff_dt
-- FROM (WAREHOUSE2.TELECOM t
-- INNER JOIN WAREHOUSE2.AUTHORIZATION a
-- ON (t.TELECOM_ID = a.ASSOC_ID))
-- INNER JOIN MIDDLE_TIER2.IND_XREF ix
-- ON (ix.INDIVIDUAL_ID = t.INDIVIDUAL_ID)
-- WHERE ( (ix.ACTIVE_FLAG = 'A'
    -- AND a.ASSOC_TYP_CD = 'T')
    -- AND a.AUTH_FLG IS NOT NULL)
-- GROUP BY ix.INDIVIDUAL_ID
  -- , a.ASSOC_TYP_CD
  -- , a.AUTH_CD
  -- , a.SCP_CD
  -- , a.DATA_SOURCE
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
select 
  ix.individual_id, 
  'I' assoc_typ_cd, 
  'GFTREN' auth_cd, 
  'GLB' scp_cd, 
  'CDS' data_source, 
  'N' auth_flg, 
  sysdate fst_dt, 
  sysdate eff_dt 
FROM MIDDLE_TIER2.IND_XREF ix
  , WAREHOUSE2.OFFLINE_ACCOUNT oa
  , WAREHOUSE2.EXTERNAL_REF er
WHERE ix.individual_id = er.individual_id
    AND oa.acct_id = er.internal_key
    AND NVL (SUBSTR (oa.gft_keycd, 1, 1), 'X') NOT IN ('*', '/', '&', 'X')
    AND SUBSTR (er.external_key, 1, 3) IN ('CNS', 'CRH', 'CRM', 'SHM') --< added SHM artf143960
GROUP BY ix.INDIVIDUAL_ID
-------------------------------------------------------------------------------------------
UNION ALL
-------------------------------------------------------------------------------------------,
select 
  ix.individual_id, 
  'I' assoc_typ_cd, 
  'GFTOPT' auth_cd, 
  'GLB' scp_cd, 
  'CDS' data_source, 
  'N' auth_flg, 
  sysdate fst_dt, 
  sysdate eff_dt 
FROM middle_tier2.ind_xref ix
  ,warehouse2.offline_account oa
  ,warehouse2.external_ref er
WHERE ix.individual_id = er.individual_id
    AND oa.acct_id = er.internal_key
    AND SUBSTR (oa.gft_keycd, 1, 1) IN ( '*', '/', '&' )
    AND SUBSTR (er.external_key, 1, 3) IN ( 'CNS', 'CRH', 'CRM', 'CRT', 'CRE', 'SHM' ) --< added SHM artf143960
GROUP BY ix.individual_id;
;
