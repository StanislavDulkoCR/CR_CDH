drop table if exists prod.agg_preference_summary;
CREATE TABLE prod.agg_preference_summary
AS
SELECT
  ix.individual_id,
  ix.household_id as hh_id,
  /*ix.ind_urn,*/
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd = 'DCD'
            AND a.auth_flg = 'Y'
           THEN 'N'
           WHEN a.data_source IN ('CDS', 'PWI','BMO','CDB','DMP')
            AND a.auth_cd in ('DEC','DCD')
            AND a.auth_flg = 'N'
           THEN 'N'
           ELSE null
      END) dcd_flg,
  SUBSTRING(max(CASE WHEN ((a.data_source IN ('DCK', 'KBL', 'LSR') and a.auth_cd IN ('DNM', 'XXX') and a.auth_flg = 'Y')
                         or (a.data_source = 'DMP' and a.auth_cd = 'DNM' and a.auth_flg = 'N'))
                            THEN '9N'    --first char is for sorting
                            WHEN ((a.data_source IN ('DCK', 'KBL', 'LSR') and a.auth_cd IN ('SOY') and a.auth_flg = 'Y')
                            or (a.data_source IN ('DMP','CCC') and a.auth_cd = 'SOY' and a.auth_flg = 'N'))
                            THEN '81'
                            WHEN ((a.data_source IN ('DCK', 'KBL', 'LSR') and a.auth_cd IN ('STY') and a.auth_flg = 'Y')
                            or (a.data_source = 'DMP' and a.auth_cd = 'STY' and a.auth_flg = 'N'))
                            THEN '72'
                            WHEN a.data_source IN ('DCK', 'KBL', 'LSR') and a.auth_cd IN ('SOJ') and a.auth_flg = 'Y'
                            THEN '63'
                            ELSE null
             END),2) fr_mail_freq_cd,
  max(CASE WHEN a.auth_cd = 'PRISON'
            AND a.auth_flg = 'N'
           THEN 'N'
           ELSE null
      END) prison_flg,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd = 'ACK'
            AND a.auth_flg = 'Y'
           THEN 'Y'
           WHEN a.data_source = ('DMP')
            AND a.auth_cd = 'ACK'
            AND a.auth_flg = 'N'            
           THEN 'Y'
           ELSE null
      END) fr_non_ack_flg,
  max(CASE WHEN a.data_source = 'CDS'
            AND a.auth_cd = 'SGLRENW'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) non_prom_auto_rnw_flg,
  max(CASE WHEN a.data_source = 'CDS'
            AND a.auth_cd = 'GFTRENW'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) non_prom_auto_rnw_gft_flg,
  max(CASE WHEN a.data_source = 'CDS'
            AND a.auth_cd = 'ABQDIR'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) abq_non_prom_dm_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'ABQ')
                              OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'ABQ')
                              OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_cd || a.scp_cd || a.auth_flg, 'EMAIL' || 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'ABQ')
                                  OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'ABQ')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND (a.auth_cd = 'ABQEML' or a.scp_cd != 'GLB')
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'ABQ')  -- same as above SUBSTRING.  Note that oracle is smart enough to know it doesn't need to evaluate this SUBSTRING twice
                                  OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'ABQ')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND (a.auth_cd = 'ABQEML' or a.scp_cd != 'GLB')
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END abq_non_prom_em_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'ADV')
                              OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
                              OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'ADV')
                                  OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
                                  OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'ADV') 
                                  OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
                                  OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'ADV')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END adv_non_prom_em_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'APS')
                              OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'APS')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'APS')
                                  OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'APS')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'APS')  
                                  OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'APS')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END aps_non_prom_em_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CNS')
                              OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CNS')
                              OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CNS')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CNS')
                                  OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CNS')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CNS')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CNS')  
                                  OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CNS')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CNS')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END cr_non_prom_em_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'MGD')
                              OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd = 'MG')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'MGD')
                                  OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd = 'MG')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'MGD')  
                                  OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd = 'MG')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END crmg_non_prom_em_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CRE')
                              OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRO')
                              OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRE')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') ||NVL( decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CRE')
                                  OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRO')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRE')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CRE')  
                                  OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRO')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRE')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END cro_non_prom_em_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('MKT', 'ABQ'))
                              OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd in ('CRSCH', 'ABQ'))
                              OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_cd || a.scp_cd || a.auth_flg, 'EMAIL' || 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('MKT', 'ABQ'))
                              OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd in ('CRSCH', 'ABQ'))
                              OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                              AND (a.scp_cd != 'GLB' or a.auth_cd = 'ABQEML')
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'ABQ' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('MKT', 'ABQ'))
                              OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd in ('CRSCH', 'ABQ'))
                              OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                              AND (a.scp_cd != 'GLB' or a.auth_cd = 'ABQEML')
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_cd || a.auth_flg, 'ABQEML' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('MKT', 'ABQ'))
                                  OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd in ('CRSCH', 'ABQ'))
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd not in ('GLB','ABQ')
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('MKT', 'ABQ'))
                                  OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd in ('CRSCH', 'ABQ'))
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd not in ('GLB','ABQ')
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END crsch_non_prom_em_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'FUN')
                              OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'FR')
                              OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'FUN')
                                  OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'FR')
                                  OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'FUN')  
                                  OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'FR')
                                  OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END fr_non_prom_em_flg,
   CASE WHEN SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                  OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                  OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                  OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.auth_cd = 'EMAIL'
                             AND a.scp_cd = 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
        AND '1' || max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                              OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                              OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                              OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                              OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                              OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                         AND a.auth_cd = 'EMAIL'
                         AND a.scp_cd = 'GLB'
                        THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1'),'')
                   END)
            ----
            <
            ----
            greatest(nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND (a.scp_cd = 'ABQ' or a.auth_cd = 'ABQEML')
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR')
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND a.scp_cd = 'APS'
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND a.scp_cd = 'CNS'
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND a.scp_cd in ('CRE','CRO')
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND a.scp_cd = 'CRH'
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND a.scp_cd = 'CRM'
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND (a.scp_cd in ('CRSCH','MKT','ABQ') or a.auth_cd = 'ABQEML')
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND a.scp_cd in ('FUN','FR')
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND a.scp_cd in ('MG','MGD')
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND a.scp_cd in ('NCB','NCBK')
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND a.scp_cd in ('UCB','UCBK')
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND a.scp_cd = 'SHM'
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'),
                     nvl(SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                           OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                           OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                           OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                           OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                           OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                                          AND a.scp_cd = 'CARP'
                                     THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N','1','0'),'') || NVL(decode(a.auth_flg, 'N','0','1'),'') || nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000')
                                END), 10), '0'))
       THEN 'N'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                  OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                  OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                  OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.auth_cd = 'EMAIL'
                             AND a.scp_cd = 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source in ('DGI', 'AMP') AND a.auth_cd = 'EMAIL' AND a.scp_cd in ('ABQ',  'ADV', 'APS', 'CARP', 'CNS', 'CRE', 'CRH', 'CRM', 'FUN', 'MGD', 'MKT', 'NCB', 'UCB', 'SHM'))
                                  OR (a.data_source in ('PWI', 'BMO', 'CU') and a.auth_cd = 'EMAIL' and a.scp_cd in ('ABQ', 'ADV', 'APS', 'CARP', 'CNS', 'CRH', 'CRM', 'CRO', 'CRSCH', 'FR', 'MG', 'NCBK', 'UCBK', 'SHM'))
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'ABQEML')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd in ('CNS', 'CRE', 'CRH', 'CRM', 'SHM'))
                                  OR (a.data_source IN ('DMP','CCC') and a.auth_cd = 'DNE' and a.scp_cd = 'FR')
                                  OR (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd IN ('ADV','CVONEWS','CVOAA','CVOFR'))
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.auth_cd = 'EMAIL'
                             AND a.scp_cd = 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END non_prom_em_flg, 
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CRH')
                              OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRH')
                              OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRH')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CRH')
                                  OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRH')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRH')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CRH')  
                                  OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRH')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRH')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END hl_non_prom_em_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CRM')
                              OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRM')
                              OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRM')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CRM')
                                  OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRM')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRM')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CRM')  
                                  OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRM')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CRM')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END ma_non_prom_em_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'NCB')
                              OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'NCBK')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'NCB')
                                  OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'NCBK')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'NCB')  
                                  OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'NCBK')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END ncbk_non_prom_em_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'UCB')
                              OR (a.data_source in ('PWI', 'BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'UCBK')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'UCB')
                                  OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'UCBK')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'UCB')  
                                  OR (a.data_source in ('PWI','BMO') and a.auth_cd = 'EMAIL' and a.scp_cd = 'UCBK')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END ucbk_non_prom_em_flg,
  max(CASE WHEN a.auth_cd = 'RENT'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) non_prom_list_rent_flg,
  max(CASE WHEN a.data_source in ('CDS','CDB')
            AND a.auth_cd = 'PANDER'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) non_prom_pander_flg,
  max(CASE WHEN a.data_source = 'KBL'
            AND a.auth_cd = 'RAFL'
            AND a.auth_flg = 'N'
           THEN 'Y'
           WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd = 'NRF'
            AND a.auth_flg = 'Y'
           THEN 'Y'
           WHEN a.data_source in ('CDS','CDB')
            AND a.auth_cd IN ('RAFL','SWEEPS')
            AND a.auth_flg = 'N'
           THEN 'Y'
           WHEN a.data_source = 'DMP'
            AND a.auth_cd IN ('NRF')
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) non_prom_rfl_flg,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd IN ('DNT', 'XXX')
            AND a.auth_flg = 'Y'
           THEN 'Y'
           WHEN a.data_source IN ('TLS', 'DM', 'AG', 'FC', 'CDB', 'CDS')
            AND a.auth_cd = 'PHONE'
            AND a.auth_flg = 'N'
           THEN 'Y'
           WHEN a.data_source = 'TLS'
            AND a.auth_cd = 'PHONE_PRIV'
            AND a.auth_flg = 'N'
           THEN 'Y'
           WHEN a.data_source = 'DMP'
            AND a.auth_cd = 'DNT'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) non_prom_tm_flg,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd IN ('DNM', 'XXX')
            AND a.auth_flg = 'Y'
           THEN 'Y'
           WHEN a.data_source IN ('DMP','CCC')
            AND a.auth_cd = 'DNM'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) fr_non_prom_dm_flg,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd IN ('DNT', 'XXX')
            AND a.auth_flg = 'Y'
           THEN 'Y'
           WHEN a.data_source IN ('DMP','CCC')
            AND a.auth_cd = 'DNT'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) fr_non_prom_tm_flg,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd = 'NRN'
            AND a.auth_flg = 'Y'
           THEN 'Y'
           ELSE null
      END) fr_non_prom_rnw_flg,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd = 'NTM'
            AND a.auth_flg = 'Y'
           THEN 'Y'
           ELSE null
      END) fr_non_prom_tof_flg,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd = 'NTR'
            AND a.auth_flg = 'Y'
           THEN 'Y'
           ELSE null
      END) fr_non_prom_tm_rem_flg,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd = 'NCU'
            AND a.auth_flg = 'Y'
           THEN 'Y'
           WHEN a.data_source = 'DMP'
            AND a.auth_cd = 'NCU'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) fr_non_prom_cu_flg,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd = 'PRE'
            AND a.auth_flg = 'Y'
           THEN 'Y'
           WHEN a.data_source = 'DMP'
            AND a.auth_cd = 'PRE'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) fr_non_prom_prem_flg,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
           THEN CASE WHEN a.auth_cd = 'NRN'
                      AND a.auth_flg = 'Y'
                     THEN 'N'
                     WHEN a.auth_cd = 'ROY'
                      AND a.auth_flg = 'Y'
                     THEN '1'
                     ELSE null
                END
           ELSE null
      END) fr_rnw_mail_freq_cd,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd IN ('DNT', 'XXX')
            AND a.auth_flg = 'Y'
           THEN 'N'
           WHEN a.data_source IN ('DMP','CCC')
            AND a.auth_cd = 'DNT'
            AND a.auth_flg = 'N'
           THEN 'N'
           WHEN a.data_source IN ('DCK', 'KBL', 'LSR') 
            AND a.auth_cd = 'POY'
            AND a.auth_flg = 'Y'
           THEN '1'
           ELSE null
      END) fr_tm_freq_cd,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd = 'YCU'
            AND a.auth_flg = 'Y'
           THEN 'Y'
           WHEN a.data_source = 'DMP'
            AND a.auth_cd = 'NCU'
            AND a.auth_flg = 'Y'
           THEN 'Y'
           ELSE null
      END) fr_cu_ins_flg,
  max(CASE WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd = 'BME'
            AND a.auth_flg = 'Y'
           THEN 'Y'
           WHEN a.data_source = 'DMP'
            AND a.auth_cd = 'BME'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) cu_board_flg,
  max(CASE WHEN a.data_source = 'DM'
            AND a.auth_cd = 'MAIL'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) dma_non_prom_dm_flg,
  max(CASE WHEN a.data_source = 'DM'
            AND a.auth_cd = 'PHONE'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) dma_non_prom_tm_flg,
  max(CASE WHEN a.data_source = 'DM'
            AND a.auth_cd = 'EMAIL'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) dma_non_prom_em_flg,
  max(CASE WHEN a.data_source = 'AX'
            AND a.auth_cd = 'MAIL'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) acx_non_prom_dm_flg,
  max(CASE WHEN a.data_source = 'AX'
            AND a.auth_cd = 'PHONE'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) acx_non_prom_tm_flg,
  max(CASE WHEN a.data_source = 'AX'
            AND a.auth_cd = 'EMAIL'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) acx_non_prom_em_flg,
  max(case when a.data_source = 'CDB' and a.auth_cd = 'PROMOTE' and a.auth_flg = 'N' then
      'Y'
    else
      null
    end) bk_non_prom_flg,
  max(CASE WHEN a.data_source = 'CDS'
            AND a.auth_cd in ('GFTOPT','GFTREN')
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) non_prom_gft_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'SHM')
                              OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'SHM')
                              OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'SHM')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'SHM')
                                  OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'SHM')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'SHM')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'SHM')  
                                  OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'SHM')
                                  OR (a.data_source = 'CDS' and a.auth_cd = 'EMAIL' and a.scp_cd = 'SHM')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END shm_non_prom_em_flg,
  CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CARP')
                              OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CARP')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CARP')
                                  OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CARP')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'DGI' AND a.auth_cd = 'EMAIL' AND a.scp_cd = 'CARP')  
                                  OR (a.data_source = 'PWI' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CARP')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END carp_non_prom_em_flg,
  max(CASE WHEN a.data_source = 'KBL'
            AND a.auth_cd = 'RAFL'
            AND a.auth_flg = 'N'
           THEN 'Y'
           WHEN a.data_source IN ('DCK', 'KBL', 'LSR')
            AND a.auth_cd = 'NRF'
            AND a.auth_flg = 'Y'
           THEN 'Y'
           WHEN a.data_source = 'DMP'
            AND a.auth_cd = 'NRF'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) NON_PROM_CU_FR_RFL_FLG,
  max(CASE WHEN a.data_source = 'IB'
            AND a.auth_cd = 'DEC'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) ib_deceased_flg,
  max(CASE WHEN a.auth_cd = 'TEST_USER'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) test_user_flg,
  max(CASE WHEN a.auth_cd = 'DNS'
            AND a.auth_flg = 'N'
           THEN 'Y'
           ELSE null
      END) fr_non_share_flg,
  ix.household_id as osl_hh_id,
    CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CVONEWS')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CVONEWS')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN (  (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CVONEWS')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END adv_non_prom_news_em_flg,
      CASE WHEN SUBSTRING(max(CASE WHEN (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CVOAA')
                              OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM'))
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.scp_cd || a.auth_flg, 'GLB' || 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN ( (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CVOAA')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'N', '1'),'')
                       END), 9) = '1'
       THEN 'Y'
       WHEN SUBSTRING(max(CASE WHEN (  (a.data_source = 'CVO' and a.assoc_typ_cd = 'E' and a.auth_cd = 'EMAIL' and a.scp_cd = 'CVOAA')
                                  OR (a.auth_cd = 'EMAIL' and a.scp_cd = 'GLB' and a.data_source not in ('AX', 'DM')) )
                             AND a.scp_cd != 'GLB'
                            THEN nvl(to_char(a.eff_dt,'YYYYMMDD'),'00000000') || NVL(decode(a.auth_flg, 'Y', '1'),'')
                       END), 9) = '1'
       THEN 'N'
       ELSE null
  END adv_non_prom_aa_em_flg,
   max(CASE WHEN a.data_source = 'CVO'
            AND a.auth_cd = 'MAIL'
            AND a.auth_flg = 'N'
           THEN 'Y'
           WHEN a.data_source = 'CVO'
            AND a.auth_cd = 'MAIL'
            AND a.auth_flg = 'Y'
           THEN 'N'
           ELSE null
      END) adv_non_prom_dm_flg,
   max(CASE WHEN a.SCP_CD = 'CVOFR'
       AND a.auth_cd = 'EMAIL' 
       AND a.auth_flg = 'N'
      THEN 'Y'
      ELSE null
    END) advfr_non_prom_em_flg
FROM prod.agg_preference a inner join prod.individual ix on ix.individual_id = a.individual_id
GROUP BY ix.individual_id, ix.household_id
;

