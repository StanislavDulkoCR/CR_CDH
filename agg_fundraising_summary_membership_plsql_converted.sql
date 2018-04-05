---ADD MEMBERSHIP DATA

DECLARE
    --
    CURSOR c_fr_donation IS
        SELECT
              fs.rowid                            AS row_id
            , fs.individual_id
            , CASE WHEN fd.don_dt >= to_date('20050601', 'YYYYMMDD')
            THEN fd.don_dt
              END                                 AS don_dt
            , CASE WHEN fd.don_dt >= greatest(to_date('20050601', 'YYYYMMDD'), nvl(pce.qualifying_date, to_date('20050601', 'YYYYMMDD')))
            THEN fd.don_amt
              END                                 AS don_amt
            , CASE WHEN fd.don_dt >= to_date('20050601', 'YYYYMMDD')
            THEN fd.keycode
              END                                 AS keycode
            , max(decode(fm.membership_flag, 'C', 1, 0))
              OVER (
                  PARTITION BY fs.individual_id ) AS flg_c_exists
            , min(CASE WHEN fd.don_amt >= 1000
            THEN fd.don_dt END)
              OVER (
                  PARTITION BY fs.individual_id ) AS mbr_pres_cir_frst_strt_dt_tmp
            , pce.individual_id                   AS pce_individual_id
            , pce.qualifying_date                 AS pce_qualifying_date
            , fs.fr_mbr_gvng_lvl_cd               AS gvng_lvl_cd
        FROM MT_FR_SUMMARY_TEMP fs
        , agg_fundraising_donation fd
        , fundraising_membership fm
        , fr_prescircle_exclude pce
        
        WHERE fs.individual_id = fd.individual_id
              AND fs.individual_id = fm.individual_id (+)
              AND fs.individual_id = pce.individual_id (+)
              AND ((fd.don_dt >= to_date('20050601', 'YYYYMMDD')
                    AND FD.DON_CD = 'C3') OR (FM.membership_flag = 'C'))
        ORDER BY individual_id, don_dt DESC NULLS LAST, keycode;
    --
    v_row                         c_fr_donation%ROWTYPE;
    --
    row_count                     NUMBER;
    ind_id                        NUMBER := 0;
    v_last_upd_ind_id             NUMBER := 0;
    first_rec                     NUMBER := 1;
    row_id                        ROWID;
    mbr_pres_cir_frst_strt_dt_tmp DATE;
    pce_exclude_flg               NUMBER := 0;
    --
    mbr_gvng_lvl_cd MT_FR_SUMMARY_TEMP.fr_mbr_gvng_lvl_cd% TYPE;
    mbr_exp_dt MT_FR_SUMMARY_TEMP.fr_mbr_exp_dt% TYPE;
    mbr_frst_strt_dt MT_FR_SUMMARY_TEMP.fr_mbr_frst_strt_dt% TYPE;
    mbr_lst_keycode MT_FR_SUMMARY_TEMP.fr_mbr_lst_keycode% TYPE;
    mbr_lst_ren_don_amt MT_FR_SUMMARY_TEMP.fr_mbr_lst_ren_don_amt% TYPE;
    mbr_lst_add_don_amt MT_FR_SUMMARY_TEMP.fr_mbr_lst_add_don_amt% TYPE;
    mbr_lst_add_don_dt MT_FR_SUMMARY_TEMP.fr_mbr_lst_add_don_dt% TYPE;
    mbr_pres_cir_frst_strt_dt MT_FR_SUMMARY_TEMP.fr_mbr_pres_cir_frst_strt_dt% TYPE;
    --
BEGIN
    --
    OPEN c_fr_donation;
    --
    <<loop1>>
    FETCH c_fr_donation INTO v_row;
    WHILE NOT c_fr_donation%NOTFOUND LOOP
        row_count := c_fr_donation%ROWCOUNT;
        IF mod(row_count, 5000) = 0
        THEN
            COMMIT;
        END IF;
        --
        IF v_row.individual_id != ind_id OR first_rec = 1
        THEN
            IF first_rec = 0 AND ind_id != 0
            THEN
    update MT_FR_SUMMARY_TEMP
    SET fr_mbr_gvng_lvl_cd = mbr_gvng_lvl_cd, fr_mbr_exp_dt = mbr_exp_dt, fr_mbr_frst_strt_dt = mbr_frst_strt_dt, fr_mbr_lst_keycode = mbr_lst_keycode, fr_mbr_lst_ren_don_amt = mbr_lst_ren_don_amt, fr_mbr_lst_add_don_amt = mbr_lst_add_don_amt, fr_mbr_lst_add_don_dt = mbr_lst_add_don_dt, fr_mbr_pres_cir_frst_strt_dt = CASE when mbr_pres_cir_frst_strt_dt IS NOT NULL
    THEN mbr_pres_cir_frst_strt_dt
WHEN mbr_gvng_lvl_cd = 'C'
THEN mbr_pres_cir_frst_strt_dt_tmp
END
WHERE ROWID = row_id;
ind_id:=0;

END IF;
IF v_row.gvng_lvl_cd = 'C' AND v_row.don_dt <= v_row.pce_qualifying_date THEN

mbr_gvng_lvl_cd := NULL;
mbr_exp_dt := to_date('20061231', 'YYYYMMDD');
mbr_frst_strt_dt := to_date('20050601', 'YYYYMMDD');
mbr_lst_keycode := NULL;
mbr_lst_ren_don_amt := NULL;
mbr_lst_add_don_amt := NULL;
mbr_lst_add_don_dt := NULL;
mbr_pres_cir_frst_strt_dt := to_date('20050601', 'YYYYMMDD');
--
row_id := v_row.row_id;
ind_id := v_row.individual_id;
mbr_pres_cir_frst_strt_dt_tmp := v_row.mbr_pres_cir_frst_strt_dt_tmp;
first_rec := 0;
ELSIF v_row.flg_c_exists = 1 AND v_row.pce_individual_id IS NULL THEN

mbr_gvng_lvl_cd := 'C';
mbr_exp_dt := to_date('20061231', 'YYYYMMDD');
mbr_frst_strt_dt := to_date('20050601', 'YYYYMMDD');
mbr_lst_keycode := NULL;
mbr_lst_ren_don_amt := NULL;
mbr_lst_add_don_amt := NULL;
mbr_lst_add_don_dt := NULL;
mbr_pres_cir_frst_strt_dt := to_date('20050601', 'YYYYMMDD');
--
row_id := v_row.row_id;
ind_id := v_row.individual_id;
mbr_pres_cir_frst_strt_dt_tmp := v_row.mbr_pres_cir_frst_strt_dt_tmp;
first_rec := 0;
ELSIF v_row.don_amt >= 100 THEN
mbr_gvng_lvl_cd := CASE WHEN v_row.don_amt >= 100 AND v_row.don_amt < 250
THEN 'S'
WHEN v_row.don_amt >= 250 AND v_row.don_amt < 500
THEN 'G'
WHEN v_row.don_amt >= 500 AND v_row.don_amt < 1000
THEN 'P'
WHEN v_row.don_amt >= 1000
THEN 'C'
END;
mbr_exp_dt := CASE WHEN v_row.don_dt <= to_date('20051231', 'YYYYMMDD')
THEN to_date('20061231', 'YYYYMMDD')
ELSE last_day(add_months(v_row.don_dt, 12))
END;
mbr_frst_strt_dt := v_row.don_dt;
mbr_lst_keycode := v_row.keycode;
mbr_lst_ren_don_amt := NULL;
mbr_lst_add_don_amt := NULL;
mbr_lst_add_don_dt := NULL;
mbr_pres_cir_frst_strt_dt := NULL;
--
row_id := v_row.row_id;
ind_id := v_row.individual_id;
mbr_pres_cir_frst_strt_dt_tmp := v_row.mbr_pres_cir_frst_strt_dt_tmp;
first_rec := 0;

GOTO loop1;
ELSE
GOTO loop1;
END IF;
END IF;
--
--3a. Renewal
IF v_row.pce_qualifying_date IS NULL OR v_row.don_dt > v_row.pce_qualifying_date THEN
IF v_row.don_dt <= mbr_exp_dt
AND ( (mbr_gvng_lvl_cd = 'S' AND v_row.don_amt >= 100)
OR (mbr_gvng_lvl_cd = 'G' AND v_row.don_amt >= 250)
OR (mbr_gvng_lvl_cd = 'P' AND v_row.don_amt >= 500)
OR (mbr_gvng_lvl_cd = 'C' AND v_row.don_amt >= 1000) )
AND ( (substr(v_row.keycode, 2, 3) = '414' AND v_row.don_dt <= to_date('20061031', 'YYYYMMDD'))
OR (substr(v_row.keycode, 2, 2) IN ('43', '47', '48'))
OR (substr(v_row.keycode, 2, 2) = '17' AND v_row.don_dt
BETWEEN to_date('20061101', 'YYYYMMDD') AND to_date('20080430', 'YYYYMMDD')) ) THEN
mbr_gvng_lvl_cd := CASE WHEN v_row.don_amt >= 100 AND v_row.don_amt < 250
THEN 'S'
WHEN v_row.don_amt >= 250 AND v_row.don_amt < 500
THEN 'G'
WHEN v_row.don_amt >= 500 AND v_row.don_amt < 1000
THEN 'P'
WHEN v_row.don_amt >= 1000
THEN 'C'
END;
mbr_exp_dt := last_day(add_months(mbr_exp_dt, 12));
mbr_lst_keycode := v_row.keycode;
mbr_lst_ren_don_amt := v_row.don_amt;
--3b. Renewal
ELSIF v_row.don_dt > add_months(mbr_exp_dt, -6) AND v_row.don_dt <= mbr_exp_dt
AND (substr(v_row.keycode, 2, 2) IN ('47', '48')
OR (substr(v_row.keycode, 2, 2) = '17' AND v_row.don_dt
BETWEEN to_date('20061101', 'YYYYMMDD') AND to_date('20080430', 'YYYYMMDD'))  )
AND v_row.don_amt >= 100
AND ( (mbr_gvng_lvl_cd = 'G' AND v_row.don_amt < 250)
OR (mbr_gvng_lvl_cd = 'P' AND v_row.don_amt < 500) ) THEN
mbr_gvng_lvl_cd := CASE WHEN v_row.don_amt >= 100 AND v_row.don_amt < 250
THEN 'S'
WHEN v_row.don_amt >= 250 AND v_row.don_amt < 500
THEN 'G'
WHEN v_row.don_amt >= 500 AND v_row.don_amt < 1000
THEN 'P'
WHEN v_row.don_amt >= 1000
THEN 'C'
END;
mbr_exp_dt := last_day(add_months(mbr_exp_dt, 12));
mbr_lst_keycode := v_row.keycode;
mbr_lst_ren_don_amt := v_row.don_amt;
--3c. Renewal
ELSIF v_row.don_dt > add_months(mbr_exp_dt, -4) AND v_row.don_dt <= mbr_exp_dt
AND nvl(substr(v_row.keycode, 2, 1), 'X') != '4'
AND NOT (substr(v_row.keycode, 2, 2) = '17' AND v_row.don_dt
BETWEEN to_date('20061101', 'YYYYMMDD') AND to_date('20080430', 'YYYYMMDD'))
AND ( (mbr_gvng_lvl_cd = 'S' AND v_row.don_amt >= 100)
OR (mbr_gvng_lvl_cd = 'G' AND v_row.don_amt >= 250)
OR (mbr_gvng_lvl_cd = 'P' AND v_row.don_amt >= 500)
OR (mbr_gvng_lvl_cd = 'C' AND v_row.don_amt >= 1000) ) THEN
mbr_gvng_lvl_cd := CASE WHEN v_row.don_amt >= 100 AND v_row.don_amt < 250
THEN 'S'
WHEN v_row.don_amt >= 250 AND v_row.don_amt < 500
THEN 'G'
WHEN v_row.don_amt >= 500 AND v_row.don_amt < 1000
THEN 'P'
WHEN v_row.don_amt >= 1000
THEN 'C'
END;
mbr_exp_dt := last_day(add_months(mbr_exp_dt, 12));
mbr_lst_keycode := v_row.keycode;
--3d. Renewal
ELSIF v_row.don_dt <= mbr_exp_dt
AND ( (mbr_gvng_lvl_cd = 'S' AND v_row.don_amt >= 250)
OR (mbr_gvng_lvl_cd = 'G' AND v_row.don_amt >= 500)
OR (mbr_gvng_lvl_cd = 'P' AND v_row.don_amt >= 1000) ) THEN

mbr_gvng_lvl_cd := CASE WHEN v_row.don_amt >= 100 AND v_row.don_amt < 250
THEN 'S'
WHEN v_row.don_amt >= 250 AND v_row.don_amt < 500
THEN 'G'
WHEN v_row.don_amt >= 500 AND v_row.don_amt < 1000
THEN 'P'
WHEN v_row.don_amt >= 1000
THEN 'C'
END;

mbr_exp_dt := last_day(add_months(mbr_exp_dt, 12));
mbr_lst_keycode := v_row.keycode;
mbr_lst_ren_don_amt := CASE WHEN (substr(v_row.keycode, 2, 3) = '414'
AND v_row.don_dt <= to_date('20061031', 'YYYYMMDD'))
OR (substr(v_row.keycode, 2, 2) IN ('43', '47', '48'))
OR (substr(v_row.keycode, 2, 2) = '17' AND v_row.don_dt
BETWEEN to_date('20061101', 'YYYYMMDD') AND to_date('20080430', 'YYYYMMDD'))
THEN v_row.don_amt
ELSE
mbr_lst_ren_don_amt
END;
ELSIF v_row.don_dt > mbr_exp_dt
AND v_row.don_amt >= 100
AND ( (mbr_gvng_lvl_cd = 'C' AND v_row.don_amt >= 1000)
OR mbr_gvng_lvl_cd != 'C') THEN


mbr_gvng_lvl_cd := CASE WHEN v_row.don_amt >= 100 AND v_row.don_amt < 250
THEN 'S'
WHEN v_row.don_amt >= 250 AND v_row.don_amt < 500
THEN 'G'
WHEN v_row.don_amt >= 500 AND v_row.don_amt < 1000
THEN 'P'
WHEN v_row.don_amt >= 1000
THEN 'C'
END;

mbr_exp_dt := last_day(add_months(v_row.don_dt, 12));
mbr_lst_keycode := v_row.keycode;
ELSIF v_row.don_amt > 0 AND nvl(substr(v_row.keycode, 2, 1), 'X') NOT IN ('2', '3') THEN
mbr_lst_add_don_amt := v_row.don_amt;
mbr_lst_add_don_dt := v_row.don_dt;
END IF;
END IF;
--
FETCH c_fr_donation INTO v_row;
--
END LOOP;
--
IF first_rec = 0 AND ind_id!=0 THEN
UPDATE MT_FR_SUMMARY_TEMP
SET fr_mbr_gvng_lvl_cd = mbr_gvng_lvl_cd, fr_mbr_exp_dt = mbr_exp_dt, fr_mbr_frst_strt_dt = mbr_frst_strt_dt, fr_mbr_lst_keycode = mbr_lst_keycode, fr_mbr_lst_ren_don_amt = mbr_lst_ren_don_amt, fr_mbr_lst_add_don_amt = mbr_lst_add_don_amt, fr_mbr_lst_add_don_dt = mbr_lst_add_don_dt, fr_mbr_pres_cir_frst_strt_dt = mbr_pres_cir_frst_strt_dt
WHERE ROWID = row_id;
END IF;
--
COMMIT;
--
CLOSE c_fr_donation;
--
END;