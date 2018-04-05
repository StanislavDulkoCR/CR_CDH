/* CREATE TEMP TABLES */

DROP TABLE IF EXISTS agg.cte_return_record;
CREATE TABLE agg.cte_return_record DISTSTYLE KEY DISTKEY(reference_id) INTERLEAVED SORTKEY (reference_id) AS
SELECT dtd.reference_id
	, SUM(dtd.line_item_total_amount) sum_line_item_total_amount
	FROM prod.action_header ah
		JOIN prod.digital_transaction_detail dtd on ah.hash_action_id = dtd.hash_action_id
	WHERE --ah.source_name = 'PWI' AND /* Removed by DD as hash_action_id will grab only the DIGITAL records */
	ah.action_type = 'RETURN'
	GROUP BY dtd.reference_id;

DROP TABLE IF EXISTS agg.cte_recipient_order;
CREATE TABLE agg.cte_recipient_order DISTSTYLE KEY DISTKEY(addtl_action_id) INTERLEAVED SORTKEY (addtl_action_id) AS
SELECT ah.addtl_action_id
	, SUM(ah.action_amount) sum_action_amount
	, MAX(ah.action_date) max_action_date
	FROM prod.action_header ah
		JOIN prod.print_transaction pt ON ah.hash_action_id = pt.hash_action_id
	WHERE --ah.source_name = 'CDS' AND /* Removed by DD as hash_action_id will grab only the PRINT records */
		ah.action_type = 'ORDER'
		AND pt.set_code IN ('C', 'E')
	GROUP BY ah.addtl_action_id;

DROP TABLE IF EXISTS agg.cte_last_cancellation;
CREATE TABLE agg.cte_last_cancellation DISTSTYLE KEY DISTKEY(action_id) INTERLEAVED SORTKEY (action_id) AS
SELECT action_id
	, action_date
	, cancel_reason_code
	FROM (SELECT ah.action_id
		, ah.action_date
		, pc.cancel_reason_code
		, ROW_NUMBER() OVER (PARTITION BY ah.action_id ORDER BY ah.action_date DESC) ranking
		FROM prod.action_header ah
			JOIN prod.print_cancel pc ON ah.hash_action_id = pc.hash_action_id
		WHERE --ah.source_name = 'CDS' AND /* Removed by DD as hash_action_id will grab only the PRINT records */
			  ah.action_type = 'CANCEL')
	WHERE ranking = 1;

DROP TABLE IF EXISTS agg.cte_digital_item;
CREATE TABLE agg.cte_digital_item DISTSTYLE KEY DISTKEY(individual_id) INTERLEAVED SORTKEY (individual_id) AS
	SELECT
		  ah.individual_id
		, dtd.line_item_id itm_id
		, ah.action_id ord_id
		, acct.hash_account_id
		, acct.source_name
		, acct.account_subtype_code
		, ah.source_account_id acct_id
		, dtd.line_item_seq seq_num
		, DECODE(dtd.line_item_status, 'H', 'C', dtd.line_item_status) stat_cd
		, dtd.sku_num
		, dtd.line_item_total_amount tot_gross_amt
		, CASE
				WHEN dtd.sku_num < 5000000
					THEN COALESCE(dtd.line_item_total_amount + ret.sum_line_item_total_amount, dtd.line_item_total_amount, ret.sum_line_item_total_amount)
				ELSE dtd.line_item_total_amount
			END tot_amt
		, CASE
				WHEN dtd.sku_num > 5000000 THEN COALESCE(dtd.line_item_order_start_date, dtd.line_item_order_create_date)
				WHEN dtd.subscription_id IS NULL THEN COALESCE(dtd.line_item_subsrc_start_date, dtd.line_item_order_create_date)
				ELSE dtd.line_item_subsrc_start_date
			END strt_dt
		, dtd.subscription_id
		, dtd.line_item_subsrc_end_date
		, dtd.line_item_order_end_date
		, COALESCE(dtd.line_item_magazine_code, sl.product) mag_cd
		, CASE
				WHEN dtd.subscription_id IS NOT NULL THEN dtd.payment_status	-- TODO: Is this a valid mechanism to determine if there are any online_subscription records?
				WHEN dtd.credit_status IS NOT NULL THEN dtd.credit_status		-- TODO: Validate this si correct
				WHEN ah.plan_status_code IN ('C','H','M','R','T','Y') THEN 'C'
				WHEN ah.plan_status_code IN ('J') THEN 'F'
				WHEN ah.plan_status_code IN ('P','S','N') THEN 'P'
				ELSE dtd.credit_status
			END crd_stat_cd
		, dtd.credit_status dtd_credit_status
		, dtd.cancel_reason_code dtd_cancel_reason_code
		, dtd.cancel_date dtd_cancel_date
		, dtd.line_item_cancel_flag dtd_line_item_cancel_flag
		, dtd.line_item_status dtd_line_item_status
--		, CASE
--				WHEN (dtd.credit_status = 'F' OR dtd.cancel_reason_code IN ('50','06')) AND dtd.cancel_date < '20130208' THEN 'Y'
--				WHEN dtd.line_item_cancel_flag IS NOT NULL THEN dtd.line_item_cancel_flag 
--				WHEN dtd.line_item_status IN ('R','J') THEN 'Y'
--				ELSE 'N'
--			END canc_flg
		, CASE
				WHEN ISNULL(dtd.line_item_term, 0) = 0 AND COALESCE(dtd.line_item_magazine_code, sl.product) NOT IN ('NCPR', 'UCPR')
					THEN CAST(CEIL(sl.value / CASE WHEN sl.unit_flag = 'D' THEN 30 ELSE 1 END) AS INTEGER)
				ELSE CAST(dtd.line_item_term AS INTEGER)
			END term_mth_cnt
		, dtd.renewable_order_ind rnw_cd
		, dtd.set_code set_cd
		, UPPER(dtd.external_keycode) ext_keycd
		, UPPER(dtd.internal_keycode) int_keycd
		, CASE WHEN dtd.cancel_reason_code IS NULL AND dtd.service_status = 'C' THEN '50' ELSE dtd.cancel_reason_code END canc_rsn_cd
		, dtd.ship_method_code shp_meth_cd
		, CASE
				WHEN dtd.line_item_magazine_code IN ('CRO', 'CRMG') AND dtd.line_item_order_create_date < '2008-07-31' THEN COALESCE(dtd.line_item_order_start_date, dtd.line_item_order_create_date)
				ELSE dtd.line_item_order_create_date
			END crt_dt
		, dtd.report_name rpt_nam
		, dtd.subscription_id sub_id
		, dtd.renewable_subsrc_flag sub_rnw_ind
		, CASE
			WHEN dtd.service_status = 'I' THEN 'C'
			ELSE dtd.service_status
		  END svc_stat_cd
		, CASE 
				WHEN dtd.subscription_id IS NOT NULL AND dtd.cancel_date IS NOT NULL THEN dtd.cancel_date
				WHEN dtd.subscription_id IS NOT NULL AND (ISNULL(service_status,'') = 'C' OR ISNULL(dtd.payment_status,'') = 'F') THEN dtd.line_item_subsrc_modify_date
				WHEN ISNULL(dtd.line_item_status, '') = 'R' AND dtd.credit_status IS NULL THEN dtd.line_item_order_modify_date
				WHEN dtd.credit_status IS NULL AND dtd.line_item_status IN ('R', 'J') THEN dt.order_modified_date
				WHEN NVL(dtd.credit_status, '') = 'F' THEN dtd.line_item_order_modify_date
			END canc_dt
		, CASE WHEN dtd.line_item_status IN ('C','H') THEN dtd.line_item_order_modify_date END pmt_dt
		, CASE
				WHEN COALESCE(dtd.line_item_magazine_code, sl.product) IN ('CRO','CARP') THEN
					CASE
							WHEN dtd.external_keycode IS NULL AND dtd.internal_keycode IS NULL THEN 'I'
							WHEN LEFT(UPPER(ISNULL(dtd.external_keycode, dtd.internal_keycode)),2) = 'WF' THEN 'X'
							ELSE LEFT(UPPER(ISNULL(dtd.external_keycode, dtd.internal_keycode)),1)
						END
				WHEN COALESCE(dtd.line_item_magazine_code, sl.product) IN ('CRMG') THEN
					CASE
							WHEN dtd.external_keycode IS NULL AND dtd.internal_keycode IS NULL THEN 'I'
							WHEN UPPER(ISNULL(dtd.external_keycode, dtd.internal_keycode)) = 'VOLUNTARY ORDER' THEN 'I'
							WHEN LEFT(UPPER(ISNULL(dtd.external_keycode, dtd.internal_keycode)),1) = 'H' THEN SUBSTRING(UPPER(ISNULL(dtd.external_keycode, dtd.internal_keycode)), 6, 1)
							ELSE LEFT(UPPER(ISNULL(dtd.external_keycode, dtd.internal_keycode)),1)
						END
			END sub_src_cd
		, CASE
			WHEN upper(dtd.line_item_magazine_code) NOT IN ('NCBK','NCPR', 'UCBK', 'UCPR') then NULL
			WHEN LEFT(dtd.internal_keycode,1) between 0 and 9 then 1
			--First six
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK10%' then 56
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK11%' then 57
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK12%' then 58
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK13%' then 59
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK14%' then 60
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK15%' then 61
			--First five
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK10%' then 42
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK11%' then 43
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK12%' then 44
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK13%' then 45
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK14%' then 46
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK15%' then 47
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK1%' then 48
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK2%' then 49
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK3%' then 50
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK4%' then 51
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK5%' then 52
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK6%' then 53
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK7%' then 54
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UCBK8%' then 55
			--First four
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC10%' then 11
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC11%' then 12
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC12%' then 13
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC13%' then 14
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC14%' then 15
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC15%' then 16
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC10%' then 26
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC11%' then 27
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC12%' then 28
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC13%' then 29
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC14%' then 30
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC15%' then 31
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK1%' then 32
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK2%' then 33
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK3%' then 34
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK4%' then 35
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK5%' then 36
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK6%' then 37
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK7%' then 38
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'CBK8%' then 39
			--First three
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC1%' then 2
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC2%' then 3
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC3%' then 4
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC4%' then 5
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC5%' then 6
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC6%' then 7
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC7%' then 8
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC8%' then 9
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NC9%' then 10
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC1%' then 17
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC2%' then 18
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC3%' then 19
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC4%' then 20
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC5%' then 21
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC6%' then 22
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC7%' then 23
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC8%' then 24
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UC9%' then 25
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'APS%' then 1
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NKA%' then 35
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NRA%' then 4
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UKA%' then 51
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'URA%' then 19
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NKD%' then 39
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NRD%' then 5
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UKD%' then 55
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'URD%' then 20
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NKE%' then 36
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NRE%' then 7
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UKE%' then 52
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'URE%' then 22
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NKN%' then 42
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NRN%' then 9
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UKN%' then 56
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'URN%' then 24
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NKP%' then 34
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NRP%' then 2
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UKP%' then 50
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'URP%' then 17
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NKL%' then 38
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NRL%' then 10
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UKL%' then 54
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'URL%' then 25
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NKS%' then 37
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NRS%' then 8
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UKS%' then 53
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'URS%' then 23
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NKI%' then 32
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'NRI%' then 6
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'UKI%' then 48
			WHEN UPPER(NVL(dtd.external_keycode, dtd.internal_keycode)) like 'URI%' then 21
			--Else
			WHEN upper(dtd.line_item_magazine_code) = 'NCBK' then 32
			WHEN upper(dtd.line_item_magazine_code) = 'NCPR' then 6
			WHEN upper(dtd.line_item_magazine_code) = 'UCBK' then 48
			WHEN upper(dtd.line_item_magazine_code) = 'UCPR' then 21
		END aps_src_cd
		, CAST(NULL AS TIMESTAMP) cro_acct_exp_dt
		, dtd.cancelled_by
	FROM
		prod.action_header ah
		JOIN prod.digital_transaction dt ON ah.hash_action_id = dt.hash_action_id
		JOIN prod.digital_transaction_detail dtd ON ah.hash_action_id = dtd.hash_action_id
		LEFT JOIN agg.cte_return_record ret ON dtd.line_item_id = ret.reference_id
		LEFT JOIN prod.sku_lkup sl ON dtd.sku_num = sl.sku_num
		LEFT JOIN prod.account acct ON ah.hash_account_id = acct.hash_account_id
	WHERE COALESCE(dtd.reference_id, 0) = 0;
		--ah.source_name = 'PWI' /* Removed by DD as hash_action_id will grab only the DIGITAL records */
		--AND ah.action_type = 'ORDER'


/***** TRUNCATE AGG *****/
TRUNCATE TABLE prod.agg_digital_item;

/***** LOAD DIGITAL TO AGG *****/
INSERT INTO prod.agg_digital_item (
	  individual_id
	, itm_id
	, ord_id
	, hash_account_id
	, source_name
	, account_subtype_code
	, acct_id
	, seq_num
	, stat_cd
	, sku_num
	, tot_gross_amt
	, tot_amt
	, strt_dt
	, end_dt
	, mag_cd
	, crd_stat_cd
	, canc_flg
	, term_mth_cnt
	, rnw_cd
	, set_cd
	, ext_keycd
	, int_keycd
	, canc_rsn_cd
	, shp_meth_cd
	, crt_dt
	, rpt_nam
	, sub_id
	, sub_rnw_ind
	, svc_stat_cd
	, canc_dt
	, pmt_dt
	, sub_src_cd
	, aps_src_cd
	, cro_acct_exp_dt
	, cancelled_by
)
SELECT
	  individual_id
	, itm_id
	, ord_id
	, hash_account_id
	, source_name
	, account_subtype_code
	, acct_id
	, seq_num
	, stat_cd
	, sku_num
	, tot_gross_amt
	, tot_amt
	, strt_dt
	, 	CASE
			WHEN canc_dt is null AND line_item_order_end_date is null AND line_item_subsrc_end_date is null
				THEN DATEADD(MONTH, term_mth_cnt, strt_dt)
			WHEN (crd_stat_cd = 'F' OR CANC_RSN_CD IN ('50', '06')) AND canc_dt < '20130208' THEN canc_dt
			WHEN cancelled_by = 'CBFEED' AND canc_dt < '20130208' THEN canc_dt
			/* DD changed from line_item_subscr_end_date to order_end_date */ 
			/*SD 20171007: Changed back to line_item_subsrc_end_date*/
			WHEN subscription_id IS NOT NULL THEN line_item_subsrc_end_date 
			WHEN line_item_order_end_date IS NULL AND mag_cd IN ('NCPR', 'UCPR') THEN line_item_subsrc_end_date
			ELSE line_item_order_end_date
		END end_dt
	, mag_cd
	, crd_stat_cd
	, 	CASE
			WHEN dtd_credit_status = 'F' AND canc_dt < to_date('2013-02-08', 'YYYY-MM-DD') THEN 'Y'
			WHEN dtd_cancel_reason_code IN ('50','06') AND canc_dt <  to_date('2013-02-08', 'YYYY-MM-DD') THEN 'Y'
			WHEN (dtd_line_item_cancel_flag IS NULL AND dtd_line_item_status IN ('R','J')) THEN 'Y'
			ELSE dtd_line_item_cancel_flag
		END canc_flg
	, term_mth_cnt
	, rnw_cd
	, set_cd
	, ext_keycd
	, int_keycd
	, canc_rsn_cd
	, shp_meth_cd
	, crt_dt
	, UPPER(rpt_nam) as "rpt_nam"
	, sub_id
	, sub_rnw_ind
	, svc_stat_cd
	, canc_dt
	, pmt_dt
	, CASE WHEN sub_src_cd = '' THEN NULL
		ELSE sub_src_cd
	  END sub_src_cd
	, aps_src_cd
	, cro_acct_exp_dt
	, cancelled_by
FROM agg.cte_digital_item;


/* LOAD PRINT TO AGG */
INSERT INTO prod.agg_digital_item (
	  individual_id
	, itm_id
	, ord_id
	, hash_account_id
	, source_name
	, account_subtype_code
	, acct_id
	, seq_num
	, stat_cd
	, sku_num
	, tot_gross_amt
	, tot_amt
	, strt_dt
	, end_dt
	, mag_cd
	, crd_stat_cd
	, canc_flg
	, term_mth_cnt
	, rnw_cd
	, set_cd
	, ext_keycd
	, int_keycd
	, canc_rsn_cd
	, shp_meth_cd
	, crt_dt
	, rpt_nam
	, sub_id
	, sub_rnw_ind
	, svc_stat_cd
	, canc_dt
	, pmt_dt
	, sub_src_cd
	, aps_src_cd
	, cro_acct_exp_dt
	, cancelled_by
)
SELECT
	  ah.individual_id
	, ah.action_id itm_id
	, ah.action_id ord_id
	, acct.hash_account_id
	, acct.source_name
	, acct.account_subtype_code
	, ah.source_account_id acct_id
	, CAST(NULL as NUMERIC(18,0)) seq_num
	, ah.credit_status_code stat_cd
	, CAST(NULL as NUMERIC(18,0)) sku_num
	, ah.action_amount tot_gross_amt
	, CASE
		WHEN pt.set_code = 'A'
			THEN ah.action_amount
		WHEN pt.set_code IN ('B', 'D')
			THEN COALESCE(ah.action_amount + rcp_ord.sum_action_amount, ah.action_amount, rcp_ord.sum_action_amount)
	  END tot_amt
	, CASE
		WHEN COALESCE(pt.original_strt_iss_num, 0) = 0
			THEN DATEADD(MONTH, 2, ah.order_date)
		ELSE
			cre_strt_iss_lkp.pub_date
	  END strt_dt
	, CASE
		WHEN COALESCE(pt.original_strt_iss_num, 0) = 0
			THEN DATEADD(MONTH, CAST(pt.term_month_cnt AS INTEGER) + 2, ah.order_date)
		ELSE
			cre_end_iss_lkp.pub_date
	  END end_dt
	, 'CRO' mag_cd
	, DECODE(ah.credit_status_code, 'G', 'C', ah.credit_status_code) crd_stat_cd
	, DECODE(pt.cancel_flag, 'A', 'N', 'B', 'Y') canc_flg
	, pt.term_month_cnt term_mth_cnt
	, DECODE(pt.renewal_code, 'C', 'B', pt.renewal_code) rnw_cd
	, pt.set_code set_cd
	, CAST(NULL AS VARCHAR(150)) ext_keycd
	, ah.keycode int_keycd
	, last_canc.cancel_reason_code canc_rsn_cd
	, CAST(NULL AS CHAR(6))shp_meth_cd
	, ah.action_date crt_dt
	, CAST(NULL AS VARCHAR(256)) rpt_nam
	, CAST(NULL AS NUMERIC(38,0)) sub_id
	, DECODE(LEFT(UPPER(ad.keycode), 4), 'AUTO', 'Y', 'N') sub_rnw_ind
    , CASE
        WHEN ah.PLAN_STATUS_CODE = 'B' AND ad.SVC_STATUS_CODE = 'C'
			AND ad.CRO_EXP_DATE >= previous.thursday THEN 'A'
        WHEN ah.PLAN_STATUS_CODE = 'B' AND ad.SVC_STATUS_CODE = 'C' 
            AND ad.CRO_EXP_DATE < previous.thursday THEN 'C'
        WHEN ah.PLAN_STATUS_CODE = 'B' AND ad.SVC_STATUS_CODE = 'B' THEN 'C'
        WHEN ah.PLAN_STATUS_CODE = 'B' AND ad.SVC_STATUS_CODE = 'E' THEN 'P'  
		WHEN ah.PLAN_STATUS_CODE = 'B' THEN ad.SVC_STATUS_CODE
        WHEN ah.PLAN_STATUS_CODE = 'A' THEN 'F'
        WHEN ah.PLAN_STATUS_CODE = 'C' THEN 'E'
        ELSE ah.PLAN_STATUS_CODE
      END svc_stat_cd
	, last_canc.action_date canc_dt
	, CASE
		WHEN pt.set_code = 'A'
		  THEN ah.ACTION_DATE
		WHEN pt.set_code IN ('B','D')
		  THEN COALESCE(rcp_ord.max_action_date, ah.action_date)
	  END pmt_dt
	, CASE
		WHEN ah.keycode IS NULL THEN 'I'
		WHEN LEFT(UPPER(ISNULL(ah.keycode)),2) = 'WF' THEN 'X'
		ELSE LEFT(UPPER(ISNULL(ah.keycode)),1)
	  END sub_src_cd
	, CAST(NULL AS VARCHAR(152)) aps_src_cd
	, ad.CRO_EXP_DATE cro_acct_exp_dt
	, CAST(NULL AS VARCHAR(20)) cancelled_by
FROM
	prod.action_header ah
	JOIN prod.PRINT_TRANSACTION pt ON ah.hash_action_id = pt.hash_action_id
	JOIN prod.PRINT_ACCOUNT_DETAIL ad ON ah.hash_account_id = ad.hash_account_id
	LEFT JOIN agg.cte_recipient_order rcp_ord ON ah.addtl_action_id = rcp_ord.addtl_action_id
	LEFT JOIN agg.cte_last_cancellation last_canc ON ah.action_id = last_canc.action_id
	LEFT JOIN etl_proc.lookup_magazine cre_strt_iss_lkp ON 
			pt.original_strt_iss_num = cre_strt_iss_lkp.iss_num 
			AND cre_strt_iss_lkp.magazine_code = 'CRE'
	LEFT JOIN etl_proc.lookup_magazine cre_end_iss_lkp ON 
			(pt.original_strt_iss_num + pt.term_month_cnt) = cre_end_iss_lkp.iss_num 
			AND cre_end_iss_lkp.magazine_code = 'CRE'
	LEFT JOIN prod.account acct ON ah.hash_account_id = acct.hash_account_id
	CROSS JOIN (SELECT DATEADD(DAY, CASE WHEN DATEPART(DOW, CURRENT_DATE) < 5 THEN -3 ELSE 4 END - DATEPART(DOW, CURRENT_DATE), CURRENT_DATE) thursday) previous
WHERE 
	--ah.source_name = 'CDS' AND /* Removed by DD as hash_action_id will grab only the PRINT records */
	ah.action_type = 'ORDER'
	AND ad.magazine_code = 'CRE';