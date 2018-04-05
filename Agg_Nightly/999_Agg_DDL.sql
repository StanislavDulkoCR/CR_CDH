drop table if exists prod.agg_abq_donation;                                                                                                                     
 create table prod.agg_abq_donation                                                                                                                             
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	abq_yr char(4) not null encode bytedict,                                                                                                                       
	don_amt numeric(20,2) encode bytedict,                                                                                                                         
	key_abq_donation varchar(64),                                                                                                                                  
	constraint agg_abq_donation_pkey                                                                                                                               
		primary key (individual_id, abq_yr)                                                                                                                           
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, abq_yr)                                                                                                                      
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_abq_response;                                                                                                                     
 create table prod.agg_abq_response                                                                                                                             
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	abq_yr char(4) not null,                                                                                                                                       
	abq_qtr char(2),                                                                                                                                               
	keycode varchar(10) not null,                                                                                                                                  
	mbr_flg char,                                                                                                                                                  
	chnl_cd char,                                                                                                                                                  
	progress_cd numeric(2),                                                                                                                                        
	key_abq_response varchar(64) not null,                                                                                                                         
	constraint agg_abq_response_pkey                                                                                                                               
		primary key (individual_id, abq_yr, keycode)                                                                                                                  
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_abq_summary;                                                                                                                      
 create table prod.agg_abq_summary                                                                                                                              
(                                                                                                                                                               
	individual_id bigint not null distkey                                                                                                                          
		constraint agg_abq_summary_pkey1                                                                                                                              
			primary key,                                                                                                                                                 
	hh_id bigint,                                                                                                                                                  
	abq_dnr_flg char,                                                                                                                                              
	abq_mbr_flg char,                                                                                                                                              
	abq_rsp_flg char,                                                                                                                                              
	abq_lst_don_amt numeric(12,2),                                                                                                                                 
	abq_ltd_don_amt numeric(12,2),                                                                                                                                 
	abq_max_don_amt numeric(12,2),                                                                                                                                 
	abq_fst_don_dt varchar(4),                                                                                                                                     
	abq_lst_mbr_dt varchar(4),                                                                                                                                     
	abq_lst_non_mbr_dt varchar(4),                                                                                                                                 
	abq_lst_offl_rsp_dt varchar(4),                                                                                                                                
	abq_lst_onl_rsp_dt varchar(4),                                                                                                                                 
	abq_lst_don_dt varchar(4),                                                                                                                                     
	abq_fst_act_keycode varchar(20),                                                                                                                               
	abq_lst_act_keycode varchar(20),                                                                                                                               
	abq_lst_rsp_media char,                                                                                                                                        
	abq_mbr_brks_cnt integer,                                                                                                                                      
	abq_rsp_brks_cnt integer,                                                                                                                                      
	abq_curr_mbr_yrs_cnt integer,                                                                                                                                  
	abq_curr_rsp_yrs_cnt integer,                                                                                                                                  
	abq_ltd_don_cnt integer,                                                                                                                                       
	abq_ltd_mbr_cnt integer,                                                                                                                                       
	abq_ltd_rsp_cnt integer,                                                                                                                                       
	abq_fst_rsp_dt varchar(4),                                                                                                                                     
	abq_lst_rsp_dt varchar(4),                                                                                                                                     
	osl_hh_id varchar(100)                                                                                                                                         
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_account_number;                                                                                                                   
 create table prod.agg_account_number                                                                                                                           
(                                                                                                                                                               
	individual_id bigint distkey,                                                                                                                                  
	hash_account_id varchar(64),                                                                                                                                   
	source_name varchar(64),                                                                                                                                       
	account_subtype_code varchar(20),                                                                                                                              
	acct_id varchar(64),                                                                                                                                           
	acct_prefix varchar(20) not null,                                                                                                                              
	acct_num varchar(64) not null,                                                                                                                                 
	first_date timestamp,                                                                                                                                          
	acct_lst_logn_dt timestamp,                                                                                                                                    
	acct_logn_cnt integer,                                                                                                                                         
	group_id varchar(20),                                                                                                                                          
	usr_src_ind varchar(15),                                                                                                                                       
	purged_ind char,                                                                                                                                               
	key_account_number varchar(48),                                                                                                                                
	constraint agg_account_number_pkey                                                                                                                             
		primary key (acct_num, acct_prefix)                                                                                                                           
)                                                                                                                                                               
diststyle key                                                                                                                                                   
sortkey(individual_id, hash_account_id)                                                                                                                         
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_app_session;                                                                                                                      
 create table prod.agg_app_session                                                                                                                              
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	visit_id varchar(50) not null,                                                                                                                                 
	visit_num varchar(10) not null encode bytedict,                                                                                                                
	app_indicator varchar(8),                                                                                                                                      
	visit_start_time timestamp,                                                                                                                                    
	visit_time numeric(18) encode delta32k,                                                                                                                        
	app_version varchar(255),                                                                                                                                      
	install_dt timestamp,                                                                                                                                          
	count_page_views integer encode bytedict,                                                                                                                      
	mobile_device_desc varchar(100),                                                                                                                               
	mobile_device_type varchar(10),                                                                                                                                
	operating_system varchar(255),                                                                                                                                 
	entry_page varchar(255),                                                                                                                                       
	login_flg char,                                                                                                                                                
	key_app_session varchar(64) not null,                                                                                                                          
	constraint agg_app_session_pkey                                                                                                                                
		primary key (individual_id, visit_id, visit_num)                                                                                                              
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, visit_id, visit_num)                                                                                                         
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_books_item;                                                                                                                       
 create table prod.agg_books_item                                                                                                                               
(                                                                                                                                                               
	individual_id bigint not null,                                                                                                                                 
	hash_account_id varchar(64) not null distkey,                                                                                                                  
	source_name varchar(64),                                                                                                                                       
	account_subtype_code varchar(20),                                                                                                                              
	acct_id varchar(64) not null,                                                                                                                                  
	ord_num bigint not null,                                                                                                                                       
	seq_num integer not null,                                                                                                                                      
	prod_num varchar(50),                                                                                                                                          
	cdr_flg char,                                                                                                                                                  
	recip_acct_id varchar(50),                                                                                                                                     
	dnr_flg char,                                                                                                                                                  
	trn_dt timestamp,                                                                                                                                              
	qty_ord numeric(5),                                                                                                                                            
	qty_ret_can numeric(5),                                                                                                                                        
	amt_ret_can numeric(20,2),                                                                                                                                     
	ret_canc_dt timestamp,                                                                                                                                         
	ret_canc_rsn_cd varchar(10),                                                                                                                                   
	itm_typ_cd varchar(6),                                                                                                                                         
	itm_prod_gross_amt numeric(20,2) encode bytedict,                                                                                                              
	itm_prod_amt numeric(20,2),                                                                                                                                    
	itm_ship_amt numeric(20,2) encode bytedict,                                                                                                                    
	itm_misc_amt numeric(20,2),                                                                                                                                    
	itm_tax_amt numeric(20,2),                                                                                                                                     
	stat_cd varchar(10),                                                                                                                                           
	key_books_item varchar(64) not null,                                                                                                                           
	constraint agg_books_item_pkey                                                                                                                                 
		primary key (individual_id, acct_id, ord_num, seq_num)                                                                                                        
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, acct_id, ord_num, seq_num, prod_num)                                                                                         
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_books_order;                                                                                                                      
 create table prod.agg_books_order                                                                                                                              
(                                                                                                                                                               
	individual_id bigint not null,                                                                                                                                 
	hash_account_id varchar(64) distkey,                                                                                                                           
	source_name varchar(64),                                                                                                                                       
	account_subtype_code varchar(20),                                                                                                                              
	acct_id varchar(64) not null,                                                                                                                                  
	ord_num bigint not null,                                                                                                                                       
	dnr_recip_cd varchar(10),                                                                                                                                      
	ord_prem_flg varchar(10),                                                                                                                                      
	ord_cdr_flg varchar(10),                                                                                                                                       
	entr_typ_cd varchar(10) encode bytedict,                                                                                                                       
	trn_dt timestamp,                                                                                                                                              
	ord_dt timestamp,                                                                                                                                              
	ord_stat_cd varchar(10),                                                                                                                                       
	keycode varchar(20),                                                                                                                                           
	src_cd varchar(10) encode bytedict,                                                                                                                            
	ord_prod_amt numeric(12,2) encode bytedict,                                                                                                                    
	ord_ship_amt numeric(9,2),                                                                                                                                     
	ord_tax_amt numeric(9,2),                                                                                                                                      
	ord_misc_amt numeric(9,2),                                                                                                                                     
	ord_gft_cert_amt numeric(11,2),                                                                                                                                
	ord_coupn_amt numeric(9,2),                                                                                                                                    
	ord_disc_amt numeric(9,2),                                                                                                                                     
	ord_value_amt numeric(12,2) encode bytedict,                                                                                                                   
	istlmt_cd varchar(10) encode bytedict,                                                                                                                         
	gift_ord_cd varchar(10),                                                                                                                                       
	currncy_typ_cd varchar(10),                                                                                                                                    
	crd_stat_cd varchar(10) encode bytedict,                                                                                                                       
	pmt_amt numeric(20,2) encode bytedict,                                                                                                                         
	lst_pmt_dt timestamp,                                                                                                                                          
	lst_pmt_ret_dt timestamp,                                                                                                                                      
	wr_off_amt numeric(20,2),                                                                                                                                      
	lst_wr_off_dt timestamp,                                                                                                                                       
	coll_amt numeric(20,2),                                                                                                                                        
	lst_coll_dt timestamp,                                                                                                                                         
	key_books_order varchar(64) not null,                                                                                                                          
	constraint agg_books_order_pkey                                                                                                                                
		primary key (individual_id, acct_id, ord_num)                                                                                                                 
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, acct_id, ord_num)                                                                                                            
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_books_summary;                                                                                                                    
 create table prod.agg_books_summary                                                                                                                            
(                                                                                                                                                               
	individual_id bigint not null distkey                                                                                                                          
		constraint agg_books_summary_pkey                                                                                                                             
			primary key,                                                                                                                                                 
	hh_id bigint,                                                                                                                                                  
	bk_flg varchar(1),                                                                                                                                             
	cdr_actv_flg varchar(1),                                                                                                                                       
	bk_actv_flg varchar(1),                                                                                                                                        
	bk_actv_pd_flg varchar(1),                                                                                                                                     
	bk_ltd_pd_amt numeric(38,2),                                                                                                                                   
	cdr_lst_ord_dt timestamp,                                                                                                                                      
	bk_lst_pmt_dt timestamp,                                                                                                                                       
	cdr_fst_ord_keycode varchar(36),                                                                                                                               
	cdr_lst_ord_keycode varchar(36),                                                                                                                               
	cdr_num_ord bigint                                                                                                                                             
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_build_and_buy;                                                                                                                    
 create table prod.agg_build_and_buy                                                                                                                            
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	bnb_trans_id numeric(38) encode mostly32,                                                                                                                      
	bnb_status varchar(30),                                                                                                                                        
	bnb_cust_state varchar(177),                                                                                                                                   
	bnb_cust_zip varchar(37),                                                                                                                                      
	bnb_new_used varchar(37),                                                                                                                                      
	bnb_lead_recvd_dt timestamp,                                                                                                                                   
	bnb_lead_sales_dt timestamp,                                                                                                                                   
	bnb_approval_dt timestamp,                                                                                                                                     
	bnb_referrer varchar(255),                                                                                                                                     
	bnb_last_visit_dt timestamp,                                                                                                                                   
	bnb_maint_dt timestamp,                                                                                                                                        
	bnb_trans_type varchar(37)                                                                                                                                     
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, bnb_trans_id)                                                                                                                
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_cga;                                                                                                                              
 create table prod.agg_cga                                                                                                                                      
(                                                                                                                                                               
	individual_id bigint,                                                                                                                                          
	don_dt timestamp,                                                                                                                                              
	don_amt numeric(20,2)                                                                                                                                          
)                                                                                                                                                               
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_constituent;                                                                                                                      
 create table prod.agg_constituent                                                                                                                              
(                                                                                                                                                               
	individual_id bigint not null encode delta32k distkey                                                                                                          
		constraint agg_constituent_pkey                                                                                                                               
			primary key,                                                                                                                                                 
	hh_id bigint encode delta32k,                                                                                                                                  
	first_action_dt timestamp,                                                                                                                                     
	last_action_dt timestamp,                                                                                                                                      
	reward_points_total numeric(20) encode bytedict,                                                                                                               
	ytd_action_alerts integer,                                                                                                                                     
	adv_active_flg varchar(1),                                                                                                                                     
	adv_volunteer_flg varchar(1),                                                                                                                                  
	adv_fst_sur_resp_dt timestamp,                                                                                                                                 
	adv_lst_sur_resp_dt timestamp,                                                                                                                                 
	adv_ltd_sur_resp_cnt integer encode delta,                                                                                                                     
	adv_fst_alrt_resp_dt timestamp,                                                                                                                                
	adv_lst_alrt_resp_dt timestamp,                                                                                                                                
	adv_ltd_alrt_resp_cnt bigint,                                                                                                                                  
	adv_fst_int_resp_dt timestamp encode bytedict,                                                                                                                 
	adv_lst_int_resp_dt timestamp encode bytedict,                                                                                                                 
	adv_ltd_int_resp_cnt integer encode delta,                                                                                                                     
	adv_avg_don_amt numeric(12,2),                                                                                                                                 
	adv_lst_don_amt numeric(20,2),                                                                                                                                 
	adv_ltd_don_amt numeric(20,2),                                                                                                                                 
	adv_max_don_amt numeric(20,2),                                                                                                                                 
	adv_fst_don_amt numeric(20,2),                                                                                                                                 
	adv_fst_don_dt timestamp,                                                                                                                                      
	adv_lst_don_dt timestamp,                                                                                                                                      
	adv_ltd_don_cnt integer                                                                                                                                        
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, hh_id)                                                                                                                       
;                                                                                                                                                               

drop table if exists prod.agg_constituent_survey_response;
 create table prod.agg_constituent_survey_response
(
	individual_id bigint not null distkey,
	survey_id varchar(41) not null,
	submit_dt timestamp,
	key_constituent_survey_response varchar(64) not null,
	constraint agg_constituent_survey_response_pkey
		primary key (individual_id, survey_id)
)
diststyle key
interleaved sortkey(individual_id, survey_id)
;

                                                                                                                                                              
                                                                                                                                                                
drop table if exists prod.agg_outsidelist_name_address;                                                                                                         
 create table prod.agg_outsidelist_name_address                                                                                                                 
(                                                                                                                                                               
	individual_id bigint,                                                                                                                                          
	list_select_cd varchar(19),                                                                                                                                    
	campaign_id varchar(10),                                                                                                                                       
	maint_dt timestamp,                                                                                                                                            
	kept_flg char,                                                                                                                                                 
	pass_thru_id varchar(20),                                                                                                                                      
	address_id bigint,                                                                                                                                             
	data_source varchar(16),                                                                                                                                       
	salutation varchar(20),                                                                                                                                        
	first_name varchar(40),                                                                                                                                        
	middle_name varchar(20),                                                                                                                                       
	last_name varchar(40),                                                                                                                                         
	suffix varchar(20),                                                                                                                                            
	business_name varchar(40),                                                                                                                                     
	primary_number varchar(10),                                                                                                                                    
	pre_directional varchar(10),                                                                                                                                   
	street varchar(40),                                                                                                                                            
	street_suffix varchar(10),                                                                                                                                     
	post_directional varchar(10),                                                                                                                                  
	unit_designator varchar(10),                                                                                                                                   
	secondary_number varchar(10),                                                                                                                                  
	additional_address_data varchar(80),                                                                                                                           
	city varchar(40),                                                                                                                                              
	state_province varchar(10),                                                                                                                                    
	postal_cd varchar(15),                                                                                                                                         
	country varchar(20),                                                                                                                                           
	first_dt timestamp,                                                                                                                                            
	dpbc varchar(3),                                                                                                                                               
	crc varchar(4),                                                                                                                                                
	lot varchar(5),                                                                                                                                                
	dsf_deliverability_code varchar(2),                                                                                                                            
	dsf_delivery_type char,                                                                                                                                        
	edit_fail_flg char,                                                                                                                                            
	blank_address char,                                                                                                                                            
	invalid_zip char,                                                                                                                                              
	numeric_name_city char,                                                                                                                                        
	numeric_business char,                                                                                                                                         
	blank_ind_bus char,                                                                                                                                            
	apo_fpo_ind char,                                                                                                                                              
	ncoa_footnote char(2),                                                                                                                                         
	vulgar_names_flg char                                                                                                                                          
)                                                                                                                                                               
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_individual_xographic;                                                                                                             
 create table prod.agg_individual_xographic                                                                                                                     
(                                                                                                                                                               
	individual_id bigint not null encode delta32k distkey                                                                                                          
		constraint agg_individual_xographic_pkey                                                                                                                      
			primary key,                                                                                                                                                 
	gender_cd varchar(8) encode bytedict,                                                                                                                          
	acx_gender_cd varchar(8) encode bytedict,                                                                                                                      
	customer_typ varchar(8),                                                                                                                                       
	dc_online_first_dt timestamp,                                                                                                                                  
	dc_offline_first_dt timestamp,                                                                                                                                 
	dc_fund_first_dt timestamp,                                                                                                                                    
	dc_corp_first_dt timestamp,                                                                                                                                    
	dc_cdb_last_ord_dt timestamp,                                                                                                                                  
	dc_last_aps_ord_dt timestamp,                                                                                                                                  
	cga_dob varchar(8),                                                                                                                                            
	maint_dt timestamp,                                                                                                                                            
	fr_ch_status varchar(8),                                                                                                                                       
	ad_apl_keycode varchar(8)                                                                                                                                      
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, dc_last_aps_ord_dt, acx_gender_cd, customer_typ, dc_online_first_dt, dc_offline_first_dt, dc_fund_first_dt, dc_corp_first_dt)
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_constituent_backup;                                                                                                               
 create table prod.agg_constituent_backup                                                                                                                       
(                                                                                                                                                               
	individual_id bigint not null encode delta32k distkey,                                                                                                         
	hh_id bigint encode delta32k,                                                                                                                                  
	first_action_dt timestamp,                                                                                                                                     
	last_action_dt timestamp,                                                                                                                                      
	reward_points_total numeric(20) encode bytedict,                                                                                                               
	ytd_action_alerts integer,                                                                                                                                     
	adv_active_flg varchar(1),                                                                                                                                     
	adv_volunteer_flg varchar(1),                                                                                                                                  
	adv_fst_sur_resp_dt timestamp,                                                                                                                                 
	adv_lst_sur_resp_dt timestamp,                                                                                                                                 
	adv_ltd_sur_resp_cnt integer encode delta,                                                                                                                     
	adv_fst_alrt_resp_dt timestamp,                                                                                                                                
	adv_lst_alrt_resp_dt timestamp,                                                                                                                                
	adv_ltd_alrt_resp_cnt bigint,                                                                                                                                  
	adv_fst_int_resp_dt timestamp encode bytedict,                                                                                                                 
	adv_lst_int_resp_dt timestamp encode bytedict,                                                                                                                 
	adv_ltd_int_resp_cnt integer encode delta,                                                                                                                     
	adv_avg_don_amt numeric(12,2),                                                                                                                                 
	adv_lst_don_amt numeric(20,2),                                                                                                                                 
	adv_ltd_don_amt numeric(20,2),                                                                                                                                 
	adv_max_don_amt numeric(20,2),                                                                                                                                 
	adv_fst_don_amt numeric(20,2),                                                                                                                                 
	adv_fst_don_dt timestamp,                                                                                                                                      
	adv_lst_don_dt timestamp,                                                                                                                                      
	adv_ltd_don_cnt integer                                                                                                                                        
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, hh_id)                                                                                                                       
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_constituent_nightly;                                                                                                              
 create table prod.agg_constituent_nightly                                                                                                                      
(                                                                                                                                                               
	individual_id bigint not null encode delta32k distkey,                                                                                                         
	hh_id bigint encode delta32k,                                                                                                                                  
	first_action_dt timestamp,                                                                                                                                     
	last_action_dt timestamp,                                                                                                                                      
	reward_points_total numeric(20) encode bytedict,                                                                                                               
	ytd_action_alerts integer,                                                                                                                                     
	adv_active_flg varchar(1),                                                                                                                                     
	adv_volunteer_flg varchar(1),                                                                                                                                  
	adv_fst_sur_resp_dt timestamp,                                                                                                                                 
	adv_lst_sur_resp_dt timestamp,                                                                                                                                 
	adv_ltd_sur_resp_cnt integer encode delta,                                                                                                                     
	adv_fst_alrt_resp_dt timestamp,                                                                                                                                
	adv_lst_alrt_resp_dt timestamp,                                                                                                                                
	adv_ltd_alrt_resp_cnt bigint,                                                                                                                                  
	adv_fst_int_resp_dt timestamp encode bytedict,                                                                                                                 
	adv_lst_int_resp_dt timestamp encode bytedict,                                                                                                                 
	adv_ltd_int_resp_cnt integer encode delta,                                                                                                                     
	adv_avg_don_amt numeric(12,2),                                                                                                                                 
	adv_lst_don_amt numeric(20,2),                                                                                                                                 
	adv_ltd_don_amt numeric(20,2),                                                                                                                                 
	adv_max_don_amt numeric(20,2),                                                                                                                                 
	adv_fst_don_amt numeric(20,2),                                                                                                                                 
	adv_fst_don_dt timestamp,                                                                                                                                      
	adv_lst_don_dt timestamp,                                                                                                                                      
	adv_ltd_don_cnt integer                                                                                                                                        
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, hh_id)                                                                                                                       
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_constituent_alert_response;                                                                                                       
 create table prod.agg_constituent_alert_response                                                                                                               
(                                                                                                                                                               
	individual_id bigint not null encode delta distkey,                                                                                                            
	alert_response_id varchar(20) not null,                                                                                                                        
	response_dt timestamp,                                                                                                                                         
	key_constituent_alert_response varchar(64) not null,                                                                                                           
	constraint agg_constituent_alert_response_pkey                                                                                                                 
		primary key (individual_id, alert_response_id)                                                                                                                
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id)                                                                                                                              
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_constituent_donation;                                                                                                             
 create table prod.agg_constituent_donation                                                                                                                     
(                                                                                                                                                               
	individual_id bigint not null encode delta32k distkey,                                                                                                         
	transaction_id bigint not null,                                                                                                                                
	don_amt numeric(20,2) encode bytedict,                                                                                                                         
	don_dt timestamp,                                                                                                                                              
	key_constituent_donation varchar(64) not null,                                                                                                                 
	constraint agg_constituent_donation_pkey                                                                                                                       
		primary key (individual_id, transaction_id)                                                                                                                   
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, transaction_id)                                                                                                              
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_constituent_interest;                                                                                                             
 create table prod.agg_constituent_interest                                                                                                                     
(                                                                                                                                                               
	individual_id bigint not null encode delta32k distkey,                                                                                                         
	interest_id varchar(72) not null,                                                                                                                              
	interest_name varchar(100),                                                                                                                                    
	parent_interest varchar(100),                                                                                                                                  
	interest_description varchar(255),                                                                                                                             
	chnl_flg varchar(10),                                                                                                                                          
	interest_resp_dt timestamp encode bytedict,                                                                                                                    
	key_constituent_interest varchar(64) not null,                                                                                                                 
	constraint agg_constituent_interest_pkey                                                                                                                       
		primary key (individual_id, interest_id)                                                                                                                      
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, interest_id)                                                                                                                 
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_credit_card;                                                                                                                      
 create table prod.agg_credit_card                                                                                                                              
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	cc_source varchar(25) not null,                                                                                                                                
	cc_acct_num char(4),                                                                                                                                           
	cc_type_cd varchar(20),                                                                                                                                        
	cc_exp_dt varchar(10),                                                                                                                                         
	key_credit_card varchar(64) not null,                                                                                                                          
	constraint agg_credit_card_pkey                                                                                                                                
		primary key (individual_id, cc_source)                                                                                                                        
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_digital_item;                                                                                                                     
 create table prod.agg_digital_item                                                                                                                             
(                                                                                                                                                               
	individual_id bigint not null,                                                                                                                                 
	itm_id bigint not null,                                                                                                                                        
	ord_id bigint not null,                                                                                                                                        
	hash_account_id varchar(64) distkey,                                                                                                                           
	source_name varchar(64),                                                                                                                                       
	account_subtype_code varchar(20),                                                                                                                              
	acct_id varchar(64),                                                                                                                                           
	seq_num numeric(3),                                                                                                                                            
	stat_cd char,                                                                                                                                                  
	sku_num numeric(18),                                                                                                                                           
	tot_gross_amt numeric(12,2),                                                                                                                                   
	tot_amt numeric(38,2),                                                                                                                                         
	strt_dt timestamp,                                                                                                                                             
	end_dt timestamp,                                                                                                                                              
	mag_cd varchar(25),                                                                                                                                            
	crd_stat_cd char,                                                                                                                                              
	canc_flg char,                                                                                                                                                 
	term_mth_cnt numeric(38),                                                                                                                                      
	rnw_cd char,                                                                                                                                                   
	set_cd char(3),                                                                                                                                                
	ext_keycd varchar(150),                                                                                                                                        
	int_keycd varchar(150),                                                                                                                                        
	canc_rsn_cd varchar(20),                                                                                                                                       
	shp_meth_cd char(6),                                                                                                                                           
	crt_dt timestamp,                                                                                                                                              
	rpt_nam varchar(250),                                                                                                                                          
	sub_id numeric(38),                                                                                                                                            
	sub_rnw_ind char,                                                                                                                                              
	svc_stat_cd char,                                                                                                                                              
	canc_dt timestamp,                                                                                                                                             
	pmt_dt timestamp,                                                                                                                                              
	sub_src_cd varchar(150),                                                                                                                                       
	aps_src_cd varchar(152),                                                                                                                                       
	cro_acct_exp_dt timestamp,                                                                                                                                     
	cancelled_by varchar(20),                                                                                                                                      
	key_digital_item varchar(64) not null,                                                                                                                         
	constraint agg_digital_item_pkey                                                                                                                               
		primary key (individual_id, itm_id, ord_id)                                                                                                                   
)                                                                                                                                                               
diststyle key                                                                                                                                                   
sortkey(hash_account_id, individual_id, ord_id, sku_num, itm_id)                                                                                                
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_digital_login;                                                                                                                    
 create table prod.agg_digital_login                                                                                                                            
(                                                                                                                                                               
	individual_id bigint not null encode delta distkey,                                                                                                            
	external_key varchar(64) not null,                                                                                                                             
	login_week integer not null encode bytedict,                                                                                                                   
	week_login_count integer not null encode delta,                                                                                                                
	key_digital_login varchar(64) not null,                                                                                                                        
	constraint agg_digital_login_pkey                                                                                                                              
		primary key (individual_id, external_key, login_week)                                                                                                         
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_digital_order;                                                                                                                    
 create table prod.agg_digital_order                                                                                                                            
(                                                                                                                                                               
	individual_id bigint not null,                                                                                                                                 
	hash_account_id varchar(64) distkey,                                                                                                                           
	source_name varchar(64),                                                                                                                                       
	account_subtype_code varchar(20),                                                                                                                              
	acct_id varchar(64) not null,                                                                                                                                  
	ord_id bigint not null,                                                                                                                                        
	stat_cd char,                                                                                                                                                  
	ord_dt timestamp,                                                                                                                                              
	entr_typ_cd char(2),                                                                                                                                           
	pd_dt timestamp,                                                                                                                                               
	pd_amt numeric(38,2),                                                                                                                                          
	crt_dt timestamp,                                                                                                                                              
	key_digital_order varchar(64) not null,                                                                                                                        
	constraint agg_digital_order_pkey                                                                                                                              
		primary key (individual_id, acct_id, ord_id)                                                                                                                  
)                                                                                                                                                               
diststyle key                                                                                                                                                   
sortkey(hash_account_id, stat_cd, entr_typ_cd, individual_id, ord_id, acct_id, ord_dt)                                                                          
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_digital_summary;                                                                                                                  
 create table prod.agg_digital_summary                                                                                                                          
(                                                                                                                                                               
	individual_id bigint distkey,                                                                                                                                  
	hh_id bigint,                                                                                                                                                  
	cro_actv_flg varchar(1),                                                                                                                                       
	cro_actv_pd_flg varchar(1),                                                                                                                                    
	ncbk_actv_pd_flg varchar(1),                                                                                                                                   
	cro_annual_flg varchar(1),                                                                                                                                     
	crmg_canc_bad_dbt_flg varchar(1),                                                                                                                              
	cro_canc_bad_dbt_flg varchar(1),                                                                                                                               
	crmg_canc_cust_flg varchar(1),                                                                                                                                 
	cro_canc_cust_flg varchar(1),                                                                                                                                  
	cro_cr_stat_cd varchar(17),                                                                                                                                    
	cro_exp_dt timestamp,                                                                                                                                          
	crmg_lst_canc_bad_dbt_dt timestamp,                                                                                                                            
	crmg_lst_canc_cust_dt timestamp,                                                                                                                               
	cro_lst_canc_cust_dt timestamp,                                                                                                                                
	ncbk_lst_ord_dt timestamp,                                                                                                                                     
	cro_lst_pmt_dt timestamp,                                                                                                                                      
	aps_flg varchar(1),                                                                                                                                            
	crmg_flg varchar(1),                                                                                                                                           
	cro_flg varchar(1),                                                                                                                                            
	ncbk_flg varchar(1),                                                                                                                                           
	uc_rpts_flg varchar(1),                                                                                                                                        
	ucbk_flg varchar(1),                                                                                                                                           
	cro_curr_mbr_keycode varchar(282),                                                                                                                             
	cro_curr_ord_keycode varchar(166),                                                                                                                             
	cro_fst_mbr_keycode varchar(166),                                                                                                                              
	ncbk_fst_ord_keycode varchar(174),                                                                                                                             
	cro_lst_ord_keycode varchar(166),                                                                                                                              
	ncbk_lst_ord_keycode varchar(174),                                                                                                                             
	cro_lst_sub_amt varchar(37),                                                                                                                                   
	cro_monthly_flg varchar(1),                                                                                                                                    
	cro_non_sub_dnr_flg varchar(1),                                                                                                                                
	cro_brks_cnt bigint,                                                                                                                                           
	cro_rnw_cnt numeric(20),                                                                                                                                       
	cro_rec_flg varchar(1),                                                                                                                                        
	cro_svc_stat_cd varchar(17),                                                                                                                                   
	cro_curr_mbr_src_cd varchar(282),                                                                                                                              
	cro_curr_ord_src_cd varchar(166),                                                                                                                              
	cro_fst_ord_src_cd varchar(166),                                                                                                                               
	ncbk_fst_ord_src_cd varchar(176),                                                                                                                              
	cro_lst_ord_src_cd varchar(166),                                                                                                                               
	ncbk_lst_ord_src_cd varchar(176),                                                                                                                              
	cro_prior_mbr_src_cd varchar(166),                                                                                                                             
	cro_sub_dnr_flg varchar(1),                                                                                                                                    
	cro_curr_ord_term varchar(56),                                                                                                                                 
	cro_fst_dnr_dt timestamp,                                                                                                                                      
	cro_auto_rnw_flg varchar(1),                                                                                                                                   
	cro_non_dnr_flg varchar(1),                                                                                                                                    
	cro_curr_mbr_dt timestamp,                                                                                                                                     
	cro_ltd_pd_amt numeric(38,2),                                                                                                                                  
	cro_curr_ord_dt timestamp,                                                                                                                                     
	cro_fst_ord_dt timestamp,                                                                                                                                      
	ncbk_fst_ord_dt timestamp,                                                                                                                                     
	cro_lst_canc_bad_dbt_dt timestamp,                                                                                                                             
	cro_lst_ord_dt timestamp,                                                                                                                                      
	cro_lst_dnr_ord_dt timestamp,                                                                                                                                  
	crmg_canc_cust_cnt bigint,                                                                                                                                     
	cro_canc_cust_cnt bigint,                                                                                                                                      
	cro_ord_cnt bigint,                                                                                                                                            
	cro_dm_ord_cnt bigint,                                                                                                                                         
	cro_em_ord_cnt bigint,                                                                                                                                         
	cro_dnr_ord_cnt bigint,                                                                                                                                        
	cro_lst_sub_ord_role_cd varchar(1),                                                                                                                            
	lst_logn_dt timestamp,                                                                                                                                         
	logn_cnt bigint,                                                                                                                                               
	cro_lt_sub_flg varchar(1),                                                                                                                                     
	carp_actv_flg varchar(1),                                                                                                                                      
	carp_actv_pd_flg varchar(1),                                                                                                                                   
	osl_hh_id bigint                                                                                                                                               
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id)                                                                                                                              
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_email;                                                                                                                            
 create table prod.agg_email                                                                                                                                    
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	email_type_cd char not null,                                                                                                                                   
	email_address varchar(256),                                                                                                                                    
	email_id bigint,                                                                                                                                               
	valid_flag char,                                                                                                                                               
	eff_dt timestamp,                                                                                                                                              
	end_dt timestamp,                                                                                                                                              
	data_source varchar(16) encode bytedict,                                                                                                                       
	src_valid_flag char,                                                                                                                                           
	src_delv_ind char,                                                                                                                                             
	email_client_app varchar(256),                                                                                                                                 
	fst_dt timestamp,                                                                                                                                              
	key_email varchar(64) not null                                                                                                                                 
		constraint agg_email_pkey                                                                                                                                     
			primary key                                                                                                                                                  
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_email_dev_response_detail;                                                                                                        
 create table prod.agg_email_dev_response_detail                                                                                                                
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	device_name varchar(256) not null,                                                                                                                             
	program_name varchar(256),                                                                                                                                     
	campaign_name varchar(1024),                                                                                                                                   
	cell_id bigint not null,                                                                                                                                       
	cell_name varchar(256),                                                                                                                                        
	keycd varchar(30),                                                                                                                                             
	device_type char,                                                                                                                                              
	operating_system varchar(256),                                                                                                                                 
	click_flg char,                                                                                                                                                
	response_dt timestamp,                                                                                                                                         
	key_email_dev varchar(64) not null,                                                                                                                            
	constraint agg_email_dev_pkey                                                                                                                                  
		primary key (individual_id, device_name, cell_id)                                                                                                             
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_email_response_summary;                                                                                                           
 create table prod.agg_email_response_summary                                                                                                                   
(                                                                                                                                                               
	individual_id bigint not null distkey                                                                                                                          
		constraint agg_email_response_pkey                                                                                                                            
			primary key,                                                                                                                                                 
	open_cnt integer,                                                                                                                                              
	click_cnt integer,                                                                                                                                             
	bounce_cnt integer,                                                                                                                                            
	lst_open_dt timestamp,                                                                                                                                         
	lst_click_dt timestamp,                                                                                                                                        
	lst_bounce_dt timestamp,                                                                                                                                       
	opt_out_cnt integer,                                                                                                                                           
	lst_opt_out_dt timestamp,                                                                                                                                      
	lst_mob_dev varchar(30),                                                                                                                                       
	lst_mob_dev_dt timestamp                                                                                                                                       
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_ereader_subscription;                                                                                                             
 create table prod.agg_ereader_subscription                                                                                                                     
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	vendor_id varchar(25) not null,                                                                                                                                
	report_start_date timestamp not null,                                                                                                                          
	report_end_date timestamp not null,                                                                                                                            
	product varchar(20),                                                                                                                                           
	source varchar(20),                                                                                                                                            
	device varchar(30),                                                                                                                                            
	orig_email_address varchar(100),                                                                                                                               
	orig_postal_code varchar(10),                                                                                                                                  
	maintenance_date timestamp,                                                                                                                                    
	key_ereader_subscription varchar(64) not null                                                                                                                  
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, vendor_id)                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_fundraising_donation;                                                                                                             
 create table prod.agg_fundraising_donation                                                                                                                     
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	don_dt timestamp not null,                                                                                                                                     
	don_gross_amt numeric(12,2),                                                                                                                                   
	don_amt numeric(12,2),                                                                                                                                         
	pmt_typ_cd varchar(1),                                                                                                                                         
	keycode varchar(64) not null,                                                                                                                                  
	rf_amt numeric(12,2),                                                                                                                                          
	don_ind varchar(1),                                                                                                                                            
	don_cd varchar(2),                                                                                                                                             
	recur_don varchar(1),                                                                                                                                          
	key_fundraising_donation varchar(64) not null,                                                                                                                 
	constraint agg_fundraising_donation2_pkey                                                                                                                      
		primary key (individual_id, don_dt, keycode)                                                                                                                  
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, don_dt, pmt_typ_cd, keycode, don_ind, don_cd, recur_don)                                                                     
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_fundraising_name;                                                                                                                 
 create table prod.agg_fundraising_name                                                                                                                         
(                                                                                                                                                               
	individual_id bigint not null encode delta32k distkey                                                                                                          
		constraint agg_fundraising_name_pkey                                                                                                                          
			primary key,                                                                                                                                                 
	salutation varchar(20),                                                                                                                                        
	first_name varchar(40) encode text255,                                                                                                                         
	middle_name varchar(20) encode bytedict,                                                                                                                       
	last_name varchar(40),                                                                                                                                         
	suffix varchar(20)                                                                                                                                             
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_fundraising_summary;                                                                                                              
 create table prod.agg_fundraising_summary                                                                                                                      
(                                                                                                                                                               
	individual_id bigint distkey,                                                                                                                                  
	hh_id bigint,                                                                                                                                                  
	fr_avg_don_amt numeric(20,2),                                                                                                                                  
	fr_avg_gross_don_amt numeric(20,2),                                                                                                                            
	fr_lst_don_amt numeric(20,2),                                                                                                                                  
	fr_ltd_don_amt numeric(20,2),                                                                                                                                  
	fr_max_don_amt numeric(20,2),                                                                                                                                  
	fr_1_6_mth_don_amt numeric(20,2),                                                                                                                              
	fr_13_24_mth_don_amt numeric(20,2),                                                                                                                            
	fr_25_36_mth_don_amt numeric(20,2),                                                                                                                            
	fr_37_plus_mth_don_amt numeric(20,2),                                                                                                                          
	fr_7_12_mth_don_amt numeric(20,2),                                                                                                                             
	fr_prior_don_amt numeric(20,2),                                                                                                                                
	fr_fst_don_dt timestamp,                                                                                                                                       
	fr_lst_don_dt timestamp,                                                                                                                                       
	fr_prior_don_dt timestamp,                                                                                                                                     
	fr_dnr_act_flg varchar(1),                                                                                                                                     
	fr_dnr_inact_flg varchar(1),                                                                                                                                   
	fr_dnr_lps_flg varchar(1),                                                                                                                                     
	fr_mbr_gvng_lvl_cd varchar(4),                                                                                                                                 
	fr_mbr_exp_dt timestamp,                                                                                                                                       
	fr_fst_act_keycode varchar(80),                                                                                                                                
	fr_lst_act_keycode varchar(80),                                                                                                                                
	fr_prior_act_keycode varchar(64),                                                                                                                              
	fr_ltd_don_cnt bigint,                                                                                                                                         
	fr_don_ref_cnt bigint,                                                                                                                                         
	fr_times_ren_cnt double precision,                                                                                                                             
	fr_lst_don_src_cd varchar(4),                                                                                                                                  
	fr_lst_rfl_don_amt numeric(20,2),                                                                                                                              
	fr_lst_non_rfl_don_amt numeric(20,2),                                                                                                                          
	fr_mbr_frst_strt_dt timestamp,                                                                                                                                 
	fr_mbr_lst_keycode varchar(10),                                                                                                                                
	fr_mbr_lst_ren_don_amt numeric(20,2),                                                                                                                          
	fr_tof_cd varchar(1),                                                                                                                                          
	fr_lt_dnr_flg varchar(1),                                                                                                                                      
	fr_mbr_lst_add_don_amt numeric(20,2),                                                                                                                          
	fr_mbr_lst_add_don_dt timestamp,                                                                                                                               
	fr_mbr_pres_cir_frst_strt_dt timestamp,                                                                                                                        
	fr_fst_don_keycode varchar(80),                                                                                                                                
	fr_max_don_amt_12_mth numeric(20,2),                                                                                                                           
	fr_max_don_amt_24_mth numeric(20,2),                                                                                                                           
	fr_max_don_amt_36_mth numeric(20,2),                                                                                                                           
	fr_track_number varchar(1),                                                                                                                                    
	fr_conv_tag_rsp_flg varchar(1),                                                                                                                                
	fr_fst_don_amt numeric(20,2),                                                                                                                                  
	fr_lst_rfl_don_dt timestamp,                                                                                                                                   
	fr_lst_non_rfl_don_dt timestamp,                                                                                                                               
	fr_lst_eml_don_amt numeric(20,2),                                                                                                                              
	fr_lst_eml_don_dt timestamp,                                                                                                                                   
	fr_coop_eligible_flg varchar(1),                                                                                                                               
	fr_lst_dm_don_amt numeric(20,2),                                                                                                                               
	fr_lst_dm_don_dt timestamp,                                                                                                                                    
	fr_lst_org_onl_don_amt numeric(20,2),                                                                                                                          
	fr_lst_org_onl_don_dt timestamp,                                                                                                                               
	fr_lst_ecomm_don_amt numeric(20,2),                                                                                                                            
	fr_lst_ecomm_don_dt timestamp,                                                                                                                                 
	fr_1_6_mth_don_cnt bigint,                                                                                                                                     
	fr_7_12_mth_don_cnt bigint,                                                                                                                                    
	fr_13_24_mth_don_cnt bigint,                                                                                                                                   
	fr_25_36_mth_don_cnt bigint,                                                                                                                                   
	fr_37_plus_mth_don_cnt bigint,                                                                                                                                 
	fr_dm_pros_mdl_rsp_flg varchar(1),                                                                                                                             
	fr_mbr_comb_level varchar(1),                                                                                                                                  
	fr_mbr_comb_exp_dt timestamp,                                                                                                                                  
	fr_mbr_basic_fst_dt timestamp,                                                                                                                                 
	fr_mbr_basic_fst_amt numeric(20,2),                                                                                                                            
	fr_mbr_basic_lst_dt timestamp,                                                                                                                                 
	fr_mbr_basic_lst_amt numeric(20,2),                                                                                                                            
	fr_ch_status varchar(9),                                                                                                                                       
	fr_ch_curr_ttl_don_amt numeric(20,2),                                                                                                                          
	fr_ch_lst_don_dt timestamp,                                                                                                                                    
	fr_ch_curr_strt_dt timestamp,                                                                                                                                  
	fr_ch_lst_don_amt numeric(20,2),                                                                                                                               
	fr_ch_curr_don_cnt bigint,                                                                                                                                     
	fr_ch_lst_don_keycd varchar(30),                                                                                                                               
	fr_ch_status_active_dt timestamp,                                                                                                                              
	fr_mbr_basic_exp_dt timestamp,                                                                                                                                 
	fr_mbr_basic_lst_keycode varchar(112),                                                                                                                         
	osl_hh_id bigint                                                                                                                                               
)                                                                                                                                                               
diststyle key                                                                                                                                                   
sortkey(individual_id)                                                                                                                                          
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_individual;                                                                                                                       
 create table prod.agg_individual                                                                                                                               
(                                                                                                                                                               
	individual_id bigint distkey,                                                                                                                                  
	hh_id bigint,                                                                                                                                                  
	customer_typ varchar(8),                                                                                                                                       
	lst_don_amt numeric(20,2),                                                                                                                                     
	ltd_don_amt numeric(20,2),                                                                                                                                     
	max_don_amt numeric(20,2),                                                                                                                                     
	fst_don_dt timestamp,                                                                                                                                          
	lst_don_dt timestamp,                                                                                                                                          
	ltd_don_cnt bigint,                                                                                                                                            
	non_prod_fst_dt timestamp,                                                                                                                                     
	fst_non_prod_cd varchar(20),                                                                                                                                   
	non_prod_cnt bigint,                                                                                                                                           
	ind_combined_dt timestamp,                                                                                                                                     
	ind_ltd_amt numeric(38,2),                                                                                                                                     
	comp_flg varchar(1),                                                                                                                                           
	ind_fst_rel_dt timestamp,                                                                                                                                      
	ind_lst_act_dt timestamp,                                                                                                                                      
	ind_actv_cnt integer,                                                                                                                                          
	ind_relship_cnt integer,                                                                                                                                       
	fst_prod_cd varchar(57),                                                                                                                                       
	ind_rec_prod_only_flg varchar(1),                                                                                                                              
	emp_flg varchar(1),                                                                                                                                            
	cga_flg varchar(1),                                                                                                                                            
	news_car_watch_flg varchar(1),                                                                                                                                 
	news_best_buy_drug_flg varchar(1),                                                                                                                             
	news_safety_alert_flg varchar(1),                                                                                                                              
	news_hlth_alert_flg varchar(1),                                                                                                                                
	news_cro_whats_flg varchar(1),                                                                                                                                 
	news_green_choice_flg varchar(1),                                                                                                                              
	ind_dnr_only_flg varchar(1),                                                                                                                                   
	dc_cdb_last_ord_dt timestamp,                                                                                                                                  
	prod_ltd_pd_amt numeric(38,2),                                                                                                                                 
	prod_fst_ord_dt timestamp,                                                                                                                                     
	prod_lst_canc_bad_dbt_dt timestamp,                                                                                                                            
	non_prod_lst_act_dt timestamp,                                                                                                                                 
	prod_lst_ord_dt timestamp,                                                                                                                                     
	prod_ord_cnt bigint,                                                                                                                                           
	prod_dm_ord_cnt bigint,                                                                                                                                        
	prod_em_ord_cnt bigint,                                                                                                                                        
	prod_dnr_ord_cnt bigint,                                                                                                                                       
	prod_cnt bigint,                                                                                                                                               
	ad_apl_keycode varchar(8),                                                                                                                                     
	ind_email_summary_flag varchar(1),                                                                                                                             
	mrp_email_summary_flag varchar(1),                                                                                                                             
	news_email_summary_flag varchar(1),                                                                                                                            
	adv_ever varchar(1),                                                                                                                                           
	adv_active varchar(1),                                                                                                                                         
	verity_recency_flag varchar(1),                                                                                                                                
	discussions_forum_flag varchar(1),                                                                                                                             
	news_hlth_heart_flg varchar(1),                                                                                                                                
	news_hlth_child_teen_flg varchar(1),                                                                                                                           
	news_hlth_diabetes_flg varchar(1),                                                                                                                             
	news_hlth_women_flg varchar(1),                                                                                                                                
	news_hlth_after_60_flg varchar(1),                                                                                                                             
	age_infobase double precision,                                                                                                                                 
	gender_infobase varchar(1),                                                                                                                                    
	ind_fst_rel_cd varchar(57),                                                                                                                                    
	ipad_enabled_dt timestamp,                                                                                                                                     
	bnb_frst_visitor_dt timestamp,                                                                                                                                 
	bnb_last_visitor_dt timestamp,                                                                                                                                 
	bnb_frst_prospect_dt timestamp,                                                                                                                                
	bnb_last_prospect_dt timestamp,                                                                                                                                
	bnb_frst_sales_dt timestamp,                                                                                                                                   
	bnb_last_sales_dt timestamp,                                                                                                                                   
	bnb_tot_prospect_cnt bigint,                                                                                                                                   
	bnb_tot_sale_cnt bigint,                                                                                                                                       
	bnb_best_status varchar(1),                                                                                                                                    
	bnb_best_status_dt timestamp,                                                                                                                                  
	bnb_most_recent_status varchar(1),                                                                                                                             
	bnb_most_recent_dt timestamp,                                                                                                                                  
	bnb_nbr_new_sales bigint,                                                                                                                                      
	bnb_nbr_used_sales bigint,                                                                                                                                     
	bnb_most_recent_trans_type varchar(91),                                                                                                                        
	ol_match_recent_dt timestamp,                                                                                                                                  
	ol_match_cnt integer,                                                                                                                                          
	ol_match_cnt_6m integer,                                                                                                                                       
	ol_match_cnt_7_12m integer,                                                                                                                                    
	ol_match_cnt_13_18m integer,                                                                                                                                   
	ind_prospect_eml_mtch_ind varchar(1),                                                                                                                          
	prospect_email_match_dt timestamp,                                                                                                                             
	prospect_email_match_cnt bigint,                                                                                                                               
	can_spam_flg varchar(1),                                                                                                                                       
	print_sub_coop_flg varchar(1),                                                                                                                                 
	mobile_app_usage_ind varchar(1),                                                                                                                               
	rtg_app_days_since_fst_act double precision,                                                                                                                   
	rtg_app_days_since_lst_act double precision,                                                                                                                   
	email_fav_key bigint,                                                                                                                                          
	adv_cnt bigint                                                                                                                                                 
)                                                                                                                                                               
diststyle key                                                                                                                                                   
sortkey(individual_id)                                                                                                                                          
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_individual_device_history;                                                                                                        
 create table prod.agg_individual_device_history                                                                                                                
(                                                                                                                                                               
	individual_id bigint not null encode delta distkey,                                                                                                            
	device_name varchar(256) not null,                                                                                                                             
	device_type char not null,                                                                                                                                     
	operating_system varchar(256) not null,                                                                                                                        
	channel char not null,                                                                                                                                         
	last_used_dt timestamp,                                                                                                                                        
	times_used_cnt integer encode bytedict,                                                                                                                        
	key_individual_device_history varchar(64) not null,                                                                                                            
	constraint agg_individual_device_history_pkey                                                                                                                  
		primary key (individual_id, device_name, device_type, operating_system, channel)                                                                              
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_individual_device_summary;                                                                                                        
 create table prod.agg_individual_device_summary                                                                                                                
(                                                                                                                                                               
	individual_id bigint not null distkey                                                                                                                          
		constraint agg_individual_device_summary_pkey                                                                                                                 
			primary key,                                                                                                                                                 
	lst_mobile_phone_web varchar(256),                                                                                                                             
	lst_mobile_phone_web_dt timestamp,                                                                                                                             
	most_used_mobile_phone_web varchar(256),                                                                                                                       
	lst_mobile_phone_opsys_web varchar(256),                                                                                                                       
	most_used_mbl_phone_opsys_web varchar(256),                                                                                                                    
	lst_tablet_web varchar(256),                                                                                                                                   
	lst_tablet_web_dt timestamp,                                                                                                                                   
	most_used_tablet_web varchar(256),                                                                                                                             
	lst_tablet_opsys_web varchar(256),                                                                                                                             
	most_used_tablet_opsys_web varchar(256),                                                                                                                       
	lst_desktop_opsys_web varchar(256),                                                                                                                            
	lst_desktop_web_dt timestamp,                                                                                                                                  
	most_used_desktop_opsys_web varchar(256),                                                                                                                      
	lst_other_device_web varchar(256),                                                                                                                             
	lst_other_device_web_dt timestamp,                                                                                                                             
	most_used_other_device_web varchar(256),                                                                                                                       
	lst_other_device_opsys_web varchar(256),                                                                                                                       
	most_used_other_dev_opsys_web varchar(256),                                                                                                                    
	lst_mobile_phone_eml varchar(256),                                                                                                                             
	lst_mobile_phone_eml_dt timestamp,                                                                                                                             
	most_used_mobile_phone_eml varchar(256),                                                                                                                       
	lst_mobile_phone_opsys_eml varchar(256),                                                                                                                       
	most_used_mbl_phone_opsys_eml varchar(256),                                                                                                                    
	lst_tablet_eml varchar(256),                                                                                                                                   
	lst_tablet_eml_dt timestamp,                                                                                                                                   
	most_used_tablet_eml varchar(256),                                                                                                                             
	lst_tablet_opsys_eml varchar(256),                                                                                                                             
	most_used_tablet_opsys_eml varchar(256),                                                                                                                       
	lst_desktop_opsys_eml varchar(256),                                                                                                                            
	lst_desktop_eml_dt timestamp,                                                                                                                                  
	most_used_desktop_opsys_eml varchar(256),                                                                                                                      
	lst_other_device_eml varchar(256),                                                                                                                             
	lst_other_device_eml_dt timestamp,                                                                                                                             
	most_used_other_device_eml varchar(256),                                                                                                                       
	lst_other_device_opsys_eml varchar(256),                                                                                                                       
	most_used_other_dev_opsys_eml varchar(256)                                                                                                                     
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_individual_emails;                                                                                                                
 create table prod.agg_individual_emails                                                                                                                        
(                                                                                                                                                               
	individual_id bigint not null encode delta distkey,                                                                                                            
	email_address_a varchar(256),                                                                                                                                  
	email_address_i varchar(256),                                                                                                                                  
	email_address_m varchar(256),                                                                                                                                  
	email_address_n varchar(256),                                                                                                                                  
	email_hash_a varchar(64),                                                                                                                                      
	email_hash_i varchar(64),                                                                                                                                      
	email_hash_m varchar(64),                                                                                                                                      
	email_hash_n varchar(64)                                                                                                                                       
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_infobase_profile;                                                                                                                 
 create table prod.agg_infobase_profile                                                                                                                         
(                                                                                                                                                               
	individual_id bigint not null encode delta                                                                                                                     
		constraint agg_infobase_profile_pkey                                                                                                                          
			primary key,                                                                                                                                                 
	last_append_dt timestamp,                                                                                                                                      
	ib2076_community_involvement varchar(13),                                                                                                                      
	ib2200_health_allergy_related char,                                                                                                                            
	ib2201_health_arthritis_moblty char,                                                                                                                           
	ib2202_health_cholesterol_focs char,                                                                                                                           
	ib2203_health_diabetic char,                                                                                                                                   
	ib2204_health_disabled char,                                                                                                                                   
	ib2205_health_homeopathic char,                                                                                                                                
	ib2206_health_organic_focus char,                                                                                                                              
	ib2207_health_orthopedic char,                                                                                                                                 
	ib2208_health_senior_needs char,                                                                                                                               
	ib7726_community_charities char,                                                                                                                               
	ib7737_reading_magazines char,                                                                                                                                 
	ib7739_cooking_general char,                                                                                                                                   
	ib7741_cooking_low_fat char,                                                                                                                                   
	ib7743_foods_natural char,                                                                                                                                     
	ib7745_travel_international char,                                                                                                                              
	ib7751_exercise_walking char,                                                                                                                                  
	ib7770_health_medical char,                                                                                                                                    
	ib7771_dieting_weight_loss char,                                                                                                                               
	ib7779_childrens_interests char,                                                                                                                               
	ib7780_grandchildren char,                                                                                                                                     
	ib7795_investments_stocks_bond char,                                                                                                                           
	ib7797_pc_internt_onl_srvc_usr char,                                                                                                                           
	ib7800_wireless_cell_phon_ownr char,                                                                                                                           
	ib7809_environmental_issues char,                                                                                                                              
	ib7810_tennis char,                                                                                                                                            
	ib7811_golf char,                                                                                                                                              
	ib7816_home_improvement char,                                                                                                                                  
	ib7817_gardening char,                                                                                                                                         
	ib7819_gaming_lottery char,                                                                                                                                    
	ib7820_gaming_casino char,                                                                                                                                     
	ib7821_sweepstakes_contests char,                                                                                                                              
	ib7823_outdoors_grouping char,                                                                                                                                 
	ib7824_travel_grouping char,                                                                                                                                   
	ib7826_cooking_food_grouping char,                                                                                                                             
	ib7828_movie_music_grouping char,                                                                                                                              
	ib7831_investing_finance_group char,                                                                                                                           
	ib7848_read_finncl_nwslttr_sbs char,                                                                                                                           
	ib8463_home_market_value_decls varchar(2),                                                                                                                     
	ib8605_occupation_2nd_ind char,                                                                                                                                
	ib8607_length_of_residence varchar(2),                                                                                                                         
	ib8608_dwelling_type char,                                                                                                                                     
	ib8609_marital_status char,                                                                                                                                    
	ib8611_dob_1st_ind varchar(8),                                                                                                                                 
	ib8616_ag_2yr_incrmnts_1st_ind varchar(2),                                                                                                                     
	ib8617_ag_2yr_incrmnts_2nd_ind varchar(2),                                                                                                                     
	ib8622_presence_of_children char,                                                                                                                              
	ib8624_dob_ip_ind_dflt_1st_ind varchar(9),                                                                                                                     
	ib8627_ag_2y_incr_in_id_dflt_1 varchar(3),                                                                                                                     
	ib8628_number_of_adults char,                                                                                                                                  
	ib8629_household_size char,                                                                                                                                    
	ib8637_occupation_input_ind char,                                                                                                                              
	ib8639_ib_positive_mtch_indctr char,                                                                                                                           
	ib8640_number_of_sources varchar(2),                                                                                                                           
	ib8642_home_market_value char,                                                                                                                                 
	ib8655_lifestages_code varchar(3),                                                                                                                             
	ib8666_pc_operating_system char,                                                                                                                               
	ib8671_income_est_hh_narrw_rng char,                                                                                                                           
	ib8688_gender_input_ind char,                                                                                                                                  
	ib8717_teletrends_internet_usr varchar(2),                                                                                                                     
	ib8718_teletrends_cellular_usr varchar(2),                                                                                                                     
	ib8727_teletrends_intl_ld_user varchar(2),                                                                                                                     
	ib9100_overall_match_indicator char,                                                                                                                           
	ib1270_personicx_cluster_code varchar(3),                                                                                                                      
	ib1271_personicx_life_stage_cd varchar(4),                                                                                                                     
	ib9356_networth_gold char,                                                                                                                                     
	ib9514_education_input_ind char,                                                                                                                               
	ib9557_suppression_mail_dma char,                                                                                                                              
	ib9780_email_append_avail_ind char,                                                                                                                            
	ib8167_mail_order_buyer_cats varchar(31),                                                                                                                      
	ib8621_credit_card_indicator varchar(6),                                                                                                                       
	ib9150_credit_card_freq_purch varchar(7),                                                                                                                      
	ib9154_retail_prch_mst_frq_cat varchar(2),                                                                                                                     
	ib8556_trust_owned char,                                                                                                                                       
	ib8315_power_boating char,                                                                                                                                     
	ib8278_highbrow char,                                                                                                                                          
	pd1280_personicx_dg_cluster_cd varchar(3),                                                                                                                     
	pd1281_personicx_dg_groups varchar(4),                                                                                                                         
	ib8601_child_age_range_hh varchar(15),                                                                                                                         
	ib8602_num_children_hh varchar(1),                                                                                                                             
	ib8619_working_woman varchar(1),                                                                                                                               
	ib7719_wireless_product_buyer varchar(1),                                                                                                                      
	ap0594_pay_bills_onln_gflc1025 varchar(3),                                                                                                                     
	ap1487_read_sundy_nws_mflgc609 varchar(2),                                                                                                                     
	ap5532_maild_official_ap005532 varchar(2),                                                                                                                     
	ap5733_museums_ap005733 varchar(2),                                                                                                                            
	ib3101_race char,                                                                                                                                              
	ib3103_hispanic_language_pref char,                                                                                                                            
	ib3592_med_chnl_use_yllw_pgs char(2),                                                                                                                          
	ib3593_med_chnl_use_radio char(2),                                                                                                                             
	ib3594_med_chnl_use_magazine char(2),                                                                                                                          
	ib3595_med_chnl_use_newspaper char(2),                                                                                                                         
	ib6154_automotive char,                                                                                                                                        
	ib6197_books char,                                                                                                                                             
	ib6312_donation_contribution char,                                                                                                                             
	ib6330_elec_comp_tv_vid_mov_c char,                                                                                                                            
	ib6331_elec_comp_home_off_sc char,                                                                                                                             
	ib6450_health_nutra_vitamins char,                                                                                                                             
	ib6556_magazines char,                                                                                                                                         
	ib6742_photo_video_equip_c char,                                                                                                                               
	ib7724_current_affairs_politic char,                                                                                                                           
	ib7756_auto_work char,                                                                                                                                         
	ib7772_self_improvement char,                                                                                                                                  
	ib8279_high_tech_living char,                                                                                                                                  
	ib8337_highly_likely_investors char,                                                                                                                           
	ib8479_networth char,                                                                                                                                          
	ib8600_age_present_in_hh char(21),                                                                                                                             
	ib8604_occupation_1st_ind char,                                                                                                                                
	ib8606_homeowner char,                                                                                                                                         
	ib8620_mail_order_responder char,                                                                                                                              
	ib9358_heavytransactors char(2)                                                                                                                                
)                                                                                                                                                               
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_list_coop_hist;                                                                                                                   
 create table prod.agg_list_coop_hist                                                                                                                           
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	coop_list_output_dt timestamp encode bytedict,                                                                                                                 
	coop_keycode varchar(30) encode bytedict,                                                                                                                      
	queue_id integer not null encode bytedict,                                                                                                                     
	key_list_coop_hist varchar(64) not null,                                                                                                                       
	constraint agg_list_coop_pkey                                                                                                                                  
		primary key (individual_id, queue_id)                                                                                                                         
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id)                                                                                                                              
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_model_score;                                                                                                                      
 create table prod.agg_model_score                                                                                                                              
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	scr_dt timestamp not null,                                                                                                                                     
	mdl_name varchar(50) not null,                                                                                                                                 
	ver_name varchar(5),                                                                                                                                           
	mdl_scr numeric(18,9),                                                                                                                                         
	dcl_scr numeric(3),                                                                                                                                            
	key_model_score varchar(64) not null,                                                                                                                          
	constraint agg_model_score_pkey                                                                                                                                
		primary key (individual_id, scr_dt, mdl_name)                                                                                                                 
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_name_address;                                                                                                                     
 create table prod.agg_name_address                                                                                                                             
(                                                                                                                                                               
	individual_id bigint not null distkey                                                                                                                          
		constraint agg_name_address_pkey1                                                                                                                             
			primary key,                                                                                                                                                 
	hh_id bigint,                                                                                                                                                  
	osl_hh_id varchar(100),                                                                                                                                        
	account_num varchar(10),                                                                                                                                       
	pwi_account_num varchar(64),                                                                                                                                   
	address_id bigint,                                                                                                                                             
	salutation varchar(30),                                                                                                                                        
	first_name varchar(30),                                                                                                                                        
	middle_name varchar(30),                                                                                                                                       
	last_name varchar(30),                                                                                                                                         
	suffix varchar(20),                                                                                                                                            
	business_name varchar(100),                                                                                                                                    
	primary_number varchar(10),                                                                                                                                    
	pre_directional varchar(10),                                                                                                                                   
	street varchar(40),                                                                                                                                            
	street_suffix varchar(20),                                                                                                                                     
	post_directional varchar(10),                                                                                                                                  
	unit_designator varchar(25),                                                                                                                                   
	secondary_number varchar(50),                                                                                                                                  
	additional_address_data varchar(100),                                                                                                                          
	city varchar(70),                                                                                                                                              
	state_province varchar(70),                                                                                                                                    
	postal_code varchar(15),                                                                                                                                       
	scf_fsa varchar(3),                                                                                                                                            
	country_id varchar(20),                                                                                                                                        
	can_flg char,                                                                                                                                                  
	usa_flg char,                                                                                                                                                  
	foreign_flg char,                                                                                                                                              
	usa_deliverable_flg char,                                                                                                                                      
	for_deliverable_flg char,                                                                                                                                      
	dpbc varchar(3),                                                                                                                                               
	crc varchar(4),                                                                                                                                                
	lot varchar(5),                                                                                                                                                
	dsf_delivery_type char,                                                                                                                                        
	dsf_deliverability_code char(2),                                                                                                                               
	gcdi_deliverability_code char(2),                                                                                                                              
	dsf_match_flag char,                                                                                                                                           
	dsf_vacancy_indicator char,                                                                                                                                    
	dsf_seasonal_indicator char,                                                                                                                                   
	dsf_drop_point_indicator char,                                                                                                                                 
	dsf_record_type varchar(2),                                                                                                                                    
	lacs_indicator char,                                                                                                                                           
	edit_fail_flg char,                                                                                                                                            
	coa_source varchar(16),                                                                                                                                        
	coa_date timestamp,                                                                                                                                            
	ncoa_move_type char,                                                                                                                                           
	ncoa_category char,                                                                                                                                            
	ncoa_footnote varchar(2),                                                                                                                                      
	ncoa_move_eff_dt timestamp,                                                                                                                                    
	address_type char,                                                                                                                                             
	tel_valid_flag char,                                                                                                                                           
	telecom_nbr varchar(30),                                                                                                                                       
	tel_area_code varchar(3),                                                                                                                                      
	tel_prefix varchar(3),                                                                                                                                         
	mobile_telecom_nbr varchar(30),                                                                                                                                
	coa_cnt integer                                                                                                                                                
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, address_id, hh_id, account_num, pwi_account_num)                                                                             
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_preference;                                                                                                                       
 create table prod.agg_preference                                                                                                                               
(                                                                                                                                                               
	individual_id bigint distkey,                                                                                                                                  
	assoc_typ_cd char,                                                                                                                                             
	auth_cd varchar(15),                                                                                                                                           
	scp_cd varchar(20),                                                                                                                                            
	data_source varchar(60),                                                                                                                                       
	auth_flg varchar(1),                                                                                                                                           
	fst_dt timestamp,                                                                                                                                              
	eff_dt timestamp                                                                                                                                               
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id)                                                                                                                              
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_preference_summary;                                                                                                               
 create table prod.agg_preference_summary                                                                                                                       
(                                                                                                                                                               
	individual_id bigint distkey,                                                                                                                                  
	hh_id bigint,                                                                                                                                                  
	dcd_flg varchar(1),                                                                                                                                            
	fr_mail_freq_cd varchar(2),                                                                                                                                    
	prison_flg varchar(1),                                                                                                                                         
	fr_non_ack_flg varchar(1),                                                                                                                                     
	non_prom_auto_rnw_flg varchar(1),                                                                                                                              
	non_prom_auto_rnw_gft_flg varchar(1),                                                                                                                          
	abq_non_prom_dm_flg varchar(1),                                                                                                                                
	abq_non_prom_em_flg varchar(1),                                                                                                                                
	adv_non_prom_em_flg varchar(1),                                                                                                                                
	aps_non_prom_em_flg varchar(1),                                                                                                                                
	cr_non_prom_em_flg varchar(1),                                                                                                                                 
	crmg_non_prom_em_flg varchar(1),                                                                                                                               
	cro_non_prom_em_flg varchar(1),                                                                                                                                
	crsch_non_prom_em_flg varchar(1),                                                                                                                              
	fr_non_prom_em_flg varchar(1),                                                                                                                                 
	non_prom_em_flg varchar(1),                                                                                                                                    
	hl_non_prom_em_flg varchar(1),                                                                                                                                 
	ma_non_prom_em_flg varchar(1),                                                                                                                                 
	ncbk_non_prom_em_flg varchar(1),                                                                                                                               
	ucbk_non_prom_em_flg varchar(1),                                                                                                                               
	non_prom_list_rent_flg varchar(1),                                                                                                                             
	non_prom_pander_flg varchar(1),                                                                                                                                
	non_prom_rfl_flg varchar(1),                                                                                                                                   
	non_prom_tm_flg varchar(1),                                                                                                                                    
	fr_non_prom_dm_flg varchar(1),                                                                                                                                 
	fr_non_prom_tm_flg varchar(1),                                                                                                                                 
	fr_non_prom_rnw_flg varchar(1),                                                                                                                                
	fr_non_prom_tof_flg varchar(1),                                                                                                                                
	fr_non_prom_tm_rem_flg varchar(1),                                                                                                                             
	fr_non_prom_cu_flg varchar(1),                                                                                                                                 
	fr_non_prom_prem_flg varchar(1),                                                                                                                               
	fr_rnw_mail_freq_cd varchar(1),                                                                                                                                
	fr_tm_freq_cd varchar(1),                                                                                                                                      
	fr_cu_ins_flg varchar(1),                                                                                                                                      
	cu_board_flg varchar(1),                                                                                                                                       
	dma_non_prom_dm_flg varchar(1),                                                                                                                                
	dma_non_prom_tm_flg varchar(1),                                                                                                                                
	dma_non_prom_em_flg varchar(1),                                                                                                                                
	acx_non_prom_dm_flg varchar(1),                                                                                                                                
	acx_non_prom_tm_flg varchar(1),                                                                                                                                
	acx_non_prom_em_flg varchar(1),                                                                                                                                
	bk_non_prom_flg varchar(1),                                                                                                                                    
	non_prom_gft_flg varchar(1),                                                                                                                                   
	shm_non_prom_em_flg varchar(1),                                                                                                                                
	carp_non_prom_em_flg varchar(1),                                                                                                                               
	non_prom_cu_fr_rfl_flg varchar(1),                                                                                                                             
	ib_deceased_flg varchar(1),                                                                                                                                    
	test_user_flg varchar(1),                                                                                                                                      
	fr_non_share_flg varchar(1),                                                                                                                                   
	osl_hh_id bigint,                                                                                                                                              
	adv_non_prom_news_em_flg varchar(1),                                                                                                                           
	adv_non_prom_aa_em_flg varchar(1),                                                                                                                             
	adv_non_prom_dm_flg varchar(1),                                                                                                                                
	advfr_non_prom_em_flg varchar(1)                                                                                                                               
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_print_summary;                                                                                                                    
 create table prod.agg_print_summary                                                                                                                            
(                                                                                                                                                               
	individual_id bigint distkey,                                                                                                                                  
	hh_id bigint,                                                                                                                                                  
	cr_actv_flg varchar(1),                                                                                                                                        
	hl_actv_flg varchar(1),                                                                                                                                        
	ma_actv_flg varchar(1),                                                                                                                                        
	cr_actv_rptg_flg varchar(1),                                                                                                                                   
	hl_actv_rptg_flg varchar(1),                                                                                                                                   
	ma_actv_rptg_flg varchar(1),                                                                                                                                   
	cr_actv_pd_flg varchar(1),                                                                                                                                     
	hl_actv_pd_flg varchar(1),                                                                                                                                     
	ma_actv_pd_flg varchar(1),                                                                                                                                     
	cr_canc_bad_dbt_flg varchar(1),                                                                                                                                
	hl_canc_bad_dbt_flg varchar(1),                                                                                                                                
	ma_canc_bad_dbt_flg varchar(1),                                                                                                                                
	cr_canc_cust_flg varchar(1),                                                                                                                                   
	hl_canc_cust_flg varchar(1),                                                                                                                                   
	ma_canc_cust_flg varchar(1),                                                                                                                                   
	cr_crd_pend_flg varchar(1),                                                                                                                                    
	hl_crd_pend_flg varchar(1),                                                                                                                                    
	ma_crd_pend_flg varchar(1),                                                                                                                                    
	cr_crd_stat_cd varchar(1),                                                                                                                                     
	hl_crd_stat_cd varchar(1),                                                                                                                                     
	ma_crd_stat_cd varchar(1),                                                                                                                                     
	ofo_crd_stat_cd varchar(1),                                                                                                                                    
	cr_exp_dt timestamp,                                                                                                                                           
	hl_exp_dt timestamp,                                                                                                                                           
	ma_exp_dt timestamp,                                                                                                                                           
	cr_lst_canc_dt timestamp,                                                                                                                                      
	hl_lst_canc_dt timestamp,                                                                                                                                      
	ma_lst_canc_dt timestamp,                                                                                                                                      
	tl_lst_ord_dt timestamp,                                                                                                                                       
	cr_lst_pmt_dt timestamp,                                                                                                                                       
	hl_lst_pmt_dt timestamp,                                                                                                                                       
	ma_lst_pmt_dt timestamp,                                                                                                                                       
	cr_flg varchar(1),                                                                                                                                             
	hl_flg varchar(1),                                                                                                                                             
	ma_flg varchar(1),                                                                                                                                             
	cr_exp_flg varchar(1),                                                                                                                                         
	hl_exp_flg varchar(1),                                                                                                                                         
	ma_exp_flg varchar(1),                                                                                                                                         
	cr_curr_mbr_keycode varchar(46),                                                                                                                               
	hl_curr_mbr_keycode varchar(46),                                                                                                                               
	ma_curr_mbr_keycode varchar(46),                                                                                                                               
	cr_curr_ord_keycode varchar(46),                                                                                                                               
	hl_curr_ord_keycode varchar(46),                                                                                                                               
	ma_curr_ord_keycode varchar(46),                                                                                                                               
	cr_fst_mbr_keycode varchar(46),                                                                                                                                
	hl_fst_mbr_keycode varchar(46),                                                                                                                                
	ma_fst_mbr_keycode varchar(46),                                                                                                                                
	cr_lst_ord_keycode varchar(46),                                                                                                                                
	hl_lst_ord_keycode varchar(46),                                                                                                                                
	ma_lst_ord_keycode varchar(46),                                                                                                                                
	cr_lt_sub_flg varchar(1),                                                                                                                                      
	hl_lt_sub_flg varchar(1),                                                                                                                                      
	ma_lt_sub_flg varchar(1),                                                                                                                                      
	cr_non_sub_dnr_flg varchar(1),                                                                                                                                 
	hl_non_sub_dnr_flg varchar(1),                                                                                                                                 
	ma_non_sub_dnr_flg varchar(1),                                                                                                                                 
	cr_brks_cnt bigint,                                                                                                                                            
	hl_brks_cnt bigint,                                                                                                                                            
	ma_brks_cnt bigint,                                                                                                                                            
	cr_rnw_cnt varchar(41),                                                                                                                                        
	hl_rnw_cnt varchar(41),                                                                                                                                        
	ma_rnw_cnt varchar(41),                                                                                                                                        
	cr_rec_flg varchar(1),                                                                                                                                         
	hl_rec_flg varchar(1),                                                                                                                                         
	ma_rec_flg varchar(1),                                                                                                                                         
	cr_svc_stat_cd varchar(17),                                                                                                                                    
	hl_svc_stat_cd varchar(17),                                                                                                                                    
	ma_svc_stat_cd varchar(17),                                                                                                                                    
	cr_curr_mbr_src_cd varchar(20),                                                                                                                                
	hl_curr_mbr_src_cd varchar(20),                                                                                                                                
	ma_curr_mbr_src_cd varchar(20),                                                                                                                                
	cr_curr_ord_src_cd varchar(20),                                                                                                                                
	hl_curr_ord_src_cd varchar(20),                                                                                                                                
	ma_curr_ord_src_cd varchar(20),                                                                                                                                
	cr_fst_ord_src_cd varchar(20),                                                                                                                                 
	hl_fst_ord_src_cd varchar(20),                                                                                                                                 
	ma_fst_ord_src_cd varchar(20),                                                                                                                                 
	cr_lst_ord_src_cd varchar(20),                                                                                                                                 
	hl_lst_ord_src_cd varchar(20),                                                                                                                                 
	ma_lst_ord_src_cd varchar(20),                                                                                                                                 
	cr_prior_mbr_src_cd varchar(20),                                                                                                                               
	hl_prior_mbr_src_cd varchar(20),                                                                                                                               
	ma_prior_mbr_src_cd varchar(20),                                                                                                                               
	cr_sub_dnr_flg varchar(1),                                                                                                                                     
	hl_sub_dnr_flg varchar(1),                                                                                                                                     
	ma_sub_dnr_flg varchar(1),                                                                                                                                     
	cr_curr_ord_term varchar(37),                                                                                                                                  
	hl_curr_ord_term varchar(37),                                                                                                                                  
	ma_curr_ord_term varchar(37),                                                                                                                                  
	cr_fst_dnr_dt timestamp,                                                                                                                                       
	hl_fst_dnr_dt timestamp,                                                                                                                                       
	ma_fst_dnr_dt timestamp,                                                                                                                                       
	eds_lst_src_cd varchar(41),                                                                                                                                    
	morbank_mtch_cd varchar(31),                                                                                                                                   
	cr_auto_rnw_flg varchar(1),                                                                                                                                    
	hl_auto_rnw_flg varchar(1),                                                                                                                                    
	ma_auto_rnw_flg varchar(1),                                                                                                                                    
	cr_non_dnr_flg varchar(1),                                                                                                                                     
	hl_non_dnr_flg varchar(1),                                                                                                                                     
	ma_non_dnr_flg varchar(1),                                                                                                                                     
	cr_curr_mbr_dt timestamp,                                                                                                                                      
	hl_curr_mbr_dt timestamp,                                                                                                                                      
	ma_curr_mbr_dt timestamp,                                                                                                                                      
	cr_ltd_pd_amt numeric(38,2),                                                                                                                                   
	hl_ltd_pd_amt numeric(38,2),                                                                                                                                   
	ma_ltd_pd_amt numeric(38,2),                                                                                                                                   
	cr_curr_ord_dt timestamp,                                                                                                                                      
	hl_curr_ord_dt timestamp,                                                                                                                                      
	ma_curr_ord_dt timestamp,                                                                                                                                      
	cr_fst_ord_dt timestamp,                                                                                                                                       
	hl_fst_ord_dt timestamp,                                                                                                                                       
	ma_fst_ord_dt timestamp,                                                                                                                                       
	cr_lst_canc_bad_dbt_dt timestamp,                                                                                                                              
	hl_lst_canc_bad_dbt_dt timestamp,                                                                                                                              
	ma_lst_canc_bad_dbt_dt timestamp,                                                                                                                              
	cr_lst_dnr_ord_dt timestamp,                                                                                                                                   
	hl_lst_dnr_ord_dt timestamp,                                                                                                                                   
	ma_lst_dnr_ord_dt timestamp,                                                                                                                                   
	cr_lst_ord_dt timestamp,                                                                                                                                       
	hl_lst_ord_dt timestamp,                                                                                                                                       
	ma_lst_ord_dt timestamp,                                                                                                                                       
	cr_canc_bad_dbt_cnt bigint,                                                                                                                                    
	hl_canc_bad_dbt_cnt bigint,                                                                                                                                    
	ma_canc_bad_dbt_cnt bigint,                                                                                                                                    
	cr_canc_cust_cnt bigint,                                                                                                                                       
	hl_canc_cust_cnt bigint,                                                                                                                                       
	ma_canc_cust_cnt bigint,                                                                                                                                       
	cr_dm_ord_cnt bigint,                                                                                                                                          
	hl_dm_ord_cnt bigint,                                                                                                                                          
	ma_dm_ord_cnt bigint,                                                                                                                                          
	cr_em_ord_cnt bigint,                                                                                                                                          
	hl_em_ord_cnt bigint,                                                                                                                                          
	ma_em_ord_cnt bigint,                                                                                                                                          
	cr_dnr_ord_cnt bigint,                                                                                                                                         
	hl_dnr_ord_cnt bigint,                                                                                                                                         
	ma_dnr_ord_cnt bigint,                                                                                                                                         
	cr_ord_cnt bigint,                                                                                                                                             
	hl_ord_cnt bigint,                                                                                                                                             
	ma_ord_cnt bigint,                                                                                                                                             
	cr_wr_off_cnt bigint,                                                                                                                                          
	hl_wr_off_cnt bigint,                                                                                                                                          
	ma_wr_off_cnt bigint,                                                                                                                                          
	cr_lst_sub_ord_role_cd varchar(1),                                                                                                                             
	hl_lst_sub_ord_role_cd varchar(1),                                                                                                                             
	ma_lst_sub_ord_role_cd varchar(1),                                                                                                                             
	shm_actv_flg varchar(1),                                                                                                                                       
	shm_actv_rptg_flg varchar(1),                                                                                                                                  
	shm_actv_pd_flg varchar(1),                                                                                                                                    
	shm_canc_bad_dbt_flg varchar(1),                                                                                                                               
	shm_canc_cust_flg varchar(1),                                                                                                                                  
	shm_crd_pend_flg varchar(1),                                                                                                                                   
	shm_crd_stat_cd varchar(1),                                                                                                                                    
	shm_exp_dt timestamp,                                                                                                                                          
	shm_lst_canc_dt timestamp,                                                                                                                                     
	shm_lst_pmt_dt timestamp,                                                                                                                                      
	shm_flg varchar(1),                                                                                                                                            
	shm_exp_flg varchar(1),                                                                                                                                        
	shm_curr_mbr_keycode varchar(46),                                                                                                                              
	shm_curr_ord_keycode varchar(46),                                                                                                                              
	shm_fst_mbr_keycode varchar(46),                                                                                                                               
	shm_lst_ord_keycode varchar(46),                                                                                                                               
	shm_lt_sub_flg varchar(1),                                                                                                                                     
	shm_non_sub_dnr_flg varchar(1),                                                                                                                                
	shm_brks_cnt bigint,                                                                                                                                           
	shm_rnw_cnt varchar(41),                                                                                                                                       
	shm_rec_flg varchar(1),                                                                                                                                        
	shm_svc_stat_cd varchar(17),                                                                                                                                   
	shm_curr_mbr_src_cd varchar(20),                                                                                                                               
	shm_curr_ord_src_cd varchar(20),                                                                                                                               
	shm_fst_ord_src_cd varchar(20),                                                                                                                                
	shm_lst_ord_src_cd varchar(20),                                                                                                                                
	shm_prior_mbr_src_cd varchar(20),                                                                                                                              
	shm_sub_dnr_flg varchar(1),                                                                                                                                    
	shm_curr_ord_term varchar(37),                                                                                                                                 
	shm_fst_dnr_dt timestamp,                                                                                                                                      
	shm_auto_rnw_flg varchar(1),                                                                                                                                   
	shm_non_dnr_flg varchar(1),                                                                                                                                    
	shm_curr_mbr_dt timestamp,                                                                                                                                     
	shm_ltd_pd_amt numeric(38,2),                                                                                                                                  
	shm_curr_ord_dt timestamp,                                                                                                                                     
	shm_fst_ord_dt timestamp,                                                                                                                                      
	shm_lst_canc_bad_dbt_dt timestamp,                                                                                                                             
	shm_lst_dnr_ord_dt timestamp,                                                                                                                                  
	shm_lst_ord_dt timestamp,                                                                                                                                      
	shm_canc_bad_dbt_cnt bigint,                                                                                                                                   
	shm_canc_cust_cnt bigint,                                                                                                                                      
	shm_dm_ord_cnt bigint,                                                                                                                                         
	shm_em_ord_cnt bigint,                                                                                                                                         
	shm_dnr_ord_cnt bigint,                                                                                                                                        
	shm_ord_cnt bigint,                                                                                                                                            
	shm_wr_off_cnt bigint,                                                                                                                                         
	shm_lst_sub_ord_role_cd varchar(1),                                                                                                                            
	osl_hh_id bigint                                                                                                                                               
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id)                                                                                                                              
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_promotion;                                                                                                                        
 create table prod.agg_promotion                                                                                                                                
(                                                                                                                                                               
	individual_id bigint not null encode delta32k distkey,                                                                                                         
	osl_hh_id varchar(100),                                                                                                                                        
	keycode varchar(30) not null,                                                                                                                                  
	bus_unt varchar(30) not null encode bytedict,                                                                                                                  
	prom_dt timestamp not null,                                                                                                                                    
	chnl_cd varchar(1) encode bytedict,                                                                                                                            
	multi_chnl_ind char,                                                                                                                                           
	lst_don_dt timestamp,                                                                                                                                          
	lst_don_amt numeric(12,2),                                                                                                                                     
	tot_don_amt numeric(12,2),                                                                                                                                     
	contact_type char,                                                                                                                                             
	campaign_name varchar(250),                                                                                                                                    
	ol_campaign_id varchar(10),                                                                                                                                    
	list_select_cd varchar(19),                                                                                                                                    
	output_lsc varchar(19),                                                                                                                                        
	multibuyer_cnt integer,                                                                                                                                        
	queue_id numeric(6),                                                                                                                                           
	key_promotion varchar(64) not null,                                                                                                                            
	constraint agg_promotion_pkey_full                                                                                                                             
		primary key (individual_id, keycode, bus_unt, prom_dt)                                                                                                        
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(individual_id, prom_dt)                                                                                                                     
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_promotion_summary;                                                                                                                
 create table prod.agg_promotion_summary                                                                                                                        
(                                                                                                                                                               
	individual_id bigint distkey,                                                                                                                                  
	hh_id bigint,                                                                                                                                                  
	cr_lst_prom_dt timestamp,                                                                                                                                      
	crsch_em_lst_prom_dt timestamp,                                                                                                                                
	hl_lst_prom_dt timestamp,                                                                                                                                      
	fr_tm_lst_prom_dt timestamp,                                                                                                                                   
	abq_lst_keycode varchar(46),                                                                                                                                   
	adv_em_lst_prom_dt timestamp,                                                                                                                                  
	adv_em_promo_cnt bigint,                                                                                                                                       
	adv_sla_em_promo_cnt bigint,                                                                                                                                   
	prod_lst_prom_dt timestamp,                                                                                                                                    
	non_prod_lst_prom_dt timestamp,                                                                                                                                
	cro_lst_prom_dt timestamp,                                                                                                                                     
	prod_dm_lst_prom_dt timestamp,                                                                                                                                 
	cr_dm_lst_prom_dt timestamp,                                                                                                                                   
	cro_dm_lst_prom_dt timestamp,                                                                                                                                  
	fr_dm_lst_prom_dt timestamp,                                                                                                                                   
	hl_dm_lst_prom_dt timestamp,                                                                                                                                   
	ind_dm_lst_prom_dt timestamp,                                                                                                                                  
	prod_em_lst_prom_dt timestamp,                                                                                                                                 
	cr_em_lst_prom_dt timestamp,                                                                                                                                   
	cro_em_lst_prom_dt timestamp,                                                                                                                                  
	hl_em_lst_prom_dt timestamp,                                                                                                                                   
	ind_em_lst_prom_dt timestamp,                                                                                                                                  
	fr_lst_prom_dt timestamp,                                                                                                                                      
	ind_lst_prom_dt timestamp,                                                                                                                                     
	prod_prom_cnt bigint,                                                                                                                                          
	non_prod_prom_cnt bigint,                                                                                                                                      
	cr_prom_cnt bigint,                                                                                                                                            
	cro_prom_cnt bigint,                                                                                                                                           
	prod_dm_prom_cnt bigint,                                                                                                                                       
	cr_dm_prom_cnt bigint,                                                                                                                                         
	cro_dm_prom_cnt bigint,                                                                                                                                        
	fr_dm_prom_cnt bigint,                                                                                                                                         
	hl_dm_prom_cnt bigint,                                                                                                                                         
	prod_em_prom_cnt bigint,                                                                                                                                       
	cr_em_prom_cnt bigint,                                                                                                                                         
	cro_em_prom_cnt bigint,                                                                                                                                        
	crsch_em_prom_cnt bigint,                                                                                                                                      
	fr_prom_cnt bigint,                                                                                                                                            
	cr_dnr_prom_cnt bigint,                                                                                                                                        
	cro_dnr_prom_cnt bigint,                                                                                                                                       
	hl_prom_cnt bigint,                                                                                                                                            
	ind_prom_cnt bigint,                                                                                                                                           
	non_prod_sla_prom_cnt bigint,                                                                                                                                  
	fr_sla_dm_prom_cnt bigint,                                                                                                                                     
	fr_sla_prom_cnt bigint,                                                                                                                                        
	fr_sla_tm_prom_cnt bigint,                                                                                                                                     
	prod_slo_prom_cnt bigint,                                                                                                                                      
	cr_slo_prom_cnt bigint,                                                                                                                                        
	cro_slo_prom_cnt bigint,                                                                                                                                       
	cr_slo_dm_prom_cnt bigint,                                                                                                                                     
	cro_slo_dm_prom_cnt bigint,                                                                                                                                    
	cro_slo_em_prom_cnt bigint,                                                                                                                                    
	hl_slo_prom_cnt bigint,                                                                                                                                        
	offline_slo_prom_cnt bigint,                                                                                                                                   
	online_slo_prom_cnt bigint,                                                                                                                                    
	fr_tm_prom_cnt bigint,                                                                                                                                         
	osl_hh_id bigint                                                                                                                                               
)                                                                                                                                                               
diststyle key                                                                                                                                                   
sortkey(individual_id)                                                                                                                                          
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_prospect_email;                                                                                                                   
 create table prod.agg_prospect_email                                                                                                                           
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	orig_source varchar(100) not null,                                                                                                                             
	email_address varchar(256) not null,                                                                                                                           
	email_type_cd char,                                                                                                                                            
	data_source varchar(16),                                                                                                                                       
	valid_flag char,                                                                                                                                               
	eff_dt timestamp,                                                                                                                                              
	end_dt timestamp,                                                                                                                                              
	src_valid_flag char,                                                                                                                                           
	src_delv_ind char,                                                                                                                                             
	email_client_app varchar(256),                                                                                                                                 
	fst_dt timestamp,                                                                                                                                              
	key_prospect_email varchar(64) not null,                                                                                                                       
	constraint agg_prospect_email_pkey                                                                                                                             
		primary key (individual_id, email_address, orig_source)                                                                                                       
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_raw_address;                                                                                                                      
 create table prod.agg_raw_address                                                                                                                              
(                                                                                                                                                               
	individual_id bigint not null distkey                                                                                                                          
		constraint agg_raw_address2_pkey                                                                                                                              
			primary key,                                                                                                                                                 
	account_num varchar(10),                                                                                                                                       
	nameprefix varchar(60),                                                                                                                                        
	name varchar(60),                                                                                                                                              
	company varchar(60),                                                                                                                                           
	divisn varchar(60),                                                                                                                                            
	department varchar(60),                                                                                                                                        
	addrline1 varchar(150),                                                                                                                                        
	addrline2 varchar(150),                                                                                                                                        
	cityname varchar(100),                                                                                                                                         
	stateprov varchar(20),                                                                                                                                         
	zipcode varchar(20),                                                                                                                                           
	zipplus4 varchar(10),                                                                                                                                          
	prefcitycd char,                                                                                                                                               
	nametype char,                                                                                                                                                 
	exp_iss_num integer                                                                                                                                            
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_response_summary;                                                                                                                 
 create table prod.agg_response_summary                                                                                                                         
(                                                                                                                                                               
	individual_id bigint not null encode delta32k distkey,                                                                                                         
	bus_unt varchar(30) not null encode bytedict,                                                                                                                  
	keycode varchar(30) not null encode text255,                                                                                                                   
	prom_dt timestamp encode delta32k,                                                                                                                             
	abq_resp_cd char encode bytedict,                                                                                                                              
	adv_resp_cd char encode bytedict,                                                                                                                              
	resp_dt timestamp encode delta32k,                                                                                                                             
	aps_resp_cd char encode bytedict,                                                                                                                              
	fr_resp_cd char encode bytedict,                                                                                                                               
	subs_resp_cd char encode bytedict,                                                                                                                             
	crsch_resp_cd char encode bytedict,                                                                                                                            
	key_response_summary varchar(64) not null,                                                                                                                     
	constraint agg_response_summary_pkey                                                                                                                           
		primary key (individual_id, bus_unt, keycode)                                                                                                                 
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(bus_unt, keycode)                                                                                                                           
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_target_analytics;                                                                                                                 
 create table prod.agg_target_analytics                                                                                                                         
(                                                                                                                                                               
	individual_id bigint not null distkey,                                                                                                                         
	scr_dt timestamp not null,                                                                                                                                     
	echelon_scr char,                                                                                                                                              
	tag_scr char,                                                                                                                                                  
	key_target_analytics varchar(64) not null,                                                                                                                     
	constraint agg_target_analytics2_pkey                                                                                                                          
		primary key (individual_id, scr_dt)                                                                                                                           
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_web_session;                                                                                                                      
 create table prod.agg_web_session                                                                                                                              
(                                                                                                                                                               
	individual_id bigint distkey,                                                                                                                                  
	visit_id varchar(50),                                                                                                                                          
	visit_num varchar(10),                                                                                                                                         
	site_indicator char,                                                                                                                                           
	visit_start_time timestamp,                                                                                                                                    
	visit_time numeric(20),                                                                                                                                        
	ext_keycode varchar(100),                                                                                                                                      
	int_keycode varchar(100),                                                                                                                                      
	count_page_views integer,                                                                                                                                      
	referring_domain varchar(100),                                                                                                                                 
	referrer_type char,                                                                                                                                            
	search_type char,                                                                                                                                              
	mobile_device_desc varchar(1000),                                                                                                                              
	mobile_device_type char,                                                                                                                                       
	operating_system varchar(255),                                                                                                                                 
	commerce_login_flg char,                                                                                                                                       
	entry_page varchar(255),                                                                                                                                       
	entry_page_short varchar(255),                                                                                                                                 
	forums_cnt integer,                                                                                                                                            
	print_interaction_cnt integer,                                                                                                                                 
	email_interaction_cnt integer,                                                                                                                                 
	twitter_interaction_cnt integer,                                                                                                                               
	share_interaction_cnt integer,                                                                                                                                 
	facebook_interaction_cnt integer,                                                                                                                              
	google_interaction_cnt integer,                                                                                                                                
	tool_usage_cnt integer,                                                                                                                                        
	partner_zag_cnt bigint,                                                                                                                                        
	partner_amazon_cnt integer,                                                                                                                                    
	partner_sears_cnt integer,                                                                                                                                     
	partner_lowes_cnt integer,                                                                                                                                     
	partner_billshrink_cnt integer,                                                                                                                                
	partner_walmart_cnt integer,                                                                                                                                   
	partner_bestbuy_cnt integer,                                                                                                                                   
	partner_applnc_connect_cnt integer,                                                                                                                            
	partner_amazon_mrkt_cnt integer,                                                                                                                               
	partner_homedepot_cnt integer,                                                                                                                                 
	partner_other_cnt integer,                                                                                                                                     
	cro_order_flg char,                                                                                                                                            
	cro_order_amt numeric(12,2),                                                                                                                                   
	cro_cancel_options_flg char,                                                                                                                                   
	cro_sar_flg char,                                                                                                                                              
	cro_cancellation_flg char,                                                                                                                                     
	carp_order_flg char,                                                                                                                                           
	carp_bundle_order_flg char,                                                                                                                                    
	carp_order_amt numeric(12,2),                                                                                                                                  
	carp_cancel_options_flg char,                                                                                                                                  
	carp_sar_flg char,                                                                                                                                             
	carp_cancellation_flg char,                                                                                                                                    
	cro_mobile_order_flg char,                                                                                                                                     
	cro_mobile_order_amt numeric(12,2),                                                                                                                            
	cro_gift_order_flg char,                                                                                                                                       
	cro_gift_order_amt numeric(12,2),                                                                                                                              
	cro_gift_redemption_flg char,                                                                                                                                  
	cro_nook_order_flg char,                                                                                                                                       
	cro_nook_order_amt numeric(12,2),                                                                                                                              
	cr_order_flg char,                                                                                                                                             
	cr_order_amt numeric(12,2),                                                                                                                                    
	cr_gift_order_flg char,                                                                                                                                        
	cr_gift_order_amt numeric(12,2),                                                                                                                               
	oh_order_flg char,                                                                                                                                             
	oh_order_amt numeric(12,2),                                                                                                                                    
	oh_gift_order_flg char,                                                                                                                                        
	oh_gift_order_amt numeric(12,2),                                                                                                                               
	ss_order_flg char,                                                                                                                                             
	ss_gift_order_flg char,                                                                                                                                        
	ma_order_flg char,                                                                                                                                             
	ma_order_amt numeric(12,2),                                                                                                                                    
	ma_gift_order_flg char,                                                                                                                                        
	ma_gift_order_amt numeric(12,2),                                                                                                                               
	aps_order_flg char,                                                                                                                                            
	aps_order_amt numeric(12,2),                                                                                                                                   
	ncps_order_flg char,                                                                                                                                           
	ncps_order_amt numeric(12,2),                                                                                                                                  
	state varchar(50),                                                                                                                                             
	home_page_views integer,                                                                                                                                       
	home_page_visit_time numeric(20),                                                                                                                              
	cars_views integer,                                                                                                                                            
	cars_visit_time numeric(20),                                                                                                                                   
	appliances_views integer,                                                                                                                                      
	appliances_visit_time numeric(20),                                                                                                                             
	electronics_views integer,                                                                                                                                     
	electronics_visit_time numeric(20),                                                                                                                            
	home_views integer,                                                                                                                                            
	home_visit_time numeric(20),                                                                                                                                   
	babies_views integer,                                                                                                                                          
	babies_visit_time numeric(20),                                                                                                                                 
	health_views integer,                                                                                                                                          
	health_visit_time numeric(20),                                                                                                                                 
	money_views integer,                                                                                                                                           
	money_visit_time numeric(20),                                                                                                                                  
	shopping_views integer,                                                                                                                                        
	shopping_visit_time numeric(20),                                                                                                                               
	truecars_views integer,                                                                                                                                        
	food_views integer,                                                                                                                                            
	service_views integer,                                                                                                                                         
	subscriber_flg char,                                                                                                                                           
	subscriber_flg_2 char                                                                                                                                          
)                                                                                                                                                               
diststyle key                                                                                                                                                   
;                                                                                                                                                               
                                                                                                                                                                
drop table if exists prod.agg_print_order;                                                                                                                      
 create table prod.agg_print_order                                                                                                                              
(                                                                                                                                                               
	ord_id bigint not null,                                                                                                                                        
	individual_id bigint not null,                                                                                                                                 
	hash_account_id varchar(64) distkey,                                                                                                                           
	source_name varchar(64),                                                                                                                                       
	account_subtype_code varchar(20),                                                                                                                              
	acct_id varchar(64) not null,                                                                                                                                  
	ord_num bigint,                                                                                                                                                
	ord_dt timestamp,                                                                                                                                              
	stat_cd char,                                                                                                                                                  
	cplt_dt timestamp,                                                                                                                                             
	entr_typ_cd char(2),                                                                                                                                           
	cr_stat_cd char,                                                                                                                                               
	canc_flg varchar(1),                                                                                                                                           
	canc_rsn_cd varchar(20),                                                                                                                                       
	canc_dt timestamp,                                                                                                                                             
	iss_canc_num numeric(18),                                                                                                                                      
	term_mth_cnt numeric(3),                                                                                                                                       
	net_val_amt numeric(12,2),                                                                                                                                     
	agcy_grs_val_amt numeric(12,2),                                                                                                                                
	ofr_chngd_flg char,                                                                                                                                            
	rnw_cd char,                                                                                                                                                   
	mlt_cpy_flg char,                                                                                                                                              
	src_cd char(5),                                                                                                                                                
	md_cd char,                                                                                                                                                    
	mag_cd varchar(25),                                                                                                                                            
	mag_catg_cd numeric(3),                                                                                                                                        
	set_cd char,                                                                                                                                                   
	bulk_flg varchar(1),                                                                                                                                           
	keycode varchar(30),                                                                                                                                           
	spec_info_txt varchar(10),                                                                                                                                     
	agcy_cd numeric(5),                                                                                                                                            
	prev_exp_iss_num numeric(5),                                                                                                                                   
	orig_strt_iss_num numeric(5),                                                                                                                                  
	orig_ord_flg varchar(1),                                                                                                                                       
	first_dt timestamp,                                                                                                                                            
	pd_amt numeric(38,2),                                                                                                                                          
	pmt_dt timestamp,                                                                                                                                              
	pmt_mthd_cd varchar(5),                                                                                                                                        
	pmt_typ_cd char(5),                                                                                                                                            
	tax_pd_amt numeric(38,2),                                                                                                                                      
	spec_hdlg_pd_amt numeric(38,2),                                                                                                                                
	blg_key_num char(8),                                                                                                                                           
	key_print_order varchar(64) not null,                                                                                                                          
	constraint agg_print_order_pkey                                                                                                                                
		primary key (individual_id, acct_id, ord_id)                                                                                                                  
)                                                                                                                                                               
diststyle key                                                                                                                                                   
interleaved sortkey(hash_account_id, individual_id, ord_id, acct_id, ord_dt)                                                                                    
;                                                                                                                                                               
                                                                                                                                                                
