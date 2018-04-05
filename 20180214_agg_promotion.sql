
/***************************************************************************
*            (C) Copyright Consumer Reports 2018
*                    All Rights Reserved.
*--------------------------------------------------------------------------
* Project    :  Consumer Reports Customer Data Hub - Agg ETL
* Module Name:  agg_promotion.sql
* Date       :  2018/02/14 16:12
* Dev & QA   :  Stanislav Dulko
***************************************************************************/

alter table prod.agg_promotion drop constraint agg_promotion_pkey_full;

drop table if exists    prod.nightly_agg_promotion;
create table            prod.nightly_agg_promotion
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
    constraint agg_promotion_pkey_full primary key (individual_id, keycode, bus_unt, prom_dt)
)
diststyle key
interleaved sortkey(individual_id, prom_dt)
;
insert into prod.nightly_agg_promotion (individual_id , osl_hh_id , keycode , bus_unt , prom_dt , chnl_cd , multi_chnl_ind , lst_don_dt , lst_don_amt , tot_don_amt , contact_type , campaign_name , ol_campaign_id , list_select_cd , output_lsc , multibuyer_cnt , queue_id , key_promotion)
SELECT DISTINCT
      individual_id         AS INDIVIDUAL_ID
    , osl_hh_id             AS OSL_HH_ID
    , keycode               AS KEYCODE
    , business_unit         AS BUS_UNT
    , promotion_date        AS PROM_DT
    , channel_code          AS CHNL_CD
    , multi_channel_ind     AS MULTI_CHNL_IND
    , last_donation_date    AS LST_DON_DT
    , last_donation_amount  AS LST_DON_AMT
    , total_donation_amount AS TOT_DON_AMT
    , contact_type          AS CONTACT_TYPE
    , campaign_name         AS CAMPAIGN_NAME
    , campaign_id           AS OL_CAMPAIGN_ID
    , list_select_code      AS LIST_SELECT_CD
    , output_lsc            AS OUTPUT_LSC
    , multibuyer_cnt        AS MULTIBUYER_CNT
    , queue_id              AS QUEUE_ID
    , MD5(cast(individual_id AS VARCHAR(64)) || cast(keycode AS VARCHAR(64)) || cast(business_unit AS VARCHAR(64)) || cast(promotion_date AS VARCHAR(64))) AS key_promotion

FROM prod.promotion
;


drop table if exists prod.agg_promotion_old;
alter table prod.agg_promotion rename to agg_promotion_old;
alter table prod.nightly_agg_promotion rename to agg_promotion;
drop table prod.agg_promotion_old;

analyze prod.promotion;
