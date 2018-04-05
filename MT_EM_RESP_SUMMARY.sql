select *
  from prod.email_response
 where 1=1 and individual_id in (
1214686631
,1200034725)
 limit 10;
 
 with tt1 (individual_id, email, member_id, cell_id, catalog_type
 , legacy_individual_id, campaign_id, campaign_name, opened_flag
 , bounced_flag, clicked, cell_subject, keycode, list_id, list_desc
 , campaign_optout, email_finder_num, message_id, create_date
 , create_feed_instance_id, update_date
) as (


select '1200034725', 'iampuresuccess@hotmail.com', '1102262840', '358408', 'H', '10026100056915010', '681676', '20080327_CRO_Reactivation', '0', '0', '0', '<DI_NAME ALT="Valued&#x20;Consumer" FIRST="true" FORMAT="true" LAST="false"/>,&#x20;We&#x20;Want&#x20;You&#x20;Back!', 'K0609MA18', '10228', 'Consumer Reports Online', 'NULL', 'NULL', 'NULL', '2008-04-11 12:05:30', 'E79038EC-FCC1-4EF2-B9CD-C42128DC900F193481829486374', '2017-08-30 01:10:29', Null union all
select '1200034725', 'iampuresuccess@hotmail.com', '1150965788', '385988', 'H', '10026100056915010', '712724', '20090622_CRONL_ReactJune09', '0', '0', '0', '<DI_NAME ALT="Valued&#x20;Customer" FIRST="true" FORMAT="true" LAST="false"/>,&#x20;We&#x20;Want&#x20;You&#x20;Back!', 'K0609MA33', '10228', 'Consumer Reports Online', 'NULL', 'NULL', 'NULL', '2009-07-01 11:27:34', 'E79038EC-FCC1-4EF2-B9CD-C42128DC900F193481829486374', '2017-08-30 01:10:29', Null union all
select '1214686631', 'mamom88@aol.com', '1139824212', '485994', 'P', '10002000417562020', '950200', '20130821_CROMC_CRO_Build_BuyAugust2013', '0', '0', '0', 'Build &#38; Buy Car Program - Included in Your Subscription', 'EH38BR2', '11367', 'CRO Member Communications', 'NULL', 'NULL', 'NULL', '2013-08-29 12:26:02', 'E79038EC-FCC1-4EF2-B9CD-C42128DC900F193481829486374', '2017-08-30 01:25:08', Null union all
select '1214686631', 'mamom88@aol.com', '1139824212', '514128', 'P', '10002000417562020', '1007166', '20140528_CRONL_ReactMay2014', '0', '0', '0', '<DI_NAME ALT="Valued&#x20;Consumer" FIRST="true" FORMAT="true" LAST="false"/>,&#x20;You&#39;re&#x20;Invited&#x20;to&#x20;Re-Subs', 'K1405AT02', '10228', 'Consumer Reports Online', 'NULL', 'NULL', 'NULL', '2014-06-05 15:29:03', 'E79038EC-FCC1-4EF2-B9CD-C42128DC900F193481829486374', '2017-08-30 01:25:08', Null union all
select '1214686631', 'mamom88@aol.com', '1139824212', '479108', 'P', '10002000417562020', '933756', '20130521_CUAQB_Week7', '0', '0', '0', 'Annual Survey Reminder: Rate Your Car', '20135008C', '10239', 'Consumer Reports ABQ', 'NULL', 'NULL', 'NULL', '2013-05-30 13:26:01', 'E79038EC-FCC1-4EF2-B9CD-C42128DC900F193481829486374', '2017-08-30 01:25:08', Null union all
select '1214686631', 'mamom88@aol.com', '1139824212', '548120', 'P', '10002000417562020', '1039678', '201401219_CRONL_ReactDec2014', '0', '0', '0', '<DI_NAME ALT="Valued&#x20;Consumer" FIRST="true" FORMAT="true" LAST="false"/>,&#x20;You&#39;re&#x20;Invited&#x20;to&#x20;Re-Subs', 'K1412AD07', '10228', 'Consumer Reports Online', 'NULL', 'NULL', 'NULL', '2014-12-30 14:36:01', 'E79038EC-FCC1-4EF2-B9CD-C42128DC900F193481829486374', '2017-08-30 01:25:08', Null union all
select '1214686631', 'mamom88@aol.com', '1139824212', '656676', 'P', '10002000417562020', '1101730', '20160728_CRMAG_ACQDURING', '1', '0', '0', '<DI_NAME ALT="Valued&#x20;Consumer" FIRST="true" FORMAT="true" LAST="false"/>,&#x20;don&#39;t&#x20;forget&#x20;your&#x20;special', 'EP67CMEA2', '10233', 'Consumer Reports Email', 'NULL', 'NULL', 'NULL', '2016-07-29 04:51:02', 'E79038EC-FCC1-4EF2-B9CD-C42128DC900F193481829486374', '2017-08-30 01:25:08', Null union all
select '1214686631', 'mamom88@aol.com', '1139824212', '376456', 'O', '10002000417562020', '701358', '20090129_CNS_D_0902', '0', '0', '0', 'URGENT, Renew now and save with Consumer Reports', 'ERENEW376456', '10233', 'Consumer Reports Email', 'NULL', 'NULL', 'NULL', '2009-02-04 14:36:47', 'E79038EC-FCC1-4EF2-B9CD-C42128DC900F193481829486374', '2017-08-30 01:25:08', Null union all
select '1214686631', 'mamom88@aol.com', '1139824212', '482962', 'P', '10002000417562020', '942848', '20130716_CRONH_MCBefore', '1', '0', '0', 'Here you go - Complimentary offer for Consumer Reports on Health', 'DN37HBC1', '10230', 'Consumer Reports On Health', 'NULL', 'NULL', 'NULL', '2013-07-25 15:02:02', 'E79038EC-FCC1-4EF2-B9CD-C42128DC900F193481829486374', '2017-08-30 01:25:08', Null union all
select '1214686631', 'mamom88@aol.com', '1139824212', '480000', 'P', '10002000417562020', '942848', '20130716_CRONH_MCBefore', '1', '0', '0', 'Here you go - Complimentary offer for Consumer Reports on Health', 'DN37HBC1', '10230', 'Consumer Reports On Health', 'NULL', 'NULL', 'NULL', '2013-07-25 15:02:02', 'E79038EC-FCC1-4EF2-B9CD-C42128DC900F193481829486374', '2017-08-30 01:25:08', Null union all

select '1214686631', 'mamom88@aol.com', '1139824212', '590176', 'P', '10002000417562020', '1066558', '20150825_CROMC_CRO_BuildandBuyAug2015', '1', '0', '0', 'Consumer Reports: Subscriber savings on new and used cars', 'EH58BR1', '11367', 'CRO Member Communications', 'NULL', 'NULL', 'NULL', '2015-08-26 07:25:02', 'E79038EC-FCC1-4EF2-B9CD-C42128DC900F193481829486374', '2017-08-30 01:25:08', Null

)
;
create table cr_temp.agg_em_resp_summ_manual as 
with agg_em_res_summ_man ( INDIVIDUAL_ID  ,OPEN_CNT ,CLICK_CNT ,BOUNCE_CNT ,LST_OPEN_DT ,LST_CLICK_DT ,LST_BOUNCE_DT ,OPT_OUT_CNT ,LST_OPT_OUT_DT ,LST_MOB_DEV ,LST_MOB_DEV_DT) as (
select tt1.individual_id
, count( distinct case when opened_flag = 1 then campaign_id else null end)
, count( distinct case when clicked = 1 then campaign_id else null end)
, count( distinct case when bounced_flag = 1 then campaign_id else null end)
, max( case when opened_flag = 1 then create_date else null end)
, max( case when clicked = 1 then create_date else null end)
, max( case when bounced_flag = 1 then create_date else null end)
, count( distinct case when campaign_optout = 0 then campaign_id else null end)
, max( case when campaign_optout = 0 then create_date else null end)
--, max( update_date
--,*
, substring(max(evdate_devname),1,19) 
, substring(max(evdate_devname),1+19,max(len(evdate_devname)))
from prod.email_response tt1 left join (
select individual_id, max( event_date||device_name) as evdate_devname
  from prod.impact_mobile_device
 where 1=1 
 group by individual_id
 ) imd on imd.individual_id = tt1.individual_id

group by tt1.individual_id )

select top 100 *
from agg_em_res_summ_man
--Table Level rule:  All individuals with data in the Data Warehouse EMAIL_RESPONSE table keeping one row per individual.									
;
;
create table cr_temp.agg_em_resp_summ_manual_1 as 
with agg_em_res_summ_man ( INDIVIDUAL_ID  ,OPEN_CNT ,CLICK_CNT ,BOUNCE_CNT ,LST_OPEN_DT ,LST_CLICK_DT ,LST_BOUNCE_DT ,OPT_OUT_CNT ,LST_OPT_OUT_DT ,LST_MOB_DEV ,LST_MOB_DEV_DT) as (
select tt1.individual_id
, count( distinct case when opened_flag = 1 then campaign_id else null end)
, count( distinct case when clicked = 1 then campaign_id else null end)
, count( distinct case when bounced_flag = 1 then campaign_id else null end)
, max( case when opened_flag = 1 then create_date else null end)
, max( case when clicked = 1 then create_date else null end)
, max( case when bounced_flag = 1 then create_date else null end)
, count( distinct case when campaign_optout = 0 then campaign_id else null end)
, max( case when campaign_optout = 0 then create_date else null end)
--, max( update_date
--,*
, substring(max(evdate_devname),1,19) 
, substring(max(evdate_devname),1+19,max(len(evdate_devname)))
from prod.email_response tt1 left join (
select individual_id, max( event_date||device_name) as evdate_devname
  from prod.impact_mobile_device
 where 1=1 
 group by individual_id
 ) imd on imd.individual_id = tt1.individual_id

group by tt1.individual_id )

select *
from agg_em_res_summ_man
;
select individual_id, max( event_date||device_name) as evdate_devname
  from prod.impact_mobile_device
 where 1=1 and individual_id in (1214686631,1200034725)
 group by individual_id

;


select length('2014-09-29 00:00:00Apple iPad'),length('2014-09-29 00:00:00'), substring('2014-09-29 00:00:00Apple iPad',1,19), substring('2014-09-29 00:00:00Apple iPad',1+19,len('2014-09-29 00:00:00Apple iPad'))
;
LEGACY_IMPACT_MOBILE_DEVICE.DEVICE_NAME associated to the maximum LEGACY_IMPACT_MOBILE_DEVICE.EVENT_DATE for the individual_id
maximum LEGACY_IMPACT_MOBILE_DEVICE.EVENT_DATE for the individual_id
;

 limit 1000;
 
 select count(i) as i, count(c) as c, INDIVIDUAL_ID  ,OPEN_CNT ,CLICK_CNT ,BOUNCE_CNT ,LST_OPEN_DT ,LST_CLICK_DT ,LST_BOUNCE_DT ,OPT_OUT_CNT ,LST_OPT_OUT_DT ,LST_MOB_DEV ,LST_MOB_DEV_DT
from (
select  '1' i, null c,INDIVIDUAL_ID  ,OPEN_CNT ,CLICK_CNT ,BOUNCE_CNT ,LST_OPEN_DT ,LST_CLICK_DT ,LST_BOUNCE_DT ,OPT_OUT_CNT ,LST_OPT_OUT_DT ,LST_MOB_DEV ,LST_MOB_DEV_DT from cr_temp.agg_em_resp_summ_manual_1
union all
select  null i,'1' c, INDIVIDUAL_ID  ,OPEN_CNT ,CLICK_CNT ,BOUNCE_CNT ,LST_OPEN_DT ,LST_CLICK_DT ,LST_BOUNCE_DT ,OPT_OUT_CNT ,LST_OPT_OUT_DT ,LST_MOB_DEV ,LST_MOB_DEV_DT from prod.agg_email_response_summary
)

group by INDIVIDUAL_ID  ,OPEN_CNT ,CLICK_CNT ,BOUNCE_CNT ,LST_OPEN_DT ,LST_CLICK_DT ,LST_BOUNCE_DT ,OPT_OUT_CNT ,LST_OPT_OUT_DT ,LST_MOB_DEV ,LST_MOB_DEV_DT
having count(i) != count(c)


;
