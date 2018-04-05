individual_id
case when device_type = '' and channel = '' then substr(min(nvl(last_used_dt,'0101') || ar.keycode),5) abq_fst_act_keycode end qq

;
drop table cr_temp.manual_agg_individual_device_summary
;
create table cr_temp.manual_agg_individual_device_summary as 
with group1_raw as (

select * 
, row_number () over (partition by individual_id, device_type, channel order by last_used_dt	desc, times_used_cnt asc)		as date_rank
, row_number () over (partition by individual_id, device_type, channel order by times_used_cnt desc, last_used_dt asc)	as coun_rank
from prod.agg_individual_device_history
), group2_mapping as (

select individual_id	

,max(case when device_type = 'M'                    and channel = 'W' and date_rank = 1 then device_name        end) LST_MOBILE_PHONE_WEB
,max(case when device_type = 'M'                    and channel = 'W' and date_rank = 1 then last_used_dt       end) LST_MOBILE_PHONE_WEB_DT
,max(case when device_type = 'M'                    and channel = 'W' and coun_rank = 1 then device_name        end) MOST_USED_MOBILE_PHONE_WEB
,max(case when device_type = 'M'                    and channel = 'W' and date_rank = 1 then OPERATING_SYSTEM   end) LST_MOBILE_PHONE_OPSYS_WEB
,max(case when device_type = 'M'                    and channel = 'W' and coun_rank = 1 then OPERATING_SYSTEM   end) MOST_USED_MBL_PHONE_OPSYS_WEB
,max(case when device_type = 'T'                    and channel = 'W' and date_rank = 1 then device_name        end) LST_TABLET_WEB
,max(case when device_type = 'T'                    and channel = 'W' and date_rank = 1 then last_used_dt       end) LST_TABLET_WEB_DT
,max(case when device_type = 'T'                    and channel = 'W' and coun_rank = 1 then device_name        end) MOST_USED_TABLET_WEB
,max(case when device_type = 'T'                    and channel = 'W' and date_rank = 1 then OPERATING_SYSTEM   end) LST_TABLET_OPSYS_WEB
,max(case when device_type = 'T'                    and channel = 'W' and coun_rank = 1 then OPERATING_SYSTEM   end) MOST_USED_TABLET_OPSYS_WEB
,max(case when device_type = 'D'                    and channel = 'W' and date_rank = 1 then OPERATING_SYSTEM   end) LST_DESKTOP_OPSYS_WEB
,max(case when device_type = 'D'                    and channel = 'W' and date_rank = 1 then last_used_dt       end) LST_DESKTOP_WEB_DT
,max(case when device_type = 'D'                    and channel = 'W' and coun_rank = 1 then OPERATING_SYSTEM   end) MOST_USED_DESKTOP_OPSYS_WEB
,max(case when device_type NOT IN ('D','T','M')     and channel = 'W' and date_rank = 1 then device_name        end) LST_OTHER_DEVICE_WEB
,max(case when device_type NOT IN ('D','T','M')     and channel = 'W' and date_rank = 1 then last_used_dt       end) LST_OTHER_DEVICE_WEB_DT
,max(case when device_type NOT IN ('D','T','M')     and channel = 'W' and coun_rank = 1 then device_name        end) MOST_USED_OTHER_DEVICE_WEB
,max(case when device_type NOT IN ('D','T','M')     and channel = 'W' and date_rank = 1 then OPERATING_SYSTEM   end) LST_OTHER_DEVICE_OPSYS_WEB
,max(case when device_type NOT IN ('D','T','M')     and channel = 'W' and coun_rank = 1 then OPERATING_SYSTEM   end) MOST_USED_OTHER_DEV_OPSYS_WEB
,max(case when device_type = 'M'                    and channel = 'E' and date_rank = 1 then device_name        end) LST_MOBILE_PHONE_EML
,max(case when device_type = 'M'                    and channel = 'E' and date_rank = 1 then last_used_dt       end) LST_MOBILE_PHONE_EML_DT
,max(case when device_type = 'M'                    and channel = 'E' and coun_rank = 1 then device_name        end) MOST_USED_MOBILE_PHONE_EML
,max(case when device_type = 'M'                    and channel = 'E' and date_rank = 1 then OPERATING_SYSTEM   end) LST_MOBILE_PHONE_OPSYS_EML
,max(case when device_type = 'M'                    and channel = 'E' and coun_rank = 1 then OPERATING_SYSTEM   end) MOST_USED_MBL_PHONE_OPSYS_EML
,max(case when device_type = 'T'                    and channel = 'E' and date_rank = 1 then device_name        end) LST_TABLET_EML
,max(case when device_type = 'T'                    and channel = 'E' and date_rank = 1 then last_used_dt       end) LST_TABLET_EML_DT
,max(case when device_type = 'T'                    and channel = 'W' and coun_rank = 1 then device_name        end) MOST_USED_TABLET_EML
,max(case when device_type = 'T'                    and channel = 'E' and date_rank = 1 then OPERATING_SYSTEM   end) LST_TABLET_OPSYS_EML
,max(case when device_type = 'T'                    and channel = 'W' and coun_rank = 1 then OPERATING_SYSTEM   end) MOST_USED_TABLET_OPSYS_EML
,max(case when device_type = 'D'                    and channel = 'E' and date_rank = 1 then OPERATING_SYSTEM   end) LST_DESKTOP_OPSYS_EML
,max(case when device_type = 'D'                    and channel = 'E' and date_rank = 1 then last_used_dt       end) LST_DESKTOP_EML_DT
,max(case when device_type = 'D'                    and channel = 'E' and coun_rank = 1 then OPERATING_SYSTEM   end) MOST_USED_DESKTOP_OPSYS_EML
,max(case when device_type NOT IN ('D','T','M')     and channel = 'E' and date_rank = 1 then device_name        end) LST_OTHER_DEVICE_EML
,max(case when device_type NOT IN ('D','T','M')     and channel = 'E' and date_rank = 1 then last_used_dt       end) LST_OTHER_DEVICE_EML_DT
,max(case when device_type NOT IN ('D','T','M')     and channel = 'E' and coun_rank = 1 then device_name        end) MOST_USED_OTHER_DEVICE_EML
,max(case when device_type NOT IN ('D','T','M')     and channel = 'E' and date_rank = 1 then OPERATING_SYSTEM   end) LST_OTHER_DEVICE_OPSYS_EML
,max(case when device_type NOT IN ('D','T','M')     and channel = 'E' and coun_rank = 1 then OPERATING_SYSTEM   end) MOST_USED_OTHER_DEV_OPSYS_EML

from group1_raw
group by individual_id
)


select *
from group2_mapping
;
with tt1 (q,w,e) as (

select 1,1,2
union all select 1,3,4
union all select 1,5,6
), tt2 as (
select q
, case when w = 3 then e end s 
from tt1)

select * from tt2
group by q,s 
--group by q, w
;
select dh.*
, row_number () over (partition by device_type, channel order by last_used_dt)		as date_rank
, row_number () over (partition by device_type, channel order by times_used_cnt)	as date_cnt
--individual_id, device_type, channel, max(times_used_cnt), max(last_used_dt)
from prod.agg_individual_device_history dh
where individual_id = 1218782101
--group by individual_id, device_type, channel

;
SELECT TOP 100 *
FROM prod.agg_individual_device_summary
WHERE 1=1 and individual_id = 1230446105
;
SELECT TOP 100 *
FROM prod.agg_individual_device_history
WHERE 1=1 and individual_id = 1218782101
;
;
SELECT TOP 100 *
FROM prod.
WHERE 1=1
;
;
with diff as (
select * from (
SELECT  individual_id  ,lst_mobile_phone_web,lst_mobile_phone_web_dt,most_used_mobile_phone_web,lst_mobile_phone_opsys_web,most_used_mbl_phone_opsys_web,lst_tablet_web,lst_tablet_web_dt,most_used_tablet_web,lst_tablet_opsys_web,most_used_tablet_opsys_web,lst_desktop_opsys_web,lst_desktop_web_dt,most_used_desktop_opsys_web,lst_other_device_web,lst_other_device_web_dt,most_used_other_device_web,lst_other_device_opsys_web,most_used_other_dev_opsys_web,lst_mobile_phone_eml,lst_mobile_phone_eml_dt,most_used_mobile_phone_eml,lst_mobile_phone_opsys_eml,most_used_mbl_phone_opsys_eml,lst_tablet_eml,lst_tablet_eml_dt/*,most_used_tablet_eml,lst_tablet_opsys_eml,most_used_tablet_opsys_eml*//*,lst_desktop_opsys_eml*/,lst_desktop_eml_dt,most_used_desktop_opsys_eml,lst_other_device_eml,lst_other_device_eml_dt,most_used_other_device_eml,lst_other_device_opsys_eml,most_used_other_dev_opsys_eml
FROM cr_temp.manual_agg_individual_device_summary
--WHERE 1=1 and individual_id in (  1230446105 )
union all
SELECT  individual_id  ,lst_mobile_phone_web,lst_mobile_phone_web_dt,most_used_mobile_phone_web,lst_mobile_phone_opsys_web,most_used_mbl_phone_opsys_web,lst_tablet_web,lst_tablet_web_dt,most_used_tablet_web,lst_tablet_opsys_web,most_used_tablet_opsys_web,lst_desktop_opsys_web,lst_desktop_web_dt,most_used_desktop_opsys_web,lst_other_device_web,lst_other_device_web_dt,most_used_other_device_web,lst_other_device_opsys_web,most_used_other_dev_opsys_web,lst_mobile_phone_eml,lst_mobile_phone_eml_dt,most_used_mobile_phone_eml,lst_mobile_phone_opsys_eml,most_used_mbl_phone_opsys_eml,lst_tablet_eml,lst_tablet_eml_dt/*,most_used_tablet_eml,lst_tablet_opsys_eml,most_used_tablet_opsys_eml*//*,lst_desktop_opsys_eml*/,lst_desktop_eml_dt,most_used_desktop_opsys_eml,lst_other_device_eml,lst_other_device_eml_dt,most_used_other_device_eml,lst_other_device_opsys_eml,most_used_other_dev_opsys_eml
FROM prod.agg_individual_device_summary
--WHERE 1=1 and individual_id in (  1230446105 )
)
 group by individual_id,lst_mobile_phone_web,lst_mobile_phone_web_dt,most_used_mobile_phone_web,lst_mobile_phone_opsys_web,most_used_mbl_phone_opsys_web,lst_tablet_web,lst_tablet_web_dt,most_used_tablet_web,lst_tablet_opsys_web,most_used_tablet_opsys_web,lst_desktop_opsys_web,lst_desktop_web_dt,most_used_desktop_opsys_web,lst_other_device_web,lst_other_device_web_dt,most_used_other_device_web,lst_other_device_opsys_web,most_used_other_dev_opsys_web,lst_mobile_phone_eml,lst_mobile_phone_eml_dt,most_used_mobile_phone_eml,lst_mobile_phone_opsys_eml,most_used_mbl_phone_opsys_eml,lst_tablet_eml,lst_tablet_eml_dt/*,most_used_tablet_eml,lst_tablet_opsys_eml,most_used_tablet_opsys_eml*//*,lst_desktop_opsys_eml*/,lst_desktop_eml_dt,most_used_desktop_opsys_eml,lst_other_device_eml,lst_other_device_eml_dt,most_used_other_device_eml,lst_other_device_opsys_eml,most_used_other_dev_opsys_eml
 having count(*) = 1
 )
 
 select top 300 *
 from diff
 --where LST_MOBILE_PHONE_WEB_DT != lead(LST_MOBILE_PHONE_WEB_DT,1) over (partition by individual_id order by individual_id)

 order by 1
 ;
 ;

SELECT TOP 100 *
FROM cr_temp.manual_agg_individual_device_summary
WHERE 1=1 and individual_id in (  1200000058 )
union all
SELECT TOP 100 *
FROM prod.agg_individual_device_summary
WHERE 1=1 and individual_id in (  1200000058 )

;
;
SELECT TOP 100 *
FROM prod.agg_individual_device_history
WHERE 1=1 and individual_id = 1200000059

