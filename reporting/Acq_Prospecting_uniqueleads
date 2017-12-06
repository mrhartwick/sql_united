select
cast(r1.date as date)                     as dcmdate,
campaign.campaign                         as campaign,
r1.campaign_id                            as campaign_id,
r1.site_id_dcm                            as site_id_dcm,
directory.site_dcm                        as site_dcm,
left(p1.placement,6)                      as plce_id,
replace(replace(p1.placement ,',', ''),'"','') as placement,
r1.placement_id                           as placement_id,
sum(r1.vew_led)                           as vew_led,
sum(r1.clk_led)                           as clk_led,
sum(r1.vew_led) + sum(r1.clk_led)         as led

from (


select
cast (timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),'SS') as date ) as "date"
,ta.campaign_id as campaign_id
,ta.site_id_dcm as site_id_dcm
,ta.placement_id as placement_id
,sum(case when activity_id = 1086066 and ta.conversion_id = 1 then 1 else 0 end) as clk_led
,sum(case when activity_id = 1086066 and ta.conversion_id = 2 then 1 else 0 end) as vew_led


from
(
select distinct(user_id), activity_id, conversion_id, interaction_time, campaign_id, site_id_dcm, placement_id
from diap01.mec_us_united_20056.dfa2_activity
where cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date ) between '2017-08-15' and '2017-11-24'
and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
and (activity_id = 1086066)
and (campaign_id = 10742878) -- display 2017
and (advertiser_id <> 0)
and (length(isnull(event_sub_type,'')) > 0)
and (user_id <> '0')
) as ta


group by
cast (timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),'SS') as date )
,ta.campaign_id
,ta.site_id_dcm
,ta.placement_id



) as r1

left join
(
select cast (campaign as varchar (4000)) as 'campaign',campaign_id as 'campaign_id'
from diap01.mec_us_united_20056.dfa2_campaigns
) as campaign
on r1.campaign_id = campaign.campaign_id

left join
(
select cast (placement as varchar (4000)) as 'placement',placement_id as 'placement_id',campaign_id as 'campaign_id',site_id_dcm as 'site_id_dcm'

-- from ( select campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast (placement_start_date as date ) as thisdate,
-- row_number() over (partition by campaign_id,site_id_dcm,placement_id order by cast (placement_start_date as date ) desc ) as x1
from diap01.mec_us_united_20056.dfa2_placements

-- ) as p1
-- where x1 = 1
) as p1
on r1.placement_id    = p1.placement_id
and r1.campaign_id = p1.campaign_id
and r1.site_id_dcm  = p1.site_id_dcm

left join
(
select cast (site_dcm as varchar (4000)) as 'site_dcm',site_id_dcm as 'site_id_dcm'
from diap01.mec_us_united_20056.dfa2_sites
) as directory
on r1.site_id_dcm = directory.site_id_dcm

where p1.placement like '%PROS_FT%' or p1.placement like '%Weather%'
and r1.site_id_dcm in (1190273,1239319, 1853562, 3267410)

group by
cast (r1.date as date )
,directory.site_dcm
,r1.site_id_dcm
,r1.campaign_id
,campaign.campaign
,r1.placement_id
,p1.placement
