select t1.user_id, p1.placement, count(*) as impressions
from wmprodfeeds.united.dfa2_impression as t1

left outer join wmprodfeeds.united.dfa2_placements as p1
on t1.placement_id = p1.placement_id

where t1.campaign_id = 20606595
and t1.site_id_dcm = 1239319
and (p1.placement not like '%Route%' or p1.placement not like '%PROS_FT%')
and t1.md_event_date_loc = '2018-09-03'
and t1.advertiser_id <> 0
and t1.user_id <> '0'
group by t1.user_id, p1.placement
