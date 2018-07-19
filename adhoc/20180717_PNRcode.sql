select
     md_event_date_loc                             as booking_date
    , Left(Right(other_data, Len(other_data) - Position('u18=' in other_data) - 3), Position(';' in Right(other_data, Len(other_data) - Position('u18=' in other_data) - 3)) - 1) as BASE64_PNR
    , udf_base64Decode(Left(Right(other_data, Len(other_data) - Position('u18=' in other_data) - 3), Position(';' in Right(other_data, Len(other_data) - Position('u18=' in other_data) - 3)) - 1)) as PNR
    , sum(total_conversions)                              as Tickets
--     ,p1.placement
from
    united.dfa2_activity as t1

    left join wmprodfeeds.united.dfa2_placements as p1
    on t1.placement_id = p1.placement_id

where
            t1.activity_id = 978826
        and t1.md_interaction_date_loc between '2018-01-01' and '2018-06-30'
        and (t1.advertiser_id <> 0)
        and (length(isnull (t1.event_sub_type, '')) > 0)
--        and p1.placement like '%PROS_FT%'
--        and p1.placement like '%RouteSupport%'
        and total_revenue <> '0'
        and t1.campaign_id in (20713692, 21230517)
--        and t1.site_id_dcm in (1190273, 1239319)
group by
    other_data,
     md_event_date_loc
-- 	p1. placement
