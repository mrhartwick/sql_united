


select
--    cast(date_part(month, (t1.md_interaction_date_loc)) as int) as reportmonth
      md_interaction_time_loc as booking_time
    , md_interaction_date_loc                             as booking_date
    , cast(replace(replace(regexp_substr(other_data,'(u9\\=)(\\d.?\\d.?\\d.?\\d.?\\d.?\\d.?\\d.?\\d)(\\;u10\\=)'),'u9=',''),';u10=','') as date) as traveldate_1
    , Left(Right(other_data, Len(other_data) - Position('u18=' in other_data) - 3), Position(';' in Right(other_data, Len(other_data) - Position('u18=' in other_data) - 3)) - 1) as BASE64_PNR
    , udf_base64Decode(Left(Right(other_data, Len(other_data) - Position('u18=' in other_data) - 3), Position(';' in Right(other_data, Len(other_data) - Position('u18=' in other_data) - 3)) - 1)) as PNR
    , sum(total_conversions)                              as Tickets
--       ,p1.placement
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
        and t1.campaign_id in (20561981, 8063775, 9917294, 8161352, 9961760, 6146233)
--        and t1.site_id_dcm = 3267410
group by
    other_data,
    md_interaction_date_loc,
--  cast(date_part(month, (t1.md_interaction_date_loc)) as int),
    md_interaction_time_loc

