    SELECT
      cast(date_part(month,(t1.md_interaction_date_loc)) as int) as reportmonth
       ,md_interaction_date_loc AS "date"
       ,Left(Right(other_data, Len(other_data) - Position('u18=' IN other_data) -3 ),Position(';' IN Right(other_data, Len(other_data) - Position('u18=' IN other_data) -3 ) ) -1) AS BASE64_PNR
      ,sum(total_conversions) AS Tickets
--       ,p1.placement
    FROM
       united.dfa2_activity as t1

    left join wmprodfeeds.united.dfa2_placements as p1
    on t1.placement_id = p1.placement_id

    WHERE
       t1.activity_id = 978826
       and t1.md_interaction_date_loc between '2018-01-01' and '2018-06-30'
       and(t1.advertiser_id <> 0)
       and (length(isnull(t1.event_sub_type,'')) > 0)
--        and p1.placement like '%PROS_FT%'
--        and p1.placement like '%RouteSupport%'
       and total_revenue <> '0'
       and t1.campaign_id = 20721526
--        and t1.site_id_dcm = 3267410
    group by other_data, md_interaction_date_loc, cast(date_part(month,(t1.md_interaction_date_loc)) as int)