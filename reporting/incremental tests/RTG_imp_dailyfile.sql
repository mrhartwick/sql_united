select t1.user_id,
Case
       when (regexp_instr(p1.placement,'General_'))
              then SUBSTRING(p1.placement,1,(regexp_instr(p1.placement,'General_')+6))
       when (regexp_instr(p1.placement,'Non-Member_'))
              then SUBSTRING(p1.placement,1,(regexp_instr(p1.placement,'Non-Member_')+9))
       when (regexp_instr(p1.placement,'Premier_'))
              then SUBSTRING(p1.placement,1,(regexp_instr(p1.placement,'Premier_')+6))
END as placement, count(*) as impressions

from wmprodfeeds.united.dfa2_impression as t1

left outer join wmprodfeeds.united.dfa2_placements as p1
on t1.placement_id = p1.placement_id

where t1.campaign_id = 20606595
and t1.site_id_dcm = 1239319
and p1.placement not like '%PROS%'
and p1.placement like '%days%'
and t1.md_event_date_loc = '2018-09-08'
and t1.advertiser_id <> 0
and t1.user_id <> '0'
group by t1.user_id, p1.placement
