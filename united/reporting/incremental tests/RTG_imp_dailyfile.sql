select

t2.user_id as user_id,
t2.date as "date",
case when t2.placement = 'Test9_22days_Non-Member' then 'Test9_22+days_Non-Member'
     when t2.placement = 'Test6_22days_Premier' then 'Test6_22+days_Premier'
     else t2.placement end as placement,
sum(impressions) as impressions

from(

select t1.user_id,
Case
       when (regexp_instr(p1.placement,'General_'))
              then SUBSTRING(p1.placement,43,(regexp_instr(p1.placement,'General_')-36))
       when (regexp_instr(p1.placement,'Non-Member_'))
              then SUBSTRING(p1.placement,43,(regexp_instr(p1.placement,'Non-Member_')-33))
       when (regexp_instr(p1.placement,'Premier_'))
              then SUBSTRING(p1.placement,43,(regexp_instr(p1.placement,'Premier_')-36))
END as placement, count(*) as impressions, t1.md_event_date_loc as "date"

from wmprodfeeds.united.dfa2_impression as t1

left outer join wmprodfeeds.united.dfa2_placements as p1
on t1.placement_id = p1.placement_id

where t1.campaign_id = 20606595
and t1.site_id_dcm = 1239319
and p1.placement not like '%PROS%'
and p1.placement not like '%Bid%'
and t1.md_event_date_loc between '2018-09-21' and '2018-09-23'
and t1.advertiser_id <> 0
group by t1.user_id, p1.placement, t1.md_event_date_loc) as t2

group by t2.user_id, t2.placement, t2.date
