
select
        t1.date,
        t1.campaign,
        t1.campaign_id,
        t1.site_dcm,
        t1.site_id_dcm,
        t1.placement,
        t1.placement_id,
        f1.creative_ver,
        t1.creative_ver_id,
        sum(t1.impressions) as impressions,
        sum(t1.clicks) as clicks,
--         sum(t1.clk_led) as clk_led,
        sum(t1.clk_led) + sum(t1.vew_led) as led,
        sum(t1.clk_con) + sum(t1.vew_con) as  con,
        sum(t1.clk_tix) + sum(t1.vew_tix) as tix,
--      sum(t1.clk_con) as clk_con,
--         sum(t1.clk_tix) as clk_tix,
--         sum(t1.clk_rev) as clk_rev,
--         sum(t1.vew_led) as vew_led,
--         sum(t1.vew_con) as vew_con,
--         sum(t1.vew_tix) as vew_tix,
--         sum(t1.vew_rev) as vew_rev,
        sum(t1.rev) as rev

from (
           select *
           from openQuery(verticaunited,

    '
select
t2.date,
t2.campaign,
t2.campaign_id,
t2.site_dcm,
t2.site_id_dcm,
t2.placement,
t2.placement_id,
t2.creative_ver_id,
sum(t2.impressions) as impressions,
sum(t2.clicks) as clicks,
sum(t2.clk_led) as clk_led,
sum(t2.clk_con) as clk_con,
sum(t2.clk_tix) as clk_tix,
sum(t2.clk_rev) as clk_rev,
sum(t2.vew_led) as vew_led,
sum(t2.vew_con) as vew_con,
sum(t2.vew_tix) as vew_tix,
sum(t2.vew_rev) as vew_rev,
sum(t2.rev) as rev

from (
select
t1.event_time,
t1.date,
t1.user_id,
c1.campaign,
t1.campaign_id,
s1.site_dcm,
t1.site_id_dcm,
p1.placement,
t1.placement_id,
t1.creative_ver_id,
sum(t1.impressions) as impressions,
sum(t1.clicks) as clicks,
sum(case when ta.activity_id = 1086066 and ta.conversion_id = 1 then 1 else 0 end) as clk_led,
sum(case when ta.activity_id = 978826 and ta.conversion_id = 1 and ta.total_revenue <> 0 then 1 else 0 end) as clk_con,
sum(case when ta.activity_id = 978826 and ta.conversion_id = 1 and ta.total_revenue <> 0 then ta.total_conversions else 0 end) as clk_tix,
sum(case when ta.conversion_id = 1 then (ta.total_revenue * 1000000) / (ta.exchange_rate) else 0 end) as clk_rev,
sum(case when ta.activity_id = 1086066 and ta.conversion_id = 2 then 1 else 0 end) as vew_led,
sum(case when ta.activity_id = 978826 and ta.conversion_id = 2 and ta.total_revenue <> 0 then 1 else 0 end) as vew_con,
sum(case when ta.activity_id = 978826 and ta.conversion_id = 2 and ta.total_revenue <> 0 then ta.total_conversions else 0 end) as vew_tix,
sum(case when ta.conversion_id = 2 then (ta.total_revenue * 1000000) / (ta.exchange_rate) else 0 end) as vew_rev,
sum(ta.total_revenue * 1000000 / ta.exchange_rate) as rev
from (

select
t01.event_time,
t01.date,
t01.user_id,
t01.campaign_id,
t01.site_id_dcm,
t01.placement_id,
t01.creative_ver_id,
sum(t01.impressions) as impressions,
sum(t01.clicks) as clicks

from (
select
ti.event_time                                                             as event_time,
cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000),''SS'') as date) as "date",
ti.user_id                                                                as user_id,
ti.campaign_id                                                            as campaign_id,
ti.site_id_dcm                                                            as site_id_dcm,
ti.placement_id                                                           as placement_id,
cast(substring(u_value,(instr(u_value,''U='') + 2),7) as int)               as creative_ver_id,
count(*)                                                                  as impressions,
0                                                                         as clicks

from (
select *
from diap01.mec_us_united_20056.dfa2_impression
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),''SS'') as date) between ''2017-08-01'' and ''2017-08-20''
and regexp_like(u_value,''(U\=)(\d){7}'',''ib'')
and (advertiser_id <> 0)

) as ti
group by
cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000),''SS'') as date),
ti.campaign_id,
ti.site_id_dcm,
ti.user_id,
ti.event_time,
cast(substring(u_value,(instr(u_value,''U='') + 2),7) as int),
ti.placement_id
union all

select
tc.event_time                                                             as event_time,
cast(timestamp_trunc(to_timestamp(tc.event_time / 1000000),''SS'') as date) as "date",
tc.user_id                                                                as user_id,
tc.campaign_id                                                            as campaign_id,
tc.site_id_dcm                                                            as site_id_dcm,
tc.placement_id                                                           as placement_id,
cast(substring(u_value,(instr(u_value,''U='') + 2),7) as int)               as creative_ver_id,
0                                                                         as impressions,
count(*)                                                                  as clicks

from (

select *
from diap01.mec_us_united_20056.dfa2_click
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),''SS'') as date) between ''2017-08-01'' and ''2017-08-20''
and regexp_like(u_value,''(U\=)(\d){7}'',''ib'')
and (advertiser_id <> 0)

) as tc

group by
cast(timestamp_trunc(to_timestamp(tc.event_time / 1000000),''SS'') as date),
cast(substring(u_value,(instr(u_value,''U='') + 2),7) as int),
tc.campaign_id,
tc.user_id,
tc.event_time,
tc.site_id_dcm,
tc.placement_id
) as t01
group by
t01.event_time,
t01.date,
t01.user_id,
t01.campaign_id,
t01.site_id_dcm,
t01.placement_id,
t01.creative_ver_id
) as t1

left join (
select
a1.*,
rates.exchange_rate as exchange_rate
from diap01.mec_us_united_20056.dfa2_activity as a1
left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
on upper(substring(a1.other_data,(instr(a1.other_data,''u3='') + 3),3)) = upper(rates.currency)
and cast(timestamp_trunc(to_timestamp(a1.interaction_time / 1000000),''SS'') as date) = rates.date
where not regexp_like(substring(other_data,(instr(other_data,''u3='') + 3),5),''mil.*'',''ib'')
and (activity_id = 978826 or activity_id = 1086066)
and (advertiser_id <> 0)
and (length(isnull(event_sub_type,'''')) > 0)

) as ta

on t1.event_time = ta.interaction_time
and t1.user_id = ta.user_id

left join
(
select
cast(campaign as varchar(4000)) as ''campaign'',campaign_id as ''campaign_id''
from diap01.mec_us_united_20056.dfa2_campaigns
) as c1
on t1.campaign_id = c1.campaign_id

left join
(
select
cast(p1.placement as varchar(4000)) as ''placement'',p1.placement_id as ''placement_id'',p1.campaign_id as ''campaign_id'',p1.site_id_dcm as ''site_id_dcm''

from (select
campaign_id                                            as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast(placement_start_date as date) as thisdate,
row_number() over ( partition by campaign_id,site_id_dcm,placement_id
order by cast(placement_start_date as date) desc ) as x1
from diap01.mec_us_united_20056.dfa2_placements

) as p1
where x1 = 1
) as p1
on t1.placement_id = p1.placement_id
and t1.campaign_id = p1.campaign_id
and t1.site_id_dcm = p1.site_id_dcm

left join
(
select
cast(site_dcm as varchar(4000)) as ''site_dcm'',site_id_dcm as ''site_id_dcm''
from diap01.mec_us_united_20056.dfa2_sites
) as s1
on t1.site_id_dcm = s1.site_id_dcm

where regexp_like(p1.placement,''P.?'',''ib'')
and not regexp_like(p1.placement,''.?do\s?not\s?use.?'',''ib'')
and regexp_like(c1.campaign,''.*2017.*'',''ib'')
and not regexp_like(c1.campaign,''.*Search.*'',''ib'')
and not regexp_like(c1.campaign,''.*BidManager.*'',''ib'')

group by
t1.event_time,
t1.date,
t1.user_id,
c1.campaign,
t1.campaign_id,
s1.site_dcm,
t1.site_id_dcm,
p1.placement,
t1.placement_id,
t1.creative_ver_id
) as t2

group by
t2.date,
t2.campaign,
t2.campaign_id,
t2.site_dcm,
t2.site_id_dcm,
t2.placement,
t2.placement_id,
t2.creative_ver_id'
    )) as t1

    left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_FT_SUMMARY as f1
    on t1.date = f1.ftdate
    and t1.creative_ver_id = f1.creative_ver_id

group by
            t1.date,
        t1.campaign,
        t1.campaign_id,
        t1.site_dcm,
        t1.site_id_dcm,
        t1.placement,
        t1.placement_id,
        f1.creative_ver,
        t1.creative_ver_id