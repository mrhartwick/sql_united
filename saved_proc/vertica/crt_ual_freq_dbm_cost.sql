create table diap01.mec_us_united_20056.ual_freq_dbm_cost
(
user_id varchar(1000) not null,
-- dcmDate      int            not null,
-- plce_id      varchar(1000)    not null,
-- placement_id int            not null,
dbm_cost    decimal(20,10) not null,
imps        int            not null,
clicks      int            not null,
con         int             not null,
vew_con     int             not null,
clk_con     int             not null,
tix         int             not null,
vew_tix     int             not null,
clk_tix     int             not null,
rev         decimal(20,10)  not null,
vew_rev     decimal(20,10)  not null,
clk_rev     decimal(20,10)  not null
);




insert into diap01.mec_us_united_20056.ual_freq_dbm_cost
-- (user_id,dcmDate,plce_id,placement_id,dbm_cost,imps,clicks,con,tix,rev)
(user_id,dbm_cost,imps,clicks,con,vew_con,clk_con,tix,vew_tix,clk_tix,rev,vew_rev,clk_rev)

(select
  t2.user_id as user_id,
--     t2.dcmmatchdate                                     as dcmdate,
--     t2.plce_id                                          as plce_id,
--     t2.placement_id                                     as placement_id,
    sum(t2.dbm_cost)                                    as dbm_cost,
    sum(t2.imps)                                        as imps,
    sum(t2.clicks)                                      as clicks,
    isNull(sum(t2.con),cast(0 as int))                  as con,
    isNull(sum(t2.vew_con),cast(0 as int))              as vew_con,
    isNull(sum(t2.clk_con),cast(0 as int))              as clk_con,
    isNull(sum(t2.tix),cast(0 as int))                  as tix,
    isNull(sum(t2.vew_tix),cast(0 as int))              as vew_tix,
    isNull(sum(t2.clk_tix),cast(0 as int))              as clk_tix,
    isNull(sum(t2.rev),cast(0 as decimal(20,10)))       as rev,
    isNull(sum(t2.vew_rev),cast(0 as int))              as vew_rev,
    isNull(sum(t2.clk_rev),cast(0 as int))              as clk_rev
from (
select
  t1.user_id,
    t1.dcmdate,
    t1.dcmmatchdate,

--     t1.campaign,
    t1.campaign_id,
    t1.site_dcm,
    t1.site_id_dcm,
    t1.plce_id,
--     t1.lagplcnbr,
--     t1.placement,
    t1.placement_id,
         t1.site_rank,

t1.dbm_cost    as dbm_cost1,
sum(t1.dbm_cost) over (partition by t1.user_id, t1.dcmdate, t1.plce_id order by t1.user_id, t1.dcmdate, t1.plce_id, t1.site_rank desc range between unbounded preceding and current row) as dbm_cost,
t1.impressions as imps,
t1.clicks      as clicks,
t1.con         as con1,
sum(t1.con) over (partition by t1.user_id, t1.dcmdate, t1.plce_id order by t1.user_id, t1.dcmdate, t1.plce_id, t1.site_rank desc range between unbounded preceding and current row) as con,
t1.vew_con         as vew_con1,
sum(t1.vew_con) over (partition by t1.user_id, t1.dcmdate, t1.plce_id order by t1.user_id, t1.dcmdate, t1.plce_id, t1.site_rank desc range between unbounded preceding and current row) as vew_con,
t1.clk_con         as clk_con1,
sum(t1.clk_con) over (partition by t1.user_id, t1.dcmdate, t1.plce_id order by t1.user_id, t1.dcmdate, t1.plce_id, t1.site_rank desc range between unbounded preceding and current row) as clk_con,
t1.tix         as tix1,
sum(t1.tix) over (partition by t1.user_id, t1.dcmdate, t1.plce_id order by t1.user_id, t1.dcmdate, t1.plce_id, t1.site_rank desc range between unbounded preceding and current row) as tix,
t1.vew_tix         as vew_tix1,
sum(t1.vew_tix) over (partition by t1.user_id, t1.dcmdate, t1.plce_id order by t1.user_id, t1.dcmdate, t1.plce_id, t1.site_rank desc range between unbounded preceding and current row) as vew_tix,
t1.clk_tix         as clk_tix1,
sum(t1.clk_tix) over (partition by t1.user_id, t1.dcmdate, t1.plce_id order by t1.user_id, t1.dcmdate, t1.plce_id, t1.site_rank desc range between unbounded preceding and current row) as clk_tix,
t1.rev         as rev1,
sum(t1.rev) over (partition by t1.user_id, t1.dcmdate, t1.plce_id order by t1.user_id, t1.dcmdate, t1.plce_id, t1.site_rank desc range between unbounded preceding and current row) as rev,
t1.vew_rev         as vew_rev1,
sum(t1.vew_rev) over (partition by t1.user_id, t1.dcmdate, t1.plce_id order by t1.user_id, t1.dcmdate, t1.plce_id, t1.site_rank desc range between unbounded preceding and current row) as vew_rev,
t1.clk_rev         as clk_rev1,
sum(t1.clk_rev) over (partition by t1.user_id, t1.dcmdate, t1.plce_id order by t1.user_id, t1.dcmdate, t1.plce_id, t1.site_rank desc range between unbounded preceding and current row) as clk_rev


from (select
    dcmreport.user_id as user_id,
          dcmreport.dcmdate                                                                    as dcmdate,
          diap01.mec_us_united_20056.udf_dateToInt(dcmreport.dcmdate)                          as dcmmatchdate,
--           dcmreport.campaign                                                                   as campaign,
          dcmreport.campaign_id                                                                as campaign_id,
          dcmreport.site_dcm                                                                   as site_dcm,
          dcmreport.site_id_dcm                                                                as site_id_dcm,
          case when dcmreport.site_dcm like 'Google%' then 1 else 2 end as site_rank,
          dcmReport.plce_id                                                            as plce_id,
          dcmreport.placement_id                                                               as placement_id,
          sum(dcmreport.dbm_cost)                                                              as dbm_cost,
          sum(dcmreport.impressions)                                                           as impressions,
          sum(dcmreport.clicks)                                                                as clicks,
          sum(dcmreport.vew_con)                                                                  as vew_con,
          sum(dcmreport.clk_con)                                                                  as clk_con,
          sum(dcmreport.clk_con) + sum(dcmreport.vew_con)                                       as con,
          sum(dcmreport.vew_tix)                                                                   as vew_tix,
          sum(dcmreport.clk_tix)                                                                   as clk_tix,
          sum(dcmreport.clk_tix) + sum(dcmreport.vew_tix)                                       as tix,
          sum(dcmreport.vew_rev)                                                                   as vew_rev,
          sum(dcmreport.clk_rev)                                                                   as clk_rev,
          sum(dcmreport.rev)                                                                   as rev

      from (

select
  r1.user_id,
cast(r1.date as date)    as dcmdate,
-- campaign.campaign            as campaign,
r1.campaign_id           as campaign_id,
r1.site_id_dcm           as site_id_dcm,
directory.site_dcm           as site_dcm,
left(placements.placement,6) as plce_id,
-- placements.placement         as placement,
r1.placement_id          as placement_id,
sum(r1.impressions)      as impressions,
sum(r1.dbm_cost)         as dbm_cost,
sum(r1.clicks)           as clicks,
sum(r1.vew_con) as vew_con,
sum(r1.clk_con) as clk_con,
sum(r1.vew_tix) as vew_tix,
sum(r1.clk_tix) as clk_tix,
sum(cast(r1.vew_rev as decimal (10,2))) as vew_rev,
sum(cast(r1.clk_rev as decimal (10,2))) as clk_rev,
sum(cast(r1.revenue as decimal (10,2))) as rev
-- sum(r1.conv)             as conv,
-- sum(r1.tix)              as tix
from (

select
ta.user_id as user_id,
cast(timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),'SS') as date) as "date",
ta.campaign_id   as campaign_id,
ta.site_id_dcm   as site_id_dcm,
ta.placement_id  as placement_id,
0                as impressions,
0                as dbm_cost,
0      as clicks,
sum(case when ta.conversion_id = 1 then 1 else 0 end ) as clk_con,
sum(case when ta.conversion_id = 1 then ta.total_conversions else 0 end ) as clk_tix,
sum(case when ta.conversion_id = 1 then (ta.total_revenue * 1000000) / (rates.exchange_rate) else 0 end ) as clk_rev,
sum(case when ta.conversion_id = 2 then 1 else 0 end ) as vew_con,
sum(case when ta.conversion_id = 2 then ta.total_conversions else 0 end ) as vew_tix,
sum(case when ta.conversion_id = 2 then (ta.total_revenue * 1000000) / (rates.exchange_rate) else 0 end ) as vew_rev,
sum(ta.total_revenue * 1000000/rates.exchange_rate) as revenue


from
(
select *
from diap01.mec_us_united_20056.dfa2_activity
where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2016-07-15' and '2016-07-15'
and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
and (total_revenue * 1000000) <> 0
and total_conversions <> 0
and activity_id = 978826
and (advertiser_id <> 0)
and user_id <> '0'
and (campaign_id = 9639387 or campaign_id = 8958859)  -- TMK 2016
and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
-- and dbm_advertiser_id = 649134
) as ta

left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
on upper ( substring (other_data,(instr(other_data,'u3=')+3),3)) = upper (rates.currency)
and cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date ) = rates.date

group by
ta.user_id
,cast(timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),'SS') as date)
,ta.campaign_id
,ta.site_id_dcm
,ta.placement_id


union all

select
ti.user_id as user_id,
cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000),'SS') as date) as "date",
ti.campaign_id  as campaign_id,
ti.site_id_dcm  as site_id_dcm,
ti.placement_id as placement_id,
count(*)        as impressions,
cast((sum(dbm_media_cost_usd) / 1000000000) as decimal(20,10))            as dbm_cost,
0               as clicks,
0 as clk_con,
0 as clk_tix,
0 as clk_rev,
0 as vew_con,
0 as vew_tix,
0 as vew_rev,
0 as revenue


from (
select *
from diap01.mec_us_united_20056.dfa2_impression
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-07-15'
and (campaign_id = 9639387 or campaign_id = 8958859)  -- TMK 2016 & ghost campaign
and (site_id_dcm = 1578478 or site_id_dcm = 2202011)  -- real & ghost site
and user_id <> '0'
and (advertiser_id <> 0)
) as ti
group by
ti.user_id
,cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000),'SS') as date)
,ti.campaign_id
,ti.site_id_dcm
,ti.placement_id

union all

select
tc.user_id as user_id,
cast(timestamp_trunc(to_timestamp(tc.event_time / 1000000),'SS') as date) as "date",
tc.campaign_id  as campaign_id,
tc.site_id_dcm  as site_id_dcm,
tc.placement_id as placement_id,
0               as impressions,
0               as dbm_cost,
count(*)        as clicks,
0 as clk_con,
0 as clk_tix,
0 as clk_rev,
0 as vew_con,
0 as vew_tix,
0 as vew_rev,
0 as revenue

from (

select *
from diap01.mec_us_united_20056.dfa2_click
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-07-15'
and (advertiser_id <> 0)
and user_id <> '0'
and (campaign_id = 9639387 or campaign_id = 8958859)  -- TMK 2016
and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
-- and dbm_advertiser_id = 649134
) as tc

group by
tc.user_id
,cast(timestamp_trunc(to_timestamp(tc.event_time / 1000000),'SS') as date)
,tc.campaign_id
,tc.site_id_dcm
,tc.placement_id

) as r1

left join
(
select
cast(t1.placement as varchar(4000)) as 'placement',
t1.placement_id as 'placement_id',
t1.campaign_id  as 'campaign_id',
t1.site_id_dcm  as 'site_id_dcm'

from (select
campaign_id  as campaign_id,
site_id_dcm  as site_id_dcm,
placement_id as placement_id,
placement    as placement,
cast(placement_start_date as date) as thisdate,
row_number() over (partition by campaign_id,site_id_dcm,placement_id
order by cast(placement_start_date as date) desc) as r1
from diap01.mec_us_united_20056.dfa2_placements

) as t1
where r1 = 1
) as placements
on r1.placement_id = placements.placement_id
and r1.campaign_id = placements.campaign_id
and r1.site_id_dcm = placements.site_id_dcm


left join
(
select
cast(site_dcm as varchar(4000)) as 'site_dcm',
site_id_dcm                     as 'site_id_dcm'
from diap01.mec_us_united_20056.dfa2_sites
) as directory
on r1.site_id_dcm = directory.site_id_dcm

where not regexp_like(placements.placement,'.do\s*not\s*use.','ib')
-- and not regexp_like(campaign.campaign,'.2016.','ib')
and not regexp_like(placements.placement,'DBM.','ib')
and (r1.site_id_dcm = 1578478 or r1.site_id_dcm = 2202011)
group by
r1.user_id
,cast(r1.date as date)
,directory.site_dcm
,r1.site_id_dcm
,r1.campaign_id
-- ,campaign.campaign
,r1.placement_id
,placements.placement


           ) as dcmreport

group by
  dcmreport.user_id
    ,dcmreport.dcmdate
--     ,dcmreport.campaign
    ,dcmreport.campaign_id
    ,dcmreport.site_dcm
    ,dcmreport.site_id_dcm
    ,dcmreport.plce_id
--     ,dcmreport.placement
    ,dcmreport.placement_id

     ) as t1

     ) as t2
where t2.site_rank = 1
group by
  t2.user_id
--     t2.dcmmatchdate,
--     t2.campaign,
--     t2.campaign_id,
--     t2.plce_id,
--     t2.placement,
--     t2.placement_id
);
commit;