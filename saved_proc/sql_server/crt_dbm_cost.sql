alter procedure dbo.crt_dbm_cost
as
if OBJECT_ID('master.dbo.dbm_cost',N'U') is not null
    drop table master.dbo.dbm_cost;


create table master.dbo.dbm_cost
(
dcmDate      int            not null,
campaign     nvarchar(4000) not null,
campaign_id  int            not null,
plce_id      nvarchar(6)    not null,
placement    nvarchar(4000) not null,
placement_id int            not null,
dbm_cost     decimal(20,10) not null,
imps         int            not null,
clicks       int            not null,
vew_con     int             not null,
clk_con     int             not null,
con         int             not null,
vew_tix     int             not null,
clk_tix     int             not null,
tix         int             not null,
vew_rev     decimal(20,10)  not null,
clk_rev     decimal(20,10)  not null,
rev         decimal(20,10)  not null
);

insert into master.dbo.dbm_cost

select
    t2.dcmmatchdate                                     as dcmdate,
    t2.campaign                                         as campaign,
    t2.campaign_id                                      as campaign_id,
    t2.plce_id                                          as plce_id,
    t2.placement                                        as placement,
    t2.placement_id                                     as placement_id,
    sum(t2.dbm_cost)                                    as dbm_cost,
    sum(t2.imps)                                        as imps,
    sum(t2.clicks)                                      as clicks,
    isNull(sum(t2.vew_con),cast(0 as int))              as vew_con,
    isNull(sum(t2.clk_con),cast(0 as int))              as clk_con,
    isNull(sum(t2.con),cast(0 as int))                  as con,
    isNull(sum(t2.vew_tix) ,cast(0 as int))             as vew_tix,
    isNull(sum(t2.clk_tix) ,cast(0 as int))             as clk_tix,
    isNull(sum(t2.tix),cast(0 as int))                  as tix,
    isNull(sum(t2.vew_rev) ,cast(0 as decimal(20,10)))  as vew_rev,
    isNull(sum(t2.clk_rev) ,cast(0 as decimal(20,10)))  as clk_rev,
    isNull(sum(t2.rev)     ,cast(0 as decimal(20,10)))  as rev

from (
select
    t1.dcmdate,
    t1.dcmmatchdate,
    t1.campaign,
    t1.campaign_id,
    t1.site_dcm,
    t1.site_id_dcm,
    t1.plce_id,
--     t1.lagplcnbr,
    t1.placement,
    t1.placement_id,
    sum(t1.lag_dbm_cost)                                                          as dbm_cost,
    sum(t1.impressions)                                                           as imps,
    sum(t1.clicks)                                                                as clicks,
    sum(t1.vew_con) + sum(t1.lag_vew_con)                                         as vew_con,
    sum(t1.clk_con) + sum(t1.lag_clk_con)                                         as clk_con,
    sum(t1.vew_con) + sum(t1.lag_vew_con) + sum(t1.clk_con) + sum(t1.lag_clk_con) as con,
    sum(t1.vew_tix) + sum(t1.lag_vew_tix)                                         as vew_tix,
    sum(t1.clk_tix) + sum(t1.lag_clk_tix)                                         as clk_tix,
    sum(t1.vew_tix) + sum(t1.lag_vew_tix) + sum(t1.clk_tix) + sum(t1.lag_clk_tix) as tix,
    sum(t1.vew_rev) + sum(t1.lag_vew_rev)                                         as vew_rev,
    sum(t1.clk_rev) + sum(t1.lag_clk_rev)                                         as clk_rev,
    sum(t1.rev) + sum(t1.lag_rev)                                                 as rev


from (select
          dcmreport.dcmdate                                                                    as dcmdate,
          cast(month(cast(dcmreport.dcmdate as date)) as int)                                  as dcmmonth,
          [dbo].udf_dateToInt(dcmreport.dcmdate)                                               as dcmmatchdate,
          dcmreport.campaign                                                                   as campaign,
          dcmreport.campaign_id                                                                as campaign_id,
          dcmreport.site_dcm                                                                   as site_dcm,
          dcmreport.site_id_dcm                                                                as site_id_dcm,
          case when dcmReport.plce_id in ('PBKB7J','PBKB7H','PBKB7K') then 'PBKB7J'
          else dcmReport.plce_id end                                                           as plce_id,
          lag(dcmReport.plce_id,1,0) over (
              order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lagplcnbr,
          case when dcmReport.placement like 'PBKB7J%' or dcmReport.placement like 'PBKB7H%' or dcmReport.placement like 'PBKB7K%' or dcmReport.placement = 'United 360 - Polaris 2016 - Q4 - Amobee' then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
          else dcmReport.placement end                                                         as placement,
          dcmreport.placement_id                                                               as placement_id,
          sum(dcmreport.dbm_cost)                                                              as dbm_cost,
          lag(sum(dcmreport.dbm_cost),1,0) over ( order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lag_dbm_cost,
          sum(dcmreport.impressions)                                                           as impressions,
          sum(dcmreport.clicks)                                                                as clicks,
          sum(dcmreport.vew_conv)                                                                  as vew_con,
          lag(sum(dcmreport.vew_conv),1,0) over ( order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lag_vew_con,
          sum(dcmreport.clk_conv)                                                                  as clk_con,
          lag(sum(dcmreport.clk_conv),1,0) over ( order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lag_clk_con,
          sum(dcmreport.vew_tix)                                                                   as vew_tix,
          lag(sum(dcmreport.vew_tix),1,0) over ( order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lag_vew_tix,
          sum(dcmreport.clk_tix)                                                                   as clk_tix,
          lag(sum(dcmreport.clk_tix),1,0) over ( order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lag_clk_tix,
          sum(dcmreport.vew_rev)                                                                   as vew_rev,
          lag(sum(dcmreport.vew_rev),1,0) over ( order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lag_vew_rev,
          sum(dcmreport.clk_rev)                                                                   as clk_rev,
          lag(sum(dcmreport.clk_rev),1,0) over ( order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lag_clk_rev,
          sum(dcmreport.rev)                                                                   as rev,
          lag(sum(dcmreport.rev),1,0) over ( order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lag_rev

      -- openquery function call must not exceed 8,000 characters; no room for comments inside the function
      from (
               select *
               from openquery(verticaunited,'
select
cast(report.date as date)    as dcmdate,
campaign.campaign            as campaign,
report.campaign_id           as campaign_id,
report.site_id_dcm           as site_id_dcm,
directory.site_dcm           as site_dcm,
left(placements.placement,6) as plce_id,
placements.placement         as placement,
report.placement_id          as placement_id,
sum(report.impressions)      as impressions,
sum(report.dbm_cost)         as dbm_cost,
sum(report.clicks)           as clicks,
sum(report.vew_conv) as vew_conv,
sum(report.clk_conv) as clk_conv,
sum(report.vew_tix) as vew_tix,
sum(report.clk_tix) as clk_tix,
sum(cast(report.vew_rev as decimal (10,2))) as vew_rev,
sum(cast(report.clk_rev as decimal (10,2))) as clk_rev,
sum(cast(report.revenue as decimal (10,2))) as rev
-- sum(report.conv)             as conv,
-- sum(report.tix)              as tix
from (

select
cast(timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),''SS'') as date) as "date",
ta.campaign_id   as campaign_id,
ta.site_id_dcm   as site_id_dcm,
ta.placement_id  as placement_id,
0                as impressions,
0                as dbm_cost,
0      as clicks,
sum(case when ta.conversion_id = 1 then 1 else 0 end ) as clk_conv,
sum(case when ta.conversion_id = 1 then ta.total_conversions else 0 end ) as clk_tix,
sum(case when ta.conversion_id = 1 then (ta.total_revenue * 1000000) / (rates.exchange_rate) else 0 end ) as clk_rev,
sum(case when ta.conversion_id = 2 then 1 else 0 end ) as vew_conv,
sum(case when ta.conversion_id = 2 then ta.total_conversions else 0 end ) as vew_tix,
sum(case when ta.conversion_id = 2 then (ta.total_revenue * 1000000) / (rates.exchange_rate) else 0 end ) as vew_rev,
sum(ta.total_revenue * 1000000/rates.exchange_rate) as revenue
-- sum(case when ta.conversion_id = 1 or ta.conversion_id = 2 then 1
-- else 0 end)      as conv,
-- sum(case when ta.conversion_id = 1 or ta.conversion_id = 2 then ta.total_conversions
-- else 0 end)      as tix

from
(
select *
from diap01.mec_us_united_20056.dfa2_activity
where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),''SS'') as date) > ''2016-07-15''
and not regexp_like(substring(other_data,(instr(other_data,''u3='') + 3),5),''mil.*'',''ib'')
and total_revenue != 0
and total_conversions != 0
and activity_id = 978826
and (advertiser_id <> 0)
and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
-- and dbm_advertiser_id = 649134
) as ta

left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
on upper ( substring (other_data,(instr(other_data,''u3='')+3),3)) = upper (rates.currency)
and cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),''SS'') as date ) = rates.date

group by
cast(timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),''SS'') as date)
,ta.campaign_id
,ta.site_id_dcm
,ta.placement_id


union all

select
cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000),''SS'') as date) as "date",
ti.campaign_id  as campaign_id,
ti.site_id_dcm  as site_id_dcm,
ti.placement_id as placement_id,
count(*)        as impressions,
cast((sum(dbm_media_cost_usd) / 1000000000) as decimal(20,10))            as dbm_cost,
0               as clicks,
0 as clk_conv,
0 as clk_tix,
0 as clk_rev,
0 as vew_conv,
0 as vew_tix,
0 as vew_rev,
0 as revenue
-- 0               as conv,
-- 0               as tix


from (
select *
from diap01.mec_us_united_20056.dfa2_impression
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),''SS'') as date) > ''2016-07-15''
and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
and (advertiser_id <> 0)
-- and dbm_advertiser_id = 649134
) as ti
group by
cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000),''SS'') as date)
,ti.campaign_id
,ti.site_id_dcm
,ti.placement_id

union all

select
cast(timestamp_trunc(to_timestamp(tc.event_time / 1000000),''SS'') as date) as "date",
tc.campaign_id  as campaign_id,
tc.site_id_dcm  as site_id_dcm,
tc.placement_id as placement_id,
0               as impressions,
0               as dbm_cost,
count(*)        as clicks,
0 as clk_conv,
0 as clk_tix,
0 as clk_rev,
0 as vew_conv,
0 as vew_tix,
0 as vew_rev,
0 as revenue
-- 0               as conv,
-- 0               as tix

from (

select *
from diap01.mec_us_united_20056.dfa2_click
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),''SS'') as date) > ''2016-07-15''
and (advertiser_id <> 0)
and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
-- and dbm_advertiser_id = 649134
) as tc

group by
cast(timestamp_trunc(to_timestamp(tc.event_time / 1000000),''SS'') as date)
,tc.campaign_id
,tc.site_id_dcm
,tc.placement_id

) as report

left join
(
select
cast(campaign as varchar(4000)) as ''campaign'',
campaign_id                     as ''campaign_id''
from diap01.mec_us_united_20056.dfa2_campaigns
) as campaign
on report.campaign_id = campaign.campaign_id

left join
(
select
cast(t1.placement as varchar(4000)) as ''placement'',
t1.placement_id as ''placement_id'',
t1.campaign_id  as ''campaign_id'',
t1.site_id_dcm  as ''site_id_dcm''

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
on report.placement_id = placements.placement_id
and report.campaign_id = placements.campaign_id
and report.site_id_dcm = placements.site_id_dcm

left join
(
select
cast(site_dcm as varchar(4000)) as ''site_dcm'',
site_id_dcm                     as ''site_id_dcm''
from diap01.mec_us_united_20056.dfa2_sites
) as directory
on report.site_id_dcm = directory.site_id_dcm

where not regexp_like(placements.placement,''.do\s*not\s*use.'',''ib'')
and not regexp_like(campaign.campaign,''.2016.'',''ib'')
and not regexp_like(placements.placement,''DBM.'',''ib'')
and (report.site_id_dcm = 1578478 or report.site_id_dcm = 2202011)
group by
cast(report.date as date)
-- , cast(month(cast(report.date as date)) as int)
,directory.site_dcm
,report.site_id_dcm
,report.campaign_id
,campaign.campaign
,report.placement_id
,placements.placement
')

           ) as dcmreport


-- left join
-- (select * from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].summarytable) as prisma
-- on dcmreport.placement_id = prisma.adserverplacementid
-- where prisma.costmethod != 'Flat'
-- and prisma.cost_id = 'P8FSSK'

group by
    dcmreport.dcmdate
    ,cast(month(cast(dcmreport.dcmdate as date)) as int)
    ,dcmreport.campaign
    ,dcmreport.campaign_id
    ,dcmreport.site_dcm
    ,dcmreport.site_id_dcm
    ,dcmreport.plce_id
    ,dcmreport.placement
    ,dcmreport.placement_id

     ) as t1

where t1.site_id_dcm = 1578478

group by
    t1.dcmdate,
    t1.dcmmatchdate,
    t1.campaign,
    t1.campaign_id,
    t1.site_dcm,
    t1.site_id_dcm,
    t1.plce_id,
    t1.placement,
    t1.placement_id

     ) as t2

group by
    t2.dcmmatchdate,
    t2.campaign,
    t2.campaign_id,
    t2.plce_id,
    t2.placement,
    t2.placement_id