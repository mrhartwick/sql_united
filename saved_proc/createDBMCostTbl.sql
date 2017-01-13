select
    t1.dcmdate,
    t1.dcmmonth,
    t1.dcmmatchdate,
    t1.campaign,
    t1.campaign_id,
    t1.site_dcm,
    t1.site_id_dcm,
    t1.plce_id,
    t1.lagplcnbr,
    t1.placement,
    t1.placement_id,
    sum(t1.lag_dbm_cost)                as dbm_cost,
    sum(t1.impressions)                 as imps,
    sum(t1.clicks)                      as clicks,
    sum(t1.conv) + sum(t1.lag_dbm_conv) as conv,
    sum(t1.tix) + sum(t1.lag_dbm_tix)   as tix


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
--                      partition by dcmreport.dcmdate
              order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lagplcnbr,

          case when dcmReport.placement like 'PBKB7J%' or dcmReport.placement like 'PBKB7H%' or dcmReport.placement like 'PBKB7K%' or dcmReport.placement = 'United 360 - Polaris 2016 - Q4 - Amobee' then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
          else dcmReport.placement end                                                         as placement,
          dcmreport.placement_id                                                               as placement_id,
--        amobee video 360 placements, tracked differently across dcm, innovid, and moat; this combines the three placements into one
          sum(dcmreport.dbm_cost)                                                              as dbm_cost,
          lag(sum(dcmreport.dbm_cost),1,0) over (
--           partition by dcmreport.dcmdate
              order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lag_dbm_cost,
          sum(dcmreport.impressions)                                                           as impressions,
          sum(dcmreport.clicks)                                                                as clicks,
          sum(dcmreport.conv)                                                                  as conv,
          lag(sum(dcmreport.conv),1,0) over (
--                      partition by dcmreport.dcmdate
              order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lag_dbm_conv,
          sum(dcmreport.tix)                                                                   as tix,
          lag(sum(dcmreport.tix),1,0) over (
--                      partition by dcmreport.dcmdate
              order by dcmreport.dcmdate asc,dcmreport.plce_id asc,dcmreport.site_id_dcm desc) as lag_dbm_tix

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
sum(report.conv)             as conv,
sum(report.tix)              as tix

from (

select
cast(timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),''SS'') as date) as "date",
ta.campaign_id   as campaign_id,
ta.site_id_dcm   as site_id_dcm,
ta.placement_id  as placement_id,
0                as impressions,
0                as dbm_cost,
0                as clicks,
sum(case when ta.conversion_id = 1 or ta.conversion_id = 2 then 1
else 0 end)      as conv,
sum(case when ta.conversion_id = 1 or ta.conversion_id = 2 then ta.total_conversions
else 0 end)      as tix

from
(
select *
from diap01.mec_us_united_20056.dfa2_activity
where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),''SS'') as date) > ''2017-01-01''
and upper(substring(other_data,(instr(other_data,''u3='') + 3),3)) != ''mil''
and substring(other_data,(instr(other_data,''u3='') + 3),5) != ''miles''
and total_revenue != 0
and total_conversions != 0
and activity_id = 978826
and (advertiser_id <> 0)
and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
) as ta


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
0               as conv,
0               as tix


from (
select *
from diap01.mec_us_united_20056.dfa2_impression
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),''SS'') as date) > ''2017-01-01''
-- where cast(timestamp_trunc(to_timestamp(event_time / 1000000), ''SS'') as date) between ''2017-01-01'' and ''2017-01-08''
and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
and (advertiser_id <> 0)
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
0               as conv,
0               as tix

from (

select *
from diap01.mec_us_united_20056.dfa2_click
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),''SS'') as date) > ''2017-01-01''
-- where cast(timestamp_trunc(to_timestamp(event_time / 1000000), ''SS'') as date) between ''2017-01-01'' and ''2017-01-08''
and (advertiser_id <> 0)
and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
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
-- , placements.plce_id
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
--      ,prisma.packagecat
--      ,prisma.rate
--      ,prisma.costmethod
--      ,prisma.cost_id
--      ,prisma.planned_amt
--      ,prisma.planned_cost
--      ,prisma.placementend
--      ,prisma.placementstart
--      ,prisma.stdate
--      ,prisma.eddate
--      ,prisma.dv_map

     ) as t1

where t1.site_id_dcm = 1578478

group by

    t1.dcmdate,
    t1.dcmmonth,
    t1.dcmmatchdate,
    t1.campaign,
    t1.campaign_id,
    t1.site_dcm,
    t1.site_id_dcm,
    t1.plce_id,
    t1.lagplcnbr,
    t1.placement,
    t1.placement_id