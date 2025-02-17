create procedure dbo.crt_dbm_cost_r_full
as
if OBJECT_ID('master.dbo.dbm_cost_r_full',N'U') is not null
  drop table master.dbo.dbm_cost_r_full;


create table master.dbo.dbm_cost_r_full
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
  vew_con      int            not null,
  clk_con      int            not null,
  con          int            not null,
  vew_tix      int            not null,
  clk_tix      int            not null,
  tix          int            not null,
  vew_rev      decimal(20,10) not null,
  clk_rev      decimal(20,10) not null,
  rev          decimal(20,10) not null,
  vew_led      int            not null,
  clk_led      int            not null,
  led          int            not null,
);

insert into master.dbo.dbm_cost_r_full

  select
    t3.dcmmatchdate                                   as dcmdate,
    t3.campaign                                       as campaign,
    t3.campaign_id                                    as campaign_id,
    t3.plce_id                                        as plce_id,
    t3.placement                                      as placement,
    t3.placement_id                                   as placement_id,
    sum(t3.dbm_cost)                                  as dbm_cost,
    sum(t3.imps)                                      as imps,
    sum(t3.clicks)                                    as clicks,
    isNull(sum(t3.vew_con),cast(0 as int))            as vew_con,
    isNull(sum(t3.clk_con),cast(0 as int))            as clk_con,
    isNull(sum(t3.con),cast(0 as int))                as con,
    isNull(sum(t3.vew_tix),cast(0 as int))            as vew_tix,
    isNull(sum(t3.clk_tix),cast(0 as int))            as clk_tix,
    isNull(sum(t3.tix),cast(0 as int))                as tix,
    isNull(sum(t3.vew_rev),cast(0 as decimal(20,10))) as vew_rev,
    isNull(sum(t3.clk_rev),cast(0 as decimal(20,10))) as clk_rev,
    isNull(sum(t3.rev),cast(0 as decimal(20,10)))     as rev,
    isNull(sum(t3.vew_led),cast(0 as int))            as vew_led,
    isNull(sum(t3.clk_led),cast(0 as int))            as clk_led,
    isNull(sum(t3.led),cast(0 as int))                as led

  from (
         select
           t2.dcmdate,
           t2.dcmmatchdate,
           t2.campaign,
           t2.campaign_id,
           t2.site_dcm,
           t2.site_id_dcm,
           t2.plce_id,
           t2.site_rank_1,
           t2.site_rank_2,
--     t2.lagplcnbr,
           t2.placement,
           t2.placement_id,
           t2.dbm_cost as dbm_cost2,
           sum(t2.dbm_cost) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as dbm_cost,
           t2.impressions                                                                                           as imps,
           t2.clicks                                                                                                as clicks,
           t2.vew_con                                                                                               as vew_con1,
           sum(t2.vew_con) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as vew_con,
           t2.clk_con                                                                                               as clk_con1,
           sum(t2.clk_con) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as clk_con,
           t2.con                                                                                                   as con1,
           sum(t2.con) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as con,
           t2.vew_tix                                                                                               as vew_tix1,
           sum(t2.vew_tix) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as vew_tix,
           t2.clk_tix                                                                                               as clk_tix1,
           sum(t2.clk_tix) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as clk_tix,
           t2.tix                                                                                                   as tix1,
           sum(t2.tix) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as tix,
           t2.vew_rev                                                                                               as vew_rev1,
           sum(t2.vew_rev) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as vew_rev,
           t2.clk_rev                                                                                               as clk_rev1,
           sum(t2.clk_rev) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as clk_rev,
           t2.rev                                                                                                   as rev1,
           sum(t2.rev) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as rev,

           t2.vew_led                                                                                               as vew_led1,
           sum(t2.vew_led) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as vew_led,
           t2.clk_led                                                                                               as clk_led1,
           sum(t2.clk_led) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as clk_led,
           t2.led                                                                                                   as led1,
           sum(t2.led) over (partition by t2.dcmdate,t2.plce_id
             order by t2.dcmdate asc,t2.plce_id asc,t2.site_rank_1 desc, t2.site_rank_2 desc range between unbounded preceding and current row) as led


         from (select
             t1.dcmdate                                             as dcmdate,
             cast(month(cast(t1.dcmdate as date)) as int )           as dcmmonth,
             [dbo].udf_dateToInt(t1.dcmdate)                        as dcmmatchdate,
             t1.campaign                                            as campaign,
             t1.campaign_id                                         as campaign_id,
             t1.site_dcm                                            as site_dcm,
             t1.site_id_dcm                                         as site_id_dcm,
             case when t1.site_dcm like 'Google%' or t1.site_dcm like 'DOUBLECLICK%' then 1 else 2 end as site_rank_1,
           row_number() over (partition by t1.dcmdate, t1.plce_id order by t1.placement_id) as site_rank_2,
             case when t1.plce_id in ('PBKB7J','PBKB7H','PBKB7K') then 'PBKB7J'
             else t1.plce_id end                                    as plce_id,
             prs.DV_Map                                             as DV_Map,
           case
                    when (t1.campaign_id = 10742878 OR t1.campaign_id = 8958859) AND
                    (t1.site_id_dcm = 1578478 OR t1.site_id_dcm = 2202011)
                    then 'dCPM'   else  prs.costmethod end                                              as costmethod,
             prs.Rate                                               as rate,
             case when t1.placement like 'PBKB7J%' or t1.placement like 'PBKB7H%' or t1.placement like 'PBKB7K%' or t1.placement = 'United 360 - Polaris 2016 - Q4 - Amobee' then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
             else t1.placement end                                  as placement,
             t1.placement_id                                        as placement_id,
--              sum(t1.dbm_cost)                                       as dbm_cost,
             sum(t1.dbm_tot_cost)                                   as dbm_cost,
             sum(t1.impressions)                                    as impressions,
             sum(t1.clicks)                                         as clicks,
             sum(t1.vew_led)                                        as vew_led,
             sum(t1.clk_led)                                        as clk_led,
             sum(t1.led)                                            as led,
             sum(t1.vew_con)                                        as vew_con,
             sum(t1.clk_con)                                        as clk_con,
             sum(t1.clk_con) + sum(t1.vew_con)                      as con,
             sum(t1.vew_tix)                                        as vew_tix,
             sum(t1.clk_tix)                                        as clk_tix,
             sum(t1.clk_tix) + sum(t1.vew_tix)                      as tix,
             sum(t1.vew_rev)                                        as vew_rev,
             sum(t1.clk_rev)                                        as clk_rev,
             sum(t1.rev)                                            as rev

               from (
                      select *
                      from openquery(redshift,
'
select
r1.date as dcmdate,
c1.campaign                       as campaign,
r1.campaign_id                          as campaign_id,
r1.site_id_dcm                          as site_id_dcm,
s1.site_dcm                      as site_dcm,
cast(left(p1.placement,6) as varchar(6)) as plce_id,
replace(replace(p1.placement ,'','', ''''),''"'','''') as placement,
r1.placement_id                         as placement_id,
sum(r1.impressions)                     as impressions,
sum(r1.dbm_cost)                        as dbm_cost,
sum(r1.dbm_tot_cost)                    as dbm_tot_cost,
sum(r1.clicks)                          as clicks,
sum(r1.vew_led)                         as vew_led,
sum(r1.clk_led)                         as clk_led,
sum(r1.vew_led) + sum(r1.clk_led)       as led,
sum(r1.vew_con)                         as vew_con,
sum(r1.clk_con)                         as clk_con,
sum(r1.vew_tix)                         as vew_tix,
sum(r1.clk_tix)                         as clk_tix,
sum(cast(r1.vew_rev as decimal (20,10))) as vew_rev,
sum(cast(r1.clk_rev as decimal (20,10))) as clk_rev,
sum(cast(r1.rev as decimal (20,10)))     as rev
from (

select
ta.md_interaction_date_loc as "date",
ta.campaign_id   as campaign_id,
ta.site_id_dcm   as site_id_dcm,
ta.placement_id  as placement_id,
0                as impressions,
0                as dbm_cost,
0                as dbm_tot_cost,
0      as clicks,
sum(case when activity_id = 1086066 and ta.conversion_id = 1 then 1 else 0 end) as clk_led,
sum(case when activity_id = 978826 and ta.conversion_id = 1 and ta.total_revenue <> 0 then 1 else 0 end ) as clk_con,
sum(case when activity_id = 978826 and ta.conversion_id = 1 and ta.total_revenue <> 0 then ta.total_conversions else 0 end ) as clk_tix,
sum(case when regexp_instr(substring(other_data,(regexp_instr(other_data,''u3\\='') + 3),3),''Mil.*'') = 0 and ta.conversion_id = 1 then cast(((ta.total_revenue * 1000000) / (rates.exchange_rate)) as decimal(20,10))
          when regexp_instr(substring(other_data,(regexp_instr(other_data,''u3\\='') + 3),3),''Mil.*'') > 0 and ta.conversion_id = 1 then cast(((ta.total_revenue*1000)/.0103) as decimal(20,10)) else 0 end ) as clk_rev,
sum(case when activity_id = 1086066 and ta.conversion_id = 2 then 1 else 0 end) as vew_led,
sum(case when activity_id = 978826  and ta.conversion_id = 2 and ta.total_revenue <> 0 then 1 else 0 end ) as vew_con,
sum(case when activity_id = 978826  and ta.conversion_id = 2 and ta.total_revenue <> 0 then ta.total_conversions else 0 end ) as vew_tix,
sum(case when regexp_instr(substring(other_data,(regexp_instr(other_data,''u3\\='') + 3),3),''Mil.*'') = 0 and ta.conversion_id = 2 then cast(((ta.total_revenue * 1000000) / (rates.exchange_rate)) as decimal(20,10))
          when regexp_instr(substring(other_data,(regexp_instr(other_data,''u3\\='') + 3),3),''Mil.*'') > 0 and ta.conversion_id = 2 then cast(((ta.total_revenue*1000)/.0103) as decimal(20,10)) else 0 end ) as vew_rev,
sum(case when regexp_instr(substring(other_data,(regexp_instr(other_data,''u3\\='') + 3),3),''Mil.*'') = 0 then cast(((ta.total_revenue * 1000000) / (rates.exchange_rate)) as decimal(20,10))
          when regexp_instr(substring(other_data,(regexp_instr(other_data,''u3\\='') + 3),3),''Mil.*'') > 0 then cast(((ta.total_revenue*1000)/.0103) as decimal(20,10)) end) as rev

from
(
select *
from wmprodfeeds.united.dfa2_activity
-- where md_interaction_date_loc > ''2018-01-01''
and (activity_id = 978826 or activity_id = 1086066)
and (advertiser_id <> 0)
and (site_id_dcm = 1578478 or site_id_dcm = 2202011 or site_id_dcm = 3266673)
and (length(isnull(event_sub_type,'''')) > 0)
) as ta

left join wmprodfeeds.exchangerates.exchange_rates as rates
on upper(substring(other_data,(regexp_instr(other_data,''u3\\='') + 3),3)) = upper(rates.currency)
and md_event_date_loc = rates.date

group by
 ta.md_interaction_date_loc
,ta.campaign_id
,ta.site_id_dcm
,ta.placement_id


union all

select
ti.md_event_date_loc as "date",
ti.campaign_id  as campaign_id,
ti.site_id_dcm  as site_id_dcm,
ti.placement_id as placement_id,
count(*)        as impressions,
cast((sum(dbm_media_cost_usd) / 1000000000) as decimal(20,10))            as dbm_cost,
cast((sum(dbm_total_media_cost_usd) / 1000000000) as decimal(20,10))            as dbm_tot_cost,
0               as clicks,
0 as clk_led,
0 as clk_con,
0 as clk_tix,
0 as clk_rev,
0 as vew_led,
0 as vew_con,
0 as vew_tix,
0 as vew_rev,
0 as rev
-- 0               as con,
-- 0               as tix


from (
select *
from wmprodfeeds.united.dfa2_impression
-- where md_event_date_loc > ''2018-01-01''
and (site_id_dcm = 1578478 or site_id_dcm = 2202011 or site_id_dcm = 3266673)
and (advertiser_id <> 0)
) as ti
group by
 ti.md_event_date_loc
,ti.campaign_id
,ti.site_id_dcm
,ti.placement_id

union all

select
tc.md_event_date_loc as "date",
tc.campaign_id  as campaign_id,
tc.site_id_dcm  as site_id_dcm,
tc.placement_id as placement_id,
0               as impressions,
0               as dbm_cost,
0                as dbm_tot_cost,
count(*)        as clicks,
0 as clk_led,
0 as clk_con,
0 as clk_tix,
0 as clk_rev,
0 as vew_led,
0 as vew_con,
0 as vew_tix,
0 as vew_rev,
0 as rev
-- 0               as con,
-- 0               as tix

from (

select *
from wmprodfeeds.united.dfa2_click
-- where md_event_date_loc > ''2018-01-01''
and (advertiser_id <> 0)
and (site_id_dcm = 1578478 or site_id_dcm = 2202011 or site_id_dcm = 3266673)
-- and dbm_advertiser_id = 649134
) as tc

group by
 tc.md_event_date_loc
,tc.campaign_id
,tc.site_id_dcm
,tc.placement_id

) as r1

left join
(
select cast (campaign as varchar (4000)) as campaign,campaign_id as campaign_id
from wmprodfeeds.united.dfa2_campaigns
) as c1
on r1.campaign_id = c1.campaign_id


left join
(
select cast (placement as varchar (4000)) as placement,placement_id as placement_id
from wmprodfeeds.united.dfa2_placements
) as p1
on r1.placement_id = p1.placement_id


left join
(
select cast (site_dcm as varchar (4000)) as site_dcm,site_id_dcm as site_id_dcm
from wmprodfeeds.united.dfa2_sites
) as s1
on r1.site_id_dcm = s1.site_id_dcm

where
    regexp_instr(p1.placement,''.?do\s?not\s?use.?'') =0
and regexp_instr(c1.campaign,''.*Search.*'') = 0
and (r1.site_id_dcm = 1578478 or r1.site_id_dcm = 2202011 or r1.site_id_dcm = 3266673)

group by
 r1.date
,s1.site_dcm
,r1.site_id_dcm
,r1.campaign_id
,c1.campaign
,r1.placement_id
,p1.placement
')

                    ) as t1

                       left join
      (
        select *
        from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].prs_summ
      ) as prs
--         on t1.placement_id = prs.adserverplacementid
        on t1.plce_id = prs.placementnumber


         group by
           t1.dcmdate
           ,cast(month(cast(t1.dcmdate as date)) as int)
           ,t1.campaign
           ,t1.campaign_id
           ,t1.site_dcm
           ,prs.DV_Map
           ,prs.costmethod
           ,prs.Rate
           ,t1.site_id_dcm
           ,t1.plce_id
           ,t1.placement
           ,t1.placement_id

              ) as t2
where t2.costmethod like '[Dd][Cc][Pp][Mm]'
--    where t2.placement like '%CN_Retargeting%'
--   where t2.site_rank_2 = 1
       ) as t3
where t3.site_rank_2 = 1
--


  group by
    t3.dcmmatchdate,
    t3.campaign,
    t3.campaign_id,
    t3.plce_id,
    t3.placement,
    t3.placement_id
go
