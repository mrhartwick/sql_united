CREATE procedure dbo.crt_dfa_flatCost_dt2_r_full
as
if OBJECT_ID('master.dbo.dfa_flatCost_dt2_r_full',N'U') is not null
    drop table master.dbo.dfa_flatCost_dt2_r_full;


create table master.dbo.dfa_flatCost_dt2_r_full
(
    Cost_ID        nvarchar(10)    not null,
    dcmDate        int            not null,
    dcmYrMo        int            not null,
    prsCostMethod  nvarchar(100)  not null,
    PackageCat     nvarchar(100)  not null,
    prsRate        decimal(20,10) not null,
    prsStYrMo      int            not null,
    prsEdYrMo      int            not null,
    prsStDate      int            not null,
    prsEdDate      int            not null,
    diff           bigint         not null,
    flatCost       decimal(20,10) not null,
    lagCost        decimal(20,10) not null,
    lagRemain      decimal(20,10) not null,
    flatCostRunTot decimal(20,10) not null,
    flatCostRemain decimal(20,10) not null,
    Imps           int            not null,
    impsRunTot     int            not null,
    impsRemain     int            not null,
    Planned_Amt    int            not null

);

insert into master.dbo.dfa_flatCost_dt2_r_full


    select
        f3.Cost_ID                                          as Cost_ID,
        f3.dcmDate                                          as dcmDate,
        f3.dcmYrMo                                          as dcmYrMo,
        f3.prsCostMethod                                    as prsCostMethod,
        f3.PackageCat                                       as PackageCat,
        f3.prsRate                                          as prsRate,
        f3.prsStYrMo                                        as prsStYrMo,
        f3.prsEdYrMo                                        as prsEdYrMo,
        f3.stDate                                           as prsStDate,
        f3.edDate                                           as prsEdDate,
        isNull(f3.diff,cast(0 as int))                      as diff,

        case
        when f3.dcmDate = f3.stDate and f3.stDate = f3.edDate then f3.flatCost
        -- if there are days left in the flight, and cost/impressions
        -- don't exceed planned amounts, then flatCost
        when f3.diff > 0 and f3.flatCost < f3.prsRate and f3.impsRunTot < f3.Planned_Amt then f3.flatCost
        -- if there are days left in the flight, and previous day's
        -- a.) cost/b.) remaining cost are zero, then flatCost
        when f3.diff > 0 and f3.lagCost = 0 and f3.lagRemain = 0 then f3.flatCost
        -- if there are days left in the flight, and a.) current day's remaining cost
        -- is zero and b.) there is remaining cost, then remaining cost
        when f3.diff > 0 and f3.flatCostRemain = 0 and f3.lagRemain > 0 then f3.lagRemain
        when f3.flatCost = 0 then 0
        when f3.flatCost > f3.lagRemain then f3.lagRemain
        else f3.flatCost - f3.lagRemain end                 as flatCost,

        isNull(f3.lagCost,cast(0 as decimal(20,10)))        as lagCost,
        isNull(f3.lagRemain,cast(0 as decimal(20,10)))      as lagRemain,
        isNull(f3.flatCostRunTot,cast(0 as decimal(20,10))) as flatCostRunTot,
        isNull(f3.flatCostRemain,cast(0 as decimal(20,10))) as flatCostRemain,
        f3.Imps                                             as Imps,
        f3.impsRunTot                                       as impsRunTot,
        isNull(f3.impsRemain,cast(0 as int))                as impsRemain,
        isNull(f3.Planned_Amt,cast(0 as int))               as Planned_Amt

    from (
             select
                 f2.stDate                                              as stDate,
                 f2.edDate                                              as edDate,
                 f2.dcmDate                                             as dcmDate,
                 f2.Cost_ID                                             as Cost_ID,
                 f2.dcmYrMo                                             as dcmYrMo,
                 f2.prsCostMethod               as prsCostMethod,
                 f2.PackageCat                                          as PackageCat,
                 f2.prsRate                                             as prsRate,
                 f2.prsStYrMo                                           as prsStYrMo,
                 f2.prsEdYrMo                                           as prsEdYrMo,
                 isNull(f2.diff,cast(0 as decimal(20,10)))              as diff,

                 case when f2.flatCost is null then cast(0 as decimal(20,10))
                 -- if there are days left in the flight and Impressions in current row exceed planned Impressions
                 when f2.diff > 0 and f2.Imps > f2.Planned_Amt
                     -- then subtract previous day's cost from current day's cost
                     then isNull(f2.flatCost,cast(0 as decimal(20,10))) - isNull(f2.lagCost,cast(0 as decimal(20,10)))
                 -- otherwise, flatCost
                 else isNull(f2.flatCost,cast(0 as decimal(20,10))) end as flatCost,

                 isNull(f2.lagCost,cast(0 as decimal(20,10)))           as lagCost,
                 --               remaining flat fee from previous day
                 lag(isNull(f2.flatCostRemain,cast(0 as decimal(20,10))),1,0) over (partition by f2.Cost_ID
                     order by f2.dcmYrMo)                               as lagRemain,
                 isNull(f2.flatCostRunTot,cast(0 as decimal(20,10)))    as flatCostRunTot,
                 isNull(f2.flatCostRemain,cast(0 as decimal(20,10)))    as flatCostRemain,
                 isNull(f2.Imps,cast(0 as int))                         as Imps,
                 isNull(f2.impsRunTot,cast(0 as int))                   as impsRunTot,
                 isNull(f2.impsRemain,cast(0 as int))                   as impsRemain,
                 isNull(f2.Planned_Amt,cast(0 as int))                  as Planned_Amt
             from (
                      select
                          f1.stDate                                                                      as stDate,
                          f1.edDate                                                                      as edDate,
                          f1.dcmDate                                                                     as dcmDate,
                          f1.Cost_ID                                                                     as Cost_ID,
                          f1.dcmYrMo                                                                     as dcmYrMo,
                          f1.Cost_Method                                                                 as prsCostMethod,
                          f1.PackageCat                                                                  as PackageCat,
                          f1.Rate                                                                        as prsRate,
                          f1.stYrMo                                                                      as prsStYrMo,
                          f1.edYrMo                                                                      as prsEdYrMo,
                          isNull(f1.ed_diff,cast(0 as decimal(20,10)))                                   as diff,
                          -- using OVER clause to get sums aggregated by specific, limited dimensions
                          -- (instead of by all dimensions, as with GROUP)
                          sum(f1.flatCost) over (partition by f1.Cost_ID,f1.dcmDate)                     as flatCost,
                          -- lag() gives us data from a previous row in the result set; here, for each subsequent row
                          -- ranged within each Cost_ID, we retrieve flatCost for the previous day
                          lag(f1.flatcost,1,0) over (partition by f1.Cost_ID
                              order by f1.dcmDate)                                                       as lagCost,
                    -- this field not strictly necessary in this block, as it's duplicated in flatCostRemain;
                          -- but used in logic of f3; this is a running total of the flatCost field
                          sum(f1.flatCost) over (
                          partition by f1.Cost_ID                -- grouped by Cost_ID
                              order by f1.dcmDate asc -- sorted by dcmDate in ascending order (oldest date first)
                          range between -- the window (upper and lower bounds, by row, for Cost_ID) being
                          unbounded preceding and current row -- the very top of the result set and the current row
                              -- range arguments are the default, but written out for clarity
                          )                                                                              as flatCostRunTot,

                          -- the "remaining" cost for each Cost_ID/day, expressed as the difference between
                          -- planned cost and a running total of the flatCost field
                          case
                          when (cast(f1.Rate as decimal) - sum(f1.flatCost) over (partition by f1.Cost_ID
                              order by f1.dcmDate asc range between unbounded preceding and current row)) <= 0
                              then 0
                          else (cast(f1.Rate as decimal) - sum(f1.flatCost) over (partition by f1.Cost_ID
                              order by f1.dcmDate asc range between unbounded preceding and current row))
                          end                                                                            as flatCostRemain,

                          -- first and LAST time we sum Impressions, here grouped by Cost_ID and dcmDate
                          -- using OVER clause to get sums aggregated by specific, limited dimensions
                          -- (instead of by all dimensions, as with GROUP)
                          sum(f1.impressions) over (partition by f1.Cost_ID,f1.dcmDate)                  as Imps,
                          -- running total of the flatCost field
                          sum(f1.impressions) over (partition by f1.Cost_ID
                              order by
                                  f1.dcmDate asc range between unbounded preceding and current row)      as impsRunTot,
                          -- the "remaining" Impressions for each Cost_ID/day, expressed as the difference between
                          -- planned impressions and a running total of the Impressions field
                          case
                          when (cast(f1.Planned_Amt as decimal) - sum(f1.impressions) over (partition by f1.Cost_ID
                              order by f1.dcmDate asc range between unbounded preceding and current row)) <= 0 then 0
                          else (cast(f1.Planned_Amt as decimal) - sum(f1.impressions) over (partition by f1.Cost_ID
                              order by
                                  f1.dcmDate asc range between unbounded preceding and current row)) end as impsRemain,
                          f1.Planned_Amt                                                                 as Planned_Amt
                      from
                          (
                              select
                                  f0.dcmmonth         as dcmmonth,
                                  f0.dcmDate                                       as dcmDate,
                                  f0.dcmyear                                       as dcmyear,
                                  f0.dcmYrMo                                       as dcmYrMo,
                                  f0.Cost_ID                                       as Cost_ID,
                                  f0.cost_method                                   as cost_method,
                                  -- the first round of flat fee logic is simple incremental cost, and sets the stage for subsequent calculations
                                  --
                                  -- the second round (f3; final select, above) relies on additional columns created in f1 and f2 (directly above)
                                  case
                                  -- if traffic date occurs before package start date, then no cost
                                  when f0.dcmDate - f0.stDate < 0 then 0
                                  -- if traffic date is equal to package start date AND end date (one-day flight),
                                  -- then charge the full rate; this calculation doesn't get more granular than DAY
                                  when f0.edDate = f0.dcmDate and f0.edDate = f0.dcmDate then f0.rate
                                  -- if traffic date occurs before package end date (there are days left in the flight)
                                  -- BUT Impressions delivery exceeds the planned amount, then charge the full rate;
                                  -- this has the effect of zeroing out cost for all future delivery
                                  when (f0.edDate - f0.dcmDate) > 0
                                      and sum(f0.impressions) > f0.planned_amt then f0.rate
                                  -- otherwise, if traffic date occurs before package end date (there are days left in the flight), then
                                  -- 1.) take % of delivered Impressions (for that package, on that day) to total planned impressions
                                  -- 2.) apply that % to total planned cost to yield an incremental flat fee cost
                                  when (f0.edDate - f0.dcmDate) > 0
                                      then sum((cast(f0.impressions as decimal(20,10)) /
                                          NULLIF(cast(f0.planned_amt as decimal(20,10)),0)) *
                                                   cast(f0.rate as decimal(20,10)))
                                  -- if traffic date is equal to package end date, then charge the full rate;
                                  -- this clause appears at the end of the statement because we want other, more nuanced conditions
                                  -- to be evaluated first
                                  when (f0.edDate - f0.dcmDate) = 0 then f0.rate
                                  -- if traffic date occurs after package end date, then no cost
                                  when (f0.edDate - f0.dcmDate) < 0 then 0
                                  else 0 end                                       as flatCost,
                                  f0.rate                                          as rate,
                                  f0.PackageCat                                    as PackageCat,
                                  f0.stYrMo                                        as stYrMo,
                                  f0.edYrMo                                        as edYrMo,
                                  f0.stDate                                        as stDate,
                                  f0.edDate                                        as edDate,
                                  -- difference (in days) between traffic date and end date of package
                                  cast(f0.edDate as int) - cast(f0.dcmDate as int) as ed_diff,
                                  f0.planned_amt                                   as planned_amt,
                                  case when f0.dcmDate - f0.stDate < 0 then 0
                                  else sum(f0.impressions)     end                         as impressions
                              from
                                  (
                                      select
                                          Prisma.adserverPlacementID                 as adserverPlacementID,
                                          r1.plce_id                  as plce_id,
                                          r1.placement                   as placement,
                                          MONTH(cast(r1.dcmDate as date))     as dcmMonth,
                                          YEAR(cast(r1.dcmDate as date))      as dcmYear,
                                          [dbo].udf_yrmoToInt(r1.dcmDate)     as dcmYrMo,
                                          [dbo].udf_dateToInt(r1.dcmDate)     as dcmDate,
                                          r1.site_dcm,
                                          Prisma.CostMethod                          as Cost_Method,
                                          Prisma.Cost_ID                             as Cost_ID,
                                          Prisma.Rate                                as Rate,
                                          Prisma.PackageCat                          as PackageCat,
                                          Prisma.stYrMo                              as stYrMo,
                                          Prisma.edYrMo                              as edYrMo,
                                          [dbo].udf_dateToInt(Prisma.PlacementStart) as stDate,
                                          [dbo].udf_dateToInt(Prisma.PlacementEnd)   as edDate,
                                          SUM(r1.Impressions)                 as impressions,
                                          SUM(r1.clicks)                      as clicks,
                                          Prisma.Planned_Amt                         as Planned_Amt

                                      from (
                                               --
                                               select *
                                               from openQuery(redshift,
'
select
r1.date    as dcmdate,
cast(date_part(month,(r1.date)) as int) as reportmonth,
c1.campaign            as campaign,
r1.campaign_id           as campaign_id,
r1.site_id_dcm           as site_id_dcm,
s1.site_dcm           as site_dcm,
cast(left(p1.placement,6) as varchar(6)) as plce_id,
replace(replace(p1.placement ,'','', ''''),''"'','''') as placement,
r1.placement_id          as placement_id,
sum(r1.impressions)      as impressions,
sum(r1.clicks)           as clicks

from (

select
 ti.md_event_date_loc as "date"
,ti.campaign_id  as campaign_id
,ti.site_id_dcm  as site_id_dcm
,ti.placement_id as placement_id
,count(*)        as impressions
,0               as clicks

from  (
select *
from wmprodfeeds.united.dfa2_impression
-- where md_event_date_loc > ''2018-01-01''
and (advertiser_id <> 0)
) as ti

group by
 ti.md_event_date_loc
,ti.campaign_id
,ti.site_id_dcm
,ti.placement_id

union all

select
 tc.md_event_date_loc as "date"
,tc.campaign_id  as campaign_id
,tc.site_id_dcm  as site_id_dcm
,tc.placement_id as placement_id
,0               as impressions
,count(*)        as clicks

from  (
select *
from wmprodfeeds.united.dfa2_click
-- where md_event_date_loc > ''2018-01-01''
and (advertiser_id <> 0)
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

group by
 cast(r1.date as date)
,s1.site_dcm
,r1.site_id_dcm
,r1.campaign_id
,c1.campaign
,r1.placement_id
,p1.placement

')

                                           ) as r1
                                          -- =========================================================================================================================
                                          left join
                                          (
                                              select *
                                              from [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.prs_summ

                                          ) as Prisma
                                              on r1.Placement_ID = Prisma.AdserverPlacementId

                                      where Prisma.CostMethod = 'Flat'
                                      and r1.campaign not like '%Search%'
                                      and r1.campaign not like '%[_]UK[_]%'
                                      and r1.campaign not like '%2016%'
                                      and r1.campaign not like '%2015%'
                                      and r1.campaign_id != 10698273  -- UK Acquisition 2017
                                      and r1.campaign_id != 11221036  -- Hong Kong 2017

-- =========================================================================================================================
                                      group by
                                          cast(r1.dcmDate as date)
                                          ,r1.plce_id
                                          ,Prisma.CostMethod
                                          ,[dbo].udf_dateToInt(r1.dcmDate)
                                          ,[dbo].udf_yrmoToInt(r1.dcmDate)
                                          ,Prisma.stYrMo
                                          ,Prisma.edYrMo
                                          ,Prisma.PlacementStart
                                          ,Prisma.PlacementEnd
                                          ,Prisma.Rate
                                          ,Prisma.Planned_Amt
                                          ,Prisma.PackageCat
                                          ,Prisma.adserverPlacementID
                                          ,r1.site_dcm
                                          ,r1.placement
                                          ,Prisma.Cost_ID
                                  ) as f0

                              group by
                                  f0.dcmmonth,
                                  f0.dcmyear,
                                  f0.dcmYrMo,
                                  f0.PackageCat,
                                  f0.cost_method,
                                  f0.edDate,
                                  f0.stDate,
                                  f0.dcmDate,
                                  f0.rate,
                                  f0.stYrMo,
                                  f0.edYrMo,
                                  f0.Cost_ID,
                                  f0.planned_amt
                          ) as f1
                  ) as f2
where (len(isnull(f2.Cost_ID,'')) != 0)
         ) as f3
go
