-- table to hold daily cost
alter procedure dbo.createCostTblDT1
as
if OBJECT_ID('master.dbo.costTable_dt1',N'U') is not null
    drop table master.dbo.costTable_dt1;


create table master.dbo.costTable_dt1
(
    cost_id        nvarchar(6)    not null,
    plce_id        nvarchar(6)    not null,
    dcmDate        int            not null,
    prsCostMethod  nvarchar(100)  not null,
    PackageCat     nvarchar(100)  not null,
    prsRate        decimal(20,10) not null,
    prsStDate      int            not null,
    prsEdDate      int            not null,
    diff           bigint         not null,
    cost       decimal(20,10) not null,
    lagCost        decimal(20,10) not null,
    lagCostRemain      decimal(20,10) not null,
    CostRunTot decimal(20,10) not null,
    CostRemain decimal(20,10) not null,
    Imps           int            not null,
    impsRunTot     int            not null,
    impsRemain     int            not null,
    planned_amt    int            not null,
    planned_cost decimal(20,10) not null

);

insert into master.dbo.costTable_dt1

    select
        f3.cost_id                                          as cost_id,
        f3.plce_id as plce_id,
        f3.dcmDate                                          as dcmDate,
        f3.prsCostMethod                                    as prsCostMethod,
        f3.PackageCat                                       as PackageCat,
        f3.prsRate                                          as prsRate,
        f3.stDate                                           as prsStDate,
        f3.edDate                                           as prsEdDate,
        isNull(f3.diff,cast(0 as int))                      as diff,

        case
--         when f3.dcmDate = f3.stDate and f3.stDate = f3.edDate then f3.cost
        -- if there are days left in the flight, and cost/impressions
        -- don't exceed planned amounts, then cost
        when f3.diff > 0 and f3.cost < f3.planned_cost and f3.impsRunTot < f3.planned_amt then f3.cost
        -- if there are days left in the flight, and previous day's
        -- a.) cost/b.) remaining cost are zero, then cost
        when f3.diff > 0 and f3.lagCost = 0 and f3.lagCostRemain = 0 then f3.cost
        -- if there are days left in the flight, and a.) current day's remaining cost
        -- is zero and b.) there is remaining cost, then remaining cost
        when f3.diff > 0 and f3.costRemain = 0 and f3.lagCostRemain > 0 then f3.lagCostRemain
        when f3.cost = 0 then 0
        when f3.cost > f3.lagCostRemain then f3.lagCostRemain
        else f3.cost - f3.lagCostRemain end                 as cost,

        isNull(f3.lagCost,cast(0 as decimal(20,10)))        as lagCost,

        isNull(f3.costRunTot,cast(0 as decimal(20,10))) as costRunTot,
        isNull(f3.costRemain,cast(0 as decimal(20,10))) as costRemain,
        isNull(f3.lagCostRemain,cast(0 as decimal(20,10)))      as lagCostRemain,
        f3.Imps                                             as Imps,
        f3.impsRunTot                                       as impsRunTot,
        isNull(f3.impsRemain,cast(0 as int))                as impsRemain,
        isNull(f3.planned_amt,cast(0 as int))               as planned_amt,
        isNull(f3.planned_cost,cast(0 as int))               as planned_cost

    from (
             select
                 f2.stDate                                              as stDate,
                 f2.edDate                                              as edDate,
                 f2.dcmDate                                             as dcmDate,
                 f2.cost_id                                             as cost_id,
                 f2.plce_id as plce_id,
--                  f2.dcmYrMo                                             as dcmYrMo,
                 f2.prsCostMethod                                       as prsCostMethod,
                 f2.PackageCat                                          as PackageCat,
                 f2.prsRate                                             as prsRate,
--                  f2.prsStYrMo                                           as prsStYrMo,
--                  f2.prsEdYrMo                                           as prsEdYrMo,
                 isNull(f2.diff,cast(0 as decimal(20,10)))              as diff,

                 case when f2.cost is null then cast(0 as decimal(20,10))
                 -- if there are days left in the flight and Impressions in current row exceed planned Impressions
                 when f2.diff > 0 and f2.Imps > f2.planned_amt
                     -- then subtract previous day's cost from current day's cost
                     then isNull(f2.cost,cast(0 as decimal(20,10))) - isNull(f2.lagCost,cast(0 as decimal(20,10)))
                 -- otherwise, cost
                 else isNull(f2.cost,cast(0 as decimal(20,10))) end as cost,

                 isNull(f2.lagCost,cast(0 as decimal(20,10)))           as lagCost,

                 isNull(f2.costRunTot,cast(0 as decimal(20,10)))    as costRunTot,
                 isNull(f2.costRemain,cast(0 as decimal(20,10)))    as costRemain,
                 --               remaining flat fee from previous day
                 lag(isNull(f2.costRemain,cast(0 as decimal(20,10))),1,0) over (partition by f2.cost_id
                     order by
                         f2.dcmDate,plce_id)                                    as lagCostRemain,
                 isNull(f2.Imps,cast(0 as int))                         as Imps,
                 isNull(f2.impsRunTot,cast(0 as int))                   as impsRunTot,
                 isNull(f2.impsRemain,cast(0 as int))                   as impsRemain,
                 isNull(f2.planned_amt,cast(0 as int))                  as planned_amt,
             isNull(f2.planned_cost,cast(0 as int))                  as planned_cost
             from (
                      select
                          f1.dcmDate                                                                     as dcmDate,
                          f1.stDate                                                                      as stDate,
                          f1.edDate                                                                      as edDate,

                          f1.cost_id                                                                     as cost_id,
                          f1.plce_id                                                                     as plce_id,
--                           f1.dcmYrMo                                                                     as dcmYrMo,
                          f1.costmethod                                                                 as prsCostMethod,
                          f1.PackageCat                                                                  as PackageCat,
                          f1.Rate                                                                        as prsRate,
--                           f1.stYrMo                                                                      as prsStYrMo,
--                           f1.edYrMo                                                                      as prsEdYrMo,
                          isNull(f1.ed_diff,cast(0 as decimal(20,10)))                                   as diff,
                          -- using OVER clause to get sums aggregated by specific, limited dimensions
                          -- (instead of by all dimensions, as with GROUP)
                          sum(f1.cost) over (partition by f1.cost_id, f1.plce_id ,f1.dcmDate)                     as cost,
                          -- lag() gives us data from a previous row in the result set; here, for each subsequent row
                          -- ranged within each cost_id, we retrieve cost for the previous day
                          lag(f1.cost,1,0) over (partition by f1.cost_id
                              order by f1.dcmDate, f1.plce_id)                                                       as lagCost,
                          -- this field not strictly necessary in this block, as it's duplicated in costRemain;
                          -- but used in logic of f3; this is a running total of the cost field
                          sum(f1.cost) over (
                          partition by f1.cost_id                -- grouped by cost_id
                              order by f1.dcmDate, f1.plce_id asc -- sorted by dcmDate in ascending order (oldest date first)
                          range between -- the window (upper and lower bounds, by row, for cost_id) being
                          unbounded preceding and current row -- the very top of the result set and the current row
                              -- range arguments are the default, but written out for clarity
                          )                                                                              as costRunTot,

                          -- the "remaining" cost for each cost_id/day, expressed as the difference between
                          -- planned cost and a running total of the cost field
                          case
                          when (cast(f1.planned_cost as decimal) - sum(f1.cost) over (partition by f1.cost_id
                              order by f1.dcmDate, f1.plce_id asc range between unbounded preceding and current row)) <= 0
                              then 0
                          else (cast(f1.planned_cost as decimal) - sum(f1.cost) over (partition by f1.cost_id
                              order by f1.dcmDate, f1.plce_id asc range between unbounded preceding and current row))
                          end                                                                            as costRemain,

                          -- old version of this statement, written without explicit range arguments
                          --              case
                          --              when (cast(f1.Rate as decimal) - sum(f1.cost) over (partition by f1.cost_id order by f1.dcmDate asc)) <= 0
                          --                then 0
                          --                           else (cast(f1.Rate as decimal) - sum(f1.cost) over (partition by f1.cost_id order by f1.dcmDate asc))
                          --              end as costRemain,

                          -- first and LAST time we sum Impressions, here grouped by cost_id and dcmDate
                          -- using OVER clause to get sums aggregated by specific, limited dimensions
                          -- (instead of by all dimensions, as with GROUP)
                          sum(f1.billimps) over (partition by f1.cost_id, f1.plce_id, f1.dcmDate)                  as Imps,
                          -- running total of the flatCost field
                          sum(f1.billimps) over (partition by f1.cost_id
                              order by
                                  f1.dcmDate, f1.plce_id asc range between unbounded preceding and current row)      as impsRunTot,
                          -- the "remaining" Impressions for each cost_id/day, expressed as the difference between
                          -- planned impressions and a running total of the Impressions field
                          case
                          when (cast(f1.planned_amt as decimal) - sum(f1.billimps) over (partition by f1.cost_id
                              order by
                                  f1.dcmDate, f1.plce_id asc range between unbounded preceding and current row)) <= 0 then 0
                          else (cast(f1.planned_amt as decimal) - sum(f1.billimps) over (partition by f1.cost_id
                              order by
                                  f1.dcmDate, f1.plce_id asc range between unbounded preceding and current row)) end as impsRemain,
                          f1.planned_amt                                                                 as planned_amt,
                          f1.planned_cost as planned_cost
                      from
                          (
                              select
                                  f0.dcmmonth                                      as dcmmonth,
                                  f0.dcmDate                                       as dcmDate,
--                                   f0.dcmyear                                       as dcmyear,
--                                   f0.dcmYrMo                                       as dcmYrMo,
                                  f0.cost_id                                       as cost_id,
                                  f0.plce_id as plce_id,
                                  f0.costmethod                                   as costmethod,
                                  -- the first round of flat fee logic is simple incremental cost, and sets the stage for subsequent calculations
                                  --
                                  -- the second round (f3; final select, above) relies on additional columns created in f1 and f2 (directly above)
                                  case
                                  -- if traffic date occurs before package start date, then no cost
                                  when f0.dcmDate - f0.stDate < 0 then 0
                                  -- if traffic date is equal to package start date AND end date (one-day flight),
                                  -- then charge the full rate; this calculation doesn't get more granular than DAY
--                                   DOES NOT APPLY
--                                   when f0.edDate = f0.dcmDate and f0.edDate = f0.dcmDate then f0.rate
                                  -- if traffic date occurs before package end date (there are days left in the flight)
                                  -- BUT Impressions delivery exceeds the planned amount, then charge the full rate;
                                  -- this has the effect of zeroing out cost for all future delivery
                                  when (f0.edDate - f0.dcmDate) > 0
                                      and sum(f0.billimps) > f0.planned_amt then f0.planned_cost
                                  -- otherwise, if traffic date occurs before package end date (there are days left in the flight), then
                                  -- 1.) take % of delivered Impressions (for that package, on that day) to total planned impressions
                                  -- 2.) apply that % to total planned cost to yield an incremental flat fee cost
                                  when (f0.edDate - f0.dcmDate) > 0
                                      then sum(f0.cost)
                                  -- if traffic date is equal to package end date, then charge the full rate;
                                  -- this clause appears at the end of the statement because we want other, more nuanced conditions
                                  -- to be evaluated first
--                                   DOES NOT APPLY
--                                   when (f0.edDate - f0.dcmDate) = 0 then f0.rate
                                  -- if traffic date occurs after package end date, then no cost
                                  when (f0.edDate - f0.dcmDate) < 0 then 0
                                  else 0 end                                       as cost,
                                  f0.rate                                          as rate,
                                  f0.PackageCat                                    as PackageCat,
--                                   f0.stYrMo                                        as stYrMo,
--                                   f0.edYrMo                                        as edYrMo,
                                  f0.stDate                                        as stDate,
                                  f0.edDate                                        as edDate,
                                  -- difference (in days) between traffic date and end date of package
                                  cast(f0.edDate as int) - cast(f0.dcmDate as int) as ed_diff,
                                  f0.planned_cost as planned_cost,
                                  f0.planned_amt                                   as planned_amt,
                                  case when f0.dcmDate - f0.stDate < 0 then 0
                                  else sum(f0.billimps)     end                         as billimps
                              from
                                  (


select

    [dbo].udf_dateToInt(t2.dcmdate)         as dcmdate,
    t2.dcmmonth        as dcmmonth,
    t2.diff            as diff,
    t2.packagecat      as packagecat,
    t2.cost_id         as cost_id,
    t2.costmethod      as costmethod,
    t2.placementnumber as plce_id,
    t2.placement_id    as placement_id,
    [dbo].udf_dateToInt(t2.placementend)    as edDate,
    [dbo].udf_dateToInt(t2.placementstart)  as stDate,
    t2.dv_map          as dv_map,
    t2.planned_amt     as planned_amt,
    t2.planned_cost    as planned_cost,
    t2.cost            as cost,
    t2.rate            as rate,
    t2.dlvrimps        as dlvrimps,
    t2.billimps        as billimps,
    t2.dfa_imps        as dfa_imps,
    t2.iv_imps         as iv_imps,
    t2.dv_imps         as dv_imps,
    t2.mt_imps         as mt_imps

-- ============================================================================================================================================

from (
--
--
-- declare @report_st date,
-- @report_ed date;
-- -- --
-- set @report_ed = '2017-01-31';
-- set @report_st = '2017-01-01';

         select
             cast(t1.dcmdate as date)                                                   as dcmdate,
             t1.dcmmonth                                                                as dcmmonth,
             t1.diff                                                                    as diff,
             dv.joinkey                                                                 as dvjoinkey,
             mt.joinkey                                                                 as mtjoinkey,
             iv.joinkey                                                                 as ivjoinkey,
             t1.packagecat                                                              as packagecat,
             t1.cost_id                                                                 as cost_id,
             t1.campaign                                                                as campaign,
             t1.campaign_id                                                             as campaign_id,
--          t1.site_dcm                                                      as site_dcm,
--          t1.site_id_dcm                                                             as site_id_dcm,
             t1.costmethod                                                              as costmethod,
             sum(1) over (partition by t1.cost_id,t1.placementnumber
                 order by
                     t1.dcmmonth asc range between unbounded preceding and current row) as newcount,
             t1.placementnumber                                                         as placementnumber,
             t1.placement                                                               as placement,
             t1.placement_id                                                            as placement_id,
             t1.placementend                                                            as placementend,
             t1.placementstart                                                          as placementstart,
             t1.dv_map                                                                  as dv_map,
             t1.planned_amt                                                             as planned_amt,
             t1.planned_cost                                                            as planned_cost,
-- flat.flatcost as flatcost,
--  logic excludes flat fees
             case
             --       zeros out cost for placements traffic before specified start date or after specified end date
             when ((t1.dv_map = 'N' or t1.dv_map = 'Y') and (t1.eddate - t1.dcmmatchdate < 0 or t1.dcmmatchdate - t1.stdate < 0)
                 and (t1.costmethod = 'CPM' or t1.costmethod = 'CPMV' or t1.costmethod = 'CPE' or t1.costmethod = 'CPC' or
                 t1.costmethod = 'CPCV'))
                 then cast(0 as decimal(20,10))
             --       click source innovid
             when ((t1.dv_map = 'Y' or t1.dv_map = 'N') and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0)
                 and (t1.costmethod = 'CPC' or t1.costmethod = 'CPCV') and (len(isnull(iv.joinkey,'')) > 0))
                 then cast((sum(cast(iv.click_thrus as decimal(20,10))) * cast(t1.rate as decimal(20,10))) as
                           decimal(20,10))
             --       click source dcm
             when ((t1.dv_map = 'Y' or t1.dv_map = 'N') and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0)
                 and (t1.costmethod = 'CPC' or t1.costmethod = 'CPCV'))
                 then cast((sum(cast(t1.clicks as decimal(20,10))) * cast(t1.rate as decimal(20,10))) as decimal(20,10))

             --           impression-based cost; not subject to viewability; innovid source
             when (t1.dv_map = 'N' and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0) and
                 (t1.costmethod = 'CPM' or t1.costmethod = 'CPMV' or t1.costmethod = 'CPE') and (len(
                 isnull(iv.joinkey,'')) > 0))
                 then cast((sum(cast(iv.impressions as decimal(20,10))) * cast(t1.rate as decimal(20,10))) / 1000 as
                           decimal(20,10))

             --           impression-based cost; not subject to viewability; dcm source
             when (t1.dv_map = 'N' and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0) and
                 (t1.costmethod = 'CPM' or t1.costmethod = 'CPMV' or t1.costmethod = 'CPE'))
                 then cast((sum(cast(t1.impressions as decimal(20,10))) * cast(t1.rate as decimal(20,10))) / 1000 as
                           decimal(20,10))

             --           impression-based cost; subject to viewability with flag; mt source
             when (t1.dv_map = 'Y' and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0) and
                 (t1.costmethod = 'CPM' or t1.costmethod = 'CPMV' or t1.costmethod = 'CPE') and mt.joinkey is not null)
                 then cast((sum(cast(mt.groupm_billable_impressions as decimal(20,10))) * cast(t1.rate as
                                                                                               decimal(20,10))) / 1000
                           as decimal(20,10))

             --           impression-based cost; subject to viewability; dv source
             when (t1.dv_map = 'Y' and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0) and
                 (t1.costmethod = 'CPM' or t1.costmethod = 'CPMV' or t1.costmethod = 'CPE'))
                 then cast((sum(cast(dv.groupm_billable_impressions as decimal(20,10))) * cast(t1.rate as
                                                                                               decimal(20,10))) / 1000
                           as decimal(20,10))

             --           impression-based cost; subject to viewability; moat source
             when (t1.dv_map = 'M' and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0) and
                 (t1.costmethod = 'CPM' or t1.costmethod = 'CPMV' or t1.costmethod = 'CPE'))
                 then cast((sum(cast(mt.groupm_billable_impressions as decimal(20,10))) * cast(t1.rate as
                                                                                               decimal(20,10))) / 1000
                           as decimal(20,10))

             else cast(0 as decimal(20,10)) end                                         as cost,
             t1.rate                                                                    as rate,
             sum(t1.incrflatcost)                                                       as incrflatcost,
--       total impressions as reported by 1.) dcm for "n," 2.) dv for "y," or moat for "m"
             sum(case
                 when t1.dv_map = 'Y' and (len(isnull(mt.joinkey,'')) > 0) then mt.total_impressions
                 when t1.dv_map = 'Y' then dv.total_impressions
                 when t1.dv_map = 'M' then mt.total_impressions
                 when t1.dv_map = 'N' and (len(isnull(iv.joinkey,'')) > 0) then iv.impressions
                 else t1.impressions end)                                               as dlvrimps,

--       billable impressions as reported by 1.) dcm for "n," 2.) dv for "y," or moat for "m"
             sum(case
                 when t1.dv_map = 'Y' and (len(isnull(mt.joinkey,'')) > 0) then mt.groupm_billable_impressions
                 when t1.dv_map = 'Y' then dv.groupm_billable_impressions
                 when t1.dv_map = 'M' then mt.groupm_billable_impressions
                 when t1.dv_map = 'N' and (len(isnull(iv.joinkey,'')) > 0) then iv.impressions
                 else t1.impressions end)                                               as billimps,

--       dcm impressions (for comparison (qa) to dcm console)
             sum(t1.impressions)                                                        as dfa_imps,
             sum(iv.impressions)                                                        as iv_imps,
             sum(iv.click_thrus)                                                        as iv_clicks,
             sum(iv.all_completion)                                                     as iv_completes,
             sum(cast(dv.total_impressions as int))                                     as dv_imps,
             sum(dv.groupm_passed_impressions)                                          as dv_viewed,
             sum(cast(dv.groupm_billable_impressions as decimal(20,10)))                as dv_groupmpayable,
             sum(cast(mt.total_impressions as int))                                     as mt_imps,
             sum(mt.groupm_passed_impressions)                                          as mt_viewed,
             sum(cast(mt.groupm_billable_impressions as decimal(20,10)))                as mt_groupmpayable,
--       clicks
             sum(case
                 when (len(isnull(iv.joinkey,'')) > 0) then iv.click_thrus
                 else t1.clicks end)                                                    as clicks
--              sum(t1.conv)                                                               as conv,
--              sum(t1.tix)                                                                as tix


         from
             (
-- =========================================================================================================================

                 select
                     dcmreport.dcmdate                                                          as dcmdate,
                     cast(month(cast(dcmreport.dcmdate as date)) as int)                        as dcmmonth,
                     [dbo].udf_dateToInt(dcmreport.dcmdate)                                     as dcmmatchdate,
                     dcmreport.campaign                                                         as campaign,
                     dcmreport.campaign_id                                                      as campaign_id,
                     dcmreport.site_dcm                                                         as site_dcm,
                     dcmreport.site_id_dcm                                                      as site_id_dcm,
                     case when dcmReport.PlacementNumber in ('PBKB7J','PBKB7H','PBKB7K') then 'PBKB7J'
                     else dcmReport.PlacementNumber end                                         as PlacementNumber,

                     case when dcmReport.placement like 'PBKB7J%' or dcmReport.placement like 'PBKB7H%' or dcmReport.placement like 'PBKB7K%' or dcmReport.placement = 'United 360 - Polaris 2016 - Q4 - Amobee' then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
                     else dcmReport.placement end                                               as placement,
--        amobee video 360 placements, tracked differently across dcm, innovid, and moat; this combines the three placements into one
                     case when dcmreport.placement_id in (137412510,137412401,137412609) then 137412609
                     else dcmreport.placement_id end                                            as placement_id,
                     prisma.stdate                                                              as stdate,
                     case when dcmreport.campaign_id = 9923634 and dcmreport.site_id_dcm != 1190258 then 20161022
                     else
                         prisma.eddate end                                                      as eddate,
                     prisma.packagecat                                                          as packagecat,
                     prisma.costmethod                                                          as costmethod,
                     prisma.cost_id                                                             as cost_id,
                     prisma.planned_amt                                                         as planned_amt,
                     prisma.planned_cost                                                        as planned_cost,
                     prisma.placementstart                                                      as placementstart,
                     case when dcmreport.campaign_id = 9923634 and dcmreport.site_id_dcm != 1190258 then '2016-10-22'
                     else
                         prisma.placementend end                                                as placementend,

                     sum((cast(dcmreport.impressions as decimal(20,10)) / nullif(
                         cast(prisma.planned_amt as decimal(20,10)),0)) * cast(prisma.rate as
                                                                               decimal(20,10))) as incrflatcost,
                     cast(prisma.rate as decimal(20,10))                                        as rate,
                     sum(dcmreport.impressions)                                                 as impressions,
                     sum(dcmreport.clicks)                                                      as clicks,
--                      sum(dcmreport.conv)                                                        as conv,
--                      sum(dcmreport.tix)                                                         as tix,

                     case when cast(month(prisma.placementend) as int) - cast(month(cast(dcmreport.dcmdate as date)) as
                                                                              int) <= 0 then 0
                     else cast(month(prisma.placementend) as int) - cast(month(cast(dcmreport.dcmdate as date)) as
                                                                         int) end               as diff,

                     case
                     when prisma.costmethod = 'Flat' or prisma.costmethod = 'CPC' or prisma.costmethod = 'CPCV' or prisma.costmethod = 'DCPM'
                         then 'N'

                     --           live intent for sfo-sin campaign is email (not subject to viewab.), but mistakenly labeled with "y"
                     when
                         dcmreport.campaign_id = '9923634' -- sfo-sin
                             and dcmreport.site_id_dcm = '1853564' -- live intent
                         then 'N'

                     --           corrections to sme placements
                     when dcmreport.campaign_id = '10090315' and (dcmreport.site_id_dcm = '1513807' or dcmreport.site_id_dcm = '1592652')
                         then 'Y'

                     --           corrections to sfo-sin placements
                     when dcmreport.campaign_id = '9923634' and dcmreport.site_id_dcm = '1534879' and prisma.costmethod = 'CPM'
                         then 'N'
                     --           designates all xaxis placements as "y." not always true.
                     --            when dcmreport.site_id_dcm = '1592652' then 'Y'

                     --           flipboard unable to implement moat tags; must bill off of dfa impressions
                     when dcmreport.site_id_dcm = '2937979' then 'N'
                     --           all targeted marketing subject
                     when dcmreport.campaign_id = '9639387' then 'Y'

                     --           designates all sfo-akl placements as "y." not always true. apparently.
                     --             when dcmreport.campaign_id = '9973506' then 'Y'

                     when Prisma.CostMethod = 'CPMV' and
                         (dcmReport.placement like '%[Mm][Oo][Bb][Ii][Ll][Ee]%' or dcmReport.placement like '%[Vv][Ii][Dd][Ee][Oo]%' or dcmReport.placement like '%[Pp][Rr][Ee]%[Rr][Oo][Ll][Ll]%' or dcmReport.site_id_dcm = '1995643'
-- 							or dcmReport.site_id_dcm = '1474576'
                             or dcmReport.site_id_dcm = '2854118')
                         then 'M'
                     -- 					 Look for viewability flags Investment began including in placement names 6/16.
                     when dcmReport.placement like '%[_]DV[_]%' then 'Y'
                     when dcmReport.placement like '%[_]MOAT[_]%' then 'M'
                     when dcmReport.placement like '%[_]NA[_]%' then 'N'
                     --
                     when Prisma.CostMethod = 'CPMV' and Prisma.DV_Map = 'N' then 'Y'
                     else Prisma.DV_Map end                                                     as DV_Map

                 --           prisma.dv_map as dv_map,


                 -- ==========================================================================================================================================================

                 -- openquery function call must not exceed 8,000 characters; no room for comments inside the function
from (
select *
from openquery(verticaunited,
'SELECT
cast(Report.Date AS DATE)                   	as dcmdate,
cast(month(cast(Report.Date as date)) as int) 	as reportmonth,
Campaign.Buy                                	as campaign,
Report.order_id                               	as campaign_id,
Report.Site_ID 									as site_id_dcm,
Directory.Directory_Site                    	as site_dcm,
	left(Placements.Site_Placement,6) 			as ''placementnumber'',
Placements.Site_Placement                   	as placement,
Report.page_id                         			as placement_id,
sum(Report.Impressions)                     AS impressions,
sum(Report.Clicks)                          AS clicks,
sum(Report.View_Thru_Conv)                  AS View_Thru_Conv,
sum(Report.Click_Thru_Conv)                 AS Click_Thru_Conv,
sum(Report.View_Thru_Tickets)               AS View_Thru_Tickets,
sum(Report.Click_Thru_Tickets)              AS Click_Thru_Tickets,
sum(cast(Report.View_Thru_Revenue AS DECIMAL(10, 2)))                   AS View_Thru_Revenue,
sum(cast(Report.Click_Thru_Revenue AS DECIMAL(10, 2)))                   AS Click_Thru_Revenue,
sum(cast(Report.Revenue AS DECIMAL(10, 2))) AS revenue
from (
SELECT

cast(Conversions.Click_Time as date) as "Date"
,order_id                                                                       as order_id
,Conversions.site_id                                                                        as Site_ID
,Conversions.page_id                                                                        as page_id
,0                                                                                          as Impressions
,0                                                                                          as Clicks
,sum(Case When Event_ID = 1 THEN 1 ELSE 0 END)                                              as Click_Thru_Conv
,sum(Case When Event_ID = 1 Then Conversions.Quantity Else 0 End)                           as Click_Thru_Tickets
,sum(Case When Event_ID = 1 Then (Conversions.Revenue) / (Rates.exchange_rate) Else 0 End)  as Click_Thru_Revenue
,sum(Case When Event_ID = 2 THEN 1 ELSE 0 END)                                              as View_Thru_Conv
,sum(Case When Event_ID = 2 Then Conversions.Quantity Else 0 End)                           as View_Thru_Tickets
,sum(Case When Event_ID = 2 Then (Conversions.Revenue) / (Rates.exchange_rate) Else 0 End)  as View_Thru_Revenue
,sum(Conversions.Revenue/Rates.exchange_rate)                                               as Revenue

from
(
SELECT *
FROM diap01.mec_us_united_20056.dfa_activity
WHERE (cast(Click_Time as date) BETWEEN ''2016-01-01'' AND ''2016-12-31'')
and UPPER(SUBSTRING(Other_Data, (INSTR(Other_Data,''u3='')+3), 3)) != ''MIL''
and SUBSTRING(Other_Data, (INSTR(Other_Data,''u3='')+3), 5) != ''Miles''
and revenue != 0
and quantity != 0
AND (Activity_Type = ''ticke498'')
AND (Activity_Sub_Type = ''unite820'')
and order_id in (9304728, 9407915, 9408733, 9548151, 9630239, 9639387, 9739006, 9923634, 9973506, 9994694, 9999841, 10094548, 10276123, 10121649, 10307468, 10090315, 10505745) -- Display 2016

and (advertiser_id <> 0)
) as Conversions

LEFT JOIN diap01.mec_us_mecexchangerates_20067.EXCHANGE_RATES AS Rates
ON UPPER(SUBSTRING(Other_Data, (INSTR(Other_Data,''u3='')+3), 3)) = UPPER(Rates.Currency)
AND cast(Conversions.Click_Time as date) = Rates.DATE

GROUP BY
-- Conversions.Click_Time
cast(Conversions.Click_Time as date)
,Conversions.order_id
,Conversions.site_id
,Conversions.page_id


UNION ALL

SELECT
-- Impressions.impression_time as "Date"
cast(Impressions.impression_time as date) as "Date"
,Impressions.order_ID                 as order_id
,Impressions.Site_ID                  as Site_ID
,Impressions.Page_ID                  as page_id
,count(*)                             as Impressions
,0                                    as Clicks
,0                                    as Click_Thru_Conv
,0                                    as Click_Thru_Tickets
,0                                    as Click_Thru_Revenue
,0                                    as View_Thru_Conv
,0                                    as View_Thru_Tickets
,0                                    as View_Thru_Revenue
,0                                    as Revenue

FROM  (
SELECT *
FROM diap01.mec_us_united_20056.dfa_impression
WHERE cast(impression_time as date) BETWEEN ''2016-01-01'' AND ''2016-12-31''
and order_id in (9304728, 9407915, 9408733, 9548151, 9630239, 9639387, 9739006, 9923634, 9973506, 9994694, 9999841, 10094548, 10276123, 10121649, 10307468, 10090315, 10505745) -- Display 2016

and (advertiser_id <> 0)
) AS Impressions
GROUP BY
-- Impressions.impression_time
cast(Impressions.impression_time as date)
,Impressions.order_ID
,Impressions.Site_ID
,Impressions.Page_ID

UNION ALL
SELECT
-- Clicks.click_time       as "Date"
cast(Clicks.click_time as date)       as "Date"
,Clicks.order_id                      as order_id
,Clicks.Site_ID                       as Site_ID
,Clicks.Page_ID                       as page_id
,0                                    as Impressions
,count(*)                             as Clicks
,0                                    as Click_Thru_Conv
,0                                    as Click_Thru_Tickets
,0                                    as Click_Thru_Revenue
,0                                    as View_Thru_Conv
,0                                    as View_Thru_Tickets
,0                                    as View_Thru_Revenue
,0                                    as Revenue

FROM  (

SELECT *
FROM diap01.mec_us_united_20056.dfa_click
WHERE cast(click_time as date) BETWEEN ''2016-01-01'' AND ''2016-12-31''
and order_id in (9304728, 9407915, 9408733, 9548151, 9630239, 9639387, 9739006, 9923634, 9973506, 9994694, 9999841, 10094548, 10276123, 10121649, 10307468, 10090315, 10505745) -- Display 2016
and (advertiser_id <> 0)
) AS Clicks

GROUP BY
-- Clicks.Click_time
cast(Clicks.Click_time as date)
,Clicks.order_id
,Clicks.Site_ID
,Clicks.Page_ID
) as report
LEFT JOIN
(

select cast(buy as varchar(4000)) as ''buy'', order_id as ''order_id''
from diap01.mec_us_united_20056.dfa_campaign
) AS Campaign
ON Report.order_id = Campaign.order_id

left join
(
select cast(t1.site_placement as varchar(4000)) as ''site_placement'',  t1.page_id as ''page_id'', t1.order_id as ''order_id'', t1.site_id as ''site_id''

from (select order_id as order_id, site_id as site_id, page_id as page_id, site_placement as site_placement, cast(start_date as date) as thisDate,
	row_number() over (partition by order_id, site_id, page_id  order by cast(start_date as date) desc) as r1
    FROM diap01.mec_us_united_20056.dfa_page

) as t1
where r1 = 1
) AS placements
on 	report.page_id 	= placements.page_id
and Report.order_id = placements.order_id
and report.site_ID  = placements.site_id

LEFT JOIN
(
select cast(directory_site as varchar(4000)) as ''directory_site'', site_id as ''site_id''
from diap01.mec_us_united_20056.dfa_site
) AS Directory
ON Report.Site_ID = Directory.Site_ID

WHERE NOT REGEXP_LIKE(site_placement,''.do\s*not\s*use.'',''ib'')
and Report.Site_ID !=''1485655''
GROUP BY
cast(Report.Date AS DATE)
-- , cast(month(cast(Report.Date as date)) as int)
, Directory.Directory_Site
,Report.Site_ID
, Report.order_id
, Campaign.Buy
, Report.page_id
, Placements.Site_Placement
-- , Placements.PlacementNumber
')

                      ) as dcmreport


                     left join
                     (
                         select *
                         from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].summarytable
                     ) as prisma
                         on dcmreport.placement_id = prisma.adserverplacementid
                 where prisma.costmethod != 'Flat'

                 group by
                     dcmreport.dcmdate
                     ,cast(month(cast(dcmreport.dcmdate as date)) as int)
                     ,dcmreport.campaign
                     ,dcmreport.campaign_id
                     ,dcmreport.site_dcm
                     ,dcmreport.site_id_dcm
                     ,dcmreport.placementnumber
                     ,dcmreport.placement
                     ,dcmreport.placement_id
                     ,prisma.packagecat
                     ,prisma.rate
                     ,prisma.costmethod
                     ,prisma.cost_id
                     ,prisma.planned_amt
                     ,prisma.planned_cost
                     ,prisma.placementend
                     ,prisma.placementstart
                     ,prisma.stdate
                     ,prisma.eddate
                     ,prisma.dv_map
             ) as t1

             left join
             (
                 select *
                 from master.dbo.flattableDT2
             ) as flat
                 on t1.cost_id = flat.cost_id
                 and t1.dcmmatchdate = flat.dcmdate

-- dv table join ==============================================================================================================================================

             left join (
                           select *
                           from master.dbo.dvtable
-- where dvdate between @report_st and @report_ed
                       ) as dv
                 on
                     left(t1.placement,6) + '_' + [dbo].udf_sitekey(t1.site_dcm) + '_'
                         + cast(t1.dcmdate as varchar(10)) = dv.joinkey

-- moat table join ==============================================================================================================================================

             left join (
                           select *
                           from master.dbo.mttable
-- where mtdate between @report_st and @report_ed
                       ) as mt
                 on
                     left(t1.placement,6) + '_' + [dbo].udf_sitekey(t1.site_dcm) + '_'
                         + cast(t1.dcmdate as varchar(10)) = mt.joinkey


-- innovid table join ==============================================================================================================================================

             left join (
                           select *
                           from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].innovidexttable
-- where ivdate between @report_st and @report_ed
                       ) as iv
                 on
                     left(t1.placement,6) + '_' + [dbo].udf_sitekey(t1.site_dcm) + '_'
                         + cast(t1.dcmdate as varchar(10)) = iv.joinkey
         where t1.costmethod != 'Flat'
--              and (t1.cost_id = 'P6SF6Y' or t1.cost_id = 'P8H1HG')



         group by

             t1.campaign
             ,t1.campaign_id
             ,t1.cost_id
             ,t1.dv_map
--   ,t1.site_dcm
--   ,t1.site_id_dcm
             ,t1.packagecat
             ,t1.placementend
             ,t1.placementstart
             ,t1.placementnumber
             ,t1.placement_id
             ,t1.planned_amt
             ,t1.planned_cost
             ,t1.rate
             ,t1.placement
             ,t1.eddate
             ,t1.stdate
             ,t1.dcmdate
             ,t1.dcmmatchdate
             ,t1.dcmmonth
             ,t1.diff
             ,dv.joinkey
             ,mt.joinkey
             ,iv.joinkey
-- ,flat.flatcost
             ,t1.costmethod
     ) as t2

                                  ) as f0

                              group by
                                  f0.dcmmonth,
--                                   f0.dcmyear,
--                                   f0.dcmYrMo,
                                  f0.PackageCat,
                                  f0.costmethod,
                                  f0.edDate,
                                  f0.stDate,
                                  f0.dcmDate,
                                  f0.rate,
--                                   f0.stYrMo,
--                                   f0.edYrMo,
                                  f0.cost_id,
                                  f0.planned_amt,
                                  f0.planned_cost,
                                  f0.plce_id
--                                   f0.cost
                          ) as f1
                  ) as f2
         ) as f3