--  Master Query, new cluster
--  version that makes use of costTable_dt1

/*  This query is a bit of a hack. Non-optimal aspects are necessitated by the particularities of the current tech stack on United.
    Code is most easily read by starting at the "innermost" block, inside the openQuery call.

    Data must be pulled and reconciled from 1) Prisma, in Datamart, and 2) DFA/DV/Moat in Vertica*.



    Because of its scale, log-level DFA data (DTF 1.0) must be kept in Vertica to preserve performance. DV and Moat are stored there as well, mostly for convenience.
    Intermediary summary tables are necessary for this query, so we need to be able to create our own tables and run stored procedures to refresh those tables. But we don't have write access to Vertica, so we can't keep routines and tables there.

    DI could do this, but edits to these routines are frequent (esp. in joinKey fields), so keeping this process in-house is more convenient for all parties.

*/
-- these summary/reference tables can be run once a day as a regular process or before the query is run
--
-- EXEC master.dbo.createDVTbl GO    -- create separate DV aggregate table and store it in my instance; joining to the Vertica table in the query
-- EXEC master.dbo.createMTTbl GO    -- create separate MOAT aggregate table and store it in my instance; joining to the Vertica table in the query
-- EXEC [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.createInnovidExtTbl GO
-- EXEC [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.createViewTbl GO
-- EXEC [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.createAmtTbl GO
-- EXEC [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.createPackTbl GO
-- EXEC [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.createSumTbl GO
-- EXEC master.dbo.crt_dfa_flatCost_dt1 GO
-- EXEC master.dbo.crt_dfa_flatCost_dt2 GO
-- EXEC master.dbo.createCostTblDT1 GO



DECLARE @report_st date
DECLARE @report_ed date
--
SET @report_ed = '2016-07-25'
SET @report_st = '2016-05-16'

--
-- SET @report_ed = DateAdd(DAY, -DatePart(DAY, getdate()), getdate());
-- SET @report_st = DateAdd(DAY, 1 - DatePart(DAY, @report_ed), @report_ed);

select
    -- DCM ad server date
    cast(final.dcmDate as date)                                                                             as "Date",
    -- DCM ad server week (from date)
    cast(dateadd(week,datediff(week,0,cast(final.dcmDate as date)),0) as date)                              as "Week",
    -- DCM ad server month (from date)
    dateName(month,cast(final.dcmDate as date))                                                             as "Month",
    -- DCM ad server quarter + year (from date)
    'Q' + dateName(quarter,cast(final.dcmDate as date)) + ' ' + dateName(year,cast(final.dcmDate as date))  as "Quarter",
    -- Reference/optional: difference, in months, between placement end date and report date. Field is used deterministically in other fields below.
--  final.diff                                                                                              as diff,
    -- Reference/optional: match key from the DV table; only present when DV data is available.
    final.dvJoinKey                                                                                         as dvJoinKey,
    -- Reference/optional: match key from the Moat table; only present when Moat data is available.
    final.mtJoinKey                                                                                         as mtJoinKey,
    -- Reference/optional: package category from Prisma (standalone; package; child). Useful for exchanging info with Planning/Investment
    final.PackageCat                                                                                        as PackageCat,
    -- Reference/optional: first six characters of package-level placement name, used to join 1) Prisma table, and 2) flat fee table
    final.Cost_ID                                                                                           as Cost_ID,
    -- DCM Campaign name
--  final.Buy                                                                                               as "DCM Campaign",
    -- Friendly Campaign name
    [dbo].udf_campaignName(final.order_id, final.Buy)                                                       as Campaign,
    -- DCM campaign ID
    final.order_id                                                                                          as "Campaign ID",
    -- Reference/optional: three-character designation, sometimes descriptive, from placement name.
--  final.campaignShort                                                                                     as "Campaign Short Name",
    -- Designation specified by Planning/Investment in 2016.
--  final.campaignType                                                                                      as "Campaign Type",
    -- DCM site name
--  final.site                                                                                    as SITE,
-- Preferred, friendly site name; also corresponds to what's used in the joinKey fields across DFA, DV, and Moat.
    [dbo].udf_siteName(final.site) as "Site",
    -- DCM site ID
    final.Site_ID                                                                                           as "Site ID",
    -- Reference/optional: package cost/pricing model, from Prisma; attributed to all placements within package.
    final.CostMethod                                                                                        as "Cost Method",
    -- Reference/optional: first six characters of placement name. Used for matching across datasets when no common placement ID is available, e.g. DFA-DV.
--  final.PlacementNumber                                                                                   as PlacementNumber,
    -- DCM placement name
    final.plc                                                                                    as Placement,
    -- DCM placement ID
    final.page_id                                                                                           as page_id,
    -- Reference/optional: planned package end date, from Prisma; attributed to all placements within package.
    final.PlacementEnd                                                                                      as "Placement End",
    final.PlacementStart                                                                                    as "Placement Start",
    final.DV_Map                                                                                            as "DV Map",
    final.Rate                                                                                              as Rate,
    final.Planned_Amt                                                                                       as "Planned Amt",
--  final.Planned_Cost                                                                                       as "Planned Cost",
--      final.Planned_Cost/final.newCount                                                                                      as "Planned Cost 2",
--  final.flatCostRemain                                                                                    as flatCostRemain,
--  final.impsRemain                                                                                        as impsRemain,
--  sum(final.cost)                                                                                         as cost,
    case when final.CostMethod='Flat' then final.flatCost/max(final.newCount) else sum(final.cost) end      as cost,
    sum(final.dlvrImps)                                                                                     as "Delivered Impressions",
    sum(final.billImps)                                                                                     as "Billable Impressions",
    sum(final.cnslImps)                                                                                     as "DFA Impressions",
    sum(final.IV_Impressions)                                                                               as "Innovid Impressions",
--  sum(MT.human_impressions)                                                                               as MT_Impressions,
--  sum(final.dv_impressions)                                                                               as DV_Impressions,
--  sum(final.DV_Viewed)                                                                                    as DV_Viewed,
--  sum(final.DV_GroupMPayable)                                                                             as DV_GroupMPayable,
    sum(final.Clicks)                                                                                       as clicks,
    case when sum(final.dlvrImps) = 0 then 0
    else (sum(cast(final.Clicks as decimal(20,10)))/sum(cast(final.dlvrImps as decimal(20,10))))*100 end    as CTR,

    sum(final.conv)                                                                                         as Transactions,
    -- sum(final.View_Thru_Conv)                                                                               as View_Thru_Trns,
    -- sum(final.Click_Thru_Conv)                                                                              as Clck_Thru_Trns,
    -- sum(final.View_Thru_Tickets)                                                                            as View_Thru_Tickets,
    -- sum(final.Click_Thru_Tickets)                                                                           as Click_Thru_Tickets,
    sum(final.tickets)                                                                                      as Tickets,
    case when sum(final.conv) = 0 then 0
    else sum(final.cost)/sum(final.conv) end                                                                as "Cost/Transaction",
    sum(final.vt_rev)                                                                            as vt_rev,
    sum(final.ct_rev)                                                                           as Clck_Thru_Revenue,
    sum(final.Revenue)                                                                                      as Revenue,
    sum(final.viewRevenue)                                                                                  as "Billable Revenue",
    sum(final.adjsRevenue)                                                                                  as "Adjusted (Final) Revenue"
--  case when sum(final.cost) = 0 then 0
--  else ((sum(adjsRevenue)-sum(final.cost))/sum(final.cost)) end * 100                                     as aROAS
from (

-- for running the code here instead of at "final," above
--
-- DECLARE @report_st date,
-- @report_ed date;
-- --
-- SET @report_ed = '2016-01-31';
-- SET @report_st = '2016-01-01';

         select
             -- DCM ad server date
             cast(almost.dcmDate as date)                                               as dcmDate,
             -- DCM ad server week (from date)
             almost.dcmMonth                                                            as dcmMonth,

             almost.diff                                                                as diff,
             DV.JoinKey                                                                 as dvJoinKey,
             MT.joinKey                                                                 as mtJoinKey,
             IV.joinKey                                                                 as ivJoinKey,
             almost.PackageCat                                                          as PackageCat,
             almost.Cost_ID                                                             as Cost_ID,
             almost.Buy                                                                 as Buy,
             almost.order_id                                                            as order_id,
             almost.campaignShort                                                       as campaignShort,
             almost.campaignType                                                        as campaignType,
             almost.site                                                      as site,
             almost.Site_ID                                                             as Site_ID,
             almost.CostMethod                                                          as CostMethod,
             sum(1) over (partition by almost.Cost_ID,almost.dcmMatchDate order by
                 almost.dcmMonth asc range between unbounded preceding and current row) as newCount,
             almost.PlacementNumber                                                     as PlacementNumber,
             almost.plc                                                      as plc,
             almost.page_id                                                             as page_id,
             almost.PlacementEnd                                                        as PlacementEnd,
             almost.PlacementStart                                                      as PlacementStart,
             almost.DV_Map                                                              as DV_Map,
             almost.Planned_Amt                                                         as Planned_Amt,
--           almost.Planned_Cost                                                        as Planned_Cost,
             flat.flatcost                                                              as flatcost,
--  Logic excludes flat fees
             case
--           Zeros out cost for placements traffic BEFORE specified start date or AFTER specified end date
             when ((almost.DV_Map = 'N' or almost.DV_Map = 'Y') and (almost.edDate - almost.dcmMatchDate < 0 or almost.dcmMatchDate - almost.stDate < 0)
                   and (almost.CostMethod = 'CPM' or almost.CostMethod = 'CPMV' or almost.CostMethod = 'CPE' or almost.CostMethod = 'CPC' or
                        almost.CostMethod = 'CPCV'))
                   then 0
--           Click source Innovid
             when ((almost.DV_Map = 'Y' or almost.DV_Map = 'N') and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0)
                   and (almost.CostMethod = 'CPC' or almost.CostMethod = 'CPCV') and (len(ISNULL(IV.joinKey,''))>0))
                   then (sum(cast(IV.click_thrus as decimal(10,2))) * cast(almost.Rate as decimal(10,2)))
--           Click source DCM
             when ((almost.DV_Map = 'Y' or almost.DV_Map = 'N') and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0)
                   and (almost.CostMethod = 'CPC' or almost.CostMethod = 'CPCV'))
                   then (sum(cast(almost.Clicks as decimal(10,2))) * cast(almost.Rate as decimal(10,2)))

--           Impression-based cost; not subject to viewability; Innovid source
             when (almost.DV_Map = 'N' and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0) and
                   (almost.CostMethod = 'CPM' or almost.CostMethod = 'CPMV' or almost.CostMethod = 'CPE') and (len(ISNULL(IV.joinKey,''))>0))
                   then ((sum(cast(IV.impressions as decimal(10,2))) * cast(almost.Rate as decimal(10,2)) / 1000))

--           Impression-based cost; not subject to viewability; DCM source
             when (almost.DV_Map = 'N' and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0) and
                   (almost.CostMethod = 'CPM' or almost.CostMethod = 'CPMV' or almost.CostMethod = 'CPE'))
                   then ((sum(cast(almost.Impressions as decimal(10,2))) * cast(almost.Rate as decimal(10,2)) / 1000))

--           Impression-based cost; subject to viewability with flag; MT source
             when (almost.DV_Map = 'Y' and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0) and
                   (almost.CostMethod = 'CPM' or almost.CostMethod = 'CPMV' or almost.CostMethod = 'CPE') and MT.joinKey is not null)
                   then ((sum(cast(MT.groupm_billable_impressions as decimal(10,2))) * cast(almost.Rate as decimal(10,2)) / 1000))

--           Impression-based cost; subject to viewability; DV source
             when (almost.DV_Map = 'Y' and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0) and
                   (almost.CostMethod = 'CPM' or almost.CostMethod = 'CPMV' or almost.CostMethod = 'CPE'))
                   then ((sum(cast(DV.groupm_billable_impressions as decimal(10,2))) * cast(almost.Rate as decimal(10,2)) / 1000))

--           Impression-based cost; subject to viewability; MOAT source
             when (almost.DV_Map = 'M' and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0) and
                   (almost.CostMethod = 'CPM' or almost.CostMethod = 'CPMV' or almost.CostMethod = 'CPE'))
                   then ((sum(cast(MT.groupm_billable_impressions as decimal(10,2))) * cast(almost.Rate as decimal(10,2)) / 1000))

             else 0 end                                                                 as Cost,
             almost.Rate                                                                as Rate,
             sum(almost.incrFlatCost)                                                   as incrFlatCost,
             sum(case
                 --         not subject to viewability
                 when (almost.DV_Map = 'N')
                     then almost.vt_rev + almost.ct_rev

                 --         subject to viewability with flag; MT source
                 when (almost.DV_Map = 'Y' and (len(ISNULL(MT.joinKey,''))>0))
                     then ((almost.vt_rev) *
                          (cast(MT.groupm_passed_impressions as decimal) /
                            nullif(cast(MT.total_impressions as decimal),0))) + almost.ct_rev
                 --         subject to viewability; DV source
                 when (almost.DV_Map = 'Y')
                     then ((almost.vt_rev) *
                          (cast(DV.groupm_passed_impressions as decimal) /
                            nullif(cast(DV.total_impressions as decimal),0))) + almost.ct_rev
                 --         subject to viewability; MOAT source
                 when (almost.DV_Map = 'M')
                     then ((almost.vt_rev) *
                          (cast(MT.groupm_passed_impressions as decimal) /
                            nullif(cast(MT.total_impressions as decimal),0))) + almost.ct_rev
                 else 0 end)                                                            as viewRevenue,


             sum(case

--               not subject to viewability
                 when (almost.DV_Map = 'N')
                     then cast((almost.vt_rev * .2 * .15) + almost.ct_rev as decimal(10,2))

--               subject to viewability with flag; MT source
                 when (almost.DV_Map = 'Y' and (len(ISNULL(MT.joinKey,''))>0))
                     then cast(
                         (((almost.vt_rev) *
                                (cast(MT.groupm_passed_impressions as decimal) /
                                  nullif(cast(MT.total_impressions as decimal),0))) * .2 * .15) + almost.ct_rev as decimal(10,2))

--               subject to viewability; DV source
                 when (almost.DV_Map = 'Y')
                     then cast(
                         (((almost.vt_rev) *
                              (cast(DV.groupm_passed_impressions as decimal) /
                                  nullif(cast(DV.total_impressions as decimal),0))) * .2 * .15) + almost.ct_rev as decimal(10,2))

--               subject to viewability; MOAT source
                 when (almost.DV_Map = 'M')
                     then cast(
                         (((almost.vt_rev) *
                                (cast(MT.groupm_passed_impressions as decimal) /
                                  nullif(cast(MT.total_impressions as decimal),0))) * .2 * .15) + almost.ct_rev as decimal(10,2))
                 else 0 end)                                                            as adjsRevenue,

--           Total impressions as reported by 1.) DCM for "N," 2.) DV for "Y," or MOAT for "M"
             sum(case
                 when almost.DV_Map = 'Y' and (len(ISNULL(MT.joinKey,''))>0) then MT.total_impressions
                 when almost.DV_Map = 'Y' then DV.total_impressions
                 when almost.DV_Map = 'M' then MT.total_impressions
                 when almost.DV_Map = 'N' and (len(ISNULL(IV.joinKey,''))>0) then IV.impressions
                 else almost.Impressions end)                                           as dlvrImps,

--           Billable impressions as reported by 1.) DCM for "N," 2.) DV for "Y," or MOAT for "M"
             sum(case
                 when almost.DV_Map = 'Y' and (len(ISNULL(MT.joinKey,''))>0) then MT.groupm_billable_impressions
                 when almost.DV_Map = 'Y' then DV.groupm_billable_impressions
                 when almost.DV_Map = 'M' then MT.groupm_billable_impressions
                 when almost.DV_Map = 'N' and (len(ISNULL(IV.joinKey,''))>0) then IV.impressions
                 else almost.Impressions end)                                           as billImps,

--           DCM Impressions (for comparison (QA) to DCM console)
             sum(almost.Impressions)                                                    as cnslImps,
             sum(IV.impressions)                                                        as IV_Impressions,
             sum(IV.click_thrus)                                                        as IV_Clicks,
                 sum(IV.all_completion) as IV_Completes,
             sum(cast(DV.total_impressions as int))                                     as DV_Impressions,
             sum(DV.groupm_passed_impressions)                                          as DV_Viewed,
             sum(cast(DV.groupm_billable_impressions as decimal(10,2)))                 as DV_GroupMPayable,
             sum(cast(MT.total_impressions as int))                                     as MT_Impressions,
             sum(MT.groupm_passed_impressions)                                          as MT_Viewed,
             sum(cast(MT.groupm_billable_impressions as decimal(10,2)))                 as MT_GroupMPayable,
--           Clicks
             sum(case
                 when (len(ISNULL(IV.joinKey,''))>0) then IV.click_thrus
                 else almost.Clicks end)                                                as clicks,
             -- sum(almost.View_Thru_Conv)                                                 as View_Thru_Conv,
             -- sum(almost.Click_Thru_Conv)                                                as Click_Thru_Conv,
             sum(almost.conv)                                                           as conv,
             -- sum(almost.View_Thru_Tickets)                                              as View_Thru_Tickets,
             -- sum(almost.Click_Thru_Tickets)                                             as Click_Thru_Tickets,
             sum(almost.tickets)                                                        as tickets,
             sum(almost.vt_rev)                                              as vt_rev,
             sum(almost.ct_rev)                                             as ct_rev,
             sum(almost.Revenue)                                                        as revenue

         from
             (
-- =========================================================================================================================
-- --
-- DECLARE @report_st date,
-- @report_ed date;
-- --
-- SET @report_ed = '2016-10-21';
-- SET @report_st = '2016-10-10';
                select
                     dcmReport.dcmDate as dcmDate,
                     cast(month(cast(dcmReport.dcmDate as date)) as int) as dcmmonth,
                     [dbo].udf_dateToInt(dcmreport.dcmdate) as dcmmatchdate,
                     dcmReport.Buy             as Buy,
                     dcmReport.order_id        as order_id,
                     dcmReport.site  as site,
                     dcmReport.Site_ID         as Site_ID,
                     case when dcmReport.PlacementNumber in('PBKB7J', 'PBKB7H', 'PBKB7K') then 'PBKB7J' end as PlacementNumber,

                     case when dcmReport.plc like 'PBKB7J%' or dcmReport.plc like 'PBKB7H%' or dcmReport.plc like 'PBKB7K%' or dcmReport.plc ='United 360 - Polaris 2016 - Q4 - Amobee'        then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID' else  dcmReport.plc end     as plc,
--              Amobee Video 360 placements, tracked differently across DCM, Innovid, and MOAT; this combines the three placements into one
                    case when dcmReport.page_id in (137412510, 137412401, 137412609) then 137412609 else dcmReport.page_id end as page_id,
                     Prisma.stDate             as stDate,
                     case when dcmReport.order_id = 9923634 and dcmReport.Site_ID != 1190258 then 20161022 else
                     Prisma.edDate end         as edDate,
                     Prisma.PackageCat         as PackageCat,
                     Prisma.CostMethod         as CostMethod,
                     Prisma.Cost_ID            as Cost_ID,
                     Prisma.Planned_Amt        as Planned_Amt,
--                   Prisma.Planned_Cost       as Planned_Cost,
                     Prisma.PlacementStart     as PlacementStart,
                     case when dcmReport.order_id = 9923634 and dcmReport.Site_ID != 1190258 then '2016-10-22' else
                     Prisma.PlacementEnd end   as PlacementEnd,

--              Flat.flatCostRemain                                                               AS flatCostRemain,
--              Flat.impsRemain                                                                   AS impsRemain,
                     sum(( cast(dcmReport.imps as decimal(10,2)) /
                         nullif(cast(Prisma.Planned_Amt as decimal(10,2)),0) ) * cast(Prisma.Rate as decimal(10,2))) as incrFlatCost,
                     cast(Prisma.Rate as decimal(10,2))                                   as Rate,
                     sum(dcmReport.imps)                                           as impressions,
                     sum(dcmReport.Clicks)                                                as clicks,
                     -- sum(dcmReport.View_Thru_Conv)                                        as View_Thru_Conv,
                     -- sum(dcmReport.Click_Thru_Conv)                                       as Click_Thru_Conv,
                     -- sum(dcmReport.View_Thru_Conv) + sum(dcmReport.Click_Thru_Conv)       as conv,
                     sum(dcmReport.con)      as conv,
                     -- sum(dcmReport.View_Thru_Tickets)                                     as View_Thru_Tickets,
                     -- sum(dcmReport.Click_Thru_Tickets)                                    as Click_Thru_Tickets,
                     -- sum(dcmReport.View_Thru_Tickets) + sum(dcmReport.Click_Thru_Tickets) as tickets,
                     sum(dcmReport.tix) as tickets,
                     sum(cast(dcmReport.vt_rev as decimal(10,2)))              as vt_rev,
                     sum(cast(dcmReport.ct_rev as decimal(10,2)))             as ct_rev,
                     sum(cast(dcmReport.ct_rev as decimal(10,2))) + sum(cast(dcmReport.vt_rev as decimal(10,2))) as revenue,
                     case when cast(month(Prisma.PlacementEnd) as int) - cast(month(cast(dcmReport.dcmDate as date)) as int) <= 0 then 0
                     else cast(month(Prisma.PlacementEnd) as int) - cast(month(cast(dcmReport.dcmDate as date)) as int) end as diff,


                     case
                     when Prisma.CostMethod = 'Flat' or Prisma.CostMethod = 'CPC' or Prisma.CostMethod = 'CPCV' or Prisma.CostMethod = 'dCPM'
                         then 'N'

--                   Live Intent for SFO-SIN campaign is email (not subject to viewab.), but mistakenly labeled with "Y"
                     when
                             dcmReport.order_id = '9923634' -- SFO-SIN
                         and dcmReport.Site_ID = '1853564' -- Live Intent
                         then 'N'

--                   Corrections to SME placements
                     when dcmReport.order_id = '10090315' and (dcmReport.Site_ID = '1513807' or dcmReport.Site_ID = '1592652')
                         then 'Y'

--                   Corrections to SFO-SIN placements
                     when dcmReport.order_id = '9923634' and dcmReport.Site_ID = '1534879' and Prisma.CostMethod = 'CPM'
                         then 'N'
--                   designates all Xaxis placements as "Y." Not always true.
--                    when dcmReport.Site_ID = '1592652' then 'Y'

--                   FlipBoard unable to implement MOAT tags; must bill off of DFA imps
                     when dcmReport.Site_ID = '2937979' then 'N'
--                   All Targeted Marketing subject
                     when dcmReport.order_id = '9639387' then 'Y'

--                   Designates all SFO-AKL placements as "Y." Not always true. Apparently.
--                   when dcmReport.order_id = '9973506' then 'Y'

                     when Prisma.CostMethod = 'CPMV' and
                          ( dcmReport.plc like '%[Mm][Oo][Bb][Ii][Ll][Ee]%' or dcmReport.plc like '%[Vv][Ii][Dd][Ee][Oo]%' or dcmReport.plc like '%[Pp][Rr][Ee]%[Rr][Oo][Ll][Ll]%' or dcmReport.Site_ID = '1995643'
--                          or dcmReport.Site_ID = '1474576'
                            or dcmReport.Site_ID = '2854118')
                         then 'M'
--                   Look for viewability flags Investment began including in placement names 6/16.
                     when dcmReport.plc like '%[_]DV[_]%' then 'Y'
                     when dcmReport.plc like '%[_]MOAT[_]%' then 'M'
                     when dcmReport.plc like '%[_]NA[_]%' then 'N'
--
                     when Prisma.CostMethod = 'CPMV' and Prisma.DV_Map = 'N' then 'Y'
                     else Prisma.DV_Map end as DV_Map,

--                   Prisma.DV_Map as DV_Map,
                     SUBSTRING(dcmReport.plc,( CHARINDEX(dcmReport.plc,'_UAC_') + 12 ),
                               3)                                                                                                                           as campaignShort,
                     case when SUBSTRING(dcmReport.plc,( CHARINDEX(dcmReport.plc,'_UAC_') + 12 ),3) =
                               'TMK'
                         then 'Acquisition'
                     else 'Non-Acquisition' end as campaignType
                       -- ==========================================================================================================================================================

                       -- openQuery function call must not exceed 8,000 characters; no room for comments inside the function
                       from (
                                select *
                                from openQuery(VerticaUnited,
                                               'select
cast(rpt.date as date) as dcmdate,
cast(month (cast(rpt.date as date)) as int) as reportmonth,
cp1.buy as buy,
rpt.order_id as order_id,
rpt.site_id  as site_id,
d1.site as site,
left(j2.plc,6) as placementnumber,
j2.plc as plc,
rpt.page_id as page_id,
sum(rpt.imps) as imps,
sum(rpt.clicks) as clicks,
sum(rpt.con) as con,
sum(rpt.tix) as tix,
sum(rpt.ct_rev) as ct_rev,
sum(rpt.vt_rev) as vt_rev

from (
select
c9.date as "date",
c9.order_id as order_id,
c9.site_id as site_id,
c9.page_id as page_id,
0 as imps,
0 as clicks,
sum(c9.con) as con,
sum(c9.tix) as tix,
sum(c9.ct_rev) as ct_rev,
sum(c9.vt_rev) as vt_rev

from (
select
*   from diap01.mec_us_united_20056.ual_bru_activity

) as c9

group by
c9.date,
c9.order_id,
c9.site_id,
c9.page_id

union all

select
cast(i1.impression_time as date) as "date"
,i1.order_id as order_id
,i1.site_id as site_id
,i1.page_id as page_id
,count (*) as imps
,0 as clicks
,0 as con
,0 as tix
,0 as ct_rev
,0 as vt_rev


from (
select *
from diap01.mec_us_united_20056.dfa_impression as i2
where cast(i2.impression_time as date) between ''2016-11-07'' and ''2016-12-31''
and i2.order_id=10505745
and i2.advertiser_id <> 0
) as i1
group by
cast(i1.impression_time as date)
,i1.order_ID
,i1.Site_ID
,i1.Page_ID
union all
select
cast(ck.click_time as date) as "Date"
,ck.order_id as order_id
,ck.Site_ID as Site_ID
,ck.Page_ID as page_id
,0 as imps
,count(*) as Clicks
,0 as con
,0 as tix
,0 as ct_rev
,0 as vt_rev
from (
select *
from diap01.mec_us_united_20056.dfa_click as ck1
where cast(ck1.click_time as date) between ''2016-11-07'' and ''2016-12-31''
and ck1.order_id=10505745
and ck1.advertiser_id <> 0
) as ck
group by
cast(ck.Click_time as date)
,ck.order_id
,ck.Site_ID
,ck.Page_ID
) as rpt
left join
(
select cast(cp2.buy as varchar (4000)) as buy,cp2.order_id
from diap01.mec_us_united_20056.dfa_campaign as cp2
) as cp1
on rpt.order_id=cp1.order_id
left join
(
select cast(t99.plc as varchar (4000)) as plc,t99.page_id,t99.order_id,t99.site_id
from (select p3.order_id,p3.site_id,p3.page_id,p3.site_placement as plc,cast(p3.start_date as date) as thisDate,
row_number() over (partition by p3.order_id,p3.site_id,p3.page_id order by cast(p3.start_date as date)desc) as r1
from diap01.mec_us_united_20056.dfa_page as p3
) as t99
where r1=1
) as j2
on rpt.page_id=j2.page_id
and rpt.order_id=j2.order_id
and rpt.site_ID=j2.site_id
left join
(
select cast(directory_site as varchar (4000)) as site,site_id
from diap01.mec_us_united_20056.dfa_site
) as d1
on rpt.Site_ID=d1.Site_ID
where not REGEXP_LIKE(j2.plc,''.do\s*not\s*use.'',''ib'')
group by
cast (rpt.Date as date )
,d1.site
,rpt.Site_ID
,rpt.order_id
,cp1.Buy
,rpt.page_id
,j2.plc
')

                            ) as dcmReport


            LEFT JOIN
            (
                SELECT *
                FROM [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.[dbo].prs_summ
            ) AS Prisma
                ON dcmReport.page_id = Prisma.AdserverPlacementId

        group by
            dcmReport.dcmDate
            ,cast(month(cast(dcmReport.dcmDate as date)) as int)
            ,dcmReport.Buy
            ,dcmReport.order_id
            ,dcmReport.site
            ,dcmReport.Site_ID
            ,dcmReport.PlacementNumber
            ,dcmReport.plc
            ,dcmReport.page_id
            ,Prisma.PackageCat
            ,Prisma.Rate
            ,Prisma.CostMethod
            ,Prisma.Cost_ID
            ,Prisma.Planned_Amt
--          ,Prisma.Planned_Cost
            ,Prisma.PlacementEnd
            ,Prisma.PlacementStart
            ,Prisma.stDate
            ,Prisma.edDate
--          ,DV.joinKey
--          ,Flat.flatCostRemain
--          ,Flat.impsRemain
            ,Prisma.DV_Map
    ) as almost

    left join
    (
        select *
        from master.dbo.dfa_flatCost_dt1
    ) as Flat
        on almost.Cost_ID = Flat.Cost_ID
           and almost.dcmMatchDate = flat.dcmDate

-- DV Table JOIN ==============================================================================================================================================

    left join (
                  select *
                  from master.dbo.dv_summ
                  where dvDate between @report_st and @report_ed
              ) as DV
            on
            left(almost.plc,6) + '_' + [dbo].udf_siteKey( almost.site) + '_'
            + cast(almost.dcmDate as varchar(10)) = DV.joinKey

-- MOAT Table JOIN ==============================================================================================================================================

    left join (
                  select *
                  from master.dbo.mt_summ
                  where mtDate between @report_st and @report_ed
              ) as MT
            on
            left(almost.plc,6) + '_' + [dbo].udf_siteKey( almost.site) + '_'
            + cast(almost.dcmDate as varchar(10)) = MT.joinKey


-- Innovid Table JOIN ==============================================================================================================================================

    left join (
                  select *
                  from [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.[dbo].ivd_summ_agg
                  where ivDate between @report_st and @report_ed
              ) as IV
            on
            left(almost.plc,6) + '_' + [dbo].udf_siteKey( almost.site) + '_'
            + cast(almost.dcmDate as varchar(10)) = IV.joinKey

-- where almost.cost_id = 'P8FSSK'
--  where almost.CostMethod = 'Flat'
--     where almost.Site_ID ='1853562'
--           where almost.plc like 'PBF7M2%'

group by

    almost.Buy
    ,almost.order_id
    ,almost.Cost_ID
    ,almost.DV_Map
    ,almost.site
    ,almost.Site_ID
    ,almost.PackageCat
    ,almost.PlacementEnd
    ,almost.PlacementStart
    ,almost.PlacementNumber
    ,almost.page_id
    ,almost.Planned_Amt
--  ,almost.Planned_Cost
    ,almost.Rate
    ,almost.plc
    ,almost.edDate
    ,almost.stDate
    ,almost.campaignShort
    ,almost.campaignType
    ,almost.dcmDate
    ,almost.dcmMatchDate
    ,almost.dcmMonth
    ,almost.diff
    ,DV.JoinKey
    ,MT.joinKey
    ,IV.joinKey
-- ,almost.flatCostRemain
-- ,almost.impsRemain
    ,almost.CostMethod
    ,Flat.flatCostRemain
    ,Flat.impsRemain
    ,Flat.flatCost

     ) as final

--  where (final.dvJoinKey is null or final.mtJoinKey is NULL) and (final.DV_Map = 'Y' or final.DV_Map = 'M')
--  where (final.dvJoinKey is null and final.DV_Map = 'Y') or (final.mtJoinKey is NULL and final.DV_Map = 'M')
--      where final.mtJoinKey is NULL and  final.DV_Map = 'M'
--  where final.site like '%[Bb]usiness%[Ii]nsider%'
--              where final.site like '%Verve%' or final.site like '%xAd%'
--     where final.Site_ID ='1853562'
--     where final.order_id ='10276123'

--  where final.Site_ID = '1853564' and final.DV_Map = 'Y'
--  where final.CostMethod = 'CPC'
    --  where final.CostMethod = 'Flat'


group by
    final.dcmDate
    ,final.dcmMonth
    ,final.diff
    ,final.dvJoinKey
    ,final.mtJoinKey
    ,final.PackageCat
    ,final.Cost_ID
    ,final.Buy
    ,final.order_id
    ,final.newCount
    ,final.campaignShort
    ,final.campaignType
    ,final.site
    ,final.Site_ID
    ,final.CostMethod
    ,final.Rate
-- ,final.PlacementNumber
    ,final.plc
    ,final.page_id
    ,final.PlacementEnd
    ,final.PlacementStart
    ,final.DV_Map
    ,final.Planned_Amt
--  ,final.Planned_Cost
-- ,final.flatCostRemain
-- ,final.impsRemain

    ,final.flatcost


order by
    final.Cost_ID,
    final.dcmDate;
