--  master query, dt 2.0
/*  this query is a bit of a hack. non-optimal aspects are necessitated by the particularities of the current tech stack on united.
  code is most easily read by starting at the "innermost" block, inside the openquery call.

  data must be pulled and reconciled from 1) prisma, in datamart, and 2) dfa/dv/moat in vertica*.



  because of its scale, log-level dfa data (dtf 1.0) must be kept in vertica to preserve performance. dv and moat are stored there as well, mostly for convenience.
  intermediary summary tables are necessary for this query, so we need to be able to create our own tables and run stored procedures to refresh those tables. but we don't have write access to vertica, so we can't keep routines and tables there.

  di could do this, but edits to these routines are frequent (esp. in joinkey fields), so keeping this process in-house is more convenient for all parties.

*/
-- these summary/reference tables can be run once a day as a regular process or before the query is run
--
-- exec master.dbo.createdvtbl go    -- create separate dv aggregate table and store it in my instance; joining to the vertica table in the query
-- exec master.dbo.createmttbl go    -- create separate moat aggregate table and store it in my instance; joining to the vertica table in the query
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.createinnovidexttbl go
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.createviewtbl go
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.createamttbl go
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.createpacktbl go
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.createsumtbl go
-- exec master.dbo.createflattblday go



declare @report_st date
declare @report_ed date
--
set @report_ed = '2016-12-30';
set @report_st = '2016-12-29';

--
-- set @report_ed = dateadd(day, -datepart(day, getdate()), getdate());
-- set @report_st = dateadd(day, 1 - datepart(day, @report_ed), @report_ed);

select
  -- dcm ad server date
  cast(final.dcmdate as date)                                                                             as "date",
  -- dcm ad server week (from date)
  cast(dateadd(week,datediff(week,0,cast(final.dcmdate as date)),0) as date)                              as "week",
  -- dcm ad server month (from date)
  datename(month,cast(final.dcmdate as date))                                                             as "month",
  -- dcm ad server quarter + year (from date)
  'q' + datename(quarter,cast(final.dcmdate as date)) + ' ' + datename(year,cast(final.dcmdate as date))  as "quarter",
  -- reference/optional: difference, in months, between placement end date and report date. field is used deterministically in other fields below.
--  final.diff                                                                                              as diff,
  -- reference/optional: match key from the dv table; only present when dv data is available.
  final.dvjoinkey                                                                                         as dvjoinkey,
  -- reference/optional: match key from the moat table; only present when moat data is available.
  final.mtjoinkey                                                                                         as mtjoinkey,
  -- reference/optional: package category from prisma (standalone; package; child). useful for exchanging info with planning/investment
  final.packagecat                                                                                        as packagecat,
  -- reference/optional: first six characters of package-level placement name, used to join 1) prisma table, and 2) flat fee table
  final.cost_id                                                                                           as cost_id,
  -- dcm campaign name
--  final.campaign                                                                                               as "dcm campaign",
  -- friendly campaign name
  [dbo].udf_campaignname(final.campaign_id, final.campaign)                             as campaign,
  -- dcm campaign id
  final.campaign_id                                                                                          as "campaign id",
  -- reference/optional: three-character designation, sometimes descriptive, from placement name.
--  final.campaignshort                                                                                     as "campaign short name",
  -- designation specified by planning/investment in 2016.
--  final.campaigntype                                                                                      as "campaign type",
  -- dcm site name
--  final.site_dcm                                                                        as site,
-- preferred, friendly site name; also corresponds to what's used in the joinkey fields across dfa, dv, and moat.
    [dbo].udf_sitename(final.site_dcm) as "site",
  -- dcm site id
  final.site_id_dcm                                                                                           as "site id",
  -- reference/optional: package cost/pricing model, from prisma; attributed to all placements within package.
  final.costmethod                                                                                        as "cost method",
  -- reference/optional: first six characters of placement name. used for matching across datasets when no common placement id is available, e.g. dfa-dv.
--  final.placementnumber                                                                       as placementnumber,
  -- dcm placement name
  final.placement                                                                                    as placement,
  -- dcm placement id
  final.placement_id                                                                                           as placement_id,
  -- reference/optional: planned package end date, from prisma; attributed to all placements within package.
  final.placementend                                                                                      as "placement end",
  final.placementstart                                                                                    as "placement start",
  final.dv_map                                                                                            as "dv map",
  final.rate                                                                                              as rate,
  final.planned_amt                                                                                       as "planned amt",
--  final.planned_cost                                                                                       as "planned cost",
--      final.planned_cost/final.newcount                                                                                      as "planned cost 2",
--  final.flatcostremain                                                                        as flatcostremain,
--  final.impsremain                                                                            as impsremain,
--  sum(final.cost)                                                                         as cost,
  case when final.costmethod='flat' then final.flatcost/max(final.newcount) else sum(final.cost) end      as cost,
  sum(final.dlvrimps)                                                                                     as "delivered impressions",
  sum(final.billimps)                                                                                     as "billable impressions",
  sum(final.cnslimps)                                                                                     as "dfa impressions",
  sum(final.iv_impressions)                                                                               as "innovid impressions",
--  sum(mt.human_impressions)                                                           as mt_impressions,
--  sum(final.dv_impressions)                                                                   as dv_impressions,
--  sum(final.dv_viewed)                                                                                    as dv_viewed,
--  sum(final.dv_groupmpayable)                                                                 as dv_groupmpayable,
  sum(final.clicks)                                                                                       as clicks,
  case when sum(final.dlvrimps) = 0 then 0
  else (sum(cast(final.clicks as decimal(20,10)))/sum(cast(final.dlvrimps as decimal(20,10))))*100 end  as ctr,

  sum(final.conv)                                                                                         as transactions,
  sum(final.vew_conv)                                                                 as vew_trns,
  sum(final.clk_conv)                                                                as clck_thru_trns,
  sum(final.vew_tix)                                                                as vew_tix,
  sum(final.clk_tix)                                                               as clk_tix,
  sum(final.tickets)                                                                                      as tickets,
  case when sum(final.conv) = 0 then 0
  else sum(final.cost)/sum(final.conv) end                                                              as "cost/transaction",
  sum(final.vew_rev)                                                                as vew_rev,
  sum(final.clk_rev)                                                               as clck_thru_rev,
  sum(final.revenue)                                                                                      as revenue,
  sum(final.viewrevenue)                                                                                  as "billable revenue",
  sum(final.adjsrevenue)                                                                                  as "adjusted (final) revenue"
--  case when sum(final.cost) = 0 then 0
--  else ((sum(adjsrevenue)-sum(final.cost))/sum(final.cost)) end * 100                                   as aroas
from (

-- for running the code here instead of at "final," above

-- declare @report_st date,
-- @report_ed date;
-- --
-- set @report_ed = '2016-12-30';
-- set @report_st = '2016-12-29';

       select
       -- dcm ad server date
         cast(almost.dcmdate as date)                                               as dcmdate,
       -- dcm ad server week (from date)
         almost.dcmmonth                                                            as dcmmonth,

         almost.diff                                                                as diff,
         dv.joinkey                                                                 as dvjoinkey,
         mt.joinkey                                 as mtjoinkey,
       iv.joinkey                                 as ivjoinkey,
         almost.packagecat                                                          as packagecat,
         almost.cost_id                                                             as cost_id,
         almost.campaign                                                                 as campaign,
         almost.campaign_id                                                            as campaign_id,
         almost.campaignshort                                                       as campaignshort,
         almost.campaigntype                                                        as campaigntype,
         almost.site_dcm                                                      as site_dcm,
         almost.site_id_dcm                                                             as site_id_dcm,
         almost.costmethod                                                          as costmethod,
         sum(1) over (partition by almost.cost_id,almost.dcmmatchdate order by
           almost.dcmmonth asc range between unbounded preceding and current row) as newcount,
         almost.placementnumber                                                     as placementnumber,
         almost.placement                                                      as placement,
         almost.placement_id                                                             as placement_id,
         almost.placementend                                                        as placementend,
         almost.placementstart                                                      as placementstart,
         almost.dv_map                                                              as dv_map,
         almost.planned_amt                                                         as planned_amt,
--       almost.planned_cost                                                        as planned_cost,
         flat.flatcost                                                              as flatcost,
--  logic excludes flat fees
         case
--       zeros out cost for placements traffic before specified start date or after specified end date
         when ((almost.dv_map = 'N' or almost.dv_map = 'Y') and (almost.eddate - almost.dcmmatchdate < 0 or almost.dcmmatchdate - almost.stdate < 0)
               and (almost.costmethod = 'CPM' or almost.costmethod = 'CPMV' or almost.costmethod = 'CPE' or almost.costmethod = 'CPC' or
                    almost.costmethod = 'CPCV'))
             then 0
--       click source innovid
       when ((almost.dv_map = 'Y' or almost.dv_map = 'N') and (almost.eddate - almost.dcmmatchdate >= 0 or almost.dcmmatchdate - almost.stdate >= 0)
               and (almost.costmethod = 'CPC' or almost.costmethod = 'CPCV') and (len(isnull(iv.joinkey,''))>0))
             then (sum(cast(iv.click_thrus as decimal(10,2))) * cast(almost.rate as decimal(10,2)))
--       click source dcm
         when ((almost.dv_map = 'Y' or almost.dv_map = 'N') and (almost.eddate - almost.dcmmatchdate >= 0 or almost.dcmmatchdate - almost.stdate >= 0)
               and (almost.costmethod = 'CPC' or almost.costmethod = 'CPCV'))
             then (sum(cast(almost.clicks as decimal(10,2))) * cast(almost.rate as decimal(10,2)))

--           impression-based cost; not subject to viewability; innovid source
         when (almost.dv_map = 'N' and (almost.eddate - almost.dcmmatchdate >= 0 or almost.dcmmatchdate - almost.stdate >= 0) and
               (almost.costmethod = 'CPM' or almost.costmethod = 'CPMV' or almost.costmethod = 'CPE') and (len(isnull(iv.joinkey,''))>0))
             then ((sum(cast(iv.impressions as decimal(10,2))) * cast(almost.rate as decimal(10,2)) / 1000))

--           impression-based cost; not subject to viewability; dcm source
         when (almost.dv_map = 'N' and (almost.eddate - almost.dcmmatchdate >= 0 or almost.dcmmatchdate - almost.stdate >= 0) and
               (almost.costmethod = 'CPM' or almost.costmethod = 'CPMV' or almost.costmethod = 'CPE'))
             then ((sum(cast(almost.impressions as decimal(10,2))) * cast(almost.rate as decimal(10,2)) / 1000))

--           impression-based cost; subject to viewability with flag; mt source
         when (almost.dv_map = 'Y' and (almost.eddate - almost.dcmmatchdate >= 0 or almost.dcmmatchdate - almost.stdate >= 0) and
               (almost.costmethod = 'CPM' or almost.costmethod = 'CPMV' or almost.costmethod = 'CPE') and mt.joinkey is not null)
             then ((sum(cast(mt.groupm_billable_impressions as decimal(10,2))) * cast(almost.rate as decimal(10,2)) / 1000))

--           impression-based cost; subject to viewability; dv source
         when (almost.dv_map = 'Y' and (almost.eddate - almost.dcmmatchdate >= 0 or almost.dcmmatchdate - almost.stdate >= 0) and
               (almost.costmethod = 'CPM' or almost.costmethod = 'CPMV' or almost.costmethod = 'CPE'))
             then ((sum(cast(dv.groupm_billable_impressions as decimal(10,2))) * cast(almost.rate as decimal(10,2)) / 1000))

--           impression-based cost; subject to viewability; moat source
         when (almost.dv_map = 'M' and (almost.eddate - almost.dcmmatchdate >= 0 or almost.dcmmatchdate - almost.stdate >= 0) and
               (almost.costmethod = 'CPM' or almost.costmethod = 'CPMV' or almost.costmethod = 'CPE'))
             then ((sum(cast(mt.groupm_billable_impressions as decimal(10,2))) * cast(almost.rate as decimal(10,2)) / 1000))

         else 0 end                                                                 as cost,
         almost.rate                                                                as rate,
         sum(almost.incrflatcost)                                                   as incrflatcost,
         sum(case
             --     not subject to viewability
             when (almost.dv_map = 'N')
               then almost.vew_rev + almost.clk_rev

             --     subject to viewability with flag; mt source
             when (almost.dv_map = 'Y' and (len(isnull(mt.joinkey,''))>0))
               then ((almost.vew_rev) *
                    (cast(mt.groupm_passed_impressions as decimal) /
                      nullif(cast(mt.total_impressions as decimal),0))) + almost.clk_rev
             --     subject to viewability; dv source
             when (almost.dv_map = 'Y')
               then ((almost.vew_rev) *
                    (cast(dv.groupm_passed_impressions as decimal) /
                      nullif(cast(dv.total_impressions as decimal),0))) + almost.clk_rev
             --     subject to viewability; moat source
             when (almost.dv_map = 'M')
               then ((almost.vew_rev) *
                    (cast(mt.groupm_passed_impressions as decimal) /
                      nullif(cast(mt.total_impressions as decimal),0))) + almost.clk_rev
             else 0 end)                                                            as viewrevenue,


         sum(case

--         not subject to viewability
             when (almost.dv_map = 'N')
               then cast((almost.vew_rev * .2 * .15) + almost.clk_rev as decimal(10,2))

--         subject to viewability with flag; mt source
             when (almost.dv_map = 'Y' and (len(isnull(mt.joinkey,''))>0))
               then cast(
             (((almost.vew_rev) *
                          (cast(mt.groupm_passed_impressions as decimal) /
                            nullif(cast(mt.total_impressions as decimal),0))) * .2 * .15) + almost.clk_rev as decimal(10,2))

--         subject to viewability; dv source
             when (almost.dv_map = 'Y')
               then cast(
             (((almost.vew_rev) *
                (cast(dv.groupm_passed_impressions as decimal) /
                            nullif(cast(dv.total_impressions as decimal),0))) * .2 * .15) + almost.clk_rev as decimal(10,2))

--         subject to viewability; moat source
             when (almost.dv_map = 'M')
               then cast(
             (((almost.vew_rev) *
                          (cast(mt.groupm_passed_impressions as decimal) /
                            nullif(cast(mt.total_impressions as decimal),0))) * .2 * .15) + almost.clk_rev as decimal(10,2))
             else 0 end)                                                            as adjsrevenue,

--       total impressions as reported by 1.) dcm for "n," 2.) dv for "y," or moat for "m"
         sum(case
         when almost.dv_map = 'Y' and (len(isnull(mt.joinkey,''))>0) then mt.total_impressions
         when almost.dv_map = 'Y' then dv.total_impressions
             when almost.dv_map = 'M' then mt.total_impressions
         when almost.dv_map = 'N' and (len(isnull(iv.joinkey,''))>0) then iv.impressions
             else almost.impressions end)                                           as dlvrimps,

--       billable impressions as reported by 1.) dcm for "n," 2.) dv for "y," or moat for "m"
         sum(case
         when almost.dv_map = 'Y' and (len(isnull(mt.joinkey,''))>0) then mt.groupm_billable_impressions
         when almost.dv_map = 'Y' then dv.groupm_billable_impressions
             when almost.dv_map = 'M' then mt.groupm_billable_impressions
         when almost.dv_map = 'N' and (len(isnull(iv.joinkey,''))>0) then iv.impressions
             else almost.impressions end)                                           as billimps,

--       dcm impressions (for comparison (qa) to dcm console)
         sum(almost.impressions)                                                    as cnslimps,
       sum(iv.impressions)                            as iv_impressions,
       sum(iv.click_thrus)                            as iv_clicks,
         sum(iv.all_completion) as iv_completes,
         sum(cast(dv.total_impressions as int))                                     as dv_impressions,
         sum(dv.groupm_passed_impressions)                                          as dv_viewed,
         sum(cast(dv.groupm_billable_impressions as decimal(10,2)))                 as dv_groupmpayable,
         sum(cast(mt.total_impressions as int))                                     as mt_impressions,
         sum(mt.groupm_passed_impressions)                                          as mt_viewed,
         sum(cast(mt.groupm_billable_impressions as decimal(10,2)))                 as mt_groupmpayable,
--       clicks
         sum(case
         when (len(isnull(iv.joinkey,''))>0) then iv.click_thrus
             else almost.clicks end)                                              as clicks,
         sum(almost.vew_conv)                                                 as vew_conv,
         sum(almost.clk_conv)                                                as clk_conv,
         sum(almost.conv)                                                           as conv,
         sum(almost.vew_tix)                                              as vew_tix,
         sum(almost.clk_tix)                                             as clk_tix,
         sum(almost.tickets)                                                        as tickets,
         sum(almost.vew_rev)                                              as vew_rev,
         sum(almost.clk_rev)                                             as clk_rev,
         sum(almost.revenue)                                                        as revenue

       from
         (
-- =========================================================================================================================
-- --
-- declare @report_st date,
-- @report_ed date;
-- --
-- set @report_ed = '2016-10-21';
-- set @report_st = '2016-10-10';
                     select
             dcmreport.dcmdate as dcmdate,
             cast(month(cast(dcmreport.dcmdate as date)) as int) as dcmmonth,
             [dbo].udf_dateToInt(dcmreport.dcmdate) as dcmmatchdate,
           dcmreport.campaign             as campaign,
           dcmreport.campaign_id        as campaign_id,
           dcmreport.site_dcm  as site_dcm,
           dcmreport.site_id_dcm         as site_id_dcm,
     case when dcmReport.PlacementNumber in('PBKB7J', 'PBKB7H', 'PBKB7K') then 'PBKB7J' end as PlacementNumber,

           case when dcmReport.placement like 'PBKB7J%' or dcmReport.placement like 'PBKB7H%' or dcmReport.placement like 'PBKB7K%' or dcmReport.placement ='United 360 - Polaris 2016 - Q4 - Amobee'        then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID' else  dcmReport.placement end     as placement,
--        amobee video 360 placements, tracked differently across dcm, innovid, and moat; this combines the three placements into one
          case when dcmreport.placement_id in (137412510, 137412401, 137412609) then 137412609 else dcmreport.placement_id end as placement_id,
           prisma.stdate             as stdate,
           case when dcmreport.campaign_id = 9923634 and dcmreport.site_id_dcm != 1190258 then 20161022 else
           prisma.eddate end         as eddate,
           prisma.packagecat         as packagecat,
           prisma.costmethod         as costmethod,
           prisma.cost_id            as cost_id,
           prisma.planned_amt        as planned_amt,
--           prisma.planned_cost       as planned_cost,
           prisma.placementstart     as placementstart,
           case when dcmreport.campaign_id = 9923634 and dcmreport.site_id_dcm != 1190258 then '2016-10-22' else
           prisma.placementend end   as placementend,

--        flat.flatcostremain                                                               as flatcostremain,
--        flat.impsremain                                                                   as impsremain,
             sum(( cast(dcmreport.impressions as decimal(10,2)) / nullif(cast(prisma.planned_amt as decimal(10,2)),0) ) * cast(prisma.rate as
                                                                                                                     decimal(10,2)))                as incrflatcost,
           cast(prisma.rate as decimal(10,2))                                   as rate,
           sum(dcmreport.impressions)                                           as impressions,
           sum(dcmreport.clicks)                                                as clicks,
           sum(dcmreport.vew_conv)                                        as vew_conv,
           sum(dcmreport.clk_conv)                                       as clk_conv,
           sum(dcmreport.vew_conv) + sum(dcmreport.clk_conv)       as conv,
           sum(dcmreport.vew_tix)                                     as vew_tix,
           sum(dcmreport.clk_tix)                                    as clk_tix,
           sum(dcmreport.vew_tix) + sum(dcmreport.clk_tix) as tickets,
           sum(cast(dcmreport.vew_rev as decimal(10,2)))              as vew_rev,
           sum(cast(dcmreport.clk_rev as decimal(10,2)))             as clk_rev,
           sum(cast(dcmreport.revenue as decimal(10,2)))                        as revenue,
             case when cast(month(prisma.placementend) as int) - cast(month(cast(dcmreport.dcmdate as date)) as int) <= 0 then 0
             else cast(month(prisma.placementend) as int) - cast(month(cast(dcmreport.dcmdate as date)) as int) end as diff,

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
                  ( dcmReport.placement like '%[Mm][Oo][Bb][Ii][Ll][Ee]%' or dcmReport.placement like '%[Vv][Ii][Dd][Ee][Oo]%' or dcmReport.placement like '%[Pp][Rr][Ee]%[Rr][Oo][Ll][Ll]%' or dcmReport.Site_ID = '1995643'
--              or dcmReport.Site_ID = '1474576'
              or dcmReport.Site_ID = '2854118')
               then 'M'
--           Look for viewability flags Investment began including in placement names 6/16.
           when dcmReport.placement like '%[_]DV[_]%' then 'Y'
           when dcmReport.placement like '%[_]MOAT[_]%' then 'M'
           when dcmReport.placement like '%[_]NA[_]%' then 'N'
--
           when Prisma.CostMethod = 'CPMV' and Prisma.DV_Map = 'N' then 'Y'
             else Prisma.DV_Map end as DV_Map,

--           Prisma.DV_Map as DV_Map,
             SUBSTRING(dcmReport.placement,( CHARINDEX(dcmReport.placement,'_UAC_') + 12 ),
                       3)                                                                                                                           as campaignShort,
             case when SUBSTRING(dcmReport.placement,( CHARINDEX(dcmReport.placement,'_UAC_') + 12 ),3) =
                       'TMK'
               then 'Acquisition'
             else 'Non-Acquisition' end as campaignType


-- ==========================================================================================================================================================

-- openquery function call must not exceed 8,000 characters; no room for comments inside the function
    from (
           select *
           from openquery(verticaunited,
                          'select
cast(report.date as date)                   as dcmdate,
cast(month(cast(report.date as date)) as int) as reportmonth,
campaign.campaign                                as campaign,
report.campaign_id                               as campaign_id,
report.site_id_dcm as site_id_dcm,
directory.site_dcm                    as site_dcm,
  left(placements.placement,6) as ''placementnumber'',
placements.placement                   as placement,
report.placement_id                         as placement_id,
sum(report.impressions)                     as impressions,
sum(report.clicks)                          as clicks,
sum(report.vew_conv)                  as vew_conv,
sum(report.clk_conv)                 as clk_conv,
sum(report.vew_tix)               as vew_tix,
sum(report.clk_tix)              as clk_tix,
sum(cast(report.vew_rev as decimal(10, 2)))                   as vew_rev,
sum(cast(report.clk_rev as decimal(10, 2)))                   as clk_rev,
sum(cast(report.revenue as decimal(10, 2))) as revenue
from (


select
cast(timestamp_trunc(to_timestamp(ta.interaction_time / 1000000), ''SS'') as date) as "date"
,ta.campaign_id                                                                       as campaign_id
,ta.site_id_dcm                                                                        as site_id_dcm
,ta.placement_id                                                                        as placement_id
,0                                                                                          as impressions
,0                                                                                          as clicks
,sum(case when ta.conversion_id = 1 then 1 else 0 end)                                              as clk_conv
,sum(case when ta.conversion_id = 1 then ta.total_conversions else 0 end)                           as clk_tix
,sum(case when ta.conversion_id = 1 then (ta.total_revenue * 1000000) / (rates.exchange_rate) else 0 end)  as clk_rev
,sum(case when ta.conversion_id = 2 then 1 else 0 end)                                              as vew_conv
,sum(case when ta.conversion_id = 2 then ta.total_conversions else 0 end)                           as vew_tix
,sum(case when ta.conversion_id = 2 then (ta.total_revenue * 1000000) / (rates.exchange_rate) else 0 end)  as vew_rev
,sum(ta.total_revenue * 1000000/rates.exchange_rate)                                               as revenue

from
(
select *
from diap01.mec_us_united_20056.dfa2_activity
where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000), ''SS'') as date) between ''2016-12-29'' and ''2016-12-31''
and upper(substring(other_data, (instr(other_data,''u3='')+3), 3)) != ''mil''
and substring(other_data, (instr(other_data,''u3='')+3), 5) != ''miles''
and total_revenue != 0
and total_conversions != 0
and activity_id = 978826
and campaign_id in (9304728, 9407915, 9408733, 9548151, 9630239, 9639387, 9739006, 9923634, 9973506, 9994694, 9999841, 10094548, 10276123, 10121649, 10307468, 10090315, 10505745) -- display 2016
and (advertiser_id <> 0)
) as ta

left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
on upper(substring(other_data, (instr(other_data,''u3='')+3), 3)) = upper(rates.currency)
and cast(timestamp_trunc(to_timestamp(interaction_time / 1000000), ''SS'') as date) = rates.date

group by
-- ta.click_time
cast(timestamp_trunc(to_timestamp(ta.interaction_time / 1000000), ''SS'') as date)
,ta.campaign_id
,ta.site_id_dcm
,ta.placement_id


union all

select
-- ti.impression_time as "date"
cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000), ''SS'') as date) as "date"
,ti.campaign_id                 as campaign_id
,ti.site_id_dcm                  as site_id_dcm
,ti.placement_id                  as placement_id
,count(*)                             as impressions
,0                                    as clicks
,0                                    as clk_conv
,0                                    as clk_tix
,0                                    as clk_rev
,0                                    as vew_conv
,0                                    as vew_tix
,0                                    as vew_rev
,0                                    as revenue

from  (
select *
from diap01.mec_us_united_20056.dfa2_impression
where cast(timestamp_trunc(to_timestamp(event_time / 1000000), ''SS'') as date) between ''2016-12-29'' and ''2016-12-31''
and campaign_id in (9304728, 9407915, 9408733, 9548151, 9630239, 9639387, 9739006, 9923634, 9973506, 9994694, 9999841, 10094548, 10276123, 10121649, 10307468, 10090315, 10505745) -- display 2016

and (advertiser_id <> 0)
) as ti
group by
-- ti.impression_time
cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000), ''SS'') as date)
,ti.campaign_id
,ti.site_id_dcm
,ti.placement_id

union all

select
-- tc.click_time       as "date"
cast(timestamp_trunc(to_timestamp(tc.event_time / 1000000), ''SS'') as date)       as "date"
,tc.campaign_id                      as campaign_id
,tc.site_id_dcm                       as site_id_dcm
,tc.placement_id                       as placement_id
,0                                    as impressions
,count(*)                             as clicks
,0                                    as clk_conv
,0                                    as clk_tix
,0                                    as clk_rev
,0                                    as vew_conv
,0                                    as vew_tix
,0                                    as vew_rev
,0                                    as revenue

from  (

select *
from diap01.mec_us_united_20056.dfa2_click
where cast(timestamp_trunc(to_timestamp(event_time / 1000000), ''SS'') as date) between ''2016-12-29'' and ''2016-12-31''
and campaign_id in (9304728, 9407915, 9408733, 9548151, 9630239, 9639387, 9739006, 9923634, 9973506, 9994694, 9999841, 10094548, 10276123, 10121649, 10307468, 10090315, 10505745) -- display 2016
and (advertiser_id <> 0)
) as tc

group by
-- tc.click_time
cast(timestamp_trunc(to_timestamp(tc.event_time / 1000000), ''SS'') as date)
,tc.campaign_id
,tc.site_id_dcm
,tc.placement_id

) as report

left join
(
select cast(campaign as varchar(4000)) as ''campaign'', campaign_id as ''campaign_id''
from diap01.mec_us_united_20056.dfa2_campaigns
) as campaign
on report.campaign_id = campaign.campaign_id

left join
(
select cast(t1.placement as varchar(4000)) as ''placement'',  t1.placement_id as ''placement_id'', t1.campaign_id as ''campaign_id'', t1.site_id_dcm as ''site_id_dcm''

from (select campaign_id as campaign_id, site_id_dcm as site_id_dcm, placement_id as placement_id, placement as placement, cast(placement_start_date as date) as thisdate,
  row_number() over (partition by campaign_id, site_id_dcm, placement_id  order by cast(placement_start_date as date) desc) as r1
    from diap01.mec_us_united_20056.dfa2_placements

) as t1
where r1 = 1
) as placements
on  report.placement_id   = placements.placement_id
and report.campaign_id = placements.campaign_id
and report.site_id_dcm  = placements.site_id_dcm

left join
(
select cast(site_dcm as varchar(4000)) as ''site_dcm'', site_id_dcm as ''site_id_dcm''
from diap01.mec_us_united_20056.dfa2_sites
) as directory
on report.site_id_dcm = directory.site_id_dcm

where not regexp_like(placement,''.do\s*not\s*use.'',''ib'')
and report.site_id_dcm !=''1485655''
group by
cast(report.date as date)
-- , cast(month(cast(report.date as date)) as int)
, directory.site_dcm
,report.site_id_dcm
, report.campaign_id
, campaign.campaign
, report.placement_id
, placements.placement
-- , placements.placementnumber
')

         ) as dcmreport


      left join
      (
        select *
        from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].summarytable
      ) as prisma
        on dcmreport.placement_id = prisma.adserverplacementid

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
--      ,prisma.planned_cost
      ,prisma.placementend
      ,prisma.placementstart
      ,prisma.stdate
      ,prisma.eddate
--      ,dv.joinkey
--      ,flat.flatcostremain
--      ,flat.impsremain
      ,prisma.dv_map
  ) as almost

  left join
  (
    select *
    from master.dbo.flattableday
  ) as flat
    on almost.cost_id = flat.cost_id
       and almost.dcmmatchdate = flat.dcmdate

-- dv table join ==============================================================================================================================================

  left join (
              select *
              from master.dbo.dvtable
              where dvdate between @report_st and @report_ed
            ) as dv
      on
      left(almost.placement,6) + '_' + [dbo].udf_sitekey(almost.site_dcm) + '_'
      + cast(almost.dcmdate as varchar(10)) = dv.joinkey

-- moat table join ==============================================================================================================================================

  left join (
              select *
              from master.dbo.mttable
              where mtdate between @report_st and @report_ed
            ) as mt
      on
      left(almost.placement,6) + '_' + [dbo].udf_sitekey( almost.site_dcm) + '_'
      + cast(almost.dcmdate as varchar(10)) = mt.joinkey


-- innovid table join ==============================================================================================================================================

  left join (
              select *
              from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].innovidexttable
              where ivdate between @report_st and @report_ed
            ) as iv
      on
      left(almost.placement,6) + '_' + [dbo].udf_sitekey( almost.site_dcm) + '_'
      + cast(almost.dcmdate as varchar(10)) = iv.joinkey


--  where almost.costmethod = 'flat'
--     where almost.site_id_dcm ='1853562'
group by

  almost.campaign
  ,almost.campaign_id
  ,almost.cost_id
  ,almost.dv_map
  ,almost.site_dcm
  ,almost.site_id_dcm
  ,almost.packagecat
  ,almost.placementend
  ,almost.placementstart
  ,almost.placementnumber
  ,almost.placement_id
  ,almost.planned_amt
--  ,almost.planned_cost
  ,almost.rate
  ,almost.placement
  ,almost.eddate
  ,almost.stdate
  ,almost.campaignshort
  ,almost.campaigntype
  ,almost.dcmdate
  ,almost.dcmmatchdate
  ,almost.dcmmonth
  ,almost.diff
  ,dv.joinkey
  ,mt.joinkey
  ,iv.joinkey
-- ,almost.flatcostremain
-- ,almost.impsremain
  ,almost.costmethod
  ,flat.flatcostremain
  ,flat.impsremain
  ,flat.flatcost

     ) as final

--  where (final.dvjoinkey is null or final.mtjoinkey is null) and (final.dv_map = 'Y' or final.dv_map = 'M')
--  where (final.dvjoinkey is null and final.dv_map = 'Y') or (final.mtjoinkey is null and final.dv_map = 'M')
--    where final.mtjoinkey is null and  final.dv_map = 'M'
--  where final.site_dcm like '%[bb]usiness%[ii]nsider%'
--        where final.site_dcm like '%verve%' or final.site_dcm like '%xad%'
--     where final.site_id_dcm ='1853562'
--     where final.campaign_id ='10276123'

--  where final.site_id_dcm = '1853564' and final.dv_map = 'Y'
--  where final.costmethod = 'cpc'
  --  where final.costmethod = 'flat'


group by
  final.dcmdate
  ,final.dcmmonth
  ,final.diff
  ,final.dvjoinkey
  ,final.mtjoinkey
  ,final.packagecat
  ,final.cost_id
  ,final.campaign
  ,final.campaign_id
  ,final.newcount
  ,final.campaignshort
  ,final.campaigntype
  ,final.site_dcm
  ,final.site_id_dcm
  ,final.costmethod
  ,final.rate
-- ,final.placementnumber
  ,final.placement
  ,final.placement_id
  ,final.placementend
  ,final.placementstart
  ,final.dv_map
  ,final.planned_amt
--  ,final.planned_cost
-- ,final.flatcostremain
-- ,final.impsremain

  ,final.flatcost


order by
  final.cost_id,
  final.dcmdate;
