

--      Version incorporating capped cost and DBM cost
--
--  this query is a bit of a hack. non-optimal aspects are necessitated by the particularities of the current tech stack on united.
--   code is most easily read by starting at the "innermost" block, inside the openquery call.
--
--   data must be pulled and reconciled from 1) prisma and moat in datamart, and 2) dfa/dv/moat in vertica*.
--   because of its scale, log-level dfa data (dtf 1.0) must be kept in vertica to preserve performance. dv and moat are stored there as well, mostly for convenience.
--   intermediary summary tables are necessary for this query, so we need to be able to create our own tables and run stored procedures to refresh those tables. but we don't have write access to vertica, so we can't keep routines and tables there.
--   di could do this, but edits to these routines are frequent (esp. in joinkey fields), so keeping this process in-house is more convenient for all parties.
-- */

-- these summary/reference tables can be run once a day as a regular process or before the query is run
-- -- --
-- exec master.dbo.crt_dv_summ go    -- crt_ separate dv aggregate table and store it in my instance; joining to the vertica table in the query
-- exec master.dbo.crt_mt_summ go    -- crt_ separate moat aggregate table and store it in my instance; joining to the vertica table in the query
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_ivd_summTbl go
--
-- exec [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.crt_prs_viewTbl go
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_prs_amttbl go
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_prs_packtbl go
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_prs_summtbl go
-- exec master.dbo.crt_dfa_flatCost_dt2 go
-- exec master.dbo.crt_dbm_cost go
-- exec master.dbo.crt_dfa_cost_dt2 go



declare @report_st date
declare @report_ed date
--
set @report_ed = '2017-05-08';
set @report_st = '2017-02-01';

--
-- set @report_ed = dateadd(day, -datepart(day, getdate()), getdate());
-- set @report_st = dateadd(day, 1 - datepart(day, @report_ed), @report_ed);

select
-- dcm ad server date
    cast(t3.dcmdate as date)                                                                           as "date",
-- dcm ad server week (from date)
    cast(dateadd(week,datediff(week,0,cast(t3.dcmdate as date)),0) as date)                            as "week",
-- dcm ad server month (from date)
    datename(month,cast(t3.dcmdate as date))                                                           as "month",
-- dcm ad server quarter + year (from date)
    'Q' + datename(quarter,cast(t3.dcmdate as date)) + ' ' + datename(year,cast(t3.dcmdate as date))   as "quarter",
-- reference/optional: difference, in months, between placement end date and report date. field is used deterministically in other fields below.
--  t3.diff                                                                                              as diff,
-- reference/optional: match key from the dv table; only present when dv data is available.
    t3.dvjoinkey                                                                                       as dvjoinkey,
-- reference/optional: match key from the moat table; only present when moat data is available.
    t3.mtjoinkey                                                                                       as mtjoinkey,
-- reference/optional: package category from prisma (standalone; package; child). useful for exchanging info with planning/investment
    t3.packagecat                                                                                      as packagecat,
-- reference/optional: first six characters of package-level placement name, used to join 1) prisma table, and 2) flat fee table
    t3.cost_id                                                                                         as cost_id,
-- dcm campaign name
--  t3.campaign                                                                                               as "dcm campaign",
-- friendly campaign name
    [dbo].udf_campaignname(t3.campaign_id,t3.campaign)                                                 as campaign,
-- dcm campaign id
    t3.campaign_id,
--campaign type: Acquisition, Branding/Routes, Added Value                                                                                       as "campaign id",
    case when campaign_id = '10742878' then 'Acquisition'
    when campaign_id = '10918234' or campaign_id = '10942240' or campaign_id = '10768497' or campaign_id = '11069476' then 'Branding/Routes'
    when campaign_id = '10740457' or campaign_id = '10812738' then 'Added Value'
    else 'non-Acquisition' end                                                                         as "campaign_type",

    t3.site_dcm as site_orig,
-- preferred, friendly site name; also corresponds to what's used in the joinkey fields across dfa, dv, and moat.
    [dbo].udf_sitename(t3.site_dcm)                                                                    as "site",

-- dcm site id
    t3.site_id_dcm                                                                                     as "site id",
-- reference/optional: package cost/pricing model, from prisma; attributed to all placements within package.
    t3.costmethod                                                                                      as "cost method",
-- reference/optional: first six characters of placement name. used for matching across datasets when no common placement id is available, e.g. dfa-dv.
--  t3.plce_id                                                                       as plce_id,
-- dcm placement name
    t3.placement                                                                                       as placement,
-- dcm placement id
    t3.placement_id                                                                                    as placement_id,
-- reference/optional: planned package end date, from prisma; attributed to all placements within package.
    t3.placementend                                                                                    as "placement end",
    t3.placementstart                                                                                  as "placement start",
    t3.dv_map                                                                                          as "dv map",
    t3.rate                                                                                            as rate,
    t3.planned_amt                                                                                     as "planned amt",
    t3.planned_cost                                                                                    as "planned cost",
    t3.planned_cost / max(t3.amt_count)                                                                as planned_cost,
--  old field, with integrated flatCost
--  case when t3.costmethod like '[Ff]lat' then t3.flatcost / max(t3.flat_count) else sum(t3.cost) end as cost,
--  new field, with flat cost from dfa_cost_dt2
    sum(case when t3.costmethod like '[Ff]lat' then t3.flatcost else t3.cost end)                      as cost,
    sum(t3.tot_led)                                                                                    as leads,
    sum(t3.dlvrimps)                                                                                   as "delivered impressions",
    sum(t3.billimps)                                                                                   as "billable impressions",
    sum(t3.cnslimps)                                                                                   as "dfa impressions",
    sum(t3.iv_impressions)                                                                             as "innovid impressions",
    sum(t3.clicks)                                                                                     as clicks,
--   case when sum(t3.dlvrimps) = 0 then 0
--   else (sum(cast(t3.clicks as decimal(20,10)))/sum(cast(t3.dlvrimps as decimal(20,10))))*100 end  as ctr,
    sum(t3.tot_con)                                                                                    as transactions,
    sum(t3.vew_con)                                                                                    as vew_trns,
    sum(t3.clk_con)                                                                                    as clck_thru_trns,
    sum(t3.tot_tix)                                                                                    as tix,
    sum(t3.vew_tix)                                                                                    as vew_tix,
    sum(t3.clk_tix)                                                                                    as clk_tix,
    sum(t3.tot_rev)                                                                                    as revenue,
    sum(t3.vew_rev)                                                                                    as vew_rev,
    sum(t3.clk_rev)                                                                                    as clk_thru_rev,
    sum(t3.billrevenue)                                                                                as "billable revenue",
    sum(t3.adjsrevenue)                                                                                as "adjusted (final) revenue"
from (

-- for running the code here instead of at "f3," above

-- declare @report_st date,
-- @report_ed date;
-- --
-- set @report_ed = '2017-05-08';
-- set @report_st = '2017-02-01';

select
    cast(t2.dcmdate as date)                                                   as dcmdate,
    t2.dcmmonth                                                                as dcmmonth,
    t2.dcmmatchdate                                                            as dcmmatchdate,
    t2.diff                                                                    as diff,
    dv.joinkey                                                                 as dvjoinkey,
    mt.joinkey                                                                 as mtjoinkey,
    iv.joinkey                                                                 as ivjoinkey,
    t2.packagecat                                                              as packagecat,
    t2.cost_id                                                                 as cost_id,
    t2.campaign                                                                as campaign,
    t2.campaign_id                                                             as campaign_id,
    t2.site_dcm                                                                as site_dcm,
    t2.site_id_dcm                                                             as site_id_dcm,
    t2.costmethod                                                              as costmethod,
    sum(1) over (partition by t2.cost_id,t2.dcmmatchdate
        order by t2.dcmmonth asc range between unbounded preceding and current row) as flat_count,
    sum(1) over (partition by t2.cost_id
        order by t2.dcmmonth asc range between unbounded preceding and current row) as amt_count,
    t2.plce_id                                                                 as plce_id,
--     cst.plce_id as dbm_plce_id,
    t2.placement                                                               as placement,
    t2.placement_id                                                            as placement_id,
    t2.placementend                                                            as placementend,
    t2.placementstart                                                          as placementstart,
    t2.dv_map                                                                  as dv_map,
--     t2.dv_map_2                                                                  as dv_map_2,
    t2.planned_amt                                                             as planned_amt,
    t2.planned_cost                                                            as planned_cost,
--  old field, with integrated flatCost
--  flt.flatcost                                                               as flatcost,
--  new field, with flat cost from dfa_cost_dt2
    sum(cst.flatcost)                                                          as flatcost,
    sum(cst.cost)                                                              as cost,
    t2.rate                                                                    as rate,

--  Billable revenue without United discounts applied
    sum(case
--         not subject to viewability, DBM
             when (t2.dv_map = 'N' and t2.costmethod = 'dCPM')
               then cast((cst.vew_rev) + cst.clk_rev as decimal(10,2))

--         not subject to viewability
             when (t2.dv_map = 'N')
               then cast((t2.vew_rev) + t2.clk_rev as decimal(10,2))

--         Win NY TapAd placements, which Medialets failed to tag
--         using average viewability rate for Feb, Mar, Apr
             when (
                    (t2.dv_map = 'Y' and t2.site_id_dcm = 2854118 and (len(isnull(dv.joinkey,''))=0)) or
                    (t2.dv_map = 'M' and t2.site_id_dcm = 2854118 and (len(isnull(mt.joinkey,''))=0))
                  )
             then
                    case
                    when t2.dcmmonth = 2
                    then cast(((t2.vew_rev) * .41) + t2.clk_rev as decimal(10,2))
                    when t2.dcmmonth = 3
                    then cast(((t2.vew_rev) * .48) + t2.clk_rev as decimal(10,2))
                    when t2.dcmmonth = 4
                    then cast(((t2.vew_rev) * .56) + t2.clk_rev as decimal(10,2))
                    end

--         Win NY Verve placements, which Medialets failed to tag
--         using average viewability rate for Feb, Mar, Apr
             when (
                    (t2.dv_map = 'Y' and t2.site_id_dcm = 1995643 and (len(isnull(dv.joinkey,''))=0)) or
                    (t2.dv_map = 'M' and t2.site_id_dcm = 1995643 and (len(isnull(mt.joinkey,''))=0))
                  )
             then
                    case
                    when t2.dcmmonth = 2
                    then cast(((t2.vew_rev) * .59) + t2.clk_rev as decimal(10,2))
                    when t2.dcmmonth = 3
                    then cast(((t2.vew_rev) * .77) + t2.clk_rev as decimal(10,2))
                    when t2.dcmmonth = 4
                    then cast(((t2.vew_rev) * .79) + t2.clk_rev as decimal(10,2))
                    end
--
--         Win NY Forbes placements, which Medialets failed to tag
--         using average viewability rate for Feb, Mar, Apr
             when (
                    (t2.dv_map = 'Y' and t2.site_id_dcm = 1485655 and (len(isnull(dv.joinkey,''))=0)) or
                    (t2.dv_map = 'M' and t2.site_id_dcm = 1485655 and (len(isnull(mt.joinkey,''))=0))
                  )
             then
                    case
                    when t2.dcmmonth = 2
                    then cast(((t2.vew_rev) * .38) + t2.clk_rev as decimal(10,2))
                    when t2.dcmmonth = 3
                    then cast(((t2.vew_rev) * .59) + t2.clk_rev as decimal(10,2))
                    when t2.dcmmonth = 4
                    then cast(((t2.vew_rev) * .64) + t2.clk_rev as decimal(10,2))
                    end

--         Win NY Ninth Decimal placements, which Medialets failed to tag
--         using average viewability rate for Feb, Mar, Apr
             when (
                    (t2.dv_map = 'Y' and t2.site_id_dcm = 1329066 and (len(isnull(dv.joinkey,''))=0)) or
                    (t2.dv_map = 'M' and t2.site_id_dcm = 1329066 and (len(isnull(mt.joinkey,''))=0))
                  )
             then
                    case
                    when t2.dcmmonth = 2
                    then cast(((t2.vew_rev) * .78) + t2.clk_rev as decimal(10,2))
                    when t2.dcmmonth = 3
                    then cast(((t2.vew_rev) * .89) + t2.clk_rev as decimal(10,2))
                    when t2.dcmmonth = 4
                    then cast(((t2.vew_rev) * .68) + t2.clk_rev as decimal(10,2))
                    end
--         Win NY NewYorkMagazine placements, which Medialets failed to tag
--         using average viewability rate for Feb, Mar, Apr

             when (
                    (t2.dv_map = 'Y' and t2.site_id_dcm = 3246841 and (len(isnull(dv.joinkey,''))=0)) or
                    (t2.dv_map = 'M' and t2.site_id_dcm = 3246841 and (len(isnull(mt.joinkey,''))=0))
                  )
             then
                    case
                    when t2.dcmmonth = 2
                    then cast(((t2.vew_rev) * .54) + t2.clk_rev as decimal(10,2))
                    when t2.dcmmonth = 3
                    then cast(((t2.vew_rev) * .62) + t2.clk_rev as decimal(10,2))
                    when t2.dcmmonth = 4
                    then cast(((t2.vew_rev) * .64) + t2.clk_rev as decimal(10,2))
                    end


--         subject to viewability with flag; mt source
             when (t2.dv_map = 'Y' and (len(isnull(mt.joinkey,''))>0))
               then cast(
             (((t2.vew_rev) *
                          (cast(mt.groupm_passed_impressions as decimal) /
                            nullif(cast(mt.total_impressions as decimal),0)))) + t2.clk_rev as decimal(10,2))

--         subject to viewability; dv source
             when (t2.dv_map = 'Y')
               then cast(
             (((t2.vew_rev) *
                (cast(dv.groupm_passed_impressions as decimal) /
                            nullif(cast(dv.total_impressions as decimal),0)))) + t2.clk_rev as decimal(10,2))

--         subject to viewability; moat source
             when (t2.dv_map = 'M')
               then cast(
             (((t2.vew_rev) *
                          (cast(mt.groupm_passed_impressions as decimal) /
                            nullif(cast(mt.total_impressions as decimal),0)))) + t2.clk_rev as decimal(10,2))
             else 0 end)                                                            as billrevenue,

--         Billable revenue with United discounts applied
               sum(case
--         not subject to viewability, DBM
             when (t2.dv_map = 'N' and t2.costmethod = 'dCPM')
               then cast((cst.vew_rev + cst.clk_rev) * .2 * .15 as decimal(10,2))

--         not subject to viewability
             when (t2.dv_map = 'N')
               then cast((t2.vew_rev + t2.clk_rev) * .2 * .15 as decimal(10,2))

--         Win NY TapAd placements, which Medialets failed to tag
--         using average viewability rate for Feb, Mar, Apr

             when (
                    (t2.dv_map = 'Y' and t2.site_id_dcm = 2854118 and (len(isnull(dv.joinkey,''))=0)) or
                    (t2.dv_map = 'M' and t2.site_id_dcm = 2854118 and (len(isnull(mt.joinkey,''))=0))
                  )
             then
                    case
                    when t2.dcmmonth = 2
                    then cast( ( (t2.vew_rev * .41) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    when t2.dcmmonth = 3
                    then cast( ( (t2.vew_rev * .48) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    when t2.dcmmonth = 4
                    then cast( ( (t2.vew_rev * .56) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    end
--         Win NY Verve placements, which Medialets failed to tag
--         using average viewability rate for Feb, Mar, Apr
             when (
                    (t2.dv_map = 'Y' and t2.site_id_dcm = 1995643 and (len(isnull(dv.joinkey,''))=0)) or
                    (t2.dv_map = 'M' and t2.site_id_dcm = 1995643 and (len(isnull(mt.joinkey,''))=0))
                  )
             then
                    case
                    when t2.dcmmonth = 2
                    then cast( ( (t2.vew_rev * .59) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    when t2.dcmmonth = 3
                    then cast( ( (t2.vew_rev * .77) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    when t2.dcmmonth = 4
                    then cast( ( (t2.vew_rev * .79) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    end

--         Win NY Forbes placements, which Medialets failed to tag
--         using average viewability rate for Feb, Mar, Apr

             when (
                    (t2.dv_map = 'Y' and t2.site_id_dcm = 1485655 and (len(isnull(dv.joinkey,''))=0)) or
                    (t2.dv_map = 'M' and t2.site_id_dcm = 1485655 and (len(isnull(mt.joinkey,''))=0))
                  )
             then
                    case
                    when t2.dcmmonth = 2
                    then cast( ( (t2.vew_rev * .38) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    when t2.dcmmonth = 3
                    then cast( ( (t2.vew_rev * .59) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    when t2.dcmmonth = 4
                    then cast( ( (t2.vew_rev * .64) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    end

--         Win NY Ninth Decimal placements, which Medialets failed to tag
--         using average viewability rate for Feb, Mar, Apr
             when (
                    (t2.dv_map = 'Y' and t2.site_id_dcm = 1329066 and (len(isnull(dv.joinkey,''))=0)) or
                    (t2.dv_map = 'M' and t2.site_id_dcm = 1329066 and (len(isnull(mt.joinkey,''))=0))
                  )
             then
                    case
                    when t2.dcmmonth = 2
                    then cast( ( (t2.vew_rev * .78) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    when t2.dcmmonth = 3
                    then cast( ( (t2.vew_rev * .89) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    when t2.dcmmonth = 4
                    then cast( ( (t2.vew_rev * .68) + t2.clk_rev) * .2 * .15 as decimal(10,2))

                    end

--         Win NY NewYorkMagazine placements, which Medialets failed to tag
--         using average viewability rate for Feb, Mar, Apr
             when (
                    (t2.dv_map = 'Y' and t2.site_id_dcm = 3246841 and (len(isnull(dv.joinkey,''))=0)) or
                    (t2.dv_map = 'M' and t2.site_id_dcm = 3246841 and (len(isnull(mt.joinkey,''))=0))
                  )
             then
                    case
                    when t2.dcmmonth = 2
                    then cast( ( (t2.vew_rev * .54) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    when t2.dcmmonth = 3
                    then cast( ( (t2.vew_rev * .62) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    when t2.dcmmonth = 4
                    then cast( ( (t2.vew_rev * .64) + t2.clk_rev) * .2 * .15 as decimal(10,2))
                    end

--         subject to viewability with flag; mt source
             when (t2.dv_map = 'Y' and (len(isnull(mt.joinkey,''))>0))
               then cast(
             (((t2.vew_rev) *
                          (cast(mt.groupm_passed_impressions as decimal) /
                            nullif(cast(mt.total_impressions as decimal),0))) + t2.clk_rev ) * .2 * .15 as decimal(10,2))

--         subject to viewability; dv source
             when (t2.dv_map = 'Y')
               then cast(
             (((t2.vew_rev) *
                (cast(dv.groupm_passed_impressions as decimal) /
                            nullif(cast(dv.total_impressions as decimal),0))) + t2.clk_rev ) * .2 * .15 as decimal(10,2))

--         subject to viewability; moat source
             when (t2.dv_map = 'M')
               then cast(
             (((t2.vew_rev) *
                          (cast(mt.groupm_passed_impressions as decimal) /
                            nullif(cast(mt.total_impressions as decimal),0))) + t2.clk_rev ) * .2 * .15 as decimal(10,2))
             else 0 end)                                                            as adjsrevenue,

    sum(case when t2.costmethod = 'Flat' then t2.impressions else cst.dlvrimps end) as dlvrimps,
    sum(case when t2.costmethod = 'Flat' then t2.impressions else cst.billimps end) as billimps,
--      dcm impressions (for comparison (qa) to dcm console)
--     sum(cst.dfa_imps)                                                             as cnslimps,
    sum(case when t2.costmethod = 'Flat' then t2.impressions else cst.dfa_imps end) as cnslimps,
    sum(iv.impressions)                                                           as iv_impressions,
    sum(cst.iv_imps)                                                              as iv_impressions_chk,
    sum(iv.click_thrus)                                                           as iv_clicks,
    sum(iv.all_completion)                                                        as iv_completes,
    sum(cast(dv.total_impressions as int))                                        as dv_impressions,
    sum(cst.dv_imps)                                                              as dv_impressions_chk,
    sum(dv.groupm_passed_impressions)                                             as dv_viewed,
    sum(cast(dv.groupm_billable_impressions as decimal(10,2)))                    as dv_groupmpayable,
    sum(cast(mt.total_impressions as int))                                        as mt_impressions,
    sum(cst.mt_imps)                                                              as mt_impressions_chk,
    sum(mt.groupm_passed_impressions)                                             as mt_viewed,
    sum(cast(mt.groupm_billable_impressions as decimal(10,2)))                    as mt_groupmpayable,
    sum(case
        when (len(isnull(iv.joinkey,'')) > 0) then iv.click_thrus
        else t2.clicks end)                                                       as clicks,
    sum(case when t2.costmethod = 'dCPM' then cst.clk_con else  t2.clk_con end)      as clk_con,
    sum(case when t2.costmethod = 'dCPM' then cst.clk_rev else  t2.clk_rev end)      as clk_rev,
    sum(case when t2.costmethod = 'dCPM' then cst.clk_tix else  t2.clk_tix end)      as clk_tix,
    sum(case when t2.costmethod = 'dCPM' then cst.con     else  t2.con     end)      as tot_con,
    sum(case when t2.costmethod = 'dCPM' then cst.rev     else  t2.rev     end)      as tot_rev,
    sum(case when t2.costmethod = 'dCPM' then cst.tix     else  t2.tix     end)      as tot_tix,
    sum(case when t2.costmethod = 'dCPM' then cst.vew_con else  t2.vew_con end)      as vew_con,
    sum(case when t2.costmethod = 'dCPM' then cst.vew_rev else  t2.vew_rev end)      as vew_rev,
    sum(case when t2.costmethod = 'dCPM' then cst.vew_tix else  t2.vew_tix end)      as vew_tix,
    sum(case when t2.costmethod = 'dCPM' then cst.clk_led else  t2.clk_led end)      as clk_led,
    sum(case when t2.costmethod = 'dCPM' then cst.vew_led else  t2.vew_led end)      as vew_led,
    sum(case when t2.costmethod = 'dCPM' then cst.led     else  t2.led     end)      as tot_led




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
               t1.dcmdate                                   as dcmdate,
               cast(month(cast(t1.dcmdate as date)) as int) as dcmmonth,
               [dbo].udf_dateToInt(t1.dcmdate)              as dcmmatchdate,
               t1.campaign                                  as campaign,
               t1.campaign_id                               as campaign_id,
               t1.site_dcm                                  as site_dcm,
               t1.site_id_dcm                               as site_id_dcm,
               case when t1.plce_id in ('PBKB7J','PBKB7H','PBKB7K') then 'PBKB7J'
               else t1.plce_id end                          as plce_id,
               case when t1.placement like 'PBKB7J%' or t1.placement like 'PBKB7H%' or t1.placement like 'PBKB7K%' or t1.placement = 'United 360 - Polaris 2016 - Q4 - Amobee' then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
               else t1.placement end                        as placement,
--        amobee video 360 placements, tracked differently across dcm, innovid, and moat; this combines the three placements into one
               case when t1.placement_id in (137412510,137412401,137412609) then 137412609
               else t1.placement_id end                     as placement_id,
               prs.stdate                                   as stdate,
               case when t1.campaign_id = 9923634 and t1.site_id_dcm != 1190258 then 20161022
               else prs.eddate end                          as eddate,
               prs.packagecat                               as packagecat,
               prs.costmethod                               as costmethod,
               prs.cost_id                                  as cost_id,
               prs.planned_amt                              as planned_amt,
               prs.planned_cost                             as planned_cost,
               prs.placementstart                           as placementstart,
               case when t1.campaign_id = 9923634 and t1.site_id_dcm != 1190258 then '2016-10-22'
               else prs.placementend end                    as placementend,
               cast(prs.rate as decimal(10,2))              as rate,
               sum(t1.impressions)                          as impressions,
               sum(t1.clicks)                               as clicks,
               sum(t1.vew_led)                              as vew_led,
               sum(t1.clk_led)                              as clk_led,
               sum(t1.led)                                  as led,
               sum(t1.vew_con)                              as vew_con,
               sum(t1.clk_con)                              as clk_con,
               sum(t1.con)                                  as con,
               sum(t1.vew_tix)                              as vew_tix,
               sum(t1.clk_tix)                              as clk_tix,
               sum(t1.tix)                                  as tix,
               sum(t1.vew_rev)                              as vew_rev,
               sum(t1.clk_rev)                              as clk_rev,
               sum(t1.rev)                                  as rev,
             case when cast(month(prs.placementend) as int) - cast(month(cast(t1.dcmdate as date)) as int) <= 0 then 0
             else cast(month(prs.placementend) as int) - cast(month(cast(t1.dcmdate as date)) as int) end as diff,

--                case
--                -- Flat-fee, cost-per-click, CPCV, and dynamic CPM should never be subject to viewability
--                when prs.costmethod = 'Flat' or prs.costmethod = 'CPC' or prs.costmethod = 'CPCV' or prs.costmethod = 'dCPM'
--                    then 'N'
--
--                -- Corrections to SME 2016
--                when t1.campaign_id = '10090315' and
--                -- Inc
--                    (t1.site_id_dcm = '1513807' or
--                -- Xaxis
--                     t1.site_id_dcm = '1592652')
--                    then 'Y'
--
--                -- Corrections to SFO-SIN 2016
--                when t1.campaign_id = '9923634' and
--                -- Business Insider
--                   ((t1.site_id_dcm = '1534879' and prs.costmethod = 'CPM') or
--                -- Live Intent
--                    (t1.site_id_dcm = '1853564'))
--                    then 'N'
--
--                -- FlipBoard unable to implement Moat tags; must bill off of DFA impressions
--                when t1.site_id_dcm = '2937979' then 'N'
--                -- All targeted marketing subject to viewability; mark "Y"
--                when t1.campaign_id = '9639387' then 'Y'
--
--                -- If it's CPMV and the placement has the words "Mobile," "Video," or "Pre-Roll," then it should be in Moat
--                when prs.CostMethod = 'CPMV' and
--                    (t1.placement like '%[Mm][Oo][Bb][Ii][Ll][Ee]%' or
--                     t1.placement like '%[Vv][Ii][Dd][Ee][Oo]%' or
--                     t1.placement like '%[Pp][Rr][Ee]%[Rr][Oo][Ll][Ll]%' or
--                -- Verve
--                     t1.site_id_dcm = '1995643'
--                ) then 'M'
--
--                -- Look for viewability flags Investment began to include in placement names 6/16.
--                when t1.placement like '%[_]DV[_]%' then 'Y'
--                when t1.placement like '%[_]MOAT[_]%' then 'M'
--                when t1.placement like '%[_]NA[_]%' then 'N'
--
--                -- If it's CPMV and marked "N," change to "Y"
--                when prs.costmethod = 'CPMV' and prs.DV_Map = 'N' then 'Y'
--                else prs.DV_Map end as DV_Map,

--             Because dv_map logic exists in more than one query, abstract it into function for easier updating
               [dbo].udf_dvMap(t1.campaign_id,t1.site_id_dcm,t1.placement,prs.CostMethod,prs.dv_map) as dv_map



-- ==========================================================================================================================================================

-- openquery function call must not exceed 8,000 characters; no room for comments inside the function
    from (
           select *
           from openQuery(verticaunited,
'
select
cast(r1.date as date)                     as dcmdate,
cast(month(cast(r1.date as date)) as int) as reportmonth,
campaign.campaign                         as campaign,
r1.campaign_id                            as campaign_id,
r1.site_id_dcm                            as site_id_dcm,
directory.site_dcm                        as site_dcm,
left(p1.placement,6)                      as plce_id,
-- p1.placement                              as placement,
replace(replace(p1.placement ,'','', ''''),''"'','''') as placement,
r1.placement_id                           as placement_id,
sum(r1.impressions)                       as impressions,
sum(r1.clicks)                            as clicks,
sum(r1.vew_led)                           as vew_led,
sum(r1.clk_led)                           as clk_led,
sum(r1.vew_led) + sum(r1.clk_led)         as led,
sum(r1.vew_con)                           as vew_con,
sum(r1.clk_con)                           as clk_con,
sum(r1.vew_con) + sum(r1.clk_con)         as con,
sum(r1.vew_tix)                           as vew_tix,
sum(r1.clk_tix)                           as clk_tix,
sum(r1.vew_tix) + sum(r1.clk_tix)         as tix,
sum(r1.vew_rev)                           as vew_rev,
sum(r1.clk_rev)                           as clk_rev,
sum(r1.rev)                               as rev
from (


select
cast (timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),''SS'') as date ) as "date"
,ta.campaign_id as campaign_id
,ta.site_id_dcm as site_id_dcm
,ta.placement_id as placement_id
,0 as impressions
,0 as clicks
,sum(case when activity_id = 1086066 and ta.conversion_id = 1 then 1 else 0 end) as clk_led
,sum(case when activity_id = 978826 and ta.conversion_id = 1 and ta.total_revenue <> 0 then 1 else 0 end ) as clk_con
,sum(case when activity_id = 978826 and ta.conversion_id = 1 and ta.total_revenue <> 0 then ta.total_conversions else 0 end ) as clk_tix
,sum(case when ta.conversion_id = 1 then (ta.total_revenue * 1000000) / (rates.exchange_rate) else 0 end ) as clk_rev
,sum(case when activity_id = 1086066 and ta.conversion_id = 2 then 1 else 0 end) as vew_led
,sum(case when activity_id = 978826  and ta.conversion_id = 2 and ta.total_revenue <> 0 then 1 else 0 end ) as vew_con
,sum(case when activity_id = 978826  and ta.conversion_id = 2 and ta.total_revenue <> 0 then ta.total_conversions else 0 end ) as vew_tix
,sum(case when ta.conversion_id = 2 then (ta.total_revenue * 1000000) / (rates.exchange_rate) else 0 end ) as vew_rev
,sum(ta.total_revenue * 1000000/rates.exchange_rate) as rev

from
(
select *
from diap01.mec_us_united_20056.dfa2_activity
where cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),''SS'') as date ) between ''2017-02-01'' and ''2017-05-08''
and not regexp_like(substring(other_data,(instr(other_data,''u3='') + 3),5),''mil.*'',''ib'')
and (activity_id = 978826 or activity_id = 1086066)
-- and campaign_id in (10768497, 9801178, 10742878, 10812738, 10740457) -- display 2017
and (advertiser_id <> 0)
and (length(isnull(event_sub_type,'''')) > 0)
) as ta

left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
on upper ( substring (other_data,(instr(other_data,''u3='')+3),3)) = upper (rates.currency)
and cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),''SS'') as date ) = rates.date

group by
cast (timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),''SS'') as date )
,ta.campaign_id
,ta.site_id_dcm
,ta.placement_id

union all

select
cast (timestamp_trunc(to_timestamp(ti.event_time / 1000000),''SS'') as date ) as "date"
,ti.campaign_id as campaign_id
,ti.site_id_dcm as site_id_dcm
,ti.placement_id as placement_id
,count (*) as impressions
,0 as clicks
,0 as clk_led
,0 as clk_con
,0 as clk_tix
,0 as clk_rev
,0 as vew_led
,0 as vew_con
,0 as vew_tix
,0 as vew_rev
,0 as rev

from (
select *
from diap01.mec_us_united_20056.dfa2_impression
where cast (timestamp_trunc(to_timestamp(event_time / 1000000),''SS'') as date ) between ''2017-02-01'' and ''2017-05-08''
-- and campaign_id in (10768497, 9801178, 10742878, 10812738, 10740457) -- display 2017

and (advertiser_id <> 0)
) as ti
group by
cast (timestamp_trunc(to_timestamp(ti.event_time / 1000000),''SS'') as date )
,ti.campaign_id
,ti.site_id_dcm
,ti.placement_id

union all

select
cast (timestamp_trunc(to_timestamp(tc.event_time / 1000000),''SS'') as date ) as "date"
,tc.campaign_id as campaign_id
,tc.site_id_dcm as site_id_dcm
,tc.placement_id as placement_id
,0 as impressions
,count (*) as clicks
,0 as clk_led
,0 as clk_con
,0 as clk_tix
,0 as clk_rev
,0 as vew_led
,0 as vew_con
,0 as vew_tix
,0 as vew_rev
,0 as rev

from (

select *
from diap01.mec_us_united_20056.dfa2_click
where cast (timestamp_trunc(to_timestamp(event_time / 1000000),''SS'') as date ) between ''2017-02-01'' and ''2017-05-08''
-- and campaign_id in (10768497, 9801178, 10742878, 10812738, 10740457) -- display 2017
and (advertiser_id <> 0)
) as tc

group by
cast (timestamp_trunc(to_timestamp(tc.event_time / 1000000),''SS'') as date )
,tc.campaign_id
,tc.site_id_dcm
,tc.placement_id

) as r1

left join
(
select cast (campaign as varchar (4000)) as ''campaign'',campaign_id as ''campaign_id''
from diap01.mec_us_united_20056.dfa2_campaigns
) as campaign
on r1.campaign_id = campaign.campaign_id

left join
(
select cast (p1.placement as varchar (4000)) as ''placement'',p1.placement_id as ''placement_id'',p1.campaign_id as ''campaign_id'',p1.site_id_dcm as ''site_id_dcm''

from ( select campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast (placement_start_date as date ) as thisdate,
row_number() over (partition by campaign_id,site_id_dcm,placement_id order by cast (placement_start_date as date ) desc ) as x1
from diap01.mec_us_united_20056.dfa2_placements

) as p1
where x1 = 1
) as p1
on r1.placement_id    = p1.placement_id
and r1.campaign_id = p1.campaign_id
and r1.site_id_dcm  = p1.site_id_dcm

left join
(
select cast (site_dcm as varchar (4000)) as ''site_dcm'',site_id_dcm as ''site_id_dcm''
from diap01.mec_us_united_20056.dfa2_sites
) as directory
on r1.site_id_dcm = directory.site_id_dcm

where  regexp_like(p1.placement,''P.?'',''ib'')
and not regexp_like(p1.placement,''.?do\s?not\s?use.?'',''ib'')
-- and not regexp_like(campaign.campaign,''.*2016.*'',''ib'')
and  regexp_like(campaign.campaign,''.*2017.*'',''ib'')
and not regexp_like(campaign.campaign,''.*Search.*'',''ib'')
and not regexp_like(campaign.campaign,''.*BidManager.*'',''ib'')
-- and r1.site_id_dcm <>''1485655''
group by
cast (r1.date as date )
,directory.site_dcm
,r1.site_id_dcm
,r1.campaign_id
,campaign.campaign
,r1.placement_id
,p1.placement
')

         ) as t1

        --    Prisma data
      left join
      (
        select *
        from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].prs_summ
      ) as prs
        on t1.placement_id = prs.adserverplacementid


        where t1.campaign not like '%Search%'
--         and t1.placement like 'P%'
        and t1.campaign not like '%[_]UK[_]%'
        and t1.campaign not like '%2016%'
        and t1.campaign not like '%2015%'
        and t1.campaign_id != 10698273  -- UK Acquisition 2017
        and t1.campaign_id != 11221036  -- Hong Kong 2017
        and t1.campaign_id != 11385662  -- Monagas (Venezuela) -> SFO 2017

    group by
       t1.dcmdate
      ,cast(month(cast(t1.dcmdate as date)) as int)
      ,t1.campaign
      ,t1.campaign_id
      ,t1.site_dcm
      ,t1.site_id_dcm
      ,t1.plce_id
      ,t1.placement
      ,t1.placement_id
      ,prs.packagecat
      ,prs.rate
      ,prs.costmethod
      ,prs.cost_id
      ,prs.planned_amt
      ,prs.planned_cost
      ,prs.placementend
      ,prs.placementstart
      ,prs.stdate
      ,prs.eddate
      ,prs.dv_map

  ) as t2

--   left join
--   (
--     select *
--     from master.dbo.dfa_flatCost_dt2
--   ) as flt
--     on t2.cost_id = flt.cost_id
--        and t2.dcmmatchdate = flt.dcmdate

--    Cost data
      left join
      (
        select *
        from master.dbo.dfa_cost_dt2
      ) as cst
        on cast(t2.dcmmatchdate as varchar(8)) + t2.plce_id = cast(cst.dcmdate as varchar(8)) + cst.plce_id


-- dv table join ==============================================================================================================================================

  left join (
              select *
              from master.dbo.dv_summ
              where dvdate between @report_st and @report_ed
            ) as dv
      on
      left(t2.placement,6) + '_' + [dbo].udf_sitekey(t2.site_dcm) + '_'
      + cast(t2.dcmdate as varchar(10)) = dv.joinkey

-- moat table join ==============================================================================================================================================

  left join (
              select *
              from master.dbo.mt_summ
              where mtdate between @report_st and @report_ed
            ) as mt
      on
      left(t2.placement,6) + '_' + [dbo].udf_sitekey( t2.site_dcm) + '_'
      + cast(t2.dcmdate as varchar(10)) = mt.joinkey


-- innovid table join ==============================================================================================================================================

  left join (
              select *
              from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].ivd_summ_agg
              where ivdate between @report_st and @report_ed
            ) as iv
      on
      left(t2.placement,6) + '_' + [dbo].udf_sitekey( t2.site_dcm) + '_'
      + cast(t2.dcmdate as varchar(10)) = iv.joinkey


--  where t2.costmethod = 'Flat'
group by

  t2.campaign
  ,t2.campaign_id
  ,t2.cost_id
  ,t2.dv_map
--   ,t2.dv_map_2
  ,t2.site_dcm
  ,t2.site_id_dcm
  ,t2.packagecat
  ,t2.placementend
  ,t2.placementstart
  ,t2.plce_id
  ,t2.placement_id
  ,t2.planned_amt
  ,t2.planned_cost
  ,t2.rate
  ,t2.placement
  ,t2.eddate
  ,t2.stdate
  ,t2.dcmdate
  ,t2.dcmmatchdate
  ,t2.dcmmonth
  ,t2.diff
  ,dv.joinkey
  ,mt.joinkey
  ,iv.joinkey
  ,cst.plce_id
  ,t2.costmethod
--   ,flt.flatcost

     ) as t3


group by
  t3.dcmdate
  ,t3.dcmmonth
  ,t3.diff
  ,t3.dvjoinkey
  ,t3.mtjoinkey
  ,t3.packagecat
  ,t3.cost_id
  ,t3.campaign
  ,t3.campaign_id
--   ,t3.newcount
  ,t3.site_dcm
  ,t3.site_id_dcm
  ,t3.costmethod
  ,t3.rate
-- ,t3.plce_id
  ,t3.placement
  ,t3.placement_id
  ,t3.placementend
  ,t3.placementstart
  ,t3.dv_map
  ,t3.planned_amt
  ,t3.planned_cost
--   ,t3.flatcost
--   ,t3.flatcost_chk


order by
  t3.cost_id,
  t3.dcmdate;
