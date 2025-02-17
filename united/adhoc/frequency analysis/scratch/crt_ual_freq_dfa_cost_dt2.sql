-- Not necessary

create procedure dbo.crt_ual_freq_dfa_cost_dt2
as
if OBJECT_ID('master.dbo.ual_freq_dfa_cost_dt2',N'U') is not null
    drop table master.dbo.ual_freq_dfa_cost_dt2;


create table master.dbo.ual_freq_dfa_cost_dt2
(
  cost_id       nvarchar(6)    not null,
  plce_id       nvarchar(6)    not null,
  dcmDate       int            not null,
  prsCostMethod nvarchar(100)  not null,
--   PackageCat    nvarchar(100)  not null,
  prsRate       decimal(20,10) not null,
  prsStDate     int            not null,
  prsEdDate     int            not null,
  diff          bigint         not null,
  flatcost      decimal(20,10) not null,
  cost          decimal(20,10) not null,
  lagCost       decimal(20,10) not null,
  lagCostRemain decimal(20,10) not null,
  CostRunTot    decimal(20,10) not null,
  CostRemain    decimal(20,10) not null,
  billimps      int            not null,
  impsRunTot    int            not null,
  impsRemain    int            not null,
  dlvrimps      int            not null,
  dfa_imps      int            not null,
  dv_imps       int            not null,
  iv_imps       int            not null,
  mt_imps       int            not null,
  Clks          int            not null,
  ClksRunTot    int            not null,
  ClksRemain    int            not null,
  planned_amt   int            not null,
  planned_cost  decimal(20,10) not null,
  vew_con       int            not null,
  clk_con       int            not null,
  vew_tix       int            not null,
  clk_tix       int            not null,
  vew_rev       decimal(20,10) not null,
  clk_rev       decimal(20,10) not null,
  rev           decimal(20,10) not null,
  con           int            not null,
  tix           int            not null
);

insert into master.dbo.ual_freq_dfa_cost_dt2
  select
    t7.cost_id       as cost_id,
    t7.plce_id       as plce_id,
    t7.dcmDate       as dcmDate,
    t7.costmethod    as prsCostMethod,
--     t7.PackageCat    as PackageCat,
    t7.rate          as prsRate,
    t7.stDate        as prsStDate,
    t7.edDate        as prsEdDate,
    t7.diff          as diff,
    t7.flatcost      as flatcost,
    t7.cost          as cost,
    t7.lagCost       as lagCost,
    t7.costRunTot    as costRunTot,
    t7.costRemain    as costRemain,
    t7.lagCostRemain as lagCostRemain,
    t7.Imps          as Imps,
    t7.impsRunTot    as impsRunTot,
    t7.impsRemain    as impsRemain,
    t7.dlvrimps      as dlvrimps,
    t7.dfa_imps      as dfa_imps,
--     t7.dv_imps       as dv_imps,
--     t7.iv_imps       as iv_imps,
--     t7.mt_imps       as mt_imps,
    t7.clks          as clks,
    t7.clksRunTot    as clksRunTot,
    t7.clksRemain    as clksRemain,
    t7.planned_amt   as planned_amt,
    t7.planned_cost  as planned_cost,
    t7.vew_con       as vew_con,
    t7.clk_con       as clk_con,
    t7.vew_tix       as vew_tix,
    t7.clk_tix       as clk_tix,
    t7.vew_rev       as vew_rev,
    t7.clk_rev       as clk_rev,
    t7.rev           as rev,
    t7.con           as con,
    t7.tix           as tix
    from (


             select
--                  t6.dcmmonth                                        as dcmmonth,
                 t6.dcmDate                                         as dcmDate,
                 t6.cost_id                                         as cost_id,
                 t6.plce_id                                         as plce_id,
                 t6.costmethod                                      as costmethod,
--                  t6.dvjoinkey                                       as dvjoinkey,
--                  t6.mtjoinkey                                       as mtjoinkey,
--                  t6.ivjoinkey                                       as ivjoinkey,
--                  t6.campaign                                        as campaign,
--                  t6.campaign_id                                     as campaign_id,
--                  t6.placement                                       as placement,
                 t6.placement_id                                    as placement_id,
                 t6.dv_map                                          as dv_map,
                 t6.rate                                                as rate,
--                  t6.PackageCat                                      as PackageCat,
                 t6.stDate                                          as stDate,
                 t6.edDate                                          as edDate,
                 isNull(t6.diff,cast(0 as int))                     as diff,
               isNull(t6.flatcost,cast(0 as decimal(20,10)))       as flatCost,
                 case
                 when t6.costmethod     like '[Cc][Pp][Cc]%' and t6.diff > 0 and t6.cost < t6.planned_cost and t6.clksRunTot < t6.planned_amt then t6.cost
                 when t6.diff > 0 and t6.cost < t6.planned_cost and t6.impsRunTot < t6.planned_amt then t6.cost
                 when t6.diff > 0 and t6.lagCost = 0 and t6.lagCostRemain = 0 then t6.cost
                 when t6.diff > 0 and t6.costRemain = 0 and t6.lagCostRemain > 0 then t6.lagCostRemain
                 when t6.cost = 0 then 0
                 when t6.cost > t6.lagCostRemain then t6.lagCostRemain
                 else t6.cost - t6.lagCostRemain
                 end                                                as cost,
                 isNull(t6.lagCost,cast(0 as decimal(20,10)))       as lagCost,
                 isNull(t6.costRunTot,cast(0 as decimal(20,10)))    as costRunTot,
                 isNull(t6.costRemain,cast(0 as decimal(20,10)))    as costRemain,
                 isNull(t6.lagCostRemain,cast(0 as decimal(20,10))) as lagCostRemain,
                 isNull(t6.Imps,cast(0 as decimal(20,10)))          as Imps,
                 isNull(t6.impsRunTot,cast(0 as decimal(20,10)))    as impsRunTot,
                 isNull(t6.impsRemain,cast(0 as int))               as impsRemain,
                 isNull(t6.planned_amt,cast(0 as int))              as planned_amt,
                 isNull(t6.planned_cost,cast(0 as int))             as planned_cost,
                 isNull(t6.dlvrimps,cast(0 as int))                 as dlvrimps,
                 isNull(t6.dfa_imps,cast(0 as int))                 as dfa_imps,
--                  isNull(t6.iv_imps,cast(0 as int))                  as iv_imps,
--                  isNull(t6.dv_imps,cast(0 as int))                  as dv_imps,
--                  isNull(t6.mt_imps,cast(0 as decimal(20,10)))       as mt_imps,
                 isNull(t6.clks,cast(0 as decimal(20,10)))          as clks,
--                  isNull(t6.clksRunTot,cast(0 as decimal(20,10)))    as clksRunTot,
--                  isNull(t6.clksRemain,cast(0 as int))               as clksRemain,
--                  isNull(t6.vew_con,cast(0 as int))                  as vew_con,
--                  isNull(t6.clk_con,cast(0 as int))                  as clk_con,
--                  isNull(t6.vew_tix,cast(0 as int))                  as vew_tix,
--                  isNull(t6.clk_tix,cast(0 as int))                  as clk_tix,
--                  isNull(t6.vew_rev,cast(0 as decimal(20,10)))       as vew_rev,
--                  isNull(t6.clk_rev,cast(0 as decimal(20,10)))       as clk_rev,
                 isNull(t6.rev,cast(0 as decimal(20,10)))           as rev,
                 isNull(t6.con,cast(0 as int))                      as con,
                 isNull(t6.tix,cast(0 as int))                      as tix

             from (
                      select
                          t5.user_id as user_id,
--                         t5.dcmmonth                                     as dcmmonth,
                        t5.dcmDate                                      as dcmDate,
                        t5.cost_id                                      as cost_id,
                        t5.plce_id                                      as plce_id,
--                         t5.costmethod                                   as costmethod,
--                         t5.dvjoinkey                                    as dvjoinkey,
--                         t5.mtjoinkey                                    as mtjoinkey,
--                         t5.ivjoinkey                                    as ivjoinkey,
--                         t5.campaign                                     as campaign,
--                         t5.campaign_id                                  as campaign_id,
--                         t5.placement                                    as placement,
                        t5.placement_id                                 as placement_id,
--                         t5.dv_map                                       as dv_map,
--                         t5.rate                                         as rate,
--                         t5.PackageCat                                   as PackageCat,
--                         t5.stDate                                       as stDate,
--                         t5.edDate                                       as edDate,
                        isNull(t5.diff,cast(0 as decimal(20,10)))       as diff,
--                         t5.flatcost as flatcost,
                        case
                        when t5.cost is null then cast(0 as decimal(20,10))
                        when t5.diff > 0 and t5.costmethod like '[Cc][Pp][Cc]%' and t5.clks > t5.planned_amt then isNull(t5.cost,cast(0 as decimal(20,10))) - isNull(t5.lagCost,cast(0 as decimal(20,10)))
                        when t5.diff > 0 and t5.Imps > t5.planned_amt then isNull(t5.cost,cast(0 as decimal(20,10))) - isNull(t5.lagCost,cast(0 as decimal(20,10)))
                        else isNull(t5.cost,cast(0 as decimal(20,10)))
                        end                                             as cost,
                        isNull(t5.lagCost,cast(0 as decimal(20,10)))    as lagCost,
                        isNull(t5.costRunTot,cast(0 as decimal(20,10))) as costRunTot,
                        isNull(t5.costRemain,cast(0 as decimal(20,10))) as costRemain,
                        lag(isNull(t5.costRemain,cast(0 as decimal(20,10))),1,0) over (partition by t5.cost_id
                          order by t5.dcmDate,plce_id)                as lagCostRemain,
                        isNull(t5.Imps,cast(0 as int))                  as Imps,
                        isNull(t5.impsRunTot,cast(0 as int))            as impsRunTot,
                        isNull(t5.impsRemain,cast(0 as int))            as impsRemain,
                        isNull(t5.planned_amt,cast(0 as int))           as planned_amt,
                        isNull(t5.planned_cost,cast(0 as int))          as planned_cost,
                        t5.billimps                                     as billimps,
--                         t5.dlvrimps                                     as dlvrimps,
--                         t5.dfa_imps                                     as dfa_imps,
--                         t5.iv_imps                                      as iv_imps,
--                         t5.dv_imps                                      as dv_imps,
--                         t5.mt_imps                                      as mt_imps,
                        isNull(t5.clks,cast(0 as int))                  as clks,
                        isNull(t5.clksRunTot,cast(0 as int))            as clksRunTot,
                        isNull(t5.clksRemain,cast(0 as int))            as clksRemain,
--                         t5.vew_con                                      as vew_con,
--                         t5.clk_con                                      as clk_con,
--                         t5.vew_tix                                      as vew_tix,
--                         t5.clk_tix                                      as clk_tix,
--                         t5.vew_rev                                      as vew_rev,
--                         t5.clk_rev                                      as clk_rev,
                        t5.rev                                          as rev,
                        t5.con                                          as con,
                        t5.tix                                          as tix
             from (
                    select
                        t4.user_id as user_id,
--                       t4.dcmmonth                                                                               as dcmmonth,
                      t4.dcmDate                                                                                as dcmDate,
                      t4.cost_id                                                                                as cost_id,
                      t4.plce_id                                                                                as plce_id,
                      t4.costmethod                                                                             as costmethod,
--                       t4.dvjoinkey                                                                              as dvjoinkey,
--                       t4.mtjoinkey                                                                              as mtjoinkey,
--                       t4.ivjoinkey                                                                              as ivjoinkey,
--                       t4.campaign                                                                               as campaign,
--                       t4.campaign_id                                                                            as campaign_id,
--                       t4.placement                                                                              as placement,
                      t4.placement_id                                                                           as placement_id,
                      t4.dv_map                                                                                 as dv_map,
                      t4.rate                                                                                   as rate,
--                       t4.PackageCat                                                                             as PackageCat,
--                       t4.stDate                                                                                 as stDate,
--                       t4.edDate                                                                                 as edDate,
                      t4.planned_cost                                                                           as planned_cost,
                      t4.planned_amt                                                                            as planned_amt,
                      isNull(t4.ed_diff,cast(0 as decimal(20,10)))                                              as diff,
--                       t4.flatcost as flatcost,
                      sum(t4.cost) over (partition by t4.cost_id,t4.plce_id,t4.dcmDate, t4.user_id)                         as cost,
                      lag(t4.cost,1,0) over (partition by t4.cost_id
                        order by t4.dcmDate,t4.plce_id, t4.user_id)                                                       as lagCost,
                      sum(t4.cost) over (partition by t4.cost_id
                        order by  t4.user_id, t4.dcmDate,t4.plce_id asc range between unbounded preceding and current row) as costRunTot,
                      case
                      when (cast(t4.planned_cost as decimal(20,10)) - sum(t4.cost) over (partition by t4.cost_id
                        order by  t4.user_id, t4.dcmDate,t4.plce_id asc range between unbounded preceding and current row)) <= 0 then 0
                      else (cast(t4.planned_cost as decimal(20,10)) - sum(t4.cost) over (partition by t4.cost_id
                        order by  t4.user_id, t4.dcmDate,t4.plce_id asc range between unbounded preceding and current row))
                      end                                                                                       as costRemain,

                      sum(t4.billimps) over (partition by t4.cost_id,t4.plce_id,t4.dcmDate, t4.user_id)                     as Imps,
                      sum(t4.billimps) over (partition by t4.cost_id
                        order by  t4.user_id, t4.dcmDate,t4.plce_id asc range between unbounded preceding and current row) as impsRunTot,
                      case
                      when (cast(t4.planned_amt as decimal(20,10)) - sum(t4.billimps) over (partition by t4.cost_id
                        order by  t4.user_id, t4.dcmDate,t4.plce_id asc range between unbounded preceding and current row)) <= 0 then 0
                      else (cast(t4.planned_amt as decimal(20,10)) - sum(t4.billimps) over (partition by t4.cost_id
                        order by  t4.user_id, t4.dcmDate,t4.plce_id asc range between unbounded preceding and current row))
                      end                                                                                       as impsRemain,
                      t4.billimps                                                                               as billimps,
--                       t4.dlvrimps                                                                               as dlvrimps,
--                       t4.dfa_imps                                                                               as dfa_imps,
--                       t4.iv_imps                                                                                as iv_imps,
--                       t4.dv_imps                                                                                as dv_imps,
--                       t4.mt_imps                                                                                as mt_imps,
                      sum(t4.clicks) over (partition by t4.cost_id,t4.plce_id,t4.dcmDate, t4.user_id)                       as clks,
                      sum(t4.clicks) over (partition by t4.cost_id
                        order by  t4.user_id, t4.dcmDate,t4.plce_id asc range between unbounded preceding and current row) as clksRunTot,
                      case
                      when (cast(t4.planned_amt as decimal(20,10)) - sum(t4.clicks) over (partition by t4.cost_id
                        order by  t4.user_id, t4.dcmDate,t4.plce_id asc range between unbounded preceding and current row)) <= 0 then 0
                      else (cast(t4.planned_amt as decimal(20,10)) - sum(t4.clicks) over (partition by t4.cost_id
                        order by  t4.user_id, t4.dcmDate,t4.plce_id asc range between unbounded preceding and current row))
                      end                                                                                       as clksRemain,
--                       t4.vew_con                                                                                as vew_con,
--                       t4.clk_con                                                                                as clk_con,
--                       t4.vew_tix                                                                                as vew_tix,
--                       t4.clk_tix                                                                                as clk_tix,
--                       t4.vew_rev                                                                                as vew_rev,
--                       t4.clk_rev                                                                                as clk_rev,
                      t4.rev                                                                                    as rev,
                      t4.con                                                                                    as con,
                      t4.tix                                                                                    as tix

                      from
                          (
                            select
                              t3.user_id                                       as user_id,
                              t3.dcmmonth                                      as dcmmonth,
                              t3.dcmDate                                       as dcmDate,
                              t3.cost_id                                       as cost_id,
                              t3.plce_id                                       as plce_id,
                              t3.costmethod                                    as costmethod,
--                               t3.dvjoinkey                                     as dvjoinkey,
--                               t3.mtjoinkey                                     as mtjoinkey,
--                               t3.ivjoinkey                                     as ivjoinkey,
--                               t3.campaign                                      as campaign,
--                               t3.campaign_id                                   as campaign_id,
--                               t3.placement                                     as placement,
                              t3.placement_id                                  as placement_id,
                              t3.dv_map                                        as dv_map,
                              t3.rate                                          as rate,
--                               t3.PackageCat                                    as PackageCat,
                              t3.stDate                                        as stDate,
--                               t3.edDate                                        as edDate,
                              t3.planned_cost                                  as planned_cost,
                              t3.planned_amt                                   as planned_amt,
--                               case when t3.costmethod like '[Ff]lat' then t3.flatcost/max(t3.cst_count) else 0 end      as flatcost,
                              case
                              when t3.dcmDate - t3.stDate < 0 then 0
--                               when (t3.edDate - t3.dcmDate) > 0 and t3.costmethod like '[Cc][Pp][Cc]%' and sum(t3.clicks) > t3.planned_amt then t3.planned_cost
                              when (t3.edDate - t3.dcmDate) > 0 and sum(t3.billimps) > t3.planned_amt then t3.planned_cost
                              when (t3.edDate - t3.dcmDate) > 0 then sum(t3.cost)
                              when (t3.edDate - t3.dcmDate) < 0 then 0
                              else 0
                              end                                              as cost,
                              cast(t3.edDate as int) - cast(t3.dcmDate as int) as ed_diff,
                              case
                              when t3.dcmDate - t3.stDate < 0 then 0
                              else sum(t3.billimps) end                        as billimps,
--                               sum(t3.dlvrimps)                                 as dlvrimps,
--                               sum(t3.dfa_imps)                                 as dfa_imps,
--                               sum(t3.iv_imps)                                  as iv_imps,
--                               sum(t3.dv_imps)                                  as dv_imps,
--                               sum(t3.mt_imps)                                  as mt_imps,
                              sum(t3.clicks)                                   as clicks,
--                               sum(t3.vew_con)                                  as vew_con,
--                               sum(t3.clk_con)                                  as clk_con,
--                               sum(t3.vew_tix)                                  as vew_tix,
--                               sum(t3.clk_tix)                                  as clk_tix,
--                               sum(t3.vew_rev)                                  as vew_rev,
--                               sum(t3.clk_rev)                                  as clk_rev,
                              sum(t3.rev)                                      as rev,
                              sum(t3.con)                                      as con,
                              sum(t3.tix)                                      as tix

                              from
                                  (

select
  t2.user_id as user_id,
  t2.dcmdate        as dcmdate,
  t2.dcmmonth                            as dcmmonth,
--   t2.diff                                as diff,
--   t2.dvjoinkey                           as dvjoinkey,
--   t2.mtjoinkey                           as mtjoinkey,
--   t2.ivjoinkey                           as ivjoinkey,
--   t2.campaign                            as campaign,
--   t2.campaign_id                         as campaign_id,
--   t2.placement                           as placement,
  t2.plce_id                             as plce_id,
  t2.placement_id                        as placement_id,
  t2.dv_map                              as dv_map,
--   t2.packagecat                          as packagecat,
  t2.cost_id                             as cost_id,
  t2.cst_count                           as cst_count,
  t2.costmethod                          as costmethod,
--   t2.flatcost as flatcost,
t2.placementend   as edDate,
t2.placementstart as stDate,
  t2.planned_amt                         as planned_amt,
  t2.planned_cost                        as planned_cost,
--   case when t2.costmethod = 'dCPM' then db.dbm_cost
--   else t2.cost end                       as cost,
db.dbm_cost as cost,
  db.dbm_cost                            as dbm_cost,
  t2.rate                                as rate,
--   case when t2.costmethod = 'dCPM' then db.imps
--   else t2.dlvrimps end                   as dlvrimps,
db.imps as dlvrimps,
--   case when t2.costmethod = 'dCPM' then db.imps
--   else t2.billimps end                   as billimps,
db.imps as billimps,
--   case when t2.costmethod = 'dCPM' then db.imps
--   else t2.dfa_imps end                   as dfa_imps,
db.imps as dfa_imps,
--   t2.iv_imps                             as iv_imps,
--   t2.dv_imps                             as dv_imps,
--   t2.mt_imps                             as mt_imps,
--   case when t2.costmethod = 'dCPM' then db.clicks
--   else t2.clicks end                     as clicks,
db.clicks as clicks,
--   db.vew_con                             as vew_con,
--   db.clk_con                             as clk_con,
  db.con                                 as con,
--   db.vew_tix                             as vew_tix,
--   db.clk_tix                             as clk_tix,
  db.tix                                 as tix,
--   db.vew_rev                             as vew_rev,
--   db.clk_rev                             as clk_rev,
  db.rev                                 as rev


-- ============================================================================================================================================

from (
--
--
-- declare @report_st date,
-- @report_ed date;
-- -- --
-- set @report_ed = '2017-01-31';
-- set @report_st = '2016-07-15';

         select
             t1.user_id as user_id,
             t1.dcmmonth                                                                as dcmmonth,
--              t1.dcmdate as dcmdate,
             t1.dcmmatchdate as dcmdate,
--              t1.diff                                                                    as diff,
--              dv.joinkey                                                                 as dvjoinkey,
--              mt.joinkey                                                                 as mtjoinkey,
--              iv.joinkey                                                                 as ivjoinkey,
--              t1.packagecat                                                              as packagecat,
             t1.cost_id                                                                 as cost_id,
--              t1.campaign                                                                as campaign,
--              t1.campaign_id                                                             as campaign_id,
--              t1.site_dcm                                                                as site_dcm,
--              t1.site_id_dcm                                                             as site_id_dcm,
             t1.costmethod                                                              as costmethod,
--           Count of # times cost_id appears per day. Important b/c planned_amt and planned_cost are listed at
--         cost_id level.
             sum(1) over (partition by t1.cost_id,t1.dcmdate
             order by
             t1.dcmmonth asc range between unbounded preceding and current row)         as cst_count,
         t1.plce_id                                                                 as plce_id,
--              t1.placement                                                               as placement,
             t1.placement_id                                                            as placement_id,
             t1.placementend                                                            as placementend,
             t1.placementstart                                                          as placementstart,
             t1.dv_map                                                                  as dv_map,
             t1.planned_amt                                                             as planned_amt,
             t1.planned_cost                                                            as planned_cost,
--         flat.flatcost as flatcost,
  --  logic excludes flat fees
--              case
--              --       zeros out cost for placements traffic before specified start date or after specified end date
--              when ((t1.dv_map = 'N' or t1.dv_map = 'Y') and (t1.eddate - t1.dcmmatchdate < 0 or t1.dcmmatchdate - t1.stdate < 0)
--                  and (t1.costmethod = 'CPM' or t1.costmethod = 'CPMV' or t1.costmethod = 'CPE' or t1.costmethod = 'CPC' or
--                  t1.costmethod = 'CPCV'))
--                  then cast(0 as decimal(20,10))
--              --       click source innovid
--              when ((t1.dv_map = 'Y' or t1.dv_map = 'N') and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0)
--                  and (t1.costmethod = 'CPC' or t1.costmethod = 'CPCV') and (len(isnull(iv.joinkey,'')) > 0))
--                  then cast((sum(cast(iv.click_thrus as decimal(20,10))) * cast(t1.rate as decimal(20,10))) as
--                            decimal(20,10))
--              --       click source dcm
--              when ((t1.dv_map = 'Y' or t1.dv_map = 'N') and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0)
--                  and (t1.costmethod = 'CPC' or t1.costmethod = 'CPCV'))
--                  then cast((sum(cast(t1.clicks as decimal(20,10))) * cast(t1.rate as decimal(20,10))) as decimal(20,10))
--
--              --           impression-based cost; not subject to viewability; innovid source
--              when (t1.dv_map = 'N' and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0) and
--                  (t1.costmethod = 'CPM' or t1.costmethod = 'CPMV' or t1.costmethod = 'CPE') and (len(
--                  isnull(iv.joinkey,'')) > 0))
--                  then cast((sum(cast(iv.impressions as decimal(20,10))) * cast(t1.rate as decimal(20,10))) / 1000 as
--                            decimal(20,10))
--
--              --           impression-based cost; not subject to viewability; dcm source
--              when (t1.dv_map = 'N' and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0) and
--                  (t1.costmethod = 'CPM' or t1.costmethod = 'CPMV' or t1.costmethod = 'CPE'))
--                  then cast((sum(cast(t1.impressions as decimal(20,10))) * cast(t1.rate as decimal(20,10))) / 1000 as
--                            decimal(20,10))
--
--              --           impression-based cost; subject to viewability with flag; mt source
--              when (t1.dv_map = 'Y' and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0) and
--                  (t1.costmethod = 'CPM' or t1.costmethod = 'CPMV' or t1.costmethod = 'CPE') and mt.joinkey is not null)
--                  then cast((sum(cast(mt.groupm_billable_impressions as decimal(20,10))) * cast(t1.rate as
--                                                                                                decimal(20,10))) / 1000
--                            as decimal(20,10))
--
--              --           impression-based cost; subject to viewability; dv source
--              when (t1.dv_map = 'Y' and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0) and
--                  (t1.costmethod = 'CPM' or t1.costmethod = 'CPMV' or t1.costmethod = 'CPE'))
--                  then cast((sum(cast(dv.groupm_billable_impressions as decimal(20,10))) * cast(t1.rate as
--                                                                                                decimal(20,10))) / 1000
--                            as decimal(20,10))
--
--              --           impression-based cost; subject to viewability; moat source
--              when (t1.dv_map = 'M' and (t1.eddate - t1.dcmmatchdate >= 0 or t1.dcmmatchdate - t1.stdate >= 0) and
--                  (t1.costmethod = 'CPM' or t1.costmethod = 'CPMV' or t1.costmethod = 'CPE'))
--                  then cast((sum(cast(mt.groupm_billable_impressions as decimal(20,10))) * cast(t1.rate as
--                                                                                                decimal(20,10))) / 1000
--                            as decimal(20,10))
--
--              else
                 cast(0 as decimal(20,10)) as cost,
             t1.rate                                                                    as rate,
--       total impressions as reported by 1.) dcm for "n," 2.) dv for "y," or moat for "m"
--              sum(case
--                  when t1.dv_map = 'Y' and (len(isnull(mt.joinkey,'')) > 0) then mt.total_impressions
--                  when t1.dv_map = 'Y' then dv.total_impressions
--                  when t1.dv_map = 'M' then mt.total_impressions
--                  when t1.dv_map = 'N' and (len(isnull(iv.joinkey,'')) > 0) then iv.impressions
--                  else t1.impressions end)                                               as dlvrimps,
                sum(t1.impressions) as dlvrimps,
--       billable impressions as reported by 1.) dcm for "n," 2.) dv for "y," or moat for "m"
--              sum(case
--                  when t1.dv_map = 'Y' and (len(isnull(mt.joinkey,'')) > 0) then mt.groupm_billable_impressions
--                  when t1.dv_map = 'Y' then dv.groupm_billable_impressions
--                  when t1.dv_map = 'M' then mt.groupm_billable_impressions
--                  when t1.dv_map = 'N' and (len(isnull(iv.joinkey,'')) > 0) then iv.impressions
--                  else t1.impressions end)                                               as billimps,
                 sum(t1.impressions) as billimps,
--       dcm impressions (for comparison (qa) to dcm console)
             sum(t1.impressions)                                                        as dfa_imps,
--              sum(iv.impressions)                                                        as iv_imps,
--              sum(iv.click_thrus)                                                        as iv_clicks,
--              sum(iv.all_completion)                                                     as iv_completes,
--              sum(cast(dv.total_impressions as int))                                     as dv_imps,
--              sum(dv.groupm_passed_impressions)                                          as dv_viewed,
--              sum(cast(dv.groupm_billable_impressions as decimal(20,10)))                as dv_groupmpayable,
--              sum(cast(mt.total_impressions as int))                                     as mt_imps,
--              sum(mt.groupm_passed_impressions)                                          as mt_viewed,
--              sum(cast(mt.groupm_billable_impressions as decimal(20,10)))                as mt_groupmpayable,
--       clicks
--              sum(case
--                  when (len(isnull(iv.joinkey,'')) > 0) then iv.click_thrus
--                  else t1.clicks end)                                                    as clicks
                sum(t1.clicks) as clicks
--              sum(t1.con)                                                               as con,
--              sum(t1.tix)                                                                as tix


         from
             (
-- =========================================================================================================================

                 select
                     dcmreport.user_id as user_id,
                        dcmreport.dcmdate             as dcmdate,
                     cast(month(cast(dcmreport.dcmdate as date)) as int) as dcmmonth,
                     [dbo].udf_dateToInt(dcmreport.dcmdate)                                     as dcmmatchdate,
--                      dcmreport.campaign                                                         as campaign,
--                      dcmreport.campaign_id                                                      as campaign_id,
--                      dcmreport.site_dcm                                                         as site_dcm,
--                      dcmreport.site_id_dcm                                                      as site_id_dcm,
--                      case when dcmreport.plce_id in ('PBKB7J','PBKB7H','PBKB7K') then 'PBKB7J'
--                     else dcmreport.plce_id end                                                           as plce_id,
                     dcmreport.plce_id                                                            as plce_id,
--                      case when dcmreport.placement like 'PBKB7J%' or dcmreport.placement like 'PBKB7H%' or dcmreport.placement like 'PBKB7K%' or dcmreport.placement = 'United 360 - Polaris 2016 - Q4 - Amobee' then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
--           else dcmreport.placement end                                                         as placement,
--        amobee video 360 placements, tracked differently across dcm, innovid, and moat; this combines the three placements into one
--                      case when dcmreport.placement_id in (137412510,137412401,137412609) then 137412609
--                      else dcmreport.placement_id end                                            as placement_id,
                     dcmreport.placement_id                                             as placement_id,
                     prisma.stdate                                                              as stdate,
--                      case when dcmreport.campaign_id = 9923634 and dcmreport.site_id_dcm != 1190258 then 20161022
--                      else
--                          prisma.eddate end                                                      as eddate,
                     prisma.eddate                                                       as eddate,
--                      prisma.packagecat                                                          as packagecat,
                     prisma.costmethod                                                          as costmethod,
                     prisma.cost_id                                                             as cost_id,
                     prisma.planned_amt                                                         as planned_amt,
                     prisma.planned_cost                                                        as planned_cost,
--                      prisma.placementstart                                                      as placementstart,
                       [dbo].udf_dateToInt(prisma.placementend)   as placementend,
                        [dbo].udf_dateToInt(prisma.placementstart ) as placementstart,
--                      case when dcmreport.campaign_id = 9923634 and dcmreport.site_id_dcm != 1190258 then '2016-10-22'
--                      else
--                          prisma.placementend end                                                as placementend,
--                         prisma.placementend                                                 as placementend,
--                      sum((cast(dcmreport.impressions as decimal(20,10)) / nullif(
--                          cast(prisma.planned_amt as decimal(20,10)),0)) * cast(prisma.rate as
--                                                                                decimal(20,10))) as incrflatcost,
                     cast(prisma.rate as decimal(20,10))                                        as rate,
                     sum(dcmreport.impressions)                                                 as impressions,
                     sum(dcmreport.clicks)                                                      as clicks,
--                      sum(dcmreport.con)                                                        as con,
--                      sum(dcmreport.tix)                                                         as tix,

--                      case when cast(month(prisma.placementend) as int) - cast(month(cast(dcmreport.dcmdate as date)) as
--                   int) <= 0 then 0
--                      else cast(month(prisma.placementend) as int) - cast(month(cast(dcmreport.dcmdate as date)) as
--                                                                          int) end               as diff,

--                      case
--                      when prisma.costmethod = 'Flat' or prisma.costmethod = 'CPC' or prisma.costmethod = 'CPCV' or prisma.costmethod = 'DCPM'
--                          then 'N'
-- --                   live intent for sfo-sin campaign is email (not subject to viewability), but mistakenly labeled with "Y"
--                      when
--                          dcmreport.campaign_id = '9923634' -- sfo-sin
--                              and dcmreport.site_id_dcm = '1853564' -- live intent
--                          then 'N'
-- --                   corrections to sme placements
--                      when dcmreport.campaign_id = '10090315' and (dcmreport.site_id_dcm = '1513807' or dcmreport.site_id_dcm = '1592652')
--                          then 'Y'
--
-- --                   corrections to sfo-sin placements
--                      when dcmreport.campaign_id = '9923634' and dcmreport.site_id_dcm = '1534879' and prisma.costmethod = 'CPM'
--                          then 'N'
-- --                   designates all xaxis placements as "y." not always true.
-- --                   when dcmreport.site_id_dcm = '1592652' then 'Y'
--
-- --                   flipboard unable to implement moat tags; must bill off of dfa impressions
--                      when dcmreport.site_id_dcm = '2937979' then 'N'
--                      --           all targeted marketing subject
--                      when dcmreport.campaign_id = '9639387' then 'Y'
--
--                      --           designates all sfo-akl placements as "y." not always true. apparently.
--                      --             when dcmreport.campaign_id = '9973506' then 'Y'
--
--                      when Prisma.CostMethod = 'CPMV' and
--                          (dcmreport.placement like '%[Mm][Oo][Bb][Ii][Ll][Ee]%' or dcmreport.placement like '%[Vv][Ii][Dd][Ee][Oo]%' or dcmreport.placement like '%[Pp][Rr][Ee]%[Rr][Oo][Ll][Ll]%' or dcmreport.site_id_dcm = '1995643'
-- --                             or dcmreport.site_id_dcm = '1474576'
--                              or dcmreport.site_id_dcm = '2854118')
--                          then 'M'
--                      --                      Look for viewability flags Investment began including in placement names 6/16.
--                      when dcmreport.placement like '%[_]DV[_]%' then 'Y'
--                      when dcmreport.placement like '%[_]MOAT[_]%' then 'M'
--                      when dcmreport.placement like '%[_]NA[_]%' then 'N'
--                      --
--                      when Prisma.CostMethod = 'CPMV' and Prisma.DV_Map = 'N' then 'Y'
--                      else Prisma.DV_Map end                                                     as DV_Map
                 Prisma.DV_Map                                                      as DV_Map

                 --           prisma.dv_map as dv_map,


                 -- ==========================================================================================================================================================

                 -- openquery function call must not exceed 8,000 characters; no room for comments inside the function
from (
select *
from openquery(verticaunited,'
select
r1.user_id                                 as user_id,
-- cast(r1.date as date)                      as dcmdate,
  r1.date as dcmdate,
-- campaign.campaign                          as campaign,
r1.campaign_id                             as campaign_id,
-- r1.site_id_dcm                             as site_id_dcm,
-- directory.site_dcm                         as site_dcm,
left(p1.placement,6) as plce_id,
-- p1.placement as placement,
r1.placement_id as placement_id,
  sum(r1.impressions) as impressions,
sum (r1.clicks) as clicks,
sum (r1.vew_con) as vew_con,
sum (r1.clk_con) as clk_con,
sum (r1.vew_con) + sum (r1.clk_con) as con,
sum (r1.vew_tix) as vew_tix,
sum (r1.clk_tix) as clk_tix,
sum (r1.vew_tix) + sum (r1.clk_tix) as tix
from (


select
ta.user_id as user_id,
cast(timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),''SS'') as date ) as "date",
ta.campaign_id as campaign_id,
-- ta.site_id_dcm as site_id_dcm,
ta.placement_id as placement_id,
  0 as impressions,
0 as clicks,
sum ( case when ta.conversion_id = 1 then 1 else 0 end ) as clk_con,
sum ( case when ta.conversion_id = 1 then ta.total_conversions else 0 end ) as clk_tix,
sum ( case when ta.conversion_id = 2 then 1 else 0 end ) as vew_con,
sum ( case when ta.conversion_id = 2 then ta.total_conversions else 0 end ) as vew_tix

from
(
select *
from diap01.mec_us_united_20056.dfa2_activity
where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),''SS'') as date ) between ''2016-07-15'' and ''2016-07-15''
and not regexp_like( substring (other_data,(instr(other_data,''u3='') + 3),5),''mil.*'',''ib'')
and total_revenue <> 0      -- screen for floodlight fires on homepage
and total_conversions <> 0  -- screen for floodlight fires on homepage
and activity_id = 978826    -- ticket confirmation only
and campaign_id = 9639387   -- TMK 2016
and site_id_dcm = 1578478   -- Google only
and (advertiser_id <> 0)
and user_id <> ''0''
and ( length (isnull(event_sub_type,'''')) > 0)
) as ta

group by
ta.user_id,
cast(timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),''SS'') as date ),
ta.campaign_id,
-- ta.site_id_dcm,
ta.placement_id

--
union all

select
ti.user_id as user_id,
cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000),''SS'') as date ) as "date"
,ti.campaign_id as campaign_id
-- ,ti.site_id_dcm as site_id_dcm
,ti.placement_id as placement_id
,count (*) as impressions
,0 as clicks
,0 as clk_con
,0 as clk_tix
,0 as vew_con
,0 as vew_tix

from (
select *
from diap01.mec_us_united_20056.dfa2_impression
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),''SS'') as date ) between ''2016-07-15'' and ''2016-07-15''
and campaign_id = 9639387   -- TMK 2016
and site_id_dcm = 1578478   -- Google only
and (advertiser_id <> 0)
and user_id <> ''0''
) as ti
group by
ti.user_id,
cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000),''SS'') as date )
,ti.campaign_id
-- ,ti.site_id_dcm
,ti.placement_id

union all

select
tc.user_id as user_id,
cast(timestamp_trunc(to_timestamp(tc.event_time / 1000000),''SS'') as date ) as "date",
tc.campaign_id as campaign_id,
-- tc.site_id_dcm as site_id_dcm,
tc.placement_id as placement_id,
0 as impressions,
count (*) as clicks,
0 as clk_con,
0 as clk_tix,
0 as vew_con,
0 as vew_tix

from (

select *
from diap01.mec_us_united_20056.dfa2_click
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),''SS'') as date ) between ''2016-07-15'' and ''2016-07-15''
and campaign_id = 9639387   -- TMK 2016
and site_id_dcm = 1578478   -- Google only
and (advertiser_id <> 0)
and user_id <> ''0''
) as tc

group by
tc.user_id,
cast(timestamp_trunc(to_timestamp(tc.event_time / 1000000),''SS'') as date ),
tc.campaign_id,
-- tc.site_id_dcm,
tc.placement_id

) as r1

-- left join
-- (
-- select cast(campaign as varchar(4000)) as campaign,campaign_id as campaign_id
-- from diap01.mec_us_united_20056.dfa2_campaigns
-- ) as campaign
-- on r1.campaign_id = campaign.campaign_id

left join
(
select cast(p1.placement as varchar(4000)) as placement,p1.placement_id as placement_id,p1.campaign_id as campaign_id,p1.site_id_dcm as site_id_dcm

from ( select campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast(placement_start_date as date) as thisdate,
row_number() over (partition by campaign_id,site_id_dcm,placement_id order by cast(placement_start_date as date ) desc ) as x1
from diap01.mec_us_united_20056.dfa2_placements

) as p1
where x1 = 1
) as p1
on r1.placement_id    = p1.placement_id
and r1.campaign_id = p1.campaign_id
-- and r1.site_id_dcm  = p1.site_id_dcm

-- left join
-- (
-- select cast(site_dcm as varchar(4000)) as site_dcm,site_id_dcm as site_id_dcm
-- from diap01.mec_us_united_20056.dfa2_sites
-- ) as directory
-- on r1.site_id_dcm = directory.site_id_dcm

where not regexp_like(placement,''.do\s* not \s*use.'',''ib'')
-- and not regexp_like(campaign.campaign,''.*2016.*'',''ib'')
-- and not regexp_like(campaign.campaign,''.*Search.*'',''ib'')
-- and not regexp_like(campaign.campaign,''.*BidManager.*'',''ib'')
group by
r1.user_id,
  r1.date,
-- cast(r1.date as date),
-- directory.site_dcm,
-- r1.site_id_dcm,
r1.campaign_id,
-- campaign.campaign,
r1.placement_id,
p1.placement

')

                      ) as dcmreport


                     left join
                     (
                         select *
                         from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].prs_summ
                     ) as prisma
                         on dcmreport.placement_id = prisma.adserverplacementid
--                  where prisma.costmethod != 'Flat'
--     and prisma.cost_id = 'P8FSSK'

where prisma.costmethod like '[Dd][Cc][Pp][Mm]'

                 group by
                     dcmreport.user_id
                     ,dcmreport.dcmdate
--                      ,cast(month(cast(dcmreport.dcmdate as date)) as int)
--                      ,dcmreport.campaign
--                      ,dcmreport.campaign_id
--                      ,dcmreport.site_dcm
--                      ,dcmreport.site_id_dcm
                     ,dcmreport.plce_id
--                      ,dcmreport.placement
                     ,dcmreport.placement_id
--                      ,prisma.packagecat
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

--              left join
--              (
--                  select *
--                  from master.dbo.dfa_flatCost_dt2
--              ) as flat
--                  on t1.cost_id = flat.cost_id
--                  and t1.dcmmatchdate = flat.dcmdate

-- dv table join ==============================================================================================================================================

--              left join (
--                            select *
--                            from master.dbo.dv_summ
-- -- where dvdate between @report_st and @report_ed
--                        ) as dv
--                  on
--                      left(t1.placement,6) + '_' + [dbo].udf_sitekey(t1.site_dcm) + '_'
--                          + cast(t1.dcmdate as varchar(10)) = dv.joinkey
--
-- -- moat table join ==============================================================================================================================================
--
--              left join (
--                            select *
--                            from master.dbo.mt_summ
-- -- where mtdate between @report_st and @report_ed
--                        ) as mt
--                  on
--                      left(t1.placement,6) + '_' + [dbo].udf_sitekey(t1.site_dcm) + '_'
--                          + cast(t1.dcmdate as varchar(10)) = mt.joinkey
--
--
-- -- innovid table join ==============================================================================================================================================
--
--              left join (
--                            select *
--                            from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].ivd_summ_agg
-- -- where ivdate between @report_st and @report_ed
--                        ) as iv
--                  on
--                      left(t1.placement,6) + '_' + [dbo].udf_sitekey(t1.site_dcm) + '_'
--                          + cast(t1.dcmdate as varchar(10)) = iv.joinkey

--          where t1.campaign not like 'BidManager%Campaign%'
--              and t1.site_dcm not like '%DfaSite%'

         group by
             t1.user_id
--              ,t1.campaign
--              ,t1.campaign_id
             ,t1.cost_id
             ,t1.dv_map
--              ,t1.site_dcm
--              ,t1.site_id_dcm
--              ,t1.packagecat
             ,t1.placementend
             ,t1.placementstart
             ,t1.plce_id
             ,t1.placement_id
             ,t1.planned_amt
             ,t1.planned_cost
             ,t1.rate
--              ,t1.placement
             ,t1.eddate
             ,t1.stdate
             ,t1.dcmdate
             ,t1.dcmmatchdate
             ,t1.dcmmonth
--              ,t1.diff
--              ,dv.joinkey
--              ,mt.joinkey
--              ,iv.joinkey
             ,t1.costmethod
--        ,flat.flatcost

     ) as t2
--                                                left join (
--                            select *
--                            from master.dbo.ual_freq_dbm_cost
--                        ) as db
--                       on t2.dcmdate = db.dcmdate
--                       and t2.plce_id = db.plce_id
--                                       and t2.user_id = db.user_id


                             left join openQuery(verticaunited,
                          'select * from diap01.mec_us_united_20056.ual_freq_dbm_cost') as db
                          on t2.dcmdate = db.dcmdate
                      and cast(t2.plce_id as varchar(6)) = db.plce_id
                                      and t2.user_id = db.user_id


                                  ) as t3

                              group by
                                  t3.user_id,
                                  t3.dcmmonth,
--                                   t3.PackageCat,
                                  t3.costmethod,
                                  t3.edDate,
                                  t3.stDate,
                                  t3.dcmDate,
                                  t3.rate,
                                  t3.cost_id,
                                  t3.planned_amt,
                                  t3.planned_cost,
--                                   t3.dvjoinkey,
--                                   t3.mtjoinkey,
--                                   t3.ivjoinkey,
--                                   t3.campaign ,
--                                   t3.campaign_id,
--                                   t3.placement,
                                  t3.placement_id,
                                  t3.dv_map,
--                                 t3.flatcost,
                                  t3.plce_id
                          ) as t4
-- where t4.costmethod != 'Flat'
                  ) as t5
         ) as t6
) as t7
