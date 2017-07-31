alter procedure dbo.crt_dfa_cost_dt2
as
if OBJECT_ID('master.dbo.dfa_cost_dt2',N'U') is not null
    drop table master.dbo.dfa_cost_dt2;


create table master.dbo.dfa_cost_dt2
(
  cost_id       nvarchar(6)    not null,
  plce_id       nvarchar(6)    not null,
  dcmDate       int            not null,
  prsCostMethod nvarchar(100)  not null,
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
  tix           int            not null,
  vew_led       int            not null,
  clk_led       int            not null,
  led           int            not null
);

insert into master.dbo.dfa_cost_dt2
 select
    t8.cost_id       as cost_id,
    t8.plce_id       as plce_id,
    t8.dcmDate       as dcmDate,
    t8.costmethod    as prsCostMethod,
--     t8.PackageCat    as PackageCat,
    t8.rate          as prsRate,
    t8.stDate        as prsStDate,
    t8.edDate        as prsEdDate,
    t8.diff          as diff,
    t8.flatcost      as flatcost,
    t8.cost          as cost,
    -- case when t8.costmethod like '[Ff]lat' then t8.flatcost else t8.cost end as cost,
    t8.lagCost       as lagCost,
    t8.costRunTot    as costRunTot,
    t8.costRemain    as costRemain,
    t8.lagCostRemain as lagCostRemain,
    t8.Imps          as Imps,
    t8.impsRunTot    as impsRunTot,
    t8.impsRemain    as impsRemain,
    t8.dlvrimps      as dlvrimps,
    t8.dfa_imps      as dfa_imps,
    t8.dv_imps       as dv_imps,
    t8.iv_imps       as iv_imps,
    t8.mt_imps       as mt_imps,
    t8.clks          as clks,
    t8.clksRunTot    as clksRunTot,
    t8.clksRemain    as clksRemain,
    t8.planned_amt   as planned_amt,
    t8.planned_cost  as planned_cost,
    t8.vew_con       as vew_con,
    t8.clk_con       as clk_con,
    t8.vew_tix       as vew_tix,
    t8.clk_tix       as clk_tix,
    t8.vew_rev       as vew_rev,
    t8.clk_rev       as clk_rev,
    t8.rev           as rev,
    t8.con           as con,
    t8.tix           as tix,
    t8.vew_led       as vew_led,
    t8.clk_led       as clk_led,
    t8.led           as led
    from (


             select
                 t7.dcmmonth                                        as dcmmonth,
                 t7.dcmDate                                         as dcmDate,
                 t7.cost_id                                         as cost_id,
                 t7.plce_id                                         as plce_id,
                 t7.costmethod                                      as costmethod,
                 t7.dvjoinkey                                       as dvjoinkey,
                 t7.mtjoinkey                                       as mtjoinkey,
                 t7.ivjoinkey                                       as ivjoinkey,
--                  t7.campaign                                        as campaign,
                 t7.campaign_id                                     as campaign_id,
--                  t7.placement                                       as placement,
                 t7.placement_id                                    as placement_id,
                 t7.dv_map                                          as dv_map,
                 t7.rate                                                as rate,
--                  t7.PackageCat                                      as PackageCat,
                 t7.stDate                                          as stDate,
                 t7.edDate                                          as edDate,
                 isNull(t7.diff,cast(0 as int))                     as diff,
                 isNull(t7.flatcost,cast(0 as decimal(20,10)))       as flatCost,

-- ############# FINAL COST CALCULATION #################
-- If planned Impressions are reached (impsRunTot = planned_amt) before cost is capped, cost will be returned as a negative number
                 case

--               cond_2_CPC
                 when t7.costmethod like '[Cc][Pp][Cc]%' and
                      t7.diff >= 0 and
                      t7.cost < t7.planned_cost and
                      t7.clksRunTot <= t7.planned_amt
                 then t7.cost


-- --               TEMPORARY CONDITION FOR SAN JOSE
-- --               cond_2_dCPM
--                  when t7.costmethod like '[Dd][Cc][Pp][Mm]%' and
--                       t7.campaign_id = 11224605
--                  then t7.cost


--               cond_2_dCPM
                 when t7.costmethod like '[Dd][Cc][Pp][Mm]%'
--                      and
--                       t7.diff >= 0 and
--                       t7.cost < t7.planned_cost and
--                       t7.costRunTot <= t7.planned_cost and
--                       t7.lagCostRemain > 0
                 then t7.cost

--               cond_2_1
                 when t7.diff >= 0 and
                      t7.cost < t7.planned_cost and
                      t7.impsRunTot <= t7.planned_amt
                 then t7.cost

--               cond_2_2
                 when t7.diff >= 0 and
                      t7.costRemain = 0 and
                      t7.lagCostRemain > 0
                 then t7.lagCostRemain

--               cond_2_3
                 when t7.diff >= 0 and
                      t7.lagCost = 0 and
                      t7.lagCostRemain = 0
                 then cast(0 as decimal(20,10))

--               cond_2_4
                 when t7.cost = 0
                 then 0

--               cond_2_5
                 when t7.cost >= t7.lagCostRemain
                 then t7.lagCostRemain

--               cond_2_else
                 else t7.cost - t7.lagCostRemain
                 end                                                as cost,

                 t7.cost                                            as cost_plain,
                 t7.cond_1                                          as cond_1,

/*               Second field to help debug when cost is calculated incorrectly.
                 Condition order matches that of the cost case statement above.
 */
                 case
                 when t7.costmethod like '[Cc][Pp][Cc]%' and
                      t7.diff >= 0 and
                      t7.cost < t7.planned_cost and
                      t7.clksRunTot <= t7.planned_amt
                 then 'cond_2_CPC'


                 when t7.costmethod like '[Dd][Cc][Pp][Mm]%'
--                      and
--                       t7.diff >= 0 and
--                       t7.cost < t7.planned_cost and
--                       t7.costRunTot <= t7.planned_cost and
--                       t7.lagCostRemain > 0
                 then 'cond_2_dCPM'


                 when t7.diff >= 0 and
                      t7.cost < t7.planned_cost and
                      t7.impsRunTot <= t7.planned_amt
                 then 'cond_2_1'

                 when t7.diff >= 0 and
                      t7.costRemain = 0 and
                      t7.lagCostRemain > 0
                 then 'cond_2_2'

                 when t7.diff >= 0 and
                      t7.lagCost = 0 and
                      t7.lagCostRemain = 0
                 then 'cond_2_3'

                 when t7.cost = 0
                 then 'cond_2_4'

                 when t7.cost >= t7.lagCostRemain
                 then 'cond_2_5'

                 else 'cond_2_else'
                 end                                                as cond_2,
                 isNull(t7.lagCost,cast(0 as decimal(20,10)))       as lagCost,
                 isNull(t7.costRunTot,cast(0 as decimal(20,10)))    as costRunTot,
                 isNull(t7.costRemain,cast(0 as decimal(20,10)))    as costRemain,
                 isNull(t7.lagCostRemain,cast(0 as decimal(20,10))) as lagCostRemain,
                 isNull(t7.Imps,cast(0 as decimal(20,10)))          as Imps,
                 isNull(t7.impsRunTot,cast(0 as decimal(20,10)))    as impsRunTot,
                 isNull(t7.impsRemain,cast(0 as int))               as impsRemain,
                 isNull(t7.planned_amt,cast(0 as int))              as planned_amt,
                 isNull(t7.planned_cost,cast(0 as int))             as planned_cost,
                 isNull(t7.dlvrimps,cast(0 as int))                 as dlvrimps,
                 isNull(t7.dfa_imps,cast(0 as int))                 as dfa_imps,
                 isNull(t7.iv_imps,cast(0 as int))                  as iv_imps,
                 isNull(t7.dv_imps,cast(0 as int))                  as dv_imps,
                 isNull(t7.mt_imps,cast(0 as decimal(20,10)))       as mt_imps,
                 isNull(t7.clks,cast(0 as decimal(20,10)))          as clks,
                 isNull(t7.clksRunTot,cast(0 as decimal(20,10)))    as clksRunTot,
                 isNull(t7.clksRemain,cast(0 as int))               as clksRemain,
                 isNull(t7.vew_con,cast(0 as int))                  as vew_con,
                 isNull(t7.clk_con,cast(0 as int))                  as clk_con,
                 isNull(t7.vew_tix,cast(0 as int))                  as vew_tix,
                 isNull(t7.clk_tix,cast(0 as int))                  as clk_tix,
                 isNull(t7.vew_rev,cast(0 as decimal(20,10)))       as vew_rev,
                 isNull(t7.clk_rev,cast(0 as decimal(20,10)))       as clk_rev,
                 isNull(t7.rev,cast(0 as decimal(20,10)))           as rev,
                 isNull(t7.con,cast(0 as int))                      as con,
                 isNull(t7.tix,cast(0 as int))                      as tix,
                 isNull(t7.vew_led,cast(0 as decimal(20,10)))       as vew_led,
                 isNull(t7.clk_led,cast(0 as decimal(20,10)))       as clk_led,
                 isNull(t7.led,cast(0 as decimal(20,10)))           as led

             from (
                      select
                        t6.dcmmonth                                     as dcmmonth,
                        t6.dcmDate                                      as dcmDate,
                        t6.cost_id                                      as cost_id,
                        t6.plce_id                                      as plce_id,
                        t6.costmethod                                   as costmethod,
                        t6.dvjoinkey                                    as dvjoinkey,
                        t6.mtjoinkey                                    as mtjoinkey,
                        t6.ivjoinkey                                    as ivjoinkey,
--                         t6.campaign                                     as campaign,
                        t6.campaign_id                                  as campaign_id,
--                         t6.placement                                    as placement,
                        t6.placement_id                                 as placement_id,
                        t6.dv_map                                       as dv_map,
                        t6.rate                                         as rate,
--                         t6.PackageCat                                   as PackageCat,
                        t6.stDate                                       as stDate,
                        t6.edDate                                       as edDate,
                        isNull(t6.diff,cast(0 as decimal(20,10)))       as diff,
                        t6.flatcost as flatcost,
                        case
                        when t6.cost is null
                        then cast(0 as decimal(20,10))


-- --                      TEMPORARY CONDITION FOR SAN JOSE
--                          when t6.costmethod like '[Dd][Cc][Pp][Mm]%' and
--                               t6.campaign_id = 11224605
--                          then t6.cost


                        when t6.diff >= 0 and
                             t6.costmethod like '[Cc][Pp][Cc]%' and
                             t6.clks >= t6.planned_amt
                        then isNull(t6.cost,cast(0 as decimal(20,10))) - isNull(t6.lagCost,cast(0 as decimal(20,10)))

                        when t6.diff >= 0 and
                             t6.Imps >= t6.planned_amt
                        then isNull(t6.cost,cast(0 as decimal(20,10))) - isNull(t6.lagCost,cast(0 as decimal(20,10)))

                        else isNull(t6.cost,cast(0 as decimal(20,10)))
                        end                                             as cost,

/*                      First field to help debug when cost is calculated incorrectly.
                        Condition order matches that of the cost case statement above.
 */
                        case
                        when t6.cost is null
                        then 'cond_1_1'

                        when t6.diff >= 0 and
                             t6.costmethod like '[Cc][Pp][Cc]%' and
                             t6.clks >= t6.planned_amt
                        then 'cond_1_2'

                        when t6.diff >= 0 and
                             t6.Imps >= t6.planned_amt
                        then 'cond_1_3'

                        else 'cond_1_else'
                        end                                             as cond_1,

                        isNull(t6.lagCost,cast(0 as decimal(20,10)))    as lagCost,
                        isNull(t6.costRunTot,cast(0 as decimal(20,10))) as costRunTot,
                        isNull(t6.costRemain,cast(0 as decimal(20,10))) as costRemain,
                        lag(isNull(t6.costRemain,cast(0 as decimal(20,10))),1,0) over (partition by t6.cost_id
                          order by t6.dcmDate,plce_id)                as lagCostRemain,
                        isNull(t6.Imps,cast(0 as int))                  as Imps,
                        isNull(t6.impsRunTot,cast(0 as int))            as impsRunTot,
                        isNull(t6.impsRemain,cast(0 as int))            as impsRemain,
                        isNull(t6.planned_amt,cast(0 as int))           as planned_amt,
                        isNull(t6.planned_cost,cast(0 as int))          as planned_cost,
                        t6.billimps                                     as billimps,
                        t6.dlvrimps                                     as dlvrimps,
                        t6.dfa_imps                                     as dfa_imps,
                        t6.iv_imps                                      as iv_imps,
                        t6.dv_imps                                      as dv_imps,
                        t6.mt_imps                                      as mt_imps,
                        isNull(t6.clks,cast(0 as int))                  as clks,
                        isNull(t6.clksRunTot,cast(0 as int))            as clksRunTot,
                        isNull(t6.clksRemain,cast(0 as int))            as clksRemain,
                        t6.vew_con                                      as vew_con,
                        t6.clk_con                                      as clk_con,
                        t6.vew_tix                                      as vew_tix,
                        t6.clk_tix                                      as clk_tix,
                        t6.vew_rev                                      as vew_rev,
                        t6.clk_rev                                      as clk_rev,
                        t6.rev                                          as rev,
                        t6.con                                          as con,
                        t6.tix                                          as tix,
                        t6.vew_led                                      as vew_led,
                        t6.clk_led                                      as clk_led,
                        t6.led                                          as led
             from (
                    select
                      t5.dcmmonth                                                                               as dcmmonth,
                      t5.dcmDate                                                                                as dcmDate,
                      t5.cost_id                                                                                as cost_id,
                      t5.plce_id                                                                                as plce_id,
                      t5.costmethod                                                                             as costmethod,
                      t5.dvjoinkey                                                                              as dvjoinkey,
                      t5.mtjoinkey                                                                              as mtjoinkey,
                      t5.ivjoinkey                                                                              as ivjoinkey,
--                       t5.campaign                                                                               as campaign,
                      t5.campaign_id                                                                            as campaign_id,
--                       t5.placement                                                                              as placement,
                      t5.placement_id                                                                           as placement_id,
                      t5.dv_map                                                                                 as dv_map,
                      t5.rate                                                                                   as rate,
--                       t5.PackageCat                                                                             as PackageCat,
                      t5.stDate                                                                                 as stDate,
                      t5.edDate                                                                                 as edDate,
                      t5.planned_cost                                                                           as planned_cost,
                      t5.planned_amt                                                                            as planned_amt,
                      isNull(t5.ed_diff,cast(0 as decimal(20,10)))                                              as diff,
                      t5.flatcost as flatcost,
                      sum(t5.cost) over (partition by t5.cost_id,t5.plce_id,t5.dcmDate)                         as cost,
                      lag(t5.cost,1,0) over (partition by t5.cost_id
                        order by t5.dcmDate,t5.plce_id)                                                       as lagCost,
                      sum(t5.cost) over (partition by t5.cost_id
                        order by t5.dcmDate,t5.plce_id asc range between unbounded preceding and current row) as costRunTot,
                      case
                      when (cast(t5.planned_cost as decimal(20,10)) - sum(t5.cost) over (partition by t5.cost_id
                        order by t5.dcmDate,t5.plce_id asc range between unbounded preceding and current row)) <= 0 then 0
                      else (cast(t5.planned_cost as decimal(20,10)) - sum(t5.cost) over (partition by t5.cost_id
                        order by t5.dcmDate,t5.plce_id asc range between unbounded preceding and current row))
                      end                                                                                       as costRemain,

                      sum(t5.billimps) over (partition by t5.cost_id,t5.plce_id,t5.dcmDate)                     as Imps,
                      sum(t5.billimps) over (partition by t5.cost_id
                        order by t5.dcmDate,t5.plce_id asc range between unbounded preceding and current row) as impsRunTot,
                      case
                      when (cast(t5.planned_amt as decimal(20,10)) - sum(t5.billimps) over (partition by t5.cost_id
                        order by t5.dcmDate,t5.plce_id asc range between unbounded preceding and current row)) <= 0 then 0
                      else (cast(t5.planned_amt as decimal(20,10)) - sum(t5.billimps) over (partition by t5.cost_id
                        order by t5.dcmDate,t5.plce_id asc range between unbounded preceding and current row))
                      end                                                                                       as impsRemain,
                      t5.billimps                                                                               as billimps,
                      t5.dlvrimps                                                                               as dlvrimps,
                      t5.dfa_imps                                                                               as dfa_imps,
                      t5.iv_imps                                                                                as iv_imps,
                      t5.dv_imps                                                                                as dv_imps,
                      t5.mt_imps                                                                                as mt_imps,
                      sum(t5.clicks) over (partition by t5.cost_id,t5.plce_id,t5.dcmDate)                       as clks,
                      sum(t5.clicks) over (partition by t5.cost_id
                        order by t5.dcmDate,t5.plce_id asc range between unbounded preceding and current row) as clksRunTot,
                      case
                      when (cast(t5.planned_amt as decimal(20,10)) - sum(t5.clicks) over (partition by t5.cost_id
                        order by t5.dcmDate,t5.plce_id asc range between unbounded preceding and current row)) <= 0 then 0
                      else (cast(t5.planned_amt as decimal(20,10)) - sum(t5.clicks) over (partition by t5.cost_id
                        order by t5.dcmDate,t5.plce_id asc range between unbounded preceding and current row))
                      end                                                                                       as clksRemain,
                      t5.vew_con                                                                                as vew_con,
                      t5.clk_con                                                                                as clk_con,
                      t5.vew_tix                                                                                as vew_tix,
                      t5.clk_tix                                                                                as clk_tix,
                      t5.vew_rev                                                                                as vew_rev,
                      t5.clk_rev                                                                                as clk_rev,
                      t5.rev                                                                                    as rev,
                      t5.con                                                                                    as con,
                      t5.tix                                                                                    as tix,
                      t5.vew_led                                                                                as vew_led,
                      t5.clk_led                                                                                as clk_led,
                      t5.led                                                                                    as led

                      from
                          (
                            select
                              t4.dcmmonth                                      as dcmmonth,
                              t4.dcmDate                                       as dcmDate,
                              t4.cost_id                                       as cost_id,
                              t4.plce_id                                       as plce_id,
                              t4.costmethod                                    as costmethod,
                              t4.dvjoinkey                                     as dvjoinkey,
                              t4.mtjoinkey                                     as mtjoinkey,
                              t4.ivjoinkey                                     as ivjoinkey,
--                               t4.campaign                                      as campaign,
                              t4.campaign_id                                   as campaign_id,
--                               t4.placement                                     as placement,
                              t4.placement_id                                  as placement_id,
                              t4.dv_map                                        as dv_map,
                              t4.rate                                          as rate,
--                               t4.PackageCat                                    as PackageCat,
                              t4.stDate                                        as stDate,
                              t4.edDate                                        as edDate,
                              t4.planned_cost                                  as planned_cost,
                              t4.planned_amt                                   as planned_amt,
                              case when t4.costmethod like '[Ff]lat' then t4.flatcost/max(t4.cst_count) else 0 end      as flatcost,
                              case

--                               un-comment if dCPM is coming out 0
--                               when t4.costmethod like '[Dd][Cc][Pp][Mm]%' then sum(t4.cost)

                              when t4.dcmDate - t4.stDate < 0 then 0
                              when (t4.costmethod like '[Cc][Pp][Cc]' and (t4.edDate - t4.dcmDate) >= 0 and sum(t4.clicks) >= t4.planned_amt) then t4.planned_cost
                              when t4.costmethod not like '[Dd][Cc][Pp][Mm]%' and (t4.edDate - t4.dcmDate) >= 0 and sum(t4.billimps) >= t4.planned_amt then t4.planned_cost
                              when (t4.edDate - t4.dcmDate) >= 0 then sum(t4.cost)
                              when (t4.edDate - t4.dcmDate) < 0 then 0
                              else 0
                              end                                              as cost,

                              case
                              when t4.dcmDate - t4.stDate < 0 then 'cond_0_1'
                              when (t4.edDate - t4.dcmDate) >= 0 and t4.costmethod like '[Cc][Pp][Cc]' and sum(t4.clicks) >= t4.planned_amt then 'cond_0_2'
                              when (t4.edDate - t4.dcmDate) >= 0 and sum(t4.billimps) >= t4.planned_amt then 'cond_0_2'
                              when (t4.edDate - t4.dcmDate) >= 0 then 'cond_0_3'
                              when (t4.edDate - t4.dcmDate) < 0 then 'cond_0_4'
                              else 'cond_0_else'
                              end                                              as cond_0,
                                  sum(t4.dbm_cost) as dbm_cost,
                              cast(t4.edDate as int) - cast(t4.dcmDate as int) as ed_diff,
                              case
                              when t4.dcmDate - t4.stDate < 0 then 0
                              else sum(t4.billimps) end                        as billimps,
                              sum(t4.dlvrimps)                                 as dlvrimps,
                              sum(t4.dfa_imps)                                 as dfa_imps,
                              sum(t4.iv_imps)                                  as iv_imps,
                              sum(t4.dv_imps)                                  as dv_imps,
                              sum(t4.mt_imps)                                  as mt_imps,
                              sum(t4.clicks)                                   as clicks,
                              sum(t4.vew_con)                                  as vew_con,
                              sum(t4.clk_con)                                  as clk_con,
                              sum(t4.vew_tix)                                  as vew_tix,
                              sum(t4.clk_tix)                                  as clk_tix,
                              sum(t4.vew_rev)                                  as vew_rev,
                              sum(t4.clk_rev)                                  as clk_rev,
                              sum(t4.rev)                                      as rev,
                              sum(t4.con)                                      as con,
                              sum(t4.tix)                                      as tix,
                              sum(t4.vew_led)                                  as vew_led,
                              sum(t4.clk_led)                                  as clk_led,
                              sum(t4.led)                                      as led

                              from
                                  (

select

  [dbo].udf_dateToInt(t3.dcmdate)        as dcmdate,
  t3.dcmmonth                            as dcmmonth,
  t3.diff                                as diff,
  t3.dvjoinkey                           as dvjoinkey,
  t3.mtjoinkey                           as mtjoinkey,
  t3.ivjoinkey                           as ivjoinkey,
  t3.campaign                            as campaign,
  t3.campaign_id                         as campaign_id,
  t3.placement                           as placement,
  t3.plce_id                             as plce_id,
  t3.placement_id                        as placement_id,
  t3.dv_map                              as dv_map,
  t3.packagecat                          as packagecat,
  t3.cost_id                             as cost_id,
  t3.cst_count                           as cst_count,
  t3.costmethod                          as costmethod,
  t3.flatcost as flatcost,
  [dbo].udf_dateToInt(t3.placementend)   as edDate,
  [dbo].udf_dateToInt(t3.placementstart) as stDate,
  t3.planned_amt                         as planned_amt,
  t3.planned_cost                        as planned_cost,
  case when t3.costmethod = 'dCPM' then db.dbm_cost
  else t3.cost end                       as cost,
  db.dbm_cost                            as dbm_cost,
  t3.rate                                as rate,
  case when t3.costmethod = 'dCPM' then db.imps
  else t3.dlvrimps end                   as dlvrimps,
  case when t3.costmethod = 'dCPM' then db.imps
  else t3.billimps end                   as billimps,
  case when t3.costmethod = 'dCPM' then db.imps
  else t3.dfa_imps end                   as dfa_imps,
  t3.iv_imps                             as iv_imps,
  t3.dv_imps                             as dv_imps,
  t3.mt_imps                             as mt_imps,
  case when t3.costmethod = 'dCPM' then db.clicks
  else t3.clicks end                     as clicks,
-- bring in DBM metrics
  db.vew_con                             as vew_con,
  db.clk_con                             as clk_con,
  db.con                                 as con,
  db.vew_tix                             as vew_tix,
  db.clk_tix                             as clk_tix,
  db.tix                                 as tix,
  db.vew_rev                             as vew_rev,
  db.clk_rev                             as clk_rev,
  db.rev                                 as rev,
  db.vew_led                             as vew_led,
  db.clk_led                             as clk_led,
  db.led                                 as led


-- ============================================================================================================================================

from (
--
--
-- declare @report_st date,
-- @report_ed date;
-- --
-- set @report_ed = '2017-03-27';
-- set @report_st = '2017-02-14';

         select
             cast(t2.dcmdate as date)                                                   as dcmdate,
             t2.dcmmonth                                                                as dcmmonth,
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
--  Count of # times cost_id appears per day. Important b/c planned_amt and planned_cost are listed at
--    cost_id level.
             sum(1) over (partition by t2.cost_id,t2.dcmdate order by
             t2.dcmmonth asc range between unbounded preceding and current row)         as cst_count,
             t2.plce_id                                                                 as plce_id,
             t2.placement                                                               as placement,
             t2.placement_id                                                            as placement_id,
             t2.placementend                                                            as placementend,
             t2.placementstart                                                          as placementstart,
             t2.dv_map                                                                  as dv_map,
             t2.planned_amt                                                             as planned_amt,
             t2.planned_cost                                                            as planned_cost,
             flat.flatcost                                                              as flatcost,
  --  logic excludes flat fees
             case
             --  Zeros out cost for placements traffic before specified start date or after specified end date
             when   (
                    (t2.dv_map = 'N' or t2.dv_map = 'Y') and
                    (t2.eddate - t2.dcmmatchdate < 0 or t2.dcmmatchdate - t2.stdate < 0) and
                    (t2.costmethod = 'CPM' or t2.costmethod = 'CPMV' or t2.costmethod = 'CPE' or t2.costmethod = 'CPC' or t2.costmethod = 'CPCV')
                    )
             then   cast(0 as decimal(20,10))

             --  Click-based cost; source Innovid
             when   (
                    (t2.dv_map = 'Y' or t2.dv_map = 'N') and
                    (t2.eddate - t2.dcmmatchdate >= 0 or t2.dcmmatchdate - t2.stdate >= 0) and
                    (t2.costmethod = 'CPC' or t2.costmethod = 'CPCV') and
                    (len(isnull(iv.joinkey,'')) > 0)
                    )
             then   cast((sum(cast(iv.click_thrus as decimal(20,10))) * cast(t2.rate as decimal(20,10))) as decimal(20,10))

             --  Click-based cost; source DCM
             when   (
                    (t2.dv_map = 'Y' or t2.dv_map = 'N') and
                    (t2.eddate - t2.dcmmatchdate >= 0 or t2.dcmmatchdate - t2.stdate >= 0) and
                    (t2.costmethod = 'CPC' or t2.costmethod = 'CPCV')
                    )
             then   cast((sum(cast(t2.clicks as decimal(20,10))) * cast(t2.rate as decimal(20,10))) as decimal(20,10))

             --  Impression-based cost; not subject to viewability; Innovid source
             when   (
                    (t2.dv_map = 'N') and
                    (t2.eddate - t2.dcmmatchdate >= 0 or t2.dcmmatchdate - t2.stdate >= 0) and
                    (t2.costmethod = 'CPM' or t2.costmethod = 'CPMV' or t2.costmethod = 'CPE') and
                    (len(isnull(iv.joinkey,'')) > 0)
                    )
             then   cast((sum(cast(iv.impressions as decimal(20,10))) * cast(t2.rate as decimal(20,10))) / 1000 as
                           decimal(20,10))

             --  Impression-based cost; not subject to viewability; DCM source - CPE
             when   (
                    (t2.dv_map = 'N') and
                    (t2.eddate - t2.dcmmatchdate >= 0 or t2.dcmmatchdate - t2.stdate >= 0) and
                    (t2.costmethod = 'CPE')
                    )
             then   cast((sum(cast(t2.impressions as decimal(20,10))) * cast(t2.rate as decimal(20,10))) as decimal(20,10))

             --  Impression-based cost; not subject to viewability; DCM source
             when   (
                    (t2.dv_map = 'N') and
                    (t2.eddate - t2.dcmmatchdate >= 0 or t2.dcmmatchdate - t2.stdate >= 0) and
                    (t2.costmethod = 'CPM' or t2.costmethod = 'CPMV')
                    )
             then   cast((sum(cast(t2.impressions as decimal(20,10))) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))

             --  Impression-based cost; subject to viewability with DV flag; DV data present
             when   (
                    (t2.dv_map = 'Y') and
                    (t2.eddate - t2.dcmmatchdate >= 0 or t2.dcmmatchdate - t2.stdate >= 0) and
                    (t2.costmethod = 'CPM' or t2.costmethod = 'CPMV' or t2.costmethod = 'CPE') and
                    (len(isnull(dv.joinkey,'')) > 0)
                    )
             then   cast((sum(cast(dv.groupm_billable_impressions as decimal(20,10))) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))

             --  Impression-based cost; subject to viewability with DV flag; DV data not present; MT data present
             when   (
                    (t2.dv_map = 'Y') and
                    (t2.eddate - t2.dcmmatchdate >= 0 or t2.dcmmatchdate - t2.stdate >= 0) and
                    (t2.costmethod = 'CPM' or t2.costmethod = 'CPMV' or t2.costmethod = 'CPE') and
                    (len(isnull(dv.joinkey,'')) = 0) and
                    (len(isnull(mt.joinkey,'')) > 0)
                    )
             then   cast((sum(cast(mt.groupm_billable_impressions as decimal(20,10))) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))


             --  Impression-based cost; subject to viewability; MT source
             when   (
                    (t2.dv_map = 'M') and
                    (t2.eddate - t2.dcmmatchdate >= 0 or t2.dcmmatchdate - t2.stdate >= 0) and
                    (t2.costmethod = 'CPM' or t2.costmethod = 'CPMV' or t2.costmethod = 'CPE') and
                    (len(isnull(mt.joinkey,'')) > 0)
                    )
             then   cast((sum(cast(mt.groupm_billable_impressions as decimal(20,10))) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))


             --  Fixes for Win NY, which Medialets failed to tag
             --  Using average viewability rate for Feb, Mar, Apr
             --  Impression-based cost; subject to viewability; MT source
             when   (
                    (t2.dv_map = 'M') and
                    (t2.campaign_id = 10918234) and
                    (t2.site_id_dcm in (1995643, 1485655, 2854118, 1329066, 3246841)) and
                    (t2.eddate - t2.dcmmatchdate >= 0 or t2.dcmmatchdate - t2.stdate >= 0) and
                    (t2.costmethod = 'CPM' or t2.costmethod = 'CPMV' or t2.costmethod = 'CPE') and
                    (len(isnull(mt.joinkey,'')) = 0)
                    )
             then
                    case
                    when  t2.site_id_dcm = 1995643  -- Verve
                    then
                            case
                            when  t2.dcmmonth = 2
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .59) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            when  t2.dcmmonth = 3
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .77) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            when  t2.dcmmonth = 4
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .79) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
--                             else cast((sum(cast(mt.groupm_billable_impressions as decimal(20,10))) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            end
                    when  t2.site_id_dcm = 1485655  -- Forbes
                    then
                            case
                            when  t2.dcmmonth = 2
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .38) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            when  t2.dcmmonth = 3
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .59) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            when  t2.dcmmonth = 4
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .64) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
--                             else cast((sum(cast(mt.groupm_billable_impressions as decimal(20,10))) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            end
                    when  t2.site_id_dcm = 2854118  -- TapAd
                    then
                            case
                            when  t2.dcmmonth = 2
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .41) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            when  t2.dcmmonth = 3
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .48) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            when  t2.dcmmonth = 4
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .56) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
--                             else cast((sum(cast(mt.groupm_billable_impressions as decimal(20,10))) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            end
                    when  t2.site_id_dcm = 1329066  -- Ninth Decimal
                    then
                            case
                            when  t2.dcmmonth = 2
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .78) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            when  t2.dcmmonth = 3
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .89) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            when  t2.dcmmonth = 4
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .68) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
--                             else cast((sum(cast(mt.groupm_billable_impressions as decimal(20,10))) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            end
                    when  t2.site_id_dcm = 3246841  -- NY Mag
                    then
                            case
                            when  t2.dcmmonth = 2
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .54) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            when  t2.dcmmonth = 3
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .62) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            when  t2.dcmmonth = 4
                            then  cast((sum(cast(t2.impressions as decimal(20,10)) * .64) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
--                             else cast((sum(cast(mt.groupm_billable_impressions as decimal(20,10))) * cast(t2.rate as decimal(20,10))) / 1000 as decimal(20,10))
                            end end





             else   cast(0 as decimal(20,10)) end                                       as cost,


             t2.rate                                                                    as rate,
             --  total impressions as reported by 1.) dcm for "n," 2.) dv for "y," or moat for "m"
             sum    (
                    case


                    when t2.dv_map = 'Y' and
                        (len(isnull(dv.joinkey,'')) = 0) and
                        (len(isnull(mt.joinkey,'')) > 0)
                    then mt.total_impressions

                    when t2.dv_map = 'Y' then dv.total_impressions

                    when t2.dv_map = 'M' and
                        (len(isnull(mt.joinkey,'')) = 0) and
                        (t2.campaign_id = 10918234) and
                        (t2.site_id_dcm in (1995643, 1485655, 2854118, 1329066, 3246841)) and
                        (t2.eddate - t2.dcmmatchdate >= 0 or t2.dcmmatchdate - t2.stdate >= 0) and
                        (t2.costmethod = 'CPM' or t2.costmethod = 'CPMV' or t2.costmethod = 'CPE')
                    then t2.impressions

                    when t2.dv_map = 'M' then mt.total_impressions
                    when t2.dv_map = 'N' and (len(isnull(iv.joinkey,'')) > 0) then iv.impressions
                    else t2.impressions end
                    )                                               as dlvrimps,

             --  billable impressions as reported by 1.) dcm for "n," 2.) dv for "y," or moat for "m"
             sum    (
                    case
                    when t2.dv_map = 'Y' and
                        (len(isnull(dv.joinkey,'')) = 0) and
                        (len(isnull(mt.joinkey,'')) > 0)
                    then mt.groupm_billable_impressions
                    when t2.dv_map = 'Y' then dv.groupm_billable_impressions
                    when t2.dv_map = 'M' and
                        (len(isnull(mt.joinkey,'')) = 0) and
                        (t2.campaign_id = 10918234) and
                        (t2.site_id_dcm in (1995643, 1485655, 2854118, 1329066, 3246841)) and
                        (t2.eddate - t2.dcmmatchdate >= 0 or t2.dcmmatchdate - t2.stdate >= 0) and
                        (t2.costmethod = 'CPM' or t2.costmethod = 'CPMV' or t2.costmethod = 'CPE')

                    then t2.impressions
                    when t2.dv_map = 'M' then mt.groupm_billable_impressions
                    when t2.dv_map = 'N' and (len(isnull(iv.joinkey,'')) > 0) then iv.impressions
                    else t2.impressions end
                    )                                               as billimps,

             sum(t2.impressions)                                                        as dfa_imps,
             sum(iv.impressions)                                                        as iv_imps,
             sum(iv.click_thrus)                                                        as iv_clicks,
             sum(iv.all_completion)                                                     as iv_completes,
             sum(cast(dv.total_impressions as int))                                     as dv_imps,
             sum(dv.groupm_passed_impressions)                                          as dv_viewed,
             sum(cast(dv.groupm_billable_impressions as decimal(20,10)))                as dv_groupmpayable,
             sum(cast(mt.total_impressions as int))                                     as mt_imps,
             sum(mt.groupm_passed_impressions)                                          as mt_viewed,
             sum(cast(mt.groupm_billable_impressions as decimal(20,10)))                as mt_groupmpayable,

             sum(case
                 when (len(isnull(iv.joinkey,'')) > 0) then iv.click_thrus
                 else t2.clicks end)                                                    as clicks

         from
             (
-- =========================================================================================================================
                  select
                    t1.dcmdate                                                                            as dcmdate,
                    cast(month(cast(t1.dcmdate as date)) as int)                                          as dcmmonth,
                    [dbo].udf_dateToInt(t1.dcmdate)                                                       as dcmmatchdate,
                    t1.campaign                                                                           as campaign,
                    t1.campaign_id                                                                        as campaign_id,
                    t1.site_dcm                                                                           as site_dcm,
                    t1.site_id_dcm                                                                        as site_id_dcm,
                    case
                    when t1.plce_id in ('PBKB7J','PBKB7H','PBKB7K')
                    then 'PBKB7J'
                    else t1.plce_id end                                                                   as plce_id,
                    case
                    when t1.placement like 'PBKB7J%' or
                         t1.placement like 'PBKB7H%' or
                         t1.placement like 'PBKB7K%' or
                         t1.placement = 'United 360 - Polaris 2016 - Q4 - Amobee'
                    then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
                    else t1.placement end                                                                 as placement,
                  --   amobee video 360 placements, tracked differently across dcm, innovid, and moat; this combines the three placements into one
                    case
                    when t1.placement_id in (137412510,137412401,137412609)
                    then 137412609
                    else t1.placement_id end                                                              as placement_id,
                    prs.stdate                                                                            as stdate,
                    case
                    when t1.campaign_id = 9923634 and
                         t1.site_id_dcm != 1190258
                    then 20161022
                    else prs.eddate end                                                                   as eddate,
                    prs.packagecat                                                                        as packagecat,
                    prs.costmethod                                                                        as costmethod,
                    prs.cost_id                                                                           as cost_id,
                    prs.planned_amt                                                                       as planned_amt,
                    prs.planned_cost                                                                      as planned_cost,
                    prs.placementstart                                                                    as placementstart,
                    case
                    when t1.campaign_id = 9923634 and
                         t1.site_id_dcm != 1190258
                    then '2016-10-22'
                    else prs.placementend end                                                             as placementend,

                    sum((cast(t1.impressions as decimal(20,10)) / nullif(
                      cast(prs.planned_amt as decimal(20,10)),0)) * cast(prs.rate as
                                                                           decimal(20,10)))               as incrflatcost,
                    cast(prs.rate as decimal(20,10))                                                      as rate,
                    sum(t1.impressions)                                                                   as impressions,
                    sum(t1.clicks)                                                                        as clicks,
                  --        sum(t1.con)                                                        as con,
                  --        sum(t1.tix)                                                         as tix,

                    case when cast(month(prs.placementend) as int) - cast(month(cast(t1.dcmdate as date)) as
                                                                          int) <= 0 then 0
                    else cast(month(prs.placementend) as int) - cast(month(cast(t1.dcmdate as date)) as
                                                                     int) end                             as diff,

                    [dbo].udf_dvMap(t1.campaign_id,t1.site_id_dcm,t1.placement,prs.CostMethod,prs.dv_map) as dv_map
                 -- ==========================================================================================================================================================

                 -- openquery function call must not exceed 8,000 characters; no room for comments inside the function
from (
select *
from openquery(verticaunited,'
select
cast(report.date as date)                   as dcmdate,
cast(month(cast(report.date as date)) as int) as reportmonth,
campaign.campaign                                as campaign,
report.campaign_id                               as campaign_id,
report.site_id_dcm as site_id_dcm,
directory.site_dcm                    as site_dcm,
left(placements.placement,6) as plce_id,
-- placements.placement                              as placement,
replace(replace(placements.placement ,'','', ''''),''"'','''') as placement,
report.placement_id                         as placement_id,
sum(report.impressions)                     as impressions,
sum(report.clicks)                          as clicks,
sum(report.con)                  as con,
sum(report.tix)                 as tix

from (

select
cast(timestamp_trunc(to_timestamp(ta.interaction_time / 1000000), ''SS'') as date) as "date"
,ta.campaign_id                                                                       as campaign_id
,ta.site_id_dcm                                                                        as site_id_dcm
,ta.placement_id                                                                        as placement_id
,0                                                                                          as impressions
,0                                                                                          as clicks
,sum(case when ta.conversion_id = 1 or ta.conversion_id = 2 then 1 else 0 end) as con
,sum(case when ta.conversion_id = 1 or ta.conversion_id = 2 then ta.total_conversions else 0 end) as tix

from
(
select *
from diap01.mec_us_united_20056.dfa2_activity
where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000), ''SS'') as date) > ''2017-01-01''
and not regexp_like(substring(other_data,(instr(other_data,''u3='') + 3),5),''mil.*'',''ib'')
and total_revenue != 0
and total_conversions != 0
and activity_id = 978826
and (advertiser_id <> 0)
) as ta


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
,0                                    as con
,0                                    as tix


from  (
select *
from diap01.mec_us_united_20056.dfa2_impression
where cast(timestamp_trunc(to_timestamp(event_time / 1000000), ''SS'') as date) > ''2017-01-01''

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
,0                                    as con
,0                                    as tix

from  (

select *
from diap01.mec_us_united_20056.dfa2_click
where cast(timestamp_trunc(to_timestamp(event_time / 1000000), ''SS'') as date) > ''2017-01-01''
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
select cast(t2.placement as varchar(4000)) as ''placement'',  t2.placement_id as ''placement_id'', t2.campaign_id as ''campaign_id'', t2.site_id_dcm as ''site_id_dcm''

from (select campaign_id as campaign_id, site_id_dcm as site_id_dcm, placement_id as placement_id, placement as placement, cast(placement_start_date as date) as thisdate,
row_number() over (partition by campaign_id, site_id_dcm, placement_id  order by cast(placement_start_date as date) desc) as r1
from diap01.mec_us_united_20056.dfa2_placements

) as t2
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

where not regexp_like(placements.placement,''.do\s*not\s*use.'',''ib'')
-- and not regexp_like(campaign.campaign,''.2016.'',''ib'')
and not regexp_like(campaign.campaign,''.*Search.*'',''ib'')
and not regexp_like(campaign.campaign,''.*BidManager.*'',''ib'')
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

                      ) as t1


                     left join
                     (
                         select *
                         from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].prs_summ
                     ) as prs
                         on t1.placement_id = prs.adserverplacementid
--    where prs.costmethod != 'Flat'
--     where prs.cost_id = 'PFHH1N'
where t1.site_id_dcm =1578478
                     and prs.eddate >= 20170101

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

             left join
             (
                 select *
                 from master.dbo.dfa_flatCost_dt2
             ) as flat
                 on t2.cost_id = flat.cost_id
                 and t2.dcmmatchdate = flat.dcmdate

-- dv table join ==============================================================================================================================================

             left join (
                           select *
                           from master.dbo.dv_summ
-- where dvdate between @report_st and @report_ed
                       ) as dv
                 on
                     left(t2.placement,6) + '_' + [dbo].udf_sitekey(t2.site_dcm) + '_'
                         + cast(t2.dcmdate as varchar(10)) = dv.joinkey

-- moat table join ==============================================================================================================================================

             left join (
                           select *
                           from master.dbo.mt_summ
-- where mtdate between @report_st and @report_ed
                       ) as mt
                 on
                     left(t2.placement,6) + '_' + [dbo].udf_sitekey(t2.site_dcm) + '_'
                         + cast(t2.dcmdate as varchar(10)) = mt.joinkey


-- innovid table join ==============================================================================================================================================

             left join (
                           select *
                           from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].ivd_summ_agg
-- where ivdate between @report_st and @report_ed
                       ) as iv
                 on
                     left(t2.placement,6) + '_' + [dbo].udf_sitekey(t2.site_dcm) + '_'
                         + cast(t2.dcmdate as varchar(10)) = iv.joinkey

         where t2.campaign not like 'BidManager%Campaign%'
             and t2.site_dcm not like '%DfaSite%'

         group by

             t2.campaign
             ,t2.campaign_id
             ,t2.cost_id
             ,t2.dv_map
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
             ,t2.costmethod
             ,flat.flatcost

     ) as t3
                                               left join (
                           select *
                           from master.dbo.dbm_cost
                       ) as db
                      on [dbo].udf_dateToInt(t3.dcmdate) = db.dcmdate
                      and t3.plce_id = db.plce_id

                                  ) as t4

                              group by
                                  t4.dcmmonth,
--                                   t4.PackageCat,
                                  t4.costmethod,
                                  t4.edDate,
                                  t4.stDate,
                                  t4.dcmDate,
                                  t4.rate,
                                  t4.cost_id,
                                  t4.planned_amt,
                                  t4.planned_cost,
                                  t4.dvjoinkey,
                                  t4.mtjoinkey,
                                  t4.ivjoinkey,
--                                   t4.campaign ,
                                  t4.campaign_id,
--                                   t4.placement,
                                  t4.placement_id,
                                  t4.dv_map,
                                  t4.flatcost,
                                  t4.plce_id
                          ) as t5
-- where t5.costmethod != 'Flat'
                  ) as t6
         ) as t7
        where (len(isnull(t7.cost_id,'')) != 0)
) as t8