
select
con_window,
-- con_date,
sum(conversions_new) as conversions

from
(
    select t4.con_date,
           t4.con_window,
           sum(conversions)                            as conversions,
           sum((conversions - stock_chek) + stock_new) as conversions_new,
           sum(stock_chek)                             as stock_chek,
           sum(stock_prog)                             as stock_prog,
           sum(new_dis)                                as new_dis,
           sum(stock_new)                              as stock_new

    from (
             select t3.con_date,
                    t3.con_window,
                    sum(conversions)                        as conversions,
                    sum(stock_chek)                         as stock_chek,
                    sum(stock_prog)                         as stock_prog,
                    sum(new_dis)                            as new_dis,
-- sum(case when date_flag = 1 and stock_prog = 0 and stock_chek <= 8
--      then stock_chek
--  when date_flag = 1 and stock_chek <= new_dis
--      then stock_chek
--  when date_flag = 1 then new_dis
--  else stock_chek end) as stock_new
                    round(case when date_flag = 1 and sum(stock_prog) = 0 and sum(stock_chek) <= 8
                                   then sum(stock_chek)
                               when date_flag = 1 and sum(stock_chek) <= sum(new_dis)
                                   then sum(stock_chek)
                               when date_flag = 1 then sum(new_dis)
                               else sum(stock_chek) end, 0) as stock_new
             from (
                      select t2.con_date,
                             t2.con_window,
                             case when t2.con_date between '2018-05-09' and '2018-08-16' then 1 else 2 end as date_flag,
                             t2.conversionname,
                             t2.conversiontagid,
                             sum(conversions)                                                              as conversions,
                             sum(stock_chek)                                                               as stock_chek,
                             sum(stock_prog)                                                               as stock_prog,
                             sum(stock_prog * 1.5799076146)                                                as new_dis

                      from (
                               select conversiondatedefaulttimezone::date                           as con_date,
                                      winnereventdatedefaulttimezone::date                          as evt_date,
                                      datediff('day', winnereventdatedefaulttimezone::date,
                                               conversiondatedefaulttimezone::date)                 as con_window,
-- c1.campaignname,
-- t1.campaignid,
                                      c2.conversionname,
                                      t1.conversiontagid,
                                      count(*)                                                      as conversions,
                                      sum(case when t1.conversiontagid = 596537 then 1 else 0 end)  as stock_chek,
                                      sum(case when t1.conversiontagid = 1061036 then 1 else 0 end) as stock_prog
                               from wmprodfeeds.ikea.sizmek_conversion_events t1

                                        left join wmprodfeeds.ikea.sizmek_display_campaigns c1
                                                  on t1.campaignid = c1.campaignid

                                        left join wmprodfeeds.ikea.sizmek_conversions_tags c2
                                                  on t1.conversiontagid = c2.conversiontagid


                               where conversiondatedefaulttimezone::date between '2018-03-01' and '2018-08-31' and
                                     t1.campaignid = 813500 -- Commercial
                                     and isconversion = true
                                     and t1.eventtypeid = 1
--          and t1.conversiontagid = 1061036
--                                    and t1.conversiontagid in (593715, 596537, 598734, 598735, 598736, 598737, 598738,
--                                                               598739,
--                                                               598740, 598741, 598742, 598743, 598744, 598745, 598746,
--                                                               598747,
--                                                               598748, 598749, 598750, 598751, 598752, 598753, 598754,
--                                                               598755,
--                                                               598756, 598757, 598758, 598759, 598760, 598761, 598762,
--                                                               598763,
--                                                               598764, 598765, 598766, 598767, 598768, 598769, 598770,
--                                                               598771,
--                                                               598772, 598773, 598774, 612254, 612256, 736352, 930091,
--                                                               1018765,
--                                                               1076168, 1084971, 1098595, 1100473, 1174087, 1061036)
                               group by t1.conversiondatedefaulttimezone::date,
                                        t1.winnereventdatedefaulttimezone::date,
-- c1.campaignname,
-- t1.campaignid,
                                        c2.conversionname,
                                        t1.conversiontagid) t2
                      group by t2.con_date,
                               t2.conversionname,
                               t2.conversiontagid,
                               t2.con_window
                  ) t3
             group by t3.con_date,
                      t3.con_window,
                      date_flag
-- order by t3.con_date asc
         ) as t4
    group by t4.con_date,
             t4.con_window
) t5
group by
con_window
-- con_date


--  ======================================================================================================

select
count(*) from wmprodfeeds.ikea.sizmek_standard_events
where eventdatedefaulttimezone::date between '2018-03-01' and '2018-08-31' and
campaignid = 813500 -- Commercial
-- and eventtypeid <> 3
and eventtypeid = 1
--  ================================================
select
count( distinct userid) from wmprodfeeds.ikea.sizmek_standard_events
where eventdatedefaulttimezone::date between '2018-03-01' and '2018-08-31' and
campaignid = 813500 -- Commercial
-- and eventtypeid <> 3
and eventtypeid = 1
--  ================================================
select
count( distinct userid) from wmprodfeeds.ikea.sizmek_conversion_events
where conversiondatedefaulttimezone::date between '2018-03-01' and '2018-08-31' and
campaignid = 813500 -- Commercial
-- and eventtypeid <> 3
and isconversion = true

-- and conversiontagid in (593715, 596537, 598734, 598735, 598736, 598737, 598738,
-- 598739,
-- 598740, 598741, 598742, 598743, 598744, 598745, 598746,
-- 598747,
-- 598748, 598749, 598750, 598751, 598752, 598753, 598754,
-- 598755,
-- 598756, 598757, 598758, 598759, 598760, 598761, 598762,
-- 598763,
-- 598764, 598765, 598766, 598767, 598768, 598769, 598770,
-- 598771,
-- 598772, 598773, 598774, 612254, 612256, 736352, 930091,
-- 1018765,
-- 1076168, 1084971, 1098595, 1100473, 1174087, 1061036)
--  ================================================
select
eventdatedefaulttimezone::date as event_date,
count( distinct userid) from wmprodfeeds.ikea.sizmek_standard_events
where eventdatedefaulttimezone::date between '2018-03-01' and '2018-08-31' and
campaignid = 813500 -- Commercial
-- and eventtypeid <> 3
and eventtypeid = 1
group by eventdatedefaulttimezone::date
order by eventdatedefaulttimezone::date asc