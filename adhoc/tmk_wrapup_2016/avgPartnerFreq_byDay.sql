
-- Average Impressions Per User (Per Day)


select

--             t3.wk_time,
             t3.site_dcm,
             t3.site_id,
--              t3.user_id,
--              row_number() over (partition by t3.user_id order by t3.user_id) as user_cnt,
             avg(per_user) as avg_per,
--             avg( avg_per_wk) as avg_per2,
--             avg(t3.per_user) over (partition by t3.wk_time ) as avg_per_wk,
--              t3.user_cnt3,
             sum(t3.per_user) as imp

from (
         select
--              t2.day_time,
             t2.wk_time,
             t2.site_dcm,
             t2.site_id,
             t2.user_id,
--              t2.avg_per_user,
             t2.per_user,
             case when t2.per_user = 0 then 0 else avg(t2.per_user) over (partition by t2.wk_time ) end as avg_per_wk,
--              row_number() over (partition by t2.user_id order by t2.user_id) as user_cnt,
             t2.user_cnt3
--              t2.imp


         from (
                  select
--                       t1.day_time                                                      as day_time,
--                       t1.month_time                                                      as "date",
--                           t1.qrt_time                                                   as "date",
                      t1.wk_time                                                   as wk_time,
                      case when t1.site_id = 1578478 then 'Google'  else s1.site_dcm end                as site_dcm,
                      t1.site_id                                                                       as site_id,
                      t1.user_id                                                                       as user_id,
                      sum(case when t1.user_cnt3 = 1 then t1.user_cnt3 else 0 end)                     as user_cnt3,
--                       avg(t1.imp) over (partition by t1.user_id,t1.wk_time order by t1.wk_time) as avg_per_user,
                      sum(t1.imp) over (partition by t1.user_id, t1.wk_time, t1.site_id order by t1.wk_time) as per_user,
--                       t1.user_cnt3                    as user_cnt3,
                        sum(t1.imp) as imp


                  from
                      (
                          select
                              cast(impression_time as date) as day_time,
                             date_part('month', cast(impression_time as date)) as month_time,
                              date_part('quarter', cast(impression_time as date)) as qrt_time,
                            date_part('week', cast(impression_time as date)) as wk_time,
                              count(*) as imp,
                              row_number() over (partition by user_id,date_part('week', cast(impression_time as date)) order by date_part('week', cast(impression_time as date))) as user_cnt3,
                              user_id,
                              case when site_id = 2331004 then 2988923 else site_id end as site_id
                          from diap01.mec_us_united_20056.dfa_impression as a

                          where cast(impression_time as date) between '2016-01-01' and '2016-12-31'
--                               where cast(impression_time as date) between '2016-06-01' and '2016-06-30'
                            and order_id = 9639387
--                             and site_id = 1578478
                            and advertiser_id <> 0
                            and user_id <> '0'

                          group by
                              cast(impression_time as date),
                          user_id,
                              site_id

                      ) as t1

--                       left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
--                           on upper(substring(other_data,(instr(other_data,'u3=') + 3),3)) = upper(rates.currency)
--                         and cast(impression_time as date) = rates.date
                      left join
                      (
                          select
                              cast(site as varchar(4000)) as 'site_dcm',
                              site_id as 'site_id_dcm'
                          from diap01.mec_us_united_20056.dfa_site
                      ) as s1
                          on t1.site_id = s1.site_id_dcm

                  group by
--                       day_time,
--                      t1.month_time,
--                       t1.qrt_time,
                      t1.wk_time,
                      t1.imp,
                  t1.user_id,
                  t1.user_cnt3,
                  s1.site_dcm,
                  t1.site_id
                  order by user_id
              ) as t2
where t2.user_cnt3 = 1


     ) as t3
group by
--                 t3.wk_time,
             t3.site_dcm,
             t3.site_id