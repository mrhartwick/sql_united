
-- Retargeting/Prospecting Users and Clicks

select
    t3.date,
    t3.site_dcm,
    t3.site_id,
--     count(t3.user_cnt) as user_cnt,
    sum(t3.user_cnt3)  as user_cnt3,
    sum(t3.clk)   as clk

from (
         select
             t2.date,
             t2.site_dcm,
             t2.site_id,
             t2.user_id,
--              row_number() over (partition by t2.user_id order by t2.site_id) as user_cnt,
             t2.user_cnt3,
             t2.clk

         from (
                  select
--                       t1.click_time                                                      as "date",
--                       t1.month_time                                                      as "date",
                          t1.qrt_time                                                   as "date",
--     t1.click_time                                                   as "date",
                      case when t1.site_id = 1578478 then 'Google' else s1.site_dcm end                as site_dcm,
                      t1.site_id                                                                       as site_id,
                      t1.user_id                                                                       as user_id,
                      sum(case when t1.user_cnt3 = 1 then t1.user_cnt3 else 0 end)                     as user_cnt3,
                      count(*)                                    as clk

                  from
                      (
                          select
                              cast(click_time as date) as click_time,
                             date_part('month', cast(click_time as date)) as month_time,
                              date_part('quarter', cast(click_time as date)) as qrt_time,
                              row_number()                over (partition by user_id order by user_id) as user_cnt3,
                              user_id,
                              site_id
                          from diap01.mec_us_united_20056.dfa_click as a
        where
            a.user_id in (
              select  user_id
--               ,cast(click_time as date)
              from diap01.mec_us_united_20056.dfa_click as t99
              where
                  cast(click_time as date) between '2016-01-01' and '2016-12-31'
                    and advertiser_id <> 0
--  Google
                    and site_id <> 1578478
                    and advertiser_id <> 0
                    and order_id = 9639387
                    and user_id <> '0'
                                    and (
--                       Retargeting conversion within 7 days of prospecting conversion
                      datediff('dd', cast(a.click_time as date), cast(t99.click_time as date)) < 8
--                       or
--                       prospecting conversion within 7 days of Retargeting conversion
--                       datediff('dd', cast(t99.click_time as date), cast(a.click_time as date)) < 8
                  )
--                       and cast(a.click_time as date) = cast(t99.click_time as date)
--                     and date_part('month', cast(a.click_time as date)) = date_part('month', cast(t99.click_time as date))
--                 and date_part('quarter', cast(a.click_time as date)) = date_part('quarter', cast(t99.click_time as date))
            )

                          and cast(click_time as date) between '2016-01-01' and '2016-12-31'
--                             and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
--                             and revenue <> 0
--                             and quantity <> 0
--                             and activity_type = 'ticke498'
--                             and activity_sub_type = 'unite820'
                            and order_id = 9639387
                            and site_id = 1578478
                            and advertiser_id <> 0
                            and user_id <> '0'
                      ) as t1

--                       left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
--                           on upper(substring(other_data,(instr(other_data,'u3=') + 3),3)) = upper(rates.currency)
--                         and cast(click_time as date) = rates.date
                      left join
                      (
                          select
                              cast(site as varchar(4000)) as 'site_dcm',
                              site_id as 'site_id_dcm'
                          from diap01.mec_us_united_20056.dfa_site
                      ) as s1
                          on t1.site_id = s1.site_id_dcm

                  group by
--                       click_time,
--                      t1.month_time,
                      t1.qrt_time,
                  t1.user_id,
                  t1.user_cnt3,
                  s1.site_dcm,
                  t1.site_id
                  order by user_id
              ) as t2

     ) as t3

group by
    t3.date,
    t3.site_dcm,
    t3.site_id
-- ,Conversions.page_id