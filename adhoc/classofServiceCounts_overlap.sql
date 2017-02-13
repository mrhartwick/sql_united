
-- Retargeting/Prospecting Users and Tickets

select
--     t3.date,
  t3.class,
--  sum( t3.user_cnt) as user_ids,
--     t3.site_dcm,
--     t3.site_id,
--     t3.placement,
--     t3.page_id,
--     t3.class1,
--     t3.class2,
--     sum(t3.user_cnt) as user_cnt,
  sum(t3.user_cnt1)  as user_cnt1,
    sum(t3.user_cnt3)  as user_cnt2,
    sum(t3.clck_con)   as clck_con,
    sum(t3.view_con)   as view_con,
    sum(t3.con)        as con,
    sum(t3.clck_tix)   as clck_tix,
    sum(t3.view_tix)   as view_tix,
    sum(t3.tix)        as tix,
    sum(t3.clck_rev)   as clck_rev,
    sum(t3.view_rev)   as view_rev,
    sum(t3.rev)        as rev

from (
         select
--              t2.date,
--              t2.placement,
--              t2.page_id,
--              t2.site_dcm,
--              t2.site_id,
--              t2.user_id,
           t2.class,
--              t2.class1,
--              t2.class2,
--              row_number() over (partition by t2.class, t2.user_id order by t2.user_id) as user_cnt,
         sum(t2.user_cnt1) as user_cnt1,
             sum(t2.user_cnt3) as user_cnt3,
    sum(t2.clck_con)   as clck_con,
    sum(t2.view_con)   as view_con,
    sum(t2.con)        as con,
    sum(t2.clck_tix)   as clck_tix,
    sum(t2.view_tix)   as view_tix,
    sum(t2.tix)        as tix,
    sum(t2.clck_rev)   as clck_rev,
    sum(t2.view_rev)   as view_rev,
    sum(t2.rev)        as rev
         from (
                  select
--                      t1.click_time                                                   as "date",
--                       t1.month_time                                                      as "date",
--                           t1.qrt_time                                                   as "date",
--                       case when t1.site_id = 1578478 then 'Google' else j1.site_dcm end                as site_dcm,
--                       t1.site_id                                                                       as site_id,
                      t1.user_id                                                                       as user_id,
                      t1.class as class,
                  row_number() over (partition by  t1.user_id  order by  t1.user_id ) as user_cnt1,
--                       t1.class1                                                                        as class1,
--                       t1.class2                                                                        as class2,
--                       j2.placement                                                                     as placement,
--                  sum(case when t1.user_cnt3 = 1 then t1.user_cnt3 else 0 end) over (partition by t1.user_id order by t1.class) as user_cnt,
--                       t1.page_id                                                                       as page_id,
--                       sum(case when t1.user_cnt3 = 1 then t1.user_cnt3 else 0 end)                     as user_cnt3,
                  case when t1.user_cnt3 = 1 then t1.user_cnt3 else 0 end                     as user_cnt3,
                      sum(case when event_id = 1 then 1 else 0 end)                                    as clck_con,
                      sum(case when event_id = 2 then 1 else 0 end)                                    as view_con,
                      sum(case when event_id = 2 or event_id = 1 then 1 else 0 end)                    as con,
                      sum(case when event_id = 1 then t1.quantity else 0 end)                          as clck_tix,
                      sum(case when event_id = 2 then t1.quantity else 0 end)                          as view_tix,
                      sum(t1.quantity)                                                                 as tix,
                      sum(case when event_id = 1 then (t1.revenue) / (rates.exchange_rate) else 0 end) as clck_rev,
                      sum(case when event_id = 2 then (t1.revenue) / (rates.exchange_rate) else 0 end) as view_rev,
                      sum(t1.revenue / rates.exchange_rate)                                            as rev

                  from
                      (
                          select
                              cast(click_time as date) as click_time,
                              row_number() over (partition by user_id order by user_id) as user_cnt3,
                              case
                                  when lower(substring(other_data,(instr(other_data,'u11=') + 4),5)) = 'busin' or lower(substring(other_data,(instr(other_data,'u12=') + 4),5)) = 'busin' then 'business'
                                  when lower(substring(other_data,(instr(other_data,'u11=') + 4),5)) = 'first' or lower(substring(other_data,(instr(other_data,'u12=') + 4),5)) = 'first' then 'first'
                                  when lower(substring(other_data,(instr(other_data,'u11=') + 4),5)) = 'coach' and lower(substring(other_data,(instr(other_data,'u12=') + 4),5)) = 'coach' then 'coach'
                                  when (lower(substring(other_data,(instr(other_data,'u11=') + 4),5)) = 'coach' and (lower(substring(other_data,(instr(other_data,'u12=') + 4),5)) <> 'first' and lower(substring(other_data,(instr(other_data,'u12=') + 4),5)) <> 'busin' )) then 'coach'
                              else 'other' end as class,
                              page_id ,
                              event_id,
                              order_id,
                              quantity,
                              other_data,
                              revenue,
                              user_id,
                              site_id
                          from diap01.mec_us_united_20056.dfa_activity as a
        where
            a.user_id in (
              select user_id
              from diap01.mec_us_united_20056.dfa_activity as t99
              where
                  cast(click_time as date) between '2016-01-01' and '2016-12-31'
                    and advertiser_id <> 0
--  Google
                    and site_id <> 1578478
                    and revenue <> 0
                    and quantity <> 0
                    and (activity_type = 'ticke498')
                    and (activity_sub_type = 'unite820')
                    and advertiser_id <> 0
                    and order_id = 9639387
                    and user_id <> '0'
                    and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
                    and a.user_id = t99.user_id
                                    and (
--                       Retargeting conversion within 7 days of prospecting conversion
                      datediff('dd', cast(a.click_time as date), cast(t99.click_time as date)) < 8

                  )

            )

                            and cast(click_time as date) between '2016-01-01' and '2016-12-31'
                            and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
--                             and site_id = 1578478
                            and site_id = 1578478
                            and revenue <> 0
                            and quantity <> 0
                            and activity_type = 'ticke498'
                            and activity_sub_type = 'unite820'
                            and order_id = 9639387
                            and advertiser_id <> 0
                            and user_id <> '0'
                      ) as t1

                      left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
                          on upper(substring(other_data,(instr(other_data,'u3=') + 3),3)) = upper(rates.currency)
                        and cast(click_time as date) = rates.date


                      left join
                      (
                          select
                              cast(site as varchar(4000)) as site_dcm,
                              site_id as site_id_dcm
                          from diap01.mec_us_united_20056.dfa_site
                      ) as j1
                          on t1.site_id = j1.site_id_dcm
--
-- left join
-- (
-- select cast(t1.placement as varchar(4000)) as placement,  t1.page_id as page_id, t1.order_id as order_id, t1.site_id as site_id
--
-- from (select
--           order_id as order_id,
--           site_id as site_id,
--           page_id as page_id,
--           site_placement as placement,
--           cast(start_date as date) as thisdate,
--         row_number() over (partition by order_id, site_id, page_id  order by cast(start_date as date) desc) as r1
--     from diap01.mec_us_united_20056.dfa_page
--
-- ) as t1
-- where r1 = 1
-- ) as j2

-- on  t1.page_id  = j2.page_id
-- and t1.order_id = j2.order_id
-- and t1.site_id  = j2.site_id

                  group by
--                     t1.click_time,
                    t1.class,
--                     t1.class1,
--                     t1.class2,
--                      t1.month_time,
--                       t1.qrt_time,
                  t1.user_id,
                  t1.user_cnt3
--                         j2.placement,
--                       t1.page_id,
--                   j1.site_dcm,
--                   t1.site_id
--                   order by t1.class
              ) as t2
group by t2.class
     ) as t3

group by
  t3.class
--     t3.date,
--     t3.class1,
--     t3.class2,
--     t3.site_dcm,
--     t3.site_id
--     t3.placement,
--     t3.page_id
-- ,Conversions.page_id

