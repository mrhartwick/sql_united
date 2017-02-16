select
  t11.path,
  sum(t11.path_rank),
  sum(t11.con) as con,
  sum(t11.tix) as tix,
  sum(t11.rev) as rev,
  sum(t11.imp) as imp

from (
       select
         t10.user_id,
         t10.path,
         t10.con,
         t10.tix,
         t10.rev,
          t10.imp,
         row_number() over (partition by t10.user_id,t10.path order by t10.user_id) as path_rank
       from (
              select
                t91.user_id,
                t91.path,
                sum(j4.con) as con,
                sum(j4.tix) as tix,
                sum(j4.rev) as rev,
                 sum(t91.imp) as imp

              from
                (select
                   t9.user_id,
                  t9.imp,
--                   t9.path_1 || '->' || t9.path_2 || '->' || t9.path_3 || '->' || t9.path_4 || '->' || t9.path_5 as path
                t9.path_1 || '->' || t9.path_2 || '->' || t9.path_3  as path

                 from (select
                         t8.user_id,
                   t8.imp,
                         case when t8.path_1 = 1 then 'Adara'
                         when t8.path_1 = 2 then 'Amobee'
                         when t8.path_1 = 3 then 'Google'
                         when t8.path_1 = 4 then 'Internet Brands'
                         when t8.path_1 = 5 then 'Travel Spike'
                         when t8.path_1 = 6 then 'Viant'
                         when t8.path_1 = 7 then 'Xaxis' else 'xxx' end as path_1,

                         case when t8.path_2 = 1 then 'Adara'
                         when t8.path_2 = 2 then 'Amobee'
                         when t8.path_2 = 3 then 'Google'
                         when t8.path_2 = 4 then 'Internet Brands'
                         when t8.path_2 = 5 then 'Travel Spike'
                         when t8.path_2 = 6 then 'Viant'
                         when t8.path_2 = 7 then 'Xaxis' else 'xxx' end as path_2,

                         case when t8.path_3 = 1 then 'Adara'
                         when t8.path_3 = 2 then 'Amobee'
                         when t8.path_3 = 3 then 'Google'
                         when t8.path_3 = 4 then 'Internet Brands'
                         when t8.path_3 = 5 then 'Travel Spike'
                         when t8.path_3 = 6 then 'Viant'
                         when t8.path_3 = 7 then 'Xaxis' else 'xxx' end as path_3
--
--                         case when t8.path_4 = 1 then 'Adara'
--                         when t8.path_4 = 2 then 'Amobee'
--                         when t8.path_4 = 3 then 'Google'
--                         when t8.path_4 = 4 then 'Internet Brands'
--                         when t8.path_4 = 5 then 'Travel Spike'
--                         when t8.path_4 = 6 then 'Viant'
--                         when t8.path_4 = 7 then 'Xaxis' else 'xxx' end as path_4,
--
--                         case when t8.path_5 = 1 then 'Adara'
--                         when t8.path_5 = 2 then 'Amobee'
--                         when t8.path_5 = 3 then 'Google'
--                         when t8.path_5 = 4 then 'Internet Brands'
--                         when t8.path_5 = 5 then 'Travel Spike'
--                         when t8.path_5 = 6 then 'Viant'
--                         when t8.path_5 = 7 then 'Xaxis' else 'xxx' end as path_5

                       from (

                              select
                                t7.user_id,
                                sum(t7.path_1) as path_1,
                                sum(t7.path_2) as path_2,
                                sum(t7.path_3) as path_3,
--                                sum(t7.path_4) as path_4,
--                                sum(t7.path_5) as path_5,
                                 sum(t7.imp) as imp

                              from (
                                     select
                                       t6.user_id,
                                       t6.imp,
                                       case when regexp_like(t6.path_1,'adar.*','ib') then 1
                                       when regexp_like(t6.path_1,'amobee.*','ib') then 2
                                       when regexp_like(t6.path_1,'google.*','ib') then 3
                                       when regexp_like(t6.path_1,'internet.*','ib') then 4
                                       when regexp_like(t6.path_1,'travel.*','ib') then 5
                                       when regexp_like(t6.path_1,'viant.*','ib') then 6
                                       when regexp_like(t6.path_1,'xaxis.*','ib') then 7
                                       else 0 end as path_1,

                                       case
                                       when regexp_like(t6.path_2,'adar.*','ib') then 1
                                       when regexp_like(t6.path_2,'amobee.*','ib') then 2
                                       when regexp_like(t6.path_2,'google.*','ib') then 3
                                       when regexp_like(t6.path_2,'internet.*','ib') then 4
                                       when regexp_like(t6.path_2,'travel.*','ib') then 5
                                       when regexp_like(t6.path_2,'viant.*','ib') then 6
                                       when regexp_like(t6.path_2,'xaxis.*','ib') then 7
                                       else 0 end as path_2,

                                       case
                                       when regexp_like(t6.path_3,'adar.*','ib') then 1
                                       when regexp_like(t6.path_3,'amobee.*','ib') then 2
                                       when regexp_like(t6.path_3,'google.*','ib') then 3
                                       when regexp_like(t6.path_3,'internet.*','ib') then 4
                                       when regexp_like(t6.path_3,'travel.*','ib') then 5
                                       when regexp_like(t6.path_3,'viant.*','ib') then 6
                                       when regexp_like(t6.path_3,'xaxis.*','ib') then 7
                                       else 0 end as path_3

--                                       case
--                                       when regexp_like(t6.path_4,'adar.*','ib') then 1
--                                       when regexp_like(t6.path_4,'amobee.*','ib') then 2
--                                       when regexp_like(t6.path_4,'google.*','ib') then 3
--                                       when regexp_like(t6.path_4,'internet.*','ib') then 4
--                                       when regexp_like(t6.path_4,'travel.*','ib') then 5
--                                       when regexp_like(t6.path_4,'viant.*','ib') then 6
--                                       when regexp_like(t6.path_4,'xaxis.*','ib') then 7
--                                       else 0 end as path_4,
--
--                                       case
--                                       when regexp_like(t6.path_5,'adar.*','ib') then 1
--                                       when regexp_like(t6.path_5,'amobee.*','ib') then 2
--                                       when regexp_like(t6.path_5,'google.*','ib') then 3
--                                       when regexp_like(t6.path_5,'internet.*','ib') then 4
--                                       when regexp_like(t6.path_5,'travel.*','ib') then 5
--                                       when regexp_like(t6.path_5,'viant.*','ib') then 6
--                                       when regexp_like(t6.path_5,'xaxis.*','ib') then 7
--                                       else 0 end as path_5
                                     --                                 case when t6.path_1 = 'Adara' then 1
                                     --                                 when t6.path_1 = 'Amobee' then 2
                                     --                                 when t6.path_1 = 'Google' then 3
                                     --                                 when t6.path_1 = 'Internet Brands' then 4
                                     --                                 when t6.path_1 = 'Travel Spike' then 5
                                     --                                 when t6.path_1 = 'Viant' then 6
                                     --                                 when t6.path_1 = 'Xaxis' then 7 else 0 end as path_1,
                                     --
                                     --                                 case when t6.path_2 = 'Adara' then 1
                                     --                                 when t6.path_2 = 'Amobee' then 2
                                     --                                 when t6.path_2 = 'Google' then 3
                                     --                                 when t6.path_2 = 'Internet Brands' then 4
                                     --                                 when t6.path_2 = 'Travel Spike' then 5
                                     --                                 when t6.path_2 = 'Viant' then 6
                                     --                                 when t6.path_2 = 'Xaxis' then 7 else 0 end as path_2,
                                     --
                                     --                                 case when t6.path_3 = 'Adara' then 1
                                     --                                 when t6.path_3 = 'Amobee' then 2
                                     --                                 when t6.path_3 = 'Google' then 3
                                     --                                 when t6.path_3 = 'Internet Brands' then 4
                                     --                                 when t6.path_3 = 'Travel Spike' then 5
                                     --                                 when t6.path_3 = 'Viant' then 6
                                     --                                 when t6.path_3 = 'Xaxis' then 7 else 0 end as path_3,
                                     --
                                     --                                 case when t6.path_4 = 'Adara' then 1
                                     --                                 when t6.path_4 = 'Amobee' then 2
                                     --                                 when t6.path_4 = 'Google' then 3
                                     --                                 when t6.path_4 = 'Internet Brands' then 4
                                     --                                 when t6.path_4 = 'Travel Spike' then 5
                                     --                                 when t6.path_4 = 'Viant' then 6
                                     --                                 when t6.path_4 = 'Xaxis' then 7 else 0 end as path_4,
                                     --
                                     --                                 case when t6.path_5 = 'Adara' then 1
                                     --                                 when t6.path_5 = 'Amobee' then 2
                                     --                                 when t6.path_5 = 'Google' then 3
                                     --                                 when t6.path_5 = 'Internet Brands' then 4
                                     --                                 when t6.path_5 = 'Travel Spike' then 5
                                     --                                 when t6.path_5 = 'Viant' then 6
                                     --                                 when t6.path_5 = 'Xaxis' then 7 else 0 end as path_5

                                     from (
                                            select

--                                       t5.site_dcm,
                                              t5.user_id,
                                              t5.tp_rank,
                              t5.imp,
                              case when t5.tp_rank = 3 then t5.site_dcm else 'zzz' end as path_1,
                              case when t5.tp_rank = 2 then t5.site_dcm else 'zzz' end as path_2,
                              case when t5.tp_rank = 1 then t5.site_dcm else 'zzz' end as path_3
--                              case when t5.tp_rank = 2 then t5.site_dcm else 'zzz' end as path_4,
--                              case when t5.tp_rank = 1 then t5.site_dcm else 'zzz' end as path_5

                                            from (
                                                   select

                                                     t4.date,
                                                     t4.site_dcm,
                                                     t4.user_id,
                                                     t4.tp_rank,
                                                     sum(t4.imp) as imp,
                                                     sum(t4.clk) as clk


                                                   from (
                                                          select
                                                            t3.date              as "date",
                                                            case
                                                            when t3.site_id = 1578478 then 'Google'
                                                            when t3.site_id = 1190273 then 'Adara'
                                                            when t3.site_id = 1592652 then 'Xaxis'
                                                            when t3.site_id = 2988923 then 'Viant'
                                                            when t3.site_id = 2331004 then 'Viant'
                                                            else j1.site_dcm end as site_dcm,
--                                                     j1.site_dcm  as site_dcm,
                                                            t3.user_id           as user_id,

                                                            row_number()            over (partition by t3.user_id order by t3.date desc) as tp_rank,
                                                            sum(t3.impressions)  as imp,
                                                            sum(t3.clicks)       as clk

                                                          from
                                                            (
                                                              select
--                                                         cast(impression_time as date) as "date",
                                                                impression_time as "date",
                                                                count(*)        as impressions,
                                                                0               as clicks,
                                                                t1.user_id      as user_id,
                                                                t1.site_id      as site_id,
                                                                t1.order_id     as order_id,
                                                                t1.page_id      as page_id

                                                              from (select *
                                                                    from diap01.mec_us_united_20056.dfa_impression

                                                                    where cast(impression_time as date) between '2016-01-01' and '2016-12-31'
                                                                      and order_id = 9639387
                                                                      and advertiser_id <> 0
                                                                      and user_id <> '0'
                                                                   ) as t1

                                                              group by
--                                                         cast (t1.impression_time as date ),
                                                                t1.impression_time,
                                                                t1.user_id,
                                                                t1.page_id,
                                                                t1.order_id,
                                                                t1.site_id

                                                              union all

                                                              select
--                                                         cast (t2.click_time as date ) as "date",
                                                                t2.click_time as "date",
                                                                0             as impressions,
                                                                count(*)      as clicks,
                                                                t2.user_id    as user_id,
                                                                t2.site_id    as site_id,
                                                                t2.order_id   as order_id,
                                                                t2.page_id    as page_id
                                                              from (select *
                                                                    from diap01.mec_us_united_20056.dfa_click
                                                                    where cast(click_time as date) between '2016-01-01' and '2016-12-31'
                                                                      and order_id = 9639387
                                                                      and advertiser_id <> 0
                                                                      and user_id <> '0'
                                                                   ) as t2

                                                              group by
--                                                              cast (t2.click_time as date ),
                                                                t2.click_time,
                                                                t2.user_id,
                                                                t2.page_id,
                                                                t2.order_id,
                                                                t2.site_id

                                                            ) as t3

                                                            left join
                                                            (
                                                              select
                                                                cast(site as varchar(4000)) as 'site_dcm',
                                                                site_id as 'site_id_dcm'
                                                              from diap01.mec_us_united_20056.dfa_site
                                                            ) as j1
                                                              on t3.site_id = j1.site_id_dcm

                                                            left join
                                                            (
                                                              select
                                                                cast(t1.placement as varchar(4000)) as placement,
                                                                t1.page_id                          as page_id,
                                                                t1.order_id                         as order_id,
                                                                t1.site_id                          as site_id

                                                              from (select
                                                                      order_id                 as order_id,
                                                                      site_id                  as site_id,
                                                                      page_id                  as page_id,
                                                                      site_placement           as placement,
                                                                      cast(start_date as date) as thisdate,
                                                                      row_number()                over (partition by order_id,site_id,page_id order by cast (start_date as date ) desc) as r1
                                                                    from diap01.mec_us_united_20056.dfa_page
                                                                   ) as t1
                                                              where r1 = 1
                                                            ) as j2
                                                              on t3.page_id = j2.page_id
                                                              and t3.order_id = j2.order_id
                                                              and t3.site_id = j2.site_id

                                                          where t3.user_id in (
                                                            select user_id
                                                            from diap01.mec_us_united_20056.dfa_activity as t4
                                                            where
                                                              cast(activity_time as date) between '2016-01-01' and '2016-12-31'
                                                                and advertiser_id <> 0
                                                                and revenue <> 0
                                                                and quantity <> 0
                                                                and (activity_type = 'ticke498')
                                                                and (activity_sub_type = 'unite820')
                                                                and advertiser_id <> 0
                                                                and order_id = 9639387
                                                                and user_id <> '0'
                                                                and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
                                                                and t3.user_id = t4.user_id
                                                                and datediff('dd',cast(t3.date as date),cast(t4.activity_time as date)) < 8

                                                          )
                                                          group by
                                                            t3.date,
                                                            t3.site_id,
                                                            j1.site_dcm,
                                                            t3.user_id
                                                        ) as t4

--                                            where t4.tp_rank <= 4
                                                   group by
                                                     t4.date,
                                                     t4.site_dcm,
                                                     t4.user_id,
                                                     t4.tp_rank
                                                 ) as t5
--                                     group by
--                                       t5.user_id,
--                                       t5.site_dcm,
--                                       t5.tp_rank

                                          ) as t6
                                   ) as t7
--  where t7.user_id = 'AMsySZbAqdsOvZSqwZf-POJxzM2g'
                              group by
                                t7.user_id

                            ) as t8
                      ) as t9
                ) as t91
                left join (select
                             user_id                               as user_id,
                             count(*)                              as con,
                             sum(a1.quantity)                      as tix,
                             sum(a1.revenue / rates.exchange_rate) as rev

                           from
                             (
                               select *
                               from diap01.mec_us_united_20056.dfa_activity
                               where (cast(click_time as date) between '2016-01-01' and '2016-12-31')
                                 and advertiser_id <> 0
                                 and revenue <> 0
                                 and quantity <> 0
                                 and (activity_type = 'ticke498')
                                 and (activity_sub_type = 'unite820')
                                 and advertiser_id <> 0
                                 and order_id = 9639387
                                 and user_id <> '0'
                                 and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
                             ) as a1

                             left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
                               on upper(substring(other_data,(instr(other_data,'u3=') + 3),3)) = upper(rates.currency)
                               and cast(a1.click_time as date) = rates.date

                           group by

                             a1.user_id

                          ) as j4
                  on t91.user_id = j4.user_id

              group by
                t91.user_id,
                t91.path
            ) as t10
     ) as t11
group by t11.path