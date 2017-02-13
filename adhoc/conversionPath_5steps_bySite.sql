-- Pathing, Five steps, by Site

select
    t11.path,
    sum(t11.path_rank)

from (
         select
             t10.user_id,
             t10.path,
             row_number() over (partition by t10.path,t10.user_id order by t10.path) as path_rank

         from
             (select
                  t9.user_id,
                  t9.path_1 || '->' || t9.path_2 || '->' || t9.path_3 || '->' || t9.path_4 || '->' || t9.path_5 as path

              from (select
                        t8.user_id,
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
                        when t8.path_3 = 7 then 'Xaxis' else 'xxx' end as path_3,

                        case when t8.path_4 = 1 then 'Adara'
                        when t8.path_4 = 2 then 'Amobee'
                        when t8.path_4 = 3 then 'Google'
                        when t8.path_4 = 4 then 'Internet Brands'
                        when t8.path_4 = 5 then 'Travel Spike'
                        when t8.path_4 = 6 then 'Viant'
                        when t8.path_4 = 7 then 'Xaxis' else 'xxx' end as path_4,

                        case when t8.path_5 = 1 then 'Adara'
                        when t8.path_5 = 2 then 'Amobee'
                        when t8.path_5 = 3 then 'Google'
                        when t8.path_5 = 4 then 'Internet Brands'
                        when t8.path_5 = 5 then 'Travel Spike'
                        when t8.path_5 = 6 then 'Viant'
                        when t8.path_5 = 7 then 'Xaxis' else 'xxx' end as path_5

                    from (

                             select
                                 t7.user_id,
                                 sum(t7.path_1) as path_1,
                                 sum(t7.path_2) as path_2,
                                 sum(t7.path_3) as path_3,
                                 sum(t7.path_4) as path_4,
                                 sum(t7.path_5) as path_5

                             from (
                                      select
                                          t6.user_id,

                                          case when t6.path_1 = 'Adara' then 1
                                          when t6.path_1 = 'Amobee' then 2
                                          when t6.path_1 = 'Google' then 3
                                          when t6.path_1 = 'Internet Brands' then 4
                                          when t6.path_1 = 'Travel Spike' then 5
                                          when t6.path_1 = 'Viant' then 6
                                          when t6.path_1 = 'Xaxis' then 7 else 0 end as path_1,

                                          case when t6.path_2 = 'Adara' then 1
                                          when t6.path_2 = 'Amobee' then 2
                                          when t6.path_2 = 'Google' then 3
                                          when t6.path_2 = 'Internet Brands' then 4
                                          when t6.path_2 = 'Travel Spike' then 5
                                          when t6.path_2 = 'Viant' then 6
                                          when t6.path_2 = 'Xaxis' then 7 else 0 end as path_2,

                                          case when t6.path_3 = 'Adara' then 1
                                          when t6.path_3 = 'Amobee' then 2
                                          when t6.path_3 = 'Google' then 3
                                          when t6.path_3 = 'Internet Brands' then 4
                                          when t6.path_3 = 'Travel Spike' then 5
                                          when t6.path_3 = 'Viant' then 6
                                          when t6.path_3 = 'Xaxis' then 7 else 0 end as path_3,

                                          case when t6.path_4 = 'Adara' then 1
                                          when t6.path_4 = 'Amobee' then 2
                                          when t6.path_4 = 'Google' then 3
                                          when t6.path_4 = 'Internet Brands' then 4
                                          when t6.path_4 = 'Travel Spike' then 5
                                          when t6.path_4 = 'Viant' then 6
                                          when t6.path_4 = 'Xaxis' then 7 else 0 end as path_4,

                                          case when t6.path_5 = 'Adara' then 1
                                          when t6.path_5 = 'Amobee' then 2
                                          when t6.path_5 = 'Google' then 3
                                          when t6.path_5 = 'Internet Brands' then 4
                                          when t6.path_5 = 'Travel Spike' then 5
                                          when t6.path_5 = 'Viant' then 6
                                          when t6.path_5 = 'Xaxis' then 7 else 0 end as path_5

                                      from (
                                               select

                                                   t5.site_dcm,
                                                   t5.user_id,
                                                   t5.tp_rank,

                                                   case when t5.tp_rank = 5 then t5.site_dcm else 'zzz' end as path_1,
                                                   case when t5.tp_rank = 4 then t5.site_dcm else 'zzz' end as path_2,
                                                   case when t5.tp_rank = 3 then t5.site_dcm else 'zzz' end as path_3,
                                                   case when t5.tp_rank = 2 then t5.site_dcm else 'zzz' end as path_4,
                                                   case when t5.tp_rank = 1 then t5.site_dcm else 'zzz' end as path_5

                                               from (
                                                        select
                                                            t4.date,
                                                            t4.site_dcm,
                                                            t4.user_id,
                                                            t4.tp_rank,
                                                            t4.imps,
                                                            t4.clks


                                                        from (
                                                                 select
                                                                     t3.date              as "date",
                                                                     case
                                                                     when t3.site_id = 1578478 then 'Google'
                                                                     when t3.site_id = 1190273 then 'Adara'
                                                                     when t3.site_id = 1592652 then 'Xaxis'
                                                                     when t3.site_id = 2988923 then 'Viant'
                                                                     else j1.site_dcm end as site_dcm,
                                                                     t3.user_id           as user_id,

                                                                     row_number()            over (partition by t3.user_id order by t3.date desc) as tp_rank,
                                                                     sum(t3.impressions)  as imps,
                                                                     sum(t3.clicks)       as clks

                                                                 from
                                                                     (
                                                                         select
                                                                             cast(impression_time as date) as "date",
                                                                             count(*)                      as impressions,
                                                                             0                             as clicks,
                                                                             t1.user_id                    as user_id,
                                                                             t1.site_id                    as site_id,
                                                                             t1.order_id                   as order_id,
                                                                             t1.page_id                    as page_id

                                                                         from (select *
                                                                               from diap01.mec_us_united_20056.dfa_impression

                                                                               where cast(impression_time as date) between '2016-01-01' and '2016-12-31'
                                                                                   and order_id = 9639387
                                                                                   and advertiser_id <> 0
                                                                                   and user_id <> '0'
                                                                              ) as t1

                                                                         group by
                                                                             cast (t1.impression_time as date ),
                                                                         t1.user_id,
                                                                         t1.page_id,
                                                                         t1.order_id,
                                                                           t1.site_id

                                                                           union all

                                                                           select
                                                                           cast (t2.click_time as date ) as "date",
                                                                                                         0 as impressions,
                                                                                                         count (*) as clicks,
                                                                                                                   t2.user_id as user_id,
                                                                                                                   t2.site_id as site_id,
                                                                                                                     t2.order_id as order_id,
                                                                         t2.page_id as page_id
                                                                         from ( select *
                                                                                from diap01.mec_us_united_20056.dfa_click

                                                                                where cast (click_time as date ) between '2016-01-01' and '2016-12-31'
                                                                                                                 and order_id = 9639387
                                                                                                                 and advertiser_id <> 0
                                                                                                                                   and user_id <> '0'
                                                                              ) as t2

                                                                                group by
                                                                                cast (t2.click_time as date ),
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

                                                        where t4.tp_rank < 6

                                                    ) as t5
                                               group by
                                                   t5.user_id,
                                                   t5.site_dcm,
                                                   t5.tp_rank

                                           ) as t6
                                  ) as t7
--  where t7.user_id = 'AMsySZbAqdsOvZSqwZf-POJxzM2g'
                             group by
                                 t7.user_id

                         ) as t8
                   ) as t9
             ) as t10
     ) as t11
group by t11.path