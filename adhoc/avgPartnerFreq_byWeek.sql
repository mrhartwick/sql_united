
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
--         where
--             a.user_id in (
--               select  user_id
-- --               ,cast(impression_time as date)
--               from diap01.mec_us_united_20056.dfa_impression as t99
--               where
--                   cast(impression_time as date) between '2016-01-01' and '2016-12-31'
--                     and advertiser_id <> 0
-- --  Google
--                     and site_id <> 1578478
--                     and advertiser_id <> 0
--                     and order_id = 9639387
--                     and user_id <> '0'
--                     and a.user_id = t99.user_id
--                     and (
-- --                       Retargeting Impression within 7 days of prospecting Impression
-- --                       datediff('dd', cast(a.impression_time as date), cast(t99.impression_time as date)) < 8
-- --                       or
-- --                       prospecting Impression within 7 days of Retargeting Impression
--                       datediff('dd', cast(t99.impression_time as date), cast(a.impression_time as date)) < 8
--                   )
-- --                 and cast(a.impression_time as date) = cast(t99.impression_time as date)
-- --                     and date_part('month', cast(a.impression_time as date)) = date_part('month', cast(t99.impression_time as date))
-- --                 and date_part('quarter', cast(a.impression_time as date)) = date_part('quarter', cast(t99.impression_time as date))
--             )

                          where cast(impression_time as date) between '2016-01-01' and '2016-12-31'
--                               where cast(impression_time as date) between '2016-06-01' and '2016-06-30'
                            and order_id = 9639387
--                             and site_id = 1578478
                            and advertiser_id <> 0
                            and user_id <> '0'
--                               and (user_id in ('AMsySZY--0SB6f1Zm2Nk7KKNiVsd',
-- 'AMsySZY--0nN09zuQz3sSyFsNtJ3',
-- 'AMsySZY--1A7k-cvjkwcvnfgjCm6',
-- 'AMsySZY--3gxZ686adiy6KHgowSy',
-- 'AMsySZY--8Obd3tOmGk9TGC25a6V',
-- 'AMsySZY--8Obd3tOmGk9TGC25a6V',
-- 'AMsySZY--B0JF4pmfCzvF62cxS8A',
-- 'AMsySZY--C8BXbS_RY-E29gYyxJo',
-- 'AMsySZY--D6KvA4T6SekznEqGaNC',
-- 'AMsySZY--GC1RoKDfY49RYzu-kKS',
-- 'AMsySZY--GC1RoKDfY49RYzu-kKS',
-- 'AMsySZY--HZScUJUUnnAxMoYUqrA',
-- 'AMsySZY--JlSlGq9CtdMcRHtKnEi',
-- 'AMsySZY--Ms609QosQi02UrzzWO9',
-- 'AMsySZY--NyAV-U15ttTK_jQqssv',
-- 'AMsySZY--Ozf079GjWJMmYNuL5Xf',
-- 'AMsySZY--QzeSFC9bhV2kfdbpx9Y',
-- 'AMsySZY--SSlYKb23uNlYVmKuphe',
-- 'AMsySZY--SSlYKb23uNlYVmKuphe',
-- 'AMsySZY--SSlYKb23uNlYVmKuphe',
-- 'AMsySZY--SSlYKb23uNlYVmKuphe',
-- 'AMsySZY--T7BtQlMgWrnqO1gkZgh',
-- 'AMsySZY--UE52JTxBwYrIytE-c82',
-- 'AMsySZY--VPV14RJX7QpL_eV6H89',
-- 'AMsySZY--W0F4CscbJS8KS0ddjig',
-- 'AMsySZY--W2XAWXRYLML1kuy2TjJ',
-- 'AMsySZY--XsivB6q_T-E1kHsMo-M',
-- 'AMsySZY--Z1FtMnP61a3y_z7F5we',
-- 'AMsySZY--_xfYxcRqH5aWgQWWid7',
-- 'AMsySZY--bchl7GKhjGbLuXahVuM',
-- 'AMsySZY--iUcjaIdMI11aaCG53fe',
-- 'AMsySZY--iUcjaIdMI11aaCG53fe',
-- 'AMsySZY--knSW64d4XQgHJ7zZLw3',
-- 'AMsySZY--rTJGfJgXKh-ns4Pm-qs',
-- 'AMsySZY--r_FUrtrFntPOxtF9Gw_',
-- 'AMsySZY--uGGW67TZLiW18alpQVx',
-- 'AMsySZY--wtUYuMPmUJ1octO3B8D',
-- 'AMsySZY--wtUYuMPmUJ1octO3B8D',
-- 'AMsySZY--yvudfSjQkNdFSln0Dgd',
-- 'AMsySZY--zXse7-gXtM-Th-lp-FJ',
-- 'AMsySZY-0-6vG5gRTYXsqzQ4Hlxg',
-- 'AMsySZY-00Cphy3jGqZXD-1gg93s',
-- 'AMsySZY-00Cphy3jGqZXD-1gg93s',
-- 'AMsySZY-00ESE5zG10JgBcNfKW7x',
-- 'AMsySZY-00ESE5zG10JgBcNfKW7x',
-- 'AMsySZY-00ESE5zG10JgBcNfKW7x',
-- 'AMsySZY-02sTeBgi9uj41ao9N7YO',
-- 'AMsySZY-05YEMK59wv-zEIvgxHJY',
-- 'AMsySZY-06ow4IOKALwIaXCmAdQk',
-- 'AMsySZY-08Qot5neb6JIzZnjwHbk',
-- 'AMsySZY-0Hke0X4wUSLpWRXHW1Uv',
-- 'AMsySZY-0Hke0X4wUSLpWRXHW1Uv',
-- 'AMsySZY-0Hke0X4wUSLpWRXHW1Uv',
-- 'AMsySZY-0J2WPFP5CaeblRr9kdg6',
-- 'AMsySZY-0KDxlBm6kIhy4ZY1SeYs',
-- 'AMsySZY-0LipOFD4wyacuv_i0qXo',
-- 'AMsySZY-0MXD5DrMSMNh5wV2-f5n',
-- 'AMsySZY-0NrmljLgPSMm5TlDoRPI',
-- 'AMsySZY-0RasNMyZlqDWaBuc25r7',
-- 'AMsySZY-0SX9fMaNOMBENML25hUm',
-- 'AMsySZY-0VEUMOhNE0EVzDGeMMD_',
-- 'AMsySZY-0VXChhhj4sb2H8E7_hug',
-- 'AMsySZY-0W9kV2iOTivaTSmJ5c8W',
-- 'AMsySZY-0XF13lmpI4WpKBoRKmQG',
-- 'AMsySZY-0XUGZjRfjtJx0lJvPQyi',
-- 'AMsySZY-0X_Nk_LgfhKXjLGpgVvH',
-- 'AMsySZY-0ceuRTe9p0ZXkAH0WhLB',
-- 'AMsySZY-0g9CtjSLbUXkbROU7wwL',
-- 'AMsySZY-0g9CtjSLbUXkbROU7wwL',
-- 'AMsySZY-0g9CtjSLbUXkbROU7wwL',
-- 'AMsySZY-0icz7MJ2Zz3-9lpAG9xs',
-- 'AMsySZY-0jC-8sC00-I_QdL2IvYS',
-- 'AMsySZY-0jlLOloJNyDhpYHSA1WM',
-- 'AMsySZY-0lVWCt4FhOXstOaR17E3',
-- 'AMsySZY-0mOApadiicjG03mH_wvb',
-- 'AMsySZY-0n7APUMcUxgv7Dh3Oqdz',
-- 'AMsySZY-0n7APUMcUxgv7Dh3Oqdz',
-- 'AMsySZY-0n7APUMcUxgv7Dh3Oqdz',
-- 'AMsySZY-0n7APUMcUxgv7Dh3Oqdz',
-- 'AMsySZY-0n8xqHWRq_CGFjpZ832y',
-- 'AMsySZY-0n8xqHWRq_CGFjpZ832y',
-- 'AMsySZY-0n8xqHWRq_CGFjpZ832y',
-- 'AMsySZY-0nAxcz5SUFO1yeOQRV_7',
-- 'AMsySZY-0p4v4Bt01bvNprP-lE_5',
-- 'AMsySZY-0pbO1doj378mEltM7VbN',
-- 'AMsySZY-0qSh_w9YcyVB7KumpjVG',
-- 'AMsySZY-0sJLWmCCVz1me18WuGuN',
-- 'AMsySZY-0sOKMyzLARBmxjNKS0ar',
-- 'AMsySZY-0ukNyhCxXDxfHYQxRkrD',
-- 'AMsySZY-0vAI55KCWmm__47kXTtU',
-- 'AMsySZY-0wexj1O9N6hX3GePYqoF',
-- 'AMsySZY-0yFwSzABNCJz29FheFeO',
-- 'AMsySZY-1-NZdihGN-QkNqsUeUVj',
-- 'AMsySZY-1-bzIv4S2dVBJ1I2Cg9G',
-- 'AMsySZY-1-bzIv4S2dVBJ1I2Cg9G',
-- 'AMsySZY-1-bzIv4S2dVBJ1I2Cg9G',
-- 'AMsySZY-1-bzIv4S2dVBJ1I2Cg9G',
-- 'AMsySZY-106VrDCuqhi_MRI8LdTI',
-- 'AMsySZY-107qZWg_r9yiQtdh3W38',
-- 'AMsySZY-11134yGXjl3yeLgZQkVl',
-- 'AMsySZY-15_8naMQLivq9iQ9tZH-',
-- 'AMsySZY-15xLlM5gZjmqQ0HsVuEo'))
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