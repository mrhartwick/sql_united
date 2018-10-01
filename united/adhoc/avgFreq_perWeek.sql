select

  t3.wk_time,
  t3.site_dcm,
  count(distinct t3.user_id) as users,
  avg(per_user)              as avg_per,
  sum(t3.per_user)           as imp

from (
       select
         t2.wk_time,
         t2.site_dcm,
         count(distinct t2.user_id) as users,
         avg(t2.imp)                as avg_per_wk,
         sum(imp)                   as imp
       from (
              select
                t1.wk_time  as wk_time,
                site_dcm,
                t1.user_id  as user_id,
                sum(t1.imp) as imp

              from
                (
                  select
                    cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date)                   as day_time,
                    date_part('week',cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date)) as wk_time,
                    count(*)                                                                                   as imp,
                    user_id,
                    site_id_dcm

                  from diap01.mec_us_united_20056.dfa2_impression as a
                  where cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-09-30'
                    and (site_id_dcm = 1190273 or site_id_dcm = 1239319 or site_id_dcm = 3267410 or site_id_dcm = 1853562)
                    and advertiser_id <> 0
                    and user_id <> '0'
                  group by
                    cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date),
                    user_id,
                    site_id_dcm

                ) as t1


                left join diap01.mec_us_united_20056.dfa2_sites as s1
                on t1.site_id_dcm = s1.site_id_dcm
              group by
                t1.wk_time,
                t1.user_id,
                s1.site_dcm

            ) as t2
       group by
         t2.wk_time,
         t2.site_dcm

     ) as t3
group by
  t3.wk_time,
  t3.site_dcm



