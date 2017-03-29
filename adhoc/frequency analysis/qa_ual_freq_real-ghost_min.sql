/*
Check for dupe conversions
 */

select
    a.user_id,
--          conversiontime,
--          revenue,
--  real_tim,
    timestamp_trunc(real_tim,'MI') as real_tim,
    sum(real_imp) as real_imp,
--  sum(real_bid_cst) as real_bid_cst,
    sum(real_cst) as real_cst,
--  sum(real_con) as real_con,
--  sum(real_rev) as real_rev,
--  ghst_tim,
    timestamp_trunc(ghst_tim,'MI') as ghst_tim,
    sum(ghst_imp) as ghst_imp,
--  sum(ghst_bid_cst) as ghst_bid_cst,
    sum(ghst_cst) as ghst_cst
--  sum(ghst_con) as ghst_con,
--  sum(ghst_rev) as ghst_rev
--          impressiontime,
--          cvr_nbr,
--          imp_nbr
from
    (select
         user_id,
--               timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as impressiontime,
--       cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as real_tim,
--              timestamp_trunc(to_timestamp(event_time / 1000000),'HH') as real_tim,
--              timestamp_trunc(to_timestamp(event_time / 1000000),'MI') as real_tim,
--              timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as real_tim,
        to_timestamp(event_time / 1000000) as real_tim,
         0                                                                      as real_con,
         0                                                                      as real_rev,
         count(*)                                                               as real_imp,
--       sum(cast((dbm_bid_price_usd / 1000000000) as decimal(20,10)))         as real_bid_cst,
         sum(cast((dbm_total_media_cost_usd / 1000000000) as decimal(20,10)))         as real_cst
     from
         diap01.mec_us_united_20056.dfa2_impression

             where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
--   where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-11-28' and '2016-11-28'
         and (site_id_dcm = 1578478)
            and campaign_id = 9639387


         and user_id <> '0'
         and (advertiser_id <> 0)
     group by
         user_id,
--                  timestamp_trunc(to_timestamp(event_time / 1000000),'HH')
--              timestamp_trunc(to_timestamp(event_time / 1000000),'MI')
--              timestamp_trunc(to_timestamp(event_time / 1000000),'SS')
        to_timestamp(event_time / 1000000)
--       to_timestamp(dbm_request_time / 1000000)
--       cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date)
    ) as a,
    (select
         user_id,
--               timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as impressiontime,
        to_timestamp(event_time / 1000000) as ghst_tim,
--       cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as ghst_tim,
--              timestamp_trunc(to_timestamp(event_time / 1000000),'HH') as ghst_tim,
--              timestamp_trunc(to_timestamp(event_time / 1000000),'MI') as ghst_tim,
--              timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as ghst_tim,
         0                                                                      as ghst_con,
         0                                                                      as ghst_rev,
         count(*)                                                               as ghst_imp,
--      sum(cast((dbm_bid_price_usd / 1000000000) as decimal(20,10)))         as ghst_bid_cst,
         sum(cast((dbm_total_media_cost_usd / 1000000000) as decimal(20,10)))         as ghst_cst
     from
         diap01.mec_us_united_20056.dfa2_impression

             where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
--   where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-11-28' and '2016-11-28'
--               and (campaign_id = 9639387 or campaign_id = 8958859)
         and (site_id_dcm = 2202011)
                 and campaign_id = 8958859
         and user_id <> '0'
         and (advertiser_id <> 0)
     group by
         user_id,
--                  timestamp_trunc(to_timestamp(event_time / 1000000),'HH')
--              timestamp_trunc(to_timestamp(event_time / 1000000),'MI')
--              timestamp_trunc(to_timestamp(event_time / 1000000),'SS')
        to_timestamp(event_time / 1000000)
--       to_timestamp(dbm_request_time / 1000000)
--       cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date)
    ) as b
where
    a.user_id = b.user_id
    and
        abs(datediff(mi, real_tim, ghst_tim)) <= 1
        and
        real_cst <> 0
        and
        ghst_cst <> 0
--              and
--              conversiontime > impressiontime
group by
    a.user_id,
--  real_tim,
--  ghst_tim,
    timestamp_trunc(real_tim,'MI'),
    timestamp_trunc(ghst_tim,'MI')
--  real_req_tim,
--  ghst_req_tim