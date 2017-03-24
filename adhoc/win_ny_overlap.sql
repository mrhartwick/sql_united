(
        select
--      sum(a.imp1) as imp1,
--      sum(b.imp2) as imp2

        count(distinct a.imp_nbr1) as imp1,
        count(distinct b.imp_nbr2) as imp2
        from
            (select
--              count(*) as imp1,
                 user_id,
                event_time,
                 row_number() over() as imp_nbr1
             from
                 diap01.mec_us_united_20056.dfa2_impression

             where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-03-22'
--               Win NY 2017
                 and (campaign_id = 10918234)
--              Uber, NYTimes, NY Magazine
--              and site_id_dcm in (3247316, 3246841, 1267159)
                 and site_id_dcm =
                 1485655 -- Forbes
--               3247316 -- Uber
--               3246841 -- NYTimes
--               1267159 -- NY Magazine
                 and user_id <> '0'
                 and (advertiser_id <> 0)
-- group by user_id
            ) as a,
            (select
--              count(*) as imp2,
                 user_id,
                event_time,
                 row_number() over() as imp_nbr2
             from
                 diap01.mec_us_united_20056.dfa2_impression

             where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-03-22'
--               Win NY 2017
                 and (campaign_id = 10918234)
--               Ninth Decimal, TapAd, Verve
                and site_id_dcm in (1329066, 2854118, 1995643)
                 and user_id <> '0'
                 and (advertiser_id <> 0)
--              group by user_id
            ) as b
        where
            a.user_id = b.user_id

);