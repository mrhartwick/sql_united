select
        wk_time,
        site_dcm,
        count(distinct user_id) as user_id,
        count(imp_nbr) as imps

from (
        select
                user_id,
                date_part('week', cast (timestamp_trunc(to_timestamp(event_time / 1000000), 'SS') as date)) as wk_time,
                s1.site_dcm,
                t2.site_id_dcm,
                event_time as impressiontime,
                row_number() over () as imp_nbr
        from
                diap01.mec_us_united_20056.dfa2_impression as t2

        left join
                diap01.mec_us_united_20056.dfa2_sites as s1
                on t2.site_id_dcm = s1.site_id_dcm

        where
                user_id != '0'
                and campaign_id = 10742878
                and cast (timestamp_trunc(to_timestamp(event_time / 1000000), 'SS') as date) between '2017-01-01' and '2017-12-31'
                and advertiser_id <> 0
        ) as t1

group by
        wk_time,
        site_dcm