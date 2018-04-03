select
        wk_time,
        t1.site    as site,
        count(distinct user_id) as user_id,
        count(imp_nbr) as imps

from (
        select
                user_id,
                date_part('week', cast (timestamp_trunc(to_timestamp(t2.event_time / 1000000), 'SS') as date)) as wk_time,
                case when p1.placement like '%PROS_FT%' and t2.site_id_dcm = 1239319 then 'Sojern-PROS'
                when p1.placement like '%PROS_FT%' and t2.site_id_dcm = 1578478 then 'DBM-PROS'
                when not p1.placement like '%PROS_FT%' and t2.site_id_dcm = 1578478 then 'DBM-RTG'
                when not p1.placement like '%PROS_FT%' and t2.site_id_dcm = 1239319 then 'Sojern-RTG'
                else s1.site_dcm end                                                                          as site,
                t2.site_id_dcm,
                p1.placement,
                t2.placement_id,
                t2.event_time as impressiontime,
                row_number() over () as imp_nbr
        from
                diap01.mec_us_united_20056.dfa2_impression as t2

        left join
                diap01.mec_us_united_20056.dfa2_sites as s1
                on t2.site_id_dcm = s1.site_id_dcm

        left join
                diap01.mec_us_united_20056.dfa2_placements as p1
                on t2.placement_id = p1.placement_id


        where
                t2.user_id != '0'
                and t2.campaign_id = 10742878
                and cast (timestamp_trunc(to_timestamp(t2.event_time / 1000000), 'SS') as date) between '2017-01-01' and '2017-12-31'
                and t2.advertiser_id <> 0

        ) as t1

group by
        wk_time,
        site
