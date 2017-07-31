-- Query to find unique users (who searched on united.com), by Search campaign

select
    ta.paid_search_campaign,
    count(ta.user_id)
from
    (
        select
            a.*,
            row_number() over ( partition by a.user_id,s1.paid_search_campaign
                order by a.event_time ) as r1,
            s1.paid_search_campaign
        from diap01.mec_us_united_20056.dfa2_activity as a

            left join diap01.mec_us_united_20056.ual_dfa2_paid_search_meta_u as s1
            on a.segment_value_1 = s1.paid_search_legacy_keyword_id
                and a.ad_id = s1.ad_id
        where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2017-06-08' and '2017-07-10'

            and (a.activity_id = 1086066)
            and (a.advertiser_id <> 0)
            and (length(isnull(a.event_sub_type,'')) > 0)
            and a.campaign_id = 8063775
            and s1.paid_search_campaign in ('Houston Texas - Search','Houston Texas - Search_URL Test','San Francisco California - Search','San Francisco California - Search_URL Test','New York - Newark - Search','New York - Newark - Search_URL Test')
    ) as ta
where ta.r1 = 1

group by
    ta.paid_search_campaign


