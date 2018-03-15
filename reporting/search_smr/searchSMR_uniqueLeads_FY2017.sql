select
--     ta.user_id,
--  ta.date,
    month(ta.date) as month,
--     ta.week as week,
--     ta.advertiser,
--  ta.campaign,
ta.type,
    ta.site_dcm,
--     ta.site_id_dcm    as siteid_dcm,

--     ta.paid_search_keyword,
--     ta.segment_value_1 as keyword_id,
--     ta.keyword_id as keyword_id,
--     ta.ad_id,
    ta.paid_search_campaign,
--     ta.paid_search_ad_group,
--  ta.paid_search_bid_strategy,
--  sum(thing)        as thing,
    count(distinct ta.user_id)/sum(leads) as rate,
    count(distinct ta.user_id) as u_leads,
--     count(r1) as r1,
--  count(r2)         as r2,
--     sum(tot_con)      as tot_con,
    sum(leads) as leads
--     sum(ta.r1) as row_count

from
    (
        select
--             a.*,
            a.user_id,
            a.segment_value_1,
            a.ad_id,
            a.site_id_dcm,
            c1.campaign,
            case when regexp_like(c1.campaign, '.*Dart.*' ,'ib') then 'Search'
            when c1.campaign = 'UAC-TMK-018_2018 Targeted Marketing Search' then 'Meta' else 'n/a' end as type,
            d1.site_dcm,
            a1.advertiser,
            cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date) as date,
            date_trunc('week', timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS')) -1 as week,
            row_number() over ()                                                         as r2,
            1                                                                            as thing,
            count(*)                                                                     as leads,
            sum(total_conversions)                                                       as tot_con,
            s1.paid_search_keyword_id                                                    as keyword_id,
            s1.paid_search_keyword,
--          s1.paid_search_bid_strategy,
            s1.paid_search_campaign,
            s1.paid_search_ad_group
        from diap01.mec_us_united_20056.dfa2_activity as a

            left join diap01.mec_us_united_20056.ual_dfa2_paid_search_meta_u as s1
            on a.segment_value_1 = s1.paid_search_legacy_keyword_id
                and a.ad_id = s1.ad_id

            left join diap01.mec_us_united_20056.dfa2_sites as d1
            on a.site_id_dcm = d1.site_id_dcm

            left join diap01.mec_us_united_20056.dfa2_advertisers as a1
            on a.advertiser_id = a1.advertiser_id

            left join diap01.mec_us_united_20056.dfa2_campaigns as c1
            on a.campaign_id = c1.campaign_id

-- left join diap01.mec_us_united_20056.dfa2_campaigns as c1
-- on a.campaign_id = c1.campaign_id

        where cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-12-31'
            and a.activity_id = 1086066
            and a.advertiser_id <> 0
            and (a1.advertiser = 'United_Search' or a1.advertiser = 'United')
            and (length(isnull (a.event_sub_type,'')) > 0)
            and a.user_id <> '0'
--          and a.campaign_id = 8063775
        group by
            a.user_id,
            a.segment_value_1,
            a.ad_id,
            cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date),
            date_trunc('week', timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS')),
--  date_trunc('week', timestamp_trunc(to_timestamp(event_time / 1000000),'SS')),
            s1.paid_search_keyword_id,
            a.site_id_dcm,
            s1.paid_search_campaign,
            c1.campaign,
            s1.paid_search_keyword,
            d1.site_dcm,
--          s1.paid_search_bid_strategy,
            a1.advertiser,
            s1.paid_search_ad_group

    ) as ta
--     where ta.segment_value_1 != 0
--     and (length(isnull(cast(ta.segment_value_1 as varchar),'')) > 0)
-- where regexp_like(ta.paid_search_campaign, '.*Fares.*' ,'ib')
-- where ta.advertiser = 'United_Search'
group by
--  ta.date,
--     ta.paid_search_keyword,
    ta.paid_search_campaign,
--     ta.week,
--     ta.keyword_id,
    ta.type,
    month(ta.date),
--     ta.segment_value_1,
--  ta.campaign,
--     ta.site_id_dcm,
--     ta.advertiser,
    ta.site_dcm