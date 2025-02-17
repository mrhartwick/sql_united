-- Insert latest Search metadata into Vertica table
copy diap01.mec_us_united_20056.ual_dfa2_paid_search_meta from local 'C:\Users\matthew.hartwick\Documents\paid_search_match_20171001-20171031.csv' with DELIMITER ',' DIRECT commit;

-- drop existing uniques table
drop table diap01.mec_us_united_20056.ual_dfa2_paid_search_meta_u;

-- create new uniques table
create table diap01.mec_us_united_20056.ual_dfa2_paid_search_meta_u (
    ad_id                         int,
    advertiser_id                 int,
    campaign_id                   int,
    paid_search_ad_id             int,
    paid_search_legacy_keyword_id int,
    paid_search_keyword_id        int,
    paid_search_campaign          varchar(6000),
    paid_search_ad_group          varchar(6000),
    paid_search_bid_strategy      varchar(6000),
    paid_search_landing_page_url  varchar(6000),
    paid_search_keyword           varchar(6000),
    paid_search_match_type        varchar(6000)
);


insert into diap01.mec_us_united_20056.ual_dfa2_paid_search_meta_u
(ad_id,advertiser_id,campaign_id,paid_search_ad_id,paid_search_legacy_keyword_id,paid_search_keyword_id,
paid_search_campaign,paid_search_ad_group,paid_search_bid_strategy,paid_search_landing_page_url,paid_search_keyword,paid_search_match_type)
    (
        select
            distinct
            ad_id,
            advertiser_id,
            campaign_id,
            paid_search_ad_id,
            paid_search_legacy_keyword_id,
            paid_search_keyword_id,
            paid_search_campaign,
            paid_search_ad_group,
            paid_search_bid_strategy,
            paid_search_landing_page_url,
            paid_search_keyword,
            paid_search_match_type
        from diap01.mec_us_united_20056.ual_dfa2_paid_search_meta
    );
commit;


-- pull unique leads at desired level
select
--     ta.user_id,
--  ta.date,
    ta.week as week,
    ta.advertiser,
--  ta.campaign,
    ta.site_dcm,
    ta.site_id_dcm    as siteid_dcm,
--     ta.paid_search_keyword,
--     ta.keyword_id as keyword_id,
--     ta.ad_id,
    ta.paid_search_campaign,
    ta.paid_search_ad_group,
--  ta.paid_search_bid_strategy,
--  sum(thing)        as thing,
    count(ta.user_id) as u_leads,
--     count(r1) as r1,
--  count(r2)         as r2,
    sum(tot_con)      as tot_con
--     sum(ta.r1) as row_count

from
    (
        select
--             a.*,
            a.user_id,
            a.segment_value_1,
            a.ad_id,
            a.site_id_dcm,
--          c1.campaign,
            d1.site_dcm,
            a1.advertiser,
            cast(timestamp_trunc(to_timestamp(a.interaction_time / 1000000),'SS') as date) as date,
            date_trunc('week', timestamp_trunc(to_timestamp(a.interaction_time / 1000000),'SS')) -1 as week,
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

-- left join diap01.mec_us_united_20056.dfa2_campaigns as c1
-- on a.campaign_id = c1.campaign_id

        where cast(timestamp_trunc(to_timestamp(a.interaction_time / 1000000),'SS') as date) between '2017-10-01' and '2017-10-31'
            and (a.activity_id = 1086066)
            and (a.advertiser_id <> 0)
            and (length(isnull (a.event_sub_type,'')) > 0)
--          and a.campaign_id = 8063775
        group by
            a.user_id,
            a.segment_value_1,
            a.ad_id,
            cast(timestamp_trunc(to_timestamp(a.interaction_time / 1000000),'SS') as date),
            date_trunc('week', timestamp_trunc(to_timestamp(a.interaction_time / 1000000),'SS')),
--  date_trunc('week', timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS')),
            s1.paid_search_keyword_id,
            a.site_id_dcm,
            s1.paid_search_campaign,
--   c1.campaign,
            s1.paid_search_keyword,
            d1.site_dcm,
--          s1.paid_search_bid_strategy,
            a1.advertiser,
            s1.paid_search_ad_group

    ) as ta
-- where ta.r1 = 1

group by
--  ta.date,
    ta.week,
--  ta.campaign,
    ta.site_id_dcm,
    ta.advertiser,
    ta.site_dcm,
    ta.paid_search_ad_group,
--  ta.paid_search_bid_strategy,
--     ta.paid_search_keyword,
--     ta.keyword_id,
--     ta.ad_id
    ta.paid_search_campaign


