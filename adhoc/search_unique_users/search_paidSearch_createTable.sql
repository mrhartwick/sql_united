create table diap01.mec_us_united_20056.ual_dfa2_paid_search_meta (
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

-- combo file of all match_table_paid_search files since 20170526
copy diap01.mec_us_united_20056.ual_dfa2_paid_search_meta from local 'C:\Users\matthew.hartwick\Documents\paid_search_match_20170526-20170727.csv' with DELIMITER ',' DIRECT commit;

-- De-duped version
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


insert /*+ direct */ into diap01.mec_us_united_20056.ual_dfa2_paid_search_meta_u
(ad_id,advertiser_id,campaign_id,paid_search_ad_id,paid_search_legacy_keyword_id,paid_search_keyword_id,paid_search_campaign,paid_search_ad_group,paid_search_bid_strategy,paid_search_landing_page_url,paid_search_keyword,paid_search_match_type)
    (
--  explain
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





