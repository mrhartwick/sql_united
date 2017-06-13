select
    distinct count(t001.user_id) as unique_reach,
    c1.campaign as campaign,
    s1.site_dcm as site


from diap01.mec_us_united_20056.dfa2_impression as t001


   LEFT outer JOIN
   diap01.mec_us_united_20056.dfa2_campaigns  AS c1
   ON t001.campaign_id = c1.campaign_id

   LEFT outer JOIN
   diap01.mec_us_united_20056.dfa2_sites AS s1
   ON t001.site_id_dcm = s1.site_id_dcm



where
         user_id <> '0'
        and t001.campaign_id in (10918234)
        and t001.advertiser_id <> '0'
        and cast(timestamp_trunc(to_timestamp(t001.event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-06-06'

group by
    c1.campaign,
    s1.site_dcm
