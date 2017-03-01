-- Search SMR Automation 3
-- Can't do routes in THIS query - it messes everything up
-- select



-- from (

    select
--         cast(std1.date as date),
--         cast(dateadd(week,datediff(week,0,cast(std1.date as date)),0) as date) as "week",
        cast(std1.week as date) as week1,
        std1.pdsearch_engineaccount_id,
        e1.Paid_SearchEngine,
        std1.placement_id,
        c1.paid_search_campaign,
        std1.pdsearch_campaign_id,
        std1.pdsearch_keyword_id,
-- 	k2.paid_search_ad_group,
        ad1.Paid_Search_AdGroup,
--     k2.paid_search_keyword,
        k1.keyword,
        std1.pdsearch_ad_id,
        std1.pdsearch_adgroup_id,
        std1.pdsearch_matchtype,
        sum(std1.pdsearch_impressions)                                         as imps,
        sum(std1.pdsearch_clicks)                                              as clicks,
        avg(std1.pdsearch_avg_position)                                        as avg_pos,
        sum(cast(std1.pdsearch_actions as int))                                as actions,
        sum(std1.pdsearch_page_visits)                                         as visits,
        sum(fld2.total_revenue)                                                as rev,
        sum(cast(fld2.transaction_count as int))                               as con,
        sum(fld2.number_of_tickets)                                            as tix


    from [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_search_standard as std1

        left join [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_dim_paid_searchcampaign as c1
            on std1.pdsearch_campaign_id = c1.paid_search_campaign_id

-- 			    LEFT JOIN master.dbo.sch_keyword AS k2
-- 				on cast(std1.pdsearch_keyword_id as bigint) = k2.paid_search_keyword_id

        left join master.dbo.sch_keyword2 as k1
-- 				on cast(std1.pdsearch_ad_id as int) = k1.ad_id
            on cast(std1.pdsearch_keyword_id as bigint) = k1.keyword_id

        left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_SearchAdGroup as ad1
            on std1.pdsearch_adgroup_id = ad1.Paid_Search_AdGroup_ID

        left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchEngine as e1
            on std1.pdsearch_engineaccount_id = e1.Paid_SearchEngine_ID


        left join
        (select
             cast(fld1.date as date)     as "date",
--          cast(dateadd(week,datediff(week,0,cast(fld1.Activity_Date as date)),0) as date) as "week",
             fld1.pdsearch_engineaccount_id,
             fld1.pdsearch_ad_id,
             fld1.pdsearch_adgroup_id,
             fld1.placement_id,
             fld1.pdsearch_campaign_id,
             fld1.pdsearch_keyword_id,
             sum(fld1.total_conversions) as total_conversions,
             sum(fld1.total_revenue)     as total_revenue,
             sum(fld1.transaction_count) as transaction_count,
             count(*)                    as this_count,
             sum(number_of_tickets)      as number_of_tickets

         from (select
--                cast(fld0.date as date)                       as "date",
                   cast(fld0.activity_date as date)              as "date",
--          cast(dateadd(week,datediff(week,0,cast(fld0.Activity_Date as date)),0) as date) as "week",
                   fld0.pdsearch_engineaccount_id,
                   fld0.pdsearch_ad_id,
                   fld0.pdsearch_adgroup_id,
                   fld0.placement_id,
                   fld0.pdsearch_campaign_id,
                   fld0.pdsearch_keyword_id,
                   fld0.currency,
                   sum(fld0.total_conversions)                   as total_conversions,
                   sum(fld0.total_revenue / rates.exchange_rate) as total_revenue,
                   sum(fld0.transaction_count)                   as transaction_count,
                   count(*)                                      as this_count,
                   sum(number_of_tickets)                        as number_of_tickets

               from [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_search_floodlight as fld0

                   left join openQuery(verticaunited,
                                       'select * from diap01.mec_us_mecexchangerates_20067.EXCHANGE_RATES') as rates
                       on fld0.currency = rates.currency
                       and cast(fld0.date as date) = cast(rates.date as date)


               where cast(fld0.date as date) between '2017-01-01' and '2017-01-15'
--       and fld0.pdsearch_engineaccount_id is not null
                   and fld0.pdsearch_engineaccount_id != '0'
                   and fld0.currency != 'Miles'
                   and fld0.currency is not null
                   and fld0.currency != '--'
               group by
--          cast(fld0.date as date),
                   cast(fld0.activity_date as date),
--          cast(dateadd(week,datediff(week,0,cast(fld0.Activity_Date as date)),0) as date),
                   fld0.pdsearch_engineaccount_id,
                   fld0.placement_id,
                   fld0.currency,
                   fld0.pdsearch_ad_id,
                   fld0.pdsearch_adgroup_id,
                   fld0.pdsearch_keyword_id,
                   fld0.pdsearch_campaign_id
              ) as fld1
         group by
             cast(fld1.date as date),
--          cast(dateadd(week,datediff(week,0,cast(fld1.Activity_Date as date)),0) as date),
             fld1.pdsearch_engineaccount_id,
             fld1.placement_id,
             fld1.pdsearch_ad_id,
             fld1.pdsearch_adgroup_id,
             fld1.pdsearch_keyword_id,
             fld1.pdsearch_campaign_id
        ) as fld2
            on std1.pdsearch_campaign_id = fld2.pdsearch_campaign_id
            and std1.placement_id = fld2.placement_id
            and cast(std1.date as date) = cast(fld2.date as date)
            and std1.pdsearch_engineaccount_id = fld2.pdsearch_engineaccount_id
            and std1.pdsearch_keyword_id = fld2.pdsearch_keyword_id
            and std1.pdsearch_ad_id = fld2.pdsearch_ad_id
            and std1.pdsearch_adgroup_id = fld2.pdsearch_adgroup_id

    where cast(std1.date as date) between '2017-01-01' and '2017-01-15'
--    and c1.paid_search_campaign='new york - newark - remarketing'
--       and std1.pdsearch_engineaccount_id is not null
        and std1.pdsearch_engineaccount_id != '0'
        and std1.pdsearch_advertiser_id != '0'
--    and std1.pdsearch_campaign_id != '0'
--    and fld2.pdsearch_campaign_id != '0'
        and fld2.pdsearch_engineaccount_id != '0'


    group by
--         cast(std1.date as date),
--         cast(dateadd(week,datediff(week,0,cast(std1.date as date)),0) as date),
        cast(std1.week as date),
        std1.pdsearch_matchtype,
        std1.pdsearch_engineaccount_id,
        std1.placement_id,
        c1.paid_search_campaign,
        e1.Paid_SearchEngine,
-- 	k1.paid_search_keyword,
--     k2.paid_search_ad_group,
        ad1.Paid_Search_AdGroup,
--     k2.paid_search_keyword,
        k1.keyword,
        std1.pdsearch_campaign_id,
        std1.pdsearch_keyword_id,
        std1.pdsearch_ad_id,
        std1.pdsearch_adgroup_id
-- ) as t1

