-- Search SMR Automation 5
-- Can't do routes in THIS query - it messes everything up


-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_sch_adGroupTbl go
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_sch_keywordTbl go
-- exec master.dbo.crt_sch_keywordTbl go
-- exec master.dbo.crt_sch_keywordTbl2 go
-- exec master.dbo.crt_sch_keywordTbl3 go


declare @report_st date
declare @report_ed date
--
set @report_ed = '2017-02-28';
set @report_st = '2017-01-01';

select

-- cast(t1.date as date),
--         cast(dateadd(week,datediff(week,0,cast(t1.date as date)),0) as date) as "week",
  t1.week1                     as week1,
  t1.pdsearch_engineaccount_id,
  t1.Paid_SearchEngine,
  case
  when
    (t1.paid_search_campaign like '%[Rr]emarketing%' or t1.Paid_Search_AdGroup like '%[Rr]emarketing%') then 'Display'
  when (t1.paid_search_campaign like '%[Rr][Ll][Ss][Aa]%' or t1.Paid_Search_AdGroup like '%[Rr][Ll][Ss][Aa]%' or t1.Paid_SearchEngine like '%[Rr][Ll][Ss][Aa]%') then 'RLSA'
  when (t1.paid_search_campaign like '%[Ss]earch%' or t1.Paid_Search_AdGroup like '%[Ss]earch%') then 'Search'
  else 'Metasearch' end        as network,
  t1.placement_id,
--    t1.siteid_dcm,
--    t1.site_dcm,
  t1.pdsearch_campaign_id,
  t1.paid_search_campaign,
  t1.pdsearch_keyword_id,
  t1.keyword,
--  k2.paid_search_ad_group,
  t1.pdsearch_adgroup_id,
  t1.Paid_Search_AdGroup,
--     k2.paid_search_keyword,
  t1.pdsearch_ad_id,

  t1.pdsearch_matchtype,
  sum(t1.imps)                 as imps,
  sum(t1.clicks)               as clicks,
  avg(t1.avg_pos)              as avg_pos,
  sum(cast(t1.actions as int)) as actions,
  sum(t1.visits)               as visits,
  sum(t1.rev)                  as rev,
  sum(cast(t1.con as int))     as con,
  sum(t1.tix)                  as tix

from (select
        cast(std1.date as date)                                                as "Date",
        cast(dateadd(week,datediff(week,0,cast(std1.date as date)),0) as date) as "week1",
--         cast(std1.week as date) as week1,
        std1.pdsearch_engineaccount_id                                         as pdsearch_engineaccount_id,
        e1.Paid_SearchEngine                                                   as Paid_SearchEngine,
        std1.placement_id                                                      as placement_id,
--       std1.siteid_dcm as siteid_dcm,
--      s1.site_dcm as site_dcm,
        std1.pdsearch_campaign_id                                              as pdsearch_campaign_id,
        c1.paid_search_campaign                                                as paid_search_campaign,
        std1.pdsearch_keyword_id                                               as pdsearch_keyword_id,
        k1.paid_search_keyword                                                 as keyword,
--         k1.keyword as keyword,
--  k2.paid_search_ad_group,
        std1.pdsearch_adgroup_id                                               as pdsearch_adgroup_id,
        ad1.Paid_Search_AdGroup                                                as Paid_Search_AdGroup,
--     k2.paid_search_keyword,
        std1.pdsearch_ad_id                                                    as pdsearch_ad_id,

        std1.pdsearch_matchtype,
        std1.pdsearch_impressions                                              as imps,
        std1.pdsearch_clicks                                                   as clicks,
        std1.pdsearch_avg_position                                             as avg_pos,
        std1.pdsearch_actions                                                  as actions,
        std1.pdsearch_page_visits                                              as visits,
        fld2.total_revenue                                                     as rev,
        fld2.transaction_count                                                 as con,
        fld2.number_of_tickets                                                 as tix

      from [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_search_standard as std1

        left join [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_dim_paid_searchcampaign as c1
          on std1.pdsearch_campaign_id = c1.paid_search_campaign_id

        left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchKeyword as k1
          on std1.pdsearch_keyword_id = k1.Paid_Search_Keyword_ID

--                      left join master.dbo.sch_keyword3 as k1
--             on cast(std1.pdsearch_keyword_id as bigint) = k1.keyword_id

        left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_SearchAdGroup as ad1
          on std1.pdsearch_adgroup_id = ad1.Paid_Search_AdGroup_ID

        left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchEngine as e1
          on std1.pdsearch_engineaccount_id = e1.Paid_SearchEngine_ID

--      left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Site as s1
--             on std1.siteid_dcm = s1.site_id_dcm

        left join
        (select
           cast(fld1.date as date)     as "date",
--          cast(dateadd(week,datediff(week,0,cast(fld1.Activity_Date as date)),0) as date) as "week",
           fld1.pdsearch_engineaccount_id,
           fld1.pdsearch_ad_id,
           fld1.pdsearch_adgroup_id,
           fld1.placement_id,
--          fld1.siteid_dcm,
           fld1.pdsearch_campaign_id,
           fld1.pdsearch_keyword_id,
           sum(fld1.total_revenue)     as total_revenue,
           sum(fld1.transaction_count) as transaction_count,
           sum(number_of_tickets)      as number_of_tickets

         from (select
--                cast(fld0.date as date)                       as "date",
                 cast(fld0.activity_date as date)              as "date",
--          cast(dateadd(week,datediff(week,0,cast(fld0.Activity_Date as date)),0) as date) as "week",
                 fld0.pdsearch_engineaccount_id,
                 fld0.pdsearch_ad_id,
                 fld0.pdsearch_adgroup_id,
                 fld0.placement_id,
--                 fld0.siteid_dcm,
                 fld0.pdsearch_campaign_id,
                 fld0.pdsearch_keyword_id,
                 fld0.currency,
                 sum(fld0.total_revenue / rates.exchange_rate) as total_revenue,
                 sum(fld0.transaction_count)                   as transaction_count,
                 count(*)                                      as this_count,
                 sum(number_of_tickets)                        as number_of_tickets

               from [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_search_floodlight as fld0

                 left join openQuery(verticaunited,
                                     'select currency, date, exchange_rate from diap01.mec_us_mecexchangerates_20067.EXCHANGE_RATES
        ') as rates
                   on fld0.currency = rates.currency
                   and cast(fld0.activity_date as date) = cast(rates.date as date)

               where cast(fld0.activity_date as date) between @report_st and @report_ed
                 and (LEN(ISNULL(fld0.pdsearch_engineaccount_id,'')) > 0)
                 and (LEN(ISNULL(fld0.pdsearch_adgroup_id,'')) > 0)
                 and (LEN(ISNULL(fld0.pdsearch_campaign_id,'')) > 0)
                 and (LEN(ISNULL(fld0.pdsearch_keyword_id,'')) > 0)
                 and fld0.PdSearch_Advertiser_ID <> '0'
                 and fld0.currency <> 'Miles'
                 and (LEN(ISNULL(fld0.currency,'')) > 0)
                 and PdSearch_EngineAccount like '%TMK%'
                 and fld0.currency <> '--'
                 and fld0.activity_id = 978826
               group by
--          cast(fld0.date as date),
                 cast(fld0.activity_date as date),
--          cast(dateadd(week,datediff(week,0,cast(fld0.Activity_Date as date)),0) as date),
                 fld0.pdsearch_engineaccount_id,
                 fld0.placement_id,
--                 fld0.siteid_dcm,
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
--           fld1.siteid_dcm,
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
--      and std1.siteid_dcm = fld2.siteid_dcm

where cast(std1.date as date) between @report_st and @report_ed
  and (LEN(ISNULL(std1.pdsearch_keyword_id,'')) > 0)
  and (LEN(ISNULL(std1.pdsearch_engineaccount_id,'')) > 0)
  and (LEN(ISNULL(fld2.pdsearch_engineaccount_id,'')) > 0)
  and (LEN(ISNULL(std1.pdsearch_campaign_id,'')) > 0)
  and std1.pdsearch_advertiser_id <> '0'
  and e1.Paid_SearchEngine like '%TMK%'

     ) as t1

group by
-- cast(t1.date as date),
--         cast(dateadd(week,datediff(week,0,cast(t1.date as date)),0) as date),
  t1.week1,
  t1.pdsearch_matchtype,
  t1.pdsearch_engineaccount_id,
  t1.placement_id,
--      t1.siteid_dcm,
--  t1.site_dcm,
  t1.paid_search_campaign,
  t1.Paid_SearchEngine,
--  t1.paid_search_keyword,
--     t1.paid_search_ad_group,
  t1.Paid_Search_AdGroup,
--     t1.paid_search_keyword,
  t1.keyword,
  t1.pdsearch_campaign_id,
  t1.pdsearch_keyword_id,
  t1.pdsearch_ad_id,
  t1.pdsearch_adgroup_id