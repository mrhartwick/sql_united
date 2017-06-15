-- Search SMR Automation 5
-- matching at day/keyword/ad_id level doesn't work; there are dupes in Standard file
-- have to do it at week level

/* GOT RID OF AD_ID in my created DIM table*/
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_sch_adGroupTbl go
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_sch_keywordTbl go
-- exec master.dbo.crt_sch_keywordTbl go
-- exec master.dbo.crt_sch_keywordTbl_2 go
-- exec master.dbo.crt_sch_keywordTbl_3 go


declare @report_st date
declare @report_ed date
--
set @report_ed = '2017-05-06';
set @report_st = '2017-01-01';

select
-- t1.date,
--         cast(dateadd(week,datediff(week,0,cast(t1.date as date)),0) as date) as "week",
  t1.week                     as [Week],
  e1.Paid_SearchEngine         as [Search Engine],
--   t1.pdsearch_engineaccount_id as [Search Engine ID],
  case
  when
    (c1.paid_search_campaign like '%[Rr]emarketing%' or ad1.Paid_Search_AdGroup like '%[Rr]emarketing%') then 'Display'
  when (c1.paid_search_campaign like '%[Rr][Ll][Ss][Aa]%' or ad1.Paid_Search_AdGroup like '%[Rr][Ll][Ss][Aa]%' or e1.Paid_SearchEngine like '%[Rr][Ll][Ss][Aa]%') then 'RLSA'
  when (c1.paid_search_campaign like '%[Ss]earch%' or ad1.Paid_Search_AdGroup like '%[Ss]earch%') then 'Search'
  else 'Search' end        as Network,
--  t1.placement_id,
  c1.paid_search_campaign      as Campaign,
--   t1.pdsearch_campaign_id      as [Campaign ID],
--   t1.paid_search_keyword                   as Keyword,
--   t1.pdsearch_keyword_id       as [Keyword ID],
  ad1.Paid_Search_AdGroup       as [Ad Group],
--   t1.pdsearch_adgroup_id       as [Ad Group ID],
  s1.site_dcm                  as Site,
--   t1.siteid_dcm                as [Site ID],
--   t1.pdsearch_ad_id            as [Ad ID],
  t1.pdsearch_matchtype        as [Match Type],
  sum(t1.imps)                 as Impressions,
  sum(t1.cost)               as cost,
  sum(t1.clicks)               as Clicks,
  avg(t1.avg_pos)              as [Avg Position],
  sum(case when t1.imps = 0 then 0 else t1.avg_pos_1/t1.imps end) as avg_pos_2,
  case when sum(t1.imps) = 0 then 0 else sum(t1.avg_pos * t1.imps)/sum(t1.imps) end as avg_pos_3,
  sum(fld2.rev * .15 * .02)                  as Revenue,
  sum(cast(fld2.prch as int))     as prch,
  sum(cast(fld2.lead as int))     as lead,
--   sum(cast(fld2.tot_con as int))     as Transactions,
  sum(fld2.tix)                  as Tickets

from (

--
--
--  declare @report_st date
-- declare @report_ed date
--
-- set @report_ed = '2017-02-28';
-- set @report_st = '2017-02-28';


       select
--         cast(std1.date as date)                                                as "Date",
        cast(dateadd(week,datediff(week,0,cast(std1.date as date)),-1) as date) as [week],
        std1.pdsearch_engineaccount_id                                         as pdsearch_engineaccount_id,
--        std1.placement_id                                                      as placement_id,
        std1.siteid_dcm                                                        as siteid_dcm,
        std1.pdsearch_campaign_id                                              as pdsearch_campaign_id,
        std1.pdsearch_keyword_id                                               as pdsearch_keyword_id,
        std1.pdsearch_adgroup_id                                               as pdsearch_adgroup_id,
        std1.pdsearch_ad_id                                                    as pdsearch_ad_id,
        std1.pdsearch_matchtype                                                as pdsearch_matchtype,
        sum(std1.pdsearch_impressions)                                              as imps,
      sum(std1.pdsearch_cost)                                              as cost,
        sum(std1.pdsearch_clicks)                                                   as clicks,
        avg(std1.pdsearch_avg_position)                                             as avg_pos,
        sum(std1.pdsearch_impressions * std1.pdsearch_avg_position)             as avg_pos_1
--         std1.pdsearch_avg_position                                             as avg_pos


      from [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_search_standard as std1


where cast(std1.date as date) between @report_st and @report_ed
and std1.pdsearch_advertiser_id <> '0'

group by
cast(dateadd(week,datediff(week,0,cast(std1.date as date)),-1) as date),
        std1.pdsearch_engineaccount_id,
        std1.siteid_dcm,
        std1.pdsearch_campaign_id,
        std1.pdsearch_keyword_id,
        std1.pdsearch_adgroup_id,
--    std1.pdsearch_avg_position,
        std1.pdsearch_ad_id,
        std1.pdsearch_matchtype

     ) as t1

       left join [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_dim_paid_searchcampaign as c1
          on t1.pdsearch_campaign_id = c1.paid_search_campaign_id

        left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_SearchAdGroup as ad1
          on t1.pdsearch_adgroup_id = ad1.Paid_Search_AdGroup_ID

        left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchEngine as e1
          on t1.pdsearch_engineaccount_id = e1.Paid_SearchEngine_ID

        left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Site as s1
          on t1.siteid_dcm = s1.site_id_dcm


       left join
        (
--
--                declare @report_st date
-- declare @report_ed date
-- --
-- set @report_ed = '2017-02-28';
-- set @report_st = '2017-02-28';

        select
--            fld1.date     as "date",
--  fld1.pdsearch_advertiser as pdsearch_advertiser,
--  fld1.pdsearch_advertiser_id as pdsearch_advertiser_id,
          fld1.week as [week],
           fld1.pdsearch_engineaccount_id,
           fld1.pdsearch_ad_id,
           fld1.pdsearch_adgroup_id,
--           fld1.placement_id,
           fld1.siteid_dcm,
           fld1.pdsearch_campaign_id,
--  fld1.pdsearch_keyword,
           fld1.pdsearch_keyword_id,
--  fld1.currency as currency,
           sum(fld1.total_revenue)     as rev,
           sum(fld1.prch) as prch,
            sum(fld1.lead) as lead,
--             sum(fld1.total_conversions) as tot_con,
           sum(number_of_tickets)      as tix

         from (


                select
--           fld0.pdsearch_advertiser,
--           fld0.pdsearch_advertiser_id,
                 cast(fld0.date as date)              as "date",
                 cast(dateadd(week,datediff(week,0,cast(fld0.date as date)),-1) as date) as [week],
                 fld0.pdsearch_engineaccount_id,
                 fld0.pdsearch_ad_id,
                 fld0.pdsearch_adgroup_id,
--           fld0.pdsearch_adgroup,
--                 fld0.placement_id,
                 fld0.siteid_dcm,
                 fld0.pdsearch_campaign_id,
                 fld0.pdsearch_keyword_id,
--               fld0.pdsearch_keyword,
--                  fld0.currency,
                 sum(case
                     when fld0.activity_id = 978826 and
                          fld0.currency not like '%--%' and
                          fld0.currency <> 'Miles' and
                          fld0.number_of_tickets <> 0
                     then fld0.total_revenue / rates.exchange_rate
                     end) as total_revenue,
                 sum(case
                     when fld0.activity_id = 978826 and
                          fld0.currency not like '%--%' and
                          fld0.currency <> 'Miles' and
                          fld0.total_revenue > 0
                     then fld0.transaction_count
                     end) as prch,
                 sum(case
                     when fld0.activity_id = 1086066
                     then fld0.transaction_count
                     end) as lead,
--                  sum(fld0.total_conversions) as total_conversions,
--                  count(*)                                      as this_count,
                 sum(case
                     when fld0.activity_id = 978826 and
                          fld0.currency not like '%--%' and
                          fld0.currency <> 'Miles'
                     then number_of_tickets
                     end) as number_of_tickets


               from [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_search_floodlight as fld0

                 left join openQuery(verticaunited,
                                     'select currency, date, exchange_rate from diap01.mec_us_mecexchangerates_20067.EXCHANGE_RATES
                                    ') as rates
                   on fld0.currency = rates.currency
                   and cast(fld0.date as date) = cast(rates.date as date)

               where cast(fld0.date as date) between @report_st and @report_ed
--                  and (LEN(ISNULL(fld0.pdsearch_engineaccount_id,'')) > 0)
--                  and (LEN(ISNULL(fld0.pdsearch_adgroup_id,'')) > 0)
--                  and (LEN(ISNULL(fld0.pdsearch_campaign_id,'')) > 0)
--                  and (LEN(ISNULL(fld0.pdsearch_keyword_id,'')) > 0)
--                  and fld0.PdSearch_ad_ID <> '0'
--                  and fld0.number_of_tickets <> 0
--                  and fld0.currency <> 'Miles'
--                  and (LEN(ISNULL(fld0.currency,'')) > 0)
--                  and (PdSearch_EngineAccount not like '%[Br][Rr]%[Nn][Dd]%' and PdSearch_EngineAccount not like '%[Ss][Mm][Ee]%')
                   and (PdSearch_EngineAccount not like '%[Ss][Mm][Ee]%')
--                  and fld0.currency not like '%--%'
                 and (fld0.activity_id = 978826 or fld0.activity_id = 1086066)
--                  and fld0.activity_id = 1086066

               group by
--          cast(fld0.date as date),
                 cast(fld0.date as date),
         cast(dateadd(week,datediff(week,0,cast(fld0.date as date)),0) as date),
                 fld0.pdsearch_engineaccount_id,
--                 fld0.placement_id,
--                 fld0.pdsearch_advertiser,
                 fld0.siteid_dcm,
--                 fld0.pdsearch_advertiser_id,
--                  fld0.currency,
--                 fld0.pdsearch_keyword,
--                 fld0.pdsearch_adgroup,
--                 fld0.pnr_base64encoded,
                 fld0.activity_id,
                 fld0.pdsearch_ad_id,
                 fld0.pdsearch_adgroup_id,
                 fld0.pdsearch_keyword_id,
                 fld0.pdsearch_campaign_id


              ) as fld1



         group by
--            fld1.date,
--          cast(dateadd(week,datediff(week,0,cast(fld1.Activity_Date as date)),0) as date),
           fld1.pdsearch_engineaccount_id,
--           fld1.placement_id,
           fld1.week,
--           fld1.pdsearch_adgroup,
--           fld1.pnr_base64encoded,
--           fld1.pdsearch_keyword,
--           fld1.pdsearch_advertiser_id,
           fld1.siteid_dcm,
           fld1.pdsearch_ad_id,
           fld1.pdsearch_adgroup_id,
           fld1.pdsearch_keyword_id,
--           fld1.currency,
--           fld1.pdsearch_advertiser,
           fld1.pdsearch_campaign_id
        ) as fld2
          on t1.pdsearch_campaign_id = fld2.pdsearch_campaign_id
      and t1.week = fld2.week
          and t1.pdsearch_engineaccount_id = fld2.pdsearch_engineaccount_id
          and t1.pdsearch_keyword_id = fld2.pdsearch_keyword_id
          and t1.pdsearch_ad_id = fld2.pdsearch_ad_id
          and t1.pdsearch_adgroup_id = fld2.pdsearch_adgroup_id
     and t1.siteid_dcm = fld2.siteid_dcm

--       where e1.Paid_SearchEngine like '%TMK%'
-- where (e1.Paid_SearchEngine not like '%[Br][Rr]%[Nn][Dd]%' and e1.Paid_SearchEngine not like '%[Bb][Rr][Nn][Dd]%' and e1.Paid_SearchEngine not like '%[Ss][Mm][Ee]%')
where (e1.Paid_SearchEngine not like '%[Bb][Rr][Nn][Dd]%' and e1.Paid_SearchEngine not like '%[Ss][Mm][Ee]%')
group by
-- t1.date,
--         cast(dateadd(week,datediff(week,0,cast(t1.date as date)),0) as date),
  t1.week,
  t1.pdsearch_matchtype,
--   t1.pdsearch_engineaccount_id,
-- -- -- --  t1.placement_id,
--   t1.siteid_dcm,
  s1.site_dcm,
  c1.paid_search_campaign,
  e1.Paid_SearchEngine,
  ad1.Paid_Search_AdGroup
-- -- --   t1.paid_search_keyword
--   t1.pdsearch_campaign_id,
-- --   t1.pdsearch_keyword_id,
-- --   t1.pdsearch_ad_id,
--   t1.pdsearch_adgroup_id



