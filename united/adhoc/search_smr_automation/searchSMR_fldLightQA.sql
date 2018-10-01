-- Search SMR Automation
-- QA for the floodlight table

/* GOT RID OF AD_ID in my created DIM table*/
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_sch_adGroupTbl go
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_sch_keywordTbl go
-- exec master.dbo.crt_sch_keywordTbl go
-- exec master.dbo.crt_sch_keywordTbl2 go
-- exec master.dbo.crt_sch_keywordTbl3 go


declare @report_st date
declare @report_ed date
--
set @report_ed = '2017-01-07';
set @report_st = '2017-01-01';


select
  cast(fld1.date as date)     as "date",
--          cast(dateadd(week,datediff(week,0,cast(fld1.Activity_Date as date)),0) as date) as "week",

  fld1.pdsearch_advertiser    as pdsearch_advertiser,
  fld1.pdsearch_advertiser_id as pdsearch_advertiser_id,
--          fld1.week as [week],
  e1.Paid_SearchEngine        as Paid_SearchEngine,
  fld1.pdsearch_engineaccount_id,
  fld1.pdsearch_ad_id,
  fld1.pdsearch_adgroup,
  ad1.Paid_Search_AdGroup     as Paid_Search_AdGroup,
  fld1.pdsearch_adgroup_id,
--           fld1.placement_id,
  fld1.siteid_dcm,
  c1.paid_search_campaign     as paid_search_campaign,
  fld1.pdsearch_campaign_id,
--           k1.paid_search_keyword                                                 as keyword,
  k1.paid_search_keyword as keyword,
  fld1.pdsearch_keyword,
  fld1.pdsearch_keyword_id,
  fld1.currency               as currency,
  fld1.pnr_base64encoded,
  sum(fld1.total_revenue)     as total_revenue,
  sum(fld1.transaction_count) as transaction_count,
  sum(number_of_tickets)      as number_of_tickets

from (select
--                cast(fld0.date as date)                       as "date",
        fld0.pdsearch_advertiser,
        fld0.pdsearch_advertiser_id,
        cast(fld0.activity_date as date)              as "date",
--          cast(dateadd(week,datediff(week,0,cast(fld0.Activity_Date as date)),-1) as date) as "week",
        fld0.pdsearch_engineaccount_id,
        fld0.pdsearch_ad_id,
        fld0.pdsearch_adgroup_id,
        fld0.pdsearch_adgroup,
        fld0.pnr_base64encoded,
--                 fld0.placement_id,
        fld0.siteid_dcm,
        fld0.pdsearch_campaign_id,
        fld0.pdsearch_keyword_id,
        fld0.pdsearch_keyword,
        fld0.currency,
        sum(fld0.total_revenue / rates.exchange_rate) as total_revenue,
        sum(fld0.transaction_count)                   as transaction_count,
--                  count(*)                                      as this_count,
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
        and fld0.PdSearch_Advertiser_ID = 21700000001003602
        and fld0.PdSearch_ad_ID <> '0'
        and fld0.number_of_tickets <> 0
        and fld0.currency <> 'Miles'
        and (LEN(ISNULL(fld0.currency,'')) > 0)
        and (PdSearch_EngineAccount not like '%[Br][Rr]%[Nn][Dd]%' and PdSearch_EngineAccount not like '%[Ss][Mm][Ee]%')
        and fld0.currency <> '--'
        and fld0.activity_id = 978826
      group by
        cast(fld0.date as date),
        cast(fld0.activity_date as date),
--          cast(dateadd(week,datediff(week,0,cast(fld0.Activity_Date as date)),0) as date),
        fld0.pdsearch_engineaccount_id,
--                 fld0.placement_id,
        fld0.pdsearch_advertiser,
        fld0.siteid_dcm,
        fld0.pdsearch_advertiser_id,
        fld0.currency,
        fld0.pdsearch_keyword,
        fld0.pdsearch_adgroup,
        fld0.pnr_base64encoded,
        fld0.pdsearch_ad_id,
        fld0.pdsearch_adgroup_id,
        fld0.pdsearch_keyword_id,
        fld0.pdsearch_campaign_id
     ) as fld1


  left join [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_dim_paid_searchcampaign as c1
    on fld1.pdsearch_campaign_id = c1.paid_search_campaign_id

--        left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchKeyword as k1
--          on std1.pdsearch_keyword_id = k1.Paid_Search_Keyword_ID

  left join master.dbo.sch_keyword_2 as k1
    on cast(fld1.pdsearch_keyword_id as bigint) = k1.paid_search_keyword_id


--                                 left join master.dbo.sch_keyword as k1
--             on cast(fld1.pdsearch_keyword_id as bigint) = k1.paid_search_keyword_id

  left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_SearchAdGroup as ad1
    on fld1.pdsearch_adgroup_id = ad1.Paid_Search_AdGroup_ID

  left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchEngine as e1
    on fld1.pdsearch_engineaccount_id = e1.Paid_SearchEngine_ID

  left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Site as s1
    on fld1.siteid_dcm = s1.site_id_dcm


group by
  cast(fld1.date as date),
--          cast(dateadd(week,datediff(week,0,cast(fld1.Activity_Date as date)),0) as date),
  fld1.pdsearch_engineaccount_id,
--           fld1.placement_id,
  e1.Paid_SearchEngine,
--           fld1.week,
  fld1.pdsearch_adgroup,
  fld1.pnr_base64encoded,
  ad1.Paid_Search_AdGroup,
  c1.paid_search_campaign,
  fld1.pdsearch_keyword,
--           k1.paid_search_keyword,
  k1.paid_search_keyword,
  fld1.pdsearch_advertiser_id,
  fld1.siteid_dcm,
  fld1.pdsearch_ad_id,
  fld1.pdsearch_adgroup_id,
  fld1.pdsearch_keyword_id,
  fld1.currency,
  fld1.pdsearch_advertiser,
  fld1.pdsearch_campaign_id

