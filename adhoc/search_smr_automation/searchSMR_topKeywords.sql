
declare @report_st date
declare @report_ed date
--
set @report_ed = '2017-07-31';
set @report_st = '2017-07-01';


select
--  fld1.date     as "date",
--  fld1.pdsearch_advertiser as pdsearch_advertiser,
--  fld1.pdsearch_advertiser_id as pdsearch_advertiser_id,
--             fld1.month               as [month],
--             fld1.week               as [week],
--             fld1.pdsearch_engineaccount_id,
--             fld1.pdsearch_ad_id,
--             fld1.pdsearch_adgroup_id,
-- --  fld1.placement_id,
--             fld1.siteid_dcm,
--             fld1.pdsearch_campaign_id,
      fld1.pdsearch_campaign,
 fld1.pdsearch_keyword,
--             fld1.pdsearch_keyword_id,
--  fld1.currency as currency,
            sum(fld1.total_revenue * .08) as rev,
            sum(fld1.prch)          as prch,
            sum(fld1.lead)          as lead,
--  sum(fld1.total_conversions) as tot_con,
            sum(fld1.number_of_tickets)  as tix,
    row_number() over (partition by fld1.pdsearch_campaign order by sum(fld1.lead) desc ) as r1

        from (

                 select
--           fld0.pdsearch_advertiser,
--           fld0.pdsearch_advertiser_id,
                     cast(fld0.date as date)                                                 as "date",
--                      datename(month,cast(fld0.date as date))                                 as [month],
--                      cast(dateadd(week,datediff(week,0,cast(fld0.date as date)),-1) as date) as [week],
                     fld0.pdsearch_engineaccount_id,
                     fld0.pdsearch_ad_id,
                     fld0.pdsearch_adgroup_id,
--           fld0.pdsearch_adgroup,
--                 fld0.placement_id,
                     fld0.siteid_dcm,
                     fld0.pdsearch_campaign_id,
                     fld0.pdsearch_campaign,
                     fld0.pdsearch_keyword_id,
                     fld0.pdsearch_keyword,
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
                 and (LEN(ISNULL(fld0.pdsearch_keyword,'')) > 0)
--                  and fld0.PdSearch_ad_ID <> '0'
--                  and fld0.number_of_tickets <> 0
--                  and fld0.currency <> 'Miles'
--                  and (LEN(ISNULL(fld0.currency,'')) > 0)
--                  and (PdSearch_EngineAccount not like '%[Br][Rr]%[Nn][Dd]%' and PdSearch_EngineAccount not like '%[Ss][Mm][Ee]%')
                     and (PdSearch_EngineAccount not like '%[Ss][Mm][Ee]%')
                     and (PdSearch_EngineAccount not like '%[Bb][Rr][Nn][Dd]%')
                     and (PdSearch_EngineAccount not like '%[Pp][Ll][Cc][Yy]%')
--                  and fld0.currency not like '%--%'
                     and (fld0.activity_id = 978826 or fld0.activity_id = 1086066)

                 group by
--          cast(fld0.date as date),
                     cast(fld0.date as date),
--                      cast(dateadd(week,datediff(week,0,cast(fld0.date as date)),0) as date),
                     fld0.pdsearch_engineaccount_id,
--                 fld0.placement_id,
--                 fld0.pdsearch_advertiser,
                     fld0.siteid_dcm,
--                 fld0.pdsearch_advertiser_id,
--                  fld0.currency,
                     fld0.pdsearch_campaign,
                fld0.pdsearch_keyword,
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
--          cast(dateadd(week,datediff(week,0,cast(fld1.date as date)),0) as date),
--             fld1.pdsearch_engineaccount_id,
--           fld1.placement_id,
--             fld1.week,
--             fld1.month,
--           fld1.pdsearch_adgroup,
--           fld1.pnr_base64encoded,
--           fld1.pdsearch_keyword,
--           fld1.pdsearch_advertiser_id,
--             fld1.siteid_dcm,
                  fld1.pdsearch_campaign,
 fld1.pdsearch_keyword
--             fld1.pdsearch_ad_id,
--             fld1.pdsearch_adgroup_id,
--             fld1.pdsearch_keyword_id,
--           fld1.currency,
--           fld1.pdsearch_advertiser,
--             fld1.pdsearch_campaign_id