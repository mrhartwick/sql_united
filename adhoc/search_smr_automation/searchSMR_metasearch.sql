-- Search SMR Automation  - Metasearch only
-- Can't do routes in THIS query - it messes everything up


-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_sch_adGroupTbl go
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_sch_keywordTbl go
-- exec master.dbo.crt_sch_keywordTbl go
-- exec master.dbo.crt_sch_keywordTbl2 go
-- exec master.dbo.crt_sch_keywordTbl3 go

declare @report_st date
declare @report_ed date
--2017-02-2
set @report_ed = '2017-04-30';
set @report_st = '2017-04-01';

select
-- 	cast(fld1.date as date)     as "date",
	fld1.month                  as month,
	fld1.week                   as Week,
	null                        as [Search Engine],
	'Metasearch'                as Network,
	c1.campaign                 as Campaign,
-- 	null                        as Keyword,
-- 	null                        as [Keyword ID],
	null                        as [Ad Group],
-- 	null                        as [Ad Group ID],
--    p1.placement,
--    fld1.placement_id,
	s1.site_dcm                 as Site,
-- 	null                        as [Ad ID],
	null                        as [Match Type],
	0                           as Impressions,
	0                           as cost,
	0                           as Clicks,
	0                           as [Avg Position],
    sum(fld1.total_revenue * .15 * .02) as rev,
    sum(fld1.prch)          as purchases,
    sum(fld1.lead)          as leads,
--  sum(fld1.total_conversions) as tot_con,
    sum(fld1.number_of_tickets)  as tickets

from (

	     select
-- 		     cast(fld0.activity_date as date)                                                as "date",
		     datename(month,cast(fld0.date as date))                                 as [month],
		     cast(dateadd(week,datediff(week,0,cast(fld0.Activity_Date as date)),0) as date) as [week],
             fld0.pdsearch_engineaccount_id,
	         fld0.pdsearch_ad_id,
	         fld0.pdsearch_adgroup_id,
             fld0.pdsearch_campaign_id,
             fld0.pdsearch_keyword_id,
-- 	         fld0.placement_id,
		     fld0.campaign_id,
		     fld0.siteid_dcm,

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
			     and cast(fld0.activity_date as date) = cast(rates.date as date)

	     where cast(fld0.activity_date as date) between @report_st and @report_ed
-- 		     and fld0.currency <> 'Miles'
-- 		     and (LEN(ISNULL(fld0.currency,'')) > 0)
-- 		     and fld0.currency <> '--'
		     and (fld0.activity_id = 978826 or fld0.activity_id = 1086066)
-- 		     and fld0.campaign_id = '10740194' -- TMK Search 2017
	     group by
-- 		     cast(fld0.activity_date as date),
		     datename(month,cast(fld0.date as date)),
		     cast(dateadd(week,datediff(week,0,cast(fld0.Activity_Date as date)),0) as date),
             fld0.pdsearch_engineaccount_id,
	         fld0.pdsearch_ad_id,
	         fld0.pdsearch_adgroup_id,
             fld0.pdsearch_campaign_id,
             fld0.pdsearch_keyword_id,
-- 		     fld0.placement_id,
		     fld0.siteid_dcm,
		     fld0.campaign_id,
		     fld0.currency
     ) as fld1

-- 	left join openQuery(verticaunited,
-- 	                    'select  campaign_id, cast(campaign as varchar(4000)) as campaign from diap01.mec_us_united_20056.dfa2_campaigns
-- 			 ') as c1
-- 		on cast(fld1.Campaign_ID as int) = c1.campaign_id


	      left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Campaign as c1
		      on fld1.Campaign_ID = c1.campaign_id

-- 	      left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Site as s1
-- 		      on fld1.siteid_dcm = s1.site_id_dcm

	left join openQuery(verticaunited,
	                    'select  site_id_dcm, cast(site_dcm as varchar(4000)) as site_dcm from diap01.mec_us_united_20056.dfa2_sites
			 ') as s1
		on cast(fld1.siteid_dcm as int) = s1.site_id_dcm



where c1.campaign like '%[Tt]argeted_[Mm]arketing_[Ss]earch%'

group by
	fld1.week,
	fld1.month,
-- 	fld1.placement_id,
-- 	fld1.pdsearch_engineaccount_id,
-- 	e1.Paid_SearchEngine,
-- 	fld1.siteid_dcm,
	s1.site_dcm,
-- 	k1.paid_search_keyword,
-- 	fld1.pdsearch_keyword_id,
	c1.campaign
-- 	p1.placement,
-- 	fld1.campaign_id


