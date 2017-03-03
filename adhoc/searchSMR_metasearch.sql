-- Search SMR Automation  - Metasearch only
-- Can't do routes in THIS query - it messes everything up


-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_sch_adGroupTbl go
-- exec [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.crt_sch_keywordTbl go
-- exec master.dbo.crt_sch_keywordTbl go
-- exec master.dbo.crt_sch_keywordTbl2 go
-- exec master.dbo.crt_sch_keywordTbl3 go

declare @report_st date
declare @report_ed date
--2017-02-28
set @report_ed = '2017-02-28';
set @report_st = '2017-01-01';

select
-- 	cast(fld1.date as date)     as "date",
	fld1.week1                  as Week,
	null                        as [Search Engine],
	null                        as [Search Engine ID],
	'Metasearch'                as Network,
	c1.campaign                 as Campaign,
	fld1.campaign_id            as [Campaign ID],
	null                        as Keyword,
	null                        as [Keyword ID],
	null                        as [Ad Group],
	null                        as [Ad Group ID],
--    p1.placement,
--    fld1.placement_id,
	s1.site_dcm                 as Site,
	fld1.siteid_dcm             as [Site ID],
	null                        as [Ad ID],
	null                        as [Match Type],
	0                           as Impressions,
	0                           as Clicks,
	0                           as [Avg Position],
	0                           as Actions,
	0                           as Visits,
	sum(fld1.total_revenue)     as Revenue,
	sum(fld1.transaction_count) as Conversions,
	sum(number_of_tickets)      as Tickets

from (

	     select
		     cast(fld0.activity_date as date)                                                as "date",
		     cast(dateadd(week,datediff(week,0,cast(fld0.Activity_Date as date)),0) as date) as "week1",
             fld0.pdsearch_engineaccount_id,
	         fld0.pdsearch_ad_id,
	         fld0.pdsearch_adgroup_id,
             fld0.pdsearch_campaign_id,
             fld0.pdsearch_keyword_id,
-- 	         fld0.placement_id,
		     fld0.campaign_id,
		     fld0.siteid_dcm,
		     fld0.currency,
		     sum(fld0.total_revenue / rates.exchange_rate)                                   as total_revenue,
		     sum(fld0.transaction_count)                                                     as transaction_count,
		     count(*)                                                                        as this_count,
		     sum(number_of_tickets)                                                          as number_of_tickets

	     from [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_search_floodlight as fld0

		     left join openQuery(verticaunited,
		                         'select currency, date, exchange_rate from diap01.mec_us_mecexchangerates_20067.EXCHANGE_RATES
-- 					   where cast(date as date) between ''2017-01-28'' and ''2017-02-28''
     ') as rates
			     on fld0.currency = rates.currency
			     and cast(fld0.activity_date as date) = cast(rates.date as date)

	     where cast(fld0.activity_date as date) between @report_st and @report_ed
		     and fld0.currency <> 'Miles'
		     and (LEN(ISNULL(fld0.currency,'')) > 0)
		     and fld0.currency <> '--'
		     and fld0.activity_id = '978826'
-- 		     and fld0.campaign_id = '10740194' -- TMK Search 2017
	     group by
		     cast(fld0.activity_date as date),
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

--        JOINS in place but will bring back nulls (except placement)
-- 	      left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Placement as p1
-- 		      on fld1.placement_id = p1.placement_id


-- 	      left join [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_dim_paid_searchcampaign as c1
-- 		      on fld1.pdsearch_campaign_id = c1.paid_search_campaign_id

-- 	      left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchKeyword as k1
-- 		      on fld1.pdsearch_keyword_id = k1.Paid_Search_Keyword_ID
--
-- 	      left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_SearchAdGroup as ad1
-- 		      on fld1.pdsearch_adgroup_id = ad1.Paid_Search_AdGroup_ID
--
-- 	      left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchEngine as e1
-- 		      on fld1.pdsearch_engineaccount_id = e1.Paid_SearchEngine_ID

-- Wait and see if they need placement

-- 	left join openQuery(verticaunited,
-- 	'
-- select cast (p1.placement as varchar (4000)) as ''placement'',p1.placement_id as ''placement_id'',p1.campaign_id as ''campaign_id'',p1.site_id_dcm as ''site_id_dcm''
--
-- from ( select campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast (placement_start_date as date ) as thisdate,
-- row_number() over (partition by campaign_id,site_id_dcm,placement_id order by cast (placement_start_date as date ) desc ) as x1
-- from diap01.mec_us_united_20056.dfa2_placements
--
-- ) as p1
-- where x1 = 1
-- ') as p1
-- on fld1.placement_id    = p1.placement_id
-- and fld1.campaign_id = p1.campaign_id
-- and fld1.siteid_dcm  = p1.site_id_dcm


where c1.campaign like '%[Tt]argeted_[Mm]arketing_[Ss]earch%'

group by
	cast(fld1.date as date),
-- 	fld1.placement_id,
-- 	fld1.pdsearch_engineaccount_id,
-- 	e1.Paid_SearchEngine,
	fld1.siteid_dcm,
	s1.site_dcm,
-- 	k1.paid_search_keyword,
-- 	fld1.pdsearch_keyword_id,
	c1.campaign,
	fld1.week1,
-- 	p1.placement,
	fld1.campaign_id


