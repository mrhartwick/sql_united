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
	fld1.week1                  as "week",
	fld1.placement_id,
-- 	p1.placement,
	s1.site_dcm,
	fld1.siteid_dcm,
	fld1.campaign_id,
	c1.campaign,
	sum(fld1.total_revenue)     as total_revenue,
	sum(fld1.transaction_count) as transaction_count,
	sum(number_of_tickets)      as number_of_tickets

from (

	     select
		     cast(fld0.activity_date as date)                                                as "date",
		     cast(dateadd(week,datediff(week,0,cast(fld0.Activity_Date as date)),0) as date) as "week1",
		     fld0.placement_id,
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
		     fld0.placement_id,
		     fld0.siteid_dcm,
		     fld0.campaign_id,
		     fld0.currency
     ) as fld1

	left join openQuery(verticaunited,
	                    'select  campaign_id, cast(campaign as varchar(4000)) as campaign from diap01.mec_us_united_20056.dfa2_campaigns
			 ') as c1
		on cast(fld1.Campaign_ID as int) = c1.campaign_id


	left join openQuery(verticaunited,
	                    'select  site_id_dcm, cast(site_dcm as varchar(4000)) as site_dcm from diap01.mec_us_united_20056.dfa2_sites
			 ') as s1
		on cast(fld1.siteid_dcm as int) = s1.site_id_dcm

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
	fld1.placement_id,
	fld1.siteid_dcm,
	s1.site_dcm,
	c1.campaign,
	fld1.week1,
-- 	p1.placement,
	fld1.campaign_id


