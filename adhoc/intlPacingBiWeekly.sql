--  Intl Pacing
/*  This query is a bit of a hack. Non-optimal aspects are necessitated by the particularities of the current tech stack on United.
	Code is most easily read by starting at the "innermost" block, inside the openQuery call.

	Data must be pulled and reconciled from 1) Prisma, in Datamart, and 2) DFA/DV/Moat in Vertica*.



	Because of its scale, log-level DFA data (DTF 1.0) must be kept in Vertica to preserve performance. DV and Moat are stored there as well, mostly for convenience.
	Intermediary summary tables are necessary for this query, so we need to be able to create our own tables and run stored procedures to refresh those tables. But we don't have write access to Vertica, so we can't keep routines and tables there.

	DI could do this, but edits to these routines are frequent (esp. in joinKey fields), so keeping this process in-house is more convenient for all parties.

*/
-- these summary/reference tables can be run once a day as a regular process or before the query is run

-- EXEC master.dbo.createDVTbl GO    -- create separate DV aggregate table and store it in my instance; joining to the Vertica table in the query
-- EXEC master.dbo.createMTTbl GO    -- create separate MOAT aggregate table and store it in my instance; joining to the Vertica table in the query
-- EXEC [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.createInnovidExtTbl GO
-- -- -- --
-- EXEC [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.createViewTbl GO
-- EXEC [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.createAmtTbl GO
-- EXEC [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.createPackTbl GO
-- EXEC [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.createSumTbl GO
-- EXEC master.dbo.createflatTblDay GO



DECLARE @report_st date,
@report_ed date;
--
SET @report_ed = '2016-10-21';
SET @report_st = '2016-01-01';

--
-- SET @report_ed = DateAdd(DAY, -DatePart(DAY, getdate()), getdate());
-- SET @report_st = DateAdd(DAY, 1 - DatePart(DAY, @report_ed), @report_ed);

select
	-- DCM ad server date
	cast(final.dcmDate as date)                                                                             as "Date",
	-- DCM ad server week (from date)
	cast(dateadd(week,datediff(week,0,cast(final.dcmDate as date)),0) as date)                              as "Week",
	-- DCM ad server month (from date)
	dateName(month,cast(final.dcmDate as date))                                                             as "Month",
	-- DCM ad server quarter + year (from date)
	'Q' + dateName(quarter,cast(final.dcmDate as date)) + ' ' + dateName(year,cast(final.dcmDate as date))  as "Quarter",
	-- Reference/optional: difference, in months, between placement end date and report date. Field is used deterministically below.
	final.diff                                                                                              as diff,
	-- Reference/optional: match key from the DV table; only present when DV data is available.
	final.dvJoinKey                                                                                         as dvJoinKey,
	-- Reference/optional: match key from the Moat table; only present when Moat data is available.
	final.mtJoinKey                                                                                         as mtJoinKey,
	-- Reference/optional: match key from the Innovid table; only present when Innovid data is available.
    final.ivJoinKey 																                        as ivJoinKey,
	-- Reference/optional: package category from Prisma (standalone; package; child)
	final.PackageCat                                                                                        as PackageCat,
	-- Reference/optional: first six characters of package-level placement name, used to join 1) Prisma table, and 2) flat fee table
	final.Cost_ID                                                                                           as Cost_ID,
	-- DCM Campaign name
	final.Buy                                                                                               as "DCM Campaign",
	-- Friendly Campaign name
	case
	when  final.order_id = '9973506' 						      then 'SFO-AKL'
	when  final.order_id = '9304728' 							  then 'Trade'
	when  final.order_id = '9407915' 							  then 'Google PDE'
	when  final.order_id = '9548151' 							  then 'Smithsonian'
	when  final.order_id = '9630239' 							  then 'SFO-TLV'
	when  final.order_id = '9639387' 							  then 'Targeted Marketing'
	when  final.order_id = '9739006' 							  then 'Spoke Markets'
	when  final.order_id = '9923634' 							  then 'SFO-SIN'
	when  final.order_id = '10276123' 							  then 'Polaris'
	when  final.order_id = '10094548' 							  then 'Marketing Fund'
	when  final.order_id = '10090315' 							  then 'SME'
	when  final.order_id = '9994694' 							  then 'SFO-China'
    when  final.order_id = '10307468' 							  then 'SFO-HGH/XIY'
	when  final.order_id = '9408733' 							  then 'Chile CoOp'
	when  final.order_id = '9999841' or final.order_id='10121649' then 'Olympics'
	else  final.Buy end                                                                                     as Campaign,
	-- DCM campaing ID
	final.order_id                                                                                          as "Campaign ID",
	-- Reference/optional: three-character designation, sometimes descriptive, from placement name.
	final.campaignShort                                                                                     as "Campaign Short Name",
	-- Designation specified by Planning/Investment in 2016.
	final.campaignType                                                                                      as "Campaign Type",
	-- DCM site name
-- 	final.Directory_Site                                                          							as SITE,
	-- Preferred, friendly site name; also corresponds to what's used in the joinKey fields across DFA, DV, and Moat.
	case	when ( final.Directory_Site like '%[Cc]hicago%[Tt]ribune%' or final.Directory_Site like '[Tt]ribune_[Ii]nteractive%' ) then 'ChicagoTribune'
				when ( final.Directory_Site like '[Gg][Dd][Nn]%' or final.Directory_Site like '[Gg]oogle_[Dd]isplay_[Nn]etwork%' ) then 'Google'
				when final.Directory_Site like '%[Aa]dara%' then 'Adara'
				when final.Directory_Site like '%[Aa]tlantic%' then 'The Atlantic'
				when final.Directory_Site like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
				when final.Directory_Site like '%[Cc][Nn][Nn]%' then 'CNN'
				when final.Directory_Site like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
				when final.Directory_Site like '%[Ff]orbes%' then 'Forbes'
				when final.Directory_Site like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
				when final.Directory_Site like '%[Jj]un%[Gg]roup%' then 'JunGroup'
				when final.Directory_Site like '%[Mm][Ll][Bb]%' then 'MLB'
				when final.Directory_Site like '%[Mm]ansueto%' then 'Inc'
				when final.Directory_Site like '%[Mn][Ss][Nn]%' then 'MSN'
				when final.Directory_Site like '%[Nn][Bb][Aa]%' then 'NBA'
				when final.Directory_Site like '%[Nn][Ff][Ll]%' then 'NFL'
				when final.Directory_Site like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
				when final.Directory_Site like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
				when final.Directory_Site like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
				when final.Directory_Site like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
				when final.Directory_Site like '%[Pp]riceline%' then 'Priceline'
				when final.Directory_Site like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
				when final.Directory_Site like '%[Tt]ap%[Aa]d%' then 'TapAd'
				when final.Directory_Site like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
				when final.Directory_Site like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
				when final.Directory_Site like '%[Ww]all_[Ss]treet_[Jj]ournal%'  or final.Directory_Site like '[Ww][Ss][Jj]' then 'Wall Street Journal'
				when final.Directory_Site like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
				when final.Directory_Site like '%[Yy]ahoo%' then 'Yahoo'
				when final.Directory_Site like '%[Yy]ou%[Tt]ube%' then 'YouTube'
				when final.Directory_Site like '[Aa]d[Pp]rime%' then 'AdPrime'
				when final.Directory_Site like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
				when final.Directory_Site like '[Aa]mobee%' then 'Amobee'
				when final.Directory_Site like '[Cc]ardlytics%' then 'Cardlytics'
				when final.Directory_Site like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
				when final.Directory_Site like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
				when final.Directory_Site like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
				when final.Directory_Site like '[Ff]acebook%' then 'Facebook'
				when final.Directory_Site like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
				when final.Directory_Site like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
			    when final.Directory_Site like '[Ff]lipboard%' then 'Flipboard'
				when final.Directory_Site like '[Gg]um_[Gg]um%' then 'Gum Gum'
				when final.Directory_Site like '[Hh]ulu%' then 'Hulu'
				when final.Directory_Site like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
				when final.Directory_Site like '[Ii][Nn][Cc]%' then 'Inc'
				when final.Directory_Site like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
				when final.Directory_Site like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
				when final.Directory_Site like '[Ii]ndependent%' then 'Independent'
				when final.Directory_Site like '[Kk]ayak%' then 'Kayak'
				when final.Directory_Site like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
				when final.Directory_Site like '[Mm]artini_[Mm]edia%' then 'Martini Media'
				when final.Directory_Site like '[Oo]rbitz%' then 'Orbitz'
				when final.Directory_Site like '[Ss]kyscanner%' then 'Skyscanner'
				when final.Directory_Site like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
				when final.Directory_Site like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
				when final.Directory_Site like '[Ss]mithsonian%' then 'Smithsonian'
				when final.Directory_Site like '[Ss]ojern%' then 'Sojern'
				when final.Directory_Site like '[Ss]pecific_[Mm]edia%' or final.Directory_Site like '[Vv]iant%' then 'Viant'
				when final.Directory_Site like '[Ss]potify%' then 'Spotify'
				when final.Directory_Site like '[Tt]ime%[Ii]nc%' then 'Time Inc'
				when final.Directory_Site like '[Tt]ony%[As]wards%' then 'TonyAwards'
				when final.Directory_Site like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
				when final.Directory_Site like '[Tt]ravelocity%' then 'Travelocity'
				when final.Directory_Site like '[Tt]riggit%' then 'Triggit'
				when final.Directory_Site like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
				when final.Directory_Site like '[Uu]ndertone%' then 'Undertone'
				when final.Directory_Site like '[Uu]nited%' then 'United'
				when final.Directory_Site like '[Vv]erve%' then 'VerveMobile'
				when final.Directory_Site like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
				when final.Directory_Site like '[Vv]ox%' then 'Vox'
				when final.Directory_Site like '[Ww]ired%' then 'Wired'
				when final.Directory_Site like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
				when final.Directory_Site like '[Xx]ad%' then 'xAd Inc'
				when final.Directory_Site like '[Yy]ieldbot%' then 'Yieldbot'
				when final.Directory_Site like '[Yy]u[Mm]e%' then 'YuMe'
				else final.Directory_Site end                                                               as Site,
	-- DCM site ID
	final.Site_ID                                                                                           as "Site ID",
	-- Reference/optional: package cost/pricing model, from Prisma; attributed to all placements within package.
	final.CostMethod                                                                                        as "Cost Method",
	-- Reference/optional: first six characters of placement name. Used for matching across datasets when no common placement ID is available, e.g. DFA-DV.
-- 	final.PlacementNumber                                                         							as PlacementNumber,
	-- DCM placement name
	final.Site_Placement                                                                                    as Placement,
	-- DCM placement ID
	final.page_id                                                                                           as page_id,
	-- Reference/optional: planned package end date, from Prisma; attributed to all placements within package.
	final.PlacementEnd                                                                                      as "Placement End",
	final.PlacementStart                                                                                    as "Placement Start",
	final.DV_Map                                                                                            as "DV Map",
	final.Rate                                                                                              as Rate,
	final.Planned_Amt                                                                                       as "Planned Amt",
-- 	final.flatCostRemain                                                          							as flatCostRemain,
-- 	final.impsRemain                                                              							as impsRemain,
-- 	sum(final.cost)                                                          								as cost,
	case when final.CostMethod='Flat' then final.flatCost/max(final.newCount) else sum(final.cost) end      as cost,
	sum(final.dlvrImps)                                                                                     as "Delivered Impressions",
	sum(final.billImps)                                                                                     as "Billable Impressions",
	sum(final.cnslImps)                                                                                     as "DFA Impressions",
	sum(final.IV_Impressions)                                                                               as "Innovid Impressions",
-- 	sum(MT.human_impressions)                                      											as MT_Impressions,
-- 	sum(final.dv_impressions)                                                     							as DV_Impressions,
-- 	sum(final.DV_Viewed)                                                                                    as DV_Viewed,
-- 	sum(final.DV_GroupMPayable)                                                   							as DV_GroupMPayable,
	sum(final.Clicks)                                                                                       as clicks,
	case when sum(final.dlvrImps) = 0 then 0
	else (sum(cast(final.Clicks as decimal(20,10)))/sum(cast(final.dlvrImps as decimal(20,10))))*100 end 	as CTR,
-- 	sum(final.View_Thru_Conv)                                                    							as View_Thru_Conv,
-- 	sum(final.Click_Thru_Conv)                                                   							as Click_Thru_Conv,
	sum(final.conv)                                                                                         as Transactions,
-- 	sum(final.View_Thru_Tickets)                                                  							as View_Thru_Tickets,
-- 	sum(final.Click_Thru_Tickets)                                                 							as Click_Thru_Tickets,
	sum(final.tickets)                                                                                      as Tickets,
	case when sum(final.conv) = 0 then 0
	else sum(final.cost)/sum(final.conv) end                                                            	as "Cost/Transaction",
-- 	sum(final.View_Thru_Revenue)                                                  							as View_Thru_Revenue,
-- 	sum(final.Click_Thru_Revenue)                                                 							as Click_Thru_Revenue,
	sum(final.Revenue)                                                                                      as Revenue,
	sum(final.viewRevenue)                                                                                  as "Billable Revenue",
	sum(final.adjsRevenue)                                                                                  as "Adjusted (Final) Revenue",
	case when sum(final.cost) = 0 then 0
	else ((sum(adjsRevenue)-sum(final.cost))/sum(final.cost)) end * 100                                 	as aROAS
from (

-- for running the code here instead of at "final," above

-- DECLARE @report_st date,
-- @report_ed date;
-- --
-- SET @report_ed = '2016-10-21';
-- SET @report_st = '2016-10-01';

	     select
		 	 -- DCM ad server date
		     cast(almost.dcmDate as date)                                               as dcmDate,
			 -- DCM ad server week (from date)
		     almost.dcmMonth                                                            as dcmMonth,

		     almost.diff                                                                as diff,
		     DV.JoinKey                                                                 as dvJoinKey,
		     MT.joinKey 																as mtJoinKey,
			 IV.joinKey 																as ivJoinKey,
		     almost.PackageCat                                                          as PackageCat,
		     almost.Cost_ID                                                             as Cost_ID,
		     almost.Buy                                                                 as Buy,
		     almost.order_id                                                            as order_id,
		     almost.campaignShort                                                       as campaignShort,
		     almost.campaignType                                                        as campaignType,
		     almost.Directory_Site                                                      as Directory_Site,
		     almost.Site_ID                                                             as Site_ID,
		     almost.CostMethod                                                          as CostMethod,
		     sum(1) over (partition by almost.Cost_ID,almost.dcmMatchDate order by
			     almost.dcmMonth asc range between unbounded preceding and current row) as newCount,
		     almost.PlacementNumber                                                     as PlacementNumber,
		     almost.Site_Placement                                                      as Site_Placement,
		     almost.page_id                                                             as page_id,
		     almost.PlacementEnd                                                        as PlacementEnd,
		     almost.PlacementStart                                                      as PlacementStart,
		     almost.DV_Map                                                              as DV_Map,
		     almost.Planned_Amt                                                         as Planned_Amt,
		     flat.flatcost                                                              as flatcost,
--  Logic excludes flat fees
		     case
-- 			 Click-based cost
		     when ((almost.DV_Map = 'N' or almost.DV_Map = 'Y') and (almost.edDate - almost.dcmMatchDate < 0 or almost.dcmMatchDate - almost.stDate < 0)
		           and (almost.CostMethod = 'CPM' or almost.CostMethod = 'CPMV' or almost.CostMethod = 'CPE' or almost.CostMethod = 'CPC' or
		                almost.CostMethod = 'CPCV'))
			       then 0
-- 			 Click source Innovid
			 when ((almost.DV_Map = 'Y' or almost.DV_Map = 'N') and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0)
		           and (almost.CostMethod = 'CPC' or almost.CostMethod = 'CPCV') and (len(ISNULL(IV.joinKey,''))>0))
			       then (sum(cast(IV.click_thrus as decimal(10,2))) * cast(almost.Rate as decimal(10,2)))
-- 			 Click source DCM
		     when ((almost.DV_Map = 'Y' or almost.DV_Map = 'N') and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0)
		           and (almost.CostMethod = 'CPC' or almost.CostMethod = 'CPCV'))
			       then (sum(cast(almost.Clicks as decimal(10,2))) * cast(almost.Rate as decimal(10,2)))

--           Impression-based cost; not subject to viewability; Innovid source
		     when (almost.DV_Map = 'N' and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0) and
		           (almost.CostMethod = 'CPM' or almost.CostMethod = 'CPMV' or almost.CostMethod = 'CPE') and (len(ISNULL(IV.joinKey,''))>0))
			       then ((sum(cast(IV.impressions as decimal(10,2))) * cast(almost.Rate as decimal(10,2)) / 1000))

--           Impression-based cost; not subject to viewability; DCM source
		     when (almost.DV_Map = 'N' and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0) and
		           (almost.CostMethod = 'CPM' or almost.CostMethod = 'CPMV' or almost.CostMethod = 'CPE'))
			       then ((sum(cast(almost.Impressions as decimal(10,2))) * cast(almost.Rate as decimal(10,2)) / 1000))

--           Impression-based cost; subject to viewability with flag; MT source
		     when (almost.DV_Map = 'Y' and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0) and
		           (almost.CostMethod = 'CPM' or almost.CostMethod = 'CPMV' or almost.CostMethod = 'CPE') and MT.joinKey is not null)
			       then ((sum(cast(MT.groupm_billable_impressions as decimal(10,2))) * cast(almost.Rate as decimal(10,2)) / 1000))

--           Impression-based cost; subject to viewability; DV source
		     when (almost.DV_Map = 'Y' and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0) and
		           (almost.CostMethod = 'CPM' or almost.CostMethod = 'CPMV' or almost.CostMethod = 'CPE'))
			       then ((sum(cast(DV.groupm_billable_impressions as decimal(10,2))) * cast(almost.Rate as decimal(10,2)) / 1000))

--           Impression-based cost; subject to viewability; MOAT source
		     when (almost.DV_Map = 'M' and (almost.edDate - almost.dcmMatchDate >= 0 or almost.dcmMatchDate - almost.stDate >= 0) and
		           (almost.CostMethod = 'CPM' or almost.CostMethod = 'CPMV' or almost.CostMethod = 'CPE'))
			       then ((sum(cast(MT.groupm_billable_impressions as decimal(10,2))) * cast(almost.Rate as decimal(10,2)) / 1000))

		     else 0 end                                                                 as Cost,
		     almost.Rate                                                                as Rate,
		     sum(almost.incrFlatCost)                                                   as incrFlatCost,
		     sum(case
		         -- 		not subject to viewability
		         when (almost.DV_Map = 'N')
			         then almost.View_Thru_Revenue

		         -- 		subject to viewability with flag; MT source
		         when (almost.DV_Map = 'Y' and (len(ISNULL(MT.joinKey,''))>0))
			         then (almost.View_Thru_Revenue) *
			              ((cast(MT.groupm_passed_impressions as decimal) /
			                nullif(cast(MT.total_impressions as decimal),0)))
		         -- 		subject to viewability; DV source
		         when (almost.DV_Map = 'Y')
			         then (almost.View_Thru_Revenue) *
			              ((cast(DV.groupm_passed_impressions as decimal) /
			                nullif(cast(DV.total_impressions as decimal),0)))

		         -- 		subject to viewability with flag; MT source
		         when (almost.DV_Map = 'Y' and (len(ISNULL(MT.joinKey,''))>0))
			         then (almost.View_Thru_Revenue) *
			              ((cast(MT.groupm_passed_impressions as decimal) /
			                nullif(cast(MT.total_impressions as decimal),0)))

		         -- 		subject to viewability; MOAT source
		         when (almost.DV_Map = 'M')
			         then (almost.View_Thru_Revenue) *
			              ((cast(MT.groupm_passed_impressions as decimal) /
			                nullif(cast(MT.total_impressions as decimal),0)))
		         else 0 end)                                                            as viewRevenue,


		     sum(case

-- 				 not subject to viewability
		         when (almost.DV_Map = 'N')
			         then cast(almost.View_Thru_Revenue * .2 * .15 as decimal(10,2))

-- 				 subject to viewability with flag; MT source
		         when (almost.DV_Map = 'Y' and (len(ISNULL(MT.joinKey,''))>0))
			         then cast(((almost.View_Thru_Revenue) *
			                    ((cast(MT.groupm_passed_impressions as decimal) /
			                      nullif(cast(MT.total_impressions as decimal),0)))) * .2 * .15 as decimal(10,2))

-- 				 subject to viewability; DV source
		         when (almost.DV_Map = 'Y')
			         then cast(((almost.View_Thru_Revenue) *
			                    ((cast(DV.groupm_passed_impressions as decimal) /
			                      nullif(cast(DV.total_impressions as decimal),0)))) * .2 * .15 as decimal(10,2))

-- 				 subject to viewability; MOAT source
		         when (almost.DV_Map = 'M')
			         then cast(((almost.View_Thru_Revenue) *
			                    ((cast(MT.groupm_passed_impressions as decimal) /
			                      nullif(cast(MT.total_impressions as decimal),0)))) * .2 * .15 as decimal(10,2))
		         else 0 end)                                                            as adjsRevenue,

-- 			 Total impressions as reported by 1.) DCM for "N," 2.) DV for "Y," or MOAT for "M"
		     sum(case
				 when almost.DV_Map = 'Y' and (len(ISNULL(MT.joinKey,''))>0) then MT.total_impressions
				 when almost.DV_Map = 'Y' then DV.total_impressions
		         when almost.DV_Map = 'M' then MT.total_impressions
				 when almost.DV_Map = 'N' and (len(ISNULL(IV.joinKey,''))>0) then IV.impressions
		         else almost.Impressions end)                                           as dlvrImps,

-- 			 Billable impressions as reported by 1.) DCM for "N," 2.) DV for "Y," or MOAT for "M"
		     sum(case
				 when almost.DV_Map = 'Y' and (len(ISNULL(MT.joinKey,''))>0) then MT.groupm_billable_impressions
				 when almost.DV_Map = 'Y' then DV.groupm_billable_impressions
		         when almost.DV_Map = 'M' then MT.groupm_billable_impressions
				 when almost.DV_Map = 'N' and (len(ISNULL(IV.joinKey,''))>0) then IV.impressions
		         else almost.Impressions end)                                           as billImps,

-- 			 DCM Impressions (for comparison (QA) to DCM console)
		     sum(almost.Impressions)                                                    as cnslImps,
			 sum(IV.impressions)														as IV_Impressions,
			 sum(IV.click_thrus)														as IV_Clicks,
		     sum(cast(DV.total_impressions as int))                                     as DV_Impressions,
		     sum(DV.groupm_passed_impressions)                                          as DV_Viewed,
		     sum(cast(DV.groupm_billable_impressions as decimal(10,2)))                 as DV_GroupMPayable,
		     sum(cast(MT.total_impressions as int))                                     as MT_Impressions,
		     sum(MT.groupm_passed_impressions)                                          as MT_Viewed,
		     sum(cast(MT.groupm_billable_impressions as decimal(10,2)))                 as MT_GroupMPayable,
-- 			 Clicks
		     sum(case
				 when (len(ISNULL(IV.joinKey,''))>0) then IV.click_thrus
		         else almost.Clicks end)                                           		as clicks,
		     sum(almost.View_Thru_Conv)                                                 as View_Thru_Conv,
		     sum(almost.Click_Thru_Conv)                                                as Click_Thru_Conv,
		     sum(almost.conv)                                                           as conv,
		     sum(almost.View_Thru_Tickets)                                              as View_Thru_Tickets,
		     sum(almost.Click_Thru_Tickets)                                             as Click_Thru_Tickets,
		     sum(almost.tickets)                                                        as tickets,
		     sum(almost.View_Thru_Revenue)                                              as View_Thru_Revenue,
		     sum(almost.Click_Thru_Revenue)                                             as Click_Thru_Revenue,
		     sum(almost.Revenue)                                                        as revenue

	     from
		     (
-- =========================================================================================================================
-- --
-- DECLARE @report_st date,
-- @report_ed date;
-- --
-- SET @report_ed = '2016-10-21';
-- SET @report_st = '2016-01-01';
			     select
				     dcmReport.dcmDate as dcmDate,
				     cast(month(cast(dcmReport.dcmDate as date)) as int) as dcmmonth,
				     case
				     when len(cast(month(cast(dcmReport.dcmDate as date)) as varchar(2))) = 1
					     then convert(int,
					                  cast(year(cast(dcmReport.dcmDate as date)) as varchar(4)) +
					                  cast(0 as varchar(1)) +
					                  cast(month(cast(dcmReport.dcmDate as date)) as varchar(2)) +
					                  right(cast(cast(dcmReport.dcmDate as date) as varchar(10)),2)
					     )
				     else
					     convert(int,cast(year(cast(dcmReport.dcmDate as date)) as varchar(4)) +
					                 cast(month(cast(dcmReport.dcmDate as date)) as varchar(2)) +
					                 right(cast(cast(dcmReport.dcmDate as date) as varchar(10)),2)
					     )
					 end                       as dcmMatchDate,
					 dcmReport.Buy             as Buy,
					 dcmReport.order_id        as order_id,
					 dcmReport.Directory_Site  as Directory_Site,
					 dcmReport.Site_ID         as Site_ID,
					 dcmReport.PlacementNumber as PlacementNumber,
					 dcmReport.Site_Placement  as Site_Placement,
					 dcmReport.page_id         as page_id,
					 Prisma.stDate             as stDate,
					 Prisma.edDate             as edDate,
					 Prisma.PackageCat         as PackageCat,
					 Prisma.CostMethod         as CostMethod,
					 Prisma.Cost_ID            as Cost_ID,
					 Prisma.Planned_Amt        as Planned_Amt,
					 Prisma.PlacementStart     as PlacementStart,
					 Prisma.PlacementEnd       as PlacementEnd,

--  			Flat.flatCostRemain                                                               AS flatCostRemain,
--  			Flat.impsRemain                                                                   AS impsRemain,
				     sum(( cast(dcmReport.Impressions as decimal(10,2)) / nullif(cast(Prisma.Planned_Amt as decimal(10,2)),0) ) * cast(Prisma.Rate as decimal(10,2)))                as incrFlatCost,
					 cast(Prisma.Rate as decimal(10,2))                                   as Rate,
					 sum(dcmReport.Impressions)                                           as impressions,
					 sum(dcmReport.Clicks)                                                as clicks,
					 sum(dcmReport.View_Thru_Conv)                                        as View_Thru_Conv,
					 sum(dcmReport.Click_Thru_Conv)                                       as Click_Thru_Conv,
					 sum(dcmReport.View_Thru_Conv) + sum(dcmReport.Click_Thru_Conv)       as conv,
					 sum(dcmReport.View_Thru_Tickets)                                     as View_Thru_Tickets,
					 sum(dcmReport.Click_Thru_Tickets)                                    as Click_Thru_Tickets,
					 sum(dcmReport.View_Thru_Tickets) + sum(dcmReport.Click_Thru_Tickets) as tickets,
					 sum(cast(dcmReport.View_Thru_Revenue as decimal(10,2)))              as View_Thru_Revenue,
					 sum(cast(dcmReport.Click_Thru_Revenue as decimal(10,2)))             as Click_Thru_Revenue,
					 sum(cast(dcmReport.Revenue as decimal(10,2)))                        as revenue,
				     case when cast(month(Prisma.PlacementEnd) as int) - cast(month(cast(dcmReport.dcmDate as date)) as int) <= 0 then 0
				     else cast(month(Prisma.PlacementEnd) as int) - cast(month(cast(dcmReport.dcmDate as date)) as int) end as diff,
				     case
					 when Prisma.CostMethod = 'Flat' or Prisma.CostMethod = 'CPC' or Prisma.CostMethod = 'CPCV' or Prisma.CostMethod = 'dCPM'
						 then 'N'
-- 					 Live Intent for SFO-SIN campaign is email (not subject to viewab.), but mistakenly labeled with "Y"
					 when dcmReport.order_id = '9923634' and dcmReport.Site_ID = '1853564'
					     then 'N'
-- 					 Corrections to SME placements
					 when dcmReport.order_id = '10090315' and (dcmReport.Site_ID = '1513807' or dcmReport.Site_ID = '1592652')
						 then 'Y'
-- 					 designates all Xaxis placements as "Y." Not always true.
-- 					  when dcmReport.Site_ID = '1592652'
-- 					     then 'Y'
-- 					 FlipBoard unable to implement MOAT tags; must bill off of DFA Impressions
					 when dcmReport.Site_ID = '2937979'
					     then 'N'
				     when dcmReport.order_id = '9639387'
					     then 'Y'
				     when dcmReport.order_id = '9973506'
					     then 'Y'
				     when Prisma.CostMethod = 'CPMV' and
				          ( dcmReport.Site_Placement like '%[Mm][Oo][Bb][Ii][Ll][Ee]%' or dcmReport.Site_Placement like '%[Vv][Ii][Dd][Ee][Oo]%' or dcmReport.Site_Placement like '%[Pp][Rr][Ee]%[Rr][Oo][Ll][Ll]%' or dcmReport.Site_ID = '1995643'
-- 							or dcmReport.Site_ID = '1474576'
							or dcmReport.Site_ID = '2854118')
					     then 'M'

				 	 when dcmReport.Site_Placement like '%_DV_%' then 'Y'
					 when dcmReport.Site_Placement like '%_MOAT_%' then 'M'
					 when dcmReport.Site_Placement like '%_NA_%' then 'N'
				     else Prisma.DV_Map end as DV_Map,
				     SUBSTRING(dcmReport.Site_Placement,( CHARINDEX(dcmReport.Site_Placement,'_UAC_') + 12 ),
				               3)                                                                                                                           as campaignShort,
				     case when SUBSTRING(dcmReport.Site_Placement,( CHARINDEX(dcmReport.Site_Placement,'_UAC_') + 12 ),3) =
				               'TMK'
					     then 'Acquisition'
				     else 'Non-Acquisition' end as campaignType



-- ==========================================================================================================================================================

-- openQuery call must not exceed 8,000 characters; no room for comments inside the function
		FROM (
			     SELECT *
			     FROM openQuery(VerticaGroupM,
			                    'SELECT
cast(Report.Date AS DATE)                   AS dcmDate,
cast(month(cast(Report.Date as date)) as int) as reportMonth,
Campaign.Buy                                AS Buy,
Report.order_id                               AS order_id,
Report.Site_ID as Site_ID,
Directory.Directory_Site                    AS Directory_Site,
	left(Placements.Site_Placement,6) as ''PlacementNumber'',
Placements.Site_Placement                   AS Site_Placement,
-- SPLIT_PART(Placements.Site_Placement, ''_'', 8) as tactic_1,
-- SPLIT_PART(Placements.Site_Placement, ''_'', 9) as tactic_2,
-- SPLIT_PART(Placements.Site_Placement, ''_'', 11) as size,
Report.page_id                         AS page_id,
sum(Report.Impressions)                     AS impressions,
sum(Report.Clicks)                          AS clicks,
sum(Report.View_Thru_Conv)                  AS View_Thru_Conv,
sum(Report.Click_Thru_Conv)                 AS Click_Thru_Conv,
sum(Report.View_Thru_Tickets)               AS View_Thru_Tickets,
sum(Report.Click_Thru_Tickets)              AS Click_Thru_Tickets,
sum(cast(Report.View_Thru_Revenue AS DECIMAL(10, 2)))                   AS View_Thru_Revenue,
sum(cast(Report.Click_Thru_Revenue AS DECIMAL(10, 2)))                   AS Click_Thru_Revenue,
sum(cast(Report.Revenue AS DECIMAL(10, 2))) AS revenue
from (
SELECT

cast(Conversions.Click_Time as date) as "Date"
,order_id                                                                       as order_id
,Conversions.site_id                                                                        as Site_ID
,Conversions.page_id                                                                        as page_id
,0                                                                                          as Impressions
,0                                                                                          as Clicks
,sum(Case When Event_ID = 1 THEN 1 ELSE 0 END)                                              as Click_Thru_Conv
,sum(Case When Event_ID = 1 Then Conversions.Quantity Else 0 End)                           as Click_Thru_Tickets
,sum(Case When Event_ID = 1 Then (Conversions.Revenue) / (Rates.exchange_rate) Else 0 End)  as Click_Thru_Revenue
,sum(Case When Event_ID = 2 THEN 1 ELSE 0 END)                                              as View_Thru_Conv
,sum(Case When Event_ID = 2 Then Conversions.Quantity Else 0 End)                           as View_Thru_Tickets
,sum(Case When Event_ID = 2 Then (Conversions.Revenue) / (Rates.exchange_rate) Else 0 End)  as View_Thru_Revenue
,sum(Conversions.Revenue/Rates.exchange_rate)                                               as Revenue

from
(
SELECT *
FROM mec.UnitedUS.dfa_activity
WHERE (cast(Click_Time as date) BETWEEN ''2016-01-01'' AND ''2016-10-21'')
and UPPER(SUBSTRING(Other_Data, (INSTR(Other_Data,''u3='')+3), 3)) != ''MIL''
and SUBSTRING(Other_Data, (INSTR(Other_Data,''u3='')+3), 5) != ''Miles''
and revenue != 0
and quantity != 0
AND (Activity_Type = ''ticke498'')
AND (Activity_Sub_Type = ''unite820'')

and order_id in (10307468, 9973506, 9923634, 9994694) -- Intl 2016
and (advertiser_id <> 0)
) as Conversions

LEFT JOIN mec.Cross_Client_Resources.EXCHANGE_RATES AS Rates
ON UPPER(SUBSTRING(Other_Data, (INSTR(Other_Data,''u3='')+3), 3)) = UPPER(Rates.Currency)
AND cast(Conversions.Click_Time as date) = Rates.DATE

GROUP BY
-- Conversions.Click_Time
cast(Conversions.Click_Time as date)
,Conversions.order_id
,Conversions.site_id
,Conversions.page_id


UNION ALL

SELECT
-- Impressions.impression_time as "Date"
cast(Impressions.impression_time as date) as "Date"
,Impressions.order_ID                 as order_id
,Impressions.Site_ID                  as Site_ID
,Impressions.Page_ID                  as page_id
,count(*)                             as Impressions
,0                                    as Clicks
,0                                    as Click_Thru_Conv
,0                                    as Click_Thru_Tickets
,0                                    as Click_Thru_Revenue
,0                                    as View_Thru_Conv
,0                                    as View_Thru_Tickets
,0                                    as View_Thru_Revenue
,0                                    as Revenue

FROM  (
SELECT *
FROM mec.UnitedUS.dfa_impression
WHERE cast(impression_time as date) BETWEEN ''2016-01-01'' AND ''2016-10-21''
and order_id in (10307468, 9973506, 9923634, 9994694) -- Intl 2016

and (advertiser_id <> 0)
) AS Impressions
GROUP BY
-- Impressions.impression_time
cast(Impressions.impression_time as date)
,Impressions.order_ID
,Impressions.Site_ID
,Impressions.Page_ID

UNION ALL
SELECT
-- Clicks.click_time       as "Date"
cast(Clicks.click_time as date)       as "Date"
,Clicks.order_id                      as order_id
,Clicks.Site_ID                       as Site_ID
,Clicks.Page_ID                       as page_id
,0                                    as Impressions
,count(*)                             as Clicks
,0                                    as Click_Thru_Conv
,0                                    as Click_Thru_Tickets
,0                                    as Click_Thru_Revenue
,0                                    as View_Thru_Conv
,0                                    as View_Thru_Tickets
,0                                    as View_Thru_Revenue
,0                                    as Revenue

FROM  (

SELECT *
FROM mec.UnitedUS.dfa_click
WHERE cast(click_time as date) BETWEEN ''2016-01-01'' AND ''2016-10-21''
and order_id in (10307468, 9973506, 9923634, 9994694) -- Intl 2016

and (advertiser_id <> 0)
) AS Clicks

GROUP BY
-- Clicks.Click_time
cast(Clicks.Click_time as date)
,Clicks.order_id
,Clicks.Site_ID
,Clicks.Page_ID
) as report
LEFT JOIN
(
-- 			     SELECT *
select cast(buy as varchar(4000)) as ''buy'', order_id as ''order_id''
from mec.UnitedUS.dfa_campaign
) AS Campaign
ON Report.order_id = Campaign.order_id

LEFT JOIN
(
-- 			     SELECT *
select  cast(site_placement as varchar(4000)) as ''site_placement'',  max(page_id) as ''page_id'', order_id as ''order_id'', site_id as ''site_id''
from mec.UnitedUS.dfa_page_name
		group by site_placement, order_id, site_id
) AS Placements
ON Report.page_id = Placements.page_id
AND Report.order_id = Placements.order_id
and report.site_ID  = placements.site_id

LEFT JOIN
(
-- 			     SELECT *
select cast(directory_site as varchar(4000)) as ''directory_site'', site_id as ''site_id''
from mec.UnitedUS.dfa_site
) AS Directory
ON Report.Site_ID = Directory.Site_ID

WHERE NOT REGEXP_LIKE(site_placement,''.do\s*not\s*use.'',''ib'')
and Report.Site_ID !=''1485655''
GROUP BY
cast(Report.Date AS DATE)
-- , cast(month(cast(Report.Date as date)) as int)
, Directory.Directory_Site
,Report.Site_ID
, Report.order_id
, Campaign.Buy
, Report.page_id
, Placements.Site_Placement
-- , Placements.PlacementNumber
')

		     ) AS dcmReport


			LEFT JOIN
			(
				SELECT *
				FROM [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.[dbo].summaryTable
			) AS Prisma
				ON dcmReport.page_id = Prisma.AdserverPlacementId

		group by
			dcmReport.dcmDate
			,cast(month(cast(dcmReport.dcmDate as date)) as int)
			,dcmReport.Buy
			,dcmReport.order_id
			,dcmReport.Directory_Site
			,dcmReport.Site_ID
			,dcmReport.PlacementNumber
			,dcmReport.Site_Placement
			,dcmReport.page_id
			,Prisma.PackageCat
			,Prisma.Rate
			,Prisma.CostMethod
			,Prisma.Cost_ID
			,Prisma.Planned_Amt
			,Prisma.PlacementEnd
			,Prisma.PlacementStart
			,Prisma.stDate
			,Prisma.edDate
-- 			,DV.joinKey
-- 			,Flat.flatCostRemain
-- 			,Flat.impsRemain
			,Prisma.DV_Map
	) as almost

	left join
	(
		select *
		from master.dbo.flatTableDay
	) as Flat
		on almost.Cost_ID = Flat.Cost_ID
		   and almost.dcmMatchDate = flat.dcmDate

-- DV Table JOIN ==============================================================================================================================================

	left join (
		          select *
		          from master.dbo.DVTable
		          where dvDate between @report_st and @report_ed
	          ) as DV
			on
			left(almost.Site_Placement,6) + '_' + replace(
			case	when ( almost.Directory_Site like '%[Cc]hicago%[Tt]ribune%' or almost.Directory_Site like '[Tt]ribune_[Ii]nteractive%' ) then 'ChicagoTribune'
				when ( almost.Directory_Site like '[Gg][Dd][Nn]%' or almost.Directory_Site like '[Gg]oogle_[Dd]isplay_[Nn]etwork%' ) then 'Google'
				when almost.Directory_Site like '%[Aa]dara%' then 'Adara'
				when almost.Directory_Site like '%[Aa]tlantic%' then 'The Atlantic'
				when almost.Directory_Site like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
				when almost.Directory_Site like '%[Cc][Nn][Nn]%' then 'CNN'
				when almost.Directory_Site like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
				when almost.Directory_Site like '%[Ff]orbes%' then 'Forbes'
				when almost.Directory_Site like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
				when almost.Directory_Site like '%[Jj]un%[Gg]roup%' then 'JunGroup'
				when almost.Directory_Site like '%[Mm][Ll][Bb]%' then 'MLB'
				when almost.Directory_Site like '%[Mm]ansueto%' then 'Inc'
				when almost.Directory_Site like '%[Mn][Ss][Nn]%' then 'MSN'
				when almost.Directory_Site like '%[Nn][Bb][Aa]%' then 'NBA'
				when almost.Directory_Site like '%[Nn][Ff][Ll]%' then 'NFL'
				when almost.Directory_Site like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
				when almost.Directory_Site like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
				when almost.Directory_Site like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
				when almost.Directory_Site like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
				when almost.Directory_Site like '%[Pp]riceline%' then 'Priceline'
				when almost.Directory_Site like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
				when almost.Directory_Site like '%[Tt]ap%[Aa]d%' then 'TapAd'
				when almost.Directory_Site like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
				when almost.Directory_Site like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
				when almost.Directory_Site like '%[Ww]all_[Ss]treet_[Jj]ournal%'  or almost.Directory_Site like '[Ww][Ss][Jj]' then 'Wall Street Journal'
				when almost.Directory_Site like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
				when almost.Directory_Site like '%[Yy]ahoo%' then 'Yahoo'
				when almost.Directory_Site like '%[Yy]ou%[Tt]ube%' then 'YouTube'
				when almost.Directory_Site like '[Aa]d[Pp]rime%' then 'AdPrime'
				when almost.Directory_Site like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
				when almost.Directory_Site like '[Aa]mobee%' then 'Amobee'
				when almost.Directory_Site like '[Cc]ardlytics%' then 'Cardlytics'
				when almost.Directory_Site like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
				when almost.Directory_Site like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
				when almost.Directory_Site like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
				when almost.Directory_Site like '[Ff]acebook%' then 'Facebook'
				when almost.Directory_Site like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
				when almost.Directory_Site like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
			    when almost.Directory_Site like '[Ff]lipboard%' then 'Flipboard'
				when almost.Directory_Site like '[Gg]um_[Gg]um%' then 'Gum Gum'
				when almost.Directory_Site like '[Hh]ulu%' then 'Hulu'
				when almost.Directory_Site like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
				when almost.Directory_Site like '[Ii][Nn][Cc]%' then 'Inc'
				when almost.Directory_Site like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
				when almost.Directory_Site like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
				when almost.Directory_Site like '[Ii]ndependent%' then 'Independent'
				when almost.Directory_Site like '[Kk]ayak%' then 'Kayak'
				when almost.Directory_Site like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
				when almost.Directory_Site like '[Mm]artini_[Mm]edia%' then 'Martini Media'
				when almost.Directory_Site like '[Oo]rbitz%' then 'Orbitz'
				when almost.Directory_Site like '[Ss]kyscanner%' then 'Skyscanner'
				when almost.Directory_Site like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
				when almost.Directory_Site like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
				when almost.Directory_Site like '[Ss]mithsonian%' then 'Smithsonian'
				when almost.Directory_Site like '[Ss]ojern%' then 'Sojern'
				when almost.Directory_Site like '[Ss]pecific_[Mm]edia%' or almost.Directory_Site like '[Vv]iant%' then 'Viant'
				when almost.Directory_Site like '[Ss]potify%' then 'Spotify'
				when almost.Directory_Site like '[Tt]ime%[Ii]nc%' then 'Time Inc'
				when almost.Directory_Site like '[Tt]ony%[As]wards%' then 'TonyAwards'
				when almost.Directory_Site like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
				when almost.Directory_Site like '[Tt]ravelocity%' then 'Travelocity'
				when almost.Directory_Site like '[Tt]riggit%' then 'Triggit'
				when almost.Directory_Site like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
				when almost.Directory_Site like '[Uu]ndertone%' then 'Undertone'
				when almost.Directory_Site like '[Uu]nited%' then 'United'
				when almost.Directory_Site like '[Vv]erve%' then 'VerveMobile'
				when almost.Directory_Site like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
				when almost.Directory_Site like '[Vv]ox%' then 'Vox'
				when almost.Directory_Site like '[Ww]ired%' then 'Wired'
				when almost.Directory_Site like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
				when almost.Directory_Site like '[Xx]ad%' then 'xAd Inc'
				when almost.Directory_Site like '[Yy]ieldbot%' then 'Yieldbot'
				when almost.Directory_Site like '[Yy]u[Mm]e%' then 'YuMe'
				else almost.Directory_Site end
				,' ','') + '_'
			+ cast(almost.dcmDate as varchar(10)) = DV.joinKey

-- MOAT Table JOIN ==============================================================================================================================================

	left join (
		          select *
		          from master.dbo.MTTable
		          where mtDate between @report_st and @report_ed
	          ) as MT
			on
			left(almost.Site_Placement,6) + '_' + replace(
				case	when ( almost.Directory_Site like '%[Cc]hicago%[Tt]ribune%' or almost.Directory_Site like '[Tt]ribune_[Ii]nteractive%' ) then 'ChicagoTribune'
				when ( almost.Directory_Site like '[Gg][Dd][Nn]%' or almost.Directory_Site like '[Gg]oogle_[Dd]isplay_[Nn]etwork%' ) then 'Google'
				when almost.Directory_Site like '%[Aa]dara%' then 'Adara'
				when almost.Directory_Site like '%[Aa]tlantic%' then 'The Atlantic'
				when almost.Directory_Site like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
				when almost.Directory_Site like '%[Cc][Nn][Nn]%' then 'CNN'
				when almost.Directory_Site like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
				when almost.Directory_Site like '%[Ff]orbes%' then 'Forbes'
				when almost.Directory_Site like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
				when almost.Directory_Site like '%[Jj]un%[Gg]roup%' then 'JunGroup'
				when almost.Directory_Site like '%[Mm][Ll][Bb]%' then 'MLB'
				when almost.Directory_Site like '%[Mm]ansueto%' then 'Inc'
				when almost.Directory_Site like '%[Mn][Ss][Nn]%' then 'MSN'
				when almost.Directory_Site like '%[Nn][Bb][Aa]%' then 'NBA'
				when almost.Directory_Site like '%[Nn][Ff][Ll]%' then 'NFL'
				when almost.Directory_Site like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
				when almost.Directory_Site like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
				when almost.Directory_Site like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
				when almost.Directory_Site like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
				when almost.Directory_Site like '%[Pp]riceline%' then 'Priceline'
				when almost.Directory_Site like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
				when almost.Directory_Site like '%[Tt]ap%[Aa]d%' then 'TapAd'
				when almost.Directory_Site like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
				when almost.Directory_Site like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
				when almost.Directory_Site like '%[Ww]all_[Ss]treet_[Jj]ournal%'  or almost.Directory_Site like '[Ww][Ss][Jj]' then 'Wall Street Journal'
				when almost.Directory_Site like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
				when almost.Directory_Site like '%[Yy]ahoo%' then 'Yahoo'
				when almost.Directory_Site like '%[Yy]ou%[Tt]ube%' then 'YouTube'
				when almost.Directory_Site like '[Aa]d[Pp]rime%' then 'AdPrime'
				when almost.Directory_Site like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
				when almost.Directory_Site like '[Aa]mobee%' then 'Amobee'
				when almost.Directory_Site like '[Cc]ardlytics%' then 'Cardlytics'
				when almost.Directory_Site like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
				when almost.Directory_Site like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
				when almost.Directory_Site like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
				when almost.Directory_Site like '[Ff]acebook%' then 'Facebook'
				when almost.Directory_Site like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
				when almost.Directory_Site like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
			    when almost.Directory_Site like '[Ff]lipboard%' then 'Flipboard'
				when almost.Directory_Site like '[Gg]um_[Gg]um%' then 'Gum Gum'
				when almost.Directory_Site like '[Hh]ulu%' then 'Hulu'
				when almost.Directory_Site like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
				when almost.Directory_Site like '[Ii][Nn][Cc]%' then 'Inc'
				when almost.Directory_Site like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
				when almost.Directory_Site like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
				when almost.Directory_Site like '[Ii]ndependent%' then 'Independent'
				when almost.Directory_Site like '[Kk]ayak%' then 'Kayak'
				when almost.Directory_Site like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
				when almost.Directory_Site like '[Mm]artini_[Mm]edia%' then 'Martini Media'
				when almost.Directory_Site like '[Oo]rbitz%' then 'Orbitz'
				when almost.Directory_Site like '[Ss]kyscanner%' then 'Skyscanner'
				when almost.Directory_Site like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
				when almost.Directory_Site like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
				when almost.Directory_Site like '[Ss]mithsonian%' then 'Smithsonian'
				when almost.Directory_Site like '[Ss]ojern%' then 'Sojern'
				when almost.Directory_Site like '[Ss]pecific_[Mm]edia%' or almost.Directory_Site like '[Vv]iant%' then 'Viant'
				when almost.Directory_Site like '[Ss]potify%' then 'Spotify'
				when almost.Directory_Site like '[Tt]ime%[Ii]nc%' then 'Time Inc'
				when almost.Directory_Site like '[Tt]ony%[As]wards%' then 'TonyAwards'
				when almost.Directory_Site like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
				when almost.Directory_Site like '[Tt]ravelocity%' then 'Travelocity'
				when almost.Directory_Site like '[Tt]riggit%' then 'Triggit'
				when almost.Directory_Site like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
				when almost.Directory_Site like '[Uu]ndertone%' then 'Undertone'
				when almost.Directory_Site like '[Uu]nited%' then 'United'
				when almost.Directory_Site like '[Vv]erve%' then 'VerveMobile'
				when almost.Directory_Site like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
				when almost.Directory_Site like '[Vv]ox%' then 'Vox'
				when almost.Directory_Site like '[Ww]ired%' then 'Wired'
				when almost.Directory_Site like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
				when almost.Directory_Site like '[Xx]ad%' then 'xAd Inc'
				when almost.Directory_Site like '[Yy]ieldbot%' then 'Yieldbot'
				when almost.Directory_Site like '[Yy]u[Mm]e%' then 'YuMe'
				else almost.Directory_Site end
				,' ','') + '_'
			+ cast(almost.dcmDate as varchar(10)) = MT.joinKey


-- Innovid Table JOIN ==============================================================================================================================================

	left join (
		          select *
		          from [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.[dbo].InnovidExtTable
		          where ivDate between @report_st and @report_ed
	          ) as IV
			on
			left(almost.Site_Placement,6) + '_' + replace(
			case	when ( almost.Directory_Site like '%[Cc]hicago%[Tt]ribune%' or almost.Directory_Site like '[Tt]ribune_[Ii]nteractive%' ) then 'ChicagoTribune'
				when ( almost.Directory_Site like '[Gg][Dd][Nn]%' or almost.Directory_Site like '[Gg]oogle_[Dd]isplay_[Nn]etwork%' ) then 'Google'
				when almost.Directory_Site like '%[Aa]dara%' then 'Adara'
				when almost.Directory_Site like '%[Aa]tlantic%' then 'The Atlantic'
				when almost.Directory_Site like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
				when almost.Directory_Site like '%[Cc][Nn][Nn]%' then 'CNN'
				when almost.Directory_Site like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
				when almost.Directory_Site like '%[Ff]orbes%' then 'Forbes'
				when almost.Directory_Site like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
				when almost.Directory_Site like '%[Jj]un%[Gg]roup%' then 'JunGroup'
				when almost.Directory_Site like '%[Mm][Ll][Bb]%' then 'MLB'
				when almost.Directory_Site like '%[Mm]ansueto%' then 'Inc'
				when almost.Directory_Site like '%[Mn][Ss][Nn]%' then 'MSN'
				when almost.Directory_Site like '%[Nn][Bb][Aa]%' then 'NBA'
				when almost.Directory_Site like '%[Nn][Ff][Ll]%' then 'NFL'
				when almost.Directory_Site like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
				when almost.Directory_Site like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
				when almost.Directory_Site like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
				when almost.Directory_Site like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
				when almost.Directory_Site like '%[Pp]riceline%' then 'Priceline'
				when almost.Directory_Site like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
				when almost.Directory_Site like '%[Tt]ap%[Aa]d%' then 'TapAd'
				when almost.Directory_Site like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
				when almost.Directory_Site like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
				when almost.Directory_Site like '%[Ww]all_[Ss]treet_[Jj]ournal%'  or almost.Directory_Site like '[Ww][Ss][Jj]' then 'Wall Street Journal'
				when almost.Directory_Site like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
				when almost.Directory_Site like '%[Yy]ahoo%' then 'Yahoo'
				when almost.Directory_Site like '%[Yy]ou%[Tt]ube%' then 'YouTube'
				when almost.Directory_Site like '[Aa]d[Pp]rime%' then 'AdPrime'
				when almost.Directory_Site like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
				when almost.Directory_Site like '[Aa]mobee%' then 'Amobee'
				when almost.Directory_Site like '[Cc]ardlytics%' then 'Cardlytics'
				when almost.Directory_Site like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
				when almost.Directory_Site like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
				when almost.Directory_Site like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
				when almost.Directory_Site like '[Ff]acebook%' then 'Facebook'
				when almost.Directory_Site like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
				when almost.Directory_Site like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
			    when almost.Directory_Site like '[Ff]lipboard%' then 'Flipboard'
				when almost.Directory_Site like '[Gg]um_[Gg]um%' then 'Gum Gum'
				when almost.Directory_Site like '[Hh]ulu%' then 'Hulu'
				when almost.Directory_Site like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
				when almost.Directory_Site like '[Ii][Nn][Cc]%' then 'Inc'
				when almost.Directory_Site like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
				when almost.Directory_Site like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
				when almost.Directory_Site like '[Ii]ndependent%' then 'Independent'
				when almost.Directory_Site like '[Kk]ayak%' then 'Kayak'
				when almost.Directory_Site like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
				when almost.Directory_Site like '[Mm]artini_[Mm]edia%' then 'Martini Media'
				when almost.Directory_Site like '[Oo]rbitz%' then 'Orbitz'
				when almost.Directory_Site like '[Ss]kyscanner%' then 'Skyscanner'
				when almost.Directory_Site like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
				when almost.Directory_Site like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
				when almost.Directory_Site like '[Ss]mithsonian%' then 'Smithsonian'
				when almost.Directory_Site like '[Ss]ojern%' then 'Sojern'
				when almost.Directory_Site like '[Ss]pecific_[Mm]edia%' or almost.Directory_Site like '[Vv]iant%' then 'Viant'
				when almost.Directory_Site like '[Ss]potify%' then 'Spotify'
				when almost.Directory_Site like '[Tt]ime%[Ii]nc%' then 'Time Inc'
				when almost.Directory_Site like '[Tt]ony%[As]wards%' then 'TonyAwards'
				when almost.Directory_Site like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
				when almost.Directory_Site like '[Tt]ravelocity%' then 'Travelocity'
				when almost.Directory_Site like '[Tt]riggit%' then 'Triggit'
				when almost.Directory_Site like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
				when almost.Directory_Site like '[Uu]ndertone%' then 'Undertone'
				when almost.Directory_Site like '[Uu]nited%' then 'United'
				when almost.Directory_Site like '[Vv]erve%' then 'VerveMobile'
				when almost.Directory_Site like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
				when almost.Directory_Site like '[Vv]ox%' then 'Vox'
				when almost.Directory_Site like '[Ww]ired%' then 'Wired'
				when almost.Directory_Site like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
				when almost.Directory_Site like '[Xx]ad%' then 'xAd Inc'
				when almost.Directory_Site like '[Yy]ieldbot%' then 'Yieldbot'
				when almost.Directory_Site like '[Yy]u[Mm]e%' then 'YuMe'
				else almost.Directory_Site end
				,' ','') + '_'
			+ cast(almost.dcmDate as varchar(10)) = IV.joinKey



-- 	where almost.CostMethod = 'Flat'
--     where almost.Site_ID ='1485655'
group by

	almost.Buy
	,almost.order_id
	,almost.Cost_ID
	,almost.DV_Map
	,almost.Directory_Site
	,almost.Site_ID
	,almost.PackageCat
	,almost.PlacementEnd
	,almost.PlacementStart
	,almost.PlacementNumber
	,almost.page_id
	,almost.Planned_Amt
	,almost.Rate
	,almost.Site_Placement
	,almost.edDate
	,almost.stDate
	,almost.campaignShort
	,almost.campaignType
	,almost.dcmDate
	,almost.dcmMatchDate
	,almost.dcmMonth
	,almost.diff
	,DV.JoinKey
	,MT.joinKey
	,IV.joinKey
-- ,almost.flatCostRemain
-- ,almost.impsRemain
	,almost.CostMethod
	,Flat.flatCostRemain
	,Flat.impsRemain
	,Flat.flatCost

     ) as final

-- 	where (final.dvJoinKey is null or final.mtJoinKey is NULL) and (final.DV_Map = 'Y' or final.DV_Map = 'M')
-- 	where (final.dvJoinKey is null and final.DV_Map = 'Y') or (final.mtJoinKey is NULL and final.DV_Map = 'M')
-- 		where final.mtJoinKey is NULL and  final.DV_Map = 'M'
-- 	where final.Directory_Site like '%[Bb]usiness%[Ii]nsider%'
-- 				where final.Directory_Site like '%Verve%' or final.Directory_Site like '%xAd%'
--     where final.Site_ID ='1853562'
--     where final.order_id ='10276123'

-- 	where final.Site_ID = '1853564' and final.DV_Map = 'Y'
-- 	where final.CostMethod = 'CPC'
	-- 	where final.CostMethod = 'Flat'


group by
	final.dcmDate
	,final.dcmMonth
	,final.diff
	,final.dvJoinKey
	,final.mtJoinKey
    ,final.ivJoinKey
	,final.PackageCat
	,final.Cost_ID
	,final.Buy
	,final.order_id
	,final.newCount
	,final.campaignShort
	,final.campaignType
	,final.Directory_Site
	,final.Site_ID
	,final.CostMethod
	,final.Rate
-- ,final.PlacementNumber
	,final.Site_Placement
	,final.page_id
	,final.PlacementEnd
	,final.PlacementStart
	,final.DV_Map
	,final.Planned_Amt
-- ,final.flatCostRemain
-- ,final.impsRemain

	,final.flatcost


order by
	final.Cost_ID,
	final.dcmDate;
