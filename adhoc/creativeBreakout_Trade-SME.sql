DECLARE @report_st date
DECLARE @report_ed date
--
SET @report_ed = '2016-11-25'
SET @report_st = '2016-01-01'
			     			     select
				     dcmReport.dcmDate as dcmDate,
				     cast(month(cast(dcmReport.dcmDate as date)) as int) as dcmmonth,
				     [dbo].udf_dateToInt(dcmReport.dcmDate) as dcmMatchDate,
					 dcmReport.Buy             as Buy,
					 dcmReport.order_id        as order_id,
					 dcmReport.Directory_Site  as Directory_Site,
					 dcmReport.Site_ID         as Site_ID,
					 case when dcmReport.PlacementNumber in('PBKB7J', 'PBKB7H', 'PBKB7K') then 'PBKB7J' end as PlacementNumber,

					 case when dcmReport.Site_Placement like 'PBKB7J%' or dcmReport.Site_Placement like 'PBKB7H%' or dcmReport.Site_Placement like 'PBKB7K%' or dcmReport.Site_Placement ='United 360 - Polaris 2016 - Q4 - Amobee'        then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID' else  dcmReport.Site_Placement end     as Site_Placement,
									dcmReport.creative as creative,
-- 				Amobee Video 360 placements, tracked differently across DCM, Innovid, and MOAT; this combines the three placements into one
-- 					case when dcmReport.page_id in (137412510, 137412401, 137412609) then 137412609 else dcmReport.page_id end as page_id,
-- 					 Prisma.stDate             as stDate,
-- -- 					 correction for SFO-SIN campaign end date
-- 					 case when dcmReport.order_id = 9923634 and dcmReport.Site_ID != 1190258 then 20161022 else
-- 					 Prisma.edDate end         as edDate,
-- 					 Prisma.PackageCat         as PackageCat,
-- 					 Prisma.CostMethod         as CostMethod,
-- 					 Prisma.Cost_ID            as Cost_ID,
-- 					 Prisma.Planned_Amt        as Planned_Amt,
-- -- 					 Prisma.Planned_Cost       as Planned_Cost,
-- 					 Prisma.PlacementStart     as PlacementStart,
-- 					 case when dcmReport.order_id = 9923634 and dcmReport.Site_ID != 1190258 then '2016-10-22' else
-- 					 Prisma.PlacementEnd end   as PlacementEnd,

--  			Flat.flatCostRemain                                                               AS flatCostRemain,
--  			Flat.impsRemain                                                                   AS impsRemain,
-- 				     sum(( cast(dcmReport.Impressions as decimal(10,2)) / nullif(cast(Prisma.Planned_Amt as decimal(10,2)),0) ) * cast(Prisma.Rate as decimal(10,2)))                as incrFlatCost,
-- 					 cast(Prisma.Rate as decimal(10,2))                                   as Rate,
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
-- 				     case when cast(month(Prisma.PlacementEnd) as int) - cast(month(cast(dcmReport.dcmDate as date)) as int) <= 0 then 0
-- 				     else cast(month(Prisma.PlacementEnd) as int) - cast(month(cast(dcmReport.dcmDate as date)) as int) end as diff,
--
--
-- 					 case
-- 					 when Prisma.CostMethod = 'Flat' or Prisma.CostMethod = 'CPC' or Prisma.CostMethod = 'CPCV' or Prisma.CostMethod = 'dCPM'
-- 						 then 'N'
--
-- -- 					 Live Intent for SFO-SIN campaign is email (not subject to viewab.), but mistakenly labeled with "Y"
-- 					 when
-- 						 	 dcmReport.order_id = '9923634' -- SFO-SIN
-- 						 and dcmReport.Site_ID = '1853564' -- Live Intent
-- 					     then 'N'
--
-- -- 					 Corrections to SME placements
-- 					 when dcmReport.order_id = '10090315' and (dcmReport.Site_ID = '1513807' or dcmReport.Site_ID = '1592652')
-- 						 then 'Y'
--
-- -- 					 Corrections to SFO-SIN placements
-- 					 when dcmReport.order_id = '9923634' and dcmReport.Site_ID = '1534879' and Prisma.CostMethod = 'CPM'
-- 						 then 'N'
-- -- 					 designates all Xaxis placements as "Y." Not always true.
-- -- 					  when dcmReport.Site_ID = '1592652' then 'Y'
--
-- -- 					 FlipBoard unable to implement MOAT tags; must bill off of DFA Impressions
-- 					 when dcmReport.Site_ID = '2937979' then 'N'
-- -- 					 All Targeted Marketing subject
-- 					 when dcmReport.order_id = '9639387' then 'Y'
--
-- -- 					 Designates all SFO-AKL placements as "Y." Not always true. Apparently.
-- -- 				     when dcmReport.order_id = '9973506' then 'Y'
--
-- 				     when Prisma.CostMethod = 'CPMV' and
-- 				          ( dcmReport.Site_Placement like '%[Mm][Oo][Bb][Ii][Ll][Ee]%' or dcmReport.Site_Placement like '%[Vv][Ii][Dd][Ee][Oo]%' or dcmReport.Site_Placement like '%[Pp][Rr][Ee]%[Rr][Oo][Ll][Ll]%' or dcmReport.Site_ID = '1995643'
-- -- 							or dcmReport.Site_ID = '1474576'
-- 							or dcmReport.Site_ID = '2854118')
-- 					     then 'M'
-- -- 					 Look for viewability flags Investment began including in placement names 6/16.
-- 				 	 when dcmReport.Site_Placement like '%[_]DV[_]%' then 'Y'
-- 					 when dcmReport.Site_Placement like '%[_]MOAT[_]%' then 'M'
-- 					 when dcmReport.Site_Placement like '%[_]NA[_]%' then 'N'
-- --
-- 					 when Prisma.CostMethod = 'CPMV' and Prisma.DV_Map = 'N' then 'Y'
-- 				     else Prisma.DV_Map end as DV_Map,

-- 					 Prisma.DV_Map as DV_Map,
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
creative.creative                                AS creative,
report.creative_id                                AS creative_id,
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
,Conversions.creative_id                                                                        as creative_id
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
WHERE (cast(Click_Time as date) BETWEEN ''2016-01-01'' AND ''2016-11-25'')
and UPPER(SUBSTRING(Other_Data, (INSTR(Other_Data,''u3='')+3), 3)) != ''MIL''
and SUBSTRING(Other_Data, (INSTR(Other_Data,''u3='')+3), 5) != ''Miles''
and revenue != 0
and quantity != 0
AND (Activity_Type = ''ticke498'')
AND (Activity_Sub_Type = ''unite820'')
-- Intl 2016
and order_id in (
9304728,
10090315
)
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
,Conversions.creative_id

UNION ALL

SELECT
-- Impressions.impression_time as "Date"
cast(Impressions.impression_time as date) as "Date"
,Impressions.order_ID                 as order_id
,Impressions.Site_ID                  as Site_ID
,Impressions.Page_ID                  as page_id
,Impressions.creative_id                                                                        as creative_id
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
WHERE cast(impression_time as date) BETWEEN ''2016-01-01'' AND ''2016-11-25''
-- Intl 2016
and order_id in (
9304728,
10090315
)

and (advertiser_id <> 0)
) AS Impressions
GROUP BY
-- Impressions.impression_time
cast(Impressions.impression_time as date)
,Impressions.order_ID
,Impressions.Site_ID
,Impressions.Page_ID
,Impressions.creative_id
UNION ALL
SELECT
-- Clicks.click_time       as "Date"
cast(Clicks.click_time as date)       as "Date"
,Clicks.order_id                      as order_id
,Clicks.Site_ID                       as Site_ID
,Clicks.Page_ID                       as page_id
,Clicks.creative_id                                                                        as creative_id
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
WHERE cast(click_time as date) BETWEEN ''2016-01-01'' AND ''2016-11-25''
-- Intl 2016
and order_id in (
9304728,
10090315
)

and (advertiser_id <> 0)
) AS Clicks

GROUP BY
-- Clicks.Click_time
cast(Clicks.Click_time as date)
,Clicks.order_id
,Clicks.Site_ID
,Clicks.Page_ID
,Clicks.creative_id
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
select cast(creative as varchar(4000)) as ''creative'', creative_id as ''creative_id''
from mec.UnitedUS.dfa_creative
) AS creative
ON Report.creative_id = creative.creative_id
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
, report.creative_id
, creative.creative
, Report.page_id
, Placements.Site_Placement
-- , Placements.PlacementNumber
')

		     ) AS dcmReport


-- 			LEFT JOIN
-- 			(
-- 				SELECT *
-- 				FROM [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.[dbo].summaryTable
-- 			) AS Prisma
-- 				ON dcmReport.page_id = Prisma.AdserverPlacementId

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
				 ,dcmReport.creative
