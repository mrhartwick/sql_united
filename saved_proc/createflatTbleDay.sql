ALTER PROCEDURE dbo.createflatTblDay
as
IF OBJECT_ID('master.dbo.flatTableDay',N'U') IS NOT NULL
	DROP TABLE master.dbo.flatTableDay;


CREATE TABLE master.dbo.flatTableDay
(
	Cost_ID        nvarchar(6)    NOT NULL,
	dcmDate 	   int 			  not null,
	dcmYrMo        int            NOT NULL,
	prsCostMethod  nvarchar(100)  NOT NULL,
	PackageCat     nvarchar(100)  NOT NULL,
	prsRate        decimal(20,10) NOT NULL,
	prsStYrMo      int            NOT NULL,
	prsEdYrMo      int            NOT NULL,
	prsStDate      int            NOT NULL,
	prsEdDate      int            NOT NULL,
	diff           bigint         NOT NULL,
	flatCost       decimal(20,10) NOT NULL,
	lagCost        decimal(20,10) NOT NULL,
	lagRemain      decimal(20,10) NOT NULL,
	flatCostRunTot decimal(20,10) NOT NULL,
	flatCostRemain decimal(20,10) NOT NULL,
	Imps           int            NOT NULL,
	impsRunTot     int            NOT NULL,
	impsRemain     int            NOT NULL,
	Planned_Amt    int            NOT NULL

);

INSERT INTO master.dbo.flatTableDay


	SELECT

		f2.Cost_ID                                                                                    as Cost_ID,
		f2.dcmDate                                                                                    as dcmDate,
		f2.dcmYrMo                                                                                    as dcmYrMo,
		f2.prsCostMethod                                                                              as prsCostMethod,
		f2.PackageCat                                                                                 as PackageCat,
		f2.prsRate                                                                                    as prsRate,
		f2.prsStYrMo                                                                                  as prsStYrMo,
		f2.prsEdYrMo                                                                                  as prsEdYrMo,
		f2.stDate                                                                                     as prsStDate,
		f2.edDate                                                                                     as prsEdDate,
		isNull(f2.diff,  cast(0 as int))                             								  as diff,
		CASE WHEN f2.dcmDate = f2.stDate AND f2.stDate = f2.edDate THEN f2.flatCost
		WHEN f2.diff > 0 AND f2.flatCost < f2.prsRate AND f2.impsRunTot < f2.Planned_Amt THEN f2.flatCost
		WHEN f2.diff > 0 AND f2.lagCost = 0 AND f2.lagRemain = 0 THEN f2.flatCost
		WHEN f2.diff > 0 AND f2.flatCostRemain = 0 AND f2.lagRemain > 0 THEN f2.lagRemain
		WHEN f2.flatCost = 0 THEN 0
		WHEN f2.flatCost > f2.lagRemain THEN f2.lagRemain
		ELSE f2.flatCost - f2.lagRemain END                                                           as flatCost,
		isNull(f2.lagCost,  cast(0 as decimal(20,10))) 			       								  as lagCost,
		isNull(f2.lagRemain,  cast(0 as decimal(20,10))) 		     								  as lagRemain,
		isNull(f2.flatCostRunTot,  cast(0 as decimal(20,10))) 										  as flatCostRunTot,
		isNull(f2.flatCostRemain,  cast(0 as decimal(20,10))) 										  as flatCostRemain,
		f2.Imps                                                                                       as Imps,
		f2.impsRunTot                                                                                 as impsRunTot,
        isNull(f2.impsRemain,  cast(0 as int))  													  as impsRemain,
        isNull(f2.Planned_Amt,  cast(0 as int)) 													  as Planned_Amt

	FROM (
		     SELECT
			     f1.stDate                                                                                as stDate,
			     f1.edDate                                                                                as edDate,
			     f1.dcmDate                                                                               as dcmDate,
			  	 f1.Cost_ID                                                                               as Cost_ID,
			     f1.dcmYrMo                                                                               as dcmYrMo,
			     f1.prsCostMethod                                                                         as prsCostMethod,
			     f1.PackageCat                                                                            as PackageCat,
			     f1.prsRate                                                                               as prsRate,
			     f1.prsStYrMo                                                                             as prsStYrMo,
			     f1.prsEdYrMo                                                                             as prsEdYrMo,
			     isNull(f1.diff,cast(0 as decimal(20,10)))                                                as diff,
			     CASE WHEN f1.flatCost IS NULL THEN cast(0 as decimal(20,10))
			     WHEN f1.diff > 0 AND f1.Imps > f1.Planned_Amt THEN f1.flatCost - f1.lagCost
			     ELSE f1.flatCost END                                                                     as flatCost,
			     f1.lagCost                                                                               as lagCost,
			     lag(f1.flatCostRemain,1,0) OVER (PARTITION BY f1.Cost_ID ORDER BY
				     f1.dcmYrMo)                                                                          as lagRemain,
			     f1.flatCostRunTot                                                                        as flatCostRunTot,
			     isNull(f1.flatCostRemain, cast(0 as decimal(20,10))) 									  as flatCostRemain,
			     f1.Imps                                                                                  as Imps,
			     f1.impsRunTot                                                                            as impsRunTot,
			     f1.impsRemain                                                                            as impsRemain,
			     f1.Planned_Amt                                                                           as Planned_Amt
		     FROM (
			          SELECT
				          almost.stDate                                                                       as stDate,
				          almost.edDate                                                                       as edDate,
				          almost.dcmDate                                                                      as dcmDate,
				          almost.Cost_ID                                                                      as Cost_ID,
				          almost.dcmYrMo                                                                      as dcmYrMo,
				          almost.Cost_Method                                                                  as prsCostMethod,
				          almost.PackageCat                                                                   as PackageCat,
				          almost.Rate                                                                         as prsRate,
				          almost.stYrMo                                                                       as prsStYrMo,
				          almost.edYrMo                                                                       as prsEdYrMo,
						  isNull(almost.ed_diff, cast(0 as decimal(20,10)))   								  as diff,
				          sum(
					          almost.flatCost) OVER (PARTITION BY almost.Cost_ID,almost.dcmDate)              as flatCost,
				          lag(almost.flatcost,1,0) OVER (PARTITION BY almost.Cost_ID ORDER BY
					          almost.dcmDate)                                                                 as lagCost,
				          sum(almost.flatCost) OVER (PARTITION BY almost.Cost_ID ORDER BY
					          almost.dcmDate ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           as flatCostRunTot,
				          CASE WHEN ( cast(almost.Rate as decimal) -
				                      sum(almost.flatCost) OVER (PARTITION BY almost.Cost_ID    ORDER BY
					                      almost.dcmDate ASC) ) <=
				                    0 THEN 0 ELSE ( cast(almost.Rate as decimal) - sum(
					          almost.flatCost) OVER (PARTITION BY almost.Cost_ID    ORDER BY
					          almost.dcmDate ASC) ) END                                                       as flatCostRemain,
				          sum(
					          almost.impressions) OVER (PARTITION BY almost.Cost_ID,almost.dcmDate)           as Imps,
				          sum(almost.impressions) OVER (PARTITION BY almost.Cost_ID    ORDER BY
					          almost.dcmDate ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           as impsRunTot,
				          CASE WHEN ( cast(almost.Planned_Amt as decimal) -
				                      sum(almost.impressions) OVER (PARTITION BY almost.Cost_ID ORDER BY
					                      almost.dcmDate ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ) <=
				                    0 THEN 0 ELSE (
					          cast(almost.Planned_Amt as decimal) -
					          sum(almost.impressions) OVER (PARTITION BY almost.Cost_ID ORDER BY
						          almost.dcmDate ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ) END as impsRemain,
				          almost.Planned_Amt                                                                  as Planned_Amt
			          FROM

				          (
					          SELECT
						          finalish.dcmmonth                                            as dcmmonth,
						          finalish.dcmDate                                             as dcmDate,
						          finalish.dcmyear                                             as dcmyear,
						          finalish.dcmYrMo                                             as dcmYrMo,
						          finalish.Cost_ID                                             as Cost_ID,
						          finalish.cost_method                                         as cost_method,
						          CASE
						          WHEN finalish.dcmDate - finalish.stDate < 0
							          THEN 0
						          WHEN finalish.edDate = finalish.dcmDate AND finalish.edDate = finalish.dcmDate
							          THEN finalish.rate
						          WHEN ( finalish.edDate - finalish.dcmDate ) > 0 AND sum(finalish.impressions) > finalish.planned_amt
							          THEN finalish.rate

						          WHEN ( finalish.edDate - finalish.dcmDate ) > 0
							          THEN sum(
								          ( cast(finalish.impressions as decimal(20,10)) /
								            NULLIF(cast(finalish.planned_amt as decimal(20,10)),0) ) *
								          cast(finalish.rate as decimal(20,10)))
						          WHEN ( finalish.edDate - finalish.dcmDate ) = 0
							          THEN finalish.rate
						          WHEN ( finalish.edDate - finalish.dcmDate ) < 0
							          THEN 0
						          ELSE 0 END                                                   as flatCost,
						          finalish.rate                                                as rate,
						          finalish.PackageCat                                          as PackageCat,
						          finalish.stYrMo                                              as stYrMo,
						          finalish.edYrMo                                              as edYrMo,
						          finalish.stDate                                              as stDate,
						          finalish.edDate                                              as edDate,
						          cast(finalish.edDate as int) - cast(finalish.dcmDate as int) as ed_diff,
						          finalish.planned_amt                                         as planned_amt,
						          sum(finalish.impressions)                                    as impressions
					          FROM
						          (
							          SELECT
								          Prisma.adserverPlacementID             as adserverPlacementID,
								 dcmReport.PlacementNumber              as PlacementNumber,
								          dcmReport.site_placement               as site_placement,
								          MONTH(cast(dcmReport.dcmDate as date)) as dcmMonth,
								          YEAR(cast(dcmReport.dcmDate as date))  as dcmYear,
								          CASE
								          WHEN len(cast(MONTH(cast(dcmReport.dcmDate as date)) as varchar(2))) = 1
									          THEN CONVERT(int,
									                       CAST(YEAR(CAST(dcmReport.dcmDate as date)) as varchar(4)) +
									                       cast(0 as varchar(1)) +
									                       CAST(MONTH(CAST(dcmReport.dcmDate as date)) as varchar(2)))
								          ELSE
									          CONVERT(int,CAST(YEAR(CAST(dcmReport.dcmDate as date)) as varchar(4)) +
									                      CAST(MONTH(CAST(dcmReport.dcmDate as date)) as varchar(2)))
								          END                                    as dcmYrMo,

								          CASE
								          WHEN len(cast(MONTH(cast(dcmReport.dcmDate as date)) as varchar(2))) = 1
									          THEN CONVERT(int,
									                       CAST(YEAR(CAST(dcmReport.dcmDate as date)) as varchar(4)) +
									                       cast(0 as varchar(1)) +
									                       CAST(MONTH(CAST(dcmReport.dcmDate as date)) as varchar(2)) +
									                       RIGHT(CAST(CAST(dcmReport.dcmDate as date) as varchar(10)),2)
									          )
								          ELSE
									          CONVERT(int,CAST(YEAR(CAST(dcmReport.dcmDate as date)) as varchar(4)) +
									                      CAST(MONTH(CAST(dcmReport.dcmDate as date)) as varchar(2)) +
									                      RIGHT(CAST(CAST(dcmReport.dcmDate as date) as varchar(10)),2)
									          )
								          END               as dcmDate,

								          dcmReport.directory_site,
								          Prisma.CostMethod                      as Cost_Method,
								          Prisma.Cost_ID                         as Cost_ID,
								          Prisma.Rate                            as Rate,
								          Prisma.PackageCat                      as PackageCat,
								          Prisma.stYrMo                          as stYrMo,
								          Prisma.edYrMo                          as edYrMo,
								          CASE WHEN len(cast(month(cast(Prisma.PlacementStart as date)) as varchar(2))) = 1
									          THEN cast(CAST(year(CAST(Prisma.PlacementStart as date)) as varchar(4)) + cast(0 as varchar(1)) +
									                    CAST(MONTH(CAST(Prisma.PlacementStart as date)) as varchar(2)) +
									                    RIGHT(CAST(CAST(Prisma.PlacementStart as date) as varchar(10)),2)
									                    as
									                    int)
								          ELSE
									          cast(CAST(YEAR(CAST(Prisma.PlacementStart as date)) as varchar(4)) +
									               CAST(MONTH(CAST(Prisma.PlacementStart as date)) as varchar(2)) +
									               RIGHT(CAST(CAST(Prisma.PlacementStart as date) as varchar(10)),2)
									               as int)
								          END                                    as stDate,
								          CASE WHEN len(cast(month(cast(Prisma.PlacementEnd as date)) as varchar(2))) = 1
									          THEN cast(CAST(year(CAST(Prisma.PlacementEnd as date)) as varchar(4)) + cast(0 as varchar(1)) +
									                    CAST(MONTH(CAST(Prisma.PlacementEnd as date)) as varchar(2)) +
									                    RIGHT(CAST(CAST(Prisma.PlacementEnd as date) as varchar(10)),2)
									                    as
									                    int)
								          ELSE
									          cast(CAST(YEAR(CAST(Prisma.PlacementEnd as date)) as varchar(4)) +
									               CAST(MONTH(CAST(Prisma.PlacementEnd as date)) as varchar(2)) +
									               RIGHT(CAST(CAST(Prisma.PlacementEnd as date) as varchar(10)),2)
									               as int)
								          END                                    as edDate,

								      SUM(dcmReport.Impressions)             as impressions,
								          SUM(dcmReport.clicks)                  as clicks,
								          SUM(dcmReport.tickets)                 as tickets,
								          Prisma.Planned_Amt                     as Planned_Amt

					     FROM (
--
						          SELECT *
						          FROM openQuery(VerticaGroupM,
			                    '
SELECT

cast(Report.Date as DATE)                   as dcmDate,
cast(month(cast(Report.Date as date)) as int) as reportMonth,
Campaign.Buy                                as Buy,
Report.Buy_ID                               as Buy_ID,
Directory.Directory_Site                    as Directory_Site,
Placements.PlacementNumber                  as PlacementNumber,
Placements.Site_Placement                   as Site_Placement,
Report.Placement_ID                         as Placement_ID,
sum(Report.Impressions)                     as impressions,
sum(Report.Clicks)                          as clicks,
sum(Report.View_Thru_Conv)                  as View_Thru_Conv,
sum(Report.Click_Thru_Conv)                 as Click_Thru_Conv,
sum(Report.View_Thru_Tickets) + sum(Report.Click_Thru_Tickets) as tickets,
sum(Report.View_Thru_Tickets)               as View_Thru_Tickets,
sum(Report.Click_Thru_Tickets)              as Click_Thru_Tickets,
sum(cast(Report.View_Thru_Revenue as DECIMAL(10, 2)))                   as View_Thru_Revenue,
sum(cast(Report.Click_Thru_Revenue as DECIMAL(10, 2)))                   as Click_Thru_Revenue,
sum(cast(Report.Revenue as DECIMAL(10, 2))) as revenue

from ( select
cast(Conversions.Click_Time as date) as "Date"
,order_id                                                                       as Buy_id
,Conversions.site_id      as Site_ID
,Conversions.page_id                                                                        as Placement_ID
,0                                                                                          as Impressions
,0                                                                                          as Clicks
,sum(Case When Event_ID = 1 THEN 1 ELSE 0 END)                                              as Click_Thru_Conv
,sum(Case When Event_ID = 1 Then Conversions.Quantity Else 0 End)                           as Click_Thru_Tickets
,sum(Case When Event_ID = 1 Then (Conversions.Revenue) / (Rates.exchange_rate) Else 0 End)  as Click_Thru_Revenue
,sum(Case When Event_ID = 2 THEN 1 ELSE 0 END)                as View_Thru_Conv
,sum(Case When Event_ID = 2 Then Conversions.Quantity Else 0 End)                           as View_Thru_Tickets
,sum(Case When Event_ID = 2 Then (Conversions.Revenue) / (Rates.exchange_rate) Else 0 End)  as View_Thru_Revenue
,sum(Conversions.Revenue/Rates.exchange_rate)                                               as Revenue

from
(
SELECT *
FROM mec.UnitedUS.dfa_activity
WHERE (cast(click_time as date) > ''2016-01-01'' )
-- 								 and UPPER(SUBSTRING(Other_Data, (INSTR(Other_Data,''u3='')+3), 3)) != ''MIL''
-- 								 and SUBSTRING(Other_Data, (INSTR(Other_Data,''u3='')+3), 5) != ''Miles''
AND (Activity_Type = ''ticke498'')
AND (Activity_Sub_Type = ''unite820'')
and (advertiser_id <> 0)
)
as Conversions

LEFT JOIN mec.Cross_Client_Resources.EXCHANGE_RATES as Rates
ON UPPER(SUBSTRING(Other_Data, (INSTR(Other_Data,''u3='')+3), 3)) = UPPER(Rates.Currency)
AND cast(Conversions.Click_Time as date) = Rates.DATE

GROUP BY
cast(Conversions.Click_Time as date)
,Conversions.order_id
,Conversions.site_id
,Conversions.page_id
UNION ALL

SELECT
cast(Impressions.impression_time as date) as "Date"
,Impressions.order_ID                 as Buy_id
,Impressions.Site_ID                  as Site_ID
,Impressions.Page_ID                  as Placement_ID
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
WHERE cast(impression_time as date) > ''2016-01-01''

) as Impressions
GROUP BY
cast(Impressions.impression_time as date)
,Impressions.order_ID
,Impressions.Site_ID
,Impressions.Page_ID

UNION ALL
SELECT
cast(Clicks.click_time as date)       as "Date"
,Clicks.order_id                      as Buy_id
,Clicks.Site_ID                       as Site_ID
,Clicks.Page_ID                       as Placement_ID
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
WHERE cast(click_time as date) > ''2016-01-01''
) as Clicks

GROUP BY
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
) as Campaign
ON Report.buy_id = Campaign.order_id

LEFT JOIN
(
-- 			     SELECT *
select cast(site_placement as varchar(4000)) as ''site_placement'', left(cast(site_placement as varchar(4000)),6) as ''PlacementNumber'', max(page_id) as ''page_id'', order_id as ''order_id''
from mec.UnitedUS.dfa_page
		group by site_placement, order_id, site_id
) as Placements
ON Report.Placement_ID = Placements.page_id
AND Report.buy_id = Placements.order_id

LEFT JOIN
(
-- 			     SELECT *
select cast(directory_site as varchar(4000)) as ''directory_site'', site_id as ''site_id''
from mec.UnitedUS.dfa_site
) as Directory
ON Report.Site_ID = Directory.Site_ID

WHERE NOT REGEXP_LIKE(Placements.site_placement,''.do\s*not\s*use.'',''ib'')


GROUP BY
cast(Report.Date as DATE)
, cast(month(cast(Report.Date as date)) as int)
, Directory.Directory_Site
, Report.Buy_ID
, Campaign.Buy
, Report.Placement_ID
, Placements.Site_Placement
, Placements.PlacementNumber

											  ')

					          ) as dcmReport
-- =========================================================================================================================
						     LEFT JOIN
						     (
							     SELECT *
							     FROM [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.[dbo].summaryTable
						     ) as Prisma
							     ON dcmReport.Placement_ID = Prisma.AdserverPlacementId
					     WHERE Prisma.CostMethod = 'Flat'


-- =========================================================================================================================
					     GROUP BY
						     cast(dcmReport.dcmDate as date)
						     ,dcmReport.PlacementNumber
						     ,CONVERT(int,cast(YEAR(cast(dcmReport.dcmDate as date)) as varchar(4)) +
						                  SUBSTRING(( CONVERT(varchar(10),cast(dcmReport.dcmDate as date)) ),
						                            ( CHARINDEX('-',
						                                        CONVERT(varchar(10),cast(dcmReport.dcmDate as date))) +
						                              1 ),2))
						     ,Prisma.CostMethod
						     ,Prisma.stYrMo
						     ,Prisma.edYrMo
						     ,Prisma.PlacementStart
						     ,Prisma.PlacementEnd
						     ,Prisma.Rate
						     ,Prisma.Planned_Amt
						     ,Prisma.PackageCat
						     ,Prisma.adserverPlacementID
						     ,dcmReport.directory_site
						     ,dcmReport.site_placement
						     ,Prisma.Cost_ID
						          ) as finalish

					          GROUP BY
						          finalish.dcmmonth,
						          finalish.dcmyear,
						          finalish.dcmYrMo,
						          finalish.PackageCat,
						          finalish.cost_method,
						          finalish.edDate,
						          finalish.stDate,
						          finalish.dcmDate,
						          finalish.rate,
						          finalish.stYrMo,
						          finalish.edYrMo,
						          finalish.Cost_ID,
						          finalish.planned_amt
		     ) as almost
     ) as f1
) as f2
go
