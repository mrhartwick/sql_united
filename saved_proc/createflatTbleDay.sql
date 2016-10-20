ALTER PROCEDURE dbo.createflatTblDay
AS
IF OBJECT_ID('master.dbo.flatTableDay',N'U') IS NOT NULL
	DROP TABLE master.dbo.flatTableDay;


CREATE TABLE master.dbo.flatTableDay
(
	Cost_ID        nvarchar(6)    NOT NULL,
	dcmDate int not null,
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

		f2.Cost_ID                                                                                    AS Cost_ID,
		f2.dcmDate                                                                                    AS dcmDate,
		f2.dcmYrMo                                                                                    AS dcmYrMo,
		f2.prsCostMethod                                                                              AS prsCostMethod,
		f2.PackageCat                                                                                 AS PackageCat,
		f2.prsRate                                                                                    AS prsRate,
		f2.prsStYrMo                                                                                  AS prsStYrMo,
		f2.prsEdYrMo                                                                                  AS prsEdYrMo,
		f2.stDate                                                                                     AS prsStDate,
		f2.edDate                                                                                     AS prsEdDate,
		CASE WHEN f2.diff IS NULL THEN cast(0 AS int) ELSE f2.diff END                                AS diff,
		CASE WHEN f2.dcmDate = f2.stDate AND f2.stDate = f2.edDate THEN f2.flatCost
		WHEN f2.diff > 0 AND f2.flatCost < f2.prsRate AND f2.impsRunTot < f2.Planned_Amt THEN f2.flatCost
		WHEN f2.diff > 0 AND f2.lagCost = 0 AND f2.lagRemain = 0 THEN f2.flatCost
		WHEN f2.diff > 0 AND f2.flatCostRemain = 0 AND f2.lagRemain > 0 THEN f2.lagRemain
		WHEN f2.flatCost = 0 THEN 0
		WHEN f2.flatCost > f2.lagRemain THEN f2.lagRemain
		ELSE f2.flatCost - f2.lagRemain END                                                           AS flatCost,
		CASE WHEN f2.lagCost IS NULL THEN cast(0 AS decimal(20,10)) ELSE f2.lagCost END               AS lagCost,
		CASE WHEN f2.lagRemain IS NULL THEN cast(0 AS decimal(20,10)) ELSE f2.lagRemain END           AS lagRemain,
		CASE WHEN f2.flatCostRunTot IS NULL THEN cast(0 AS decimal(20,10)) ELSE f2.flatCostRunTot END AS flatCostRunTot,
		CASE WHEN f2.flatCostRemain IS NULL THEN cast(0 AS decimal(20,10)) ELSE f2.flatCostRemain END AS flatCostRemain,
		f2.Imps                                                                                       AS Imps,
		f2.impsRunTot                                                                                 AS impsRunTot,
        CASE WHEN f2.impsRemain IS NULL THEN cast(0 AS int) ELSE f2.impsRemain END AS impsRemain,
            CASE WHEN f2.Planned_Amt IS NULL THEN cast(0 AS int) ELSE f2.Planned_Amt END AS Planned_Amt

	FROM (
		     SELECT
			     f1.stDate                                                                                AS stDate,
			     f1.edDate                                                                                AS edDate,
			     f1.dcmDate                                                                               AS dcmDate,
			  f1.Cost_ID                                                                               AS Cost_ID,
			     f1.dcmYrMo                                                                               AS dcmYrMo,
			     f1.prsCostMethod                                                                         AS prsCostMethod,
			     f1.PackageCat                                                                            AS PackageCat,
			     f1.prsRate                                                                               AS prsRate,
			     f1.prsStYrMo                                                                             AS prsStYrMo,
			     f1.prsEdYrMo                                                                             AS prsEdYrMo,
			     NULLIF(f1.diff,0)                                                                        AS diff,
			     CASE WHEN f1.flatCost IS NULL THEN cast(0 AS
			                                             decimal(20,10))
			     WHEN f1.diff > 0 AND f1.Imps > f1.Planned_Amt THEN f1.flatCost - f1.lagCost
			     ELSE f1.flatCost END                                                                     AS flatCost,
			     f1.lagCost                                                                               AS lagCost,
			     lag(f1.flatCostRemain,1,0) OVER (PARTITION BY f1.Cost_ID ORDER BY
				     f1.dcmYrMo)                                                                          AS lagRemain,
			     f1.flatCostRunTot                                                                        AS flatCostRunTot,
			     CASE WHEN f1.flatCostRemain IS NULL THEN cast(0 AS
			                                                   decimal(20,10)) ELSE f1.flatCostRemain END AS flatCostRemain,
			     f1.Imps                                                                                  AS Imps,
			     f1.impsRunTot                                                                            AS impsRunTot,
			     f1.impsRemain                                                                            AS impsRemain,
			     f1.Planned_Amt                                                                           AS Planned_Amt
		     FROM (
			          SELECT
				          almost.stDate                                                                       AS stDate,
				          almost.edDate                                                                       AS edDate,
				          almost.dcmDate                                                                      AS dcmDate,
				          almost.Cost_ID                                                                      AS Cost_ID,
				          almost.dcmYrMo                                                                      AS dcmYrMo,
				          almost.Cost_Method                                                                  AS prsCostMethod,
				          almost.PackageCat                                                                   AS PackageCat,
				          almost.Rate                                                                         AS prsRate,
				          almost.stYrMo                                                                       AS prsStYrMo,
				          almost.edYrMo                                                                       AS prsEdYrMo,
				          CASE WHEN almost.ed_diff IS NULL THEN cast(0 AS
				                                                     decimal(20,10)) ELSE almost.ed_diff END  AS diff,
				          sum(
					          almost.flatCost) OVER (PARTITION BY almost.Cost_ID,almost.dcmDate)              AS flatCost,
				          lag(almost.flatcost,1,0) OVER (PARTITION BY almost.Cost_ID ORDER BY
					          almost.dcmDate)                                                                 AS lagCost,
				          sum(almost.flatCost) OVER (PARTITION BY almost.Cost_ID ORDER BY
					          almost.dcmDate ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS flatCostRunTot,
				          CASE WHEN ( cast(almost.Rate AS decimal) -
				                      sum(almost.flatCost) OVER (PARTITION BY almost.Cost_ID    ORDER BY
					                      almost.dcmDate ASC) ) <=
				                    0 THEN 0 ELSE ( cast(almost.Rate AS decimal) - sum(
					          almost.flatCost) OVER (PARTITION BY almost.Cost_ID    ORDER BY
					          almost.dcmDate ASC) ) END                                                       AS flatCostRemain,
				          sum(
					          almost.impressions) OVER (PARTITION BY almost.Cost_ID,almost.dcmDate)           AS Imps,
				          sum(almost.impressions) OVER (PARTITION BY almost.Cost_ID    ORDER BY
					          almost.dcmDate ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)           AS impsRunTot,
				          CASE WHEN ( cast(almost.Planned_Amt AS decimal) -
				                      sum(almost.impressions) OVER (PARTITION BY almost.Cost_ID ORDER BY
					                      almost.dcmDate ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ) <=
				                    0 THEN 0 ELSE (
					          cast(almost.Planned_Amt AS decimal) -
					          sum(almost.impressions) OVER (PARTITION BY almost.Cost_ID ORDER BY
						          almost.dcmDate ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ) END AS impsRemain,
				          almost.Planned_Amt                                                                  AS Planned_Amt
			          FROM

				          (
					          SELECT
						          finalish.dcmmonth                                            AS dcmmonth,
						          finalish.dcmDate                                             AS dcmDate,
						          finalish.dcmyear                                             AS dcmyear,
						          finalish.dcmYrMo                                             AS dcmYrMo,
						          finalish.Cost_ID                                             AS Cost_ID,
						          finalish.cost_method                                         AS cost_method,
						          CASE
						          WHEN finalish.dcmDate - finalish.stDate < 0
							          THEN 0
						          WHEN finalish.edDate = finalish.dcmDate AND finalish.edDate = finalish.dcmDate
							          THEN finalish.rate
						          WHEN ( finalish.edDate - finalish.dcmDate ) > 0 AND sum(finalish.impressions) > finalish.planned_amt
							          THEN finalish.rate

						          WHEN ( finalish.edDate - finalish.dcmDate ) > 0
							          THEN sum(
								          ( cast(finalish.impressions AS decimal(20,10)) /
								            NULLIF(cast(finalish.planned_amt AS decimal(20,10)),0) ) *
								          cast(finalish.rate AS decimal(20,10)))
						          WHEN ( finalish.edDate - finalish.dcmDate ) = 0
							          THEN finalish.rate
						          WHEN ( finalish.edDate - finalish.dcmDate ) < 0
							          THEN 0
						          ELSE 0 END                                                   AS flatCost,
						          finalish.rate                                                AS rate,
						          finalish.PackageCat                                          AS PackageCat,
						          finalish.stYrMo                                              AS stYrMo,
						          finalish.edYrMo                                              AS edYrMo,
						          finalish.stDate                                              AS stDate,
						          finalish.edDate                                              AS edDate,
						          cast(finalish.edDate AS int) - cast(finalish.dcmDate AS int) AS ed_diff,
						          finalish.planned_amt                                         AS planned_amt,
						          sum(finalish.impressions)                                    AS impressions
					          FROM
						          (
							          SELECT
								          Prisma.adserverPlacementID             AS adserverPlacementID,
								 dcmReport.PlacementNumber              AS PlacementNumber,
								          dcmReport.site_placement               AS site_placement,
								          MONTH(cast(dcmReport.dcmDate AS date)) AS dcmMonth,
								          YEAR(cast(dcmReport.dcmDate AS date))  AS dcmYear,
								          CASE
								          WHEN len(cast(MONTH(cast(dcmReport.dcmDate AS date)) AS varchar(2))) = 1
									          THEN CONVERT(int,
									                       CAST(YEAR(CAST(dcmReport.dcmDate AS date)) AS varchar(4)) +
									                       cast(0 AS varchar(1)) +
									                       CAST(MONTH(CAST(dcmReport.dcmDate AS date)) AS varchar(2)))
								          ELSE
									          CONVERT(int,CAST(YEAR(CAST(dcmReport.dcmDate AS date)) AS varchar(4)) +
									                      CAST(MONTH(CAST(dcmReport.dcmDate AS date)) AS varchar(2)))
								          END                                    AS dcmYrMo,

								          CASE
								          WHEN len(cast(MONTH(cast(dcmReport.dcmDate AS date)) AS varchar(2))) = 1
									          THEN CONVERT(int,
									                       CAST(YEAR(CAST(dcmReport.dcmDate AS date)) AS varchar(4)) +
									                       cast(0 AS varchar(1)) +
									                       CAST(MONTH(CAST(dcmReport.dcmDate AS date)) AS varchar(2)) +
									                       RIGHT(CAST(CAST(dcmReport.dcmDate AS date) AS varchar(10)),2)
									          )
								          ELSE
									          CONVERT(int,CAST(YEAR(CAST(dcmReport.dcmDate AS date)) AS varchar(4)) +
									                      CAST(MONTH(CAST(dcmReport.dcmDate AS date)) AS varchar(2)) +
									                      RIGHT(CAST(CAST(dcmReport.dcmDate AS date) AS varchar(10)),2)
									          )
								          END               AS dcmDate,

								          dcmReport.directory_site,
								          Prisma.CostMethod                      AS Cost_Method,
								          Prisma.Cost_ID                         AS Cost_ID,
								          Prisma.Rate                            AS Rate,
								          Prisma.PackageCat                      AS PackageCat,
								          Prisma.stYrMo                          AS stYrMo,
								          Prisma.edYrMo                          AS edYrMo,
								          CASE WHEN len(cast(month(cast(Prisma.PlacementStart AS date)) AS varchar(2))) = 1
									          THEN cast(CAST(year(CAST(Prisma.PlacementStart AS date)) AS varchar(4)) + cast(0 AS varchar(1)) +
									                    CAST(MONTH(CAST(Prisma.PlacementStart AS date)) AS varchar(2)) +
									                    RIGHT(CAST(CAST(Prisma.PlacementStart AS date) AS varchar(10)),2)
									                    AS
									                    int)
								          ELSE
									          cast(CAST(YEAR(CAST(Prisma.PlacementStart AS date)) AS varchar(4)) +
									               CAST(MONTH(CAST(Prisma.PlacementStart AS date)) AS varchar(2)) +
									               RIGHT(CAST(CAST(Prisma.PlacementStart AS date) AS varchar(10)),2)
									               AS int)
								          END                                    AS stDate,
								          CASE WHEN len(cast(month(cast(Prisma.PlacementEnd AS date)) AS varchar(2))) = 1
									          THEN cast(CAST(year(CAST(Prisma.PlacementEnd AS date)) AS varchar(4)) + cast(0 AS varchar(1)) +
									                    CAST(MONTH(CAST(Prisma.PlacementEnd AS date)) AS varchar(2)) +
									                    RIGHT(CAST(CAST(Prisma.PlacementEnd AS date) AS varchar(10)),2)
									                    AS
									                    int)
								          ELSE
									          cast(CAST(YEAR(CAST(Prisma.PlacementEnd AS date)) AS varchar(4)) +
									               CAST(MONTH(CAST(Prisma.PlacementEnd AS date)) AS varchar(2)) +
									               RIGHT(CAST(CAST(Prisma.PlacementEnd AS date) AS varchar(10)),2)
									               AS int)
								          END                                    AS edDate,

								      SUM(dcmReport.Impressions)             AS impressions,
								          SUM(dcmReport.clicks)                  AS clicks,
								          SUM(dcmReport.tickets)                 AS tickets,
								          Prisma.Planned_Amt                     AS Planned_Amt

					     FROM (
--
						          SELECT *
						          FROM openQuery(VerticaGroupM,
			                    '
SELECT

cast(Report.Date AS DATE)                   AS dcmDate,
cast(month(cast(Report.Date as date)) as int) as reportMonth,
Campaign.Buy                                AS Buy,
Report.Buy_ID                               AS Buy_ID,
Directory.Directory_Site                    AS Directory_Site,
Placements.PlacementNumber                  AS PlacementNumber,
Placements.Site_Placement                   AS Site_Placement,
Report.Placement_ID                         AS Placement_ID,
sum(Report.Impressions)                     AS impressions,
sum(Report.Clicks)                          AS clicks,
sum(Report.View_Thru_Conv)                  AS View_Thru_Conv,
sum(Report.Click_Thru_Conv)                 AS Click_Thru_Conv,
sum(Report.View_Thru_Tickets) + sum(Report.Click_Thru_Tickets) as tickets,
sum(Report.View_Thru_Tickets)               AS View_Thru_Tickets,
sum(Report.Click_Thru_Tickets)              AS Click_Thru_Tickets,
sum(cast(Report.View_Thru_Revenue AS DECIMAL(10, 2)))                   AS View_Thru_Revenue,
sum(cast(Report.Click_Thru_Revenue AS DECIMAL(10, 2)))                   AS Click_Thru_Revenue,
sum(cast(Report.Revenue AS DECIMAL(10, 2))) AS revenue

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

LEFT JOIN mec.Cross_Client_Resources.EXCHANGE_RATES AS Rates
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

) AS Impressions
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
) AS Clicks

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
) AS Campaign
ON Report.buy_id = Campaign.order_id

LEFT JOIN
(
-- 			     SELECT *
select cast(site_placement as varchar(4000)) as ''site_placement'', left(cast(site_placement as varchar(4000)),6) as ''PlacementNumber'', max(page_id) as ''page_id'', order_id as ''order_id''
from mec.UnitedUS.dfa_page
		group by site_placement, order_id, site_id
) AS Placements
ON Report.Placement_ID = Placements.page_id
AND Report.buy_id = Placements.order_id

LEFT JOIN
(
-- 			     SELECT *
select cast(directory_site as varchar(4000)) as ''directory_site'', site_id as ''site_id''
from mec.UnitedUS.dfa_site
) AS Directory
ON Report.Site_ID = Directory.Site_ID

WHERE NOT REGEXP_LIKE(Placements.site_placement,''.do\s*not\s*use.'',''ib'')


GROUP BY
cast(Report.Date AS DATE)
, cast(month(cast(Report.Date as date)) as int)
, Directory.Directory_Site
, Report.Buy_ID
, Campaign.Buy
, Report.Placement_ID
, Placements.Site_Placement
, Placements.PlacementNumber

											  ')

					          ) AS dcmReport
-- =========================================================================================================================
						     LEFT JOIN
						     (
							     SELECT *
							     FROM [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.[dbo].summaryTable
						     ) AS Prisma
							     ON dcmReport.Placement_ID = Prisma.AdserverPlacementId
					     WHERE Prisma.CostMethod = 'Flat'


-- =========================================================================================================================
					     GROUP BY
						     cast(dcmReport.dcmDate AS date)
						     ,dcmReport.PlacementNumber
						     ,CONVERT(int,cast(YEAR(cast(dcmReport.dcmDate AS date)) AS varchar(4)) +
						                  SUBSTRING(( CONVERT(varchar(10),cast(dcmReport.dcmDate AS date)) ),
						                            ( CHARINDEX('-',
						                                        CONVERT(varchar(10),cast(dcmReport.dcmDate AS date))) +
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
						          ) AS finalish

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
		     ) AS almost
     ) AS f1
) as f2
go
