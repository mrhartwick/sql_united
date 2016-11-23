ALTER PROCEDURE dbo.createSumTbl
AS
IF OBJECT_ID('DM_1161_UnitedAirlinesUSA.dbo.summaryTable',N'U') IS NOT NULL
	DROP TABLE dbo.summaryTable;
CREATE TABLE dbo.summaryTable
(
	PlacementId         int            NOT NULL,
	AdserverPlacementId int,
	AdserverCampaignId  int,
	PlacementNumber     nvarchar(6)    NOT NULL,
	PlacementName       nvarchar(4000) NOT NULL,
	CampaignName        nvarchar(4000) NOT NULL,
	Planned_Cost        decimal(20,10),
	Planned_Amt         int,
	ParentId            int            NOT NULL,
	PackageCat          nvarchar(100),
	PlacementStart      date           NOT NULL,
	PlacementEnd        date           NOT NULL,
	stYrMo              int            NOT NULL,
	edYrMo              int            NOT NULL,
	stDate              int            NOT NULL,
	edDate              int            NOT NULL,
	DV_Map              nvarchar(1),
	Rate                decimal(20,10) NOT NULL,
	PackageName         nvarchar(4000),
	Cost_ID             nvarchar(6),
	CostMethod          nvarchar(100)  NOT NULL
);

INSERT INTO dbo.summaryTable

	SELECT DISTINCT
		final.PlacementId                                                                                       AS PlacementId,
		final.AdserverPlacementId                                                                               AS AdserverPlacementId,
		final.AdserverCampaignId                                                                                AS AdserverCampaignId,
		final.PlacementNumber                                                                                   AS PlacementNumber,
		final.PlacementName                                                                                     AS PlacementName,
		final.CampaignName                                                                                      AS CampaignName,
		final.PlannedCost                                                                                       AS Planned_Cost,
		final.PlannedUnits                                                                                      AS Planned_Amt,
		final.ParentId                                                                                          AS ParentId,
		final.PackageCat                                                                                        AS PackageCat,
		final.PlacementStart                                                                                    AS PlacementStart,
		final.PlacementEnd                                                                                      AS PlacementEnd,
		final.stYrMo                                                                                            AS stYrMo,
		final.edYrMo                                                                                            AS edYrMo,
		final.stDate                                                                                            AS stDate,
		final.edDate                                                                                            AS edDate,
		final.DV_Map                                                                                            AS DV_Map,
		isNull(final.Rate, cast(0 AS decimal(20,10))) AS Rate,
		final.PackageName                                                                                       AS PackageName,
		final.Cost_ID                                                                                           AS Cost_ID,
		final.CostMethod                                                                                        AS CostMethod
	FROM (

		     SELECT DISTINCT
			     t1.PlacementId,
			     t1.AdserverPlacementId,
			     t1.AdserverCampaignId,
			     t1.PlacementNumber,
			     t1.PlacementName,
			     t2.CampaignName,
			     amt.PlannedCost,
			     amt.PlannedUnits,
			     t1.ParentId,
			     t2.PackageCat,
			     cast(t1.PlacementStartDate AS
			          date)                                                            AS PlacementStart,
			     cast(t1.PlacementEndDate AS
			          date)                                                                                        AS PlacementEnd,

			    [dbo].udf_dateToInt(t1.PlacementStartDate) AS stDate,
                [dbo].udf_dateToInt(t1.PlacementEndDate) AS edDate,

-- 			Horrible code to turn start and end dates of placements into integers, ready for absolute value comparison with other, similarly transformed dates in queries.
			     CASE
			     WHEN len(cast(month(cast(t1.PlacementStartDate AS date)) AS varchar(2))) = 1
				     THEN cast(CAST(year(CAST(t1.PlacementStartDate AS date)) AS
				                    varchar(4)) + cast(0 AS varchar(1)) +
				               CAST(MONTH(CAST(t1.PlacementStartDate AS date)) AS varchar(2)) AS int)
			     ELSE
				     cast(CAST(YEAR(CAST(t1.PlacementStartDate AS date)) AS varchar(4)) +
				          CAST(MONTH(CAST(t1.PlacementStartDate AS date)) AS varchar(2)) AS int)
			     END                                                                                               AS stYrMo,
			     CASE
			     WHEN len(cast(month(cast(t1.PlacementEndDate AS date)) AS varchar(2))) = 1
				     THEN cast(CAST(year(CAST(t1.PlacementEndDate AS date)) AS
				                    varchar(4)) + cast(0 AS varchar(1)) +
				               CAST(MONTH(CAST(t1.PlacementEndDate AS date)) AS varchar(2)) AS int)
			     ELSE
				     cast(CAST(YEAR(CAST(t1.PlacementEndDate AS date)) AS varchar(4)) +
				          CAST(MONTH(CAST(t1.PlacementEndDate AS date)) AS varchar(2)) AS int)
			     END                                                                                               AS edYrMo,
			     vew.CustomColumnValue                                                                             AS DV_Map,
			     isNull(t2.Rate, cast(0 AS decimal(20,10)))  AS Rate,
			     pak.PackageName,
			     pak.Cost_ID,
			     t2.CostMethod
		     FROM (
			          SELECT DISTINCT
				          PlacementId,
				          cast(AdserverPlacementId AS int) AS AdserverPlacementId,
				          cast(AdserverCampaignId AS int)  AS AdserverCampaignId,
				          ParentId,
				          PlacementNumber,
				          PlacementName,
				          PlacementStartDate,
				          PlacementEndDate,
				          PackageType
			          FROM DM_1161_UnitedAirlinesUSA.dbo.DFID037723_PrismaAdvancedPlacementDetails_Extracted ) AS t1

			     OUTER APPLY (
				                 SELECT DISTINCT
					                 PlacementId,
					                 ParentId,
					                isNull(cast(Rate AS decimal(20,10)), 0) 							 AS Rate,
					                 CostMethod,
					                 CampaignName,
					                 PackageType                                 AS PackageCat
				                 FROM DM_1161_UnitedAirlinesUSA.dbo.DFID037722_PrismaPlacementDetails_Extracted AS t2
				                 WHERE t1.PlacementId = t2.PlacementId ) AS t2

			     OUTER APPLY (
				                 SELECT
					                 PlacementId,
					                 CustomColumnValue
				                 FROM DM_1161_UnitedAirlinesUSA.dbo.ViewTable AS vew

				                 WHERE t1.PlacementId = vew.PlacementId ) AS vew

			     OUTER APPLY (
				                 SELECT
					                 ParentId,
					                 PackageName,
					                 Cost_ID
				                 FROM DM_1161_UnitedAirlinesUSA.dbo.packageTable AS pak
				                 WHERE t1.ParentId = pak.ParentId ) AS pak

			     OUTER APPLY (
				                 SELECT
					                 ParentId,
					                 PlannedCost,
					                 PlannedUnits,
					                 PlacementStartDate

				                 FROM DM_1161_UnitedAirlinesUSA.[dbo].plannedAmtTable AS amt

				                 WHERE t1.ParentId = amt.ParentId
-- 				                       AND cast(t1.AdserverPlacementId AS int) = amt.AdserverPlacementId
			                 ) AS amt

-- 			The same as above, using JOIN syntax

-- 			LEFT JOIN (
-- 				            SELECT DISTINCT
-- 					            PlacementId,
-- 					            ParentId,
-- 					            Rate,
-- 					            CostMethod,
-- 					            CampaignName,
-- 					            PackageType AS PackageCat
-- 				            -- 				             CASE WHEN PlacementId = ParentId and PackageType != 'Standalone'
-- 				            -- 					             THEN 'Package'
-- 				            -- -- 					             when PackageType = 'Standalone'
-- 				            -- -- 					             then 'Package'
-- 				            -- 				             ELSE PackageType END AS PackageCat
-- 				            FROM DM_1161_UnitedAirlinesUSA.dbo.DFID037722_PrismaPlacementDetails_Extracted
-- -- 			         where CampaignName NOT LIKE '%DO_NOT_USE%' and CampaignName NOT LIKE '%DONOTUSE%' and CampaignName NOT LIKE 'DO_NOT_USE%'
--
-- 			          ) AS t2
-- 				       on t1.PlacementId = t2.PlacementId
--
-- 			LEFT JOIN (
-- 				            SELECT
-- 					            PlacementId,
-- 					            CustomColumnValue
-- 				            FROM DM_1161_UnitedAirlinesUSA.dbo.ViewTable) AS vew
--
-- 				            on t1.PlacementId = vew.PlacementId
--
-- 			LEFT JOIN (
-- 				            SELECT
-- 					            ParentId,
-- 					            PackageName,
-- 					            Cost_ID
-- 				            FROM DM_1161_UnitedAirlinesUSA.dbo.packageTable) AS pak
-- 				            on t1.ParentId = pak.ParentId
--
-- 			LEFT JOIN (
-- 				            SELECT
-- 					            ParentId,
-- 					            PlannedUnits,
-- 					            PlacementStartDate
--
-- 				            FROM DM_1161_UnitedAirlinesUSA.[dbo].plannedAmtTable) AS amt
--
-- 				            on t1.ParentId = amt.ParentId

-- 		     WHERE ( t1.PlacementName NOT LIKE '%DONOTUSE%' OR t1.PlacementName NOT LIKE '%DO_NOT_USE%' )
-- 		           AND ( t2.CampaignName NOT LIKE '%DO_NOT_USE%' OR t2.CampaignName NOT LIKE '%DONOTUSE%' OR t2.CampaignName NOT LIKE 'DO_NOT_USE%' )
-- 		           AND left(pak.Cost_ID,1) = 'P'
-- 		           AND
		           where cast(t1.PlacementStartDate AS date) >= '2016-01-01'

		     GROUP BY
			     t1.PlacementId,
			     t1.AdserverPlacementId,
			     t1.AdserverCampaignId,
			     t1.PlacementNumber,
			     t1.PlacementName,
			     t2.CampaignName,
			     amt.PlannedCost,
			     amt.PlannedUnits,
			     t1.ParentId,
			     t2.PackageCat,
			     t1.PlacementStartDate,
			     t1.PlacementEndDate,
			     vew.CustomColumnValue,
			     t2.Rate,
			     pak.PackageName,
			     pak.Cost_ID,
			     t2.CostMethod ) AS final

-- 	WHERE ( final.PlacementName NOT LIKE '%DONOTUSE%' AND final.PlacementName NOT LIKE '%DO_NOT_USE%' )
-- 	      AND ( final.CampaignName NOT LIKE '%DO_NOT_USE%' AND final.CampaignName NOT LIKE '%DONOTUSE%' AND final.CampaignName NOT LIKE 'DO_NOT_USE%' )
-- 	      AND left(final.Cost_ID,1) = 'P' -- Omits all Placements not beginning with "P," our current naming convention. Speeds up the query.
-- 	      AND
	      where final.stYrMo >= '201601'

	GROUP BY
		final.PlacementId,
		final.AdserverPlacementId,
		final.AdserverCampaignId,
		final.PlacementNumber,
		final.PlacementName,
		final.CampaignName,
		final.PlannedCost,
		final.PlannedUnits,
		final.ParentId,
		final.PackageCat,
		final.PlacementStart,
		final.PlacementEnd,
		final.stYrMo,
		final.edYrMo,
		final.stDate,
		final.edDate,
		final.DV_Map,
		final.Rate,
		final.PackageName,
		final.Cost_ID,
		final.CostMethod
go