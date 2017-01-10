alter PROCEDURE dbo.createSumTbl
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

			 select distinct
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
				 cast(t1.PlacementStartDate as
					  date)                                 as PlacementStart,
				 cast(t1.PlacementEndDate as
					  date)                                 as PlacementEnd,

				 [dbo].udf_dateToInt(t1.PlacementStartDate) as stDate,
				 [dbo].udf_dateToInt(t1.PlacementEndDate)   as edDate,
				 [dbo].udf_yrmoToInt(t1.PlacementStartDate) as stYrMo,
				 [dbo].udf_yrmoToInt(t1.PlacementEndDate)   as edYrMo,
				 vew.CustomColumnValue                      as DV_Map,
				 isNull(t2.Rate,cast(0 as decimal(20,10)))  as Rate,
				 pak.PackageName,
				 pak.Cost_ID,
				 t2.CostMethod
			 from (
					  select distinct
						  PlacementId,
						  cast(AdserverPlacementId as int) as AdserverPlacementId,
						  cast(AdserverCampaignId as int)  as AdserverCampaignId,
						  ParentId,
						  PlacementNumber,
						  PlacementName,
						  PlacementStartDate,
						  PlacementEndDate,
						  PackageType
					  from DM_1161_UnitedAirlinesUSA.dbo.DFID037723_PrismaAdvancedPlacementDetails_Extracted) as t1

				 outer apply (
								 select distinct
									 PlacementId,
									 ParentId,
									 isNull(cast(Rate as decimal(20,10)),0) as Rate,
									 CostMethod,
									 CampaignName,
									 PackageType                            as PackageCat
								 from DM_1161_UnitedAirlinesUSA.dbo.DFID037722_PrismaPlacementDetails_Extracted as t2
								 where t1.PlacementId = t2.PlacementId) as t2

				 outer apply (
								 select
									 PlacementId,
									 CustomColumnValue
								 from DM_1161_UnitedAirlinesUSA.dbo.ViewTable as vew

								 where t1.PlacementId = vew.PlacementId) as vew

				 outer apply (
								 select
									 ParentId,
									 PackageName,
									 Cost_ID
								 from DM_1161_UnitedAirlinesUSA.dbo.packageTable as pak
								 where t1.ParentId = pak.ParentId) as pak

				 outer apply (
								 select
									 ParentId,
									 PlannedCost,
									 PlannedUnits,
									 PlacementStartDate

								 from DM_1161_UnitedAirlinesUSA.[dbo].plannedAmtTable as amt

								 where t1.ParentId = amt.ParentId
-- 				                       AND cast(t1.AdserverPlacementId AS int) = amt.AdserverPlacementId
							 ) as amt

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