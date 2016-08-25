ALTER PROCEDURE dbo.createPackTbl
AS
	IF OBJECT_ID('DM_1161_UnitedAirlinesUSA.dbo.packageTable', N'U') IS NOT NULL
		DROP TABLE dbo.packageTable;
	CREATE TABLE dbo.packageTable
	(
		ParentId    INT NOT NULL,
		PackageName NVARCHAR(4000),
		Cost_ID NVARCHAR(6),
		PackageType NVARCHAR(100)

	);

	INSERT INTO dbo.packageTable
		SELECT DISTINCT
			advanced.ParentId,
			plc.PlacementName AS PackageName,
			left(plc.PlacementName, 6) AS Cost_ID,
			'Package'   AS PackageType
		FROM (SELECT DISTINCT *
		      FROM DM_1161_UnitedAirlinesUSA.dbo.DFID037723_PrismaAdvancedPlacementDetails_Extracted) AS advanced


			left JOIN (SELECT DISTINCT *
			           FROM DM_1161_UnitedAirlinesUSA.dbo.DFID037723_PrismaAdvancedPlacementDetails_Extracted
				WHERE PackageType = 'Package' OR PackageType = 'Standalone'
			          ) AS plc
				ON advanced.PlacementId = plc.PlacementId


		WHERE plc.PlacementName is not null

Group by
	advanced.ParentId,
	plc.PlacementName


	;
go