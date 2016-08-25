ALTER PROCEDURE dbo.createViewTbl

AS
	IF OBJECT_ID('DM_1161_UnitedAirlinesUSA.dbo.ViewTable', N'U') IS NOT NULL
		DROP TABLE dbo.ViewTable;
	CREATE TABLE dbo.ViewTable
	(
		PlacementId       INT NOT NULL,
		CustomColumnValue NVARCHAR(1)
	);

	INSERT INTO dbo.ViewTable
		SELECT DISTINCT
			cast(op.PlacementId AS INT),
			op.CustomColumnValue
		FROM [dbo].[DFID037723_PrismaAdvancedPlacementDetails_Extracted] AS op
		WHERE op.CustomColumnName = 'PUB PAID AD SERVING FEES'
		      AND CustomColumnValue IS NOT NULL;
go