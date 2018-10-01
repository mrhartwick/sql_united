CREATE PROCEDURE dbo.crt_prs_viewTbl

AS
	IF OBJECT_ID('DM_1161_UnitedAirlinesUSA.dbo.prs_view', N'U') IS NOT NULL
		DROP TABLE dbo.prs_view;
	CREATE TABLE dbo.prs_view
	(
		PlacementId       INT NOT NULL,
		CustomColumnValue NVARCHAR(1)
	);

	INSERT INTO dbo.prs_view
		SELECT DISTINCT
			cast(op.PlacementId AS INT),
			op.CustomColumnValue
		FROM [dbo].[DFID037723_PrismaAdvancedPlacementDetails_Extracted] AS op
		WHERE op.CustomColumnName = 'PUB PAID AD SERVING FEES'
		      AND CustomColumnValue IS NOT NULL
  AND CustomColumnValue <> 'Social';
