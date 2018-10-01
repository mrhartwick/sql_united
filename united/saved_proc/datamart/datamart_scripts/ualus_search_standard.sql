--====================================================================
-- Author:    Seetha Srinivasan
-- Create date:      13 Jul 2016 08:38:36 PM ET
-- Description: {Description}
-- Comments:
--====================================================================
--Delete existing records from the Fact Table (Idea is to have rolling 7 days data)
DELETE FROM [UALUS_Search_Standard]
WHERE  Date IN (SELECT DISTINCT Cast([Date] AS DATE)
                FROM   [dbo].[DFID041780_UALUS_Search_Standard_Extracted])

--Transform/Cast data types and insert cleansed data into the Fact Table
INSERT INTO [UALUS_Search_Standard]
SELECT Cast(Date AS DATE)                                    Date,
       Cast([Week] AS DATE)                                  [Week],
       Cast([Paid Search Engine Account ID] AS NVARCHAR(50)) PdSearchEngine_AccountID,
       Cast([Paid Search Campaign ID] AS NVARCHAR(50))       PdSearch_CampaignID,
       Cast([Paid Search Keyword ID] AS NVARCHAR(50))        PdSearch_KeywordID,
       Cast([Paid Search Ad Group ID] AS NVARCHAR(50))       PdSearch_AdGroupID,
       Cast([Paid Search Ad ID] AS NVARCHAR(50))             PdSearch_AdID,
       Cast([Paid Search Advertiser ID] AS NVARCHAR(50))     PdSearch_AdvertiserID,
       Cast([Paid Search Match Type] AS NVARCHAR(50))        PdSearch_MatchType,
       Cast([Site ID (DCM)] AS NVARCHAR(50))                 SiteID_DCM,
       Cast([Site ID (Site Directory)] AS NVARCHAR(50))      SiteID_SiteDir,
       Cast([Campaign ID] AS NVARCHAR(50))                   CampaignID,
       Cast([Package/Roadblock ID] AS NVARCHAR(50))          PackageID,
       Cast([Placement ID] AS NVARCHAR(50))                  PlacementID,
       CASE
         WHEN Isnumeric([Placement Total Booked Units]) = 1 THEN Cast([Placement Total Booked Units] AS BIGINT)
         ELSE 0
       END                                                   TotalBookedUnits,
       CASE
         WHEN Isnumeric([Paid Search Impressions]) = 1 THEN Cast([Paid Search Impressions] AS BIGINT)
         ELSE 0
       END                                                   Impressions,
       CASE
         WHEN Isnumeric([Paid Search Clicks]) = 1 THEN Cast([Paid Search Clicks] AS BIGINT)
         ELSE 0
       END                                                   Clicks,
       CASE
         WHEN Isnumeric([Paid Search Cost]) = 1 THEN Cast([Paid Search Cost] AS DECIMAL(38, 2))
         ELSE 0
       END                                                   Search_Cost,
       CASE
         WHEN Isnumeric([Paid Search Revenue]) = 1 THEN Cast([Paid Search Revenue] AS DECIMAL(38, 2))
         ELSE 0
       END                                                   Search_Revenue,
       CASE
         WHEN Isnumeric([Paid Search Click Rate]) = 1 THEN Cast([Paid Search Click Rate] AS DECIMAL(38, 2))
         ELSE 0
       END                                                   Click_Rate,
       CASE
         WHEN Isnumeric([Paid Search Average Position]) = 1 THEN Cast([Paid Search Average Position] AS DECIMAL(38, 2))
         ELSE 0
       END                                                   Avg_Postn,
       CASE
         WHEN Isnumeric([Paid Search Transactions]) = 1 THEN Cast([Paid Search Transactions] AS DECIMAL(38, 2))
         ELSE 0
       END                                                   Transactions,
       CASE
         WHEN Isnumeric([Paid Search Actions]) = 1 THEN Cast([Paid Search Actions] AS DECIMAL(38, 2))
         ELSE 0
       END                                                   Actions,
       CASE
         WHEN Isnumeric([Paid Search Visits]) = 1 THEN Cast([Paid Search Visits] AS BIGINT)
         ELSE 0
       END                                                   TotalVisits,
       AcquireID
FROM   [dbo].[DFID041780_UALUS_Search_Standard_Extracted]
WHERE  Cast([Date] AS DATE) NOT IN (SELECT DISTINCT [Date]
                                    FROM   UALUS_Search_Standard)

SELECT Max([Date]) ActivityDate
INTO   #DT
FROM   UALUS_Search_Standard
