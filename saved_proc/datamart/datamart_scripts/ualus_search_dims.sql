--====================================================================
-- Author:      Seetha Srinivasan
-- Create date:      11 Jul 2016 05:03:57 PM ET
-- Description: {Description}
-- Comments:
--====================================================================
--Placement
IF Object_id('tempdb..#tmp_placement') IS NOT NULL
  DROP TABLE #tmp_placement

SELECT DISTINCT [Placement ID],
                Placement,
                [Placement Start Date],
                [Placement End Date]
INTO   [#tmp_placement]
FROM   [DFID041758_UALUS_DIM_Search_Floodlight_Extracted]

DELETE FROM UALUS_DIM_PLACEMENT
WHERE  Isnull(Placement_ID, '0') IN (SELECT Isnull([Placement ID], '0') PlacementID
                                     FROM   #tmp_placement)

INSERT INTO UALUS_DIM_PLACEMENT
SELECT DISTINCT Isnull(CONVERT(NVARCHAR(50), [Placement ID]), '0') PlacementID,
                CONVERT(NVARCHAR(1000), Placement)                 Placement,
                Cast([Placement Start Date] AS DATE)               PlacementStartDate,
                Cast([Placement End Date] AS DATE)                 PlacementEndDate,
                0                                                  PlacementRate,
                ''                                                 PlacementStrategy,
                0                                                  PlacementTotalUnits,
                0                                                  PlacementCost
FROM   [#tmp_placement]
WHERE  [Placement ID] NOT IN (SELECT Placement_ID
                              FROM   UALUS_DIM_PLACEMENT)

--Placement from Search Standard Report
IF Object_id('tempdb..#tmp_placement_2') IS NOT NULL
  DROP TABLE #tmp_placement_2

SELECT DISTINCT [Placement ID],
                Placement,
                [Placement Start Date],
                [Placement End Date],
                [Placement Rate],
                [Placement Strategy],
                [Placement Total Booked Units],
                [Placement Total Planned Media Cost]
INTO   [#tmp_placement_2]
FROM   DFID041796_UALUS_DIM_Search_Standard_Extracted

DELETE FROM UALUS_DIM_PLACEMENT
WHERE  Isnull(Placement_ID, '0') IN (SELECT Isnull([Placement ID], '0') PlacementID
                                     FROM   #tmp_placement_2)

INSERT INTO UALUS_DIM_PLACEMENT
SELECT DISTINCT Isnull(CONVERT(NVARCHAR(50), [Placement ID]), '0')           PlacementID,
                CONVERT(NVARCHAR(1000), Placement),
                Cast([Placement Start Date] AS DATE)                         PlacementStartDate,
                Cast([Placement End Date] AS DATE)                           PlacementEndDate,
                Cast([Placement Rate] AS DECIMAL(38, 2))                     PlacementRate,
                CONVERT(NVARCHAR(100), [Placement Strategy])                 PlacementStrategy,
                Cast([Placement Total Booked Units] AS BIGINT)               TotalBookedUnits,
                Cast([Placement Total Planned Media Cost] AS DECIMAL(38, 2)) PlannedMediaCost
FROM   [#tmp_placement_2]
WHERE  [Placement ID] NOT IN (SELECT Placement_ID
                              FROM   UALUS_DIM_PLACEMENT)

-----------------------------------------------------------------------------------------------------------------------------------------------
--Dim Campaign
IF Object_id('tempdb..#tmp_campaign') IS NOT NULL
  DROP TABLE #tmp_campaign

SELECT DISTINCT [Campaign ID],
                Campaign,
                [Campaign Start Date],
                [Campaign End Date]
INTO   [#tmp_campaign]
FROM   [DFID041758_UALUS_DIM_Search_Floodlight_Extracted]

DELETE FROM UALUS_DIM_CAMPAIGN
WHERE  Isnull(Campaign_ID, '0') IN (SELECT Isnull([Campaign ID], '0') CampaignID
                                    FROM   #tmp_campaign)

INSERT INTO UALUS_DIM_CAMPAIGN
SELECT DISTINCT Isnull(CONVERT(NVARCHAR(50), [Campaign ID]), '0') CampaignID,
                CONVERT(NVARCHAR(1000), Campaign)                 Campaign,
                Cast([Campaign Start Date] AS DATE)               CampaignStartDate,
                Cast([Campaign End Date] AS DATE)                 CampaignEndDate
FROM   [#tmp_campaign]
WHERE  [Campaign ID] NOT IN (SELECT Campaign_ID
                             FROM   UALUS_DIM_CAMPAIGN)

--Campaign from Search Standard Feed
IF Object_id('tempdb..#tmp_campaign_2') IS NOT NULL
  DROP TABLE #tmp_campaign_2

SELECT DISTINCT [Campaign ID],
                Campaign,
                [Campaign Start Date],
                [Campaign End Date]
INTO   [#tmp_campaign_2]
FROM   DFID041796_UALUS_DIM_Search_Standard_Extracted

DELETE FROM UALUS_DIM_CAMPAIGN
WHERE  Isnull(Campaign_ID, '0') IN (SELECT Isnull([Campaign ID], '0') CampaignID
                                    FROM   #tmp_campaign_2)

INSERT INTO UALUS_DIM_CAMPAIGN
SELECT DISTINCT Isnull(CONVERT(NVARCHAR(50), [Campaign ID]), '0') CampaignID,
                CONVERT(NVARCHAR(1000), Campaign)                 Campaign,
                Cast([Campaign Start Date] AS DATE)               CampaignStartDate,
                Cast([Campaign End Date] AS DATE)                 CampaignEndDate
FROM   [#tmp_campaign_2]
WHERE  [Campaign ID] NOT IN (SELECT Campaign_ID
                             FROM   UALUS_DIM_CAMPAIGN)

-----------------------------------------------------------------------------------------------------------------------------------------------
--Dim Activity
IF Object_id('tempdb..#tmp_activity') IS NOT NULL
  DROP TABLE [#tmp_activity]

SELECT DISTINCT [Activity ID],
                Activity
INTO   [#tmp_activity]
FROM   [DFID041758_UALUS_DIM_Search_Floodlight_Extracted]

DELETE FROM UALUS_DIM_ACTIVITY
WHERE  Isnull(Activity_ID, '0') IN (SELECT Isnull([Activity ID], '0') ActivityID
                                    FROM   #tmp_activity)

INSERT INTO UALUS_DIM_ACTIVITY
SELECT DISTINCT Isnull(CONVERT(NVARCHAR(50), [Activity ID]), '0') ActivityID,
                CONVERT(NVARCHAR(1000), Activity)                 Activity
FROM   [#tmp_activity]
WHERE  [Activity ID] NOT IN (SELECT Activity_ID
                             FROM   UALUS_DIM_ACTIVITY)

-----------------------------------------------------------------------------------------------------------------------------------------------
--Dim Paid Search Engine
IF Object_id('tempdb..#tmp_pdsearchengine') IS NOT NULL
  DROP TABLE #tmp_pdsearchengine

SELECT DISTINCT [Paid Search Engine Account ID],
                [Paid Search Engine Account]
INTO   [#tmp_pdsearchengine]
FROM   DFID041796_UALUS_DIM_Search_Standard_Extracted

DELETE FROM UALUS_DIM_PAID_SEARCHENGINE
WHERE  Isnull(PAID_SEARCHENGINE_ID, '0') IN (SELECT Isnull([Paid Search Engine Account ID], '0') PdSearchEngineAccountID
                                             FROM   [#tmp_pdsearchengine])

INSERT INTO UALUS_DIM_PAID_SEARCHENGINE
SELECT DISTINCT Isnull(CONVERT(NVARCHAR(50), [Paid Search Engine Account ID]), '0') PdSearchEngineAccountID,
                CONVERT(NVARCHAR(1000), [Paid Search Engine Account])               PdSearchEngineAccount
FROM   [#tmp_pdsearchengine]
WHERE  [Paid Search Engine Account ID] NOT IN (SELECT [PAID_SEARCHENGINE_ID]
                                               FROM   UALUS_DIM_PAID_SEARCHENGINE)

-----------------------------------------------------------------------------------------------------------------------------------------------
--Dim Paid Search Campaign
IF Object_id('tempdb..#tmp_pdsearchcampaign') IS NOT NULL
  DROP TABLE #tmp_pdsearchcampaign

SELECT DISTINCT [Paid Search Campaign ID],
                [Paid Search Campaign]
INTO   [#tmp_pdsearchcampaign]
FROM   DFID041796_UALUS_DIM_Search_Standard_Extracted

DELETE FROM UALUS_DIM_PAID_SEARCHCampaign
WHERE  Isnull(PAID_SEARCH_CAMPAIGN_ID, '0') IN (SELECT Isnull([Paid Search Campaign ID], '0') PdSearchCampaignID
                                                FROM   [#tmp_pdsearchcampaign])

INSERT INTO UALUS_DIM_PAID_SEARCHCAMPAIGN
SELECT DISTINCT Isnull(CONVERT(NVARCHAR(50), [Paid Search Campaign ID]), '0') PdSearchCampaignID,
                CONVERT(NVARCHAR(1000), [Paid Search Campaign])               PdSearchCampaign
FROM   [#tmp_pdsearchcampaign]
WHERE  [Paid Search Campaign ID] NOT IN (SELECT [Paid_Search_Campaign_ID]
                                         FROM   [UALUS_DIM_PAID_SEARCHCAMPAIGN])

-----------------------------------------------------------------------------------------------------------------------------------------------
--Dim Package/Roadblock
IF Object_id('tempdb..#tmp_package') IS NOT NULL
  DROP TABLE #tmp_package

SELECT DISTINCT [Package/Roadblock ID],
                [Package/Roadblock]
INTO   #tmp_package
FROM   DFID041796_UALUS_DIM_Search_Standard_Extracted

DELETE FROM UALUS_DIM_PACKAGE
WHERE  Isnull(Package_ID, '0') IN (SELECT Isnull([Package/Roadblock ID], '0') PackageID
                                   FROM   #tmp_package)

INSERT INTO UALUS_DIM_PACKAGE
SELECT DISTINCT Isnull(CONVERT(NVARCHAR(50), [Package/Roadblock ID]), '0') PackageID,
                CONVERT(NVARCHAR(1000), [Package/Roadblock])               Package
FROM   #tmp_package
WHERE  [Package/Roadblock ID] NOT IN (SELECT Package_ID
                                      FROM   UALUS_DIM_PACKAGE)

-----------------------------------------------------------------------------------------------------------------------------------------------
--Dim Site DCM
IF Object_id('tempdb..#tmp_sitedcm') IS NOT NULL
  DROP TABLE #tmp_sitedcm

SELECT DISTINCT [Site ID (DCM)],
                [Site (DCM)],
                [Site ID (Site Directory)],
                [Site (Site Directory)]
INTO   #tmp_sitedcm
FROM   DFID041796_UALUS_DIM_Search_Standard_Extracted

DELETE FROM UALUS_DIM_Site
WHERE  Isnull(Site_ID_DCM, '0') IN (SELECT Isnull([Site ID (DCM)], '0') SiteID
                                    FROM   #tmp_sitedcm)

INSERT INTO UALUS_DIM_Site
SELECT DISTINCT Isnull(CONVERT(NVARCHAR(50), [Site ID (DCM)]), '0')            SiteID_DCM,
                CONVERT(NVARCHAR(1000), [Site (DCM)])                          Site_DCM,
                Isnull(CONVERT(NVARCHAR(50), [Site ID (Site Directory)]), '0') SiteID_SD,
                CONVERT(NVARCHAR(1000), [Site (DCM)])                          Site_SD
FROM   #tmp_sitedcm
WHERE  [Site ID (DCM)] NOT IN (SELECT Site_ID_DCM
                               FROM   UALUS_DIM_Site)

-----------------------------------------------------------------------------------------------------------------------------------------------
--Dim Paid Search Keyword
IF Object_id('tempdb..#tmp_pdsearchkwd') IS NOT NULL
  DROP TABLE #tmp_pdsearchkwd

SELECT DISTINCT [Paid Search Keyword ID],
                [Paid Search Keyword],
                [Paid Search Match Type]
INTO   #tmp_pdsearchkwd
FROM   DFID041796_UALUS_DIM_Search_Standard_Extracted

DELETE FROM UALUS_DIM_PAID_SearchKeyword
WHERE  Isnull(Paid_Search_Keyword_ID, '0') IN (SELECT Isnull([Paid Search Keyword ID], '0') PaidSearchKwID
                                               FROM   #tmp_pdsearchkwd)

INSERT INTO UALUS_DIM_PAID_SearchKeyword
SELECT DISTINCT Isnull(CONVERT(NVARCHAR(50), [Paid Search Keyword ID]), '0') PaidSearchKwID,
                CONVERT(NVARCHAR(1000), [Paid Search Keyword])               PaidSearchKeyword,
                CONVERT(NVARCHAR(50), [Paid Search Match Type])              PaidSearchMatchType
FROM   #tmp_pdsearchkwd
WHERE  [Paid Search Keyword ID] NOT IN (SELECT Paid_Search_Keyword_ID
                                        FROM   UALUS_DIM_Paid_SearchKeyword)

-----------------------------------------------------------------------------------------------------------------------------------------------
--Dim Paid Search Ad info
IF Object_id('tempdb..#tmp_pdsearchad') IS NOT NULL
  DROP TABLE #tmp_pdsearchad

SELECT DISTINCT [Paid Search Ad Group ID],
                [Paid Search Ad ID],
                [Paid Search Ad Group],
                [Paid Search Ad],
                [Paid Search Advertiser ID],
                [Paid Search Advertiser]
INTO   #tmp_pdsearchad
FROM   DFID041796_UALUS_DIM_Search_Standard_Extracted

DELETE FROM UALUS_DIM_Paid_SearchAd
WHERE  EXISTS (SELECT 'x'
               FROM   #tmp_pdsearchad aa
               WHERE  aa.[Paid Search Advertiser ID] = Paid_Search_Advertiser_ID
                      AND aa.[Paid Search Ad ID] = Paid_Search_Ad_ID)

INSERT INTO UALUS_DIM_Paid_SearchAd
SELECT DISTINCT Isnull(CONVERT(NVARCHAR(50), [Paid Search Advertiser ID]), '0') PaidSearchAdvertiserID,
                CONVERT(NVARCHAR(1000), [Paid Search Advertiser])               PaidSearchAdvertiser,
                Isnull(CONVERT(NVARCHAR(50), [Paid Search Ad Group ID]), '0')   PaidSearchAdGroupID,
                CONVERT(NVARCHAR(1000), [Paid Search Ad Group])                 PaidSearchAdGroup,
                Isnull(CONVERT(NVARCHAR(50), [Paid Search Ad ID]), '0')         PaidSearchAdID,
                CONVERT(NVARCHAR(1000), [Paid Search Ad])                       PaidSearchAd
FROM   #tmp_pdsearchad
WHERE  NOT EXISTS (SELECT 'x'
                   FROM   UALUS_DIM_Paid_SearchAd
                   WHERE  [Paid Search Advertiser ID] = Paid_Search_Advertiser_ID
                          AND [Paid Search Ad ID] = Paid_Search_Ad_ID)

-----------------------------------------------------------------------------------------------------------------------------------------------
--Output File
SELECT *
INTO   #DT
FROM   UALUS_DIM_PLACEMENT
