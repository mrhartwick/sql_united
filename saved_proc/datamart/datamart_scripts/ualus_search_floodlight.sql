--====================================================================
-- Author:    Seetha Srinivasan
-- Create date:      12 Jul 2016 05:55:21 PM ET
-- Description: {Description}
-- Comments:
--====================================================================
--Delete existing records from the Fact Table (Idea is to have rolling 7 days data)
DELETE FROM UALUS_Search_Floodlight
WHERE  Activity_Date IN (SELECT DISTINCT Cast([Activity Date/Time] AS DATE)
                         FROM   [DFID041761_UALUS_Search_Floodlight_Extracted])

--Transform/Cast data types and insert cleansed data into the Fact Table
INSERT INTO UALUS_Search_Floodlight
SELECT Cast(Date AS DATE)                                    AS Date,
       Cast([Activity ID] AS NVARCHAR(50))                   Activity_ID,
       Cast([Activity Date/Time] AS DATE)                    Activity_Date,
       Cast([Placement ID] AS NVARCHAR(50))                  Placement_ID,
       Cast([Campaign ID] AS NVARCHAR(50))                   Campaign_ID,
       Cast([Paid Search Engine Account ID] AS NVARCHAR(50)) PdSearch_Engine_Account_ID,
       Cast([Paid Search Engine Account] AS NVARCHAR(1000))  PdSearch_Engine_Account,
       Cast([Paid Search Campaign ID] AS NVARCHAR(50))       PdSearch_Campaign_ID,
       Cast([Paid Search Campaign] AS NVARCHAR(1000))        PdSearch_Campaign,
       Cast([Paid Search Ad ID] AS NVARCHAR(50))             PdSearch_AdId,
       Cast([Paid Search Ad] AS NVARCHAR(1000))              PdSearch_Ad,
       Cast([Paid Search Keyword ID] AS NVARCHAR(50))        PdSearch_KeywordID,
       Cast([Paid Search Keyword] AS NVARCHAR(1000))         PdSearch_Keyword,
       Cast([Site ID (DCM)] AS NVARCHAR(50))                 SiteID_DCM,
       Cast([Site (DCM)] AS NVARCHAR(1000))                  Site_DCM,
       Cast([Paid Search Advertiser ID] AS NVARCHAR(50))     PdSearch_Advertiser_ID,
       Cast([Paid Search Advertiser] AS NVARCHAR(1000))      PdSearch_Advertiser,
       Cast([Paid Search Ad Group ID] AS NVARCHAR(50))       PdSearch_AdGroup_ID,
       Cast([Paid Search Ad Group] AS NVARCHAR(1000))        PdSearch_AdGroup,
       Cast([Site ID (Site Directory)] AS NVARCHAR(50))      SiteID_SiteDirectory,
       Cast([Site (Site Directory)] AS NVARCHAR(1000))       Site_SiteDirectory,
       CASE
         WHEN Isnumeric([Revenue (string)]) = 1 THEN Cast([Revenue (string)] AS DECIMAL(38, 2))
         ELSE 0
       END                                                   Revenue_String,
       Cast([Currency (string)] AS NVARCHAR(50))             Currency,
       Cast([PNR_Base64Encoded (string)] AS NVARCHAR(50))    PNR_Base64_String,
       CASE
         WHEN Isnumeric([Number of Tickets (string)]) = 1 THEN Cast([Number of Tickets (string)] AS INT)
         ELSE 0
       END                                                   Number_of_Tickets,
       Cast([Origin_1 (string)] AS NVARCHAR(50))             Origin_1,
       Cast([Origin_2 (string)] AS NVARCHAR(50))             Origin_2,
       CASE
         WHEN Isnumeric([Total Conversions]) = 1 THEN Cast([Total Conversions] AS INT)
         ELSE 0
       END                                                   Total_Conversions,
       CASE
         WHEN Isnumeric([Total Revenue]) = 1 THEN Cast([Total Revenue] AS DECIMAL(38, 2))
         ELSE 0
       END                                                   Total_Revenue,
       Cast([Transaction Count] AS DECIMAL(38, 2))           Transaction_Count,
       CASE
         WHEN Isnumeric([Revenue (number)]) = 1 THEN Cast([Revenue (number)] AS DECIMAL(38, 2))
         ELSE 0
       END                                                   Revenue_Number,
       Cast([PNR_Base64Encoded (number)] AS DECIMAL(38, 2))  PNR_Base64_Number,
       Cast([Click-through Revenue] AS DECIMAL(38, 2))       ClickThrough_Revenue,
       CASE
         WHEN Isnumeric([Click-through Conversions]) = 1 THEN Cast([Click-through Conversions] AS INT)
         ELSE 0
       END                                                   ClickThrough_Conversions,
       CASE
         WHEN Isnumeric([Click-through Transaction Count]) = 1 THEN Cast([Click-through Transaction Count] AS INT)
         ELSE 0
       END                                                   ClickThrough_TransactionCount,
       AcquireID
FROM   [dbo].[DFID041761_UALUS_Search_Floodlight_Extracted]
WHERE  Cast([Activity Date/Time] AS DATE) NOT IN (SELECT DISTINCT Activity_Date
                                                  FROM   UALUS_Search_Floodlight)

SELECT Max(Activity_Date) Activity_Date
INTO   #DT
FROM   UALUS_Search_Floodlight
