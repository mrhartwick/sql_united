--====================================================================
-- author:      seetha srinivasan
-- create date:      13 jul 2016 08:38:36 pm et
-- Edited:           19 July 2017 by Matt Hartwick
-- description: {description}
-- comments:
--====================================================================
--delete existing records from the fact table (idea is to have rolling 7 days data)
DELETE FROM [ualus_search_standard]
WHERE  date IN (SELECT DISTINCT Cast([date] AS DATE)
                FROM   [dbo].[dfid041780_ualus_search_standard_extracted])

--transform/cast data types and insert cleansed data into the fact table
INSERT INTO [ualus_search_standard]
SELECT Cast([date] AS DATE)                                  AS date,
       Cast([week] AS DATE)                                  AS [week],
       Cast([paid search engine account id] AS NVARCHAR(50)) AS PdSearch_EngineAccount_ID,
       Cast([paid search campaign id] AS NVARCHAR(50))       AS PdSearch_Campaign_ID,
       Cast([paid search keyword id] AS NVARCHAR(50))        AS PdSearch_Keyword_ID,
       Cast([paid search ad group id] AS NVARCHAR(50))       AS PdSearch_AdGroup_ID,
       Cast([paid search ad id] AS NVARCHAR(50))             AS PdSearch_Ad_ID,
       Cast([paid search advertiser id] AS NVARCHAR(50))     AS PdSearch_Advertiser_ID,
       Cast([paid search match type] AS NVARCHAR(50))        AS PdSearch_MatchType,
       Cast([site id (dcm)] AS NVARCHAR(50))                 AS SiteID_DCM,
       Cast([site id (site directory)] AS NVARCHAR(50))      AS SiteID_SiteDirectory,
       Cast([campaign id] AS NVARCHAR(50))                   AS Campaign_ID,
       Cast([placement id] AS NVARCHAR(50))                  AS Placement_ID,
       case when isnumeric([paid search impressions]) = 1         then cast(replace(replace([paid search impressions],'.0','0'),'.00','0') as bigint) else 0 end                                                   as pdsearch_impressions,
       case when isnumeric([paid search clicks]) = 1              then cast(replace(replace([paid search clicks],'.0','0'),'.00','0')      as bigint) else 0 end                                                   as pdsearch_clicks,
       case when isnumeric([paid search cost]) = 1                then cast([paid search cost] as decimal(38, 2)) else 0 end                                                   as pdsearch_cost,
       case when isnumeric([paid search revenue]) = 1             then cast([paid search revenue] as decimal(38, 2)) else 0 end                                                   as pdsearch_revenue,
       case when isnumeric([paid search average position]) = 1    then cast([paid search average position] as decimal(38, 2)) else 0 end                                                   as pdsearch_avg_position,
       case when isnumeric([paid search transactions]) = 1        then cast([paid search transactions] as decimal(38, 2)) else 0 end                                                   as pdsearch_transactions,
       acquireid
FROM   [dbo].[dfid041780_ualus_search_standard_extracted]
WHERE  Cast([date] AS DATE) NOT IN (SELECT DISTINCT [date]
                                    FROM   ualus_search_standard)

SELECT Max([date]) activitydate
INTO   #DT
FROM   ualus_search_standard
