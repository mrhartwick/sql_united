--====================================================================
-- Author:    Seetha Srinivasan
-- Create date:      12 Jul 2016 05:55:21 PM ET
-- Edited:           19 July 2017 by Matt Hartwick
-- Description: {Description}
-- Comments:
--====================================================================
--Delete existing records from the Fact Table (Idea is to have rolling 7 days data)
DELETE FROM ualus_search_floodlight
WHERE  date IN (SELECT DISTINCT Cast([date] AS DATE)
                FROM   [dfid041761_ualus_search_floodlight_extracted])

--transform/cast data types and insert cleansed data into the fact table
INSERT INTO ualus_search_floodlight
SELECT
       Cast(date AS DATE)                                    AS "date",
       Cast([activity id] AS NVARCHAR(50))                   as activity_id,
       Cast([placement id] AS NVARCHAR(50))                  as placement_id,
       Cast([campaign id] AS NVARCHAR(50))                   as campaign_id,
       Cast([paid search engine account id] AS NVARCHAR(50)) as pdsearch_engine_account_id,
       Cast([paid search engine account] AS NVARCHAR(1000))  as pdsearch_engine_account,
       Cast([paid search campaign id] AS NVARCHAR(50))       as pdsearch_campaign_id,
       Cast([paid search campaign] AS NVARCHAR(1000))        as pdsearch_campaign,
       Cast([paid search ad id] AS NVARCHAR(50))             as pdsearch_adid,
       Cast([paid search ad] AS NVARCHAR(1000))              as pdsearch_ad,
       Cast([paid search keyword id] AS NVARCHAR(50))        as pdsearch_keywordid,
       Cast([paid search keyword] AS NVARCHAR(1000))         as pdsearch_keyword,
       Cast([site id (dcm)] AS NVARCHAR(50))                 as siteid_dcm,
       Cast([site (dcm)] AS NVARCHAR(1000))                  as site_dcm,
       Cast([paid search advertiser id] AS NVARCHAR(50))     as pdsearch_advertiser_id,
       Cast([paid search advertiser] AS NVARCHAR(1000))      as pdsearch_advertiser,
       Cast([paid search ad group id] AS NVARCHAR(50))       as pdsearch_adgroup_id,
       Cast([paid search ad group] AS NVARCHAR(1000))        as pdsearch_adgroup,
       Cast([site id (site directory)] AS NVARCHAR(50))      as siteid_sitedirectory,
       Cast([site (site directory)] AS NVARCHAR(1000))       as site_sitedirectory,
       CASE WHEN Isnumeric([revenue (string)]) = 1 THEN Cast([revenue (string)] AS DECIMAL(38, 2)) ELSE 0 END as revenue_string,
       Cast([currency (string)] AS NVARCHAR(50))             as currency,
       CASE WHEN Isnumeric([number of tickets (string)]) = 1 THEN Cast(replace(replace([number of tickets (string)],'.0','0'),'.00','0') AS INT) ELSE 0 END as number_of_tickets,
       Cast([origin_1 (string)] AS NVARCHAR(50))             as origin_1,
       Cast([origin_2 (string)] AS NVARCHAR(50))             as origin_2,
       CASE WHEN Isnumeric([total conversions]) = 1 THEN Cast(replace(replace([total conversions],'.0','0'),'.00','0') AS INT) ELSE 0 END as total_conversions,
       CASE WHEN Isnumeric([total revenue]) = 1 THEN Cast([total revenue] AS DECIMAL(38, 2)) ELSE 0 END as total_revenue,
       CASE WHEN Isnumeric([transaction count]) = 1 THEN Cast(replace(replace([transaction count],'.0','0'),'.00','0') AS INT) ELSE 0 END as total_conversions,
       CASE WHEN Isnumeric([revenue (number)]) = 1 THEN Cast([revenue (number)] AS DECIMAL(38, 2)) ELSE 0 END as revenue_number,
       Cast([click-through revenue] AS DECIMAL(38, 2))       as clickthrough_revenue,
       CASE WHEN Isnumeric([click-through conversions]) = 1 THEN Cast([click-through conversions] AS DECIMAL(20, 10)) ELSE 0 END as clickthrough_conversions,
       CASE WHEN Isnumeric([click-through transaction count]) = 1 THEN Cast(replace(replace([click-through transaction count],'.0','0'),'.00','0') AS INT) ELSE 0 END as clickthrough_transaction_count
FROM   [dbo].[dfid041761_ualus_search_floodlight_extracted]
WHERE  Cast([date] AS DATE) NOT IN (SELECT DISTINCT date
                                    FROM   ualus_search_floodlight)

SELECT Max(date) activity_date
INTO   #DT
FROM   ualus_search_floodlight
