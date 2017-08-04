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
SELECT Cast(date AS DATE)                                    AS date,
       Cast([activity id] AS NVARCHAR(50))                   activity_id,
       -- cast([activity date/time] as date)                    activity_date,
       Cast([placement id] AS NVARCHAR(50))                  placement_id,
       Cast([campaign id] AS NVARCHAR(50))                   campaign_id,
       Cast([paid search engine account id] AS NVARCHAR(50)) pdsearch_engine_account_id,
       Cast([paid search engine account] AS NVARCHAR(1000))  pdsearch_engine_account,
       Cast([paid search campaign id] AS NVARCHAR(50))       pdsearch_campaign_id,
       Cast([paid search campaign] AS NVARCHAR(1000))        pdsearch_campaign,
       Cast([paid search ad id] AS NVARCHAR(50))             pdsearch_adid,
       Cast([paid search ad] AS NVARCHAR(1000))              pdsearch_ad,
       Cast([paid search keyword id] AS NVARCHAR(50))        pdsearch_keywordid,
       Cast([paid search keyword] AS NVARCHAR(1000))         pdsearch_keyword,
       Cast([site id (dcm)] AS NVARCHAR(50))                 siteid_dcm,
       Cast([site (dcm)] AS NVARCHAR(1000))                  site_dcm,
       Cast([paid search advertiser id] AS NVARCHAR(50))     pdsearch_advertiser_id,
       Cast([paid search advertiser] AS NVARCHAR(1000))      pdsearch_advertiser,
       Cast([paid search ad group id] AS NVARCHAR(50))       pdsearch_adgroup_id,
       Cast([paid search ad group] AS NVARCHAR(1000))        pdsearch_adgroup,
       Cast([site id (site directory)] AS NVARCHAR(50))      siteid_sitedirectory,
       Cast([site (site directory)] AS NVARCHAR(1000))       site_sitedirectory,
       CASE
         WHEN Isnumeric([revenue (string)]) = 1 THEN Cast([revenue (string)] AS DECIMAL(38, 2))
         ELSE 0
       END                                                   revenue_string,
       Cast([currency (string)] AS NVARCHAR(50))             currency,
       -- cast([pnr_base64encoded (string)] as nvarchar(50))    pnr_base64_string,
       CASE
         WHEN Isnumeric([number of tickets (string)]) = 1 THEN Cast([number of tickets (string)] AS INT)
         ELSE 0
       END                                                   number_of_tickets,
       Cast([origin_1 (string)] AS NVARCHAR(50))             origin_1,
       Cast([origin_2 (string)] AS NVARCHAR(50))             origin_2,
       CASE
         WHEN Isnumeric([total conversions]) = 1 THEN Cast([total conversions] AS INT)
         ELSE 0
       END                                                   total_conversions,
       CASE
         WHEN Isnumeric([total revenue]) = 1 THEN Cast([total revenue] AS DECIMAL(38, 2))
         ELSE 0
       END                                                   total_revenue,
       Cast([transaction count] AS DECIMAL(38, 2))           transaction_count,
       CASE
         WHEN Isnumeric([revenue (number)]) = 1 THEN Cast([revenue (number)] AS DECIMAL(38, 2))
         ELSE 0
       END                                                   revenue_number,
       -- cast([pnr_base64encoded (number)] as decimal(38, 2))  pnr_base64_number,
       Cast([click-through revenue] AS DECIMAL(38, 2))       clickthrough_revenue,
       CASE
         WHEN Isnumeric([click-through conversions]) = 1 THEN Cast([click-through conversions] AS DECIMAL(20, 10))
         ELSE 0
       END                                                   clickthrough_conversions,
       CASE
         WHEN Isnumeric([click-through transaction count]) = 1 THEN Cast([click-through transaction count] AS INT)
         ELSE 0
       END                                                   clickthrough_transactioncount
FROM   [dbo].[dfid041761_ualus_search_floodlight_extracted]
WHERE  Cast([date] AS DATE) NOT IN (SELECT DISTINCT date
                                    FROM   ualus_search_floodlight)

SELECT Max(date) activity_date
INTO   #DT
FROM   ualus_search_floodlight
