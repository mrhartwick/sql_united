--====================================================================
-- Author:    Matthew Hartwick
-- Create date:      04 Aug 2017 12:47:20 PM ET
-- Description: {Description}
-- Comments:
--====================================================================
DELETE FROM ualus_ft_summary
WHERE  ftDate IN (SELECT DISTINCT [date]
                  FROM   [DFID060193_FT_Summ_Extracted_Post])

--transform/cast data types and insert cleansed data into the fact table
INSERT INTO ualus_ft_summary
SELECT Cast(date AS DATE)                      AS ftDate,
       Cast(campaign_name AS NVARCHAR(1000))   AS campaign_name,
       CASE
         WHEN Isnumeric(idcampaign) = 1 THEN Cast(idcampaign AS INT)
         ELSE 0
       END                                     AS campaign_id,
       Cast(site_name AS NVARCHAR(1000))       AS site_name,
       CASE
         WHEN Isnumeric(idsite) = 1 THEN Cast(idsite AS INT)
         ELSE 0
       END                                     AS site_id,
       Cast(ad_size AS NVARCHAR(1000))         AS ad_size,
       Cast(placement AS NVARCHAR(1000))       AS placement,
       CASE
         WHEN Isnumeric(idplacement) = 1 THEN Cast(idplacement AS INT)
         ELSE 0
       END                                     AS placement_id,
       Cast(creative AS NVARCHAR(1000))        AS creative,
       CASE
         WHEN Isnumeric(idcreative) = 1 THEN Cast(idcreative AS INT)
         ELSE 0
       END                                     AS creative_id,
       Cast(version AS NVARCHAR(1000))         AS creative_ver,
       CASE
         WHEN Isnumeric(idcreative_config) = 1 THEN Cast(idcreative_config AS INT)
         ELSE 0
       END                                     AS creative_ver_id,
       CASE
         WHEN Isnumeric(impressions_delivered) = 1 THEN Cast(impressions_delivered AS INT)
         ELSE 0
       END                                     AS delivered_impressions,
       CASE
         WHEN Isnumeric(clicks) = 1 THEN Cast(clicks AS INT)
         ELSE 0
       END                                     AS clicks,
       CASE
         WHEN Isnumeric(desktop_impressions) = 1 THEN Cast(desktop_impressions AS INT)
         ELSE 0
       END                                     AS desktop_impressions,
       CASE
         WHEN Isnumeric(mobile_web_impressions) = 1 THEN Cast(mobile_web_impressions AS INT)
         ELSE 0
       END                                     AS mobile_web_impressions,
       CASE
         WHEN Isnumeric(mobile_app_impressions) = 1 THEN Cast(mobile_app_impressions AS INT)
         ELSE 0
       END                                     AS mobile_app_impressions,
       CASE
         WHEN Isnumeric(affiliate_click) = 1 THEN Cast(affiliate_click AS INT)
         ELSE 0
       END                                     AS affiliate_click,
       CASE
         WHEN Isnumeric(search_integration_click) = 1 THEN Cast(search_integration_click AS INT)
         ELSE 0
       END                                     AS search_integration_click,
       CASE
         WHEN Isnumeric(ad_visible_over_3_seconds) = 1 THEN Cast(ad_visible_over_3_seconds AS INT)
         ELSE 0
       END                                     AS ad_visible_over_3_seconds,
       Cast(total_expand_time AS TIME(0))      AS total_expand_time,
       CASE
         WHEN Isnumeric(expand_views) = 1 THEN Cast(expand_views AS INT)
         ELSE 0
       END                                     AS expand_views,
       CASE
         WHEN Isnumeric(fullscreen) = 1 THEN Cast(fullscreen AS INT)
         ELSE 0
       END                                     AS fullscreen,
       CASE
         WHEN Isnumeric(interactions_unique_by_impression) = 1 THEN Cast(interactions_unique_by_impression AS INT)
         ELSE 0
       END                                     AS interactions_unique_by_impression,
       Cast(total_interaction_time AS TIME(0)) AS total_interaction_time,
       CASE
         WHEN Isnumeric(interaction_rate) = 1 THEN Cast(interaction_rate AS DECIMAL(20, 10))
         ELSE 0
       END                                     AS interaction_rate
FROM   [dbo].[DFID060193_FT_Summ_Extracted_Post]
WHERE  Cast([Date] AS DATE) NOT IN (SELECT DISTINCT ftDate
                                    FROM   ualus_ft_summary)

SELECT Max(ftDate) ftDate
INTO   #DT
FROM   ualus_ft_summary
