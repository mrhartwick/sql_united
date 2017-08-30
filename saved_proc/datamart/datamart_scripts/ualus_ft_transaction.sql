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
select cast(date as date)                      as ftdate,
       cast(campaign_name as nvarchar(1000))   as campaign_name,
       case when isnumeric(idcampaign) = 1 then cast(idcampaign as int) else 0 end                                     as campaign_id,
       cast(site_name as nvarchar(1000))       as site_name,
       case when isnumeric(idsite) = 1 then cast(idsite as int) else 0 end                                     as site_id,
       cast(ad_size as nvarchar(1000))         as ad_size,
       cast(placement as nvarchar(1000))       as placement,
       case when isnumeric(idplacement) = 1 then cast(idplacement as int) else 0 end                                     as placement_id,
       cast(creative as nvarchar(1000))        as creative,
       case when isnumeric(idcreative) = 1 then cast(idcreative as int) else 0 end                                     as creative_id,
       cast(version as nvarchar(1000))         as creative_ver,
       case when isnumeric(idcreative_config) = 1 then cast(idcreative_config as int) else 0 end                                     as creative_ver_id,
       case when isnumeric(impressions_delivered) = 1 then cast(impressions_delivered as int) else 0 end                                     as delivered_impressions,
       case when isnumeric(clicks) = 1 then cast(clicks as int) else 0 end                                     as clicks,
       case when isnumeric(desktop_impressions) = 1 then cast(desktop_impressions as int) else 0 end                                     as desktop_impressions,
       case when isnumeric(mobile_web_impressions) = 1 then cast(mobile_web_impressions as int) else 0 end                                     as mobile_web_impressions,
       case when isnumeric(mobile_app_impressions) = 1 then cast(mobile_app_impressions as int) else 0 end                                     as mobile_app_impressions,
       case when isnumeric(affiliate_click) = 1 then cast(affiliate_click as int) else 0 end                                     as affiliate_click,
       case when isnumeric(search_integration_click) = 1 then cast(search_integration_click as int) else 0 end                                     as search_integration_click,
       case when isnumeric(ad_visible_over_3_seconds) = 1 then cast(ad_visible_over_3_seconds as int) else 0 end                                     as ad_visible_over_3_seconds,
       cast(replace(total_expand_time,'.',':') as time(0))      as total_expand_time,
--    total_expand_time,
       case when isnumeric(expand_views) = 1 then cast(expand_views as int) else 0 end                                     as expand_views,
       case when isnumeric(fullscreen) = 1 then cast(fullscreen as int) else 0 end                                     as fullscreen,
       case when isnumeric(interactions_unique_by_impression) = 1 then cast(interactions_unique_by_impression as int) else 0 end                                     as interactions_unique_by_impression,
    case
    when isnumeric(left(total_interaction_time,3)) = 1 then '00:00:00'
    when cast(left(total_interaction_time,2) as int) > 23  then '00:00:00'
    else cast(total_interaction_time as time(0)) end as total_interaction_time,
       case when isnumeric(interaction_rate) = 1 then cast(interaction_rate as decimal(20, 10)) else 0 end                                     as interaction_rate
from   [dbo].[dfid060193_ft_summ_extracted_post]
where  cast([date] as date) not in (select distinct ftdate
                                    from   ualus_ft_summary)

SELECT Max(ftDate) ftDate
INTO   #DT
FROM   ualus_ft_summary
