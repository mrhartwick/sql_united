select
--  rpt.source                       as source,
--  rpt.date                         as "date",
    rpt.week                         as "week",
    rpt.engine_id                    as engine_id,
--  rpt.Placement_ID                 as placement_id,
    rpt.Campaign                     as campaign,
--  c1.Paid_Search_Campaign          as campaign,
--  rpt.campaign_id                  as campaign_id,
--  k1.Paid_Search_Keyword,
    rpt.keyword                      as keyword,
--  k1.Paid_Search_Keyword as keyword,
-- rpt.keyword_id as keyword_id,
    rpt.origin_1                     as origin_1,
    rpt.origin_2                     as origin_2,
    rpt.origin_1+' to '+rpt.origin_2 as route,
    rpt.adgroup                      as adgroup,
--  ad1.Paid_Search_AdGroup          as ad_group,
--  rpt.adgroup_id                   as adgroup_id,
    sum(rpt.imps)                    AS imps,
    sum(rpt.clicks)                  AS clicks,
    avg(rpt.avg_pos)                 AS avg_pos,
    sum(rpt.rev)                     AS rev,
    sum(rpt.trans)                   AS trans,
    sum(rpt.tix)                     AS tix


from (
         SELECT
--              'standard' as source,
             cast(std1.date AS DATE)                                                as "date",
             cast(dateadd(week,datediff(week,0,cast(std1.date as date)),0) as date) as "week",
             std1.PdSearch_EngineAccount_ID                                         as engine_id,
--              std1.Placement_ID               as placement_id,
             c1.Paid_Search_Campaign                                                as campaign,
             std1.PdSearch_Campaign_ID                                              as campaign_id,
--             right(std1.PdSearch_Keyword_ID,10) as keyword_match_id,
             std1.PdSearch_Keyword_ID                                               as keyword_id,
             k1.Paid_Search_Keyword                                                 as keyword,
             ''                                                                     as origin_1,
             ''                                                                     as origin_2,
             std1.PdSearch_AdGroup_ID                                               as adgroup_id,
             ''                                                                     as adgroup,
             sum(std1.PdSearch_Impressions)                                         AS imps,
             sum(std1.PdSearch_Clicks)                                              AS clicks,
             avg(std1.PdSearch_Avg_Position)                                        AS avg_pos,
             0                                                                      as rev,
             0                                                                      as trans,
             0                                                                      as tix
         -- sum(fld2.Number_of_Tickets)     AS tix,
         -- sum(fld2.Transaction_Count) AS Transaction_Count

         FROM (select *
               from DM_1161_UnitedAirlinesUSA.dbo.UALUS_Search_Standard
               where
                   cast(date AS DATE) BETWEEN '2016-08-01' AND '2016-08-31'
                   --       and std1.PdSearch_EngineAccount_ID is not null
                   --                    AND PdSearch_Campaign_ID='71700000006702337'
                   AND PdSearch_EngineAccount_ID!='0'
-- 	  AND std1.PdSearch_Advertiser_ID != '0'
-- 	  AND std1.PdSearch_Campaign_ID != '0'
              ) AS std1
             LEFT JOIN DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchKeyword AS k1
                 ON right(std1.PdSearch_Keyword_ID,10)=right(k1.Paid_Search_Keyword_ID,10)
-- --                  ON std1.PdSearch_Keyword_ID=k1.Paid_Search_Keyword_ID
--
--              DUPLICATES!!!
--             LEFT JOIN DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchAd AS ad1
--         ON std1.PdSearch_AdGroup_ID = ad1.Paid_Search_AdGroup_ID
--
             LEFT JOIN DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchCampaign AS c1
                 ON std1.PdSearch_Campaign_ID=c1.Paid_Search_Campaign_ID

         GROUP BY
             cast(std1.date AS DATE),

             std1.PdSearch_EngineAccount_ID,
--              std1.Placement_ID,
             k1.Paid_Search_Keyword,
             c1.Paid_Search_Campaign,
--              ad1.Paid_Search_AdGroup,
             std1.PdSearch_Campaign_ID,
--              right(std1.PdSearch_Keyword_ID,10),
             std1.PdSearch_Keyword_ID,
--              std1.PdSearch_Ad_ID,
             std1.PdSearch_AdGroup_ID

         union all

         select
--              'floodlight' as source,
             cast(fld1.Activity_Date AS DATE)                                                AS "Date",
             cast(dateadd(week,datediff(week,0,cast(fld1.Activity_Date as date)),0) as date) as "week",
             fld1.PdSearch_EngineAccount_ID                                                  as engine_id,
--              fld1.Placement_ID                as placement_id,
             fld1.PdSearch_Campaign                                                          as campaign,
             fld1.PdSearch_Campaign_ID                                                       as campaign_id,
--              right(fld1.PdSearch_Keyword_ID,10) as keyword_match_id,
             fld1.PdSearch_Keyword_ID                                                        as keyword_id,
--              k2.Paid_Search_Keyword           as keyword,
             fld1.Origin_1_string                                                            as origin_1,
             fld1.Origin_2_string                                                            as origin_2,
--              ad2.Paid_Search_AdGroup as ad_group,
             fld1.PdSearch_AdGroup_ID                                                        as adgroup_id,
             fld1.PdSearch_AdGroup                                                           as adgroup,
             0                                                                               as imps,
             0                                                                               as clicks,
             0                                                                               as avg_pos,
-- 	fld1.Currency,
             sum(fld1.Total_Revenue)                                                         AS rev,
             sum(fld1.Transaction_Count)                                                     AS trans,
             sum(fld1.Number_of_Tickets)                                                     AS tix

         FROM (select *
               from DM_1161_UnitedAirlinesUSA.dbo.UALUS_Search_Floodlight

               WHERE cast(Activity_Date AS DATE) BETWEEN '2016-08-01' AND '2016-08-31'
                     --       and PdSearch_EngineAccount_ID is not null
                     --           AND PdSearch_Advertiser_ID!='0'
                     AND PdSearch_EngineAccount_ID!='0'
                     AND Currency!='Miles'
                     --        AND Activity_ID='978826'
                     --           AND Currency IS NOT NULL
                     AND Currency!='--'
--                      AND PdSearch_Campaign_ID='71700000006702337'
              ) AS fld1

--        AND fld1.PdSearch_Campaign_ID!='0'
             LEFT JOIN DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchKeyword AS k2
-- --                  ON fld1.PdSearch_Keyword_ID=k1.Paid_Search_Keyword_ID
                 ON right(fld1.PdSearch_Keyword_ID,10)=right(k2.Paid_Search_Keyword_ID,10)
--
--         LEFT JOIN DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchAd AS ad2
--         ON fld1.PdSearch_AdGroup_ID = ad2.Paid_Search_AdGroup_ID
--
--         LEFT JOIN DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchCampaign AS c2
--         ON fld1.PdSearch_Campaign_ID = c2.Paid_Search_Campaign_ID
--
         GROUP BY
             cast(fld1.Activity_Date AS DATE),
             fld1.PdSearch_EngineAccount_ID,
--              fld1.Placement_ID,
-- 	fld1.Currency,
             fld1.Origin_1_string,
             fld1.Origin_2_string,
             fld1.PdSearch_Campaign,
             k2.Paid_Search_Keyword,
--              ad2.Paid_Search_AdGroup,
--              c2.Paid_Search_Campaign,
             fld1.PdSearch_AdGroup,
             fld1.PdSearch_AdGroup_ID,
--              right(fld1.PdSearch_Keyword_ID,10),
             fld1.PdSearch_Keyword_ID,
             fld1.PdSearch_Campaign_ID
--              fld1.PdSearch_Campaign

     ) AS rpt


--     LEFT JOIN DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchCampaign AS c1
--         ON rpt.campaign_id=c1.Paid_Search_Campaign_ID

--              LEFT JOIN DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchKeyword AS k1
--         ON right(rpt.keyword_id,10) = right(k1.Paid_Search_Keyword_ID,10)
--                  ON std1.PdSearch_Keyword_ID=k1.Paid_Search_Keyword_ID

--             LEFT JOIN DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchAd AS ad1
--         ON rpt.adgroup_id = ad1.Paid_Search_AdGroup_ID

    LEFT JOIN DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchCampaign AS c1
        ON rpt.campaign_id=c1.Paid_Search_Campaign_ID

--     LEFT JOIN DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchKeyword AS k1
--         ON rpt.keyword_id = right(k1.Paid_Search_Keyword_ID,10)
-- WHERE
-- cast(std1.date AS DATE) BETWEEN '2016-08-01' AND '2016-08-31'
--       --       and std1.PdSearch_EngineAccount_ID is not null
--
--  AND std1.PdSearch_Campaign_ID='71700000006702337'
--       AND std1.PdSearch_EngineAccount_ID!='0'
--       -- 	  AND std1.PdSearch_Advertiser_ID != '0'
--       -- 	  AND std1.PdSearch_Campaign_ID != '0'
--       AND fld2.PdSearch_Campaign_ID!='0'
--       AND fld2.PdSearch_EngineAccount_ID!='0'
where c1.Paid_Search_Campaign='New York - Newark - Remarketing'


GROUP BY
--     rpt.source,
--     rpt.date,
    rpt.adgroup,
--     rpt.campaign_id,
--     ad1.Paid_Search_AdGroup,
    rpt.week,
    rpt.Campaign,
--     c1.Paid_Search_Campaign,
    rpt.engine_id,
--     rpt.keyword_id,
--     rpt.Placement_ID,
--     c1.Paid_Search_Campaign,
--     rpt.Campaign,
--     rpt.campaign_id,
--     k1.Paid_Search_Keyword,
    rpt.keyword,
-- rpt.keyword_id	,
-- k1.Paid_Search_Keyword
    rpt.origin_1,
    rpt.origin_2
--     rpt.adgroup_id
