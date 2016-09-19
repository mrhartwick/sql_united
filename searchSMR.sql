SELECT
	cast(std1.date AS DATE),

	std1.PdSearch_EngineAccount_ID,
	std1.Placement_ID,
	c1.Paid_Search_Campaign,
	std1.PdSearch_Campaign_ID,
	std1.PdSearch_Keyword_ID,
	std1.PdSearch_Ad_ID,
	std1.PdSearch_AdGroup_ID,
-- 		std1.PdSearch_AdGroup_ID,
	sum(std1.PdSearch_Impressions)  AS imps,
	sum(std1.PdSearch_Clicks)       AS clicks,
	avg(std1.PdSearch_Avg_Position) AS avg_pos,
	sum(fld2.Number_of_Tickets)     AS tix

FROM DM_1161_UnitedAirlinesUSA.dbo.UALUS_Search_Standard AS std1

	LEFT JOIN DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchCampaign AS c1
		ON std1.PdSearch_Campaign_ID = c1.Paid_Search_Campaign_ID

	LEFT JOIN
	(SELECT
		 cast(fld1.date AS DATE)     AS "date",

		 fld1.PdSearch_EngineAccount_ID,
-- 	fld1.PdSearch_EngineAccount,
		fld1.PdSearch_Ad_ID,
		fld1.PdSearch_AdGroup_ID,
		 fld1.Placement_ID,
-- 	c1.Paid_Search_Campaign,
		 fld1.PdSearch_Campaign_ID,
-- 	fld1.Currency,
	fld1.PdSearch_Keyword_ID,
-- 		fld1.PdSearch_AdGroup_ID,
		 sum(fld1.Total_Conversions) AS Total_Conversions,
		 sum(fld1.Total_Revenue)     AS Total_Revenue,
		 sum(fld1.Transaction_Count) AS Transaction_Count,
		 count(*)                    AS this_count,
		 sum(Number_of_Tickets)      AS Number_of_Tickets

	 FROM DM_1161_UnitedAirlinesUSA.dbo.UALUS_Search_Floodlight AS fld1

-- 	left join DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchCampaign as c1
-- 	on fld1.PdSearch_Campaign_ID = c1.Paid_Search_Campaign_ID


	WHERE cast(fld1.date AS DATE) BETWEEN '2016-07-01' AND '2016-07-31'
		  --       and fld1.PdSearch_EngineAccount_ID is not null
		  AND fld1.PdSearch_EngineAccount_ID != '0'
		  AND fld1.Currency != 'Miles'
		  AND fld1.Currency IS NOT NULL
		  AND fld1.Currency != '--'


	GROUP BY
		cast(fld1.date AS DATE),

		fld1.PdSearch_EngineAccount_ID,
-- 	fld1.PdSearch_EngineAccount,
		fld1.Placement_ID,
-- 	c1.Paid_Search_Campaign,
-- 	fld1.Currency,
				fld1.PdSearch_Ad_ID,
		fld1.PdSearch_AdGroup_ID,
		fld1.PdSearch_Keyword_ID,
		fld1.PdSearch_Campaign_ID

	) AS fld2
		ON std1.PdSearch_Campaign_ID = fld2.PdSearch_Campaign_ID
		   AND std1.Placement_ID = fld2.Placement_ID
		   AND cast(std1.date AS DATE) = cast(fld2.date AS DATE)
		   AND std1.PdSearch_EngineAccount_ID = fld2.PdSearch_EngineAccount_ID
	AND std1.PdSearch_Keyword_ID = fld2.PdSearch_Keyword_ID
	AND std1.PdSearch_Ad_ID = fld2.PdSearch_Ad_ID
	AND std1.PdSearch_AdGroup_ID = fld2.PdSearch_AdGroup_ID

WHERE cast(std1.date AS DATE) BETWEEN '2016-07-01' AND '2016-07-31'
	  --       and std1.PdSearch_EngineAccount_ID is not null
	  AND std1.PdSearch_EngineAccount_ID != '0'
	  AND std1.PdSearch_Advertiser_ID != '0'
	  AND std1.PdSearch_Campaign_ID != '0'
	  AND fld2.PdSearch_Campaign_ID != '0'
	  AND fld2.PdSearch_EngineAccount_ID != '0'


GROUP BY
	cast(std1.date AS DATE),

	std1.PdSearch_EngineAccount_ID,
	std1.Placement_ID,
	c1.Paid_Search_Campaign,
	std1.PdSearch_Campaign_ID,
	std1.PdSearch_Keyword_ID,
	std1.PdSearch_Ad_ID,
	std1.PdSearch_AdGroup_ID