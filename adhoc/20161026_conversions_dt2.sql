SELECT

cast(md_event_time as date) as "Date"
,campaign_id AS campaign_id
,other_data as other_data
,Conversions.site_id_dcm AS site_id_dcm
,Conversions.placement_id AS placement_id
,SUM(Conversions.total_revenue*1000000) AS total_revenue

FROM
(

SELECT *
FROM mec.doubleclickv2_UnitedUS.dfa_activity
WHERE cast(md_event_time as date) = '2016-09-14'
AND activity_id = '978826'
AND (advertiser_id <> 0)
) AS Conversions

GROUP BY
cast(md_event_time as date)
,Conversions.campaign_id
,conversions.other_data
,Conversions.site_id_dcm
,Conversions.placement_id
