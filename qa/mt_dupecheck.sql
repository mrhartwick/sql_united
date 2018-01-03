
---To check for dupes in moat imp. table:

SELECT event_date, campaign_id, site_id, placement_id, COUNT(DISTINCT md_acquire_id)
FROM diap01.mec_us_united_20056.moat_impression
GROUP BY event_date, campaign_id, site_id, placement_id
HAVING COUNT(DISTINCT md_acquire_id) >1
ORDER BY event_date, campaign_id, site_id, placement_id
