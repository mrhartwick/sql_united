----to load MOAT data after uploading files to s3 bucket:

INSERT INTO mec_us_united_20056.moat_impression
 SELECT *
FROM
 (
 SELECT mb.event_date,
        mb.campaign_id,
        mb.campaign_label,
        mb.site_id,
        mb.site_label,
        mb.placement_id,
        mb.placement_label,
        mb.human_impressions,
        mb.human_viewable_impressions,
        mb.human_full_onscreen_impressions,
        mb.groupm_payable_impressions,
        mb.md_acquire_id
FROM mec_us_united_20056.s3_moat_staging_mobile mb
 UNION ALL
 SELECT vd.event_date,
        vd.campaign_id,
        vd.campaign_label,
        vd.site_id,
        vd.site_label,
        vd.placement_id,
        vd.placement_label,
        vd.human_impressions,
        vd.human_full_onscreen_impressions,
        vd.human_half_duration,
        vd.groupm_payable_impressions,
        vd.md_acquire_id
FROM mec_us_united_20056.s3_moat_staging_video vd
) AS pre_agg
WHERE not exists (select  'x'
                  from    mec_us_united_20056.moat_impression ip
                  where   ip.placement_id = pre_agg.placement_id
                  and     ip.event_date = pre_agg.event_date);
