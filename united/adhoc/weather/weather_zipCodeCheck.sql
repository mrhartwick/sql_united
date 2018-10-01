select
    date,
    res,
    count(zip_postal_code) as zips

from (
         select
             cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000), 'SS') as date)      as date,
             ti.zip_postal_code,
             w1.trigger_status                                                               as trigger,
             case when (length(isnull (w1.trigger_status, '')) = 0) then 'no' else 'yes' end as res
         from diap01.mec_us_united_20056.dfa2_impression as ti

             left join diap01.mec_us_united_20056.ual_weather_zip_triggers as w1
             on diap01.mec_us_united_20056.udf_dateToInt(cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000), 'SS') as date)) = w1.date
                 and ti.zip_postal_code = w1.zip

             left join
             diap01.mec_us_united_20056.dfa2_placements as p1
             on ti.placement_id = p1.placement_id

         where user_id != '0' and
             ti.site_id_dcm = 1853562 and
             country_code = 'US' and
             cast(timestamp_trunc(to_timestamp(event_time / 1000000), 'SS') as date) between '2018-01-02' and '2018-01-31' and
             advertiser_id <> 0 and
             regexp_like(p1.placement, '.*_Weather_.*', 'ib')
         group by
             cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000), 'SS') as date),
             ti.zip_postal_code,
             w1.trigger_status
     ) as t1

group by
    date,
    res