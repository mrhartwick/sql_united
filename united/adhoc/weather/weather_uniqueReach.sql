





select
                date,
                weather_cat,
                weather_region,
                sum(leads) as leads,
                sum(imps) as imps,
                count(b.user_id) as users,
                count(distinct b.user_id) as unique_users
from(
                select
                                cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date ) as "date",
                                user_id,

                                case
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_Test_.*' ,'ib') then 'Test'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_Control_.*' ,'ib') then 'Control'
                                end                                                                                                as weather_cat,

                                case
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_PAC_.*' ,'ib') then 'Pacific'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_MTN_.*' ,'ib') then 'Mountain'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_WNC_.*' ,'ib') then 'West North Central'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_WSC_.*' ,'ib') then 'West South Central'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_ENC_.*' ,'ib') then 'East North Central'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_NEW_.*' ,'ib') then 'New England'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_MID_.*' ,'ib') then 'Mid Atlantic'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_SAT_.*' ,'ib') then 'South Atlantic'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_ESC_.*' ,'ib') then 'East South Central'
                                    else 'Other'
                                end                                                                                                as weather_region,
                                count(*) as leads,
                                0 as imps
                from
                                diap01.mec_us_united_20056.dfa2_activity    as t1

                left join
                                diap01.mec_us_united_20056.dfa2_placements  as p1
                                on t1.placement_id = p1.placement_id
                where
                                t1.activity_id = 1086066 and
                                t1.site_id_dcm = 1853562 and
                                user_id != '0' and


                                cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2018-01-01' and '2018-01-25' and
                                advertiser_id <> 0 and
                                (length(isnull (event_sub_type,'')) > 0)
                group by
                                cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date ),
                                user_id,
                                p1.placement

union all

                select
                                cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date ) as "date",
                                user_id,

                                case
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_Test_.*' ,'ib') then 'Test'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_Control_.*' ,'ib') then 'Control'
                                end                                     as weather_cat,

                                case
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_PAC_.*' ,'ib') then 'Pacific'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_MTN_.*' ,'ib') then 'Mountain'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_WNC_.*' ,'ib') then 'West North Central'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_WSC_.*' ,'ib') then 'West South Central'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_ENC_.*' ,'ib') then 'East North Central'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_NEW_.*' ,'ib') then 'New England'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_MID_.*' ,'ib') then 'Mid Atlantic'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_SAT_.*' ,'ib') then 'South Atlantic'
                                when regexp_like(p1.placement, '.*_Weather_.*' ,'ib') and regexp_like(p1.placement, '.*_ESC_.*' ,'ib') then 'East South Central'
                                    else 'Other'
                                end                                                                                                as weather_region,

                                0 as leads,
                                count(*) as imps
                from
                                diap01.mec_us_united_20056.dfa2_impression      as t2
                left join
                                diap01.mec_us_united_20056.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                where
                                user_id != '0' and

                                t2.site_id_dcm = 1853562 and
                                cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-01-01' and '2018-01-25' and
                                advertiser_id <> 0
                group by
                                cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date ),
                                user_id,
                                p1.placement
                ) as b


group by
                date,
                weather_cat,
                weather_region
