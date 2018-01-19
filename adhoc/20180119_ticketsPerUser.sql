





select
                -- a.user_id,
                -- a.site_id_dcm,
                -- conversiontime,
                -- impressiontime,
--                 count(cvr_nbr),
                    sum(trans)/count(a.user_id) as tickets_per_user,

--                 count(cvr_nbr),
                    sum(trans)/count(distinct a.user_id) as tickets_per_u_user
                -- cvr_nbr,
                -- imp_nbr
from
                (select
                                user_id,
                                -- t1.site_id_dcm,
                                interaction_time                            as conversiontime,
                                total_conversions as trans,
                                row_number() over ()                        as cvr_nbr
                from
                                diap01.mec_us_united_20056.dfa2_activity    as t1
                -- these JOINs increase runtime significantly, but we need placement names to filter Sojern's tactics
                left join
                                diap01.mec_us_united_20056.dfa2_placements  as p1
                                on t1.placement_id = p1.placement_id
                where
                                t1.activity_id = 978826 and
                                user_id != '0' and
                                t1.site_id_dcm = 1239319 and

                                cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2018-01-01' and '2018-01-12' and
                                advertiser_id <> 0 and
                                (length(isnull (event_sub_type,'')) > 0)
                ) as a,

                (select
                                user_id,
                                -- t2.site_id_dcm,
                                event_time                                      as impressiontime
                                -- row_number() over ()                            as imp_nbr
                from
                                diap01.mec_us_united_20056.dfa2_impression      as t2
                left join
                                diap01.mec_us_united_20056.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                where
                                user_id != '0' and
                                -- filter results to Prospecting tactics only
                                t2.site_id_dcm = 1239319 and
                                cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-01-01' and '2018-01-12' and
                                advertiser_id <> 0
                ) as b
where
                a.user_id = b.user_id and
                -- DCM lookback window is set to 7 days, so interaction_time and event_time already match in the log files
                conversiontime = impressiontime



