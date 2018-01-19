





select
                    sum(trans) as tickets,
                    count(b.user_id) as users,
                    count(distinct b.user_id) as unique_users,
                    sum(trans)/count(b.user_id) as tickets_per_user,
                    sum(trans)/count(distinct b.user_id) as tickets_per_u_user
                -- cvr_nbr,
                -- imp_nbr
from
    (select
                                user_id,
                                -- t1.site_id_dcm,
--                                 interaction_time                            as conversiontime,
                                sum(total_conversions) as trans
--                                 row_number() over ()                        as cvr_nbr
--                              count(*) as trans
                from
                                diap01.mec_us_united_20056.dfa2_activity    as t1
                -- these JOINs increase runtime significantly, but we need placement names to filter Sojern's tactics
                left join
                                diap01.mec_us_united_20056.dfa2_placements  as p1
                                on t1.placement_id = p1.placement_id
                where
                                t1.activity_id = 978826 and
                                t1.campaign_id = 20606595 and
                                user_id != '0' and
                                (t1.site_id_dcm = 1239319 and regexp_like(placement,'.*_RON_.*','ib')) and

                                cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2018-01-01' and '2018-01-12' and
                                advertiser_id <> 0 and
                                (length(isnull (event_sub_type,'')) > 0)
                group by user_id

union all

                select
                                user_id,
                                -- t2.site_id_dcm,
--                                 event_time                                      as impressiontime
                                -- row_number() over ()                            as imp_nbr
                    0 as trans
                from
                                diap01.mec_us_united_20056.dfa2_impression      as t2
                left join
                                diap01.mec_us_united_20056.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                where
                                user_id != '0' and
                                (t2.site_id_dcm = 1239319 and regexp_like(placement,'.*_RON_.*','ib')) and
                                t2.campaign_id = 20606595 and
                                cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-01-01' and '2018-01-12' and
                                advertiser_id <> 0
        group by user_id
                ) as b
-- where
--                 a.user_id = b.user_id and
--                 -- DCM lookback window is set to 7 days, so interaction_time and event_time already match in the log files
--                 conversiontime = impressiontime



select count(distinct user_id)
from