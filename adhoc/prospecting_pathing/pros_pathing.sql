-- Prospecting partners drive users to United.com, where they hopefully perform a flight search.
-- If they do, that user's cookie goes into a pool used by Retargeting partners. The cookies expire after 30 days.

-- The goal of this query is to find the number of unique users, per partner, who
-- 1.) Were erved a Prospecting Impression, then within 7 days,
-- 2.) searched for a flight on United.com, then within 30 days,
-- 3.) were served a Retargeting Impression, then within 7 days,
-- 4.) purchased a ticket.

-- Sojern has been both a Prospecting AND Retargeting partner this year, so this query uses either "_PROS_" (prospecting)
-- or "_RON_" in the placement name to separate tactics.


-- Table 1: users who were served a Prospecting Impression and then performed a search on United.com.
create table diap01.mec_us_united_20056.ual_prsp_tbl1
(
    user_id        varchar(50) not null,
    site_id_dcm    int         not null,
    conversiontime int         not null,
    impressiontime int         not null,
    cvr_nbr        int         not null,
    imp_nbr        int         not null
);

insert into diap01.mec_us_united_20056.ual_prsp_tbl1
(user_id,site_id_dcm,conversiontime,impressiontime,cvr_nbr,imp_nbr)

(select
                a.user_id,
                a.site_id_dcm,
                conversiontime,
                impressiontime,
                cvr_nbr,
                imp_nbr
from
                (select
                                user_id,
                                t1.site_id_dcm,
                                interaction_time                            as conversiontime,
                                row_number() over ()                        as cvr_nbr
                from
                                diap01.mec_us_united_20056.dfa2_activity    as t1
                -- these JOINs increase runtime significantly, but we need placement names to filter Sojern's tactics
                left join
                                diap01.mec_us_united_20056.dfa2_placements  as p1
                                on t1.placement_id = p1.placement_id
                where
                                t1.activity_id = 1086066 and
                                user_id != '0' and

                                cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2017-08-15' and '2017-12-06' and
                                advertiser_id <> 0 and
                                (length(isnull (event_sub_type,'')) > 0)
                ) as a,

                (select
                                user_id,
                                t2.site_id_dcm,
                                event_time                                      as impressiontime,
                                row_number() over ()                            as imp_nbr
                from
                                diap01.mec_us_united_20056.dfa2_impression      as t2
                left join
                                diap01.mec_us_united_20056.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                where
                                user_id != '0' and
                                -- filter results to Prospecting tactics only
                                (
                                ((t2.campaign_id in (10742878,11177760,11224605)) and
                                (   (t2.site_id_dcm in (1853562,1190273,3267410)) or
                                    (t2.site_id_dcm = 1239319 and regexp_like(placement,'.*_PROS_.*','ib'))
                                )) or
                                (
                                    (regexp_like(placement,'.*GEN_INT_PROS_FT.*','ib'))
                                )
                                ) and
                                cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2017-08-15' and '2017-12-06' and
                                advertiser_id <> 0
                ) as b
where
                a.user_id = b.user_id and
                -- DCM lookback window is set to 7 days, so interaction_time and event_time already match in the log files
                conversiontime = impressiontime
    );
commit;

-- // =================================================================================================================

create table diap01.mec_us_united_20056.ual_prsp_tbl2
(
    user_id        varchar(50) not null,
    site_id_dcm    int         not null,
    conversiontime int         not null,
    impressiontime int         not null,
    cvr_window_day int         not null
);

insert into diap01.mec_us_united_20056.ual_prsp_tbl2
(user_id,site_id_dcm,conversiontime,impressiontime,cvr_window_day)

(select
                a.user_id,
                a.site_id_dcm,
                conversiontime,
                impressiontime,
                datediff('day',cast(timestamp_trunc(to_timestamp(a.conversiontime / 1000000),'SS') as date),cast(timestamp_trunc(to_timestamp(b.impressiontime / 1000000),'SS') as date)) as cvr_window_day
from
                (select
                                user_id,
                                site_id_dcm,
                                conversiontime
                from
                                diap01.mec_us_united_20056.ual_prsp_tbl1
                ) as a,

                (select
                                user_id,
                                t2.site_id_dcm,
                                event_time as impressiontime
                from
                                diap01.mec_us_united_20056.dfa2_impression as t2
                left join
                                diap01.mec_us_united_20056.dfa2_placements as p1
                                on t2.placement_id = p1.placement_id
                where
                                user_id != '0' and
                                -- RTG
                                (
                                    t2.campaign_id in (10742878,11177760,11224605) and
                                    (
                                        (t2.site_id_dcm = 1578478) or
                                        (t2.site_id_dcm = 1239319 and regexp_like(placement,'.*_RON_.*','ib'))
                                    )
                                ) and
                                cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2017-08-15' and '2017-12-06'
                                and (advertiser_id <> 0)
                ) as b
where
                b.user_id=a.user_id  and
                -- make sure RTG Impression is served to user within 30 days of flight search
                datediff('day',cast(timestamp_trunc(to_timestamp(a.conversiontime / 1000000),'SS') as date),cast(timestamp_trunc(to_timestamp(b.impressiontime / 1000000),'SS') as date)) between 0 and 30
);
commit;

-- // =================================================================================================================

create table diap01.mec_us_united_20056.ual_prsp_tbl3
(
    user_id        varchar(50) not null,
    site_id_dcm    int         not null,
    conversiontime int         not null,
    impressiontime int         not null,
    cvr_nbr        int         not null
);

insert into diap01.mec_us_united_20056.ual_prsp_tbl3
(user_id,site_id_dcm,conversiontime,impressiontime,cvr_nbr)

(select
                b.user_id,
                b.site_id_dcm,
                conversiontime,
                impressiontime,
                cvr_nbr
from
                (select
                                user_id,
                                t1.site_id_dcm,
                                interaction_time     as conversiontime,
                                row_number() over () as cvr_nbr
                from
                                diap01.mec_us_united_20056.dfa2_activity    as t1
                left join
                                diap01.mec_us_united_20056.dfa2_placements  as p1
                                on t1.placement_id = p1.placement_id
                where
                                t1.activity_id = 978826 and
                                user_id != '0' and
                                -- RTG
                                (
                                    (t1.campaign_id in (10742878,11177760,11224605)) and
                                    (
                                        (t1.site_id_dcm = 1578478) or
                                        (t1.site_id_dcm = 1239319 and regexp_like(placement,'.*_RON_.*','ib'))
                                    )
                                ) and
                                cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2017-08-15' and '2017-12-06'
                                and (advertiser_id <> 0)
                                and (length(isnull (event_sub_type,'')) > 0)
                ) as a,

                (select
                                user_id,
                                site_id_dcm,
                                impressiontime
                from
                                diap01.mec_us_united_20056.ual_prsp_tbl2
                ) as b
where
                a.user_id = b.user_id
                and a.conversiontime = b.impressiontime

);
commit;

-- // =================================================================================================================
-- check to see that user counts go down with each successive query
select count(distinct user_id) from diap01.mec_us_united_20056.ual_prsp_tbl1
select count(distinct user_id) from diap01.mec_us_united_20056.ual_prsp_tbl2
select count(distinct user_id) from diap01.mec_us_united_20056.ual_prsp_tbl3

-- // =================================================================================================================

select
                s1.site_dcm,
                t1.site_id_dcm,
                count(distinct user_id)
from
                diap01.mec_us_united_20056.ual_prsp_tbl3    as t1
left join
                diap01.mec_us_united_20056.dfa2_sites       as s1
                on t1.site_id_dcm = s1.site_id_dcm

group by
                s1.site_dcm,
                t1.site_id_dcm


select
                s1.site_dcm,
                t1.site_id_dcm,
                count(distinct user_id)
from
                diap01.mec_us_united_20056.ual_prsp_tbl2    as t1
left join
                diap01.mec_us_united_20056.dfa2_sites       as s1
                on t1.site_id_dcm = s1.site_id_dcm

group by
                s1.site_dcm,
                t1.site_id_dcm