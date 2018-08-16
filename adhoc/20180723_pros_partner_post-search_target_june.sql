create table wmprodfeeds.united.ual_prsp_trg_tbl1
(
    user_id        varchar(50) not null,
    site_id_dcm    int         not null,
    conversiontime bigint      not null,
    impressiontime bigint      not null,
    cvr_nbr        int         not null,
    imp_nbr        int         not null
);

insert into wmprodfeeds.united.ual_prsp_trg_tbl1
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
                                t1.user_id,
                                t1.site_id_dcm,
                                t1.event_time                               as conversiontime,
                                t1.md_event_date_loc                        as conversiondate,
                                row_number() over ()                        as cvr_nbr
                from
                                wmprodfeeds.united.dfa2_activity    as t1
                -- these JOINs increase runtime significantly, but we need placement names to filter Sojern's tactics
                left join
                                wmprodfeeds.united.dfa2_placements  as p1
                                on t1.placement_id = p1.placement_id
                where
                                t1.activity_id = 1086066 and
                                t1.user_id != '0' and
                                t1.md_event_date_loc between '2018-01-01' and '2018-06-30' and
                                t1.advertiser_id <> 0 and
                                (length(isnull (t1.event_sub_type,'')) > 0)
                ) as a,

                (select
                                t2.user_id,
                                t2.site_id_dcm,
                                t2.event_time                                   as impressiontime,
                                t2.md_event_date_loc                            as impressiondate,
                                row_number() over ()                            as imp_nbr
                from
                                wmprodfeeds.united.dfa2_impression      as t2
                left join
                                wmprodfeeds.united.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                where
                                user_id != '0'
                                -- filter results to Prospecting tactics only
                                and t2.campaign_id = 20606595 -- GM ACQ 2018
                                and t2.site_id_dcm in (1190273,1239319, 3267410)
                                and regexp_instr(p1.placement,'.*_PROS_FT.*') > 0
                                and t2.md_event_date_loc between '2018-01-01' and '2018-06-30'
                                and t2.advertiser_id <> 0
                ) as b
where
                a.user_id = b.user_id and
                -- DCM lookback window is set to 7 days, so interaction_time and event_time already match in the log files
                datediff('day',conversiondate,impressiondate) between 0 and 30
    );
commit;


-- ===================================================================================================================
-- ===================================================================================================================

-- Total Users/Impressions

select
                                s1.site_dcm,
                                t2.site_id_dcm,
--                                 event_time                                      as impressiontime,
                                count(*) as imp_nbr,
                                count(distinct user_id) as users
                from
                                wmprodfeeds.united.dfa2_impression      as t2
                left join
                                wmprodfeeds.united.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                left join
                                wmprodfeeds.united.dfa2_sites       as s1
                                on t2.site_id_dcm = s1.site_id_dcm

                where
                                user_id != '0'
                                -- filter results to Prospecting tactics only
                                and t2.campaign_id = 20606595 -- GM ACQ 2018
                                and t2.site_id_dcm in (1190273,1239319, 3267410)
                                and regexp_instr(p1.placement,'.*_PROS_FT.*') > 0
                                and t2.md_event_time_loc between '2018-01-01' and '2018-06-30'
                                and advertiser_id <> 0
group by
    s1.site_dcm,
    t2.site_id_dcm


-- ===================================================================================================================
-- ===================================================================================================================

-- Targeted Users/Impressions

select
                                s1.site_dcm,
                                t2.site_id_dcm,
                                count(distinct imp_nbr) as imp_nbr,
                                count(distinct user_id) as users
                from
                                wmprodfeeds.united.ual_prsp_trg_tbl1      as t2
                left join
                                wmprodfeeds.united.dfa2_sites       as s1
                                on t2.site_id_dcm = s1.site_id_dcm

group by
    s1.site_dcm,
    t2.site_id_dcm