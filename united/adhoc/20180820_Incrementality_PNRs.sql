
-- Create table to house test ("Incrementality-Test-Exposed") placement performance.
-- Update dates in WHERE clauses and run. From 8/16, dates for all queries will
-- expand until a 7-day window is reached. Then use 7-day rolling window.

drop table if exists wmprodfeeds.united.ual_inc_test_tbl1;
create table if not exists wmprodfeeds.united.ual_inc_test_tbl1
(
    user_id        varchar(50) not null,
    pnr            varchar(6),
    conversiontime bigint      not null,
    impressiontime bigint      not null,
    cvr_nbr        int         not null,
    tix            int,
    imp_nbr        int         not null
);

insert into wmprodfeeds.united.ual_inc_test_tbl1
(user_id,pnr,conversiontime,impressiontime,cvr_nbr,tix,imp_nbr)

(select
                a.user_id,
                a.pnr,
                conversiontime,
                impressiontime,
                cvr_nbr,
                tix,
                imp_nbr
from
                (select
                                user_id,
                                udf_base64decode(left(right(other_data, len(other_data) - position('u18=' in other_data) - 3), position(';' in right(other_data, len(other_data) - position('u18=' in other_data) - 3)) - 1)) as pnr,
                                total_conversions as tix,
                                event_time                            as conversiontime,
                                row_number() over ()                        as cvr_nbr
                from
                                wmprodfeeds.united.dfa2_activity    as t1
                left join
                                wmprodfeeds.united.dfa2_placements  as p1
                                on t1.placement_id = p1.placement_id
                where
                                t1.activity_id = 978826 and
                                t1.campaign_id = 20606595 and
                                user_id != '0' and
                                md_event_date between '2018-08-16' and '2018-08-18' and
                                advertiser_id <> 0 and
--                              regexp_instr(p1.placement,'.*Incrementality-Test-Non-Exposed.*') > 0 and
                                regexp_instr(p1.placement,'.*Incrementality-Test-Exposed.*') > 0 and
--                              regexp_instr(p1.placement,'.*Test.*') > 0 and
--                              regexp_instr(p1.placement,'.*Control.*') > 0 and
                                total_conversions > 0
                ) as a,

                (select
                                user_id,
                                event_time                                      as impressiontime,
                                row_number() over ()                            as imp_nbr
                from
                                wmprodfeeds.united.dfa2_impression      as t2
                left join
                                wmprodfeeds.united.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                where
                                t2.campaign_id = 20606595 and
                                user_id != '0' and
                                md_event_date between '2018-08-16' and '2018-08-18' and
                                advertiser_id <> 0 and
--                              regexp_instr(p1.placement,'.*Incrementality-Test-Non-Exposed.*') > 0
                                regexp_instr(p1.placement,'.*Incrementality-Test-Exposed.*') > 0
--                              regexp_instr(p1.placement,'.*Test.*') > 0 and
--                              regexp_instr(p1.placement,'.*Control.*') > 0
                ) as b
where
                a.user_id = b.user_id and
                conversiontime > impressiontime and
                length(isnull(a.pnr,'')) > 0

    );
-- ================================================================================================================

-- Create table to house control ("Incrementality-Test-Non-Exposed") placement performance.
-- Update dates in WHERE clauses and run.

drop table if exists wmprodfeeds.united.ual_inc_ctrl_tbl1;
create table if not exists wmprodfeeds.united.ual_inc_ctrl_tbl1
(
    user_id        varchar(50) not null,
    pnr            varchar(6),
    conversiontime bigint      not null,
    impressiontime bigint      not null,
    cvr_nbr        int         not null,
    tix            int,
    imp_nbr        int         not null
);

insert into wmprodfeeds.united.ual_inc_ctrl_tbl1
(user_id,pnr,conversiontime,impressiontime,cvr_nbr,tix,imp_nbr)

(select
                a.user_id,
                a.pnr,
                conversiontime,
                impressiontime,
                cvr_nbr,
                tix,
                imp_nbr
from
                (select
                                user_id,
                                udf_base64decode(left(right(other_data, len(other_data) - position('u18=' in other_data) - 3), position(';' in right(other_data, len(other_data) - position('u18=' in other_data) - 3)) - 1)) as pnr,
                                total_conversions as tix,
                                event_time                            as conversiontime,
                                row_number() over ()                        as cvr_nbr
                from
                                wmprodfeeds.united.dfa2_activity    as t1
                left join
                                wmprodfeeds.united.dfa2_placements  as p1
                                on t1.placement_id = p1.placement_id
                where
                                t1.activity_id = 978826 and
                                t1.campaign_id = 20606595 and
                                user_id != '0' and
                                md_event_date between '2018-08-16' and '2018-08-18' and
                                advertiser_id <> 0 and
                                regexp_instr(p1.placement,'.*Incrementality-Test-Non-Exposed.*') > 0 and
--                              regexp_instr(p1.placement,'.*Incrementality-Test-Exposed.*') > 0 and
--                              regexp_instr(p1.placement,'.*Test.*') > 0 and
--                              regexp_instr(p1.placement,'.*Control.*') > 0 and
--                              regexp_instr(p1.placement,'.*PROS.*') > 0 and
                                total_conversions > 0
                ) as a,

                (select
                                user_id,
                                event_time                                      as impressiontime,
                                row_number() over ()                            as imp_nbr
                from
                                wmprodfeeds.united.dfa2_impression      as t2
                left join
                                wmprodfeeds.united.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                where
                                t2.campaign_id = 20606595 and
                                user_id != '0' and
                                md_event_date between '2018-08-16' and '2018-08-18' and
                                advertiser_id <> 0 and
                                regexp_instr(p1.placement,'.*Incrementality-Test-Non-Exposed.*') > 0
--                              regexp_instr(p1.placement,'.*Incrementality-Test-Exposed.*') > 0
--                              regexp_instr(p1.placement,'.*Test.*') > 0 and
--                              regexp_instr(p1.placement,'.*Control.*') > 0
--                              regexp_instr(p1.placement,'.*PROS.*') > 0
                ) as b
where
                a.user_id = b.user_id and
                conversiontime > impressiontime and
                length(isnull(a.pnr,'')) > 0

    );
-- ================================================================================================================

-- Query for test table output

select cast(udf_bigintToTS(conversiontime, 'us') as date) as date,
       user_id,
       pnr,
       count(distinct cvr_nbr) as conversions,
       tix,
       count(distinct imp_nbr) as imps

from wmprodfeeds.united.ual_inc_test_tbl1

group by cast(udf_bigintToTS(conversiontime, 'us') as date),
         user_id,
         pnr,
         tix

-- ==========================================================================================================

-- Query for control table output

select cast(udf_bigintToTS(conversiontime, 'us') as date) as date,
       user_id,
       pnr,
       count(distinct cvr_nbr) as conversions,
       tix,
       count(distinct imp_nbr) as imps

from wmprodfeeds.united.ual_inc_ctrl_tbl1

group by cast(udf_bigintToTS(conversiontime, 'us') as date),
         user_id,
         pnr,
         tix


-- Query for next step in fractional attribution (not used)

-- create table if not exists wmprodfeeds.united.ual_inc_test_tbl2
-- (
--     user_id        varchar(50) not null,
--     site_id_dcm    int         not null,
--     conversiontime int         not null,
--     impressiontime int         not null,
--     cvr_nbr        int         not null,
--     imp_nbr        int         not null
-- );
--
-- insert into wmprodfeeds.united.ual_inc_test_tbl2
-- (user_id,site_id_dcm,conversiontime,impressiontime,cvr_nbr,imp_nbr)
--
--  (select
--                 user_id,
--                 cvr_nbr,
--                 1/count(distinct imp_nbr) as cvr_credit
-- from
--                 ual_inc_test_tbl1
-- group by
--                 1,2);