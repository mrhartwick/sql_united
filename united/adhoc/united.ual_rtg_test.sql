drop table if exists wmprodfeeds.united.ual_rtg_test_tbl1;
create table if not exists wmprodfeeds.united.ual_rtg_test_tbl1
(
    placement       varchar(6000),
    user_id        varchar(50) not null,
    pnr            varchar(6),
    conversiontime timestamp   not null,
    impressiontime timestamp   not null,
    cvr_nbr        int         not null,
    tix            int,
    imp_nbr        int
);

insert into wmprodfeeds.united.ual_rtg_test_tbl1
(placement,user_id,pnr,conversiontime,impressiontime,cvr_nbr,tix,imp_nbr)

(select
                b.placement,
                a.user_id,
--                 a.pnr,
a.other_data,
a.pnr_test,
                a.conversiontime,
                b.impressiontime,
                a.cvr_nbr,
                a.tix,
                b.imp_nbr
from
                (select
                                user_id,
--                                 udf_base64decode(replace((left(right(other_data, len(other_data) - position('u18=' in other_data) - 3), position(';' in right(other_data, len(other_data) - position('u18=' in other_data) - 3)) - 1)),'=','==')) as pnr,
                replace((left(right(other_data, len(other_data) - position('u18=' in other_data) - 3), position(';' in right(other_data, len(other_data) - position('u18=' in other_data) - 3)) - 1)),'=','==') as pnr_test,
                other_data,
                                total_conversions as tix,
                                md_event_time_loc                            as conversiontime,
                                row_number() over ()                        as cvr_nbr

                from
                                wmprodfeeds.united.dfa2_activity    as t1
                where
                                t1.activity_id = 978826 and
                                user_id != '0' and
                                md_event_date_loc between '2018-09-23' and '2018-10-07'
                                and total_conversions > 0
                                and total_revenue <> 0
                                and user_id != '%AMsySZa0UyoDj_rMLbZ6PJaAEeHZ%'
                ) as a,

                (select
                                Case
       when (regexp_instr(p1.placement,'General_'))
              then SUBSTRING(p1.placement,43,(regexp_instr(p1.placement,'General_')-36))
       when (regexp_instr(p1.placement,'Non-Member_'))
              then SUBSTRING(p1.placement,43,(regexp_instr(p1.placement,'Non-Member_')-33))
       when (regexp_instr(p1.placement,'Premier_'))
              then SUBSTRING(p1.placement,43,(regexp_instr(p1.placement,'Premier_')-36))
END as placement,

                                user_id,
                                md_event_time_loc                               as impressiontime,
                                row_number() over ()                            as imp_nbr
                from
                                wmprodfeeds.united.dfa2_impression      as t2
                left join
                                wmprodfeeds.united.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                where
                                t2.campaign_id = 20606595 and
                                user_id != '0' and
                                md_event_date_loc between '2018-09-23' and '2018-10-07'and
                                regexp_instr(p1.placement,'.*V2.*') > 0
                ) as b
where
                b.user_id = a.user_id and
                datediff('day',impressiontime,conversiontime) between 0 and 7);