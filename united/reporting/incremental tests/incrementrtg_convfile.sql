
-- Create table to house RTG Test performance.
-- Update dates in WHERE clauses and run. From 9.23, dates for all queries will
-- expand until a 7-day window is reached. Then use 7-day rolling window.

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
                a.pnr,
                a.conversiontime,
                b.impressiontime,
                a.cvr_nbr,
                a.tix,
                b.imp_nbr
from
                (select
                                user_id,
                                udf_base64decode(replace((left(right(other_data, len(other_data) - position('u18=' in other_data) - 3), position(';' in right(other_data, len(other_data) - position('u18=' in other_data) - 3)) - 1)),'=','==')) as pnr,
                                total_conversions as tix,
                                md_event_time_loc                            as conversiontime,
                                row_number() over ()                        as cvr_nbr

                from
                                wmprodfeeds.united.dfa2_activity    as t1
                where
                                t1.activity_id = 978826 and
                                user_id != '0' and
                                md_event_date_loc between '2018-09-23' and '2018-10-14'
                                and total_conversions > 0
                                and total_revenue <> 0
--                                 and user_id != 'AMsySZa0UyoDj_rMLbZ6PJaAEeHZ'
                                and user_id != 'AMsySZbgjFd0cQABkoRAWTVTAidu'
--                                 and user_id != 'AMsySZY4yddwkv0qlhtUfZLC4v4e'
                                and user_id != 'AMsySZaH7LsFsZWwAI8CZXiZw7rf'
                                and Position('u18=' IN other_data) > 0
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
                                md_event_date_loc between '2018-09-23' and '2018-10-14' and
                                regexp_instr(p1.placement,'.*V2.*') > 0
                ) as b
where
                b.user_id = a.user_id and
                datediff('day',impressiontime,conversiontime) between 0 and 7

    );


-- ================================================================================================================
-- conversions by day (any-touch conversions)
select placement,
case when t2.placement like '%Control%'  then 'Control'
    when t2.placement like '%Test%'      then 'Test' end as group,
       conversiontime,
       user_id,
       pnr,
       impressiontime,
       count(distinct cvr_nbr) as conversions,
       sum(tix) as tix


from (
select case when placement = 'Test9_22days_Non-Member' then 'Test9_22+days_Non-Member'
when placement = 'Test6_22days_Premier' then 'Test6_22+days_Premier'
else placement end as placement,
       conversiontime,
       user_id,
       pnr,
       impressiontime,
      cvr_nbr,
      tix,
    row_number() over (partition by user_id, conversiontime order by conversiontime asc) as rc_1

from (select placement,
             user_id,
             pnr,
             cast(conversiontime as date),
             cast(impressiontime as date),
            cvr_nbr,
             row_number() over (
                 partition by
                     user_id,
                     pnr
                 order by
                     cast(conversiontime as date) asc,
                     cast(impressiontime as date) asc
                 ) as conversions,
                 tix


      from wmprodfeeds.united.ual_rtg_test_tbl1

     ) as t1
where conversions = 1
    ) as t2
where rc_1 = 1

group by
       placement,
      conversiontime,
      user_id,
      pnr,
      impressiontime
;

--=========================================================================================================================================================================================
--Last touch conversions:

select
case when t2.placement = 'Test9_22days_Non-Member' then 'Test9_22+days_Non-Member'
when t2.placement = 'Test6_22days_Premier' then 'Test6_22+days_Premier'
else t2.placement end as placement,
case when t2.placement like '%Control%'  then 'Control'
    when t2.placement like '%Test%'      then 'Test' end as group,
                t2.user_id,
                t2.pnr,
                conversiondate,
                impressiondate,
                t2.cons
from
                (select
                                t1.user_id,
                                Case
       when (regexp_instr(p1.placement,'General_'))
              then SUBSTRING(p1.placement,43,(regexp_instr(p1.placement,'General_')-36))
       when (regexp_instr(p1.placement,'Non-Member_'))
              then SUBSTRING(p1.placement,43,(regexp_instr(p1.placement,'Non-Member_')-33))
       when (regexp_instr(p1.placement,'Premier_'))
              then SUBSTRING(p1.placement,43,(regexp_instr(p1.placement,'Premier_')-36))
END as placement,
                                udf_base64decode(replace((left(right(other_data, len(other_data) - position('u18=' in other_data) - 3), position(';' in right(other_data, len(other_data) - position('u18=' in other_data) - 3)) - 1)),'=','==')) as pnr,
                                t1.md_event_date_loc as conversiondate,
                                t1.md_interaction_date_loc as impressiondate,
                               count(distinct t1.activity_id) as cons



                from
                                wmprodfeeds.united.dfa2_activity    as t1

                left join
                                wmprodfeeds.united.dfa2_placements      as p1
                                on t1.placement_id = p1.placement_id
                where
                                t1.activity_id = 978826 and
                                t1.user_id != '0' and
                                t1.md_interaction_date_loc between '2018-09-23' and '2018-10-14'
                                and p1.placement like '%_V2_%'
                                and t1.campaign_id = 20606595
                                and t1.total_revenue <> 0

                group by t1.user_id, p1.placement, other_data, md_event_date_loc, md_interaction_date_loc




                ) as t2

group by
                t2.placement,
                t2.user_id,
                t2.pnr,
                t2.conversiondate,
                t2.impressiondate

--========================================================================

--unique reach:


select

cast(impressiontime as date) as impressiontime,
case when t2.placement = 'Test9_22days_Non-Member' then 'Test9_22+days_Non-Member'
     when t2.placement = 'Test6_22days_Premier' then 'Test6_22+days_Premier'
     else t2.placement end as placement,
case when t2.placement like '%Control%'  then 'Control'
    when t2.placement like '%Test%'      then 'Test' end as "group",
count(distinct user_id) as usr_count


from (select Case
       when (regexp_instr(p1.placement,'General_'))
              then SUBSTRING(p1.placement,43,(regexp_instr(p1.placement,'General_')-36))
       when (regexp_instr(p1.placement,'Non-Member_'))
              then SUBSTRING(p1.placement,43,(regexp_instr(p1.placement,'Non-Member_')-33))
       when (regexp_instr(p1.placement,'Premier_'))
              then SUBSTRING(p1.placement,43,(regexp_instr(p1.placement,'Premier_')-36))
             END as placement,
             user_id,
             md_event_time_loc as impressiontime
      from wmprodfeeds.united.dfa2_impression as t1
      left join wmprodfeeds.united.dfa2_placements as p1 on t1.placement_id = p1.placement_id
      where t1.campaign_id = 20606595
and t1.site_id_dcm = 1239319
and p1.placement like '%_V2_%'
and t1.md_event_date_loc between '2018-10-02' and '2018-10-13'
and t1.advertiser_id <> 0
and t1.user_id <> '0') as t2
group by

         cast(impressiontime as date),
         placement
order by
         cast(impressiontime as date) asc,
         "group" asc




