
-- Create table to house test ("Incrementality-Test-Exposed") placement performance.
-- Update dates in WHERE clauses and run. From 8/16, dates for all queries will
-- expand until a 7-day window is reached. Then use 7-day rolling window.

drop table if exists wmprodfeeds.united.ual_inc_test_tbl1;
create table if not exists wmprodfeeds.united.ual_inc_test_tbl1
(
  campaign       varchar(50),
  user_id        varchar(50) not null,
  pnr            varchar(6),
  conversiontime timestamp   not null,
  impressiontime timestamp   not null,
  cvr_nbr        int         not null,
  tix            int,
  rev            decimal(37,15),
  imp_nbr        int,
  prc_nbr        int
);

insert into wmprodfeeds.united.ual_inc_test_tbl1
(campaign,user_id,pnr,conversiontime,impressiontime,cvr_nbr,tix,rev,imp_nbr,prc_nbr)

(select
                b.campaign,
                a.user_id,
                a.pnr,
                a.conversiontime,
                b.impressiontime,
                a.cvr_nbr,
                a.tix,
                a.rev,
                b.imp_nbr,
                a.prc_nbr
from
                (select
                                user_id,
                                udf_base64decode(replace((left(right(other_data, len(other_data) - position('u18=' in other_data) - 3), position(';' in right(other_data, len(other_data) - position('u18=' in other_data) - 3)) - 1)),'=','==')) as pnr,
                                replace((left(right(other_data, len(other_data) - position('u18=' in other_data) - 3), position(';' in right(other_data, len(other_data) - position('u18=' in other_data) - 3)) - 1)),'=','==') as pnr_test,
                                total_conversions as tix,
                                md_event_time_loc                            as conversiontime,
                                row_number() over ()                        as cvr_nbr,
 --                             Use to limit conversions to one per cookie per day
                                row_number() over (partition by user_id, md_event_date_loc order by md_event_time_loc asc)                        as prc_nbr,
                                case when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') = 0 then cast(((t1.total_revenue * 1000000) / (rates.exchange_rate)) as decimal(20,10))
                                     when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') > 0 then cast(((t1.total_revenue*1000)/.0103) as decimal(20,10)) end as rev
                from
                                wmprodfeeds.united.dfa2_activity    as t1

                                left join wmprodfeeds.exchangerates.exchange_rates as rates
                                on upper(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3)) = upper(rates.currency)
                                and md_event_date_loc = rates.date
                where
                                t1.activity_id = 978826 and
                                user_id != '0' and
                                user_id not in ('AMsySZYdPcUq6XxR_34Vx_6kIpyP', 'AMsySZYEleBcOq0jVeYOhdEtV8d1', 'AMsySZZQoayDkqvL9bNJOqx3aMwB', 'AMsySZaH7LsFsZWwAI8CZXiZw7rf', 'AMsySZbgjFd0cQABkoRAWTVTAidu') and
                                md_event_date_loc between '2018-10-04' and '2018-10-29' and
                                total_conversions > 0

                ) as a,

                (select
                  case when regexp_instr(p1.placement, '.*Incrementality-Test-Exposed.*') > 0 then 'Exposed'
                            when regexp_instr(p1.placement, '.*Incrementality-Test-Non-Exposed.*') > 0 then 'Non-Exposed'
                            else 'XXX' end as campaign,
                                event_time,
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
                                md_event_date_loc between '2018-10-04' and '2018-10-29' and
                                regexp_instr(p1.placement,'.*Incrementality-Test.*') > 0
                ) as b
where
                b.user_id = a.user_id and
        datediff('day',impressiontime,conversiontime) between 0 and 7
    );


-- ================================================================================================================
-- conversions by day
select
     campaign,
     conversiontime,
     user_id,
     pnr,
       impressiontime,
     count(distinct cvr_nbr) as conversions,
       sum(rev) as rev


from (
select campaign,
     conversiontime,
     user_id,
     pnr,
rev,
       impressiontime,
-- sum(conversions) as conversions
--     count(distinct cvr_nbr) as conversions
    cvr_nbr,
  row_number() over (partition by user_id, conversiontime order by conversiontime asc) as rc_1

from (select campaign,
           user_id,
           pnr,
            rev,
           cast(conversiontime as date),
           cast(impressiontime as date),
--        count(distinct cvr_nbr)  over (partition by user_id, cast(conversiontime as date), pnr order by user_id, cast(conversiontime as date), pnr asc) as conversions
      cvr_nbr,
           row_number() over (
             partition by
--               cast(conversiontime as date),
--               cvr_nbr,
               user_id,
               pnr
             order by
               cast(conversiontime as date) asc,
               cast(impressiontime as date) asc
             ) as conversions

  --        sum(1) over (partition by user_id, pnr ) as conversions
  --        sum(tix) as tix,
  --        count(distinct imp_nbr) as imps

    from wmprodfeeds.united.ual_inc_test_tbl1
--
--    group by campaign,
--         user_id,
--         pnr,
--         cast(impressiontime as date),
--         cast(conversiontime as date)
     ) as t1
where conversions = 1
  ) as t2
where rc_1 = 1

group by
       campaign,
     conversiontime,
     user_id,
     pnr,
       impressiontime
;
-- ================================================================================================================
-- ================================================================================================================
-- unique conversions
-- select
--                 cast(impressiontime as date) as conversion_date,
--        campaign,
--              count(distinct cvr_nbr) as conversions
-- from (
-- select campaign,
--     user_id,
--     pnr,
--    conversiontime,
--       impressiontime,
--    row_number() over (partition by user_id, impressiontime order by impressiontime asc) as rc_1,
-- -- sum(conversions) as conversions
-- cvr_nbr
-- --      count(distinct cvr_nbr) as conversions
--
-- from (select campaign,
--           user_id,
--           pnr,
--           cast(conversiontime as date),
--           cast(impressiontime as date),
-- --         count(distinct cvr_nbr)  over (partition by user_id, cast(conversiontime as date), pnr order by user_id, cast(conversiontime as date), pnr asc) as conversions
--      cvr_nbr,
--           row_number() over (
--             partition by
-- --                cast(conversiontime as date),
-- --                cvr_nbr,
--               user_id,
--               pnr
--             order by
--               cast(conversiontime as date) asc,
--               cast(impressiontime as date)
--             desc
--             ) as conversions
--  --        sum(1) over (partition by user_id, pnr ) as conversions
--  --        sum(tix) as tix,
--  --        count(distinct imp_nbr) as imps
--
--    from wmprodfeeds.united.ual_inc_test_tbl1
-- --
-- --     group by campaign,
-- --          user_id,
-- --          pnr,
-- --          cast(impressiontime as date),
-- --          cast(conversiontime as date)
--      ) as t1
-- where conversions = 1
--
-- -- group by
-- --      campaign,
-- --      user_id,
-- --      pnr,
-- --      cast(conversiontime as date),
-- --        cast(impressiontime as date)
--  ) as t2
-- where rc_1 = 1
-- group by
--        campaign,
-- --         user_id,
-- --         pnr,
--        cast(impressiontime as date)
-- --         cast(conversiontime as date)
-- order by
--     cast(impressiontime as date) asc,
--     campaign asc
-- ;

-- ================================================================================================================
-- ================================================================================================================
-- unique conversions -- OLD VERSION
-- select
--                 cast(conversiontime as date) as conversion_date,
--         campaign,
--               count(distinct cvr_nbr) as conversions
-- from (
-- select campaign,
--      user_id,
--      pnr,
--     conversiontime,
--       impressiontime,
--     row_number() over (partition by user_id, conversiontime order by conversiontime asc) as rc_1,
-- -- sum(conversions) as conversions
-- cvr_nbr
-- --     count(distinct cvr_nbr) as conversions
--
-- from (select campaign,
--            user_id,
--            pnr,
--            cast(conversiontime as date),
--            cast(impressiontime as date),
-- --        count(distinct cvr_nbr)  over (partition by user_id, cast(conversiontime as date), pnr order by user_id, cast(conversiontime as date), pnr asc) as conversions
--       cvr_nbr,
--            row_number() over (
--              partition by
-- --               cast(conversiontime as date),
-- --               cvr_nbr,
--                user_id,
--                pnr
--              order by
--                cast(conversiontime as date) asc,
--                cast(impressiontime as date)
--              desc
--              ) as conversions
--   --        sum(1) over (partition by user_id, pnr ) as conversions
--   --        sum(tix) as tix,
--   --        count(distinct imp_nbr) as imps
--
--     from wmprodfeeds.united.ual_inc_test_tbl1
-- --
-- --    group by campaign,
-- --         user_id,
-- --         pnr,
-- --         cast(impressiontime as date),
-- --         cast(conversiontime as date)
--      ) as t1
-- where conversions = 1
--
-- -- group by
-- --     campaign,
-- --     user_id,
-- --     pnr,
-- --     cast(conversiontime as date),
-- --        cast(impressiontime as date)
--   ) as t2
-- where rc_1 = 1
-- group by
--         campaign,
-- --        user_id,
-- --        pnr,
-- --        cast(impressiontime as date)
--         cast(conversiontime as date)
-- order by
--      cast(conversiontime as date) asc,
--      campaign asc
-- ;
-- ================================================================================================================
-- ================================================================================================================
-- Summary

-- select
-- --         campaign,
-- --         user_id,
-- --         pnr,
--        cast(conversiontime as date) as conversion_date,
-- --         cast(impressiontime as date) as impression_date,
--        count(distinct cvr_nbr) as conversions,
--        sum(tix) as tix,
--        count( distinct imp_nbr) as imps
--
-- from            wmprodfeeds.united.ual_inc_test_tbl1
--
-- group by
-- --         campaign,
-- --         user_id,
-- --         pnr,
-- --         cast(impressiontime as date),
--        cast(conversiontime as date)
;
-- ================================================================================================================
-- imps by day, cookie

select
campaign,
cast(impressiontime as date) as impressiontime,
user_id,
count(distinct imp_nbr) as imps

from (select case when regexp_instr(p1.placement, '.*Incrementality-Test-Exposed.*') > 0 then 'Exposed'
                  when regexp_instr(p1.placement, '.*Incrementality-Test-Non-Exposed.*') > 0 then 'Non-Exposed'
                  else 'XXX' end as campaign,
--                 event_time,
                        user_id,
        --                 event_time as i_event,
           md_event_time_loc as impressiontime,
           row_number() over () as imp_nbr
      from wmprodfeeds.united.dfa2_impression as t2
             left join wmprodfeeds.united.dfa2_placements as p1 on t2.placement_id = p1.placement_id
      where t2.campaign_id = 20606595 and user_id != '0' and
          md_event_date_loc between '2018-10-04' and '2018-10-28' and
            regexp_instr(p1.placement, '.*Incrementality-Test.*') > 0) as t1
group by
    user_id,
     cast(impressiontime as date),
     campaign
;
-- ================================================================================================================
-- unique users by day

select

cast(impressiontime as date) as impressiontime,
campaign,
count(distinct user_id) as usr_count
-- count(distinct imp_nbr) as imps

from (select case when regexp_instr(p1.placement, '.*Incrementality-Test-Exposed.*') > 0 then 'Exposed'
                  when regexp_instr(p1.placement, '.*Incrementality-Test-Non-Exposed.*') > 0 then 'Non-Exposed'
                  else 'XXX' end as campaign,
--                 event_time,
                        user_id,
        --                 event_time as i_event,
           md_event_time_loc as impressiontime
--           row_number() over () as imp_nbr
      from wmprodfeeds.united.dfa2_impression as t2
             left join wmprodfeeds.united.dfa2_placements as p1 on t2.placement_id = p1.placement_id
      where t2.campaign_id = 20606595 and user_id != '0' and
          md_event_date_loc between '2018-10-04' and '2018-10-29' and
            regexp_instr(p1.placement, '.*Incrementality-Test.*') > 0) as t1
group by
--    user_id,
     cast(impressiontime as date),
     campaign
order by
     cast(impressiontime as date) asc,
     campaign asc

--
-- select * from wmprodfeeds.united.ual_inc_test_tbl1
--        where pnr = 'I0CZQJ'
-- order by conversiontime, impressiontime
--
--
-- delete from wmprodfeeds.united.dfa2_activity
-- where md_event_time::date = '2018-08-22'
--
-- insert into wmprodfeeds.united.dfa2_activity
-- (select * from wmprodfeeds.united.dfa2_activity_28);


-- conversiondatedefaulttimezone
--
-- wmprodfeeds.ikea.sizmek_conversion_events.conversiondate
--
--
-- select other_data
--                 from
--                                 wmprodfeeds.united.dfa2_activity    as t1
--                 where
-- --                                 t1.activity_id = 978826 and
--                                 user_id != '0' and
--             user_id = 'AMsySZZQoayDkqvL9bNJOqx3aMwB'
--  and
--                    md_event_date_loc between '2018-09-04' and '2018-09-27'



-- ================================================================================================================
-- imps by day, cookie
--
-- select
-- -- campaign,
-- cast(impressiontime as date) as impressiontime,
-- -- user_id,
-- count(distinct imp_nbr) as imps
--
-- from (select case when regexp_instr(p1.placement, '.*Incrementality-Test-Exposed.*') > 0 then 'Exposed'
--                   when regexp_instr(p1.placement, '.*Incrementality-Test-Non-Exposed.*') > 0 then 'Non-Exposed'
--                   else 'XXX' end as campaign,
-- --                 event_time,
--                         user_id,
--         --                 event_time as i_event,
--            md_event_time_loc as impressiontime,
--            row_number() over () as imp_nbr
--       from wmprodfeeds.united.dfa2_impression as t2
--              left join wmprodfeeds.united.dfa2_placements as p1 on t2.placement_id = p1.placement_id
--       where t2.campaign_id = 20606595 and user_id != '0' and
--           md_event_date_loc between '2018-10-04' and '2018-10-29' and
--             regexp_instr(p1.placement, '.*Incrementality-Test.*') > 0) as t1
-- group by
-- --     user_id,
--      cast(impressiontime as date)
-- --      campaign
-- ;