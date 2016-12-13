-- QA Query to check available dates in our vital tables

select
    cast(t1.dateTime as date) as "date",
    t2.act_date,
    t5.ip_date,
    t6.ck_date,
    t3.dv_date,
    t4.mt_date
from diap01.mec_us_united_20056.DIM_calendar as t1

    left join (select distinct cast(click_time as date) as "act_date"
               from diap01.mec_us_united_20056.dfa_activity) as t2
        on cast(t1.dateTime as date) = cast(t2.act_date as date)

    left join (select distinct cast(event_date as date) as "dv_date"
               from diap01.mec_us_united_20056.dv_impression_agg) as t3
        on cast(t1.dateTime as date) = cast(t3.dv_date as date)

    left join (select distinct cast(event_date as date) as "mt_date"
               from diap01.mec_us_united_20056.moat_impression) as t4
        on cast(t1.dateTime as date) = cast(t4.mt_date as date)

    left join (select distinct cast(impression_time as date) as "ip_date"
               from diap01.mec_us_united_20056.dfa_impression) as t5
        on cast(t1.dateTime as date) = cast(t5.ip_date as date)

    left join (select distinct cast(click_time as date) as "ck_date"
               from diap01.mec_us_united_20056.dfa_click) as t6
        on cast(t1.dateTime as date) = cast(t6.ck_date as date)

where cast(t1.dateTime as date) between '2016-09-01' and '2016-12-31'
order by cast(t1.dateTime as date) desc