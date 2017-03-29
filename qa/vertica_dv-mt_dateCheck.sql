select
    cast(t1.dateTime as date)   as "date",
    t2.event_date               as dv_date,
    sum(t2.total_impressions)   as dv_imp,
    t3.event_date               as mt_date,
    sum(t3.human_impressions)   as mt_imp
from diap01.mec_us_united_20056.DIM_calendar as t1

    left join (select *
               from diap01.mec_us_united_20056.dv_impression_agg) as t2
        on cast(t1.dateTime as date) = cast(t2.event_date as date)

    left join (select *
               from diap01.mec_us_united_20056.moat_impression) as t3
        on cast(t1.dateTime as date) = cast(t3.event_date as date)

where cast(t1.dateTime as date) between '2017-01-01' and '2017-12-31'

group by
    cast(t1.dateTime as date),
    t2.event_date,
    t3.event_date

order by cast(t1.dateTime as date) asc