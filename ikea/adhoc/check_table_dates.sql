--  ======================================================================================================
select
    cast(t1.dateTime as date) as "date",
    t2.std_date,
    t2.std_evts,
    t3.con_date,
    t3.con_evts,
    t4.int_date,
    t4.int_evts

from wmprodfeeds.ikea.dim_calendar as t1

    left join (select
                   cast(eventdatedefaulttimezone as date)       as std_date,
                   count(*)                                     as std_evts
               from wmprodfeeds.ikea.sizmek_standard_events
               where cast(eventdatedefaulttimezone as date) between '2017-01-01' and '2018-12-31'
               and (advertiserid != 0)
               group by
                   cast(eventdatedefaulttimezone as date)
              ) as t2
                on cast(t1.dateTime as date) = cast(t2.std_date as date)

    left join (select
                   cast(conversiondatedefaulttimezone as date)  as con_date,
                   count(*)                                     as con_evts
               from wmprodfeeds.ikea.sizmek_conversion_events
               where cast(conversiondatedefaulttimezone as date) between '2017-01-01' and '2018-12-31'
               and (advertiserid != 0)
               group by
                   cast(conversiondatedefaulttimezone as date)
              ) as t3
                on cast(t1.dateTime as date) = cast(t3.con_date as date)

    left join (select
                   cast(interactiondatedefaulttimezone as date) as int_date,
                   count(*)                                     as int_evts
               from wmprodfeeds.ikea.sizmek_rich_events
               where cast(interactiondatedefaulttimezone as date) between '2017-01-01' and '2018-12-31'
               and (advertiserid != 0)
               group by
                   cast(interactiondatedefaulttimezone as date)
              ) as t4
                on cast(t1.dateTime as date) = cast(t4.int_date as date)

where cast(t1.dateTime as date) between '2017-01-01' and '2018-12-31'
order by cast(t1.dateTime as date) desc;