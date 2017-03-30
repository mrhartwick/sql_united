select
    cast(t1.dateTime as date) as "date",
    t2.act_date,
    t2.act_acts,
    t5.imp_date,
    t5.imp_imps,
    t6.clk_date,
    t6.clk_clks

from diap01.mec_us_united_20056.DIM_calendar as t1

    left join (select
                   cast(to_timestamp(event_time / 1000000) as date) as "act_date",
                   count(*)                                               as act_acts
               from diap01.mec_us_united_20056.dfa2_activity
               where cast(to_timestamp(event_time / 1000000) as date) between '2017-01-01' and '2017-12-31'
               and (advertiser_id != 0)
               group by
                   cast(to_timestamp(event_time/1000000) as date )
              ) as t2
        on cast(t1.dateTime as date) = cast(t2.act_date as date)

    left join (select
                   cast(to_timestamp(event_time / 1000000) as date) as "imp_date",
                   count(*)                                               as imp_imps
               from diap01.mec_us_united_20056.dfa2_impression
               where cast(to_timestamp(event_time / 1000000) as date) between '2017-01-01' and '2017-12-31'
                   and (advertiser_id != 0)
               group by
                   cast(to_timestamp(event_time/1000000) as date )
              ) as t5
        on cast(t1.dateTime as date) = cast(t5.imp_date as date)

    left join (select
                   cast(to_timestamp(event_time / 1000000) as date) as "clk_date",
                   count(*)                                               as clk_clks
               from diap01.mec_us_united_20056.dfa2_click
               where cast(to_timestamp(event_time / 1000000) as date) between '2017-01-01' and '2017-12-31'
                   and (advertiser_id != 0)
               group by
                   cast(to_timestamp(event_time/1000000) as date )
              ) as t6
        on cast(t1.dateTime as date) = cast(t6.clk_date as date)


where cast(t1.dateTime as date) between '2017-01-01' and '2017-12-31'
order by cast(t1.dateTime as date) desc