create table diap01.mec_us_united_20056.ual_weather_act
(
    this_date  date          not null,
    city_state varchar(4000) not null,
    user_cnt   int           not null,
    led        int           not null,
    con        int           not null,
    tix        int           not null
);



insert into diap01.mec_us_united_20056.ual_weather_act
(this_date,city_state,user_cnt,led,con,tix)

    (
        select
        cast(timestamp_trunc(to_timestamp(t1.interaction_time / 1000000),'SS') as date) as this_date,
        t1.city_state as city_state,
        count(distinct t1.user_id) as user_cnt,
        sum(t1.led) as led,
        sum(t1.con) as con,
        sum(t1.tix) as tix


    from (
            select
            ta.user_id,
--  cast(timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),'SS') as date) as "date"
            ta.interaction_time,
            ta.led,
            ta.con,
            ta.tix,
            m1.city_state

        from
            (
                select
                    *,
                    case when activity_id = 1086066 and (conversion_id = 1 or conversion_id = 2) then 1 else 0 end                                             as led,
                    case when activity_id = 978826 and (conversion_id = 1 or conversion_id = 2) and total_revenue <> 0 then 1 else 0 end                    as con,
                    case when activity_id = 978826 and (conversion_id = 1 or conversion_id = 2) and total_revenue <> 0 then total_conversions else 0 end as tix
                from diap01.mec_us_united_20056.dfa2_activity
                where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2016-07-15' and '2017-06-15'
                    and (activity_id = 978826 or activity_id = 1086066)
                    and (advertiser_id <> 0)
                    and (length(isnull(event_sub_type,'')) > 0)
            ) as ta

            left join diap01.mec_us_united_20056.ual_weather_media as m1
            on ta.user_id = m1.user_id
                and ta.interaction_time = m1.event_time
        ) as t1

where (length(isnull(t1.city_state,'')) > 0)
group by
cast(timestamp_trunc(to_timestamp(t1.interaction_time / 1000000),'SS') as date),
t1.city_state
    );
commit;
-- group by
-- cast (timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),'SS') as date ),
-- ta.campaign_id,
-- ta.site_id_dcm,
-- ta.placement_id

