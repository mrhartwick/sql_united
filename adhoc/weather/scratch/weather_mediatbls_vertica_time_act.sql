create table diap01.mec_us_united_20056.ual_weather_act
(
    user_id          varchar(50)   not null,
    interaction_time int           not null,
    led              int           not null,
    con              int           not null,
    tix              int           not null,
    city_state       varchar(4000) not null

);

insert into diap01.mec_us_united_20056.ual_weather_act
(user_id,interaction_time,led,con,tix,city_state)

    (

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
                case when activity_id = 1086066 and (ta.conversion_id = 1 or ta.conversion_id = 2) then 1 else 0 end                                             as led,
                case when activity_id = 978826 and (ta.conversion_id = 1 or ta.conversion_id = 2) and ta.total_revenue <> 0 then 1 else 0 end                    as con,
                case when activity_id = 978826 and (ta.conversion_id = 1 or ta.conversion_id = 2) and ta.total_revenue <> 0 then ta.total_conversions else 0 end as tix
            from diap01.mec_us_united_20056.dfa2_activity
            where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2016-07-15' and '2017-06-15'
                and (activity_id = 978826 or activity_id = 1086066)
                and (advertiser_id <> 0)
                and (length(isnull(event_sub_type,'')) > 0)
        ) as ta

        left join diap01.mec_us_united_20056.ual_weather_media as m1
        on ta.user_id = m1.user_id
        and ta.interaction_time = m1.event_time


    );
commit;
-- group by
-- cast (timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),'SS') as date ),
-- ta.campaign_id,
-- ta.site_id_dcm,
-- ta.placement_id

