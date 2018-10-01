
create table diap01.mec_us_united_20056.ual_weather_media
(
    user_id    varchar(50) not null,
    event_time date         not null,
    city_state varchar(4000)    not null

);

insert into diap01.mec_us_united_20056.ual_weather_media
(user_id,event_time,city_state)

    (select
                a.user_id,
        a.event_time,
        a.city_state

    from (
             select
                 * from diap01.mec_us_united_20056.ual_weather_media_imp

             --     ) as a
             union all

             select
                 * from diap01.mec_us_united_20056.ual_weather_media_clk

         ) as a
group by
            a.user_id,
        a.event_time,
        a.city_state

    );
commit;

-- ====================================================================================================================
-- ====================================================================================================================
create table diap01.mec_us_united_20056.ual_weather_media_imp
(
    user_id    varchar(50) not null,
    event_time int         not null,
    city_state varchar(4000)    not null

);


insert into diap01.mec_us_united_20056.ual_weather_media_imp
(user_id,event_time,city_state)

    (select
        a.user_id,
        a.event_time,
        a.city_state

    from (
             select
                 t3.user_id,
                 t3.event_time,
--                  cast(timestamp_trunc(to_timestamp(t3.event_time / 1000000),'SS') as date) as this_date,
--      t3.state_region,
--         t3.city2,
--      t3.city_state2
                 case when (length(isnull(t3.city_state2,'')) = 0) then lower(t3.city2 || '_' || t3.state_region) else t3.city_state2 end as city_state

             from (select
                 *,
                 case when t2.city_id is null and (length(isnull(t2.zip_postal_code,'')) = 0) then 2 else 1 end as rank,
                 c2.city                                                                                        as city2,
                 z2.city_state                                                                                  as city_state2
             from
                 diap01.mec_us_united_20056.dfa2_impression as t2

                 left join diap01.mec_us_united_20056.dfa2_cities as c2
                 on t2.city_id = c2.city_id

                 left join diap01.mec_us_united_20056.ual_weather_zip as z2
                 on t2.zip_postal_code = z2.zip

             where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2017-06-15'
--     where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) ='2017-06-15'
                 and country_code = 'US'
                 and user_id <> '0'
                 and (advertiser_id <> 0)
                  ) as t3
             where t3.rank = 1


         ) as a
                where (length(isnull(a.city_state,'')) > 0)
-- group by
--             a.user_id,
--         a.this_date,
--         a.city_state

    );
commit;
-- ====================================================================================================================
create table diap01.mec_us_united_20056.ual_weather_media_clk
(
    user_id    varchar(50) not null,
    event_time date         not null,
    city_state varchar(4000)    not null

);


insert into diap01.mec_us_united_20056.ual_weather_media_clk
(user_id,event_time,city_state)

    (select
        a.user_id,
        a.event_time,
        a.city_state

    from (
             select
                 t3.user_id,
                 t3.event_time,
--                  cast(timestamp_trunc(to_timestamp(t3.event_time / 1000000),'SS') as date) as this_date,
--      t3.state_region,
--         t3.city2,
--      t3.city_state2
                 case when (length(isnull(t3.city_state2,'')) = 0) then lower(t3.city2 || '_' || t3.state_region) else t3.city_state2 end as city_state

             from (select
                 *,
                 case when t2.city_id is null and (length(isnull(t2.zip_postal_code,'')) = 0) then 2 else 1 end as rank,
                 c2.city                                                                                        as city2,
                 z2.city_state                                                                                  as city_state2
             from
                 diap01.mec_us_united_20056.dfa2_click as t2

                 left join diap01.mec_us_united_20056.dfa2_cities as c2
                 on t2.city_id = c2.city_id

                 left join diap01.mec_us_united_20056.ual_weather_zip as z2
                 on t2.zip_postal_code = z2.zip

             where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2017-06-15'
--     where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) ='2017-06-15'
                 and country_code = 'US'
                 and user_id <> '0'
                 and (advertiser_id <> 0)
                  ) as t3
             where t3.rank = 1
-- and (length(isnull(t3.city_state2,'')) = 0)

         ) as a

        where (length(isnull(a.city_state,'')) > 0)
-- group by
--             a.user_id,
--         a.this_date,
--         a.city_state

    );
commit;