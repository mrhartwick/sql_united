-- SQL Server first

create table master.dbo.ual_weather
(
    datetime_loc    datetime not null,
    datetime_utc    datetime    not null,
    city_id         int          not null,
    city_name       varchar(500) not null,
    coord_lat       decimal(20,10)      not null,
    coord_lon       decimal(20,10)      not null,
    main_temp_f     decimal(20,10)      not null,
    main_temp_min_f decimal(20,10)      not null,
    main_temp_max_f decimal(20,10)      not null,
    main_temp       decimal(20,10)      not null,
    main_temp_min   decimal(20,10)      not null,
    main_temp_max   decimal(20,10)      not null,
    main_pressure   int      not null,
    main_humidity   int      not null,
    sys_country     varchar(500) not null,
    sys_sunrise     datetime      not null,
    sys_sunset      datetime      not null,
    visibility      int          not null,
    wind_speed      decimal(20,10)      not null,
    wind_gust       decimal(20,10),
    wind_deg        decimal(20,10),
    base            varchar(500) not null,
    clouds_all      int      not null,
    weather_desc    varchar(4000)      not null,
    weather_icon    varchar(500)      not null,
    weather_id      int      not null,
    weather_main    varchar(500)      not null
);

-- then Vertica
create table diap01.mec_us_united_20056.ual_weather
(
    datetime_loc    datetime       not null,
    datetime_utc    datetime       not null,
    city_id         int            not null,
    city_name       varchar(500)   not null,
    coord_lat       decimal(20,10) not null,
    coord_lon       decimal(20,10) not null,
    main_temp_f     decimal(20,10) not null,
    main_temp_min_f decimal(20,10) not null,
    main_temp_max_f decimal(20,10) not null,
    main_temp       decimal(20,10) not null,
    main_temp_min   decimal(20,10) not null,
    main_temp_max   decimal(20,10) not null,
    main_pressure   int            not null,
    main_humidity   int            not null,
    sys_country     varchar(500)   not null,
    sys_sunrise     datetime       not null,
    sys_sunset      datetime       not null,
    visibility      int            not null,
    wind_speed      decimal(20,10) not null,
    wind_gust       decimal(20,10),
    wind_deg        decimal(20,10),
    base            varchar(500)   not null,
    clouds_all      int            not null,
    weather_desc    varchar(4000)  not null,
    weather_icon    varchar(500)   not null,
    weather_id      int            not null,
    weather_main    varchar(500)   not null
);

-- and transfer
-- transfer takes longer than a straight import into Vertica, but
-- import kept failing; error code was esoteric and not easily found

  insert into VerticaUnited.diap01.mec_us_united_20056.ual_weather
    select *
        from master.dbo.ual_weather
go
