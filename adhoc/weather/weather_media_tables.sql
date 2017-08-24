
-- Data to calculate average searches per location,
-- per specific month
create table diap01.mec_us_united_20056.ual_weather_ga_hist
(
    this_date int,
    city      varchar(100),
    state     varchar(100),
    lat       numeric(20,10),
    lon       numeric(20,10),
    led       numeric(20,10)
);

copy diap01.mec_us_united_20056.ual_weather_ga_hist from local 'C:\Users\matthew.hartwick\Dropbox\MEC_Work\Projects\United\20170615_weather\Searches_by_day_2015_2017.csv' WITH DELIMITER ',' DIRECT commit;
-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================
drop table diap01.mec_us_united_20056.ual_weather_ga_hist_3
create table diap01.mec_us_united_20056.ual_weather_ga_hist_3
(
    this_date int,
    lat       numeric(20,10),
    lon       numeric(20,10),
    led       numeric(20,10)
);

copy diap01.mec_us_united_20056.ual_weather_ga_hist_3 from local 'C:\Users\matthew.hartwick\Dropbox\MEC_Work\Projects\United\20170615_weather\20170725_Searches_By_Day.csv' WITH DELIMITER ',' DIRECT commit;

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================
-- drop table diap01.mec_us_united_20056.ual_weather_ga

-- Initial GA searches table
create table diap01.mec_us_united_20056.ual_weather_ga
(
    this_date int            not null,
    lat       decimal(20,10) not null,
    lon       decimal(20,10) not null,
    led       int            not null
);

-- Load data into the table with Vertica "COPY" - fast
copy diap01.mec_us_united_20056.ual_weather_ga from local 'C:\Users\matthew.hartwick\Dropbox\MEC_Work\Projects\United\20170615_weather\Searches_By_Day_coord.csv' WITH DELIMITER ',' DIRECT commit;
-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- select count(*) from diap01.mec_us_united_20056.ual_weather_ga_1

create table diap01.mec_us_united_20056.ual_weather_ga_1
(
    this_date int            not null,
    dcm_lat   decimal(20,10) not null,
    dcm_lon   decimal(20,10) not null,
--  coord   decimal(20,10) not null,
    coord     varchar(300) not null,
    led       int            not null

);

insert /*+ direct */ into diap01.mec_us_united_20056.ual_weather_ga_1
(this_date,dcm_lat,dcm_lon,coord,led)
    (
--  explain
        select
            t2.this_date,
            t2.lat          as dcm_lat,
            t2.lon          as dcm_lon,
            cast(t2.lat as varchar) || cast(t2.lon as varchar) as coord,
--          t2.lat + t2.lon as coord,
            t2.led
        from diap01.mec_us_united_20056.ual_weather_ga as t2
    );
commit;




select
   coord
    from(
select dcm_lat, dcm_lon, coord
    from diap01.mec_us_united_20056.ual_weather_ga_1
group by dcm_lat, dcm_lon, coord) as t1
group by coord
having count(coord) > 1

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================
-- drop table diap01.mec_us_united_20056.ual_weather_ga_1_test
-- create table diap01.mec_us_united_20056.ual_weather_ga_1_test
-- (
--
--  dcm_lat   decimal(20,10) not null,
--  dcm_lon   decimal(20,10) not null,
--  coord_1     decimal(20,10) not null,
--  coord_2     varchar(300) not null,
--  led       int            not null
--
-- );
--
-- insert /*+ direct */ into diap01.mec_us_united_20056.ual_weather_ga_1_test
-- (dcm_lat,dcm_lon,coord_1,coord_2,led)
--  (
-- --   explain
-- select
-- t3.dcm_lat,
-- t3.dcm_lon,
-- t3.coord_1,
--  t3.coord_2,
--  sum(t3.led) as led
--      from
-- ( select
-- --           t2.this_date,
-- t2.lat as dcm_lat,
-- t2.lon as dcm_lon,
-- t2.lat + t2.lon as coord_1,
-- cast(t2.lat as varchar ) || cast(t2.lon as varchar ) as coord_2,
-- t2.led
-- from diap01.mec_us_united_20056.ual_weather_ga as t2
-- ) as t3
-- group by
-- t3.dcm_lat,
-- t3.dcm_lon,
-- t3.coord_1,
--  t3.coord_2
--  );
-- commit;
--
-- SELECT
--     *
--     FROM diap01.mec_us_united_20056.ual_weather_ga_1_test
-- GROUP BY
--     *
--   HAVING COUNT(coord_1) > 1

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- Bring GA search and NOAA weather data together.
-- Join on year/month instead of location, then use
-- Haversine formula to find closest NOAA location
-- for each GA location.

-- drop table diap01.mec_us_united_20056.ual_weather_ga_2
create table diap01.mec_us_united_20056.ual_weather_ga_2
(
    this_date   int            not null,
    led         int            not null,
    dcm_lat     decimal(20,10) not null,
    dcm_lon     decimal(20,10) not null,
    coord   decimal(20,10) not null,
--  coord     varchar(300) not null,
    yr_mo_dy    int            not null,
    yr_mo       int            not null,
    yr_wk       int            not null,
    prcp        numeric(20,10),
    prv_dy_prcp numeric(20,10),
    prv_wk_prcp numeric(20,10),
    prv_mo_prcp numeric(20,10),
    snow        numeric(20,10),
    prv_dy_snow numeric(20,10),
    prv_wk_snow numeric(20,10),
    prv_mo_snow numeric(20,10),
    snwd        numeric(20,10),
    prv_dy_snwd numeric(20,10),
    prv_wk_snwd numeric(20,10),
    prv_mo_snwd numeric(20,10),
    tmax        numeric(20,10),
    prv_dy_tmax numeric(20,10),
    prv_wk_tmax numeric(20,10),
    prv_mo_tmax numeric(20,10),
    tmin        numeric(20,10),
    prv_dy_tmin numeric(20,10),
    prv_wk_tmin numeric(20,10),
    prv_mo_tmin numeric(20,10),
    tobs        numeric(20,10),
    lat         decimal(20,10),
    lon         decimal(20,10),
    city        varchar(300),
    dist        decimal(20,10)
);

insert /*+ direct */ into diap01.mec_us_united_20056.ual_weather_ga_2
(this_date,led,dcm_lat,dcm_lon,coord,yr_mo_dy,yr_mo,yr_wk,prcp,prv_dy_prcp,prv_wk_prcp,prv_mo_prcp,snow,prv_dy_snow,prv_wk_snow,prv_mo_snow,snwd,prv_dy_snwd,prv_wk_snwd,prv_mo_snwd,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,lat,lon,city,dist)
    (

        select
            t2.this_date,
            t2.led,
            t2.dcm_lat,
            t2.dcm_lon,
            t2.coord,
            t2.yr_mo_dy,
            t2.yr_mo,
            t2.yr_wk,
            t2.prcp,
            t2.prv_dy_prcp,
            t2.prv_wk_prcp,
            t2.prv_mo_prcp,
            t2.snow,
            t2.prv_dy_snow,
            t2.prv_wk_snow,
            t2.prv_mo_snow,
            t2.snwd,
            t2.prv_dy_snwd,
            t2.prv_wk_snwd,
            t2.prv_mo_snwd,
            t2.tmax,
            t2.prv_dy_tmax,
            t2.prv_wk_tmax,
            t2.prv_mo_tmax,
            t2.tmin,
            t2.prv_dy_tmin,
            t2.prv_wk_tmin,
            t2.prv_mo_tmin,
            t2.tobs,
            t2.lat,
            t2.lon,
            t2.city_state,
            t2.dist
--  t2.r1

        from (
                 select
                     t0.*,
--                   For each GA location, rank NOAA locations by distance.
--                   row_number() over ( partition by t0.this_date,t0.lat,t0.lon order by t0.dist asc ) as r1
                 row_number() over ( partition by t0.this_date,t0.coord order by t0.dist asc ) as r1
                 from (select
--      top(15)
                     t1.this_date,
                     t1.led,
                     t1.dcm_lat,
                     t1.dcm_lon,
                     t1.coord,
                     d1.yr_mo_dy,
                     d1.yr_mo,
                     d1.yr_wk,
                     d1.prcp,
                     d1.prv_dy_prcp,
                     d1.prv_wk_prcp,
                     d1.prv_mo_prcp,
                     d1.snow,
                     d1.prv_dy_snow,
                     d1.prv_wk_snow,
                     d1.prv_mo_snow,
                     d1.snwd,
                     d1.prv_dy_snwd,
                     d1.prv_wk_snwd,
                     d1.prv_mo_snwd,
                     d1.tmax,
                     d1.prv_dy_tmax,
                     d1.prv_wk_tmax,
                     d1.prv_mo_tmax,
                     d1.tmin,
                     d1.prv_dy_tmin,
                     d1.prv_wk_tmin,
                     d1.prv_mo_tmin,
                     d1.tobs,
                     d1.lat,
                     d1.lon,
                     d1.city_state,
                     diap01.mec_us_united_20056.udf_havDist(t1.dcm_lat,d1.lat,t1.dcm_lon,d1.lon) as dist
                 from diap01.mec_us_united_20056.ual_weather_ga_1 as t1

                     left join diap01.mec_us_united_20056.ual_weather_noaa_final as d1
                     on t1.this_date = d1.yr_mo_dy) as t0
             ) as t2
--      For each GA location, keep only the nearest NOAA location
        where t2.r1 = 1
    );
commit;
-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- select
--
-- city_state, city,
--  max(dist) from diap01.mec_us_united_20056.ual_weather_ga_2
-- group by city_state, city
-- order by max(dist) desc
-- limit 1
--
--
-- select * from diap01.mec_us_united_20056.ual_weather_ga_2
--  where city_state = 'honolulu_hi'
--
-- select * from diap01.mec_us_united_20056.ual_weather_noaa
--  where city_state like '%honolulu%'

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- drop table diap01.mec_us_united_20056.ual_weather_ga_3

-- Pull State from "city" field.
-- Could have accomplished this in the above table, but
-- didn't would add more complexity to already lengthy (run time)
-- query
create table diap01.mec_us_united_20056.ual_weather_ga_3
(
    this_date   int            not null,
    state       varchar(4)     not null,
    led         int            not null,
    dcm_lat     decimal(20,10) not null,
    dcm_lon     decimal(20,10) not null,
    coord   decimal(20,10) not null,
--  coord     varchar(300) not null,
    yr_mo_dy    int            not null,
    yr_mo       int            not null,
    yr_wk       int            not null,
    prcp        numeric(20,10),
    prv_dy_prcp numeric(20,10),
    prv_wk_prcp numeric(20,10),
    prv_mo_prcp numeric(20,10),
    snow        numeric(20,10),
    prv_dy_snow numeric(20,10),
    prv_wk_snow numeric(20,10),
    prv_mo_snow numeric(20,10),
    snwd        numeric(20,10),
    prv_dy_snwd numeric(20,10),
    prv_wk_snwd numeric(20,10),
    prv_mo_snwd numeric(20,10),
    tmax        numeric(20,10),
    prv_dy_tmax numeric(20,10),
    prv_wk_tmax numeric(20,10),
    prv_mo_tmax numeric(20,10),
    tmin        numeric(20,10),
    prv_dy_tmin numeric(20,10),
    prv_wk_tmin numeric(20,10),
    prv_mo_tmin numeric(20,10),
    tobs        numeric(20,10),
    lat         decimal(20,10),
    lon         decimal(20,10),
    city        varchar(300),
    dist        decimal(20,10)
);


insert /*+ direct */ into diap01.mec_us_united_20056.ual_weather_ga_3
(this_date,state,led,dcm_lat,dcm_lon,coord,yr_mo_dy,yr_mo,yr_wk,prcp,prv_dy_prcp,prv_wk_prcp,prv_mo_prcp,snow,prv_dy_snow,prv_wk_snow,prv_mo_snow,snwd,prv_dy_snwd,prv_wk_snwd,prv_mo_snwd,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,lat,lon,city,dist)

    (
        select
            this_date,state,led,dcm_lat,dcm_lon,coord,yr_mo_dy,yr_mo,yr_wk,prcp,prv_dy_prcp,prv_wk_prcp,prv_mo_prcp,snow,prv_dy_snow,prv_wk_snow,prv_mo_snow,snwd,prv_dy_snwd,prv_wk_snwd,prv_mo_snwd,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,lat,lon,city,dist
        from (

                 select
                     this_date,right(city,2) as state,led,dcm_lat,dcm_lon,coord,yr_mo_dy,yr_mo,yr_wk,prcp,prv_dy_prcp,prv_wk_prcp,prv_mo_prcp,snow,prv_dy_snow,prv_wk_snow,prv_mo_snow,snwd,prv_dy_snwd,prv_wk_snwd,prv_mo_snwd,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,lat,lon,city,dist
                 from diap01.mec_us_united_20056.ual_weather_ga_2) as t1
--      where dist <= 10
        group by
            this_date,state,led,dcm_lat,dcm_lon,coord,yr_mo_dy,yr_mo,yr_wk,prcp,prv_dy_prcp,prv_wk_prcp,prv_mo_prcp,snow,prv_dy_snow,prv_wk_snow,prv_mo_snow,snwd,prv_dy_snwd,prv_wk_snwd,prv_mo_snwd,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,lat,lon,city,dist
    );
commit;

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- Full Table, temperature in Celsius
-- drop table diap01.mec_us_united_20056.ual_weather_ga_4

-- Add Census Regions and Divisions
create table diap01.mec_us_united_20056.ual_weather_ga_4
(
    this_date   int            not null,
    state       varchar(4)     not null,
    region      varchar(100)   not null,
    division    varchar(10)    not null,
    led         int            not null,
    dcm_lat     decimal(20,10) not null,
    dcm_lon     decimal(20,10) not null,
    coord   decimal(20,10) not null,
--  coord     varchar(300) not null,
    yr_mo_dy    int            not null,
    yr_mo       int            not null,
    yr_wk       int            not null,
    prcp        numeric(20,10),
    prv_dy_prcp numeric(20,10),
    prv_wk_prcp numeric(20,10),
    prv_mo_prcp numeric(20,10),
    snow        numeric(20,10),
    prv_dy_snow numeric(20,10),
    prv_wk_snow numeric(20,10),
    prv_mo_snow numeric(20,10),
    snwd        numeric(20,10),
    prv_dy_snwd numeric(20,10),
    prv_wk_snwd numeric(20,10),
    prv_mo_snwd numeric(20,10),
    tmax        numeric(20,10),
    prv_dy_tmax numeric(20,10),
    prv_wk_tmax numeric(20,10),
    prv_mo_tmax numeric(20,10),
    tmin        numeric(20,10),
    prv_dy_tmin numeric(20,10),
    prv_wk_tmin numeric(20,10),
    prv_mo_tmin numeric(20,10),
    tobs        numeric(20,10),
    lat         decimal(20,10),
    lon         decimal(20,10),
    city        varchar(300),
    dist        decimal(20,10)
);

-- drop table diap01.mec_us_united_20056.ual_weather_ga_4

insert  /*+ direct */  into diap01.mec_us_united_20056.ual_weather_ga_4
(this_date,state,region,division,led,dcm_lat,dcm_lon,coord,yr_mo_dy,yr_mo,yr_wk,prcp,prv_dy_prcp,prv_wk_prcp,prv_mo_prcp,snow,prv_dy_snow,prv_wk_snow,prv_mo_snow,snwd,prv_dy_snwd,prv_wk_snwd,prv_mo_snwd,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,lat,lon,city,dist)

(
select this_date,state,
case when state in ('il','in','mi','oh','wi','ia','ks','mn','mo','ne','nd','sd') then 'midwest'
     when state in ('nj','ny','pa','ct','me','ma','nh','ri','vt') then 'northeast'
     when state in ('al','ky','ms','tn','de','fl','ga','md','nc','sc','va','wv','ar','la','ok','tx','dc') then 'south'
     when state in ('az','co','id','mt','nv','nm','ut','wy','ca','or','wa') then 'west'
     when state in ('ak','hi') then 'ak_hi'
else 'xxx' end as region,
    case when state in ('ak','hi') then 'ak_hi'
     when state in ('il','in','mi','oh','wi') then 'enc'
     when state in ('al','ky','ms','tn') then 'esc'
     when state in ('nj','ny','pa') then 'mid'
     when state in ('az','co','id','mt','nv','nm','ut','wy') then 'mtn'
     when state in ('ct','me','ma','nh','ri','vt') then 'new'
     when state in ('ca','or','wa') then 'pac'
     when state in ('de','fl','ga','md','nc','sc','va','wv','dc') then 'sat'
     when state in ('ia','ks','mn','mo','ne','nd','sd') then 'wnc'
     when state in ('ar','la','ok','tx') then 'wsc'
else 'xxx' end as division,

    led,dcm_lat,dcm_lon,coord,yr_mo_dy,yr_mo,yr_wk,prcp,prv_dy_prcp,prv_wk_prcp,prv_mo_prcp,snow,prv_dy_snow,prv_wk_snow,prv_mo_snow,snwd,prv_dy_snwd,prv_wk_snwd,prv_mo_snwd,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,lat,lon,city,dist
        from diap01.mec_us_united_20056.ual_weather_ga_3

    ) ;
commit;

-- select avg(led) from diap01.mec_us_united_20056.ual_weather_ga_4
-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- Same as "ga_4," but with temperature in Fahrenheit.
-- Also, 1.) remove Alaska and Hawaii; weather in those States
-- is too different from others for aggregation, and
-- 2.) limit max distance between GA location and matching
-- NOAA location to 10 km.

-- drop table diap01.mec_us_united_20056.ual_weather_ga_5
create table diap01.mec_us_united_20056.ual_weather_ga_5
(
    this_date   int            not null,
    state       varchar(4)     not null,
    region      varchar(100)   not null,
    division    varchar(10)    not null,
    led         int            not null,
    dcm_lat     decimal(20,10) not null,
    dcm_lon     decimal(20,10) not null,
    coord   decimal(20,10) not null,
--  coord     varchar(300) not null,
    yr_mo_dy    int            not null,
    yr_mo       int            not null,
    yr_wk       int            not null,
    prcp        numeric(20,10),
    prv_dy_prcp numeric(20,10),
    prv_wk_prcp numeric(20,10),
    prv_mo_prcp numeric(20,10),
    snow        numeric(20,10),
    prv_dy_snow numeric(20,10),
    prv_wk_snow numeric(20,10),
    prv_mo_snow numeric(20,10),
    snwd        numeric(20,10),
    prv_dy_snwd numeric(20,10),
    prv_wk_snwd numeric(20,10),
    prv_mo_snwd numeric(20,10),
    tmax        numeric(20,10),
    prv_dy_tmax numeric(20,10),
    prv_wk_tmax numeric(20,10),
    prv_mo_tmax numeric(20,10),
    tmin        numeric(20,10),
    prv_dy_tmin numeric(20,10),
    prv_wk_tmin numeric(20,10),
    prv_mo_tmin numeric(20,10),
    tobs        numeric(20,10),
    lat         decimal(20,10),
    lon         decimal(20,10),
    city        varchar(300),
    dist        decimal(20,10)
);

insert /*+ direct */  into diap01.mec_us_united_20056.ual_weather_ga_5
(this_date,state,region,division,led,dcm_lat,dcm_lon,coord,yr_mo_dy,yr_mo,yr_wk,prcp,prv_dy_prcp,prv_wk_prcp,prv_mo_prcp,snow,prv_dy_snow,prv_wk_snow,prv_mo_snow,snwd,prv_dy_snwd,prv_wk_snwd,prv_mo_snwd,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,lat,lon,city,dist)
    (
        select
            this_date,
            state,
            region,
            division,
            led,
            dcm_lat,
            dcm_lon,
            coord,
            yr_mo_dy,
            yr_mo,
            yr_wk,
            prcp,
            prv_dy_prcp,
            prv_wk_prcp,
            prv_mo_prcp,
            snow,
            prv_dy_snow,
            prv_wk_snow,
            prv_mo_snow,
            snwd,
            prv_dy_snwd,
            prv_wk_snwd,
            prv_mo_snwd,
            ((tmax / 10) * 1.8) + 32        as tmax,
            ((prv_dy_tmax / 10) * 1.8) + 32 as prv_dy_tmax,
            ((prv_wk_tmax / 10) * 1.8) + 32 as prv_wk_tmax,
            ((prv_mo_tmax / 10) * 1.8) + 32 as prv_mo_tmax,
            ((tmin / 10) * 1.8) + 32        as tmin,
            ((prv_dy_tmin / 10) * 1.8) + 32 as prv_dy_tmin,
            ((prv_wk_tmin / 10) * 1.8) + 32 as prv_wk_tmin,
            ((prv_mo_tmin / 10) * 1.8) + 32 as prv_mo_tmin,
            ((tobs / 10) * 1.8) + 32        as tobs,
            lat,
            lon,
            city,
            dist
        from diap01.mec_us_united_20056.ual_weather_ga_4
        where state <> 'hi'
        and state <> 'ak'
--      and dist <= 10
    );
commit;

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- drop table diap01.mec_us_united_20056.ual_weather_ga_5
--
-- create table diap01.mec_us_united_20056.ual_weather_ga_6
-- (
--  this_date   int            not null,
--  city_state  varchar(300)   not null,
--  state       varchar(4)     not null,
--  region varchar(100) not null,
--  division varchar(10) not null,
--  user_cnt    int            not null,
--  led         int            not null,
--  yr_mo_dy    int            not null,
--  yr_mo       int            not null,
--  yr_wk       int            not null,
--  tmax        numeric(20,10),
--  prv_dy_tmax numeric(20,10),
--  prv_wk_tmax numeric(20,10),
--  prv_mo_tmax numeric(20,10),
--  tmin        numeric(20,10),
--  temp_avg    numeric(20,10),
--  prv_dy_tmin numeric(20,10),
--  prv_wk_tmin numeric(20,10),
--  prv_mo_tmin numeric(20,10),
--  tobs        numeric(20,10),
--  city        varchar(300),
--  dist        decimal(20,10)
-- --   r1 int
--
-- );
--
-- insert  /*+ direct */  into diap01.mec_us_united_20056.ual_weather_ga_6
-- (this_date,city_state,state,region,division,user_cnt,led,yr_mo_dy,yr_mo,yr_wk,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,temp_avg,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,city,dist)
--
-- (
-- --   explain
-- select
-- this_date,
-- city_state,
-- state,
-- region,
-- division,
-- user_cnt,
-- led,
-- yr_mo_dy,
-- yr_mo,
-- yr_wk,
-- tmax,
-- prv_dy_tmax,
-- prv_wk_tmax,
-- prv_mo_tmax,
-- tmin,
-- (tmax + tmin)/2 as temp_avg,
-- prv_dy_tmin,
-- prv_wk_tmin,
-- prv_mo_tmin,
-- tobs,
-- city,
-- dist
--      from diap01.mec_us_united_20056.ual_weather_ga_5
--
--      where dist <= 10
--     ) ;
-- commit;

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- adding in avg leads per month per location in attempt to normalize
-- drop table diap01.mec_us_united_20056.ual_weather_ga_6

-- select count(*) from diap01.mec_us_united_20056.ual_weather_ga_5
-- select count(*) from diap01.mec_us_united_20056.ual_weather_ga_6
-- select count(*) from diap01.mec_us_united_20056.ual_weather_ga_8

-- create table diap01.mec_us_united_20056.ual_weather_ga_6
-- (
--  this_date   int            not null,
--  state       varchar(4)     not null,
--  region      varchar(100)   not null,
--  division    varchar(10)    not null,
--  led         numeric(20,10) not null,
--  n_led       numeric(20,10),
--  yr_mo       int            not null,
--  yr_wk       int            not null,
--  tmax        numeric(20,10),
--  prv_dy_tmax numeric(20,10),
--  prv_wk_tmax numeric(20,10),
--  prv_mo_tmax numeric(20,10),
--  tmin        numeric(20,10),
--  temp_avg    numeric(20,10),
--  prv_dy_tmin numeric(20,10),
--  prv_wk_tmin numeric(20,10),
--  prv_mo_tmin numeric(20,10),
--  tobs        numeric(20,10),
--  city        varchar(300),
--  dist        decimal(20,10)
-- );
--
-- insert  /*+ direct */  into diap01.mec_us_united_20056.ual_weather_ga_6
-- (this_date,state,region,division,led,n_led,yr_mo,yr_wk,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,temp_avg,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,city,dist)
--
-- (
-- select
-- t1.this_date,
-- t1.state,
-- t1.region,
-- t1.division,
-- cast(t1.led as numeric(20,10)) as led,
-- t1.led-t2.avg_led as n_led,
-- t1.yr_mo,
-- t1.yr_wk,
-- t1.tmax,
-- t1.prv_dy_tmax,
-- t1.prv_wk_tmax,
-- t1.prv_mo_tmax,
-- t1.tmin,
-- t1.temp_avg,
-- t1.prv_dy_tmin,
-- t1.prv_wk_tmin,
-- t1.prv_mo_tmin,
-- t1.tobs,
-- t1.city,
-- t1.dist
--
--
--
-- from (
-- select
-- this_date,
-- state,
-- region,
-- division,
-- led,
-- dcm_lat,
-- dcm_lon,
-- dcm_lat + dcm_lon as coord,
-- yr_mo_dy,
-- yr_mo,
-- right(cast(yr_mo as varchar),2) as this_month,
-- yr_wk,
-- tmax,
-- prv_dy_tmax,
-- prv_wk_tmax,
-- prv_mo_tmax,
-- tmin,
-- (tmax + tmin)/2 as temp_avg,
-- prv_dy_tmin,
-- prv_wk_tmin,
-- prv_mo_tmin,
-- tobs,
-- city,
-- dist
-- from diap01.mec_us_united_20056.ual_weather_ga_5
--
-- -- where dist <= 10
-- ) as t1
--      left join diap01.mec_us_united_20056.ual_weather_ga_avg_led_coord_num as t2
--  on t1.this_month = t2.this_month
-- --       and t1.dcm_lat = t2.lat
-- --       and t1.dcm_lon = t2.lon
--  and t1.coord = t2.coord
--     ) ;
-- commit;


-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- same as ga_6, but with prcp observations
-- drop table diap01.mec_us_united_20056.ual_weather_ga_7
create table diap01.mec_us_united_20056.ual_weather_ga_7
(
    this_date   int            not null,
    state       varchar(4)     not null,
    region      varchar(100)   not null,
    division    varchar(10)    not null,
    led         numeric(20,10) not null,
    n_led       numeric(20,10),
    dcm_lat     decimal(20,10) not null,
    dcm_lon     decimal(20,10) not null,
    yr_mo       int            not null,
    yr_wk       int            not null,
    prcp        numeric(20,10),
    prv_dy_prcp numeric(20,10),
    prv_wk_prcp numeric(20,10),
    prv_mo_prcp numeric(20,10),
    snow        numeric(20,10),
    prv_dy_snow numeric(20,10),
    prv_wk_snow numeric(20,10),
    prv_mo_snow numeric(20,10),
    snwd        numeric(20,10),
    prv_dy_snwd numeric(20,10),
    prv_wk_snwd numeric(20,10),
    prv_mo_snwd numeric(20,10),
    tmax        numeric(20,10),
    prv_dy_tmax numeric(20,10),
    prv_wk_tmax numeric(20,10),
    prv_mo_tmax numeric(20,10),
    tmin        numeric(20,10),
    temp_avg    numeric(20,10),
    prv_dy_tmin numeric(20,10),
    prv_wk_tmin numeric(20,10),
    prv_mo_tmin numeric(20,10),
    tobs        numeric(20,10),
    lat         decimal(20,10),
    lon         decimal(20,10),
    city        varchar(300),
    dist        decimal(20,10)
--  r1 int

);

insert  /*+ direct */  into diap01.mec_us_united_20056.ual_weather_ga_7
(this_date,state,region,division,led,n_led,dcm_lat,dcm_lon,yr_mo,yr_wk,prcp,prv_dy_prcp,prv_wk_prcp,prv_mo_prcp,snow,prv_dy_snow,prv_wk_snow,prv_mo_snow,snwd,prv_dy_snwd,prv_wk_snwd,prv_mo_snwd,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,temp_avg,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,lat,lon,city,dist)

    (

        select
            t2.this_date,
            t2.state,
            t2.region,
            t2.division,
            t2.led,
            t2.n_led,
            t2.dcm_lat,
            t2.dcm_lon,
            t2.yr_mo,
            t2.yr_wk,
            t2.prcp,
            t2.prv_dy_prcp,
            t2.prv_wk_prcp,
            t2.prv_mo_prcp,
            t2.snow,
            t2.prv_dy_snow,
            t2.prv_wk_snow,
            t2.prv_mo_snow,
            t2.snwd,
            t2.prv_dy_snwd,
            t2.prv_wk_snwd,
            t2.prv_mo_snwd,
            t2.tmax,
            t2.prv_dy_tmax,
            t2.prv_wk_tmax,
            t2.prv_mo_tmax,
            t2.tmin,
            t2.temp_avg,
            t2.prv_dy_tmin,
            t2.prv_wk_tmin,
            t2.prv_mo_tmin,
            t2.tobs,
            t2.lat,
            t2.lon,
            t2.city,
            t2.dist

        from( select

            t1.this_date,
            t1.state,
            t1.region,
            t1.division,
            cast(t1.led as numeric(20,10)) as led,
--          case when t1.led - t2.avg_led <=0 then 0 else t1.led - t2.avg_led end as n_led,
            t1.led - isnull(t2.avg_led,0) as n_led,
            t1.dcm_lat,
            t1.dcm_lon,
            t1.yr_mo,
            t1.yr_wk,
            t1.prcp,
            t1.prv_dy_prcp,
            t1.prv_wk_prcp,
            t1.prv_mo_prcp,
            t1.snow,
            t1.prv_dy_snow,
            t1.prv_wk_snow,
            t1.prv_mo_snow,
            t1.snwd,
            t1.prv_dy_snwd,
            t1.prv_wk_snwd,
            t1.prv_mo_snwd,
            t1.tmax,
            t1.prv_dy_tmax,
            t1.prv_wk_tmax,
            t1.prv_mo_tmax,
            t1.tmin,
            t1.temp_avg,
            t1.prv_dy_tmin,
            t1.prv_wk_tmin,
            t1.prv_mo_tmin,
            t1.tobs,
            t1.lat,
            t1.lon,
            t1.city,
            t1.dist


        from (
                 select
                     this_date,
                     state,
                     region,
                     division,
                     led,
                     dcm_lat,
                     dcm_lon,
--                   dcm_lat + dcm_lon               as coord,
                     coord,
                     yr_mo_dy,
                     yr_mo,
                     right(cast(yr_mo as varchar),2) as this_month,
                     yr_wk,
                     prcp,
                     prv_dy_prcp,
                     prv_wk_prcp,
                     prv_mo_prcp,
                     snow,
                     prv_dy_snow,
                     prv_wk_snow,
                     prv_mo_snow,
                     snwd,
                     prv_dy_snwd,
                     prv_wk_snwd,
                     prv_mo_snwd,
                     tmax,
                     prv_dy_tmax,
                     prv_wk_tmax,
                     prv_mo_tmax,
                     tmin,
                     (tmax + tmin) / 2               as temp_avg,
                     prv_dy_tmin,
                     prv_wk_tmin,
                     prv_mo_tmin,
                     tobs,
                     lat,
                     lon,
                     city,
                     dist
                 from diap01.mec_us_united_20056.ual_weather_ga_5

                 where dist <= 10
             ) as t1
            left join diap01.mec_us_united_20056.ual_weather_ga_avg_led_coord_num as t2
    on t1.this_month = t2.this_month
--      and t1.dcm_lat = t2.lat
--      and t1.dcm_lon = t2.lon
            and t1.coord = t2.coord
    ) as t2
-- where t2.n_led > 0


    ) ;
commit;


-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- same as ga_6, but with prcp observations
-- drop table diap01.mec_us_united_20056.ual_weather_ga_7
create table diap01.mec_us_united_20056.ual_weather_ga_8
(
    this_date   int            not null,
    state       varchar(4)     not null,
    region      varchar(100)   not null,
    division    varchar(10)    not null,
    led         numeric(20,10) not null,
    n_led       numeric(20,10),
    dcm_lat     decimal(20,10) not null,
    dcm_lon     decimal(20,10) not null,
    yr_mo       int            not null,
    yr_wk       int            not null,
    prcp        numeric(20,10),
    prv_dy_prcp numeric(20,10),
    prv_wk_prcp numeric(20,10),
    prv_mo_prcp numeric(20,10),
    snow        numeric(20,10),
    prv_dy_snow numeric(20,10),
    prv_wk_snow numeric(20,10),
    prv_mo_snow numeric(20,10),
    snwd        numeric(20,10),
    prv_dy_snwd numeric(20,10),
    prv_wk_snwd numeric(20,10),
    prv_mo_snwd numeric(20,10),
    tmax        numeric(20,10),
    prv_dy_tmax numeric(20,10),
    prv_wk_tmax numeric(20,10),
    prv_mo_tmax numeric(20,10),
    tmin        numeric(20,10),
    temp_avg    numeric(20,10),
    prv_dy_tmin numeric(20,10),
    prv_wk_tmin numeric(20,10),
    prv_mo_tmin numeric(20,10),
    tobs        numeric(20,10),
    lat         decimal(20,10),
    lon         decimal(20,10),
    city        varchar(300),
    dist        decimal(20,10)
--  r1 int

);

insert  /*+ direct */  into diap01.mec_us_united_20056.ual_weather_ga_8
(this_date,state,region,division,led,n_led,dcm_lat,dcm_lon,yr_mo,yr_wk,prcp,prv_dy_prcp,prv_wk_prcp,prv_mo_prcp,snow,prv_dy_snow,prv_wk_snow,prv_mo_snow,snwd,prv_dy_snwd,prv_wk_snwd,prv_mo_snwd,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,temp_avg,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,lat,lon,city,dist)

    (

        select
            t2.this_date,
            t2.state,
            t2.region,
            t2.division,
            t2.led,
            t2.n_led,
            t2.dcm_lat,
            t2.dcm_lon,
            t2.yr_mo,
            t2.yr_wk,
            t2.prcp,
            t2.prv_dy_prcp,
            t2.prv_wk_prcp,
            t2.prv_mo_prcp,
            t2.snow,
            t2.prv_dy_snow,
            t2.prv_wk_snow,
            t2.prv_mo_snow,
            t2.snwd,
            t2.prv_dy_snwd,
            t2.prv_wk_snwd,
            t2.prv_mo_snwd,
            t2.tmax,
            t2.prv_dy_tmax,
            t2.prv_wk_tmax,
            t2.prv_mo_tmax,
            t2.tmin,
            t2.temp_avg,
            t2.prv_dy_tmin,
            t2.prv_wk_tmin,
            t2.prv_mo_tmin,
            t2.tobs,
            t2.lat,
            t2.lon,
            t2.city,
            t2.dist

        from( select

            t1.this_date,
            t1.state,
            t1.region,
            t1.division,
            cast(t1.led as numeric(20,10)) as led,
--          case when t1.led - t2.avg_led <=0 then 0 else t1.led - t2.avg_led end as n_led,
            t1.led - t2.avg_led as n_led,
            t1.dcm_lat,
            t1.dcm_lon,
            t1.yr_mo,
            t1.yr_wk,
            t1.prcp,
            t1.prv_dy_prcp,
            t1.prv_wk_prcp,
            t1.prv_mo_prcp,
            t1.snow,
            t1.prv_dy_snow,
            t1.prv_wk_snow,
            t1.prv_mo_snow,
            t1.snwd,
            t1.prv_dy_snwd,
            t1.prv_wk_snwd,
            t1.prv_mo_snwd,
            t1.tmax,
            t1.prv_dy_tmax,
            t1.prv_wk_tmax,
            t1.prv_mo_tmax,
            t1.tmin,
            t1.temp_avg,
            t1.prv_dy_tmin,
            t1.prv_wk_tmin,
            t1.prv_mo_tmin,
            t1.tobs,
            t1.lat,
            t1.lon,
            t1.city,
            t1.dist


        from (
                 select
                     this_date,
                     state,
                     region,
                     division,
                     led,
                     dcm_lat,
                     dcm_lon,
--                   dcm_lat + dcm_lon               as coord,
                     coord,
                     yr_mo_dy,
                     yr_mo,
                     right(cast(yr_mo as varchar),2) as this_month,
                     yr_wk,
                     prcp,
                     prv_dy_prcp,
                     prv_wk_prcp,
                     prv_mo_prcp,
                     snow,
                     prv_dy_snow,
                     prv_wk_snow,
                     prv_mo_snow,
                     snwd,
                     prv_dy_snwd,
                     prv_wk_snwd,
                     prv_mo_snwd,
                     tmax,
                     prv_dy_tmax,
                     prv_wk_tmax,
                     prv_mo_tmax,
                     tmin,
                     (tmax + tmin) / 2               as temp_avg,
                     prv_dy_tmin,
                     prv_wk_tmin,
                     prv_mo_tmin,
                     tobs,
                     lat,
                     lon,
                     city,
                     dist
                 from diap01.mec_us_united_20056.ual_weather_ga_5

                 where dist <= 10
             ) as t1
            left join diap01.mec_us_united_20056.ual_weather_ga_avg_led_coord_num as t2
    on t1.this_month = t2.this_month
--      and t1.dcm_lat = t2.lat
--      and t1.dcm_lon = t2.lon
            and cast(t1.coord as decimal(20,3)) = cast(t2.coord as decimal(20,3))
    ) as t2
-- where t2.n_led > 0


    ) ;
commit;
-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================
select count(*) from diap01.mec_us_united_20056.ual_weather_ga_7
select count(*) from diap01.mec_us_united_20056.ual_weather_ga_8

SELECT
    coord
    FROM diap01.mec_us_united_20056.ual_weather_ga_avg_led
GROUP BY
    coord
  HAVING COUNT(coord) > 1


select * from diap01.mec_us_united_20056.ual_weather_ga_7
where dist <= .25
and division = 'new'

select
t1.coord
from
    (select
        coord from
    diap01.mec_us_united_20056.ual_weather_ga_avg_led_coord_num
        group by coord) as t1

GROUP BY
    coord
  HAVING COUNT(coord) > 1







select
    state,
    min(dist)
from diap01.mec_us_united_20056.ual_weather_ga_7

group by state


select
--  city,
--  state,
    division,
    min(dist) as min_dist,
    max(dist) as max_dist,
    avg(dist) as avg_dist,
    sum(n_led) as n_led
from diap01.mec_us_united_20056.ual_weather_ga_7

group by
        division
--  city,
--  state



SELECT table_schema, table_name, create_time FROM tables
