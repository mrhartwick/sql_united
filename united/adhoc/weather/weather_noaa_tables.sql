create table diap01.mec_us_united_20056.ual_weather_noaa
(
    yr_mo_dy     int          ,
    prcp         numeric(20,10),
    snow         numeric(20,10),
    snwd         numeric(20,10),
    tmax         numeric(20,10),
    tmin         numeric(20,10),
    tobs         numeric(20,10),
    lat          numeric(20,10),
    lon          numeric(20,10),
    city_state   varchar(300)
);
-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================



copy diap01.mec_us_united_20056.ual_weather_noaa from local 'C:\Users\matthew.hartwick\Dropbox\MEC_Work\Projects\United\20170615_weather\master_dbo_ual_weather_final_noaa.csv' WITH DELIMITER ',' DIRECT;

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================
create table diap01.mec_us_united_20056.ual_weather_ga_hist
(
    this_date     int          ,
    city varchar(100),
    state varchar(100),
    lat         numeric(20,10),
    lon         numeric(20,10),
led numeric(20,10)
);


copy diap01.mec_us_united_20056.ual_weather_ga_hist from local 'C:\Users\matthew.hartwick\Dropbox\MEC_Work\Projects\United\20170615_weather\Searches_by_day_2015_2017.csv' WITH DELIMITER ',' DIRECT commit;
-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- Pulled same data as "Searches_by_day_2015_2017," with city and State
-- added in. But total # rows was less than when using just lat/lon.
-- Not sure why, but likely has to do with sampling (even though I
-- requested unsampled reports)?

-- create table diap01.mec_us_united_20056.ual_weather_ga_hist_full
-- (
--     this_date int,
--     city      varchar(100),
--     state     varchar(100),
--     lat       numeric(20,10),
--     lon       numeric(20,10),
--     led       numeric(20,10)
-- );
--
--
-- copy diap01.mec_us_united_20056.ual_weather_ga_hist_full from local 'C:\Users\matthew.hartwick\Dropbox\MEC_Work\Projects\United\20170615_weather\ga_searches.csv' WITH DELIMITER ',' DIRECT commit;
-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================
create table diap01.mec_us_united_20056.ual_weather_noaa_2
(
    yr_mo_dy     int          ,
    yr_mo     int          ,
    yr_wk     int          ,
    PRCP         numeric(20,10),
    SNOW         numeric(20,10),
    SNWD         numeric(20,10),
    TMAX         numeric(20,10),
    TMIN         numeric(20,10),
    TOBS         numeric(20,10),
    lat          numeric(20,10),
    lon          numeric(20,10),
    crd     varchar(300),
--     crd          numeric(20,10),
    city_state   varchar(300)
);
-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================


-- drop table diap01.mec_us_united_20056.ual_weather_noaa_2

insert into diap01.mec_us_united_20056.ual_weather_noaa_2
(yr_mo_dy,yr_mo,yr_wk,PRCP,SNOW,SNWD,TMAX,TMIN,TOBS,lat,lon,crd,city_state)

    (
        select
yr_mo_dy,
cast(left(cast(yr_mo_dy as varchar), 6) as int) as yr_mo,
cast(cast(year(cast(cast(yr_mo_dy as varchar) as date)) as varchar) || cast(week(cast(cast(yr_mo_dy as varchar) as date)) as varchar) as int) as yr_wk,
PRCP,
-- lag(PRCP,1,0) over (partition by lat, lon order by yr_mo_dy asc) as prv_day_prcp,
SNOW,
-- lag(SNOW,1,0) over (partition by lat, lon order by yr_mo_dy asc) as prv_day_snow,
SNWD,
-- lag(SNWD,1,0) over (partition by lat, lon order by yr_mo_dy asc) as prv_day_snwd,
TMAX,
-- lag(tmax,1,0) over (partition by lat, lon order by yr_mo_dy asc) as prv_day_tmax,
TMIN,
-- lag(TMIN,1,0) over (partition by lat, lon order by yr_mo_dy asc) as prv_day_tmin,
TOBS,
-- lag(TOBS,1,0) over (partition by lat, lon order by yr_mo_dy asc) as prv_day_tobs,
lat,
lon,
cast(lat as varchar) || cast(lon as varchar) as crd,
-- lat + lon as crd,
city_state
    from diap01.mec_us_united_20056.ual_weather_noaa
           );
commit;

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================
-- drop table diap01.mec_us_united_20056.ual_weather_noaa_wk

create table diap01.mec_us_united_20056.ual_weather_noaa_wk
(
    yr_wk       int,
    prcp        numeric(20,10),
    prv_wk_prcp numeric(20,10),
    snow        numeric(20,10),
    prv_wk_snow numeric(20,10),
    snwd        numeric(20,10),
    prv_wk_snwd numeric(20,10),
    tmax        numeric(20,10),
    prv_wk_tmax numeric(20,10),
    tmin        numeric(20,10),
    prv_wk_tmin numeric(20,10),
    tobs        numeric(20,10),
    lat         numeric(20,10),
    lon         numeric(20,10),
--     crd          numeric(20,10),
    crd         varchar(300),
    city_state  varchar(300)
);


insert into diap01.mec_us_united_20056.ual_weather_noaa_wk
(yr_wk,prcp,prv_wk_prcp,SNOW,prv_wk_snow,SNWD,prv_wk_snwd,TMAX,prv_wk_tmax,TMIN,prv_wk_tmin,TOBS,lat,lon,crd,city_state)

    (
        select
t2.yr_wk,
t2.prcp,
t2.prv_wk_prcp,
t2.snow,
t2.prv_wk_snow,
t2.snwd,
t2.prv_wk_snwd,
t2.tmax,
t2.prv_wk_tmax,
t2.tmin,
t2.prv_wk_tmin,
t2.tobs,
lat,
lon,
crd,
city_state

from (select
t1.yr_wk,
t1.prcp,
t1.prcp - lag(t1.prcp,1,0) over (partition by crd order by t1.crd,t1.yr_wk asc) as prv_wk_prcp,
t1.snow,
t1.snow - lag(t1.snow,1,0) over (partition by crd order by t1.crd,t1.yr_wk asc) as prv_wk_snow,
t1.snwd,
t1.snwd - lag(t1.snwd,1,0) over (partition by crd order by t1.crd,t1.yr_wk asc) as prv_wk_snwd,
t1.tmax,
t1.tmax - lag(t1.tmax,1,0) over (partition by crd order by t1.crd,t1.yr_wk asc) as prv_wk_tmax,
t1.tmin,
t1.tmin - lag(t1.tmin,1,0) over (partition by crd order by t1.crd,t1.yr_wk asc) as prv_wk_tmin,
t1.tobs,
lat,
lon,
crd,
city_state
        from
(select
            yr_wk,
            avg(prcp) as prcp,
            avg(snow) as snow,
            avg(snwd) as snwd,
            avg(tmax) as tmax,
            avg(tmin) as tmin,
            avg(tobs) as tobs,
            lat,
            lon,
            crd,
            city_state from diap01.mec_us_united_20056.ual_weather_noaa_2
group by
yr_wk,
lat,
lon,
    crd,
city_state
        ) as t1
where t1.tmax + t1.tmin <> 0

) as t2
           );
commit;

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- drop table diap01.mec_us_united_20056.ual_weather_noaa_mo

create table diap01.mec_us_united_20056.ual_weather_noaa_mo
(
    yr_mo     int          ,
    prcp         numeric(20,10),
    prv_mo_prcp numeric(20,10),
    snow         numeric(20,10),
    prv_mo_snow numeric(20,10),
    snwd         numeric(20,10),
    prv_mo_snwd numeric(20,10),
    tmax         numeric(20,10),
    prv_mo_tmax numeric(20,10),
    tmin         numeric(20,10),
    prv_mo_tmin numeric(20,10),
    tobs         numeric(20,10),
    lat          numeric(20,10),
    lon          numeric(20,10),
        crd     varchar(300),
--     crd          numeric(20,10),
    city_state   varchar(300)
);


insert into diap01.mec_us_united_20056.ual_weather_noaa_mo
(yr_mo,prcp,prv_mo_prcp,SNOW,prv_mo_snow,SNWD,prv_mo_snwd,TMAX,prv_mo_tmax,TMIN,prv_mo_tmin,TOBS,lat,lon,crd,city_state)

    (
        select
t2.yr_mo,
t2.prcp,
t2.prv_mo_prcp,
t2.snow,
t2.prv_mo_snow,
t2.snwd,
t2.prv_mo_snwd,
t2.tmax,
t2.prv_mo_tmax,
t2.tmin,
t2.prv_mo_tmin,
t2.tobs,
lat,
lon,
crd,
city_state

from (select
t1.yr_mo,
t1.prcp,
t1.prcp - lag(t1.prcp,1,0) over (partition by crd order by t1.crd,t1.yr_mo asc) as prv_mo_prcp,
t1.snow,
t1.snow - lag(t1.snow,1,0) over (partition by crd order by t1.crd,t1.yr_mo asc) as prv_mo_snow,
t1.snwd,
t1.snwd - lag(t1.snwd,1,0) over (partition by crd order by t1.crd,t1.yr_mo asc) as prv_mo_snwd,
t1.tmax,
t1.tmax - lag(t1.tmax,1,0) over (partition by crd order by t1.crd,t1.yr_mo asc) as prv_mo_tmax,
t1.tmin,
t1.tmin - lag(t1.tmin,1,0) over (partition by crd order by t1.crd,t1.yr_mo asc) as prv_mo_tmin,
t1.tobs,
t1.lat,
t1.lon,
t1.crd,
t1.city_state
        from
(select
            yr_mo,
            avg(prcp) as prcp,
            avg(snow) as snow,
            avg(snwd) as snwd,
            avg(tmax) as tmax,
            avg(tmin) as tmin,
            avg(tobs) as tobs,
            lat,
            lon,
            crd,
            city_state from diap01.mec_us_united_20056.ual_weather_noaa_2

group by
yr_mo,
lat,
lon,
    crd,
city_state
        ) as t1
where t1.tmax + t1.tmin <> 0

) as t2
           );
commit;

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- drop table diap01.mec_us_united_20056.ual_weather_noaa_dy

create table diap01.mec_us_united_20056.ual_weather_noaa_dy
(
    yr_mo_dy     int          ,
    yr_mo     int          ,
    yr_wk     int          ,
    prcp         numeric(20,10),
    prv_dy_prcp numeric(20,10),
    snow         numeric(20,10),
    prv_dy_snow numeric(20,10),
    snwd         numeric(20,10),
    prv_dy_snwd numeric(20,10),
    tmax         numeric(20,10),
    prv_dy_tmax numeric(20,10),
    tmin         numeric(20,10),
    prv_dy_tmin numeric(20,10),
    tobs         numeric(20,10),
    lat          numeric(20,10),
    lon          numeric(20,10),
--     crd          numeric(20,10),
        crd     varchar(300),
    city_state   varchar(300)
);


insert into diap01.mec_us_united_20056.ual_weather_noaa_dy
(yr_mo_dy,yr_mo,yr_wk,prcp,prv_dy_prcp,SNOW,prv_dy_snow,SNWD,prv_dy_snwd,TMAX,prv_dy_tmax,TMIN,prv_dy_tmin,TOBS,lat,lon,crd,city_state)

    (
        select
t2.yr_mo_dy,
        t2.yr_mo,
    t2.yr_wk,
t2.prcp,
t2.prv_dy_prcp,
t2.snow,
t2.prv_dy_snow,
t2.snwd,
t2.prv_dy_snwd,
t2.tmax,
t2.prv_dy_tmax,
t2.tmin,
t2.prv_dy_tmin,
t2.tobs,
lat,
lon,
crd,
city_state

from (select
t1.yr_mo_dy,
        t1.yr_mo,
    t1.yr_wk,
t1.prcp,
t1.prcp - lag(t1.prcp,1,0) over (partition by crd order by t1.crd,t1.yr_mo_dy asc) as prv_dy_prcp,
t1.snow,
t1.snow - lag(t1.snow,1,0) over (partition by crd order by t1.crd,t1.yr_mo_dy asc) as prv_dy_snow,
t1.snwd,
t1.snwd - lag(t1.snwd,1,0) over (partition by crd order by t1.crd,t1.yr_mo_dy asc) as prv_dy_snwd,
t1.tmax,
t1.tmax - lag(t1.tmax,1,0) over (partition by crd order by t1.crd,t1.yr_mo_dy asc) as prv_dy_tmax,
t1.tmin,
t1.tmin - lag(t1.tmin,1,0) over (partition by crd order by t1.crd,t1.yr_mo_dy asc) as prv_dy_tmin,
t1.tobs,
lat,
lon,
crd,
city_state
        from
(select
            yr_mo_dy,
    yr_mo,
    yr_wk,
            avg(prcp) as prcp,
            avg(snow) as snow,
            avg(snwd) as snwd,
            avg(tmax) as tmax,
            avg(tmin) as tmin,
            avg(tobs) as tobs,
            lat,
            lon,
            crd,
            city_state from diap01.mec_us_united_20056.ual_weather_noaa_2
group by
yr_mo_dy,
        yr_mo,
    yr_wk,
    crd,
lat,
lon,
city_state
        ) as t1

where t1.tmax + t1.tmin <> 0
) as t2
           );
commit;

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================


-- drop table diap01.mec_us_united_20056.ual_weather_noaa_final

-- this table excludes all records without a temp min/max and a city_state entry
create table diap01.mec_us_united_20056.ual_weather_noaa_final
(
    yr_mo_dy     int          ,
    yr_mo       int          ,
    yr_wk       int          ,
    prcp         numeric(20,10),
    prv_dy_prcp numeric(20,10),
    prv_wk_prcp numeric(20,10),
    prv_mo_prcp numeric(20,10),
    snow         numeric(20,10),
    prv_dy_snow numeric(20,10),
    prv_wk_snow numeric(20,10),
    prv_mo_snow numeric(20,10),
    snwd         numeric(20,10),
    prv_dy_snwd numeric(20,10),
    prv_wk_snwd numeric(20,10),
    prv_mo_snwd numeric(20,10),
    tmax         numeric(20,10),
    prv_dy_tmax numeric(20,10),
    prv_wk_tmax numeric(20,10),
    prv_mo_tmax numeric(20,10),
    tmin         numeric(20,10),
    prv_dy_tmin numeric(20,10),
    prv_wk_tmin numeric(20,10),
    prv_mo_tmin numeric(20,10),
    tobs         numeric(20,10),
    lat          numeric(20,10),
    lon          numeric(20,10),
    crd          varchar(300),
    city_state   varchar(300)
);


insert into diap01.mec_us_united_20056.ual_weather_noaa_final
(yr_mo_dy,yr_mo,yr_wk,prcp,prv_dy_prcp,prv_wk_prcp,prv_mo_prcp,snow,prv_dy_snow,prv_wk_snow,prv_mo_snow,snwd,prv_dy_snwd,prv_wk_snwd,prv_mo_snwd,tmax,prv_dy_tmax,prv_wk_tmax,prv_mo_tmax,tmin,prv_dy_tmin,prv_wk_tmin,prv_mo_tmin,tobs,lat,lon,crd,city_state)



(
select
d1.yr_mo_dy,
d1.yr_mo,
d1.yr_wk,
d1.prcp,
d1.prv_dy_prcp,
w1.prv_wk_prcp,
m1.prv_mo_prcp,
d1.snow,
d1.prv_dy_snow,
w1.prv_wk_snow,
m1.prv_mo_snow,
d1.snwd,
d1.prv_dy_snwd,
w1.prv_wk_snwd,
m1.prv_mo_snwd,
d1.tmax,
d1.prv_dy_tmax,
w1.prv_wk_tmax,
m1.prv_mo_tmax,
d1.tmin,
d1.prv_dy_tmin,
w1.prv_wk_tmin,
m1.prv_mo_tmin,
d1.tobs,
d1.lat,
d1.lon,
d1.crd,
d1.city_state
from diap01.mec_us_united_20056.ual_weather_noaa_dy as d1

    left join diap01.mec_us_united_20056.ual_weather_noaa_wk as w1
    on d1.yr_wk = w1.yr_wk
    and d1.crd = w1.crd

    left join diap01.mec_us_united_20056.ual_weather_noaa_mo as m1
    on d1.yr_mo = m1.yr_mo
    and d1.crd = m1.crd

where d1.tmax + d1.tmin <> 0
    and (length(isnull(d1.city_state,'')) <> 0)

           );
commit;

