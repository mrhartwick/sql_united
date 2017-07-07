-- SQL Server first

create table master.dbo.ual_weather
(
id varchar(1000) not null,
yr_mo_dy varchar(1000) not null,
PRCP  decimal(20,10),
SNOW  decimal(20,10),
SNWD  decimal(20,10),
TMAX  decimal(20,10),
TMIN  decimal(20,10),
TOBS  decimal(20,10)
);



-- then Vertica
create table diap01.mec_us_united_20056.ual_weather
(
    id       varchar(1000) not null,
    yr_mo_dy varchar(1000) not null,
    PRCP     decimal(20,10),
    SNOW     decimal(20,10),
    SNWD     decimal(20,10),
    TMAX     decimal(20,10),
    TMIN     decimal(20,10),
    TOBS     decimal(20,10)
);

-- and transfer
-- transfer takes longer than a straight import into Vertica, but
-- import kept failing; error code was esoteric and not easily found

  insert into VerticaUnited.diap01.mec_us_united_20056.ual_weather
    select *
        from master.dbo.ual_weather
go
