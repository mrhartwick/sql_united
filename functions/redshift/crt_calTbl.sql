-- Create calendar table
create table wmprodfeeds.united.DIM_calendar (
    dateTime  timestamp,
    date      int,
    month     int,
    day       int,
    year      int,
    quarter   int,
    week      int,
    dayofyear int
);
--  ====================================================
-- Populate calendar table
insert into wmprodfeeds.united.DIM_calendar (dateTime,date,month,day,year,quarter,week,dayofyear)
    select
          a.date,
          wmprodfeeds.united.udf_dateToInt(cast(a.date as date)),
          cast(date_part(month,(cast(a.date as date))) as int),
          cast(date_part(day,(cast(a.date as date))) as int),
          cast(date_part(year,(cast(a.date as date))) as int),
          cast(date_part(QUARTER,(cast(a.date as date))) as int),
          cast(date_part(WEEK,(cast(a.date as date))) as int),
          cast(date_part(dy,(cast(a.date as date))) as int)
          from (select DATEADD(day,number,'1999-12-31') as date
          from wmprodfeeds.united.numbers
          order by number
          LIMIT  7305) a;
    commit;