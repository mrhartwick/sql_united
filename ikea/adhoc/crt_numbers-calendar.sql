create table wmprodfeeds.ikea.numbers (
    number int not null
);

insert into wmprodfeeds.ikea.numbers (number)
    select 1 as number
    union all
    select 2
    union all
    select 3
    union all
    select 4
    union all
    select 5
    union all
    select 6
    union all
    select 7
    union all
    select 8
    union all
    select 9
    union all
    select 10;

--  ====================================================
-- Populate numbers table
insert into wmprodfeeds.ikea.numbers (number)
    select ROW_NUMBER()
        OVER(order by n1.number)
    from wmprodfeeds.ikea.numbers n1
        cross join wmprodfeeds.ikea.numbers n2
        cross join wmprodfeeds.ikea.numbers n3
        cross join wmprodfeeds.ikea.numbers n4
OFFSET 10;

commit;

--  ======================================================================================================
-- Create calendar table
drop table if exists wmprodfeeds.ikea.DIM_calendar;
create table if not exists wmprodfeeds.ikea.DIM_calendar (
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
insert into wmprodfeeds.ikea.DIM_calendar (dateTime,date,month,day,year,quarter,week,dayofyear)
    select
          a.date,
          wmprodfeeds.public.udf_datetoint(cast(a.date as date)),
          date_part(mon, a.date),
          date_part(d, a.date),
          date_part(y, a.date),
          date_part(qtr, a.date),
          date_part(w, a.date),
          date_part(doy, a.date)
          from ( select dateadd(d,number,'1999-12-31') as date
          from wmprodfeeds.ikea.numbers
          order by number
          LIMIT  7305) a;
    commit;