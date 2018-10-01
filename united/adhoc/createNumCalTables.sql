
/*
A collection of four queries retrieved from the tutorial here (http://vertica.tips/2014/07/17/creating-a-numbers-and-calendar-table/)
that produce the desired result: a static calendar table for use in other queries.

I think the numbers table is used just as a counter. Maybe row_number() over() would suffice.
*/

-- Create numbers table
-- This can be dropped after the calendar table is created

create table diap01.mec_us_united_20056.numbers (
    number int not null
);

insert into diap01.mec_us_united_20056.numbers (number)
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

commit;
--  ====================================================
-- Populate numbers table
insert into diap01.mec_us_united_20056.numbers (number)
    select ROW_NUMBER()
        OVER(order by n1.number)
    from diap01.mec_us_united_20056.numbers n1
        cross join diap01.mec_us_united_20056.numbers n2
        cross join diap01.mec_us_united_20056.numbers n3
        cross join diap01.mec_us_united_20056.numbers n4
OFFSET 10;

commit;

--  ======================================================================================================
-- Create calendar table
create table diap01.mec_us_united_20056.DIM_calendar (
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
insert into diap01.mec_us_united_20056.DIM_calendar (dateTime,date,month,day,year,quarter,week,dayofyear)
    select
          a.date,
          diap01.mec_us_united_20056.udf_dateToInt(cast(a.date as date)),
          month (a.date),
          day (a.date),
          year (a.date),
          QUARTER(a.date),
          WEEK(a.date),
          DAYOFYEAR(a.date)
          from ( select TIMESTAMPADD(dd,number,'1999-12-31') as date
          from diap01.mec_us_united_20056.numbers
          order by number
          LIMIT  7305) a;
    commit;