create table wmprodfeeds.united.numbers (
    number int not null
);

insert into wmprodfeeds.united.numbers (number)
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


insert into wmprodfeeds.united.numbers (number)
    select ROW_NUMBER()
        OVER(order by n1.number)
    from wmprodfeeds.united.numbers n1
        cross join wmprodfeeds.united.numbers n2
        cross join wmprodfeeds.united.numbers n3
        cross join wmprodfeeds.united.numbers n4
OFFSET 10;

commit;