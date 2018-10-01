-- Convert a date object into a date expressed as an integer in "yyyyMMdd" format. Enables simple, accurate comparison/addition/subtraction'
-- Redshift doesn't allow named variables, so we have to use generics, e.g. "$1," "$2." Variables are numbers by the order they appear inside the call parenthesis.

create function wmprodfeeds.united.udf_dateToInt (
    date
)
returns int
stable
as $$
(select case
         when length(cast(date_part(month,(cast($1 as date))) as varchar(2))) = 1
         then cast(cast(date_part(year,(cast($1 as date))) as varchar(4)) || cast(0 as varchar(1)) || cast(date_part(month,(cast($1 as date))) as varchar(2)) || right(cast(cast($1 as date) as varchar(10)),2) as int)
         else
              cast(cast(date_part(year,(cast($1 as date))) as varchar(4)) || cast(date_part(month,(cast($1 as date))) as varchar(2)) || right(cast(cast($1 as date) as varchar(10)),2) as int)
    end);

$$ language sql;


