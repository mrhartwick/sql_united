create function diap01.mec_us_united_20056.udf_dateToInt2 (
    initDate date
)
return int
as
begin
    return
(case
         when length(cast(month(cast(initDate as date)) as varchar(2))) = 1
         then cast(cast(year(cast(initDate as date)) as varchar(4)) || cast(0 as varchar(1)) || cast(month(cast(initDate as date)) as varchar(2)) || right(cast(cast(initDate as date) as varchar(10)),2) as int)
         else
              cast(cast(year(cast(initDate as date)) as varchar(4)) || cast(month(cast(initDate as date)) as varchar(2)) || right(cast(cast(initDate as date) as varchar(10)),2) as int)
    end);
end;