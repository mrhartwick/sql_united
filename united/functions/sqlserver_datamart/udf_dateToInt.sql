alter function [dbo].udf_dateToInt (
    @initDate date
)
returns int
with execute as caller
as
begin
    declare @finalDate int
    set @finalDate = case
                     when len(cast(month(cast(@initDate as date)) as varchar(2))) = 1
                     then convert(int,cast(year(cast(@initDate as date)) as varchar(4)) + cast(0 as varchar(1)) + cast(month(cast(@initDate as date)) as varchar(2)) + right(cast(cast(@initDate as date) as varchar(10)),2)
                         )
                     else
                          convert(int,cast(year(cast(@initDate as date)) as varchar(4)) + cast(month(cast(@initDate as date)) as varchar(2)) + right(cast(cast(@initDate as date) as varchar(10)),2)
                         )
                     end;

    return @finalDate
end
go