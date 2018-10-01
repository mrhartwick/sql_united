-- Haversine distance formula (great circle calc)
-- distance between two coordinates, accounting for curvature of the earth

create function [dbo].udf_havDist(
    @lat1 decimal(20,10),@lat2 decimal(20,10),@lon1 decimal(20,10),@lon2 decimal(20,10)
)
    returns decimal(20,10)
    with execute as caller
as
    begin
        declare @dist decimal(20,10)
        set @dist =
        (acos(cos(radians(@lat1)) * -- lat1
                  cos(radians(@lat2)) * -- lat2
                  cos(radians(@lon1) - -- long1
                          radians(@lon2)) + --long2
                  sin(radians(@lat1)) * -- lat 1
                      sin(radians(@lat2))) -- lat2
            * 6371000) / 1000 -- convert to kilometers;
        return @dist
    end
go
