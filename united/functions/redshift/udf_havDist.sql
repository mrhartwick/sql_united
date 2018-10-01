create or replace function udf_havDist(
        decimal(20,10), -- @lat1; $1
        decimal(20,10), -- @lat2; $2
        decimal(20,10), -- @lon1; $3
        decimal(20,10)  -- @lon2; $4
) -- Calculates distance between two sets of coordinates, compensating for curvature of the earth; the Haversine formula.
    returns float8 -- Redshift returns an error when I set the return type to decimal (it insists that it's "double precision." I'm not crazy about going from several significant digits to 2, but it shouldn't make a practical difference in this use case)
    stable
as $$
(select
        (acos(cos(radians($1)) * -- lat1
                  cos(radians($2)) * -- lat2
                  cos(radians($3) - -- long1
                          radians($4)) + --long2
                  sin(radians($1)) * -- lat 1
                      sin(radians($2))) -- lat2
            * cast(6371000 as decimal(20,10))) / cast(1000 as decimal(20,10)) -- convert to kilometers;
);
$$ language sql;

