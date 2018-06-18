

CREATE OR REPLACE FUNCTION udf_bigintToTS(ts BIGINT, units CHAR(2))
RETURNS timestamp
STABLE
AS $$
    import pandas
    return pandas.to_datetime(ts, unit=units.rstrip())
$$ LANGUAGE plpythonu;

-- Example usage
-- SELECT udf_bigintToTS(1518959341119551,'us'); microseconds
-- SELECT udf_bigintToTS(1518959341119551,'s'); seconds