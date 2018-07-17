create function udf_base64Decode(str varchar) returns varchar
	stable
	language plpythonu
as $$
    import base64
    return base64.b64decode(str)
$$;



