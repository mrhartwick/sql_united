-- ==================================================================================================
-- User ID
set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_impressions_stg
SET
siz_uid = cast(substring(requestcustomparameters,(regexp_instr(requestcustomparameters,'uid\\=') + 4),36) as varchar(128))
where requestcustomparameters like '%&uid=%';

-- ==================================================================================================
-- Campaign ID
set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_impressions_stg
SET
siz_cid = cast(substring(requestcustomparameters,(regexp_instr(requestcustomparameters,'cid\\=') + 4),6) as int)
where requestcustomparameters like '%&cid=%'
and udf_isnumeric(substring(requestcustomparameters,(regexp_instr(requestcustomparameters,'cid\\=') + 4),6)) = true;
--
-- select * from wmprodfeeds.ikea.qualia_impressions
--     where siz_cid > 0

-- ==================================================================================================
-- Placement ID
set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_impressions_stg
SET
siz_plc = cast(substring(requestcustomparameters,(regexp_instr(requestcustomparameters,'plc\\=') + 4),6) as int)
where requestcustomparameters like '%&plc=%'
and udf_isnumeric(substring(requestcustomparameters,(regexp_instr(requestcustomparameters,'plc\\=') + 4),6)) = true;

-- ==================================================================================================
-- Site ID
set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_impressions_stg
SET
siz_sid = cast(substring(requestcustomparameters,(regexp_instr(requestcustomparameters,'sid\\=') + 4),5) as int)
where requestcustomparameters like '%&sid=%'
and udf_isnumeric(substring(requestcustomparameters,(regexp_instr(requestcustomparameters,'sid\\=') + 4),5)) = true;


insert into wmprodfeeds.ikea.qualia_impressions
(impressiondate,
requestid,
deviceid,
iscookieresistant,
isweak,
devicefamily,
deviceplatform,
browser,
browserversion,
operatingsystem,
osversion,
isbot,
metrocode,
requestcustomparameters,
siz_uid,
siz_cid,
its_con,
siz_plc,
siz_sid)

(select * from wmprodfeeds.ikea.qualia_impressions_stg);