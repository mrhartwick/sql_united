-- ==================================================================================================
-- ==================================================================================================
-- IMPRESSION
-- ==================================================================================================
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

-- ==================================================================================================
-- ==================================================================================================
-- GRAPH
-- ==================================================================================================
-- ==================================================================================================

set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_graph_2019q1
SET
householdid = replace(householdid, 'HH-', '')

-- Consumer ID
set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_graph_2019q1
SET
consumerid = replace(consumerid, 'CN-', '')

-- Screen ID
set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_graph_2019q1
SET
screenid = replace(screenid, 'SC-', '')


insert into wmprodfeeds.ikea.qualia_graph


(householdid,
metrocode,
city,
state,
latitude,
longitude,
householdlastseen,
consumerid,
consumerlastseen,
screenid,
screentype,
screenplatform,
screenlastseen,
browserapp,
version,
browserapptohouseholdscore,
platformid,
platformtype)

(select * from wmprodfeeds.ikea.qualia_graph_2019q1);

-- ==================================================================================================
-- ==================================================================================================
-- LOCATION
-- ==================================================================================================
-- ==================================================================================================

-- ==================================================================================================
-- Location Visit Date Local
set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_location
SET
visit_date_loc = trunc(timestamp_1)
where (len(isnull (store_name, '')) > 0);
-- ==================================================================================================
-- Location Visit Date UTC
set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_location
SET
visit_date_utc = trunc(timestamp_2)
where (len(isnull (store_name, '')) > 0);
-- ==================================================================================================
-- Location Store Number
set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_location
SET
store_num = cast(regexp_substr(store_name,'\\d+',0) as int)
where (len(isnull (store_name, '')) > 0);
-- ==================================================================================================
-- Location Visit DOW
set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_location
SET
visit_dow_loc = date_part(dow,timestamp_1)
where (len(isnull (store_name, '')) > 0);
-- ==================================================================================================
-- Location Visit Hour
set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_location
SET
visit_hour_loc = date_part(hr,timestamp_1)
where (len(isnull (store_name, '')) > 0);
-- ==================================================================================================
-- Location Visit Key
set statement_timeout to 0;
UPDATE wmprodfeeds.ikea.qualia_location
SET
visit_key = store_num::varchar(128) || '-' || visit_dow_loc::varchar(128)
where (len(isnull (store_name, '')) > 0);

