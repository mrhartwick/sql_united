-- ========================================================================================
select *
-- select count(distinct impressiondate)
-- select count(t3.*)
from (
select
       t1.deviceid,
       t1.siz_uid,
       t1.impressiondate,
       t2.householdid,
       t2.consumerid,
       t2.platformid,
       t2.platformtype
from wmprodfeeds.ikea.qualia_impressions t1

left join wmprodfeeds.ikea.qualia_graph t2
on t1.deviceid = t2.platformid

where t1.isbot = 'false') t3
limit 5000

-- ========================================================================================
select count(distinct impressiondate)
-- select count(t3.*)
from (
select
       t1.deviceid,
       t1.siz_uid,
       t1.impressiondate,
       t2.householdid,
       t2.consumerid,
       t2.platformid,
       t2.platformtype
from wmprodfeeds.ikea.qualia_impressions t1

join wmprodfeeds.ikea.qualia_graph t2
on t1.deviceid = t2.platformid

where t1.isbot = 'false') t3
-- ========================================================================================
select count(distinct impressiondate)
-- select count(t3.*)
from (
select
       t1.deviceid,
       t1.siz_uid,
       t1.impressiondate,
       t2.householdid,
       t2.consumerid,
       t2.platformid,
       t2.platformtype
from wmprodfeeds.ikea.qualia_impressions t1

left join wmprodfeeds.ikea.qualia_graph t2
on t1.deviceid = t2.platformid

where t1.isbot = 'false') t3
-- limit 5000
-- ========================================================================================


