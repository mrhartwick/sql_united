-- EXPOSED VISITORS

-- All
Select count(distinct device_id) from wmprodfeeds.ikea.qualia_location where device_id in (
Select PlatformID from wmprodfeeds.ikea.qualia_graph where HouseholdId in (
    Select HouseholdId
    from wmprodfeeds.ikea.qualia_impressions t1
    join wmprodfeeds.ikea.qualia_graph t2 on deviceid = PlatformID
	where t2.PlatformType = 'BlueCava Id'
--     and (t1.siz_cid = 913795 or t1.siz_cid = 913509)
    and t1.siz_cid = 813500
--     and impressiondate::date between '2018-10-08' and '2018-12-25'
--     ) and cast(timestamp_2 as date) between '2018-10-08' and '2018-12-25'
    and impressiondate::date between '2017-10-16' and '2017-12-25'
    ) and cast(timestamp_2 as date) between '2017-10-16' and '2017-12-25'
-- and device_type in ('idfa', 'aaid')

);



-- Video
Select count(distinct device_id) from wmprodfeeds.ikea.qualia_location where device_id in (
Select PlatformID from wmprodfeeds.ikea.qualia_graph where HouseholdId in (
    Select HouseholdId
    from wmprodfeeds.ikea.qualia_impressions t1
    join wmprodfeeds.ikea.qualia_graphqualia_graph t2 on deviceid = PlatformID
	where PlatformType = 'BlueCava Id'
    and (t1.siz_cid = 913795 or t1.siz_cid = 913509)
    and impressiondate::date between '2018-10-08' and '2018-12-25'
    ) and cast(timestamp_2 as date) between '2018-10-08' and '2018-12-25'
-- and device_type in ('idfa', 'aaid')

);


-- Video
Select count(distinct device_id) from wmprodfeeds.ikea.qualia_location where device_id in (
Select PlatformID from wmprodfeeds.ikea.qualia_graph where HouseholdId in (
    Select HouseholdId
    from wmprodfeeds.ikea.qualia_impressions t1
    left join wmprodfeeds.ikea.qualia_graph t2 on deviceid = PlatformID
    left join wmprodfeeds.ikea.sizmek_placements p1
    on t1.siz_plc = p1.placementid
	where PlatformType = 'BlueCava Id'
    and (t1.siz_cid = 913795 or t1.siz_cid = 913509)
    and impressiondate::date between '2018-10-08' and '2018-12-25'
--     and (p1.placementname like '%qVIDq%' or p1.placementname like '%qPREq%')
    and (p1.placementname like '%qBANq%')
    ) and cast(timestamp_2 as date) between '2018-10-08' and '2018-12-25'
-- and device_type in ('idfa', 'aaid')

);



-- Non-Video
Select count(distinct device_id) from wmprodfeeds.ikea.qualia_location where device_id in (
Select PlatformID from wmprodfeeds.ikea.qualia_graph where HouseholdId in (
    Select HouseholdId
    from wmprodfeeds.ikea.qualia_impressions t1
    join wmprodfeeds.ikea.qualia_graph t2
        on deviceid = PlatformID
	where PlatformType = 'BlueCava Id'
    and (t1.siz_cid = 913795 or t1.siz_cid = 913509)
          and (siz_plc in (25702186,25702187,25702188,25702189,25702190,25702192,25702205,25702206,25702207,25702208,25702209,25702210,25702211,25702212,25702213,25702214,25702215,25935198,25935201,25935207,25935208,25935209,25935210,25935211,25935212,25935213,25935215,25935218,25935219,25935221,25979760,26094951))
    and impressiondate::date between '2018-10-08' and '2018-12-25'
    ) and cast(timestamp_2 as date) between '2018-10-08' and '2018-12-25'
and device_type in ('idfa', 'aaid')

);
-- ===============================================================================================
-- EXPOSED

-- All
Select count(distinct deviceid) from wmprodfeeds.ikea.qualia_impressions t1
    left join wmprodfeeds.ikea.sizmek_placements p1
    on t1.siz_plc = p1.placementid
-- 	where PlatformType = 'BlueCava Id'
    where (t1.siz_cid = 913795 or t1.siz_cid = 913509)
--           where  t1.siz_cid = 813500
    and impressiondate::date between '2018-10-08' and '2018-12-25'
-- and impressiondate::date between '2017-10-16' and '2017-12-25'
-- and (p1.placementname like '%qVIDq%' or p1.placementname like '%qPREq%')
and (p1.placementname like '%qBANq%')
;

-- Video
Select count(distinct deviceid) from wmprodfeeds.ikea.qualia_impressions t1
-- 	where PlatformType = 'BlueCava Id'
	where (t1.siz_cid = 913795 or t1.siz_cid = 913509)
        and (t1.siz_plc = 25935222)
    and impressiondate::date between '2018-10-08' and '2018-12-25'
;

-- Non-Video
Select count(distinct deviceid) from wmprodfeeds.ikea.qualia_impressions t1
-- 	where PlatformType = 'BlueCava Id'
    where (t1.siz_cid = 913795 or t1.siz_cid = 913509)
          and (siz_plc in (25702186,25702187,25702188,25702189,25702190,25702192,25702205,25702206,25702207,25702208,25702209,25702210,25702211,25702212,25702213,25702214,25702215,25935198,25935201,25935207,25935208,25935209,25935210,25935211,25935212,25935213,25935215,25935218,25935219,25935221,25979760,26094951))
    and impressiondate::date between '2018-10-08' and '2018-12-25'
;

Select count(distinct deviceid) from wmprodfeeds.ikea.qualia_impressions t1
-- 	where PlatformType = 'BlueCava Id'
    where (t1.siz_cid = 913795 or t1.siz_cid = 913509)
          and (siz_plc <> 25935222)
    and impressiondate::date between '2018-10-08' and '2018-12-25'


select distinct siz_plc from  wmprodfeeds.ikea.qualia_impressions
where siz_cid = 913795



-- All Other
Select count(distinct deviceid) from wmprodfeeds.ikea.qualia_impressions t1
-- 	where PlatformType = 'BlueCava Id'
    where (t1.siz_cid = 913795 or t1.siz_cid = 913509)
          and siz_plc != 25935222
    and impressiondate::date between '2018-10-08' and '2018-12-25'

-- Select PlatformID from wmprodfeeds.ikea.qualia_graph where HouseholdId in (
--     Select HouseholdId
--     from wmprodfeeds.ikea.qualia_impressions t1
--     join wmprodfeeds.ikea.qualia_graph t2 on deviceid = PlatformID
-- 	where PlatformType = 'BlueCava Id'
--     and (t1.siz_cid = 913795 or t1.siz_cid = 913509)
--     and impressiondate::date between '2017-08-01' and '2018-11-30'
--     ) and cast(timestamp_2 as date) between '2017-08-01' and '2018-11-30'
-- and device_type in ('idfa', 'aaid')

-- ===============================================================================================
-- Non-Exposed Visitors
select count(distinct device_id) from wmprodfeeds.ikea.qualia_location where device_id not in (

Select PlatformID from wmprodfeeds.ikea.qualia_graph where HouseholdId in (
    Select HouseholdId
    from wmprodfeeds.ikea.qualia_impressions t1
    join wmprodfeeds.ikea.qualia_graph t2 on deviceid = PlatformID
	where PlatformType = 'BlueCava Id'
    and (t1.siz_cid = 913795 or t1.siz_cid = 913509)
    and impressiondate::date between '2018-10-08' and '2018-12-25'
    ) and cast(timestamp_2 as date) between '2018-10-08' and '2018-12-25'
-- and device_type in ('idfa', 'aaid')

);

-- Visitors
Select count(distinct device_id) from wmprodfeeds.ikea.qualia_location
-- where cast(timestamp_2 as date) between '2018-10-08' and '2018-12-25'
where cast(timestamp_2 as date) between '2017-10-16' and '2017-12-25'