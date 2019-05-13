-- ===============================================================================================
-- ===============================================================================================
-- SUMMARY
-- ===============================================================================================
-- ===============================================================================================

-- EXPOSED

select
			count(distinct deviceid)
from
			wmprodfeeds.ikea.qualia_impressions t1
-- if you need to filter by placement properties
-- left join
-- 			wmprodfeeds.ikea.sizmek_placements p1
--     		on t1.siz_plc = p1.placementid
where
			(t1.siz_cid = 913795 or t1.siz_cid = 913509)
    		and impressiondate::date between '2018-10-08' and '2018-12-25'
    	--  if you need to filter by placement properties
			-- and (p1.placementname like '%qBANq%')
;

-- ===============================================================================================
-- EXPOSED VISITORS

select
			count(distinct device_id)
from
			wmprodfeeds.ikea.qualia_location
where
			device_id in (
						  select
						  			PlatformID
						  from
						  			wmprodfeeds.ikea.qualia_graph
						  where 	HouseholdId in (
						  							select
						  										HouseholdId
						  							from
						  										wmprodfeeds.ikea.qualia_impressions t1
						  							join
						  										wmprodfeeds.ikea.qualia_graph t2
						  										on deviceid = PlatformID
													where
																t2.PlatformType = 'BlueCava Id'
    															and (t1.siz_cid = 913795 or t1.siz_cid = 913509)
    															and impressiondate::date between '2017-10-16' and '2017-12-25'
    												)
						  			and timestamp_2::date between '2017-10-16' and '2017-12-25'
						);

-- ===============================================================================================
-- NON-EXPOSED VISITORS
select
			count(distinct device_id)
from
			wmprodfeeds.ikea.qualia_location
where 		device_id not in (
							  select
							  			PlatformID
							  from
							  			wmprodfeeds.ikea.qualia_graph
							  where HouseholdId in (
							  						select
							  									HouseholdId
							  						from
							  									wmprodfeeds.ikea.qualia_impressions t1
    												join
    															wmprodfeeds.ikea.qualia_graph t2
    															on deviceid = PlatformID
													where
																PlatformType = 'BlueCava Id'
												    			and (t1.siz_cid = 913795 or t1.siz_cid = 913509)
												    			and impressiondate::date between '2018-10-08' and '2018-12-25'
    						) and cast(timestamp_2 as date) between '2018-10-08' and '2018-12-25'
);

-- ===============================================================================================
-- ===============================================================================================
-- PARTNER DETAIL
-- ===============================================================================================
-- ===============================================================================================

-- EXPOSED

select
            s1.sitename,
			count(distinct deviceid)
from
			wmprodfeeds.ikea.qualia_impressions t1
-- if you need to filter by placement properties
-- left join
-- 			wmprodfeeds.ikea.sizmek_placements p1
--     		on t1.siz_plc = p1.placementid


left join
			wmprodfeeds.ikea.sizmek_sites s1
    		on t1.siz_sid = s1.siteid


where
			(t1.siz_cid = 913795 or t1.siz_cid = 913509)
    		and impressiondate::date between '2018-10-08' and '2018-12-25'
    	--  if you need to filter by placement properties
			-- and (p1.placementname like '%qBANq%')
group by
			s1.sitename
;

-- ===============================================================================================
-- EXPOSED VISITORS

select
			count(distinct device_id)
from
			wmprodfeeds.ikea.qualia_location
where
			device_id in (
						  select
						  			PlatformID
						  from
						  			wmprodfeeds.ikea.qualia_graph
						  where 	HouseholdId in (
						  							select
						  										HouseholdId
						  							from
						  										wmprodfeeds.ikea.qualia_impressions t1
						  							join
						  										wmprodfeeds.ikea.qualia_graph t2
						  										on deviceid = PlatformID
													where
																t2.PlatformType = 'BlueCava Id'
    															and (t1.siz_cid = 913795 or t1.siz_cid = 913509)
    															and impressiondate::date between '2017-10-16' and '2017-12-25'
    												)
						  			and timestamp_2::date between '2017-10-16' and '2017-12-25'
						);

-- ===============================================================================================
-- NON-EXPOSED VISITORS
select
			count(distinct device_id)
from
			wmprodfeeds.ikea.qualia_location
where 		device_id not in (
							  select
							  			PlatformID
							  from
							  			wmprodfeeds.ikea.qualia_graph
							  where HouseholdId in (
							  						select
							  									HouseholdId
							  						from
							  									wmprodfeeds.ikea.qualia_impressions t1
    												join
    															wmprodfeeds.ikea.qualia_graph t2
    															on deviceid = PlatformID
													where
																PlatformType = 'BlueCava Id'
												    			and (t1.siz_cid = 913795 or t1.siz_cid = 913509)
												    			and impressiondate::date between '2018-10-08' and '2018-12-25'
    						) and cast(timestamp_2 as date) between '2018-10-08' and '2018-12-25'
);




select
distinct
			s1.sitename,
			p1.siteid

from
            wmprodfeeds.ikea.sizmek_placements p1

left join
            wmprodfeeds.ikea.sizmek_sites s1
			on p1.siteid = s1.siteid
where
			p1.campaignid in (913509, 913795)





