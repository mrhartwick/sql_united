-- Pulling total Activities (transactions), tickets, and Impressions for Travel Spike and FlyerTalk (Internet Brands)

-- Activities
select
	DATE(a.activity_Time) as 'Date',
    a.site_id as site_id,
	count(*)              as 'Total Activities',
sum(a.quantity) as tickets
from mec.UnitedUS.dfa_activity as a
where
	DATE(a.activity_Time) between '2016-01-01' and '2016-09-30' -- Start Date and End Date
	and ADVERTISER_ID != 0
    and site_id in (1707137		-- Travel Spike
				  , 1853161		-- FlyerTalk
)
and revenue != 0
and quantity != 0
AND (Activity_Type = 'ticke498')
AND (Activity_Sub_Type = 'unite820')
and advertiser_id != 0
group by DATE(a.activity_Time), a.site_id
order by "date"


-- Impressions

select
	DATE(a.impression_time) as 'Date',
    a.site_id as site_id,
	count(*)              as 'Impressions'
from mec.UnitedUS.dfa_impression as a
where
	DATE(a.impression_time) between '2016-01-01' and '2016-09-30' -- Start Date and End Date
	and ADVERTISER_ID != 0
    and site_id in (1707137		-- Travel Spike
				  , 1853161		-- FlyerTalk
)
and advertiser_id != 0
group by DATE(a.impression_time), a.site_id
order by "date"
