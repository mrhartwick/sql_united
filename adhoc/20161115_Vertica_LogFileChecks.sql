-- DFA Activities

select
        cast(a.activity_Time as date) as 'Date',
        count(*)              as 'Total Activities'
from mec.UnitedUS.dfa_activity as a
where
        cast(a.activity_Time as date) between '2016-07-01' and '2016-10-31'             -- Start Date and End Date
        and ADVERTISER_ID != 0                                                  -- omit records not attributed to media
        and activity_type = 'ticke498' and activity_sub_type = 'unite820'       -- ticket confirmations only
        and order_id = '9639387'                                                -- campaign filter
        and revenue != 0 and quantity != 0                                      -- omit homepage floodlight fires (errors)
group by cast(a.activity_Time as date)
order by "date"



-- DFA Activities/Tickets

select
        cast(a.activity_Time as date) as 'Date',
        count(*)              as 'Total Activities'
        sum(a.quantity)       as Tickets
from mec.UnitedUS.dfa_activity as a
where
        cast(a.activity_Time as date) between '2016-07-01' and '2016-10-31'             -- Start Date and End Date
        and ADVERTISER_ID != 0                                                  -- omit records not attributed to media
        and activity_type = 'ticke498' and activity_sub_type = 'unite820'       -- ticket confirmations only
        and order_id = '9639387'                                                -- campaign filter
        and revenue != 0 and quantity != 0                                      -- omit homepage floodlight fires (errors)
group by cast(a.activity_Time as date)
order by "date"



-- DFA Activities/Tickets by Site

select
        cast(a.activity_Time as date) as 'Date',
        s1.site as site,
        a.site_id as site_id,
        count(*)              as 'Total Activities',
        sum(a.quantity)       as Tickets
from mec.UnitedUS.dfa_activity as a

left join mec.UnitedUS.dfa_site as s1
on a.site_id = s1.site_id

where
        cast(a.activity_Time as date) between '2016-04-18' and '2016-10-21'             -- Start Date and End Date
        and ADVERTISER_ID != 0                                                  -- omit records not attributed to media
        and activity_type = 'ticke498' and activity_sub_type = 'unite820'       -- ticket confirmations only
        and order_id = '9639387'                                                -- campaign filter
        and revenue != 0 and quantity != 0                                      -- omit homepage floodlight fires (errors)
group by cast(a.activity_Time as date),
    s1.site,
    a.site_id

order by "date"

-- DFA Activities/Tickets by site, using media-matched time

select
        DATE(a.click_time) as 'Date',
        s1.site as site,
        a.site_id as site_id,
        count(*)              as 'Total Activities',
        sum(a.quantity)       as Tickets
from mec.UnitedUS.dfa_activity as a
        left join mec.UnitedUS.dfa_site as s1
        on a.site_id = s1.site_id
where
        DATE(a.click_time) between '2016-09-01' AND '2016-11-14'             -- Start Date and End Date
        and ADVERTISER_ID != 0                                                  -- omit records not attributed to media
        and activity_type = 'ticke498' and activity_sub_type = 'unite820'       -- ticket confirmations only
        and order_id = '9639387'                                                -- campaign filter
        and revenue != 0 and quantity != 0                                      -- omit homepage floodlight fires (errors)
group by DATE(a.click_time),
    s1.site,
    a.site_id
order by "date"





-- clicks

select
        DATE(c.click_time) as 'Date',
        count(*)              as 'Clicks'
from mec.UnitedUS.dfa_click as c
where
        DATE(c.click_time) between '2016-07-01' and '2016-10-31'             -- Start Date and End Date
        and ADVERTISER_ID != 0                                               -- omit records not attributed to media
        and order_id = '9639387'                                             -- campaign filter
group by DATE(c.click_time)
order by "date"



-- impressions
select
        DATE(i.impression_time)  as 'Date',
        count(*)                 as 'Impressions'
from mec.UnitedUS.dfa_impression as i
where
        DATE(i.impression_time) between '2016-07-01' and '2016-10-31'           -- Start Date and End Date
        and ADVERTISER_ID != 0                                                  -- omit records not attributed to media
        and order_id = '9639387'                                                -- campaign filter
group by DATE(i.impression_time)
order by "date"

-- ====================================================================================================================================================

-- DT 2.0 DFA Activities

select
        DATE(a.activity_Time) as 'Date',
        count(*)              as 'Total Activities'
from mec.UnitedUS.dfa_activity as a
where
        DATE(a.activity_Time) between '2016-07-01' and '2016-10-31'             -- Start Date and End Date
        and ADVERTISER_ID != 0                                                  -- omit records not attributed to media
        and activity_type = 'ticke498' and activity_sub_type = 'unite820'       -- ticket confirmations only
        and order_id = '9639387'                                                -- campaign filter
        and revenue != 0 and quantity != 0                                      -- omit homepage floodlight fires (errors)
group by DATE(a.activity_Time)
order by "date"



-- DT 2.0 DFA Activities/Tickets

select
        DATE(a.activity_Time) as 'Date',
        count(*)              as 'Total Activities'
        sum(a.quantity)       as Tickets
from mec.UnitedUS.dfa_activity as a
where
        DATE(a.activity_Time) between '2016-07-01' and '2016-10-31'             -- Start Date and End Date
        and ADVERTISER_ID != 0                                                  -- omit records not attributed to media
        and activity_type = 'ticke498' and activity_sub_type = 'unite820'       -- ticket confirmations only
        and order_id = '9639387'                                                -- campaign filter
        and revenue != 0 and quantity != 0                                      -- omit homepage floodlight fires (errors)
group by DATE(a.activity_Time)
order by "date"






-- DFA Activities/Tickets by Site

select
        DATE(a.activity_Time) as 'Date',
        s1.site as site,
        a.site_id as site_id,
        count(*)              as 'Total Activities',
        sum(a.quantity)       as Tickets
from mec.UnitedUS.dfa_activity as a

left join mec.UnitedUS.dfa_site as s1
on a.site_id = s1.site_id

where
        DATE(a.activity_Time) between '2016-04-18' and '2016-10-21'             -- Start Date and End Date
        and ADVERTISER_ID != 0                                                  -- omit records not attributed to media
        and activity_type = 'ticke498' and activity_sub_type = 'unite820'       -- ticket confirmations only
        and order_id = '9639387'                                                -- campaign filter
        and revenue != 0 and quantity != 0                                      -- omit homepage floodlight fires (errors)
group by DATE(a.activity_Time),
    s1.site,
    a.site_id

order by "date"

-- DFA Activities/Tickets by site, using media-matched time, and with currency-converted revenue


select
cast(to_timestamp(a1.interaction_time/1000000) as date) as "date"
-- ,campaign_id as campaign_id
-- ,other_data as other_data
,a1.site_id_dcm as site_id_dcm
,s1.site_dcm as site
-- ,a1.placement_id as placement_id
,count(*) as activities
,sum(a1.total_conversions) as tickets
,sum(a1.total_revenue*1000000) as total_revenue
,sum((a1.total_revenue*1000000)/ (r1.exchange_rate)) as cnv_total_revenue

from (
        select * from mec.doubleclickv2_unitedus.dfa_activity
        where cast(to_timestamp(interaction_time/1000000) as date) between '2016-09-01' and '2016-11-14'
        and activity_id = '978826'
        and campaign_id = '9639387'   -- TMK 2016
        and total_revenue != 0 and total_conversions != 0
        and advertiser_id != 0
        and upper(subString(other_data, (instr(other_data,'u3=')+3), 3)) != 'MIL'
        and subString(other_data, (instr(other_data,'u3=')+3), 5) != 'Miles'
    ) as a1

left join mec.doubleclickv2_unitedus.dfa_sites as s1
on a1.site_id_dcm = s1.site_id_dcm

left join mec.cross_client_resources.exchange_rates as r1
on upper(substring(other_data, (instr(other_data,'u3=')+3), 3)) = upper(r1.currency)
and cast(to_timestamp(interaction_time/1000000) as date) = r1.date

group by
cast(to_timestamp(a1.interaction_time/1000000) as date)
-- ,a1.campaign_id
-- ,a1.other_data
,a1.site_id_dcm
,s1.site_dcm



-- DT 2.0 Clicks

select
        cast(to_timestamp(t1.event_time/1000000) as date) as  "date",
        count (*) as "Clicks"
from mec.doubleclickv2_UnitedUS.dfa_click AS t1
where
        cast(to_timestamp(t1.event_time/1000000) as date) between '2016-09-01' AND '2016-11-14'
        and (advertiser_id != 0)
        and order_id = '9639387'
group by
        cast(to_timestamp(t1.event_time/1000000) as date)
order by
        cast(to_timestamp(t1.event_time/1000000) as date) asc



-- DT 2.0 Impressions
select
        cast(to_timestamp(t1.event_time / 1000000) as date) as "date",          -- to_timestamp converts the integer into a timestamp in the local timezone
        count(*) as "Impressions"
from mec.doubleclickv2_UnitedUS.dfa_impression as t1
where
        cast(to_timestamp(t1.event_time / 1000000) as date) between '2016-09-01' and '2016-11-14'
        and t1.campaign_id = '9639387' -- TMK 2016
        and (advertiser_id != 0)
group by cast(to_timestamp(t1.event_time/1000000) as date)
order by cast (to_timestamp(t1.event_time/1000000) as date) asc


