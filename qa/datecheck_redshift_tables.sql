select
	cast(md_event_time as date) as "date",
	count(distinct md_event_time) as md_event_time,
	count(distinct md_event_time_loc) as md_event_time_loc,
	count(distinct md_event_date) as md_event_date,
	count(distinct md_event_date_loc) as md_event_date_loc,
	count(*) as imps

from wmprodfeeds.united.dfa2_impression
	where cast(md_event_time as date) between '2018-01-01' and '2018-12-31'
group by cast(md_event_time as date)
-- ==========================================================================

select
	cast(md_event_time as date) as "date",
	count(distinct md_event_time) as md_event_time,
	count(distinct md_event_time_loc) as md_event_time_loc,
	count(distinct md_event_date) as md_event_date,
	count(distinct md_event_date_loc) as md_event_date_loc,
	count(*) as imps

from wmprodfeeds.united.dfa2_click
	where cast(md_event_time as date) between '2018-01-01' and '2018-12-31'
group by cast(md_event_time as date)
-- ==========================================================================

select
	cast(md_event_time as date) as "date",
	count(distinct md_event_time) as md_event_time,
	count(distinct md_event_time_loc) as md_event_time_loc,
	count(distinct md_event_date) as md_event_date,
	count(distinct md_event_date_loc) as md_event_date_loc,
	count(distinct md_interaction_time) as md_interaction_time,
	count(distinct md_interaction_time_loc) as md_interaction_time_loc,
	count(distinct md_interaction_date) as md_interaction_date,
	count(distinct md_interaction_date_loc) as md_interaction_date_loc,
	count(*) as imps

from wmprodfeeds.united.dfa2_activity
	where cast(md_event_time as date) between '2018-01-01' and '2018-12-31'
group by cast(md_event_time as date)
