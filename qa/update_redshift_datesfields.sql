--Impressions: 

set statement_timeout to 0;
UPDATE wmprodfeeds.united.dfa2_impression
SET
	md_event_time_loc = convert_timezone('UTC', 'America/New_York', md_event_time),
	md_event_date = md_event_time ::date,
	md_event_date_loc = convert_timezone('UTC', 'America/New_York', md_event_time)::date

where md_event_time::date between '2018-06-24' and '2018-06-27'

--=====================================================================================================
--Clicks: 

set statement_timeout to 0;
UPDATE wmprodfeeds.united.dfa2_click
SET
	md_event_time_loc = convert_timezone('UTC', 'America/New_York', md_event_time),
	md_event_date = md_event_time ::date,
	md_event_date_loc = convert_timezone('UTC', 'America/New_York', md_event_time)::date

where md_event_time::date between '2018-06-24' and '2018-06-27'


--=================================================================================================================
--Activity: 

set statement_timeout to 0;
UPDATE wmprodfeeds.united.dfa2_activity
SET
	md_event_time_loc = convert_timezone('UTC', 'America/New_York', md_event_time),
	md_event_date = md_event_time ::date,
	md_event_date_loc = convert_timezone('UTC', 'America/New_York', md_event_time)::date,
		md_interaction_time_loc = convert_timezone('UTC', 'America/New_York', md_interaction_time),
	md_interaction_date = md_interaction_time ::date,
	md_interaction_date_loc = convert_timezone('UTC', 'America/New_York', md_interaction_time)::date

where md_event_time::date between '2018-06-19' and '2018-06-28'
