

-- =========================================================================================================
-- Table with the store hours of all 50 IKEAs
set statement_timeout to 0;
drop table if exists wmprodfeeds.ikea.qualia_ikea_store_hrs;
create table wmprodfeeds.ikea.qualia_ikea_store_hrs (

	visit_key varchar(128) encode zstd distkey,
	open_hr    int encode zstd,
	clse_hr    int encode zstd
)
diststyle key
sortkey(visit_key);
-- =========================================================================================================
-- =========================================================================================================

-- location table with no after-hours visits
drop table if exists wmprodfeeds.ikea.qualia_location_clean_1;
create table wmprodfeeds.ikea.qualia_location_clean_1 (
	store_name     varchar(128) encode zstd,
	device_type    varchar(16) encode zstd,
	device_id      varchar(128) encode zstd distkey,
	timestamp_1    timestamp encode zstd,
	timestamp_2    timestamp encode zstd,
	visit_date_loc date encode zstd,
	visit_date_utc date encode zstd,
	visit_dow_loc  int encode zstd,
	visit_hour_loc int encode zstd,
	store_num      int encode zstd,
	visit_key      varchar(128) encode zstd
)

diststyle key
sortkey(device_id, timestamp_1, timestamp_2);

alter table wmprodfeeds.ikea.qualia_location_clean_1 owner to wmredshiftuser;

insert into wmprodfeeds.ikea.qualia_location_clean_1
(
	store_name, device_type, device_id, timestamp_1, timestamp_2, visit_date_loc, visit_date_utc, visit_dow_loc, visit_hour_loc, store_num, visit_key
 )

(select
            distinct t1.*

from
            wmprodfeeds.ikea.qualia_location t1
left join
            wmprodfeeds.ikea.qualia_ikea_store_hrs t2
            on t1.visit_key = t2.visit_key

where t1.visit_hour_loc >= t2.open_hr
    and t1.visit_hour_loc <= (t2.clse_hr + 1)
and t1.device_type in ('idfa', 'aaid')
)
--     t3



-- =========================================================================================================

-- Calculates total dwell time, by day, for each device ID in the location table

drop table if exists wmprodfeeds.ikea.qualia_location_dwell;
create table wmprodfeeds.ikea.qualia_location_dwell (
	store_name varchar(128) encode zstd,
	device_id  varchar(128) encode zstd distkey,
	visit_date_loc date encode zstd,
	visit_date_utc date encode zstd,
	first_vis  timestamp encode zstd,
	last_vis   timestamp encode zstd,
	dwell_min  decimal(20, 10) encode zstd,
	dwell_min_int int encode zstd,
	dwell_hr   decimal(20, 10) encode zstd
)

diststyle key
sortkey(device_id, visit_date_loc);

alter table wmprodfeeds.ikea.qualia_location_dwell owner to wmredshiftuser;

insert into wmprodfeeds.ikea.qualia_location_dwell
(
	store_name, device_id, visit_date_loc, visit_date_utc, first_vis, last_vis, dwell_min, dwell_min_int, dwell_hr
 )

	(select distinct store_name,
	                 device_id,
	                 visit_date_loc,
	                 visit_date_utc,
	                 first_vis,
	                 last_vis,
	                 dwell_min_int,
	                 dwell_min,
	                 dwell_hr
	 from (
		      select

store_name,
device_id,
visit_date_loc,
visit_date_utc,
first_vis,
last_vis,
datediff(minute, first_vis, last_vis) as dwell_min_int,
cast(datediff(second, first_vis, last_vis) as decimal(20, 10)) / cast(60 as decimal(20, 10)) as dwell_min,
(cast(datediff(second, first_vis, last_vis) as decimal(20, 10)) / cast(60 as decimal(20, 10))) / cast(60 as decimal(20, 10)) as dwell_hr

		      from (
			           select store_name,
                              device_id,
                              visit_date_loc,
			                  visit_date_utc,
                              timestamp_2,
                              first_value(timestamp_2)
                              over (partition by device_id, visit_date_loc order by timestamp_2 rows between unbounded preceding and unbounded following)     as first_vis,
--                          dense_rank() over (partition by store_name, device_id, visit_date order by timestamp_2 asc)                                              as first_rank,
                              last_value(timestamp_2)
                              over (partition by device_id, visit_date_loc order by timestamp_2 asc rows between unbounded preceding and unbounded following) as last_vis
--                          dense_rank() over (partition by store_name, device_id, visit_date order by timestamp_2 desc)                                             as last_rank
			           from wmprodfeeds.ikea.qualia_location_clean_1
		           ) t1
-- 		 exclude visits with no dwell time, as they'll just yield zeros for mins and hrs
	     where t1.first_vis <> t1.last_vis
	      ) t2
	);
-- =========================================================================================================

-- Same as dwell, but only includes visits of 3 hours or less

drop table if exists wmprodfeeds.ikea.qualia_location_dwell_devices_3hr;
create table wmprodfeeds.ikea.qualia_location_dwell_devices_3hr (
	device_id  varchar(128) encode zstd distkey
)

diststyle key
sortkey(device_id);

alter table wmprodfeeds.ikea.qualia_location_dwell_devices_3hr owner to wmredshiftuser;

insert into wmprodfeeds.ikea.qualia_location_dwell_devices_3hr
(
	device_id
 )

	(select
	    distinct
	                 device_id
	 from wmprodfeeds.ikea.qualia_location_dwell
where dwell_min >= 20
and dwell_hr <= 3
	    and visit_date_utc  between '2017-09-01' and '2019-03-31'
	);
-- =========================================================================================================
