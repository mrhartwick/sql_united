
-- This is a summary table that grabs all of the device IDs from the Qualia Impressions file and brings in Sizmek ITS Actions (sizmek_conversion_events table)
-- It's referenced in the ITS Actions only table

set statement_timeout to 0;
drop table if exists wmprodfeeds.ikea.qualia_its_cvr_uid;
create table if not exists wmprodfeeds.ikea.qualia_its_cvr_uid (
	cvr_time date encode zstd,
	siz_uid  varchar(128) encode zstd distkey,
	deviceid varchar(128) encode zstd,
	its_con  int
)
	diststyle key
	sortkey (
		siz_uid,cvr_time
		)
;

insert into wmprodfeeds.ikea.qualia_its_cvr_uid
(cvr_time,
 siz_uid,
 deviceid,
 its_con)

	(
		select t3.cvr_time as cvr_time,
		       t2.siz_uid,
		       t2.deviceid,
		       t3.its_con
		-- summary Qualia impressions table, with just sizmek userid and Qualia device ID
		from wmprodfeeds.ikea.qualia_impressions_slim_2 t2
		-- sizmek summary ITS actions table
		left join wmprodfeeds.ikea.siz_its_cvr_uid t3
		on t2.siz_uid = t3.c_id
	);

-- =========================================================================================================
-- =========================================================================================================

-- create a table containing household IDs associated with ITS Actions (FYI: many Sizmek user IDs -> fewer Qualia Device IDs -> one Qualia household ID)

set statement_timeout to 0;
drop table if exists wmprodfeeds.ikea.qualia_visitors_hh_its_only;
create table wmprodfeeds.ikea.qualia_visitors_hh_its_only (
	householdid varchar(128) encode zstd distkey,
	its_con     int encode zstd
)
	diststyle key
	sortkey (householdid)
;

insert into wmprodfeeds.ikea.qualia_visitors_hh_its_only
(
 householdid,
 its_con
 )
	(
		select
		      		t2.householdid,
		      		sum(t1.its_con)
		from
					wmprodfeeds.ikea.qualia_its_cvr_uid t1
		left join
					wmprodfeeds.ikea.qualia_graph_last t2
			        on t1.deviceid = t2.platformid
		where
					t1.its_con > 0
		group by
		         	t2.householdid
	);

-- =========================================================================================================
-- =========================================================================================================

-- create a table containing household IDs associated Qualia visits

set statement_timeout to 0;
drop table if exists wmprodfeeds.ikea.qualia_visitors_hh_vis_only;
create table wmprodfeeds.ikea.qualia_visitors_hh_vis_only (
	householdid varchar(128) encode zstd distkey,
	visits      int  encode zstd
)
	diststyle key
	sortkey (
	    householdid,
	    visits
	    );

insert into wmprodfeeds.ikea.qualia_visitors_hh_vis_only
(
 householdid,
 visits
 )

	(
		select
		       		t2.householdid,
		       		count(distinct t1.vis_date) as visits
-- 		Summary location file with just device_id and visit date
		from
					wmprodfeeds.ikea.qualia_location_summ t1
	    left join
	    			wmprodfeeds.ikea.qualia_graph_last t2
        			on t1.device_id = t2.platformid

		group by
			        householdid
	);

-- =========================================================================================================
-- =========================================================================================================

-- check to see if there are any HH IDs in the ITS Actions table that are NOT in the visits table

select count(householdid) from wmprodfeeds.ikea.qualia_visitors_hh_its_only
    where householdid not in (select distinct householdid from wmprodfeeds.ikea.qualia_visitors_hh_vis_only)