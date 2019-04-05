create table if not exists united.dfa2_activity_test
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_activity_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer
		constraint dfa2_activity_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer
		constraint dfa2_activity_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer
		constraint dfa2_activity_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer,
	site_id_dcm integer
		constraint dfa2_activity_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer
		constraint dfa2_activity_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_activity_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_activity_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	u_value varchar(512) encode zstd,
	activity_id integer not null encode zstd,
	tran_value varchar(10) encode zstd,
	other_data varchar(12000) encode zstd,
	ord_value varchar(12000) encode zstd,
	interaction_time bigint,
	conversion_id bigint encode zstd,
	segment_value_1 bigint,
	floodlight_configuration integer encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint,
	dbm_advertiser_id integer,
	dbm_insertion_order_id integer,
	dbm_line_item_id integer,
	dbm_creative_id integer,
	dbm_bid_price_usd integer,
	dbm_bid_price_partner_currency integer,
	dbm_bid_price_advertiser_currency integer,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer,
	dbm_country_code varchar(2) encode zstd,
	dbm_designated_market_area_dma_id integer,
	dbm_zip_postal_code integer,
	dbm_state_region_id integer,
	dbm_city_id integer,
	dbm_operating_system_id integer,
	dbm_browser_platform_id integer,
	dbm_browser_timezone_offset_minutes integer,
	dbm_net_speed integer,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer,
	dbm_device_type integer,
	dbm_mobile_make_id integer,
	dbm_mobile_model_id integer,
	total_conversions integer encode zstd,
	total_revenue numeric(37,15) encode zstd,
	dbm_media_cost_usd integer encode zstd,
	dbm_media_cost_partner_currency integer encode zstd,
	dbm_media_cost_advertiser_currency integer encode zstd,
	dbm_revenue_usd integer encode zstd,
	dbm_revenue_partner_currency integer encode zstd,
	dbm_revenue_advertiser_currency integer encode zstd,
	dbm_total_media_cost_usd integer encode zstd,
	dbm_total_media_cost_partner_currency integer encode zstd,
	dbm_total_media_cost_advertiser_currency integer encode zstd,
	dbm_cpm_fee_1_usd integer encode zstd,
	dbm_cpm_fee_1_partner_currency integer encode zstd,
	dbm_cpm_fee_1_advertiser_currency integer encode zstd,
	dbm_cpm_fee_2_usd integer encode zstd,
	dbm_cpm_fee_2_partner_currency integer encode zstd,
	dbm_cpm_fee_2_advertiser_currency integer encode zstd,
	dbm_cpm_fee_3_usd integer encode zstd,
	dbm_cpm_fee_3_partner_currency integer encode zstd,
	dbm_cpm_fee_3_advertiser_currency integer encode zstd,
	dbm_cpm_fee_4_usd integer encode zstd,
	dbm_cpm_fee_4_partner_currency integer encode zstd,
	dbm_cpm_fee_4_advertiser_currency integer encode zstd,
	dbm_cpm_fee_5_usd integer encode zstd,
	dbm_cpm_fee_5_partner_currency integer encode zstd,
	dbm_cpm_fee_5_advertiser_currency integer encode zstd,
	dbm_media_fee_1_usd integer encode zstd,
	dbm_media_fee_1_partner_currency integer encode zstd,
	dbm_media_fee_1_advertiser_currency integer encode zstd,
	dbm_media_fee_2_usd integer encode zstd,
	dbm_media_fee_2_partner_currency integer encode zstd,
	dbm_media_fee_2_advertiser_currency integer encode zstd,
	dbm_media_fee_3_usd integer encode zstd,
	dbm_media_fee_3_partner_currency integer encode zstd,
	dbm_media_fee_3_advertiser_currency integer encode zstd,
	dbm_media_fee_4_usd integer encode zstd,
	dbm_media_fee_4_partner_currency integer encode zstd,
	dbm_media_fee_4_advertiser_currency integer encode zstd,
	dbm_media_fee_5_usd integer encode zstd,
	dbm_media_fee_5_partner_currency integer encode zstd,
	dbm_media_fee_5_advertiser_currency integer encode zstd,
	dbm_data_fee_usd integer encode zstd,
	dbm_data_fee_partner_currency integer encode zstd,
	dbm_data_fee_advertiser_currency integer encode zstd,
	dbm_billable_cost_usd integer encode zstd,
	dbm_billable_cost_partner_currency integer encode zstd,
	dbm_billable_cost_advertiser_currency integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp encode zstd,
	md_dbm_request_time timestamp encode zstd,
	md_file_date integer encode zstd,
	acquiredtime timestamp encode zstd,
	md_interaction_time timestamp encode zstd,
    md_interaction_time_loc timestamp encode zstd,
    md_interaction_date date encode zstd,
    md_interaction_date_loc date encode zstd,
    md_event_time_loc timestamp encode zstd,
	md_event_date date encode zstd,
    md_event_date_loc date encode zstd
)
diststyle key
sortkey(interaction_time, user_id, other_data, md_interaction_time)
;


insert into wmprodfeeds.united.dfa2_activity_test (select *,convert_timezone('UTC','America/New_York', md_interaction_time) as md_interaction_time_loc, cast(md_interaction_time as date) as md_interaction_date, cast(convert_timezone('UTC','America/New_York', md_interaction_time) as date) as md_interaction_date_loc from wmprodfeeds.united.dfa2_activity where cast(md_interaction_time as date)= '2018-05-01' );


alter table united.dfa2_activity_test
add column md_interaction_time_loc timestamp
 convert_timezone('UTC','America/New_York', md_interaction_time);


select md_event_time::date from united.dfa2_activity_test
group by md_event_time::date

select total_revenue as rev from united.dfa2_activity_test
where md_event_time::date = '2018-06-02'
and activity_id = 978826
limit 100


select * from united.dfa2_activity_test
where activity_id = 978826
	and conversion_id != 0
limit 500



select md_interaction_date_loc from united.dfa2_activity_test
group by md_interaction_date_loc
order by md_interaction_date_loc
-- =======================================================================

insert into wmprodfeeds.united.dfa2_activity_test_2 (select * from wmprodfeeds.united.dfa2_activity where cast(md_interaction_time as date) = '2018-06-01' );





set statement_timeout to 0;
UPDATE wmprodfeeds.united.dfa2_activity
SET
	md_interaction_time = cast(timestamptz 'epoch' + (interaction_time/ 1000000) * interval '1 second' as timestamp),
	md_interaction_time_loc = convert_timezone('UTC', 'America/New_York', cast(timestamptz 'epoch' + (interaction_time/ 1000000) * interval '1 second' as timestamp)),
	md_interaction_date = cast(timestamptz 'epoch' + (interaction_time/ 1000000) * interval '1 second' as timestamp)::date,
	md_interaction_date_loc = (convert_timezone('UTC', 'America/New_York', cast(timestamptz 'epoch' + (interaction_time/ 1000000) * interval '1 second' as timestamp)))::date
where md_event_time::date = '2017-12-31'





select * from wmprodfeeds.united.dfa2_activity_test_2 limit 100


select * from wmprodfeeds.united.dfa2_activity where cast(md_event_time as date) = '2018-04-01' and activity_id = 978826
	and conversion_id != 0 limit 100


-- Count md_interaction_time instances per event_time day
select cast(md_event_time as date) as event_date, count(md_event_time) as this, count(md_event_time_loc) as inter_date , count(md_event_date_loc) as that from wmprodfeeds.united.dfa2_click
group by cast(md_event_time as date)
order by cast(md_event_time as date) desc





select cast(md_event_time as date) as event_date, count(event_time) as that, count(distinct interaction_time) as this,
-- 	count(interaction_time) as this,
	count(md_interaction_time_loc) as inter_date, count(md_interaction_date) as "date", count(md_interaction_time_loc) as that from wmprodfeeds.united.dfa2_activity
	where activity_id = 978826
group by cast(md_event_time as date)
order by cast(md_event_time as date) desc



-- ==========================================================================
set statement_timeout to 0;
UPDATE wmprodfeeds.united.dfa2_impression
SET
	md_event_time = cast(timestamp 'epoch' + (event_time/ 1000000) * interval '1 second' as timestamp)
	,
	md_event_time_loc = convert_timezone('UTC', 'America/New_York', cast(timestamptz 'epoch' + (event_time/ 1000000) * interval '1 second' as timestamp)),
	md_event_date = cast(timestamptz 'epoch' + (event_time/ 1000000) * interval '1 second' as timestamp)::date,
	md_event_date_loc = (convert_timezone('UTC', 'America/New_York', cast(timestamptz 'epoch' + (event_time/ 1000000) * interval '1 second' as timestamp)))::date
where md_event_time::date between '2017-05-01' and '2018-05-31'


select to_timestamp('20180619-15:15:42' , 'YYYYMMDD-HH:MI:SS')


-- ==========================================================================
set statement_timeout to 0;
UPDATE wmprodfeeds.united.dfa2_impression
SET
-- 	md_interaction_time = cast(timestamp 'epoch' + (interaction_time/ 1000000) * interval '1 second' as timestamp)
-- -- 	,
	md_event_time_loc = convert_timezone('UTC', 'America/New_York', md_event_time),
	md_event_date = md_event_time ::date,
	md_event_date_loc = convert_timezone('UTC', 'America/New_York', md_event_time)::date

where md_event_time::date between '2018-06-24' and '2018-06-27'

-- ================================================================================================================
-- ================================================================================================================
-- ==========================================================================

set statement_timeout to 0;
UPDATE wmprodfeeds.united.dfa2_activity
SET
	md_interaction_time = cast(timestamp 'epoch' + (interaction_time/ 1000000) * interval '1 second' as timestamp)
	,
	md_event_time_loc = convert_timezone('UTC', 'America/New_York', md_event_time),
	md_event_date = md_event_time ::date,
	md_event_date_loc = convert_timezone('UTC', 'America/New_York', md_event_time)::date,
		md_interaction_time_loc = convert_timezone('UTC', 'America/New_York', md_interaction_time),
	md_interaction_date = md_interaction_time ::date,
	md_interaction_date_loc = convert_timezone('UTC', 'America/New_York', md_interaction_time)::date

where md_event_time::date
-- 	      = '2018-10-17'
between '2018-10-21' and '2018-10-28'
;
-- ================================================================================================================
-- ==========================================================================
select
md_file_date,
md_event_date
from wmprodfeeds.united.dfa2_activity
where md_event_time::date

between '2018-09-01' and '2018-10-07'
	group by
md_file_date,
md_event_date
order by
md_file_date desc
;

-- ================================================================================================================
-- ==========================================================================
delete from united.dfa2_impression
where md_file_date = 20180928;

delete from united.dfa2_activity
where md_file_date = 20180928;

delete from united.dfa2_click
where md_file_date = 20180928;


-- ================================================================================================================
-- ==========================================================================

set statement_timeout to 0;
UPDATE wmprodfeeds.united.dfa2_activity
SET
	md_interaction_time = cast(timestamp 'epoch' + (interaction_time/ 1000000) * interval '1 second' as timestamp),
	md_interaction_time_loc = convert_timezone('UTC', 'America/New_York', cast(timestamp 'epoch' + (interaction_time/ 1000000) * interval '1 second' as timestamp)),
	md_interaction_date = cast(timestamp 'epoch' + (interaction_time/ 1000000) * interval '1 second' as timestamp)::date,
	md_interaction_date_loc = convert_timezone('UTC', 'America/New_York', cast(timestamp 'epoch' + (interaction_time/ 1000000) * interval '1 second' as timestamp))::date,

	md_event_time = cast(timestamp 'epoch' + (event_time/ 1000000) * interval '1 second' as timestamp),
	md_event_time_loc = convert_timezone('UTC', 'America/New_York', cast(timestamp 'epoch' + (event_time/ 1000000) * interval '1 second' as timestamp)),
	md_event_date = cast(timestamp 'epoch' + (event_time/ 1000000) * interval '1 second' as timestamp)::date,
	md_event_date_loc = convert_timezone('UTC', 'America/New_York', cast(timestamp 'epoch' + (event_time/ 1000000) * interval '1 second' as timestamp))::date


where md_event_time::date
-- 	      = '2018-10-09'
between '2018-10-29' and '2018-11-05'





select cast(md_event_time as date) as "date",
	   count(distinct event_time) as event_time,
	   count(distinct md_event_time) as md_event_time,
	   count(distinct md_event_time_loc) as md_event_time_loc,
	   count(distinct md_event_date) as md_event_date,
	   count(distinct md_event_date_loc) as md_event_date_loc,
	   count(distinct interaction_time) as interaction_time,
	   count(distinct md_interaction_time) as md_interaction_time,
	   count(distinct md_interaction_time_loc) as md_interaction_time_loc,
	   count(distinct md_interaction_date) as md_interaction_date,
	   count(distinct md_interaction_date_loc) as md_interaction_date_loc,
	   count(*) as conv

from wmprodfeeds.united.dfa2_activity
where cast(md_event_time as date) between '2018-10-04' and '2018-11-13'
-- where cast(md_event_time as date) = '2018-09-29'
group by cast(md_event_time as date)
order by cast(md_event_time as date) asc




create table wmprodfeeds.united.dfa2_activity_2 as select distinct * from wmprodfeeds.united.dfa2_activity
where cast(md_event_time as date) = '2018-09-24';

insert into wmprodfeeds.united.dfa2_activity
(select * from wmprodfeeds.united.dfa2_activity_2);

select distinct acquiredtime from wmprodfeeds.united.dfa2_activity_28



select cast(interaction_time as varchar) from wmprodfeeds.united.dfa2_activity_2
-- group by md_file_date
limit 100



delete from wmprodfeeds.united.dfa2_activity
where cast(timestamp 'epoch' + (event_time/ 1000000) * interval '1 second' as timestamp):: date < '2018-12-01';

delete from wmprodfeeds.united.dfa2_click
where cast(timestamp 'epoch' + (event_time/ 1000000) * interval '1 second' as timestamp):: date < '2018-12-01';
-- 			where cast(md_event_time as date) = '2018-09-29';

delete from wmprodfeeds.united.dfa2_impression
where cast(timestamp 'epoch' + (event_time/ 1000000) * interval '1 second' as timestamp):: date < '2018-12-01';


delete from wmprodfeeds.united.s3_moat_staging_video
where event_date < '2018-12-01';



row_number() over (partition by user_id, conversiontime order by conversiontime asc) as rc_1,



alter table wmprodfeeds.united.dfa2_activity_2
add column rc_1 int

select * from wmprodfeeds.united.dfa2_activity_2 limit 100

set statement_timeout to 0;
UPDATE wmprodfeeds.united.dfa2_activity_2
SET
-- 		keyz = cast(event_time as varchar) || user_id
rc_1 = row_number() over (partition by keyz order by keyz asc)
-- 	md_interaction_date_loc = convert_timezone('UTC', 'America/New_York', md_interaction_time)::date

-- ================================================================================================================
-- ================================================================================================================





select eventdate::date
from wmprodfeeds.ikea.sizmek_standard_events
group by eventdate::date
order by eventdate::date asc








select
	cast(md_event_time as date) as "date",
	count(distinct md_event_time) as md_event_time,
	count(distinct md_event_time_loc) as md_event_time_loc,
	count(distinct md_event_date) as md_event_date,
	count(distinct md_event_date_loc) as md_event_date_loc,
	count(*) as imps

from wmprodfeeds.united.dfa2_impression
	where cast(md_event_time as date) between '2018-08-30' and '2018-09-30'
group by cast(md_event_time as date)
order by cast(md_event_time as date) asc



select
	cast(md_event_time as date) as "date",
	count(distinct md_event_time) as md_event_time,
	count(distinct md_event_time_loc) as md_event_time_loc,
	count(distinct md_event_date) as md_event_date,
	count(distinct md_event_date_loc) as md_event_date_loc,
	count(*) as imps

from wmprodfeeds.united.dfa2_click
	where cast(md_event_time as date) between '2018-08-20' and '2018-08-26'
group by cast(md_event_time as date)


select  requestcustomparameters from wmprodfeeds.ikea.qualia_impressions
limit 100


delete from wmprodfeeds.united.dfa2_activity
			where cast(md_event_time as date) = '2018-09-24'

delete from wmprodfeeds.united.dfa2_impression
			where cast(md_event_time as date) between '2018-08-12' and '2018-08-15'

delete from wmprodfeeds.united.dfa2_click
			where cast(md_event_time as date) between '2018-08-12' and '2018-08-15'

select * from wmprodfeeds.ikea.sizmek_conversion_events
limit 100



select * from
              where placement = 'PD74NK_UAC_TMK_022_Audience_GOOGLE INC_Control-BidStrategy3-AP-8-21NonMember160x600_DESK_NA_Run of Network_Behavioral_P2Plus_1 x 1_Site-served_NV_NA'


select distinct md_file_date,count(*) from united.dfa2_activity
group by md_file_date
order by md_file_date desc
