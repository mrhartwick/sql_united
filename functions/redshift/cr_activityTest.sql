create table if not exists united.dfa2_activity
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_activity_optimized_advertiser_id_fkey
			references dfa2_advertisers,
	campaign_id integer
		constraint dfa2_activity_optimized_campaign_id_fkey
			references dfa2_campaigns,
	ad_id integer
		constraint dfa2_activity_optimized_ad_id_fkey
			references dfa2_ads,
	rendering_id integer
		constraint dfa2_activity_optimized_rendering_id_fkey
			references dfa2_creatives,
	creative_version integer,
	site_id_dcm integer
		constraint dfa2_activity_optimized_site_id_dcm_fkey
			references dfa2_sites,
	placement_id integer
		constraint dfa2_activity_optimized_placement_id_fkey
			references dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_activity_optimized_browser_platform_id_fkey
			references dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_activity_optimized_operating_system_id_fkey
			references dfa2_operating_systems,
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
	acquiredtime timestamp,
	md_interaction_time timestamp
)
diststyle key
sortkey(interaction_time, user_id, other_data, md_interaction_time)
;


insert into wmprodfeeds.united.dfa2_activity_test (select * from wmprodfeeds.united.dfa2_activity);