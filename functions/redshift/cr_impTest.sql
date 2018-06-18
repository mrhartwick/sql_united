create table if not exists united.dfa2_impression_test_1
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_2
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_3
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_4
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_5
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_6
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_7
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_8
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_9
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_10
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_11
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_12
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;


-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_13
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_14
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_15
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_16
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;



-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================
-- =========================================================================================================

create table if not exists united.dfa2_impression_test_17
(
	event_time bigint encode zstd,
	user_id varchar(50) encode zstd distkey,
	advertiser_id integer encode zstd
		constraint dfa2_impression_optimized_advertiser_id_fkey
			references united.dfa2_advertisers,
	campaign_id integer encode zstd
		constraint dfa2_impression_optimized_campaign_id_fkey
			references united.dfa2_campaigns,
	ad_id integer encode bytedict
		constraint dfa2_impression_optimized_ad_id_fkey
			references united.dfa2_ads,
	rendering_id integer encode bytedict
		constraint dfa2_impression_optimized_rendering_id_fkey
			references united.dfa2_creatives,
	creative_version integer encode zstd,
	site_id_dcm integer encode zstd
		constraint dfa2_impression_optimized_site_id_dcm_fkey
			references united.dfa2_sites,
	placement_id integer encode bytedict
		constraint dfa2_impression_optimized_placement_id_fkey
			references united.dfa2_placements,
	country_code char(2) encode zstd,
	state_region char(2) encode zstd,
	browser_platform_id integer encode zstd
		constraint dfa2_impression_optimized_browser_platform_id_fkey
			references united.dfa2_browsers,
	browser_platform_version double precision encode zstd,
	operating_system_id integer encode zstd
		constraint dfa2_impression_optimized_operating_system_id_fkey
			references united.dfa2_operating_systems,
	designated_market_area_dma_id integer encode bytedict
		constraint dfa2_impression_optimized_designated_market_area_dma_id_fkey
			references united.dfa2_designated_market_areas,
	city_id integer encode zstd,
	zip_postal_code varchar(10) encode zstd,
	u_value varchar(512) encode zstd,
	event_type varchar(255) encode zstd,
	event_sub_type varchar(255) encode zstd,
	dbm_auction_id varchar(255) encode zstd,
	dbm_request_time bigint encode zstd,
	dbm_advertiser_id integer encode zstd,
	dbm_insertion_order_id integer encode zstd,
	dbm_line_item_id integer encode zstd,
	dbm_creative_id integer encode zstd,
	dbm_bid_price_usd integer encode zstd,
	dbm_bid_price_partner_currency integer encode zstd,
	dbm_bid_price_advertiser_currency integer encode zstd,
	dbm_url varchar(4000) encode zstd,
	dbm_site_id bigint encode zstd,
	dbm_language varchar(255) encode zstd,
	dbm_adx_page_categories varchar(255) encode zstd,
	dbm_matching_targeted_keywords varchar(255) encode zstd,
	dbm_exchange_id integer encode zstd,
	dbm_attributed_inventory_source_external_id varchar(255) encode zstd,
	dbm_attributed_inventory_source_is_public boolean,
	dbm_ad_position integer encode zstd,
	dbm_country_code char(2) encode zstd,
	dbm_designated_market_area_dma_id integer encode zstd,
	dbm_zip_postal_code varchar(10) encode zstd,
	dbm_state_region_id integer encode zstd,
	dbm_city_id integer encode zstd,
	dbm_operating_system_id integer encode zstd,
	dbm_browser_platform_id integer encode zstd,
	dbm_browser_timezone_offset_minutes integer encode zstd,
	dbm_net_speed integer encode zstd,
	dbm_matching_targeted_segments varchar(255) encode zstd,
	dbm_isp_id integer encode zstd,
	dbm_device_type integer encode zstd,
	dbm_mobile_make_id integer encode zstd,
	dbm_mobile_model_id integer encode zstd,
	dbm_view_state varchar(255) encode zstd,
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
	active_view_eligible_impressions integer encode zstd,
	active_view_measurable_impressions integer encode zstd,
	active_view_viewable_impressions integer encode zstd,
	md_user_id_numeric bigint,
	md_user_id_0 boolean,
	md_event_time timestamp,
	md_dbm_request_time timestamp,
	md_file_date integer encode zstd,
	acquiredtime timestamp
)
diststyle key
sortkey(event_time, user_id, md_event_time)
;




-- ===============================================================================

insert into wmprodfeeds.united.dfa2_impression_test_1 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2017-01-01' and '2017-01-31');

insert into wmprodfeeds.united.dfa2_impression_test_2 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2017-02-01' and '2017-02-28');

insert into wmprodfeeds.united.dfa2_impression_test_3 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2017-03-01' and '2017-03-31');

insert into wmprodfeeds.united.dfa2_impression_test_4 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2017-04-01' and '2017-04-30');

insert into wmprodfeeds.united.dfa2_impression_test_5 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2017-05-01' and '2017-05-31');

insert into wmprodfeeds.united.dfa2_impression_test_6 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2017-06-01' and '2017-06-30');

insert into wmprodfeeds.united.dfa2_impression_test_7 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2017-07-01' and '2017-07-31');

insert into wmprodfeeds.united.dfa2_impression_test_8 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2017-08-01' and '2017-08-31');

insert into wmprodfeeds.united.dfa2_impression_test_9 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2017-09-01' and '2017-09-30');

insert into wmprodfeeds.united.dfa2_impression_test_10 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2017-10-01' and '2017-10-31');

insert into wmprodfeeds.united.dfa2_impression_test_11 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2017-11-01' and '2017-11-30');

insert into wmprodfeeds.united.dfa2_impression_test_12 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2017-12-01' and '2017-12-31');

insert into wmprodfeeds.united.dfa2_impression_test_13 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2018-01-01' and '2018-01-31');

insert into wmprodfeeds.united.dfa2_impression_test_14 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2018-02-01' and '2018-02-28');

insert into wmprodfeeds.united.dfa2_impression_test_15 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2018-03-01' and '2018-03-31');

insert into wmprodfeeds.united.dfa2_impression_test_16 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2018-04-01' and '2018-04-30');

insert into wmprodfeeds.united.dfa2_impression_test_17 (select * from wmprodfeeds.united.dfa2_impression where md_event_time::date between '2018-05-01' and '2018-05-31');