create table if not exists ikea.sizmek_conversion_events_FY18
(
	userid varchar(108) encode zstd distkey,
	conversionid varchar(108) not null encode zstd
		constraint sizmek_conversion_events_fkey
			primary key,
	conversiondate timestamp encode zstd,
	conversiontagid integer encode zstd,
	advertiserid integer encode zstd
		constraint sizmek_conversion_events_advertiserid_fkey
			references wmprodfeeds.ikea.sizmek_advertisers,
	accountid integer encode zstd
		constraint sizmek_conversion_events_accountid_fkey
			references wmprodfeeds.ikea.sizmek_accounts,
	revenue double precision encode zstd,
	currency integer encode zstd,
	quantity integer encode zstd,
	orderid varchar(50) encode zstd,
	productid varchar(300) encode zstd
		constraint sizmek_conversion_events_productid_fkey
			references wmprodfeeds.ikea.sizmek_product,
	winnerentityid integer encode zstd,
	winnersemid integer encode zstd,
	winnerseuniqueid varchar(108) encode zstd,
	eventtypeid integer encode zstd,
	winnereventdate timestamp encode zstd,
	placementid integer encode zstd
		constraint sizmek_conversion_events_placementid_fkey
			references wmprodfeeds.ikea.sizmek_placements,
	siteid integer encode zstd
		constraint sizmek_conversion_events_siteid_fkey
			references wmprodfeeds.ikea.sizmek_sites,
	campaignid integer encode zstd
		constraint sizmek_conversion_events_campaignid_fkey
			references wmprodfeeds.ikea.sizmek_display_campaigns,
	adgroupid integer,
	brandid integer encode zstd,
	countryid integer encode zstd
		constraint sizmek_conversion_events_countryid_fkey
			references wmprodfeeds.ikea.sizmek_country,
	stateid integer encode zstd
		constraint sizmek_conversion_events_stateid_fkey
			references wmprodfeeds.ikea.sizmek_state,
	dmaid integer encode bytedict
		constraint sizmek_conversion_events_dmaid_fkey
			references wmprodfeeds.ikea.sizmek_dma,
	cityid integer encode zstd
		constraint sizmek_conversion_events_cityid_fkey
			references wmprodfeeds.ikea.sizmek_city,
	zipcode varchar(32) encode zstd,
	areacode varchar(32) encode zstd,
	oscode integer encode zstd,
	browsercode integer encode zstd,
	ebcampaignid integer,
	secampaignid bigint
		constraint sizmek_conversion_events_secampaignid_fkey
			references wmprodfeeds.ikea.sizmek_search_campaigns,
	devicetypeid integer encode zstd,
	winnerversionid integer encode zstd,
	winnertargetaudienceid integer encode zstd,
	winnerdeliverygroupid integer encode zstd,
	isconversion boolean encode zstd,
	winnereventid varchar(108) encode zstd,
	conversiondatedefaulttimezone timestamp encode zstd,
	winnereventdatedefaulttimezone timestamp encode zstd,
	useridnumeric bigint encode zstd,
	md_file_date integer encode zstd,
	md_user_id_numeric bigint encode zstd,
	acquiredtime timestamp encode zstd
)
diststyle key
sortkey(winnereventdatedefaulttimezone, conversiondatedefaulttimezone, conversiondate, userid)
;


set statement_timeout to 0;
insert into wmprodfeeds.ikea.sizmek_conversion_events_FY18 (select userid,
conversionid,
conversiondate,
conversiontagid,
advertiserid,
accountid,
revenue,
currency,
quantity,
orderid,
productid,
winnerentityid,
winnersemid,
winnerseuniqueid,
eventtypeid,
winnereventdate,
placementid,
siteid,
campaignid,
adgroupid,
brandid,
countryid,
stateid,
dmaid,
cityid,
zipcode,
areacode,
oscode,
browsercode,
ebcampaignid,
secampaignid,
devicetypeid,
winnerversionid,
winnertargetaudienceid,
winnerdeliverygroupid,
isconversion,
winnereventid,
conversiondatedefaulttimezone,
winnereventdatedefaulttimezone,
useridnumeric,
md_file_date,
md_user_id_numeric,
acquiredtime from wmprodfeeds.ikea.sizmek_conversion_events where cast(conversiondatedefaulttimezone as date) between '2017-09-01' and '2018-08-31');

