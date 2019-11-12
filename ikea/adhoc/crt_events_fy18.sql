create table if not exists wmprodfeeds.ikea.sizmek_standard_events_FY18
(
		eventid varchar(108) encode zstd,
	userid varchar(108) encode zstd distkey,
	eventtypeid integer encode zstd
		constraint sizmek_standard_events_eventtypeid_fkey
			references wmprodfeeds.ikea.sizmek_event_type_standard,
	eventdate timestamp encode zstd,
	entityid integer encode zstd,
	semid integer,
	seuniqueid varchar(108) encode zstd,
	placementid integer encode zstd,
	siteid integer encode zstd,
	campaignid integer encode zstd
		constraint sizmek_standard_events_campaignid_fkey
			references wmprodfeeds.ikea.sizmek_display_campaigns,
	brandid integer encode zstd
		constraint sizmek_standard_events_brandid_fkey
			references wmprodfeeds.ikea.sizmek_brands,
	advertiserid integer encode zstd
		constraint sizmek_standard_events_advertiserid_fkey
			references wmprodfeeds.ikea.sizmek_advertisers,
	accountid integer encode zstd
		constraint sizmek_standard_events_accountid_fkey
			references wmprodfeeds.ikea.sizmek_accounts,
	searchadid bigint,
	adgroupid integer,
	countryid integer encode zstd
		constraint sizmek_standard_events_countryid_fkey
			references wmprodfeeds.ikea.sizmek_country,
	stateid integer encode zstd
		constraint sizmek_standard_events_stateid_fkey
			references wmprodfeeds.ikea.sizmek_state,
	dmaid integer encode bytedict
		constraint sizmek_standard_events_dmaid_fkey
			references wmprodfeeds.ikea.sizmek_dma,
	zipcode varchar(32) encode zstd,
	areacode varchar(32) encode zstd,
	browsercode integer encode zstd,
	oscode integer encode zstd,
	audienceid integer encode zstd,
	productid integer encode zstd,
	cityid integer encode zstd,
	ebcampaignid integer,
	secampaignid bigint
		constraint sizmek_standard_events_secampaignid_fkey
			references wmprodfeeds.ikea.sizmek_search_campaigns,
	seadgroupid bigint,
	deliverygroupid integer encode zstd,
	price integer,
	exchangeid integer,
	bidid integer,
	strategyid integer,
	impressiontypeid integer,
		buyid integer,
	eventdatedefaulttimezone timestamp encode zstd,
	useridnumeric bigint encode zstd,
	md_file_date integer encode zstd,
	md_user_id_numeric bigint,
	acquiredtime timestamp encode zstd
)
diststyle key
sortkey(eventdatedefaulttimezone, eventdate, userid)
;


insert into wmprodfeeds.ikea.sizmek_standard_events_FY18 (select eventid,
userid,
eventtypeid,
eventdate,
entityid,
semid,
seuniqueid,
placementid,
siteid,
campaignid,
brandid,
advertiserid,
accountid,
searchadid,
adgroupid,
countryid,
stateid,
dmaid,
zipcode,
areacode,
browsercode,
oscode,
audienceid,
productid,
cityid,
ebcampaignid,
secampaignid,
seadgroupid,
deliverygroupid,
price,
exchangeid,
bidid,
strategyid,
impressiontypeid,
buyid,
eventdatedefaulttimezone,
useridnumeric,
md_file_date,
md_user_id_numeric,
acquiredtime from wmprodfeeds.ikea.sizmek_standard_events where cast(eventdatedefaulttimezone as date) between '2018-01-01' and '2018-08-31');

select eventdatedefaulttimezone::date from wmprodfeeds.ikea.sizmek_standard_events_FY18
group by eventdatedefaulttimezone::date
order by eventdatedefaulttimezone::date desc
