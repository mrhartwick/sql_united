
-- Eliminate dupes from Sizmek log files (likely there as a result of Glue Jobs, which never REPLACE; always INSERT

drop table if exists  wmprodfeeds.ikea.sizmek_standard_events;
create table  wmprodfeeds.ikea.sizmek_standard_events_2
(
	eventid varchar(108) encode zstd,
	userid varchar(108) encode zstd,
	eventtypeid integer encode zstd
		constraint sizmek_standard_events_eventtypeid_fkey
			references  wmprodfeeds.ikea.sizmek_event_type_standard,
	eventdate timestamp encode zstd,
	entityid integer encode zstd,
	semid integer,
	seuniqueid varchar(108) encode zstd,
	placementid integer encode zstd,
	siteid integer encode zstd,
	campaignid integer encode zstd
		constraint sizmek_standard_events_campaignid_fkey
			references  wmprodfeeds.ikea.sizmek_display_campaigns,
	brandid integer encode zstd
		constraint sizmek_standard_events_brandid_fkey
			references  wmprodfeeds.ikea.sizmek_brands,
	advertiserid integer encode zstd
		constraint sizmek_standard_events_advertiserid_fkey
			references  wmprodfeeds.ikea.sizmek_advertisers,
	accountid integer encode zstd
		constraint sizmek_standard_events_accountid_fkey
			references  wmprodfeeds.ikea.sizmek_accounts,
	searchadid bigint,
	adgroupid integer,
	countryid integer encode zstd
		constraint sizmek_standard_events_countryid_fkey
			references  wmprodfeeds.ikea.sizmek_country,
	stateid integer encode zstd
		constraint sizmek_standard_events_stateid_fkey
			references  wmprodfeeds.ikea.sizmek_state,
	dmaid integer encode bytedict
		constraint sizmek_standard_events_dmaid_fkey
			references  wmprodfeeds.ikea.sizmek_dma,
	zipcode varchar(32) encode zstd,
	areacode varchar(32) encode zstd,
	browsercode integer encode zstd,
	oscode integer encode zstd,
	referrer varchar(12000) encode zstd,
	mobiledevice varchar(100) encode zstd,
	mobilecarrier varchar(100) encode zstd,
	audienceid integer encode zstd,
	productid integer encode zstd,
	cityid integer encode zstd,
	pcp varchar(6144) encode zstd,
	ebcampaignid integer,
	secampaignid bigint
		constraint sizmek_standard_events_secampaignid_fkey
			references  wmprodfeeds.ikea.sizmek_search_campaigns,
	seaccountid varchar(1200) encode zstd,
	seadgroupid bigint,
	versions varchar(6144) encode zstd,
	deliverygroupid integer encode zstd,
	price integer,
	exchangeid integer,
	bidid integer,
	strategyid integer,
	impressiontypeid integer,
	impressionexchangetoken varchar(150) encode zstd,
	buyid integer,
	inventorysource varchar(100) encode zstd,
	dsp_id varchar(100) encode zstd,
	eventdatedefaulttimezone timestamp encode zstd,
	useridnumeric bigint encode zstd,
	md_file_date integer encode zstd,
	md_user_id_numeric bigint distkey
)
diststyle key
interleaved sortkey(md_file_date, eventtypeid, advertiserid);



insert into  wmprodfeeds.ikea.sizmek_standard_events
(
eventid,
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
referrer,
mobiledevice,
mobilecarrier,
audienceid,
productid,
cityid,
pcp,
ebcampaignid,
secampaignid,
seaccountid,
seadgroupid,
versions,
deliverygroupid,
price,
exchangeid,
bidid,
strategyid,
impressiontypeid,
impressionexchangetoken,
buyid,
inventorysource,
dsp_id,
eventdatedefaulttimezone,
useridnumeric,
md_file_date,
md_user_id_numeric
 )
 (
select
										  distinct
										  eventid,
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
referrer,
mobiledevice,
mobilecarrier,
audienceid,
productid,
cityid,
pcp,
ebcampaignid,
secampaignid,
seaccountid,
seadgroupid,
versions,
deliverygroupid,
price,
exchangeid,
bidid,
strategyid,
impressiontypeid,
impressionexchangetoken,
buyid,
inventorysource,
dsp_id,
eventdatedefaulttimezone,
useridnumeric,
md_file_date,
md_user_id_numeric from  wmprodfeeds.ikea.sizmek_standard_events
 );

-- ===========================================================================================================
drop table if exists  wmprodfeeds.ikea.sizmek_conversion_events;
create table  wmprodfeeds.ikea.sizmek_conversion_events_2
(
	userid 										varchar(108) encode zstd,
	conversionid 										varchar(108) not null encode zstd constraint sizmek_conversion_events_pkey primary key,
	conversiondate 										timestamp encode zstd,
	conversiontagid 										integer encode zstd,
	advertiserid 										integer encode zstd constraint sizmek_conversion_events_advertiserid_fkey references  wmprodfeeds.ikea.sizmek_advertisers,
	accountid 										integer encode zstd constraint sizmek_conversion_events_accountid_fkey references  wmprodfeeds.ikea.sizmek_accounts,
	revenue 										double precision encode zstd,
	currency 										integer encode zstd,
	quantity 										integer encode zstd,
	orderid 										varchar(50) encode zstd,
	referrer 										varchar(12000) encode zstd,
	productid 										varchar(300) encode zstd constraint sizmek_conversion_events_productid_fkey references  wmprodfeeds.ikea.sizmek_product,
	productinfo 										varchar(600) encode zstd,
	winnerentityid 										integer encode zstd,
	winnersemid 										integer encode zstd,
	winnerseuniqueid 										varchar(108) encode zstd,
	eventtypeid 										integer encode zstd,
	winnereventdate 										timestamp encode zstd,
	placementid 										integer encode zstd constraint sizmek_conversion_events_placementid_fkey references  wmprodfeeds.ikea.sizmek_placements,
	siteid 										integer encode zstd constraint sizmek_conversion_events_siteid_fkey references  wmprodfeeds.ikea.sizmek_sites,
	campaignid 										integer encode zstd constraint sizmek_conversion_events_campaignid_fkey references  wmprodfeeds.ikea.sizmek_display_campaigns,
	adgroupid 										integer,
	brandid 										integer encode zstd,
	countryid 										integer encode zstd constraint sizmek_conversion_events_countryid_fkey references  wmprodfeeds.ikea.sizmek_country,
	stateid 										integer encode zstd constraint sizmek_conversion_events_stateid_fkey references  wmprodfeeds.ikea.sizmek_state,
	dmaid 										integer encode bytedict constraint sizmek_conversion_events_dmaid_fkey references  wmprodfeeds.ikea.sizmek_dma,
	cityid 										integer encode zstd constraint sizmek_conversion_events_cityid_fkey references  wmprodfeeds.ikea.sizmek_city,
	zipcode 										varchar(32) encode zstd,
	areacode 										varchar(32) encode zstd,
	oscode 										integer encode zstd,
	browsercode 										integer encode zstd,
	string1 										varchar(600) encode zstd,
	string2 										varchar(600) encode zstd,
	string3 										varchar(600) encode zstd,
	string4 										varchar(600) encode zstd,
	string5 										varchar(600) encode zstd,
	string6 										varchar(600) encode zstd,
	string7 										varchar(600) encode zstd,
	string8 										varchar(600) encode zstd,
	string9 										varchar(600) encode zstd,
	string10 										varchar(600) encode zstd,
	string11 										varchar(600) encode zstd,
	string12 										varchar(600) encode zstd,
	string13 										varchar(600) encode zstd,
	string14 										varchar(600) encode zstd,
	string15 										varchar(600) encode zstd,
	string16 										varchar(600) encode zstd,
	string17 										varchar(600) encode zstd,
	string18 										varchar(600) encode zstd,
	string19 										varchar(600) encode zstd,
	string20 										varchar(600) encode zstd,
	string21 										varchar(600) encode zstd,
	string22 										varchar(600) encode zstd,
	string23 										varchar(600) encode zstd,
	string24 										varchar(600) encode zstd,
	string25 										varchar(600) encode zstd,
	string26 										varchar(600) encode zstd,
	string27 										varchar(600) encode zstd,
	string28 										varchar(600) encode zstd,
	string29 										varchar(600) encode zstd,
	string30 										varchar(600) encode zstd,
	string31 										varchar(600) encode zstd,
	string32 										varchar(600) encode zstd,
	string33 										varchar(600) encode zstd,
	string34 										varchar(600) encode zstd,
	string35 										varchar(600) encode zstd,
	string36 										varchar(600) encode zstd,
	string37 										varchar(600) encode zstd,
	string38 										varchar(600) encode zstd,
	string39 										varchar(600) encode zstd,
	string40 										varchar(600) encode zstd,
	string41 										varchar(600) encode zstd,
	string42 										varchar(600) encode zstd,
	string43 										varchar(600) encode zstd,
	string44 										varchar(600) encode zstd,
	string45 										varchar(600) encode zstd,
	string46 										varchar(600) encode zstd,
	string47 										varchar(600) encode zstd,
	string48 										varchar(600) encode zstd,
	string49 										varchar(600) encode zstd,
	string50 										varchar(12000) encode zstd,
	ebcampaignid 										integer,
	secampaignid 										bigint constraint sizmek_conversion_events_secampaignid_fkey references  wmprodfeeds.ikea.sizmek_search_campaigns,
	seaccountid 										varchar(1200) encode zstd,
	seadgroupid 										bigint,
	devicetypeid 										integer encode zstd,
	winnerversionid 										integer encode zstd,
	winnertargetaudienceid 										integer encode zstd,
	winnerdeliverygroupid 										integer encode zstd,
	isconversion 										boolean encode zstd,
	mobiledevice 										varchar(100) encode zstd,
	winnerpcp 										varchar(12000) encode zstd,
	winnereventid 										varchar(108) encode zstd,
	conversiondatedefaulttimezone 										timestamp encode zstd,
	winnereventdatedefaulttimezone 										timestamp encode zstd,
	useridnumeric 										bigint encode zstd,
	md_file_date 										integer encode zstd,
	md_user_id_numeric 										bigint encode zstd distkey
)
diststyle key
interleaved sortkey(md_file_date, eventtypeid, winnerentityid);


insert into  wmprodfeeds.ikea.sizmek_conversion_events
(
	userid,
conversionid,
conversiondate,
conversiontagid,
advertiserid,
accountid,
revenue,
currency,
quantity,
orderid,
referrer,
productid,
productinfo,
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
string1,
string2,
string3,
string4,
string5,
string6,
string7,
string8,
string9,
string10,
string11,
string12,
string13,
string14,
string15,
string16,
string17,
string18,
string19,
string20,
string21,
string22,
string23,
string24,
string25,
string26,
string27,
string28,
string29,
string30,
string31,
string32,
string33,
string34,
string35,
string36,
string37,
string38,
string39,
string40,
string41,
string42,
string43,
string44,
string45,
string46,
string47,
string48,
string49,
string50,
ebcampaignid,
secampaignid,
seaccountid,
seadgroupid,
devicetypeid,
winnerversionid,
winnertargetaudienceid,
winnerdeliverygroupid,
isconversion,
mobiledevice,
winnerpcp,
winnereventid,
conversiondatedefaulttimezone,
winnereventdatedefaulttimezone,
useridnumeric,
md_file_date,
md_user_id_numeric

 )
 (
select
										  distinct
userid,
conversionid,
conversiondate,
conversiontagid,
advertiserid,
accountid,
revenue,
currency,
quantity,
orderid,
referrer,
productid,
productinfo,
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
string1,
string2,
string3,
string4,
string5,
string6,
string7,
string8,
string9,
string10,
string11,
string12,
string13,
string14,
string15,
string16,
string17,
string18,
string19,
string20,
string21,
string22,
string23,
string24,
string25,
string26,
string27,
string28,
string29,
string30,
string31,
string32,
string33,
string34,
string35,
string36,
string37,
string38,
string39,
string40,
string41,
string42,
string43,
string44,
string45,
string46,
string47,
string48,
string49,
string50,
ebcampaignid,
secampaignid,
seaccountid,
seadgroupid,
devicetypeid,
winnerversionid,
winnertargetaudienceid,
winnerdeliverygroupid,
isconversion,
mobiledevice,
winnerpcp,
winnereventid,
conversiondatedefaulttimezone,
winnereventdatedefaulttimezone,
useridnumeric,
md_file_date,
md_user_id_numeric
from  wmprodfeeds.ikea.sizmek_conversion_events
 );

-- ===========================================================================================================
-- ===========================================================================================================

-- Create a table with all userid/browser/device combos
-- No userid should have more than a single browser/device combo

drop table if exists  wmprodfeeds.ikea.sizmek_conversion_events_check;
create table  wmprodfeeds.ikea.sizmek_conversion_events_check
(
		userid varchar(108) encode zstd distkey,
		browsercode integer encode zstd,
		mobiledevice varchar(100) encode zstd
)
diststyle key
sortkey(userid);

insert into  wmprodfeeds.ikea.sizmek_conversion_events_check
(
userid,
browsercode,
mobiledevice
 )

 (
select
distinct
userid,
browsercode,
mobiledevice
from  wmprodfeeds.ikea.sizmek_conversion_events
-- Only Impression-attributed events, as we're relying on the Qualia Impression log for device IDs
where eventtypeid =1
 );

-- ===========================================================================================================
-- ===========================================================================================================

-- Create a table containing all userids which have more than one browser/device combo
-- This is for exclusion from later queries

drop table if exists  wmprodfeeds.ikea.sizmek_bad_ids;
create table  wmprodfeeds.ikea.sizmek_bad_ids
(
		userid varchar(108) encode zstd distkey
)
diststyle key
sortkey(userid);

insert into  wmprodfeeds.ikea.sizmek_bad_ids
(
userid
 )

(select
                 userid
	from  wmprodfeeds.ikea.sizmek_standard_events_check
	group by userid
	having COUNT(userid) > 1);

