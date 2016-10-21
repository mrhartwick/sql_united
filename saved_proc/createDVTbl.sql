ALTER procedure dbo.createDVTbl
as
if OBJECT_ID('master.dbo.DVTable',N'U') is not null
	drop table master.dbo.DVTable;


create table master.dbo.DVTable
(
	joinKey                     varchar(255) not null,
	dvDate                      date         not null,
	media_property              varchar(255) not null,
	campaign_name               varchar(255) not null,
	placement_code              varchar(255) not null,
	placement_name              varchar(255),
	total_impressions           int          not null,
	groupm_passed_impressions   int          not null,
	groupm_billable_impressions int          not null

);

insert into master.dbo.DVTable
	select distinct
		DV.joinKey,
		DV.dvDate,
		DV.media_property,
		DV.campaign_name,
		DV.placement_code,
		DV.placement_name,
		sum(DV.total_impressions),
		sum(DV.groupm_passed_impressions),
		sum(DV.groupm_billable_impressions)
	from ( select distinct


		       cast(date as date)                                as dvDate,
		       replace(left(placement_name,6) + '_' +
		               case
		               when ( media_property like '%[Cc]hicago%[Tt]ribune%' or media_property like '[Tt]ribune_[Ii]nteractive%' ) then 'ChicagoTribune'
				when ( media_property like '[Gg][Dd][Nn]%' or media_property like '[Gg]oogle_[Dd]isplay_[Nn]etwork%' ) then 'Google'
				when media_property like '%[Aa]dara%' then 'Adara'
				when media_property like '%[Aa]tlantic%' then 'The Atlantic'
				when media_property like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
				when media_property like '%[Cc][Nn][Nn]%' then 'CNN'
				when media_property like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
				when media_property like '%[Ff]orbes%' then 'Forbes'
				when media_property like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
				when media_property like '%[Jj]un%[Gg]roup%' then 'JunGroup'
				when media_property like '%[Mm][Ll][Bb]%' then 'MLB'
				when media_property like '%[Mm]ansueto%' then 'Inc'
				when media_property like '%[Mn][Ss][Nn]%' then 'MSN'
				when media_property like '%[Nn][Bb][Aa]%' then 'NBA'
				when media_property like '%[Nn][Ff][Ll]%' then 'NFL'
				when media_property like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
				when media_property like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
				when media_property like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
				when media_property like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
				when media_property like '%[Pp]riceline%' then 'Priceline'
				when media_property like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
				when media_property like '%[Tt]ap%[Aa]d%' then 'TapAd'
				when media_property like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
				when media_property like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
				when media_property like '%[Ww]all_[Ss]treet_[Jj]ournal%' then 'Wall Street Journal'
				when media_property like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
				when media_property like '%[Yy]ahoo%' then 'Yahoo'
				when media_property like '%[Yy]ou%[Tt]ube%' then 'YouTube'
				when media_property like '[Aa]d[Pp]rime%' then 'AdPrime'
				when media_property like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
				when media_property like '[Aa]mobee%' then 'Amobee'
				when media_property like '[Cc]ardlytics%' then 'Cardlytics'
				when media_property like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
				when media_property like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
				when media_property like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
				when media_property like '[Ff]acebook%' then 'Facebook'
				when media_property like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
				when media_property like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
			    when media_property like '[Ff]lipboard%' then 'Flipboard'
				when media_property like '[Gg]um_[Gg]um%' then 'Gum Gum'
				when media_property like '[Hh]ulu%' then 'Hulu'
				when media_property like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
				when media_property like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
				when media_property like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
				when media_property like '[Ii]ndependent%' then 'Independent'
				when media_property like '[Kk]ayak%' then 'Kayak'
				when media_property like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
				when media_property like '[Mm]artini_[Mm]edia%' then 'Martini Media'
				when media_property like '[Oo]rbitz%' then 'Orbitz'
				when media_property like '[Ss]kyscanner%' then 'Skyscanner'
				when media_property like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
				when media_property like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
				when media_property like '[Ss]mithsonian%' then 'Smithsonian'
				when media_property like '[Ss]ojern%' then 'Sojern'
				when media_property like '[Ss]pecific_[Mm]edia%' or media_property like '[Vv]iant%' then 'Viant'
				when media_property like '[Ss]potify%' then 'Spotify'
				when media_property like '[Tt]ime%[Ii]nc%' then 'Time Inc'
				when media_property like '[Tt]ony%[As]wards%' then 'TonyAwards'
				when media_property like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
				when media_property like '[Tt]ravelocity%' then 'Travelocity'
				when media_property like '[Tt]riggit%' then 'Triggit'
				when media_property like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
			    when media_property like '[Uu]ndertone%' then 'Undertone'
				when media_property like '[Uu]nited%' then 'United'
				when media_property like '[Vv]erve%' then 'VerveMobile'
				when media_property like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
				when media_property like '[Vv]ox%' then 'Vox'
				when media_property like '[Ww]ired%' then 'Wired'
				when media_property like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
				when media_property like '[Xx]ad%' then 'xAd Inc'
				when media_property like '[Yy]ieldbot%' then 'Yieldbot'
				when media_property like '[Yy]u[Mm]e%' then 'YuMe'
				else media_property end
		               + '_' + cast(date as varchar(10)),' ','') as joinKey,

		       campaign_name                                     as campaign_name,
		       case
		       when ( media_property like '%[Cc]hicago%[Tt]ribune%' or media_property like '[Tt]ribune_[Ii]nteractive%' ) then 'ChicagoTribune'
				when ( media_property like '[Gg][Dd][Nn]%' or media_property like '[Gg]oogle_[Dd]isplay_[Nn]etwork%' ) then 'Google'
				when media_property like '%[Aa]dara%' then 'Adara'
				when media_property like '%[Aa]tlantic%' then 'The Atlantic'
				when media_property like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
				when media_property like '%[Cc][Nn][Nn]%' then 'CNN'
				when media_property like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
				when media_property like '%[Ff]orbes%' then 'Forbes'
				when media_property like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
				when media_property like '%[Jj]un%[Gg]roup%' then 'JunGroup'
				when media_property like '%[Mm][Ll][Bb]%' then 'MLB'
				when media_property like '%[Mm]ansueto%' then 'Inc'
				when media_property like '%[Mn][Ss][Nn]%' then 'MSN'
				when media_property like '%[Nn][Bb][Aa]%' then 'NBA'
				when media_property like '%[Nn][Ff][Ll]%' then 'NFL'
				when media_property like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
				when media_property like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
				when media_property like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
				when media_property like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
				when media_property like '%[Pp]riceline%' then 'Priceline'
				when media_property like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
				when media_property like '%[Tt]ap%[Aa]d%' then 'TapAd'
				when media_property like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
				when media_property like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
				when media_property like '%[Ww]all_[Ss]treet_[Jj]ournal%' then 'Wall Street Journal'
				when media_property like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
				when media_property like '%[Yy]ahoo%' then 'Yahoo'
				when media_property like '%[Yy]ou%[Tt]ube%' then 'YouTube'
				when media_property like '[Aa]d[Pp]rime%' then 'AdPrime'
				when media_property like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
				when media_property like '[Aa]mobee%' then 'Amobee'
				when media_property like '[Cc]ardlytics%' then 'Cardlytics'
				when media_property like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
				when media_property like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
				when media_property like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
				when media_property like '[Ff]acebook%' then 'Facebook'
				when media_property like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
				when media_property like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
			    when media_property like '[Ff]lipboard%' then 'Flipboard'
				when media_property like '[Gg]um_[Gg]um%' then 'Gum Gum'
				when media_property like '[Hh]ulu%' then 'Hulu'
				when media_property like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
				when media_property like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
				when media_property like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
				when media_property like '[Ii]ndependent%' then 'Independent'
				when media_property like '[Kk]ayak%' then 'Kayak'
				when media_property like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
				when media_property like '[Mm]artini_[Mm]edia%' then 'Martini Media'
				when media_property like '[Oo]rbitz%' then 'Orbitz'
				when media_property like '[Ss]kyscanner%' then 'Skyscanner'
				when media_property like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
				when media_property like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
				when media_property like '[Ss]mithsonian%' then 'Smithsonian'
				when media_property like '[Ss]ojern%' then 'Sojern'
				when media_property like '[Ss]pecific_[Mm]edia%' or media_property like '[Vv]iant%' then 'Viant'
				when media_property like '[Ss]potify%' then 'Spotify'
				when media_property like '[Tt]ime%[Ii]nc%' then 'Time Inc'
				when media_property like '[Tt]ony%[As]wards%' then 'TonyAwards'
				when media_property like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
				when media_property like '[Tt]ravelocity%' then 'Travelocity'
				when media_property like '[Tt]riggit%' then 'Triggit'
				when media_property like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
			    when media_property like '[Uu]ndertone%' then 'Undertone'
				when media_property like '[Uu]nited%' then 'United'
				when media_property like '[Vv]erve%' then 'VerveMobile'
				when media_property like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
				when media_property like '[Vv]ox%' then 'Vox'
				when media_property like '[Ww]ired%' then 'Wired'
				when media_property like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
				when media_property like '[Xx]ad%' then 'xAd Inc'
				when media_property like '[Yy]ieldbot%' then 'Yieldbot'
				when media_property like '[Yy]u[Mm]e%' then 'YuMe'
				else media_property end                           as media_property,
		       placement_code                                    as placement_code,
		       placement_name                                    as placement_name,
		       total_impressions                                 as total_impressions,
		       groupm_passed_impressions                         as groupm_passed_impressions,
		       groupm_billable_impressions                       as groupm_billable_impressions

	       from VerticaGroupM.mec.UnitedUS.dv_impression_agg ) as DV

	where DV.placement_name not like '%[Dd][Oo]%[Nn][Oo][Tt]%[Uu][Ss][Ee]%'
	      and DV.placement_name not like '%[Nn]o%[Tt]racking%'
-- and cast(DV.dvDate AS DATE) BETWEEN '2016-01-01' AND '2016-03-31'
	group by
		DV.joinKey,
		DV.dvDate,
		DV.media_property,
		DV.campaign_name,
		DV.placement_code,
		DV.placement_name
go
