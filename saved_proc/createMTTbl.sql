ALTER procedure dbo.createMTTbl
as
if OBJECT_ID('master.dbo.MTTable',N'U') is not null
	drop table master.dbo.MTTable;


create table master.dbo.MTTable
(
	joinKey                     varchar(255) not null,
	mtDate                      date         not null,
	media_property              varchar(255) not null,
	campaign_name               varchar(255) not null,
	placement_code              varchar(255) not null,
	placement_name              varchar(255),
	total_impressions           int          not null,
	groupm_passed_impressions   int          not null,
	groupm_billable_impressions int          not null
);

insert into master.dbo.MTTable
	select distinct
		MT.joinKey,
		MT.mtDate,
		MT.site_label                      as media_property,
		MT.campaign_label                  as campaign_name,
		MT.placement_id                    as placement_code,
		MT.placement_label                 as placement_name,
		sum(MT.human_impressions)          as total_impressions,
		sum(MT.half_duration_impressions)  as groupm_passed_impressions,
		sum(MT.groupm_payable_impressions) as groupm_billable_impressions
	from (select distinct


		      cast(date as date)                                as mtDate,
		      replace(left(placement_label,6) + '_' +
		              case
		              when ( site_label like '%[Cc]hicago%[Tt]ribune%' or site_label like '[Tt]ribune_[Ii]nteractive%' ) then 'ChicagoTribune'
				when ( site_label like '[Gg][Dd][Nn]%' or site_label like '[Gg]oogle_[Dd]isplay_[Nn]etwork%' ) then 'Google'
				when site_label like '%[Aa]dara%' then 'Adara'
                when site_label like '%[Aa]tlantic%' then 'The Atlantic'
				when site_label like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
				when site_label like '%[Cc][Nn][Nn]%' then 'CNN'
				when site_label like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
				when site_label like '%[Ff]orbes%' then 'Forbes'
				when site_label like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
				when site_label like '%[Jj]un%[Gg]roup%' then 'JunGroup'
				when site_label like '%[Mm][Ll][Bb]%' then 'MLB'
				when site_label like '%[Mm]ansueto%' then 'Inc'
				when site_label like '%[Mn][Ss][Nn]%' then 'MSN'
				when site_label like '%[Nn][Bb][Aa]%' then 'NBA'
				when site_label like '%[Nn][Ff][Ll]%' then 'NFL'
				when site_label like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
				when site_label like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
				when site_label like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
				when site_label like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
				when site_label like '%[Pp]riceline%' then 'Priceline'
				when site_label like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
				when site_label like '%[Tt]ap%[Aa]d%' then 'TapAd'
				when site_label like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
				when site_label like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
				when site_label like '%[Ww]all_[Ss]treet_[Jj]ournal%' then 'Wall Street Journal'
				when site_label like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
				when site_label like '%[Yy]ahoo%' then 'Yahoo'
				when site_label like '%[Yy]ou%[Tt]ube%' then 'YouTube'
				when site_label like '[Aa]d[Pp]rime%' then 'AdPrime'
				when site_label like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
				when site_label like '[Aa]mobee%' then 'Amobee'
				when site_label like '[Cc]ardlytics%' then 'Cardlytics'
				when site_label like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
				when site_label like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
				when site_label like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
				when site_label like '[Ff]acebook%' then 'Facebook'
				when site_label like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
				when site_label like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
                when site_label like '[Ff]lipboard%' then 'Flipboard'
				when site_label like '[Gg]um_[Gg]um%' then 'Gum Gum'
				when site_label like '[Hh]ulu%' then 'Hulu'
				when site_label like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
				when site_label like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
				when site_label like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
				when site_label like '[Ii]ndependent%' then 'Independent'
				when site_label like '[Kk]ayak%' then 'Kayak'
				when site_label like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
				when site_label like '[Mm]artini_[Mm]edia%' then 'Martini Media'
				when site_label like '[Oo]rbitz%' then 'Orbitz'
				when site_label like '[Ss]kyscanner%' then 'Skyscanner'
				when site_label like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
				when site_label like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
				when site_label like '[Ss]mithsonian%' then 'Smithsonian'
				when site_label like '[Ss]ojern%' then 'Sojern'
				when site_label like '[Ss]pecific_[Mm]edia%' or site_label like '[Vv]iant%' then 'Viant'
				when site_label like '[Ss]potify%' then 'Spotify'
				when site_label like '[Tt]ime%[Ii]nc%' then 'Time Inc'
				when site_label like '[Tt]ony%[As]wards%' then 'TonyAwards'
				when site_label like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
				when site_label like '[Tt]ravelocity%' then 'Travelocity'
				when site_label like '[Tt]riggit%' then 'Triggit'
				when site_label like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
                when site_label like '[Uu]ndertone%' then 'Undertone'
				when site_label like '[Uu]nited%' then 'United'
				when site_label like '[Vv]erve%' then 'VerveMobile'
				when site_label like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
				when site_label like '[Vv]ox%' then 'Vox'
				when site_label like '[Ww]ired%' then 'Wired'
				when site_label like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
				when site_label like '[Xx]ad%' then 'xAd Inc'
				when site_label like '[Yy]ieldbot%' then 'Yieldbot'
				when site_label like '[Yy]u[Mm]e%' then 'YuMe'
				else site_label end
		              + '_' + cast(date as varchar(10)),' ','') as joinKey,
		      campaign_label                                    as campaign_label,
		      case
		      when ( site_label like '%[Cc]hicago%[Tt]ribune%' or site_label like '[Tt]ribune_[Ii]nteractive%' ) then 'ChicagoTribune'
				when ( site_label like '[Gg][Dd][Nn]%' or site_label like '[Gg]oogle_[Dd]isplay_[Nn]etwork%' ) then 'Google'
				when site_label like '%[Aa]dara%' then 'Adara'
                when site_label like '%[Aa]tlantic%' then 'The Atlantic'
				when site_label like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
				when site_label like '%[Cc][Nn][Nn]%' then 'CNN'
				when site_label like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
				when site_label like '%[Ff]orbes%' then 'Forbes'
				when site_label like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
				when site_label like '%[Jj]un%[Gg]roup%' then 'JunGroup'
				when site_label like '%[Mm][Ll][Bb]%' then 'MLB'
				when site_label like '%[Mm]ansueto%' then 'Inc'
				when site_label like '%[Mn][Ss][Nn]%' then 'MSN'
				when site_label like '%[Nn][Bb][Aa]%' then 'NBA'
				when site_label like '%[Nn][Ff][Ll]%' then 'NFL'
				when site_label like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
				when site_label like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
				when site_label like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
				when site_label like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
				when site_label like '%[Pp]riceline%' then 'Priceline'
				when site_label like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
				when site_label like '%[Tt]ap%[Aa]d%' then 'TapAd'
				when site_label like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
				when site_label like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
				when site_label like '%[Ww]all_[Ss]treet_[Jj]ournal%' then 'Wall Street Journal'
				when site_label like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
				when site_label like '%[Yy]ahoo%' then 'Yahoo'
				when site_label like '%[Yy]ou%[Tt]ube%' then 'YouTube'
				when site_label like '[Aa]d[Pp]rime%' then 'AdPrime'
				when site_label like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
				when site_label like '[Aa]mobee%' then 'Amobee'
				when site_label like '[Cc]ardlytics%' then 'Cardlytics'
				when site_label like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
				when site_label like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
				when site_label like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
				when site_label like '[Ff]acebook%' then 'Facebook'
				when site_label like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
				when site_label like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
                when site_label like '[Ff]lipboard%' then 'Flipboard'
				when site_label like '[Gg]um_[Gg]um%' then 'Gum Gum'
				when site_label like '[Hh]ulu%' then 'Hulu'
				when site_label like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
				when site_label like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
				when site_label like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
				when site_label like '[Ii]ndependent%' then 'Independent'
				when site_label like '[Kk]ayak%' then 'Kayak'
				when site_label like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
				when site_label like '[Mm]artini_[Mm]edia%' then 'Martini Media'
				when site_label like '[Oo]rbitz%' then 'Orbitz'
				when site_label like '[Ss]kyscanner%' then 'Skyscanner'
				when site_label like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
				when site_label like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
				when site_label like '[Ss]mithsonian%' then 'Smithsonian'
				when site_label like '[Ss]ojern%' then 'Sojern'
				when site_label like '[Ss]pecific_[Mm]edia%' or site_label like '[Vv]iant%' then 'Viant'
				when site_label like '[Ss]potify%' then 'Spotify'
				when site_label like '[Tt]ime%[Ii]nc%' then 'Time Inc'
				when site_label like '[Tt]ony%[As]wards%' then 'TonyAwards'
				when site_label like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
				when site_label like '[Tt]ravelocity%' then 'Travelocity'
				when site_label like '[Tt]riggit%' then 'Triggit'
				when site_label like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
                when site_label like '[Uu]ndertone%' then 'Undertone'
				when site_label like '[Uu]nited%' then 'United'
				when site_label like '[Vv]erve%' then 'VerveMobile'
				when site_label like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
				when site_label like '[Vv]ox%' then 'Vox'
				when site_label like '[Ww]ired%' then 'Wired'
				when site_label like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
				when site_label like '[Xx]ad%' then 'xAd Inc'
				when site_label like '[Yy]ieldbot%' then 'Yieldbot'
				when site_label like '[Yy]u[Mm]e%' then 'YuMe'
				else site_label end                               as site_label,
		      placement_id                                      as placement_id,
		      placement_label                                   as placement_label,
		      human_impressions                                 as human_impressions,
		      half_duration_impressions                         as half_duration_impressions,
		      groupm_payable_impressions                        as groupm_payable_impressions

	      from VerticaGroupM.mec.UnitedUS.moat_impression) as MT

	where MT.placement_label not like '%[Dd][Oo]%[Nn][Oo][Tt]%[Uu][Ss][Ee]%'
	      and MT.placement_label not like '%[Nn]o%[Tt]racking%'
	group by
		MT.joinKey,
		MT.mtDate,
		MT.site_label,
		MT.campaign_label,
		MT.placement_id,
		MT.placement_label
go
