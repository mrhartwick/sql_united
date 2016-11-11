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
        replace(left(t1.placement_name,6)+'_'+
                case	when ( t1.media_property like '%[Cc]hicago%[Tt]ribune%' or t1.media_property like '[Tt]ribune_[Ii]nteractive%' ) then 'ChicagoTribune'
				when ( t1.media_property like '[Gg][Dd][Nn]%' or t1.media_property like '[Gg]oogle_[Dd]isplay_[Nn]etwork%' ) then 'Google'
				when t1.media_property like '%[Aa]dara%' then 'Adara'
				when t1.media_property like '%[Aa]tlantic%' then 'The Atlantic'
				when t1.media_property like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
				when t1.media_property like '%[Cc][Nn][Nn]%' then 'CNN'
				when t1.media_property like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
				when t1.media_property like '%[Ff]orbes%' then 'Forbes'
				when t1.media_property like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
				when t1.media_property like '%[Jj]un%[Gg]roup%' then 'JunGroup'
				when t1.media_property like '%[Mm][Ll][Bb]%' then 'MLB'
				when t1.media_property like '%[Mm]ansueto%' then 'Inc'
				when t1.media_property like '%[Mn][Ss][Nn]%' then 'MSN'
				when t1.media_property like '%[Nn][Bb][Aa]%' then 'NBA'
				when t1.media_property like '%[Nn][Ff][Ll]%' then 'NFL'
				when t1.media_property like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
				when t1.media_property like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
				when t1.media_property like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
				when t1.media_property like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
				when t1.media_property like '%[Pp]riceline%' then 'Priceline'
				when t1.media_property like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
				when t1.media_property like '%[Tt]ap%[Aa]d%' then 'TapAd'
				when t1.media_property like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
				when t1.media_property like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
				when t1.media_property like '%[Ww]all_[Ss]treet_[Jj]ournal%'  or t1.media_property like '[Ww][Ss][Jj]' then 'Wall Street Journal'
				when t1.media_property like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
				when t1.media_property like '%[Yy]ahoo%' then 'Yahoo'
				when t1.media_property like '%[Yy]ou%[Tt]ube%' then 'YouTube'
				when t1.media_property like '[Aa]d[Pp]rime%' then 'AdPrime'
				when t1.media_property like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
				when t1.media_property like '[Aa]mobee%' then 'Amobee'
				when t1.media_property like '[Cc]ardlytics%' then 'Cardlytics'
				when t1.media_property like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
				when t1.media_property like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
				when t1.media_property like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
				when t1.media_property like '[Ff]acebook%' then 'Facebook'
				when t1.media_property like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
				when t1.media_property like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
			    when t1.media_property like '[Ff]lipboard%' then 'Flipboard'
				when t1.media_property like '[Gg]um_[Gg]um%' then 'Gum Gum'
				when t1.media_property like '[Hh]ulu%' then 'Hulu'
				when t1.media_property like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
				when t1.media_property like '[Ii][Nn][Cc]%' then 'Inc'
				when t1.media_property like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
				when t1.media_property like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
				when t1.media_property like '[Ii]ndependent%' then 'Independent'
				when t1.media_property like '[Kk]ayak%' then 'Kayak'
				when t1.media_property like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
				when t1.media_property like '[Mm]artini_[Mm]edia%' then 'Martini Media'
				when t1.media_property like '[Oo]rbitz%' then 'Orbitz'
				when t1.media_property like '[Ss]kyscanner%' then 'Skyscanner'
				when t1.media_property like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
				when t1.media_property like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
				when t1.media_property like '[Ss]mithsonian%' then 'Smithsonian'
				when t1.media_property like '[Ss]ojern%' then 'Sojern'
				when t1.media_property like '[Ss]pecific_[Mm]edia%' or t1.media_property like '[Vv]iant%' then 'Viant'
				when t1.media_property like '[Ss]potify%' then 'Spotify'
				when t1.media_property like '[Tt]ime%[Ii]nc%' then 'Time Inc'
				when t1.media_property like '[Tt]ony%[As]wards%' then 'TonyAwards'
				when t1.media_property like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
				when t1.media_property like '[Tt]ravelocity%' then 'Travelocity'
				when t1.media_property like '[Tt]riggit%' then 'Triggit'
				when t1.media_property like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
				when t1.media_property like '[Uu]ndertone%' then 'Undertone'
				when t1.media_property like '[Uu]nited%' then 'United'
				when t1.media_property like '[Vv]erve%' then 'VerveMobile'
				when t1.media_property like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
				when t1.media_property like '[Vv]ox%' then 'Vox'
				when t1.media_property like '[Ww]ired%' then 'Wired'
				when t1.media_property like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
				when t1.media_property like '[Xx]ad%' then 'xAd Inc'
				when t1.media_property like '[Yy]ieldbot%' then 'Yieldbot'
				when t1.media_property like '[Yy]u[Mm]e%' then 'YuMe'
				else t1.media_property end
                +'_'+cast(t1.mtDate as varchar(10)),' ','') as joinKey,
        t1.mtDate,
        case	when ( t1.media_property like '%[Cc]hicago%[Tt]ribune%' or t1.media_property like '[Tt]ribune_[Ii]nteractive%' ) then 'ChicagoTribune'
				when ( t1.media_property like '[Gg][Dd][Nn]%' or t1.media_property like '[Gg]oogle_[Dd]isplay_[Nn]etwork%' ) then 'Google'
				when t1.media_property like '%[Aa]dara%' then 'Adara'
				when t1.media_property like '%[Aa]tlantic%' then 'The Atlantic'
				when t1.media_property like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
				when t1.media_property like '%[Cc][Nn][Nn]%' then 'CNN'
				when t1.media_property like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
				when t1.media_property like '%[Ff]orbes%' then 'Forbes'
				when t1.media_property like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
				when t1.media_property like '%[Jj]un%[Gg]roup%' then 'JunGroup'
				when t1.media_property like '%[Mm][Ll][Bb]%' then 'MLB'
				when t1.media_property like '%[Mm]ansueto%' then 'Inc'
				when t1.media_property like '%[Mn][Ss][Nn]%' then 'MSN'
				when t1.media_property like '%[Nn][Bb][Aa]%' then 'NBA'
				when t1.media_property like '%[Nn][Ff][Ll]%' then 'NFL'
				when t1.media_property like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
				when t1.media_property like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
				when t1.media_property like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
				when t1.media_property like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
				when t1.media_property like '%[Pp]riceline%' then 'Priceline'
				when t1.media_property like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
				when t1.media_property like '%[Tt]ap%[Aa]d%' then 'TapAd'
				when t1.media_property like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
				when t1.media_property like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
				when t1.media_property like '%[Ww]all_[Ss]treet_[Jj]ournal%'  or t1.media_property like '[Ww][Ss][Jj]' then 'Wall Street Journal'
				when t1.media_property like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
				when t1.media_property like '%[Yy]ahoo%' then 'Yahoo'
				when t1.media_property like '%[Yy]ou%[Tt]ube%' then 'YouTube'
				when t1.media_property like '[Aa]d[Pp]rime%' then 'AdPrime'
				when t1.media_property like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
				when t1.media_property like '[Aa]mobee%' then 'Amobee'
				when t1.media_property like '[Cc]ardlytics%' then 'Cardlytics'
				when t1.media_property like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
				when t1.media_property like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
				when t1.media_property like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
				when t1.media_property like '[Ff]acebook%' then 'Facebook'
				when t1.media_property like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
				when t1.media_property like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
			    when t1.media_property like '[Ff]lipboard%' then 'Flipboard'
				when t1.media_property like '[Gg]um_[Gg]um%' then 'Gum Gum'
				when t1.media_property like '[Hh]ulu%' then 'Hulu'
				when t1.media_property like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
				when t1.media_property like '[Ii][Nn][Cc]%' then 'Inc'
				when t1.media_property like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
				when t1.media_property like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
				when t1.media_property like '[Ii]ndependent%' then 'Independent'
				when t1.media_property like '[Kk]ayak%' then 'Kayak'
				when t1.media_property like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
				when t1.media_property like '[Mm]artini_[Mm]edia%' then 'Martini Media'
				when t1.media_property like '[Oo]rbitz%' then 'Orbitz'
				when t1.media_property like '[Ss]kyscanner%' then 'Skyscanner'
				when t1.media_property like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
				when t1.media_property like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
				when t1.media_property like '[Ss]mithsonian%' then 'Smithsonian'
				when t1.media_property like '[Ss]ojern%' then 'Sojern'
				when t1.media_property like '[Ss]pecific_[Mm]edia%' or t1.media_property like '[Vv]iant%' then 'Viant'
				when t1.media_property like '[Ss]potify%' then 'Spotify'
				when t1.media_property like '[Tt]ime%[Ii]nc%' then 'Time Inc'
				when t1.media_property like '[Tt]ony%[As]wards%' then 'TonyAwards'
				when t1.media_property like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
				when t1.media_property like '[Tt]ravelocity%' then 'Travelocity'
				when t1.media_property like '[Tt]riggit%' then 'Triggit'
				when t1.media_property like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
				when t1.media_property like '[Uu]ndertone%' then 'Undertone'
				when t1.media_property like '[Uu]nited%' then 'United'
				when t1.media_property like '[Vv]erve%' then 'VerveMobile'
				when t1.media_property like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
				when t1.media_property like '[Vv]ox%' then 'Vox'
				when t1.media_property like '[Ww]ired%' then 'Wired'
				when t1.media_property like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
				when t1.media_property like '[Xx]ad%' then 'xAd Inc'
				when t1.media_property like '[Yy]ieldbot%' then 'Yieldbot'
				when t1.media_property like '[Yy]u[Mm]e%' then 'YuMe'
				else t1.media_property end                          as media_property,
        t1.campaign_name                                    as campaign_name,
        t1.placement_code                                   as placement_code,
        t1.placement_name                                   as placement_name,
        sum(t1.total_impressions)                           as total_impressions,
        sum(t1.groupm_passed_impressions)                   as groupm_passed_impressions,
        sum(t1.groupm_billable_impressions)                 as groupm_billable_impressions
    from (

             select
-- MT.joinKey as joinKey,
  MT.mtDate                          as mtDate,
		MT.site_label                      as media_property,
		pl.directory_site                      as media_property2,
                 case when (LEN(ISNULL(MT.site_label,''))=0) then pl.directory_site
                 else MT.site_label end             as media_property,
                 MT.campaign_label                  as campaign_name,
	mt.campaign_id as campaign_id,
	PL.order_id as campaign_id2,
                 PL.buy as campaign2,
                 MT.placement_id                    as placement_code,
		MT.placement_label as placement_name,
		PL.site_placement                 as placement_name2,
                 case when (LEN(ISNULL(MT.placement_label,''))=0) then pl.site_placement
                 else MT.placement_label end        as placement_name,
                 sum(MT.human_impressions)          as total_impressions,
                 sum(MT.half_duration_impressions)  as groupm_passed_impressions,
                 sum(MT.groupm_payable_impressions) as groupm_billable_impressions
             from (select distinct
                       cast(date as date)         as mtDate,
                       campaign_label             as campaign_label,
                       campaign_id                as campaign_id,
                       site_label                 as site_label,
                       site_id                    as site_id,
                       placement_id               as placement_id,
                       placement_label            as placement_label,
                       human_impressions          as human_impressions,
                       half_duration_impressions  as half_duration_impressions,
                       groupm_payable_impressions as groupm_payable_impressions

                   from VerticaGroupM.mec.UnitedUS.moat_impression) as MT

                 LEFT JOIN
                 (
                     SELECT *
                     FROM openQuery(VerticaGroupM,
                                    '
                                    select pg.site_placement, pg.page_id, pg.site_id, pg.order_id, st.directory_site, cp.buy
                                    from (

                                    select  cast(site_placement as varchar(4000)) as "site_placement",  max( page_id) as "page_id", site_id as "site_id", order_id as "order_id"
                                    from mec.UnitedUS.dfa_page_name
                                           group by site_placement, page_id,site_id, order_id
                                         ) as pg

                                    left join (select
                                    cast(directory_site as varchar(4000)) as "directory_site", site_id as "site_id"
                                            from mec.UnitedUS.dfa_site
                                    ) as st
                                    on pg.site_id = st.site_id

                                    left join (select
                                    cast(buy as varchar(4000)) as "buy", order_id as "order_id"
                                            from mec.UnitedUS.dfa_campaign
                                    ) as cp
                                    on pg.order_id = cp.order_id

                                      group by pg.site_placement, pg.page_id,pg.site_id, st.directory_site, pg.order_id, cp.buy'
                     )) as PL
-- 				  on cast(mt.placement_id) = pl.page_id
                     on cast(mt.placement_id as int)=pl.page_id

             where MT.placement_label not like '%[Dd][Oo]%[Nn][Oo][Tt]%[Uu][Ss][Ee]%'
                   and MT.placement_label not like '%[Nn]o%[Tt]racking%'
                   and MT.placement_id!='undefined'
             group by
-- MT.joinKey,
                 MT.mtDate,
                 MT.site_label,
                 MT.campaign_label,
                 MT.placement_id,
                 MT.site_id,
                 pl.directory_site,
				 mt.campaign_id,
				 PL.order_id,
                 pl.site_id,
                 MT.placement_label,
				  PL.buy,
                 PL.site_placement

         ) as t1
    group by
-- t1.joinKey,
        t1.mtDate,
        t1.media_property,
        t1.campaign_name,
        t1.placement_code,
        t1.placement_name
go
