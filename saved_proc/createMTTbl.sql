alter procedure dbo.createMTTbl
as
if OBJECT_ID('master.dbo.MTTable', N'U') is not null
  drop table master.dbo.MTTable;


create table master.dbo.MTTable
(
  joinKey                     varchar(255),
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
    replace(left(t2.placement_name, 6) + '_' +
            case when (t2.media_property like '%[Cc]hicago%[Tt]ribune%' or
                       t2.media_property like '[Tt]ribune_[Ii]nteractive%') then 'ChicagoTribune'
            when (t2.media_property like '[Gg][Dd][Nn]%' or
                  t2.media_property like '[Gg]oogle_[Dd]isplay_[Nn]etwork%') then 'Google'
            when t2.media_property like '%[Aa]dara%' then 'Adara'
            when t2.media_property like '%[Aa]tlantic%' then 'The Atlantic'
            when t2.media_property like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
            when t2.media_property like '%[Cc][Nn][Nn]%' then 'CNN'
            when t2.media_property like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
            when t2.media_property like '%[Ff]orbes%' then 'Forbes'
            when t2.media_property like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
            when t2.media_property like '%[Jj]un%[Gg]roup%' then 'JunGroup'
            when t2.media_property like '%[Mm][Ll][Bb]%' then 'MLB'
            when t2.media_property like '%[Mm]ansueto%' then 'Inc'
            when t2.media_property like '%[Mn][Ss][Nn]%' then 'MSN'
            when t2.media_property like '%[Nn][Bb][Aa]%' then 'NBA'
            when t2.media_property like '%[Nn][Ff][Ll]%' then 'NFL'
            when t2.media_property like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
            when t2.media_property like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
            when t2.media_property like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
            when t2.media_property like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
            when t2.media_property like '%[Pp]riceline%' then 'Priceline'
            when t2.media_property like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
            when t2.media_property like '%[Tt]ap%[Aa]d%' then 'TapAd'
            when t2.media_property like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
            when t2.media_property like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
            when t2.media_property like '%[Ww]all_[Ss]treet_[Jj]ournal%' or
                 t2.media_property like '[Ww][Ss][Jj]' then 'Wall Street Journal'
            when t2.media_property like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
            when t2.media_property like '%[Yy]ahoo%' then 'Yahoo'
            when t2.media_property like '%[Yy]ou%[Tt]ube%' then 'YouTube'
            when t2.media_property like '[Aa]d[Pp]rime%' then 'AdPrime'
            when t2.media_property like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
            when t2.media_property like '[Aa]mobee%' then 'Amobee'
            when t2.media_property like '[Cc]ardlytics%' then 'Cardlytics'
            when t2.media_property like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
            when t2.media_property like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
            when t2.media_property like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
            when t2.media_property like '[Ff]acebook%' then 'Facebook'
            when t2.media_property like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
            when t2.media_property like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
            when t2.media_property like '[Ff]lipboard%' then 'Flipboard'
            when t2.media_property like '[Gg]um_[Gg]um%' then 'Gum Gum'
            when t2.media_property like '[Hh]ulu%' then 'Hulu'
            when t2.media_property like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
            when t2.media_property like '[Ii][Nn][Cc]%' then 'Inc'
            when t2.media_property like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
            when t2.media_property like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
            when t2.media_property like '[Ii]ndependent%' then 'Independent'
            when t2.media_property like '[Kk]ayak%' then 'Kayak'
            when t2.media_property like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
            when t2.media_property like '[Mm]artini_[Mm]edia%' then 'Martini Media'
            when t2.media_property like '[Oo]rbitz%' then 'Orbitz'
            when t2.media_property like '[Ss]kyscanner%' then 'Skyscanner'
            when t2.media_property like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
            when t2.media_property like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
            when t2.media_property like '[Ss]mithsonian%' then 'Smithsonian'
            when t2.media_property like '[Ss]ojern%' then 'Sojern'
            when t2.media_property like '[Ss]pecific_[Mm]edia%' or t2.media_property like '[Vv]iant%' then 'Viant'
            when t2.media_property like '[Ss]potify%' then 'Spotify'
            when t2.media_property like '[Tt]ime%[Ii]nc%' then 'Time Inc'
            when t2.media_property like '[Tt]ony%[As]wards%' then 'TonyAwards'
            when t2.media_property like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
            when t2.media_property like '[Tt]ravelocity%' then 'Travelocity'
            when t2.media_property like '[Tt]riggit%' then 'Triggit'
            when t2.media_property like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
            when t2.media_property like '[Uu]ndertone%' then 'Undertone'
            when t2.media_property like '[Uu]nited%' then 'United'
            when t2.media_property like '[Vv]erve%' then 'VerveMobile'
            when t2.media_property like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
            when t2.media_property like '[Vv]ox%' then 'Vox'
            when t2.media_property like '[Ww]ired%' then 'Wired'
            when t2.media_property like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
            when t2.media_property like '[Xx]ad%' then 'xAd Inc'
            when t2.media_property like '[Yy]ieldbot%' then 'Yieldbot'
            when t2.media_property like '[Yy]u[Mm]e%' then 'YuMe'
            else t2.media_property end
            + '_' + cast(t2.mtDate as varchar(10)), ' ', '') as joinKey,
    t2.mtDate,
    case when (t2.media_property like '%[Cc]hicago%[Tt]ribune%' or
               t2.media_property like '[Tt]ribune_[Ii]nteractive%') then 'ChicagoTribune'
    when (t2.media_property like '[Gg][Dd][Nn]%' or
          t2.media_property like '[Gg]oogle_[Dd]isplay_[Nn]etwork%') then 'Google'
    when t2.media_property like '%[Aa]dara%' then 'Adara'
    when t2.media_property like '%[Aa]tlantic%' then 'The Atlantic'
    when t2.media_property like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
    when t2.media_property like '%[Cc][Nn][Nn]%' then 'CNN'
    when t2.media_property like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
    when t2.media_property like '%[Ff]orbes%' then 'Forbes'
    when t2.media_property like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
    when t2.media_property like '%[Jj]un%[Gg]roup%' then 'JunGroup'
    when t2.media_property like '%[Mm][Ll][Bb]%' then 'MLB'
    when t2.media_property like '%[Mm]ansueto%' then 'Inc'
    when t2.media_property like '%[Mn][Ss][Nn]%' then 'MSN'
    when t2.media_property like '%[Nn][Bb][Aa]%' then 'NBA'
    when t2.media_property like '%[Nn][Ff][Ll]%' then 'NFL'
    when t2.media_property like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
    when t2.media_property like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
    when t2.media_property like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
    when t2.media_property like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
    when t2.media_property like '%[Pp]riceline%' then 'Priceline'
    when t2.media_property like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
    when t2.media_property like '%[Tt]ap%[Aa]d%' then 'TapAd'
    when t2.media_property like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
    when t2.media_property like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
    when t2.media_property like '%[Ww]all_[Ss]treet_[Jj]ournal%' or
         t2.media_property like '[Ww][Ss][Jj]' then 'Wall Street Journal'
    when t2.media_property like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
    when t2.media_property like '%[Yy]ahoo%' then 'Yahoo'
    when t2.media_property like '%[Yy]ou%[Tt]ube%' then 'YouTube'
    when t2.media_property like '[Aa]d[Pp]rime%' then 'AdPrime'
    when t2.media_property like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
    when t2.media_property like '[Aa]mobee%' then 'Amobee'
    when t2.media_property like '[Cc]ardlytics%' then 'Cardlytics'
    when t2.media_property like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
    when t2.media_property like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
    when t2.media_property like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
    when t2.media_property like '[Ff]acebook%' then 'Facebook'
    when t2.media_property like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
    when t2.media_property like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
    when t2.media_property like '[Ff]lipboard%' then 'Flipboard'
    when t2.media_property like '[Gg]um_[Gg]um%' then 'Gum Gum'
    when t2.media_property like '[Hh]ulu%' then 'Hulu'
    when t2.media_property like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
    when t2.media_property like '[Ii][Nn][Cc]%' then 'Inc'
    when t2.media_property like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
    when t2.media_property like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
    when t2.media_property like '[Ii]ndependent%' then 'Independent'
    when t2.media_property like '[Kk]ayak%' then 'Kayak'
    when t2.media_property like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
    when t2.media_property like '[Mm]artini_[Mm]edia%' then 'Martini Media'
    when t2.media_property like '[Oo]rbitz%' then 'Orbitz'
    when t2.media_property like '[Ss]kyscanner%' then 'Skyscanner'
    when t2.media_property like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
    when t2.media_property like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
    when t2.media_property like '[Ss]mithsonian%' then 'Smithsonian'
    when t2.media_property like '[Ss]ojern%' then 'Sojern'
    when t2.media_property like '[Ss]pecific_[Mm]edia%' or t2.media_property like '[Vv]iant%' then 'Viant'
    when t2.media_property like '[Ss]potify%' then 'Spotify'
    when t2.media_property like '[Tt]ime%[Ii]nc%' then 'Time Inc'
    when t2.media_property like '[Tt]ony%[As]wards%' then 'TonyAwards'
    when t2.media_property like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
    when t2.media_property like '[Tt]ravelocity%' then 'Travelocity'
    when t2.media_property like '[Tt]riggit%' then 'Triggit'
    when t2.media_property like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
    when t2.media_property like '[Uu]ndertone%' then 'Undertone'
    when t2.media_property like '[Uu]nited%' then 'United'
    when t2.media_property like '[Vv]erve%' then 'VerveMobile'
    when t2.media_property like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
    when t2.media_property like '[Vv]ox%' then 'Vox'
    when t2.media_property like '[Ww]ired%' then 'Wired'
    when t2.media_property like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
    when t2.media_property like '[Xx]ad%' then 'xAd Inc'
    when t2.media_property like '[Yy]ieldbot%' then 'Yieldbot'
    when t2.media_property like '[Yy]u[Mm]e%' then 'YuMe'
    else t2.media_property end                               as media_property,
    t2.campaign_name                                         as campaign_name,
    t2.placement_code                                        as placement_code,
    t2.placement_name                                        as placement_name,
    sum(t2.total_impressions)                                as total_impressions,
    sum(t2.groupm_passed_impressions)                        as groupm_passed_impressions,
    sum(t2.groupm_billable_impressions)                      as groupm_billable_impressions
  from (


         select
           t1.mtDate                           as mtDate,
           case when
             (LEN(ISNULL(t1.media_property, '')) = 0) then s1.directory_site
           else t1.media_property end          as media_property,
           c1.buy                              as campaign_name,
           t1.campaign_id                      as campaign_id,
           t1.site_id                          as site_id,
           t1.placement_code                   as placement_code,
           case when
             (LEN(ISNULL(t1.placement_name, '')) = 0) then p3.site_placement
           else t1.placement_name end          as placement_name,
           sum(t1.total_impressions)           as total_impressions,
           sum(t1.groupm_passed_impressions)   as groupm_passed_impressions,
           sum(t1.groupm_billable_impressions) as groupm_billable_impressions


         from (

                select
                  -- MT.joinKey as joinKey,
                  MT.mtDate                          as mtDate,


                  MT.site_label                      as media_property,

                  MT.campaign_label                  as campaign_name,


                  --          mt.campaign_id                     as campaign_id1,
                  --          p1.order_id                        as campaign_id2,
                  --          p2.order_id                        as campaign_id3,

                  case when (
                    (LEN(ISNULL(cast(mt.campaign_id as varchar), '')) != 0) and
                    (LEN(ISNULL(cast(mt.campaign_id as varchar), '')) > 6)
                    and (LEN(ISNULL(cast(mt.campaign_id as varchar), '')) < 9)
                  )
                    then mt.campaign_id
                  when (
                    ((LEN(ISNULL(cast(mt.campaign_id as varchar), '')) = 0) or
                     (LEN(ISNULL(cast(mt.campaign_id as varchar), '')) > 9) or
                     (LEN(ISNULL(cast(mt.campaign_id as varchar), '')) < 6)
                    )
                    and (LEN(ISNULL(cast(p1.order_id as varchar), '')) = 0))
                    then p2.order_id
                  when (
                    ((LEN(ISNULL(cast(mt.campaign_id as varchar), '')) = 0) or
                     (LEN(ISNULL(cast(mt.campaign_id as varchar), '')) > 8))
                    and (LEN(ISNULL(cast(p1.order_id as varchar), '')) != 0))
                    then p1.order_id
                  else mt.campaign_id
                  end                                as campaign_id,


                  --                           mt.site_id as site_id1,
                  --                           p1.site_id as site_id2,
                  --                           p2.site_id as site_id3,
                  case when
                    MT.site_label = 'Amobee' then '1853562'
                  when (LEN(ISNULL(cast(mt.site_id as varchar), '')) = 7)
                    then mt.site_id
                  when (LEN(ISNULL(cast(mt.site_id as varchar), '')) < 7) and
                       (LEN(ISNULL(cast(p1.site_id as varchar), '')) = 0)
                    then p2.site_id
                  else p1.site_id
                  end                                as site_id,


                  --                            MT.placement_id                   as placement_code1,
                  --                            p1.page_id                  		as placement_code2,
                  --                            p2.page_id                  		as placement_code3,
                  case when

                    (LEN(ISNULL(cast(mt.placement_id as varchar), '')) < 9)
                    then p2.page_id
                  else p1.page_id
                  end                                as placement_code,

                  MT.placement_label                 as placement_name,
                  -- 				  p1.site_placement                 as placement_name2,
                  -- 				  p2.site_placement                 as placement_name3,

                  --                  case when (LEN(ISNULL(MT.placement_label,''))=0) then s1.site_placement
                  -- else MT.placement_label end        as placement_name,
                  sum(MT.human_impressions)          as total_impressions,
                  sum(MT.half_duration_impressions)  as groupm_passed_impressions,
                  sum(MT.groupm_payable_impressions) as groupm_billable_impressions
                from (select
                        cast(date as date)              as mtDate,
                        campaign_label                  as campaign_label,
                        case when campaign_id = '33809' then '10121649'
                        when campaign_id = '33809' then '10121649'
                        when campaign_id = '34957' then '10276123'
                        when campaign_id = '26315' then '8955169'
                        else campaign_id end            as campaign_id,

                        case when (site_label like '%[Cc]hicago%[Tt]ribune%' or
                                   site_label like '[Tt]ribune_[Ii]nteractive%') then 'ChicagoTribune'
                        when (site_label like '[Gg][Dd][Nn]%' or
                              site_label like '[Gg]oogle_[Dd]isplay_[Nn]etwork%') then 'Google'
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
                        when site_label like '%[Ww]all_[Ss]treet_[Jj]ournal%' or
                             site_label like '[Ww][Ss][Jj]' then 'Wall Street Journal'
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
                        when site_label like '[Ii][Nn][Cc]%' then 'Inc'
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

                        else site_label end             as site_label,
                        --                site_label                      as site_label,
                        site_id                         as site_id,
                        case when placement_id = 'undefined' then 'undefined'
                        when placement_id = '137412609' or placement_id = '300459' or placement_id = '325988' or
                             placement_id = '137412510' then '137412609'
                        else placement_id end           as placement_id,
                        case when placement_label like 'PBKB7J%' or placement_label like 'PBKB7H%' or
                                  placement_label like 'PBKB7K%' or placement_label =
                                                                    'United 360 - Polaris 2016 - Q4 - Amobee' then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
                        else placement_label end        as placement_label,
                        sum(human_impressions)          as human_impressions,
                        sum(half_duration_impressions)  as half_duration_impressions,
                        sum(groupm_payable_impressions) as groupm_payable_impressions

                      from VerticaGroupM.mec.UnitedUS.moat_impression

                      group by
                        cast(date as date),
                        campaign_label,
                        campaign_id,
                        site_label,
                        site_id,
                        placement_id,
                        placement_label,
                        human_impressions,
                        half_duration_impressions,
                        groupm_payable_impressions
                     ) as MT





                  --               Placements 1
                  left join
                  (
                    select *
                    from openQuery(VerticaGroupM,
                                   '
                                   select p1.site_placement, p1.page_id, p1.site_id, p1.order_id
                                   from (

                                   select  cast(site_placement as varchar(4000)) as "site_placement", max( page_id) as "page_id", site_id as "site_id", order_id as "order_id"
                                   from mec.UnitedUS.dfa_page_name
                                   where left(site_placement,6) like ''P%''
                                   group by site_placement,  page_id, site_id, order_id
                                   ) as p1
                                   '
                    )) as p1
                    on cast(mt.placement_id as int) = p1.page_id


                  --               Placements 2
                  left join
                  (
                    select *
                    from openQuery(VerticaGroupM,
                                   '
                                   select p2.site_placement, p2.PlacementNumber, p2.page_id, p2.site_id, p2.order_id
                                   from (

                                   select  cast(site_placement as varchar(4000)) as "site_placement",  left(cast(site_placement as varchar(4000)), 6) as "PlacementNumber", max( page_id) as "page_id", site_id as "site_id", order_id as "order_id"
                                   from mec.UnitedUS.dfa_page_name
                                   where left(site_placement,6) like ''P%''
                                   group by site_placement,  page_id, site_id, order_id
                                   ) as p2
                                   '
                    )) as p2
                    on left(mt.placement_label, 6) = p2.PlacementNumber


                --               MT.placement_label not like '%[Dd][Oo]%[Nn][Oo][Tt]%[Uu][Ss][Ee]%'
                --                    and MT.placement_label not like '%[Nn]o%[Tt]racking%'
                where MT.placement_id != 'undefined'
                group by
                  -- MT.joinKey,
                  MT.mtDate,
                  MT.site_label,
                  MT.campaign_label,
                  MT.placement_id,
                  MT.site_id,
                  --                  p1.directory_site,
                  mt.campaign_id,
                  p1.order_id,
                  p2.order_id,
                  p1.site_id,
                  p2.site_id,
                  p1.page_id,
                  p2.page_id,
                  MT.placement_label
                -- 				  p1.buy,
                --     p1.site_placement,
                --     p2.site_placement
              ) as t1

           --               Campaigns
           left join
           (
             select *
             from openQuery(VerticaGroupM,
                            '
                            select c1.order_id, c1.buy
                            from (

                            select
                            cast(buy as varchar(4000)) as "buy", order_id as "order_id"
                                    from mec.UnitedUS.dfa_campaign
                            ) as c1

                            group by c1.order_id, c1.buy'
             )) as c1
             on cast(t1.campaign_id as int) = c1.order_id


           --               Sites 2
           left join
           (
             select *
             from openQuery(VerticaGroupM,
                            '
                            select s1.site_id, s1.directory_site
                            from (

                            select
                            cast(directory_site as varchar(4000)) as "directory_site", site_id as "site_id"
                                    from mec.UnitedUS.dfa_site
                            ) as s1

                            group by s1.directory_site, s1.site_id'
             )) as s1
             on cast(t1.site_id as int) = s1.site_id

           -- Placements 3
           left join
           (
             select *
             from openQuery(VerticaGroupM,
                            '
                            select p3.site_placement, p3.page_id
                            from (

                            select  cast(site_placement as varchar(4000)) as "site_placement", max( page_id) as "page_id"
                            from mec.UnitedUS.dfa_page_name
                            where left(site_placement,6) like ''P%''
                            group by site_placement,  page_id
                            ) as p3
                            '
             )) as p3
             on cast(t1.placement_code as int) = p3.page_id

         group by
           t1.mtDate,
           t1.media_property,
           s1.directory_site,
           t1.campaign_id,
           t1.site_id,
           t1.placement_code,
           c1.buy,
           p3.site_placement,
           t1.placement_name

       ) as t2
  group by
    -- t2.joinKey,
    t2.mtDate,
    t2.media_property,
    t2.campaign_name,
    t2.placement_code,
    t2.placement_name
go