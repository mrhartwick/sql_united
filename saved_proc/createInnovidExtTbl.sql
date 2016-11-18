ALTER procedure dbo.createInnovidExtTbl
as


if OBJECT_ID('DM_1161_UnitedAirlinesUSA.dbo.innovidExtTable',N'U') is not null
    drop table dbo.innovidExtTable;
create table dbo.innovidExtTable
(
    joinKey                 varchar(255),
    ivDate                  date,
    campaign_name           varchar(1000),
    campaign_id             int,
    publisher               varchar(1000),
    publisher_id            int,
    placement_name          varchar(1000),
    impressions             int,
    click_thrus             int,
    engagement_events       int,
    twfive_completion       int,
    fifty_completion        int,
    svfive_completion       int,
    all_completion          int,
    time_earned             float,
    pctad_viewed            float,
    slate_open_by_click     int,
    slate_open_by_rollover  int,
    total_slate_open_events int,
    in_unit_clicks          int
);

insert into dbo.innovidExtTable

    select
        case when placement_name like '%United_360%Amobee%' then replace('PBKB7J'+'_'+
                case when (publisher like '%[Cc]hicago%[Tt]ribune%' or publisher like '[Tt]ribune_[Ii]nteractive%') then 'ChicagoTribune'
                when (publisher like '[Gg][Dd][Nn]%' or publisher like '[Gg]oogle_[Dd]isplay_[Nn]etwork%') then 'Google'
                when publisher like '%[Aa]dara%' then 'Adara'
                when publisher like '%[Aa]tlantic%' then 'The Atlantic'
                when publisher like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
                when publisher like '%[Cc][Nn][Nn]%' then 'CNN'
                when publisher like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
                when publisher like '%[Ff]orbes%' then 'Forbes'
                when publisher like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
                when publisher like '%[Jj]un%[Gg]roup%' then 'JunGroup'
                when publisher like '%[Mm][Ll][Bb]%' then 'MLB'
                when publisher like '%[Mm]ansueto%' then 'Inc'
                when publisher like '%[Mn][Ss][Nn]%' then 'MSN'
                when publisher like '%[Nn][Bb][Aa]%' then 'NBA'
                when publisher like '%[Nn][Ff][Ll]%' then 'NFL'
                when publisher like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
                when publisher like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
                when publisher like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
                when publisher like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
                when publisher like '%[Pp]riceline%' then 'Priceline'
                when publisher like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
                when publisher like '%[Tt]ap%[Aa]d%' then 'TapAd'
                when publisher like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
                when publisher like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
                when publisher like '%[Ww]all_[Ss]treet_[Jj]ournal%' or publisher like '[Ww][Ss][Jj]' then 'Wall Street Journal'
                when publisher like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
                when publisher like '%[Yy]ahoo%' then 'Yahoo'
                when publisher like '%[Yy]ou%[Tt]ube%' then 'YouTube'
                when publisher like '[Aa]d[Pp]rime%' then 'AdPrime'
                when publisher like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
                when publisher like '[Aa]mobee%' then 'Amobee'
                when publisher like '[Cc]ardlytics%' then 'Cardlytics'
                when publisher like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
                when publisher like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
                when publisher like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
                when publisher like '[Ff]acebook%' then 'Facebook'
                when publisher like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
                when publisher like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
                when publisher like '[Ff]lipboard%' then 'Flipboard'
                when publisher like '[Gg]um_[Gg]um%' then 'Gum Gum'
                when publisher like '[Hh]ulu%' then 'Hulu'
                when publisher like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
                when publisher like '[Ii][Nn][Cc]%' then 'Inc'
                when publisher like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
                when publisher like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
                when publisher like '[Ii]ndependent%' then 'Independent'
                when publisher like '[Kk]ayak%' then 'Kayak'
                when publisher like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
                when publisher like '[Mm]artini_[Mm]edia%' then 'Martini Media'
                when publisher like '[Oo]rbitz%' then 'Orbitz'
                when publisher like '[Ss]kyscanner%' then 'Skyscanner'
                when publisher like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
                when publisher like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
                when publisher like '[Ss]mithsonian%' then 'Smithsonian'
                when publisher like '[Ss]ojern%' then 'Sojern'
                when publisher like '[Ss]pecific_[Mm]edia%' or publisher like '[Vv]iant%' then 'Viant'
                when publisher like '[Ss]potify%' then 'Spotify'
                when publisher like '[Tt]ime%[Ii]nc%' then 'Time Inc'
                when publisher like '[Tt]ony%[As]wards%' then 'TonyAwards'
                when publisher like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
                when publisher like '[Tt]ravelocity%' then 'Travelocity'
                when publisher like '[Tt]riggit%' then 'Triggit'
                when publisher like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
                when publisher like '[Uu]ndertone%' then 'Undertone'
                when publisher like '[Uu]nited%' then 'United'
                when publisher like '[Vv]erve%' then 'VerveMobile'
                when publisher like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
                when publisher like '[Vv]ox%' then 'Vox'
                when publisher like '[Ww]ired%' then 'Wired'
                when publisher like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
                when publisher like '[Xx]ad%' then 'xAd Inc'
                when publisher like '[Yy]ieldbot%' then 'Yieldbot'
                when publisher like '[Yy]u[Mm]e%' then 'YuMe'
                else publisher end
                +'_'+cast(date as varchar(10)),' ','')

else
replace(left(placement_name,6)+'_'+
                case when (publisher like '%[Cc]hicago%[Tt]ribune%' or publisher like '[Tt]ribune_[Ii]nteractive%') then 'ChicagoTribune'
                when (publisher like '[Gg][Dd][Nn]%' or publisher like '[Gg]oogle_[Dd]isplay_[Nn]etwork%') then 'Google'
                when publisher like '%[Aa]dara%' then 'Adara'
                when publisher like '%[Aa]tlantic%' then 'The Atlantic'
                when publisher like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
                when publisher like '%[Cc][Nn][Nn]%' then 'CNN'
                when publisher like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
                when publisher like '%[Ff]orbes%' then 'Forbes'
                when publisher like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
                when publisher like '%[Jj]un%[Gg]roup%' then 'JunGroup'
                when publisher like '%[Mm][Ll][Bb]%' then 'MLB'
                when publisher like '%[Mm]ansueto%' then 'Inc'
                when publisher like '%[Mn][Ss][Nn]%' then 'MSN'
                when publisher like '%[Nn][Bb][Aa]%' then 'NBA'
                when publisher like '%[Nn][Ff][Ll]%' then 'NFL'
                when publisher like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
                when publisher like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
                when publisher like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
                when publisher like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
                when publisher like '%[Pp]riceline%' then 'Priceline'
                when publisher like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
                when publisher like '%[Tt]ap%[Aa]d%' then 'TapAd'
                when publisher like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
                when publisher like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
                when publisher like '%[Ww]all_[Ss]treet_[Jj]ournal%' or publisher like '[Ww][Ss][Jj]' then 'Wall Street Journal'
                when publisher like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
                when publisher like '%[Yy]ahoo%' then 'Yahoo'
                when publisher like '%[Yy]ou%[Tt]ube%' then 'YouTube'
                when publisher like '[Aa]d[Pp]rime%' then 'AdPrime'
                when publisher like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
                when publisher like '[Aa]mobee%' then 'Amobee'
                when publisher like '[Cc]ardlytics%' then 'Cardlytics'
                when publisher like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
                when publisher like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
                when publisher like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
                when publisher like '[Ff]acebook%' then 'Facebook'
                when publisher like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
                when publisher like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
                when publisher like '[Ff]lipboard%' then 'Flipboard'
                when publisher like '[Gg]um_[Gg]um%' then 'Gum Gum'
                when publisher like '[Hh]ulu%' then 'Hulu'
                when publisher like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
                when publisher like '[Ii][Nn][Cc]%' then 'Inc'
                when publisher like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
                when publisher like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
                when publisher like '[Ii]ndependent%' then 'Independent'
                when publisher like '[Kk]ayak%' then 'Kayak'
                when publisher like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
                when publisher like '[Mm]artini_[Mm]edia%' then 'Martini Media'
                when publisher like '[Oo]rbitz%' then 'Orbitz'
                when publisher like '[Ss]kyscanner%' then 'Skyscanner'
                when publisher like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
                when publisher like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
                when publisher like '[Ss]mithsonian%' then 'Smithsonian'
                when publisher like '[Ss]ojern%' then 'Sojern'
                when publisher like '[Ss]pecific_[Mm]edia%' or publisher like '[Vv]iant%' then 'Viant'
                when publisher like '[Ss]potify%' then 'Spotify'
                when publisher like '[Tt]ime%[Ii]nc%' then 'Time Inc'
                when publisher like '[Tt]ony%[As]wards%' then 'TonyAwards'
                when publisher like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
                when publisher like '[Tt]ravelocity%' then 'Travelocity'
                when publisher like '[Tt]riggit%' then 'Triggit'
                when publisher like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
                when publisher like '[Uu]ndertone%' then 'Undertone'
                when publisher like '[Uu]nited%' then 'United'
                when publisher like '[Vv]erve%' then 'VerveMobile'
                when publisher like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
                when publisher like '[Vv]ox%' then 'Vox'
                when publisher like '[Ww]ired%' then 'Wired'
                when publisher like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
                when publisher like '[Xx]ad%' then 'xAd Inc'
                when publisher like '[Yy]ieldbot%' then 'Yieldbot'
                when publisher like '[Yy]u[Mm]e%' then 'YuMe'
                else publisher end
                +'_'+cast(date as varchar(10)),' ','') end
as joinKey,
        date                                           as ivDate,
        campaign_name,
        campaign_id,
        case when (publisher like '%[Cc]hicago%[Tt]ribune%' or publisher like '[Tt]ribune_[Ii]nteractive%') then 'ChicagoTribune'
        when (publisher like '[Gg][Dd][Nn]%' or publisher like '[Gg]oogle_[Dd]isplay_[Nn]etwork%') then 'Google'
        when publisher like '%[Aa]dara%' then 'Adara'
        when publisher like '%[Aa]tlantic%' then 'The Atlantic'
        when publisher like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
        when publisher like '%[Cc][Nn][Nn]%' then 'CNN'
        when publisher like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
        when publisher like '%[Ff]orbes%' then 'Forbes'
        when publisher like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
        when publisher like '%[Jj]un%[Gg]roup%' then 'JunGroup'
        when publisher like '%[Mm][Ll][Bb]%' then 'MLB'
        when publisher like '%[Mm]ansueto%' then 'Inc'
        when publisher like '%[Mn][Ss][Nn]%' then 'MSN'
        when publisher like '%[Nn][Bb][Aa]%' then 'NBA'
        when publisher like '%[Nn][Ff][Ll]%' then 'NFL'
        when publisher like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
        when publisher like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
        when publisher like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
        when publisher like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
        when publisher like '%[Pp]riceline%' then 'Priceline'
        when publisher like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
        when publisher like '%[Tt]ap%[Aa]d%' then 'TapAd'
        when publisher like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
        when publisher like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
        when publisher like '%[Ww]all_[Ss]treet_[Jj]ournal%' or publisher like '[Ww][Ss][Jj]' then 'Wall Street Journal'
        when publisher like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
        when publisher like '%[Yy]ahoo%' then 'Yahoo'
        when publisher like '%[Yy]ou%[Tt]ube%' then 'YouTube'
        when publisher like '[Aa]d[Pp]rime%' then 'AdPrime'
        when publisher like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
        when publisher like '[Aa]mobee%' then 'Amobee'
        when publisher like '[Cc]ardlytics%' then 'Cardlytics'
        when publisher like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
        when publisher like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
        when publisher like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
        when publisher like '[Ff]acebook%' then 'Facebook'
        when publisher like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
        when publisher like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
        when publisher like '[Ff]lipboard%' then 'Flipboard'
        when publisher like '[Gg]um_[Gg]um%' then 'Gum Gum'
        when publisher like '[Hh]ulu%' then 'Hulu'
        when publisher like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
        when publisher like '[Ii][Nn][Cc]%' then 'Inc'
        when publisher like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
        when publisher like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
        when publisher like '[Ii]ndependent%' then 'Independent'
        when publisher like '[Kk]ayak%' then 'Kayak'
        when publisher like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
        when publisher like '[Mm]artini_[Mm]edia%' then 'Martini Media'
        when publisher like '[Oo]rbitz%' then 'Orbitz'
        when publisher like '[Ss]kyscanner%' then 'Skyscanner'
        when publisher like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
        when publisher like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
        when publisher like '[Ss]mithsonian%' then 'Smithsonian'
        when publisher like '[Ss]ojern%' then 'Sojern'
        when publisher like '[Ss]pecific_[Mm]edia%' or publisher like '[Vv]iant%' then 'Viant'
        when publisher like '[Ss]potify%' then 'Spotify'
        when publisher like '[Tt]ime%[Ii]nc%' then 'Time Inc'
        when publisher like '[Tt]ony%[As]wards%' then 'TonyAwards'
        when publisher like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
        when publisher like '[Tt]ravelocity%' then 'Travelocity'
        when publisher like '[Tt]riggit%' then 'Triggit'
        when publisher like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
        when publisher like '[Uu]ndertone%' then 'Undertone'
        when publisher like '[Uu]nited%' then 'United'
        when publisher like '[Vv]erve%' then 'VerveMobile'
        when publisher like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
        when publisher like '[Vv]ox%' then 'Vox'
        when publisher like '[Ww]ired%' then 'Wired'
        when publisher like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
        when publisher like '[Xx]ad%' then 'xAd Inc'
        when publisher like '[Yy]ieldbot%' then 'Yieldbot'
        when publisher like '[Yy]u[Mm]e%' then 'YuMe'
        else publisher end                             as publisher,
        publisher_id,

      case when placement_name like 'PBKB7J%' or placement_name like 'PBKB7H%' or placement_name like 'PBKB7K%' or placement_name ='United 360 - Polaris 2016 - Q4 - Amobee'        then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID' else  placement_name end     as placement_name,
        sum(impressions),
        sum(click_thrus),
       sum(engagement_events),
        sum(twfive_completion),
        sum(fifty_completion),
        sum(svfive_completion),
        sum(all_completion),
        sum(time_earned),
        sum(pctad_viewed),
        sum(slate_open_by_click),
        sum(slate_open_by_rollover),
        sum(total_slate_open_events),
        sum(in_unit_clicks)
    from DM_1161_UnitedAirlinesUSA.dbo.innovidTable

    group by
        date,
        campaign_name,
        campaign_id,
        publisher,
        publisher_id,
        placement_name
go