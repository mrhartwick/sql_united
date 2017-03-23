alter FUNCTION [dbo].udf_siteKey (
	@site_name varchar(4000)
)
RETURNS  varchar(4000)
    WITH EXECUTE AS CALLER
AS
    begin
		declare @finalSiteName varchar(4000)
		set @finalSiteName = replace(case when ( @site_name like '%[Cc]hicago%[Tt]ribune%' or @site_name like '[Tt]ribune_[Ii]nteractive%' ) then 'ChicagoTribune'
				when ( @site_name like '[Gg][Dd][Nn]%' or @site_name like '[Gg]oogle_[Dd]isplay_[Nn]etwork%' ) then 'Google'
				when @site_name like '%[Aa]dara%' then 'Adara'
				when @site_name like '%[Aa]tlantic%' then 'The Atlantic'
				when @site_name like '%[Bb]usiness_[Ii]nsider%' then 'Business Insider'
				when @site_name like '%[Cc][Nn][Nn]%' then 'CNN'
				when @site_name like '%[Ee][Ss][Pp][Nn]%' then 'ESPN'
				when @site_name like '%[Ff]orbes%' then 'Forbes'
				when @site_name like '%[Gg]olf%[Dd]igest%' then 'GolfDigest'
				when @site_name like '%[Jj]un%[Gg]roup%' then 'JunGroup'
				when @site_name like '%[Mm][Ll][Bb]%' then 'MLB'
				when @site_name like '%[Mm]ansueto%' then 'Inc'
				when @site_name like '%[Mn][Ss][Nn]%' then 'MSN'
				when @site_name like '%[Nn][Bb][Aa]%' then 'NBA'
				when @site_name like '%[Nn][Ff][Ll]%' then 'NFL'
				when @site_name like '%[Nn]ast_[Tt]raveler%' then 'CN Traveler'
				when @site_name like '%[Nn]ew_[Yy]ork_[Tt]imes%' then 'NYTimes'
				when @site_name like '%[Nn]ew_[Yy]orker%' then 'New Yorker'
				when @site_name like '%[Pp]eople%' and @site_name like '%[Ee]spa[nñ]ol%' then 'People En Espanol'
				when @site_name like '%[Pp][Gg][Aa]%[Tt][Oo][Uu][Rr]%' then 'PGATour'
				when @site_name like '%[Pp]riceline%' then 'Priceline'
				when @site_name like '%[Ss]ports_[Ii]llustrated%' then 'Sports Illustrated'
				when @site_name like '%[Tt]ap%[Aa]d%' then 'TapAd'
				when @site_name like '%[Tt]ime%[Oo]ut%' then 'Time Out New York'
				when @site_name like '%[Tt]ravel%[Ll]eisure%' then 'TravelLeisure'
				when @site_name like '%[Ww]all_[Ss]treet_[Jj]ournal%'  or @site_name like '[Ww][Ss][Jj]' then 'Wall Street Journal'
				when @site_name like '%[Ww]ashington_[Pp]ost%' then 'Washington Post'
				when @site_name like '%[Yy]ahoo%' then 'Yahoo'
				when @site_name like '%[Yy]ou%[Tt]ube%' then 'YouTube'
				when @site_name like '[Aa]d[Pp]rime%' then 'AdPrime'
				when @site_name like '[Aa]ds[Mm]ovil%' then 'AdsMovil'
				when @site_name like '[Aa]mobee%' then 'Amobee'
				when @site_name like '[Cc]ardlytics%' then 'Cardlytics'
				when @site_name like '[Dd][Aa][Rr][Tt]_Search%Google' then 'DART Search_Google'
				when @site_name like '[Dd][Aa][Rr][Tt]_Search%MSN' then 'DART Search_MSN'
				when @site_name like '[Dd][Aa][Rr][Tt]_Search%Other' then 'DART Search_Other'
				when @site_name like '[Ff]acebook%' then 'Facebook'
				when @site_name like '[Ff]ast%[Cc]ompany%' then 'Fast Company'
				when @site_name like '[Ff]inancial%[Tt]imes%' then 'FinancialTimes'
			    when @site_name like '[Ff]lipboard%' then 'Flipboard'
				when @site_name like '[Gg]um_[Gg]um%' then 'Gum Gum'
				when @site_name like '[Hh]ulu%' then 'Hulu'
				when @site_name like '[Ii][Nn][Vv][Ii][Tt][Ee]%[Mm][Ee][Dd][Ii][Aa]%' then 'Invite Media'
				when @site_name like '[Ii][Nn][Cc]%' then 'Inc'
				when @site_name like '[Ii]mpre%[Mm]edia%' then 'Impre Media'
				when @site_name like '[Ii]nternet%[Bb]rands%' then 'Internet Brands'
				when @site_name like '[Ii]ndependent%' then 'Independent'
				when @site_name like '[Kk]ayak%' then 'Kayak'
				when @site_name like '[Ll]ive%[Ii]ntent%' then 'LiveIntent'
				when @site_name like '[Mm]artini_[Mm]edia%' then 'Martini Media'
				when @site_name like '[Oo]rbitz%' then 'Orbitz'
				when @site_name like '[Ss]kyscanner%' then 'Skyscanner'
				when @site_name like '[Ss]mart%[Bb]r[ei][ei]f%' then 'SmartBrief'
				when @site_name like '[Ss]marter%[Tt]ravel%' then 'Trip Advisor'
				when @site_name like '[Ss]mithsonian%' then 'Smithsonian'
				when @site_name like '[Ss]ojern%' then 'Sojern'
				when @site_name like '[Ss]pecific_[Mm]edia%' or @site_name like '[Vv]iant%' then 'Viant'
				when @site_name like '[Ss]potify%' then 'Spotify'
				when @site_name like '[Tt]ime%[Ii]nc%' then 'Time Inc'
				when @site_name like '[Tt]ony%[As]wards%' then 'TonyAwards'
				when @site_name like '[Tt]ravel%[Ss]pike%' then 'Travel Spike'
				when @site_name like '[Tt]ravelocity%' then 'Travelocity'
				when @site_name like '[Tt]riggit%' then 'Triggit'
				when @site_name like '[Tt]rip%[Aa]dvisor%' then 'Trip Advisor'
				when @site_name like '%[Uu]ber%' then 'United'
				when @site_name like '%[Uu]ndertone%' then 'Undertone'
				when @site_name like '[Uu]nited%' then 'United'
				when @site_name like '[Vv]erve%' then 'VerveMobile'
				when @site_name like '[Vv]istar%[Mm]edia%' then 'VistarMedia'
				when @site_name like '[Vv]ox%' then 'Vox'
				when @site_name like '[Ww]ired%' then 'Wired'
				when @site_name like '[Xx][Aa][Xx][Ii][Ss]%' then 'Xaxis'
				when @site_name like '[Xx]ad%' then 'xAd Inc'
				when @site_name like '[Yy]ieldbot%' then 'Yieldbot'
				when @site_name like '[Yy]u[Mm]e%' then 'YuMe'
				else @site_name end,' ','');



RETURN @finalSiteName
end