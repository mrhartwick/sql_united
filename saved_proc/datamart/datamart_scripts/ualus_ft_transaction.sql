--====================================================================
-- Author:    Matthew Hartwick
-- Create date:      04 Aug 2017 12:47:20 PM ET
-- Description: {Description}
-- Comments:
--====================================================================
DELETE FROM ualus_ft_transaction
WHERE  ftDate IN (SELECT DISTINCT [Sales_Date]
                  FROM   [DFID061369_FT_Trans_Extracted])

--transform/cast data types and insert cleansed data into the fact table
INSERT INTO ualus_ft_transaction
select cast(Sales_Date as date)                      as ftdate,
      cast(Sales_Timestamp as datetime)                      as ft_timestamp,
       cast(Campaign as nvarchar(1000))   as campaign_name,
       case when isnumeric(Campaign_ID) = 1 then cast(Campaign_ID as int) else 0 end                                     as campaign_id,
       cast(Site as nvarchar(1000))       as site_name,
       case when isnumeric(Site_ID) = 1 then cast(Site_ID as int) else 0 end                                     as site_id,
       cast(placement as nvarchar(1000))       as placement,
       case when isnumeric(Placement_ID) = 1 then cast(Placement_ID as int) else 0 end                                     as placement_id,
       cast(creative as nvarchar(1000))        as creative,
       case when isnumeric(Creative_ID) = 1 then cast(Creative_ID as int) else 0 end                                     as creative_id,
       case when isnumeric(Quantity) = 1 then cast(Quantity as int) else 0 end as quantity,
       case when isnumeric(Sales_Value) = 1 then cast(Sales_Value as decimal(20, 10)) else 0 end as revenue
       cast(currency as nvarchar(3))         as currency,
       cast(Attribution_Method as nvarchar(1000))         as attribution_method,
       cast(Sales_Type as nvarchar(1000))         as sales_type,
       cast(Conversion_Type as nvarchar(1000))         as conversion_type,
       cast(version as nvarchar(1000))         as creative_ver,
       case when isnumeric(Version_ID) = 1 then cast(Version_ID as int) else 0 end as creative_ver_id,
       cast(Spotlight as nvarchar(1000))         as spotlight_name,
       case when isnumeric(Spotlight_ID) = 1 then cast(Spotlight_ID as int) else 0 end as Spotlight_ID,
       cast(Flashtalking_GUID as nvarchar(1000))         as flashtalking_guid,
       case when isnumeric(Time_To_Sale) = 1 then cast(Time_To_Sale as int) else 0 end as time_to_sale,
       cast(keyword as nvarchar(1000))         as keyword,
       cast(city as nvarchar(1000))         as city,
       cast(country as nvarchar(1000))         as country,
       cast(route_1 as nvarchar(3))         as route_1,
       cast(route_2 as nvarchar(3))         as route_2,
       case when dep_date_1 = '--' then null else cast(dep_date_1 as date) end as dep_date_1,
       case when dep_date_2 = '--' then null else cast(dep_date_2 as date) end as dep_date_2,
       cast(cbn_class as nvarchar(1000))         as cbn_class,
       cast(U6 as nvarchar(1000))         as country_code,
       cast(U7 as nvarchar(1000))         as U7,

from   [dbo].[DFID061369_FT_Trans_Extracted]
where  cast(Sales_Date as date) not in (select distinct ftdate
                                    from   ualus_ft_transaction)

SELECT Max(ftDate) ftDate
INTO   #DT
FROM   ualus_ft_transaction
