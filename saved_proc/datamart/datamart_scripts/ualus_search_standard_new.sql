--====================================================================
-- author:      seetha srinivasan
-- create date:      13 jul 2016 08:38:36 pm et
-- Edited:           19 July 2017 by Matt Hartwick
-- description: {description}
-- comments:
--====================================================================
--delete existing records from the fact table (idea is to have rolling 7 days data)
delete from [ualus_search_standard]
where  date in (select distinct cast([date] as date)
                from   [dbo].[dfid041780_ualus_search_standard_extracted])

--transform/cast data types and insert cleansed data into the fact table
insert into [ualus_search_standard]
select cast([date] as date)                                  as   date,
       cast([week] as date)                                  as [week],
       cast([paid search engine account id] as nvarchar(50)) as PdSearch_EngineAccount_ID,
       cast([paid search campaign id] as nvarchar(50))       as PdSearch_Campaign_ID,
       cast([paid search keyword id] as nvarchar(50))        as PdSearch_Keyword_ID,
       cast([paid search ad group id] as nvarchar(50))       as PdSearch_AdGroup_ID,
       cast([paid search ad id] as nvarchar(50))             as PdSearch_Ad_ID,
       cast([paid search advertiser id] as nvarchar(50))     as PdSearch_Advertiser_ID,
       cast([paid search match type] as nvarchar(50))        as PdSearch_MatchType,
       cast([site id (dcm)] as nvarchar(50))                 as SiteID_DCM,
       cast([site id (site directory)] as nvarchar(50))      as SiteID_SiteDirectory,
       cast([campaign id] as nvarchar(50))                   as Campaign_ID,
       cast([placement id] as nvarchar(50))                  as Placement_ID,
       case when isnumeric([paid search impressions]) = 1 then cast([paid search impressions] as bigint) else 0 end                       as PdSearch_Impressions,
       case when isnumeric([paid search clicks]) = 1 then cast([paid search clicks] as bigint) else 0 end                                 as PdSearch_Clicks,
       case when isnumeric([paid search cost]) = 1 then cast([paid search cost] as decimal(38, 2)) else 0 end                             as PdSearch_Cost,
       case when isnumeric([paid search revenue]) = 1 then cast([paid search revenue] as decimal(38, 2)) else 0 end                       as PdSearch_Revenue,
       case when isnumeric([paid search average position]) = 1 then cast([paid search average position] as decimal(38, 2)) else 0 end     as PdSearch_Avg_Position,
       case when isnumeric([paid search transactions]) = 1 then cast([paid search transactions] as decimal(38, 2)) else 0 end             as PdSearch_Transactions,
       acquireid
from   [dbo].[dfid041780_ualus_search_standard_extracted]
where  cast([date] as date) not in (select distinct [date]
                                    from   ualus_search_standard)

select max([date]) activitydate
into   #dt
from   ualus_search_standard
