--====================================================================
-- Author:    Seetha Srinivasan
-- Create date:      12 Jul 2016 05:55:21 PM ET
-- Description: {Description}
-- Comments:
--====================================================================
--Delete existing records from the Fact Table (Idea is to have rolling 7 days data)
delete from ualus_search_floodlight
where  activity_date in (select distinct cast([activity date/time] as date)
                         from   [dfid041761_ualus_search_floodlight_extracted])

--transform/cast data types and insert cleansed data into the fact table
insert into ualus_search_floodlight
select cast(date as date)                                    as date,
       cast([activity id] as nvarchar(50))                   activity_id,
       cast([activity date/time] as date)                    activity_date,
       cast([placement id] as nvarchar(50))                  placement_id,
       cast([campaign id] as nvarchar(50))                   campaign_id,
       cast([paid search engine account id] as nvarchar(50)) pdsearch_engine_account_id,
       cast([paid search engine account] as nvarchar(1000))  pdsearch_engine_account,
       cast([paid search campaign id] as nvarchar(50))       pdsearch_campaign_id,
       cast([paid search campaign] as nvarchar(1000))        pdsearch_campaign,
       cast([paid search ad id] as nvarchar(50))             pdsearch_adid,
       cast([paid search ad] as nvarchar(1000))              pdsearch_ad,
       cast([paid search keyword id] as nvarchar(50))        pdsearch_keywordid,
       cast([paid search keyword] as nvarchar(1000))         pdsearch_keyword,
       cast([site id (dcm)] as nvarchar(50))                 siteid_dcm,
       cast([site (dcm)] as nvarchar(1000))                  site_dcm,
       cast([paid search advertiser id] as nvarchar(50))     pdsearch_advertiser_id,
       cast([paid search advertiser] as nvarchar(1000))      pdsearch_advertiser,
       cast([paid search ad group id] as nvarchar(50))       pdsearch_adgroup_id,
       cast([paid search ad group] as nvarchar(1000))        pdsearch_adgroup,
       cast([site id (site directory)] as nvarchar(50))      siteid_sitedirectory,
       cast([site (site directory)] as nvarchar(1000))       site_sitedirectory,
       case
         when isnumeric([revenue (string)]) = 1 then cast([revenue (string)] as decimal(38, 2))
         else 0
       end                                                   revenue_string,
       cast([currency (string)] as nvarchar(50))             currency,
       -- cast([pnr_base64encoded (string)] as nvarchar(50))    pnr_base64_string,
       case
         when isnumeric([number of tickets (string)]) = 1 then cast([number of tickets (string)] as int)
         else 0
       end                                                   number_of_tickets,
       cast([origin_1 (string)] as nvarchar(50))             origin_1,
       cast([origin_2 (string)] as nvarchar(50))             origin_2,
       case
         when isnumeric([total conversions]) = 1 then cast([total conversions] as int)
         else 0
       end                                                   total_conversions,
       case
         when isnumeric([total revenue]) = 1 then cast([total revenue] as decimal(38, 2))
         else 0
       end                                                   total_revenue,
       cast([transaction count] as decimal(38, 2))           transaction_count,
       case
         when isnumeric([revenue (number)]) = 1 then cast([revenue (number)] as decimal(38, 2))
         else 0
       end                                                   revenue_number,
       -- cast([pnr_base64encoded (number)] as decimal(38, 2))  pnr_base64_number,
       cast([click-through revenue] as decimal(38, 2))       clickthrough_revenue,
       case
         when isnumeric([click-through conversions]) = 1 then cast([click-through conversions] as int)
         else 0
       end                                                   clickthrough_conversions,
       case
         when isnumeric([click-through transaction count]) = 1 then cast([click-through transaction count] as int)
         else 0
       end                                                   clickthrough_transactioncount,
       acquireid
from   [dbo].[dfid041761_ualus_search_floodlight_extracted]
where  cast([activity date/time] as date) not in (select distinct activity_date
                                                  from   ualus_search_floodlight)

select max(activity_date) activity_date
into   #dt
from   ualus_search_floodlight
