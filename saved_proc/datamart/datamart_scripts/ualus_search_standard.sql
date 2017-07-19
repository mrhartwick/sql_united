--====================================================================
-- author:      seetha srinivasan
-- create date:      13 jul 2016 08:38:36 pm et
-- description: {description}
-- comments:
--====================================================================
--delete existing records from the fact table (idea is to have rolling 7 days data)
delete from [ualus_search_standard]
where  date in (select distinct cast([date] as date)
                from   [dbo].[dfid041780_ualus_search_standard_extracted])

--transform/cast data types and insert cleansed data into the fact table
insert into [ualus_search_standard]
select cast(date as date)                                    date,
       cast([week] as date)                                  [week],
       cast([paid search engine account id] as nvarchar(50)) pdsearchengine_accountid,
       cast([paid search campaign id] as nvarchar(50))       pdsearch_campaignid,
       cast([paid search keyword id] as nvarchar(50))        pdsearch_keywordid,
       cast([paid search ad group id] as nvarchar(50))       pdsearch_adgroupid,
       cast([paid search ad id] as nvarchar(50))             pdsearch_adid,
       cast([paid search advertiser id] as nvarchar(50))     pdsearch_advertiserid,
       cast([paid search match type] as nvarchar(50))        pdsearch_matchtype,
       cast([site id (dcm)] as nvarchar(50))                 siteid_dcm,
       cast([site id (site directory)] as nvarchar(50))      siteid_sitedir,
       cast([campaign id] as nvarchar(50))                   campaignid,
       cast([package/roadblock id] as nvarchar(50))          packageid,
       cast([placement id] as nvarchar(50))                  placementid,
       case
         when isnumeric([placement total booked units]) = 1 then cast([placement total booked units] as bigint)
         else 0
       end                                                   totalbookedunits,
       case
         when isnumeric([paid search impressions]) = 1 then cast([paid search impressions] as bigint)
         else 0
       end                                                   impressions,
       case
         when isnumeric([paid search clicks]) = 1 then cast([paid search clicks] as bigint)
         else 0
       end                                                   clicks,
       case
         when isnumeric([paid search cost]) = 1 then cast([paid search cost] as decimal(38, 2))
         else 0
       end                                                   search_cost,
       case
         when isnumeric([paid search revenue]) = 1 then cast([paid search revenue] as decimal(38, 2))
         else 0
       end                                                   search_revenue,
       case
         when isnumeric([paid search click rate]) = 1 then cast([paid search click rate] as decimal(38, 2))
         else 0
       end                                                   click_rate,
       case
         when isnumeric([paid search average position]) = 1 then cast([paid search average position] as decimal(38, 2))
         else 0
       end                                                   avg_postn,
       case
         when isnumeric([paid search transactions]) = 1 then cast([paid search transactions] as decimal(38, 2))
         else 0
       end                                                   transactions,
       case
         when isnumeric([paid search actions]) = 1 then cast([paid search actions] as decimal(38, 2))
         else 0
       end                                                   actions,
       case
         when isnumeric([paid search visits]) = 1 then cast([paid search visits] as bigint)
         else 0
       end                                                   totalvisits,
       acquireid
from   [dbo].[dfid041780_ualus_search_standard_extracted]
where  cast([date] as date) not in (select distinct [date]
                                    from   ualus_search_standard)

select max([date]) activitydate
into   #dt
from   ualus_search_standard
