--====================================================================
-- Author:    Matthew Hartwick
-- Create date:      8 August 2017
-- Description: {Description}
-- Comments:
--====================================================================
--Delete existing records from the Fact Table (Idea is to have rolling 7 days data)
delete from [ualus_dv_sum]
where  date in (select distinct cast([date] as date)
                from   [dbo].[DFID060180_FTP_Extracted])

--transform/cast data types and insert cleansed data into the fact table
insert into [ualus_dv_sum]
select cast(date as date)                                    date,
       cast([Campaign Name] as nvarchar(1000))       campaign_name,
       cast([Media Property] as nvarchar(1000))       media_property,
       cast([Placement Name] as nvarchar(1000))        placement_name,
       cast([Ad Server Placement Code] as nvarchar(100))        placement_code,
       case
         when (len(isnull([GroupM Active Impressions],''))=0) then 0
         when isnumeric([GroupM Active Impressions]) = 1 then cast([GroupM Active Impressions] as bigint)
         else 0
       end                                                   gm_active_impressions,

       case
          when (len(isnull([GroupM Passed Impressions],''))=0) then 0
         when isnumeric([GroupM Passed Impressions]) = 1 then cast([GroupM Passed Impressions] as bigint)
         else 0
         end                                                   gm_passed_impressions,
         case
       when (len(isnull([GroupM Billable Impressions],''))=0) then 0
         when isnumeric([GroupM Billable Impressions]) = 1 then cast([GroupM Billable Impressions] as bigint)
         else 0
       end                                                   gm_billable_impressions
from   [dbo].[DFID060180_FTP_Extracted]
where  cast([date] as date) not in (select distinct [date]
                                    from   ualus_dv_sum)

select max([date]) activitydate
into   #dt
from   ualus_dv_sum

drop table #dt