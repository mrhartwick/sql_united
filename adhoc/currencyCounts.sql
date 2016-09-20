select cast(report.date as date)                       as dcmDate,
       cast(month(cast(report.date as date)) as int)   as reportMonth,
       campaign.buy                                    as Buy,
       report.order_id                                 as order_id,
       -- report.other_data                               as other_data,
       report.site_id                                  as Site_ID,
       directory.directory_site                        as Directory_Site,
--     COALESCE(report.currency,'blank') as currency,
--         IFNULL(report.currency,'blank') as currency,
       report.currency as currency,
--     report.index as index,
       left(placements.site_placement, 6)              as 'PlacementNumber',
       placements.site_placement                       as Site_Placement,
       report.page_id                                  as page_id,
       sum(report.impressions)                         as impressions,
       sum(report.clicks)                              as clicks,
       sum(report.conv)                                as Conv,
       sum(report.tickets)                             as Tickets,
       sum(cast(report.revenue as decimal(10, 2)))     as revenue,
       sum(cast(report.revenue_raw as decimal(10, 2))) as revenue_raw

from   (select
cast(conversions.click_time as date)           as "Date",
               order_id                                       as order_id,
--                other_data                                     as other_data,
               case
               	when instr(other_data, 'u3=') = 0
               	then 'blank' else upper(substring(other_data, ( instr(other_data, 'u3=') + 3 ), 3)) end as currency,
--                   COALESCE(upper(substring(other_data, ( instr(other_data, 'u3=') + 3 ), 3)),'blank') as currency,
--     upper(substring(other_data, ( instr(other_data, 'u3=') + 3 ), 3)) as currency,
--             instr(other_data, 'u3=') as index,
               conversions.site_id                            as Site_ID,
               conversions.page_id                            as page_id,
               0                                              as Impressions,
               0                                              as Clicks,
               sum(1)                                         as Conv,
               sum(conversions.quantity)                      as Tickets,
               sum(conversions.revenue)                       as Revenue_raw,
               sum(conversions.revenue / rates.exchange_rate) as Revenue
        from   (select *
                from   mec.unitedus.dfa_activity
                where  ( cast(click_time as date) between
                         '2016-06-01' and '2016-09-20'
                       )
                       -- 								 and UPPER(SUBSTRING(Other_Data, (INSTR(Other_Data,'u3=')+3), 3)) != 'MIL'
                       -- 								 and SUBSTRING(Other_Data, (INSTR(Other_Data,'u3=')+3), 5) != 'Miles'
                       -- and revenue != 0
                       -- and quantity != 0
                       and ( activity_type = 'ticke498' )
                       and ( activity_sub_type = 'unite820' )
                       and order_id in ( 9304728, 9407915, 9408733, 9548151, 9630239, 9639387, 9739006, 9923634, 9973506, 9994694, 9999841, 10094548, 10121649, 10090315 ) -- Display 2016
                       and ( advertiser_id <> 0 )) as conversions
               left join mec.cross_client_resources.exchange_rates as rates
                      on upper(substring(other_data, ( instr(other_data, 'u3=') + 3 ), 3 )) = upper(rates.currency)
                         and cast(conversions.click_time as date) = rates.date
        group  by
        -- Conversions.Click_Time
        cast(conversions.click_time as date),
        conversions.order_id,
        conversions.other_data,
        conversions.site_id,
        conversions.page_id
        union all
        select
        -- Impressions.impression_time as "Date"
        cast(impressions.impression_time as date) as "Date",
        impressions.order_id                      as order_id,
--         ''                                        as other_data,
        ''                                        as currency,
--     0 as index,
        impressions.site_id                       as Site_ID,
        impressions.page_id                       as page_id,
        count(*)                                  as Impressions,
        0                                         as Clicks,
        0                                         as Conv,
        0                                         as Tickets,
        0                                         as Revenue,
        0                                         as Revenue_Raw
        from   (select *
                from   mec.unitedus.dfa_impression
                where  cast(impression_time as date) between
                       '2016-06-01' and '2016-09-20'
                       and order_id in ( 9304728, 9407915, 9408733, 9548151, 9630239, 9639387, 9739006, 9923634, 9973506, 9994694, 9999841, 10094548, 10121649, 10090315 ) -- Display 2016
               ) as impressions
        group by
        -- Impressions.impression_time
        cast(impressions.impression_time as date),
        impressions.order_id,
        impressions.site_id,
        impressions.page_id
        union all
        select
        -- Clicks.click_time       as "Date"
        cast(clicks.click_time as date) as "Date",
        clicks.order_id                 as order_id,
--         ''                              as other_data,
        ''                                        as currency,
--     0 as index,
        clicks.site_id                  as Site_ID,
        clicks.page_id                  as page_id,
        0                               as Impressions,
        count(*)                        as Clicks,
        0                               as Conv,
        0                               as Tickets,
        0                               as Revenue,
        0                               as Revenue_Raw
        from   (select *
                from   mec.unitedus.dfa_click
                where  cast(click_time as date) between
                       '2016-06-01' and '2016-09-20'
                       and order_id in ( 9304728, 9407915, 9408733, 9548151, 9630239, 9639387, 9739006, 9923634, 9973506, 9994694, 9999841, 10094548, 10121649, 10090315 ) -- Display 2016
               ) as clicks
        group by
        -- Clicks.Click_time
        cast(clicks.click_time as date),
        clicks.order_id,
        clicks.site_id,
        clicks.page_id) as report
       left join (
                 -- 			     SELECT *
                 select cast(buy as varchar(4000)) as 'buy',
                        order_id                   as 'order_id'
                  from   mec.unitedus.dfa_campaign) as campaign
              on report.order_id = campaign.order_id
       left join (
                 -- 			     SELECT *
                 select cast(site_placement as varchar(4000)) as
                        'site_placement',
                        max(page_id)                          as 'page_id',
                        order_id                              as 'order_id',
                        site_id                               as 'site_id'
                  from   mec.unitedus.dfa_page_name
                  group by site_placement,
                            order_id,
                            site_id) as placements
              on report.page_id = placements.page_id
                 and report.order_id = placements.order_id
                 and report.site_id = placements.site_id
       left join (
                 -- 			     SELECT *
                 select cast(directory_site as varchar(4000)) as
                        'directory_site',
                        site_id                               as 'site_id'
                  from   mec.unitedus.dfa_site) as directory
              on report.site_id = directory.site_id
where not regexp_like(site_placement, '.do\s*not\s*use.', 'ib')
group by cast(report.date as date)
          -- , cast(month(cast(Report.Date as date)) as int)
          ,
          directory.directory_site,
          report.site_id,
          -- report.other_data,
          report.currency,
          report.order_id,
          campaign.buy,
--      report.index ,
          report.page_id,
          placements.site_placement
-- , Placements.PlacementNumber