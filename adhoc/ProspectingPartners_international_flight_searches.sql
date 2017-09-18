select
final.date as "Date"
,final.destination                                           as destination
,final.route_1_destination                                    as "Route1_destination"
,final.route_2_destination                                    as "Route2_destination"
,final.site                                                   as site
,sum(final.led)                                               as leads


from (

        select
           cast(report.date as date)       as "Date"
           ,report.campaign                as campaign
           ,report.campaign_id             as campaign_id
           ,report.site                    as site
           ,report.placement               as placement
           ,report.placement_id            as placement_id
           ,report.traveldate_1            as traveldate_1
           ,case when (length(ISNULL(report.rt_2_dest,''))=0) then report.rt_1_dest
              when report.rt_1_orig = report.rt_2_dest and report.rt_2_orig = report.rt_1_dest then report.rt_1_dest
              when report.rt_2_dest in ('SYD','MEL','PEK','PVG','SHA','CTU','XIY','HKG','EZE','SCL','LCY','LGW','LHR','DUB','GVA','EDI','FRA','HHN','CDG','HNL','KOA','ITO') then report.rt_2_dest
              when report.rt_1_dest in ('SYD','MEL','PEK','PVG','SHA','CTU','XIY','HKG','EZE','SCL','LCY','LGW','LHR','DUB','GVA','EDI','FRA','HHN','CDG','HNL','KOA','ITO') then report.rt_1_dest
              else report.rt_2_dest
              end                          as destination
--            ,case when (length(ISNULL(report.route_2,''))=0) then report.route_1
--             when report.rt_1_orig = report.rt_2_dest and report.rt_2_orig = report.rt_1_dest then report.route_1
--             else report.route_1 || ' / ' || report.route_2
--             end                            as route
--            ,case when report.rt_1_orig = report.rt_2_dest and report.rt_2_orig = report.rt_1_dest then 'round-trip' else 'one-way' end as flighttype
--            ,report.route_1                 as route_1
--            ,report.route_2                 as route_2,
           ,report.rt_1_dest               as route_1_destination
           ,report.rt_2_dest               as route_2_destination
           ,report.rt_1_orig               as route_1_origin
           ,report.rt_2_orig               as route_2_origin
           ,sum(report.vew_led)                           as vew_led
           ,sum(report.clk_led)                           as clk_led
           ,sum(report.vew_led) + sum(report.clk_led)     as led
           ,report.other_data

        from (

                select

                    cast (timestamp_trunc(to_timestamp(conversions.interaction_time / 1000000),'SS') as date ) as "Date"
                   ,campaign.campaign                       as campaign
                   ,conversions.campaign_id                 as campaign_id
                   ,directory.site_dcm                      as site
                   ,conversions.site_id_dcm                 as site_id
                   ,placements.placement                    as placement
                   ,conversions.placement_id                as placement_id
                   ,conversions.other_data                  as other_data

--                    , cast(subString(conversions.other_data,( INSTR(conversions.other_data,'u9=') + 3 ),10) as date) as traveldate_1
                      ,case when regexp_like(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;','') then
                          subString(regexp_substr(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'',2),5,2) || '-' || subString(regexp_substr(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'',2),7,2) || '-' || subString(regexp_substr(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'',2),1,4)
                          when regexp_like(conversions.other_data,'(u9=)(\d\d[/.\-]\d\d[/.\-]\d\d\d\d)\;','') then
                              regexp_substr(conversions.other_data,'(u9=)(\d\d[/.\-]\d\d[/.\-]\d\d\d\d)\;',1,1,'',2) end                                                       as traveldate_1

                          ,case when regexp_like(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                          when regexp_like(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_1_orig
                          ,case when regexp_like(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                          when regexp_like(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_1_dest

                          ,case when regexp_like(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                          when regexp_like(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_2_orig
                          ,case when regexp_like(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                          when regexp_like(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_2_dest
--
--                           ,case when regexp_like(cONVERSIONS.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
--                               regexp_like(cONVERSIONS.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
--                               then regexp_substr(cONVERSIONS.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3) ||' to '|| regexp_substr(cONVERSIONS.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
--
--                           when regexp_like(cONVERSIONS.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') and
--                               regexp_like(cONVERSIONS.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib')
--                               then regexp_substr(cONVERSIONS.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) ||' to '|| regexp_substr(cONVERSIONS.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2)
--
--                           when regexp_like(cONVERSIONS.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
--                               regexp_like(cONVERSIONS.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib')
--                               then regexp_substr(cONVERSIONS.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3) ||' to '|| regexp_substr(cONVERSIONS.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',3)
--
--                           when regexp_like(cONVERSIONS.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') and
--                               regexp_like(cONVERSIONS.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
--                               then regexp_substr(cONVERSIONS.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) ||' to '|| regexp_substr(cONVERSIONS.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
--
--                           end                                                                                                                                         as route_1,
--
--                           ,case when regexp_like(cONVERSIONS.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
--                               regexp_like(cONVERSIONS.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
--                               then regexp_substr(cONVERSIONS.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3) ||' to '|| regexp_substr(cONVERSIONS.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
--
--                           when regexp_like(cONVERSIONS.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') and
--                               regexp_like(cONVERSIONS.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib')
--                               then regexp_substr(cONVERSIONS.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) ||' to '|| regexp_substr(cONVERSIONS.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2)
--
--                           when regexp_like(cONVERSIONS.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
--                               regexp_like(cONVERSIONS.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib')
--                               then regexp_substr(cONVERSIONS.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3) ||' to '|| regexp_substr(cONVERSIONS.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',3)
--
--                           when regexp_like(cONVERSIONS.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') and
--                               regexp_like(cONVERSIONS.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
--                               then regexp_substr(cONVERSIONS.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) ||' to '|| regexp_substr(cONVERSIONS.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
--                           end                                                                                                                                         as route_2


--                    , case when (regexp_substr(conversions.other_data,'(u5=).+?\;',1,1,'ib', 3) = regexp_substr(conversions.other_data,'(u8=).+?\;',1,1,'ib', 3) and
--                    regexp_substr(conversions.other_data,'(u7=).+?\;',1,1,'ib', 3) = regexp_substr(conversions.other_data,'(u6=).+?\;',1,1,'ib', 3))
-- --                                or regexp_like(conversions.other_data,'(u6=)\-\-\;','')
--                    then 'round-trip'
--                      else 'one-way' end                           as flighttype

                      ,sum(case when conversions.activity_id = 1086066 and conversions.conversion_id = 1 then 1 else 0 end) as clk_led

                      ,sum(case when conversions.activity_id = 1086066 and conversions.conversion_id = 2 then 1 else 0 end) as vew_led


                from (

                        select *
                        from diap01.mec_us_united_20056.dfa2_activity
                        where cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date ) between '2017-08-15' and '2017-08-31'


                            and activity_id = '1086066'
                            and campaign_id = 10742878
--                             and total_revenue != 0
                            and advertiser_id <> 0



                     ) as conversions




                   left join diap01.mec_us_united_20056.dfa2_campaigns as campaign
                      on conversions.campaign_id = campaign.campaign_id

left join
(
select  placement,  max(placement_id) as placement_id, campaign_id, site_id_dcm
from diap01.mec_us_united_20056.dfa2_placements
    group by placement, campaign_id, site_id_dcm
) as Placements

ON conversions.placement_id = Placements.placement_id
and conversions.campaign_id = Placements.campaign_id
and conversions.site_id_dcm  = Placements.site_id_dcm



                   left join diap01.mec_us_united_20056.dfa2_sites as directory
                      on conversions.site_id_dcm = directory.site_id_dcm


                group by
--                     cast(conversions.date as date)
--                    ,cast(month(cast(conversions.date as date)) as int)
                    cast (timestamp_trunc(to_timestamp(conversions.interaction_time / 1000000),'SS') as date )
                   ,conversions.campaign_id
                   ,conversions.site_id_dcm
                   ,conversions.placement_id
                   ,conversions.other_data
                   ,placements.placement
                   ,directory.site_dcm
                   ,campaign.campaign

             ) as report

        where report.traveldate_1 is not null
--         and   ((report.rt_1_dest in ('SYD','MEL','PEK','PVG','SHA','CTU','XIY','HKG','EZE','SCL','LCY','LGW','LHR','DUB','GVA','EDI','FRA','HHN','CDG','HNL','KOA','ITO'))
--           or (report.rt_2_dest in ('SYD','MEL','PEK','PVG','SHA','CTU','XIY','HKG','EZE','SCL','LCY','LGW','LHR','DUB','GVA','EDI','FRA','HHN','CDG','HNL','KOA','ITO')))

       and report.placement LIKE '%GEN_INT_PROS_FT%' OR report.placement LIKE '%GEN_DOM_PROS_FT%'

        group by

            cast(report.date as date)
--            ,cast(month(cast(report.date as date)) as int)
           ,report.site
           ,report.campaign_id
           ,report.campaign
           ,report.placement_id
           ,report.placement
           ,report.traveldate_1
            ,report.rt_1_orig
            ,report.rt_1_dest
            ,report.rt_2_orig
            ,report.rt_2_dest
           ,report.other_data

     ) as final
where(

final.destination LIKE '%SYD%'
OR final.destination LIKE '%MEL%'
OR final.destination LIKE '%PEK%'
OR final.destination LIKE '%PVG%'
OR final.destination LIKE'%SHA%'
OR final.destination LIKE'%CTU%'
OR final.destination LIKE'%XIY%'
OR final.destination LIKE'%HKG%'
OR final.destination LIKE'%EZE%'
OR final.destination LIKE'%SCL%'
OR final.destination LIKE'%LCY%'
OR final.destination LIKE'%LGW%'
OR final.destination LIKE'%LHR%'
OR final.destination LIKE'%DUB%'
OR final.destination LIKE'%GVA%'
OR final.destination LIKE'%EDI%'
OR final.destination LIKE'%FRA%'
OR final.destination LIKE'%HHN%'
OR final.destination LIKE'%CDG%'
OR final.destination LIKE'%HNL%'
OR final.destination LIKE'%KOA%'
OR final.destination LIKE'%ITO%')
group by


final.site
,final.destination
,final.route_1_destination
,final.route_2_destination
,final.date
---------------------------------


