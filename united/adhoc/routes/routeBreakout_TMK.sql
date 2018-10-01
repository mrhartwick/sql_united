select

final.month                                                  as  Month
-- ,final.campaign                                               as campaign
-- ,final.campaign_id                                            as campaign_id
,final.site                                                   as site
-- ,LEFT(Placements.Site_Placement, 6)                           as ParentID
-- ,final.placement                                              as placement
-- ,final.placement_id                                           as placement_id
-- ,case when length(final.route) > 10 then 'round-trip' else  final.flighttype end as flighttype
-- ,final.flighttype
,final.route
-- ,final.other_data
,cast(final.date as date)                                     as purch_date
,cast(final.traveldate_1                                      as date) as travel_date
-- ,datediff(day, cast(final.date as date), cast(final.traveldate_1 as date) ) as adv_purch_win
-- ,sum(final.view_thru_conv)                                    as "view-through transactions"
-- ,sum(final.click_thru_conv)                                   as "click-through transactions"
,sum(final.conv) as "transactions"
-- ,sum(final.view_thru_conv) + sum(final.click_thru_conv)       as "transactions"
-- ,sum(final.view_thru_tickets)                                 as "view-through tickets"
-- ,sum(final.click_thru_tickets)                                as "click-through tickets"
-- ,sum(final.view_thru_tickets) + sum(final.click_thru_tickets) as "tickets"

,sum(final.tickets) as tickets

from (

        select

            cast(report.date as date)      as "Date"
           ,report.Month as "Month"
           ,report.campaign                as campaign
           ,report.campaign_id             as campaign_id
           ,report.site                    as site
           ,report.placement               as placement
           ,report.placement_id            as placement_id
           ,report.traveldate_1            as traveldate_1
           ,case when (length(ISNULL(report.route_2,''))=0) then report.route_1
            when report.rt_1_orig = report.rt_2_dest and report.rt_2_orig = report.rt_1_dest then report.route_1
            else report.route_1 || ' / ' || report.route_2
            end                            as route
           ,case when report.rt_1_orig = report.rt_2_dest and report.rt_2_orig = report.rt_1_dest then 'round-trip' else 'one-way' end as flighttype
           ,report.route_1                 as route_1
           ,report.route_2                 as route_2
           ,sum(report.view_thru_conv)     as view_thru_conv
           ,sum(report.click_thru_conv)    as click_thru_conv
		   ,sum(report.view_thru_conv) + sum(report.click_thru_conv) as conv
           ,sum(report.view_thru_tickets)  as view_thru_tickets
           ,sum(report.click_thru_tickets) as click_thru_tickets
		   ,sum(report.view_thru_tickets) + sum(report.click_thru_tickets) as tickets
           ,report.other_data





        from (

                select

                     cast (timestamp_trunc(to_timestamp(conversions.interaction_time / 1000000),'SS') as date ) as "Date"
                   , cast (timestamp_trunc(to_timestamp(conversions.interaction_time / 1000000),'MM') as date ) as "Month"
                   , campaign.campaign                            as campaign
                   , conversions.campaign_id                    as campaign_id
                   , directory.site_dcm                as site
                   , conversions.site_id_dcm                     as site_id
                   , placements.site_placement               as placement
                   , conversions.placement_id                    as placement_id
                   ,conversions.other_data


--                    , cast(subString(conversions.other_data,( INSTR(conversions.other_data,'u9=') + 3 ),10) as date) as traveldate_1
                     ,case when REGEXP_LIKE(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;','') then
                     subString(REGEXP_SUBSTR(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'', 2),5,2) || '-' || subString(REGEXP_SUBSTR(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'', 2),7,2) || '-' || subString(REGEXP_SUBSTR(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'', 2),1,4)
                     when REGEXP_LIKE(conversions.other_data,'(u9=)(\d\d[/.\-]\d\d[/.\-]\d\d\d\d)\;','') then
                     REGEXP_SUBSTR(conversions.other_data,'(u9=)(\d\d[/.\-]\d\d[/.\-]\d\d\d\d)\;',1,1,'', 2) end as traveldate_1

--                   Break out each origin/destination into separate fields so that we can use them deterministically later
                     ,case when REGEXP_LIKE(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then REGEXP_SUBSTR(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
                           when REGEXP_LIKE(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') then REGEXP_SUBSTR(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as rt_1_orig
                     ,case when REGEXP_LIKE(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then REGEXP_SUBSTR(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
                           when REGEXP_LIKE(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib') then REGEXP_SUBSTR(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as rt_1_dest

                     ,case when REGEXP_LIKE(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then REGEXP_SUBSTR(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
                           when REGEXP_LIKE(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') then REGEXP_SUBSTR(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as rt_2_orig
                     ,case when REGEXP_LIKE(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then REGEXP_SUBSTR(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
                           when REGEXP_LIKE(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib') then REGEXP_SUBSTR(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as rt_2_dest

--                   Route 1
--                   when both (first) routes have parenthesis
                     , case when  REGEXP_LIKE(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
                                  REGEXP_LIKE(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                     then REGEXP_SUBSTR(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3) ||' to '|| REGEXP_SUBSTR(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)

--                   when neither (first) routes have parenthesis
                     when REGEXP_LIKE(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') and
                                  REGEXP_LIKE(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib')
                     then REGEXP_SUBSTR(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) ||' to '|| REGEXP_SUBSTR(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2)

--                   when "():" origin yes, destination no
                     when  REGEXP_LIKE(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
                                   REGEXP_LIKE(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib')
                     then REGEXP_SUBSTR(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3) ||' to '|| REGEXP_SUBSTR(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 3)

--                   when "():" origin no, destination yes
                     when  REGEXP_LIKE(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') and
                                  REGEXP_LIKE(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                     then REGEXP_SUBSTR(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) ||' to '|| REGEXP_SUBSTR(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)

                     end                                     as route_1

--                   Route 2
--                   when both (second) routes have parenthesis
                     , case when  REGEXP_LIKE(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
                                  REGEXP_LIKE(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                     then REGEXP_SUBSTR(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3) ||' to '|| REGEXP_SUBSTR(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)

--                   when neither (second) routes have parenthesis
                     when REGEXP_LIKE(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') and
                                  REGEXP_LIKE(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib')
                     then REGEXP_SUBSTR(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) ||' to '|| REGEXP_SUBSTR(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2)

--                   when "():" origin yes, destination no
                     when  REGEXP_LIKE(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
                                   REGEXP_LIKE(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib')
                     then REGEXP_SUBSTR(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3) ||' to '|| REGEXP_SUBSTR(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 3)

--                   when "():" origin no, destination yes
                     when  REGEXP_LIKE(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') and
                                  REGEXP_LIKE(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                     then REGEXP_SUBSTR(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) ||' to '|| REGEXP_SUBSTR(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)

                     end                                     as route_2


--                    , case when (REGEXP_SUBSTR(conversions.other_data,'(u5=).+?\;',1,1,'ib', 3) = REGEXP_SUBSTR(conversions.other_data,'(u8=).+?\;',1,1,'ib', 3) and
--                    REGEXP_SUBSTR(conversions.other_data,'(u7=).+?\;',1,1,'ib', 3) = REGEXP_SUBSTR(conversions.other_data,'(u6=).+?\;',1,1,'ib', 3))
-- --                                or REGEXP_LIKE(conversions.other_data,'(u6=)\-\-\;','')
--                    then 'round-trip'
--                      else 'one-way' end                           as flighttype

                   , sum(case when conversion_id = 1 then 1 else 0 end) as click_thru_conv
                   , sum(case when conversion_id = 1 then conversions.total_conversions else 0 end) as click_thru_tickets
                   , sum(case when conversion_id = 2 then 1 else 0 end) as view_thru_conv
                   , sum(case when conversion_id = 2 then conversions.total_conversions else 0 end) as view_thru_tickets

                from (

                        select *
                        from diap01.mec_us_united_20056.dfa2_activity
                        where cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date ) between '2017-01-01' and '2017-03-21'

						and activity_id = 978826
                            and total_revenue <> 0
                        and total_conversions <> 0
                              and campaign_id in (10742878) -- TMK 2017


                              and advertiser_id <> 0

					and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')

                     ) as conversions

--         LEFT JOIN Cross_Client_Resources.EXCHANGE_RATES AS Rates
--                 ON UPPER(subString(Other_Data, (INSTR(Other_Data,'u3=')+3), 3)) = UPPER(Rates.Currency)
--                 and cast (timestamp_trunc(to_timestamp(conversions.interaction_time / 1000000),'SS') as date ) = rates.date




                   left join diap01.mec_us_united_20056.dfa2_campaigns as campaign
                      on conversions.campaign_id = campaign.campaign_id

left join
(
select  site_placement as site_placement,  max(page_id) as page_id, order_id as campaign_id, site_id as site_id
from diap01.mec_us_united_20056.dfa_page
		group by site_placement, order_id, site_id
) as Placements

ON conversions.placement_id= Placements.page_id
and conversions.campaign_id = Placements.campaign_id
and conversions.site_id_dcm  = placements.site_id

--                    left join diap01.mec_us_united_20056.dfa_page as placements
--                       on conversions.placement_id= placements.page_id
--                          and conversions.campaign_id = placements.campaign_id

                   left join diap01.mec_us_united_20056.dfa2_sites as directory
                      on conversions.site_id_dcm = directory.site_id_dcm

--                 where placements.site_placement like '%Strategy%'

--                       Optionally remove Miles transactions

--                        and

                group by cast (timestamp_trunc(to_timestamp(conversions.interaction_time / 1000000),'SS') as date )
                  ,cast (timestamp_trunc(to_timestamp(conversions.interaction_time / 1000000),'MM') as date )
                   ,conversions.campaign_id
                   ,conversions.site_id_dcm
                   ,conversions.placement_id
                   ,conversions.other_data
                   ,placements.site_placement
                   ,directory.site_dcm
                   ,campaign.campaign



             ) as report

        where report.traveldate_1 is not null

        group by

           cast(report.date as date)
            ,report.Month
           ,report.site
           ,report.campaign_id
           ,report.campaign
           ,report.placement_id
           ,report.placement
           ,report.traveldate_1
--            ,report.flighttype
           ,report.route_1
           ,report.route_2
            ,report.rt_1_orig
            ,report.rt_1_dest
            ,report.rt_2_orig
            ,report.rt_2_dest
--     ,Report.Origin_1
--     ,Report.Destination_1
--     ,Report.TravelDate_2
--     ,Report.Origin_2
--     ,Report.Destination_2
--             ,report.Month
           ,report.other_data
--     ,report.thisField

     ) as final

group by

cast(final.date as date)
,final.month
,final.site
-- ,final.campaign_id
,final.campaign
-- ,final.placement_id
-- ,final.placement
-- ,final.flighttype
,final.route
,final.traveldate_1
-- ,final.route_1
-- ,final.route_2
-- ,final.other_data
-- ,final.thisField
