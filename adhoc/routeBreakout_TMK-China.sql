select

final.month as "month"
,final.campaign                                               as campaign
-- ,final.campaign_id                                            as campaign_id
-- ,final.site                                                   as site
-- ,LEFT(Placements.Site_Placement, 6)                           as ParentID
-- ,final.placement                                              as placement
-- ,final.placement_id                                           as placement_id
-- ,case when length(final.route) > 10 then 'round-trip' else  final.flighttype end as flighttype
-- ,final.flighttype
,final.route
,final.route_1
,final.route_2
-- ,final.other_data
-- ,cast(final.date as date)                                     as purch_date
-- ,cast(final.traveldate_1                                      as date) as travel_date
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
           ,report.buy_id                  as campaign_id
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

                     cast(conversions.Click_Time as date) as "Date"
                   , cast(month(cast(conversions.Click_Time as date)) as int) as "Month"
                   , campaign.buy                            as campaign
                   , conversions.order_id                    as buy_id
                   , directory.directory_site                as site
                   , conversions.site_id                     as site_id
                   , placements.site_placement               as placement
                   , conversions.page_id                     as placement_id
                   ,conversions.other_data

--                    , cast(subString(conversions.other_data,( INSTR(conversions.other_data,'u9=') + 3 ),10) as date) as traveldate_1
                     ,case when regexp_like(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;','')
                           then subString(regexp_substr(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'', 2),5,2) || '-' || subString(regexp_substr(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'', 2),7,2) || '-' || subString(regexp_substr(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'', 2),1,4)
                           when regexp_like(conversions.other_data,'(u9=)(\d\d[/.\-]\d\d[/.\-]\d\d\d\d)\;','')
                           then regexp_substr(conversions.other_data,'(u9=)(\d\d[/.\-]\d\d[/.\-]\d\d\d\d)\;',1,1,'', 2) end as traveldate_1

--                   Break out each origin/destination into separate fields so that we can use them deterministically later
                     ,case when regexp_like(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
                           when regexp_like(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as rt_1_orig
                     ,case when regexp_like(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
                           when regexp_like(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as rt_1_dest

                     ,case when regexp_like(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
                           when regexp_like(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as rt_2_orig
                     ,case when regexp_like(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)
                           when regexp_like(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) end as rt_2_dest

--                   Route 1
--                   when both (first) routes have parenthesis
                     ,case when regexp_like(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                           and regexp_like(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                           then regexp_substr(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3) ||' to '|| regexp_substr(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)

      --                   when neither (first) routes have parenthesis
                           when regexp_like(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib')
                           and regexp_like(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib')
                           then regexp_substr(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) ||' to '|| regexp_substr(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2)

      --                   when "():" origin yes, destination no
                           when regexp_like(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                           and regexp_like(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib')
                           then regexp_substr(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3) ||' to '|| regexp_substr(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 3)

      --                   when "():" origin no, destination yes
                           when regexp_like(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib')
                           and regexp_like(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                           then regexp_substr(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) ||' to '|| regexp_substr(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)

                           end                                     as route_1

--                   Route 2
--                   when both (second) routes have parenthesis
                     , case when  regexp_like(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
                                  regexp_like(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                     then regexp_substr(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3) ||' to '|| regexp_substr(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)

--                   when neither (second) routes have parenthesis
                     when regexp_like(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') and
                                  regexp_like(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib')
                     then regexp_substr(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) ||' to '|| regexp_substr(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2)

--                   when "():" origin yes, destination no
                     when  regexp_like(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
                                   regexp_like(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib')
                     then regexp_substr(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3) ||' to '|| regexp_substr(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 3)

--                   when "():" origin no, destination yes
                     when  regexp_like(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') and
                                  regexp_like(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                     then regexp_substr(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib', 2) ||' to '|| regexp_substr(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib', 3)

                     end                                     as route_2


--                    , case when (regexp_substr(conversions.other_data,'(u5=).+?\;',1,1,'ib', 3) = regexp_substr(conversions.other_data,'(u8=).+?\;',1,1,'ib', 3) and
--                    regexp_substr(conversions.other_data,'(u7=).+?\;',1,1,'ib', 3) = regexp_substr(conversions.other_data,'(u6=).+?\;',1,1,'ib', 3))
-- --                                or regexp_like(conversions.other_data,'(u6=)\-\-\;','')
--                    then 'round-trip'
--                      else 'one-way' end                           as flighttype

                   , sum(case when event_id = 1 then 1 else 0 end) as click_thru_conv
                   , sum(case when event_id = 1 then conversions.quantity else 0 end) as click_thru_tickets
                   , sum(case when event_id = 2 then 1 else 0 end) as view_thru_conv
                   , sum(case when event_id = 2 then conversions.quantity else 0 end) as view_thru_tickets

                from (

                        select *
                        from mec.unitedus.dfa_activity
                        where cast(Click_Time as date) between '2016-04-18' and '2016-11-30'

                              and activity_type = 'ticke498'
                              and activity_sub_type = 'unite820'
                            and revenue != 0
                        and quantity != 0
                              and order_id in (9639387) -- Intl 2016

                              and advertiser_id <> 0

                                   and    UPPER(subString(other_data,( INSTR(other_data,'u3=') + 3 ),3)) != 'MIL'
                      and subString(other_data,( INSTR(other_data,'u3=') + 3 ),5) != 'Miles'

                     ) as conversions

--         LEFT JOIN Cross_Client_Resources.EXCHANGE_RATES AS Rates
--                 ON UPPER(subString(Other_Data, (INSTR(Other_Data,'u3=')+3), 3)) = UPPER(Rates.Currency)
--                 AND cast(Conversions.Click_Time as date) = Rates.DATE


                   left join mec.unitedus.dfa_campaign as campaign
                      on conversions.order_id = campaign.order_id

left join
(
select  site_placement as site_placement,  max(page_id) as page_id, order_id as order_id, site_id as site_id
from mec.UnitedUS.dfa_page_name
    group by site_placement, order_id, site_id
) as Placements

ON conversions.page_id = Placements.page_id
and conversions.order_id = Placements.order_id
and conversions.site_ID  = placements.site_id

--                    left join mec.unitedus.dfa_page_name as placements
--                       on conversions.page_id = placements.page_id
--                          and conversions.order_id = placements.order_id

                   left join mec.unitedus.dfa_site as directory
                      on conversions.site_id = directory.site_id

--                 where placements.site_placement like '%Strategy%'

--                       Optionally remove Miles transactions

--                        and

                group by cast(conversions.Click_Time as date)
                   ,conversions.order_id
                   ,conversions.site_id
                   ,conversions.page_id
                   ,conversions.other_data
                   ,placements.site_placement
                   ,directory.directory_site
                   ,campaign.buy

             ) as report

        where report.traveldate_1 is not null
        and ((report.rt_1_orig in ('AAT','ACX','AKA','AKU','AOG','AQG','AVA','AYN','BAV','BFU','BHY','BPX','BSD','CAN','CGD','CGO','CGQ','CHG','CHW','CIF','CIH','CKG','CNI','CSX','CTU','CZX','DAT','DAX','DDG','DGM','DIG','DLC','DLU','DNH','DOY','DSN','DYG','DZU','ENH','FOC','FUD','FUG','FUO','FYN','GHN','GOQ','GYS','HAK','HEK','HET','HFE','HGH','HHA','HJJ','HKG','HLD','HLH','HMI','HNY','HRB','HSC','HSN','HTN','HUZ','HYN','HZG','HZH','INC','IQM','IQN','JDZ','JGN','JGS','JHG','JIL','JIU','JJN','JMU','JNG','JNZ','JUZ','JZH','KCA','KHG','KHN','KMG','KOW','KRL','KRY','KWE','KWL','LCX','LHK','LHW','LIA','LJG','LLF','LNJ','LUM','LUZ','LXA','LXI','LYA','LYG','LYI','LZD','LZH','LZO','MDG','MXZ','NAO','NAY','NDG','NGB','NKG','NNG','NNY','NTG','NZH','PEK','PNJ','PVG','PZI','RUG','SHA','SHE','SHF','SHP','SHS','SIA','SJW','SWA','SXJ','SYM','SYX','SZO','SZV','SZX','TAO','TCG','TEN','TGO','TNA','TNH','TSN','TXN','TYN','URC','UYN','WEF','WEH','WHU','WNZ','WUA','WUH','WUS','WUX','WUZ','WXN','XEN','XFN','XIC','XIL','XIN','XIY','XMN','XNN','XNT','XTA','XTF','XUZ','YBP','YIH','YIN','YIW','YLN','YNJ','YNT','YNZ','YUA','ZAS','ZAT','ZAX','ZBD','ZBZ','ZCA','ZCP','ZCU','ZEF','ZGC','ZGN','ZHA','ZIY','ZMY','ZTI','ZUH','ZYI','ZYK'))
          or (report.rt_1_dest in ('AAT','ACX','AKA','AKU','AOG','AQG','AVA','AYN','BAV','BFU','BHY','BPX','BSD','CAN','CGD','CGO','CGQ','CHG','CHW','CIF','CIH','CKG','CNI','CSX','CTU','CZX','DAT','DAX','DDG','DGM','DIG','DLC','DLU','DNH','DOY','DSN','DYG','DZU','ENH','FOC','FUD','FUG','FUO','FYN','GHN','GOQ','GYS','HAK','HEK','HET','HFE','HGH','HHA','HJJ','HKG','HLD','HLH','HMI','HNY','HRB','HSC','HSN','HTN','HUZ','HYN','HZG','HZH','INC','IQM','IQN','JDZ','JGN','JGS','JHG','JIL','JIU','JJN','JMU','JNG','JNZ','JUZ','JZH','KCA','KHG','KHN','KMG','KOW','KRL','KRY','KWE','KWL','LCX','LHK','LHW','LIA','LJG','LLF','LNJ','LUM','LUZ','LXA','LXI','LYA','LYG','LYI','LZD','LZH','LZO','MDG','MXZ','NAO','NAY','NDG','NGB','NKG','NNG','NNY','NTG','NZH','PEK','PNJ','PVG','PZI','RUG','SHA','SHE','SHF','SHP','SHS','SIA','SJW','SWA','SXJ','SYM','SYX','SZO','SZV','SZX','TAO','TCG','TEN','TGO','TNA','TNH','TSN','TXN','TYN','URC','UYN','WEF','WEH','WHU','WNZ','WUA','WUH','WUS','WUX','WUZ','WXN','XEN','XFN','XIC','XIL','XIN','XIY','XMN','XNN','XNT','XTA','XTF','XUZ','YBP','YIH','YIN','YIW','YLN','YNJ','YNT','YNZ','YUA','ZAS','ZAT','ZAX','ZBD','ZBZ','ZCA','ZCP','ZCU','ZEF','ZGC','ZGN','ZHA','ZIY','ZMY','ZTI','ZUH','ZYI','ZYK'))
          or (report.rt_2_orig in ('AAT','ACX','AKA','AKU','AOG','AQG','AVA','AYN','BAV','BFU','BHY','BPX','BSD','CAN','CGD','CGO','CGQ','CHG','CHW','CIF','CIH','CKG','CNI','CSX','CTU','CZX','DAT','DAX','DDG','DGM','DIG','DLC','DLU','DNH','DOY','DSN','DYG','DZU','ENH','FOC','FUD','FUG','FUO','FYN','GHN','GOQ','GYS','HAK','HEK','HET','HFE','HGH','HHA','HJJ','HKG','HLD','HLH','HMI','HNY','HRB','HSC','HSN','HTN','HUZ','HYN','HZG','HZH','INC','IQM','IQN','JDZ','JGN','JGS','JHG','JIL','JIU','JJN','JMU','JNG','JNZ','JUZ','JZH','KCA','KHG','KHN','KMG','KOW','KRL','KRY','KWE','KWL','LCX','LHK','LHW','LIA','LJG','LLF','LNJ','LUM','LUZ','LXA','LXI','LYA','LYG','LYI','LZD','LZH','LZO','MDG','MXZ','NAO','NAY','NDG','NGB','NKG','NNG','NNY','NTG','NZH','PEK','PNJ','PVG','PZI','RUG','SHA','SHE','SHF','SHP','SHS','SIA','SJW','SWA','SXJ','SYM','SYX','SZO','SZV','SZX','TAO','TCG','TEN','TGO','TNA','TNH','TSN','TXN','TYN','URC','UYN','WEF','WEH','WHU','WNZ','WUA','WUH','WUS','WUX','WUZ','WXN','XEN','XFN','XIC','XIL','XIN','XIY','XMN','XNN','XNT','XTA','XTF','XUZ','YBP','YIH','YIN','YIW','YLN','YNJ','YNT','YNZ','YUA','ZAS','ZAT','ZAX','ZBD','ZBZ','ZCA','ZCP','ZCU','ZEF','ZGC','ZGN','ZHA','ZIY','ZMY','ZTI','ZUH','ZYI','ZYK'))
          or (report.rt_2_dest in ('AAT','ACX','AKA','AKU','AOG','AQG','AVA','AYN','BAV','BFU','BHY','BPX','BSD','CAN','CGD','CGO','CGQ','CHG','CHW','CIF','CIH','CKG','CNI','CSX','CTU','CZX','DAT','DAX','DDG','DGM','DIG','DLC','DLU','DNH','DOY','DSN','DYG','DZU','ENH','FOC','FUD','FUG','FUO','FYN','GHN','GOQ','GYS','HAK','HEK','HET','HFE','HGH','HHA','HJJ','HKG','HLD','HLH','HMI','HNY','HRB','HSC','HSN','HTN','HUZ','HYN','HZG','HZH','INC','IQM','IQN','JDZ','JGN','JGS','JHG','JIL','JIU','JJN','JMU','JNG','JNZ','JUZ','JZH','KCA','KHG','KHN','KMG','KOW','KRL','KRY','KWE','KWL','LCX','LHK','LHW','LIA','LJG','LLF','LNJ','LUM','LUZ','LXA','LXI','LYA','LYG','LYI','LZD','LZH','LZO','MDG','MXZ','NAO','NAY','NDG','NGB','NKG','NNG','NNY','NTG','NZH','PEK','PNJ','PVG','PZI','RUG','SHA','SHE','SHF','SHP','SHS','SIA','SJW','SWA','SXJ','SYM','SYX','SZO','SZV','SZX','TAO','TCG','TEN','TGO','TNA','TNH','TSN','TXN','TYN','URC','UYN','WEF','WEH','WHU','WNZ','WUA','WUH','WUS','WUX','WUZ','WXN','XEN','XFN','XIC','XIL','XIN','XIY','XMN','XNN','XNT','XTA','XTF','XUZ','YBP','YIH','YIN','YIW','YLN','YNJ','YNT','YNZ','YUA','ZAS','ZAT','ZAX','ZBD','ZBZ','ZCA','ZCP','ZCU','ZEF','ZGC','ZGN','ZHA','ZIY','ZMY','ZTI','ZUH','ZYI','ZYK')))




          )

        group by

           cast(report.date as date)
            ,report.Month
           ,report.site
           ,report.buy_id
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

-- cast(final.date as date)
final.month
-- ,final.site
-- ,final.campaign_id
,final.campaign
-- ,final.placement_id
-- ,final.placement
-- ,final.flighttype
,final.route
-- ,final.traveldate_1
,final.route_1
,final.route_2
-- ,final.other_data
-- ,final.thisField
