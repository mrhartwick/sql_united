select
  cast(final.date as date) as purch_date,
  final.month              as "month",
  final.campaign           as campaign
-- ,final.campaign_id                                            as campaign_id
  ,
  final.site               as site
  ,
  final.placement          as placement,
  final.placement_id       as placement_id
-- ,case when length(final.route) > 10 then 'round-trip' else  final.flighttype end as flighttype
-- ,final.flighttype
  ,
  final.route,
    final.class
--   final.route_1,
--   final.route_2
-- ,final.other_data

-- ,cast(final.traveldate_1                                      as date) as travel_date
-- ,datediff(day, cast(final.date as date), cast(final.traveldate_1 as date) ) as adv_purch_win
-- ,sum(final.view_thru_conv)                                    as "view-through transactions"
-- ,sum(final.click_thru_conv)                                   as "click-through transactions"
  ,
  sum(final.con)           as "transactions"
-- ,sum(final.view_thru_conv) + sum(final.click_thru_conv)       as "transactions"
-- ,sum(final.view_thru_tickets)                                 as "view-through tickets"
-- ,sum(final.click_thru_tickets)                                as "click-through tickets"
-- ,sum(final.view_thru_tickets) + sum(final.click_thru_tickets) as "tickets"

  ,
  sum(final.tix)           as tickets

from (

         select

             cast(report.date as date)                as "Date",
             report.Month                             as "Month",
             report.campaign                          as campaign,
             report.buy_id                            as campaign_id,
             report.site                              as site,
             report.placement                         as placement,
             report.placement_id                      as placement_id,
             report.traveldate_1                      as traveldate_1,
             case when (length(ISNULL(report.route_2,'')) = 0) then report.route_1
             when report.rt_1_orig = report.rt_2_dest and report.rt_2_orig = report.rt_1_dest then report.route_1
             else report.route_1 || ' / ' || report.route_2
             end                                      as route,
             case when report.rt_1_orig = report.rt_2_dest and report.rt_2_orig = report.rt_1_dest
                 then 'round-trip' else 'one-way' end as flighttype,
             report.route_1                           as route_1,
             report.route_2                           as route_2,
             sum(report.con)                          as con,
             sum(report.tix)                          as tix,
             report.class

       from (

                select
                    cast(conversions.Click_Time as date)                                                                as "Date",
                    cast(month (cast(conversions.Click_Time as date)) as int)                                           as "Month",
                    campaign.buy                                                                                        as campaign,
                    conversions.order_id                                                                                as buy_id,
                    directory.directory_site                                                                            as site,
                    conversions.site_id                                                                                 as site_id,
                    placements.site_placement                                                                           as placement,
                    conversions.page_id                                                                                 as placement_id
--                    , cast(subString(conversions.other_data,( INSTR(conversions.other_data,'u9=') + 3 ),10) as date) as traveldate_1
                    ,
                    case when regexp_like(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;','')

                        then subString(regexp_substr(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'',2),5,2) || '-' || subString(regexp_substr(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'',2),7,2) || '-' || subString(regexp_substr(conversions.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'',2),1,4)
                    when regexp_like(conversions.other_data,'(u9=)(\d\d[/.\-]\d\d[/.\-]\d\d\d\d)\;','')

                        then regexp_substr(conversions.other_data,'(u9=)(\d\d[/.\-]\d\d[/.\-]\d\d\d\d)\;',1,1,'',2) end as traveldate_1

--                   Break out each origin/destination into separate fields so that we can use them deterministically later
                    ,
                    case when regexp_like(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                        then regexp_substr(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                    when regexp_like(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib')
                        then regexp_substr(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end            as rt_1_orig,
                    case when regexp_like(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                        then regexp_substr(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                    when regexp_like(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib')
                        then regexp_substr(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end            as rt_1_dest,
                    case when regexp_like(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                        then regexp_substr(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                    when regexp_like(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib')
                        then regexp_substr(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end            as rt_2_orig,
                    case when regexp_like(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                        then regexp_substr(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                    when regexp_like(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib')
                        then regexp_substr(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end            as rt_2_dest

--                   Route 1
--                   when both (first) routes have parenthesis
                    ,
                    case when regexp_like(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                        and regexp_like(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                        then regexp_substr(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3) || ' to ' || regexp_substr(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)

                    --                   when neither (first) routes have parenthesis
                    when regexp_like(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib')
                        and regexp_like(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib')
                        then regexp_substr(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) || ' to ' || regexp_substr(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2)

                    --                   when "():" origin yes, destination no
                    when regexp_like(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                        and regexp_like(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib')
                        then regexp_substr(conversions.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3) || ' to ' || regexp_substr(conversions.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',3)

                    --                   when "():" origin no, destination yes
                    when regexp_like(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib')
                        and regexp_like(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                        then regexp_substr(conversions.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) || ' to ' || regexp_substr(conversions.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)

                    end                                                                                                 as route_1

--                   Route 2
--                   when both (second) routes have parenthesis
                    ,
                    case when regexp_like(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
                        regexp_like(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                        then regexp_substr(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3) || ' to ' || regexp_substr(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)

                    --                   when neither (second) routes have parenthesis
                    when regexp_like(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') and
                        regexp_like(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib')
                        then regexp_substr(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) || ' to ' || regexp_substr(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2)

                    --                   when "():" origin yes, destination no
                    when regexp_like(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
                        regexp_like(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib')
                        then regexp_substr(conversions.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3) || ' to ' || regexp_substr(conversions.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',3)

                    --                   when "():" origin no, destination yes
                    when regexp_like(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') and
                        regexp_like(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                        then regexp_substr(conversions.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) || ' to ' || regexp_substr(conversions.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)

                    end                                                                                                 as route_2,
                    case
                    when lower(substring(conversions.other_data,(instr(conversions.other_data,'u11=') + 4),5)) = 'busin' or lower(substring(conversions.other_data,(instr(conversions.other_data,'u12=') + 4),5)) = 'busin'
                        then 'business'
                    when lower(substring(conversions.other_data,(instr(conversions.other_data,'u11=') + 4),5)) = 'first' or lower(substring(conversions.other_data,(instr(conversions.other_data,'u12=') + 4),5)) = 'first'
                        then 'first'
                    when lower(substring(conversions.other_data,(instr(conversions.other_data,'u11=') + 4),5)) = 'coach' and lower(substring(conversions.other_data,(instr(conversions.other_data,'u12=') + 4),5)) = 'coach'
                        then 'coach'
                    when (lower(substring(conversions.other_data,(instr(conversions.other_data,'u11=') + 4),5)) = 'coach' and (lower(substring(conversions.other_data,(instr(conversions.other_data,'u12=') + 4),5)) <> 'first' and lower(substring(conversions.other_data,(instr(conversions.other_data,'u12=') + 4),5)) <> 'busin'))
                        then 'coach'
                    else 'other' end                                                                                    as class


--                    , case when (regexp_substr(conversions.other_data,'(u5=).+?\;',1,1,'ib', 3) = regexp_substr(conversions.other_data,'(u8=).+?\;',1,1,'ib', 3) and
--                    regexp_substr(conversions.other_data,'(u7=).+?\;',1,1,'ib', 3) = regexp_substr(conversions.other_data,'(u6=).+?\;',1,1,'ib', 3))
-- --                                or regexp_like(conversions.other_data,'(u6=)\-\-\;','')
--                    then 'round-trip'
--                      else 'one-way' end                           as flighttype
--          , sum(1) over (partition by user_id order by activity_time)
                ,
                sum(conversions.Con)                                                                                                                                          as Con,
                sum(conversions.Tix)                                                                                                                                          as Tix


              from (
select
                     cast(t2.Click_Time as date ) as Click_Time
                     ,t2.order_id as order_id
                     ,t2.site_id as Site_ID
                     ,t2.page_id as page_id
                     ,t2.other_data
                     ,sum ( case when Event_ID = 1 or Event_ID = 2 then 1 else 0 end ) as Con
                     ,sum ( case when Event_ID = 1 or Event_ID = 2 then t2.Quantity else 0 end ) as Tix

                     from (select *
                           from diap01.mec_us_united_20056.dfa_activity
                           where cast(Click_Time as date) between '2016-09-12' and '2016-12-31'

                             and activity_type = 'ticke498'
                             and activity_sub_type = 'unite820'
                             and revenue != 0
                             and quantity != 0
                             and order_id = 10276123 -- Polaris 2016
                             and advertiser_id <> 0
                             and not regexp_like( substring (other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
                     ) as t2
          group by
                     cast(t2.Click_Time as date )
                     ,t2.order_id
                     ,t2.site_id
                     ,t2.page_id
                     ,t2.other_data
                   ) as conversions

                left join diap01.mec_us_united_20056.dfa_campaign as campaign
                  on conversions.order_id = campaign.order_id

                left join
                (
                  select
                    cast(t1.site_placement as varchar(4000)) as site_placement,
                    t1.page_id                               as page_id,
                    t1.order_id                              as order_id,
                    t1.site_id                               as site_id

                  from (select
                          order_id                 as order_id,
                          site_id                  as site_id,
                          page_id                  as page_id,
                          site_placement           as site_placement,
                          cast(start_date as date) as thisdate,
                          row_number() over (partition by order_id,site_id,page_id  order by cast (start_date as date ) desc) as r1
                        from diap01.mec_us_united_20056.dfa_page

                       ) as t1
                  where r1 = 1
                ) as placements
                  on conversions.page_id = placements.page_id
                  and conversions.order_id = placements.order_id
                  and conversions.site_id = placements.site_id


                left join diap01.mec_us_united_20056.dfa_site as directory
                  on conversions.site_id = directory.site_id

              group by cast(conversions.Click_Time as date )
              ,conversions.order_id
              ,conversions.site_id
              ,conversions.page_id
              ,placements.site_placement
              ,directory.directory_site
              ,campaign.buy
           ,conversions.other_data

            ) as report

       where report.traveldate_1 is not null
           and NOT REGEXP_LIKE(report.placement,'.do\s*not\s*use.','ib')
and Report.Site_ID !='1485655'

       group by

         cast(report.date as date )
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
       ,report.class
--        ,report.other_data
--     ,report.thisField

     ) as final

group by

  cast(final.date as date )
,final.month
,final.site
-- ,final.campaign_id
,final.campaign
,final.placement_id
,final.placement
-- ,final.flighttype
,final.route
,final.class
-- ,final.traveldate_1
-- ,final.route_1
-- ,final.route_2
-- ,final.other_data
