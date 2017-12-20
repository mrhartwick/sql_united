/*
Outputs top 10 routes, by highest # tickets, for the campaign(s) and
time period(s) specified in the where clauses
 */


select
    t5.route,
    t5.conv,
    t5.tickets,
    t5.avg_pch_window
from (
select
    t4.route,
    t4.conv,
    t4.tickets,
    t4.avg_pch_window

from (
    select

-- t3.month                                                  as  Month,
-- t3.site                                                   as site,
        t3.route,
-- cast(t3.date as date)                                     as purch_date,
-- cast(t3.traveldate_1 as date) as travel_date,
        sum(t3.conv)        as conv,
        sum(t3.tickets)     as tickets,
        avg(pch_window_day) as avg_pch_window

    from (

             select

                 cast(t2.date as date) as "Date",
                 t2.Month              as "Month",
                 t2.campaign           as campaign,
                 t2.campaign_id        as campaign_id,
                 t2.site               as site,
                 t2.placement          as placement,
                 t2.placement_id       as placement_id,
                 t2.traveldate_1       as traveldate_1,
                 case when (length(ISNULL(t2.route_2,'')) = 0) then t2.route_1
                 when t2.rt_1_orig = t2.rt_2_dest and t2.rt_2_orig = t2.rt_1_dest then t2.route_1
                 else t2.route_1 || ' / ' || t2.route_2
                 end                   as route,
                 case when t2.rt_1_orig = t2.rt_2_dest and t2.rt_2_orig = t2.rt_1_dest then 'round-trip' else 'one-way' end
                                       as flighttype,
                 t2.route_1            as route_1,
                 t2.route_2            as route_2,
                 t2.conv               as conv,
                 t2.tickets            as tickets,
                 t2.pch_window_day     as pch_window_day,
                 t2.other_data

             from (

                      select

                          cast(timestamp_trunc(to_timestamp(t1.interaction_time / 1000000),'SS') as date) as "Date",
                          cast(timestamp_trunc(to_timestamp(t1.interaction_time / 1000000),'MM') as date) as "Month",
                          c1.campaign       as campaign,
                          t1.campaign_id    as campaign_id,
                          s1.site_dcm       as site,
                          t1.site_id_dcm    as site_id,
                          p1.placement      as placement,
                          t1.placement_id   as placement_id,
                          t1.other_data,

                          case when regexp_like(t1.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;','') then
                              subString(regexp_substr(t1.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'',2),5,2) || '-' || subString(regexp_substr(t1.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'',2),7,2) || '-' || subString(regexp_substr(t1.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'',2),1,4)
                          when regexp_like(t1.other_data,'(u9=)(\d\d[/.\-]\d\d[/.\-]\d\d\d\d)\;','') then
                              regexp_substr(t1.other_data,'(u9=)(\d\d[/.\-]\d\d[/.\-]\d\d\d\d)\;',1,1,'',2) end                                                       as traveldate_1,

                          case when regexp_like(t1.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(t1.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                          when regexp_like(t1.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(t1.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_1_orig,
                          case when regexp_like(t1.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(t1.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                          when regexp_like(t1.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(t1.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_1_dest,

                          case when regexp_like(t1.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(t1.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                          when regexp_like(t1.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(t1.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_2_orig,
                          case when regexp_like(t1.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(t1.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                          when regexp_like(t1.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(t1.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_2_dest,

                          case when regexp_like(t1.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
                              regexp_like(t1.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                              then regexp_substr(t1.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3) ||' to '|| regexp_substr(t1.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)

                          when regexp_like(t1.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') and
                              regexp_like(t1.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib')
                              then regexp_substr(t1.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) ||' to '|| regexp_substr(t1.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2)

                          when regexp_like(t1.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
                              regexp_like(t1.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib')
                              then regexp_substr(t1.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3) ||' to '|| regexp_substr(t1.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',3)

                          when regexp_like(t1.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') and
                              regexp_like(t1.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                              then regexp_substr(t1.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) ||' to '|| regexp_substr(t1.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)

                          end                                                                                                                                         as route_1,

                          case when regexp_like(t1.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
                              regexp_like(t1.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                              then regexp_substr(t1.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3) ||' to '|| regexp_substr(t1.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)

                          when regexp_like(t1.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') and
                              regexp_like(t1.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib')
                              then regexp_substr(t1.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) ||' to '|| regexp_substr(t1.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2)

                          when regexp_like(t1.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') and
                              regexp_like(t1.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib')
                              then regexp_substr(t1.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3) ||' to '|| regexp_substr(t1.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',3)

                          when regexp_like(t1.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') and
                              regexp_like(t1.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib')
                              then regexp_substr(t1.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) ||' to '|| regexp_substr(t1.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                          end                                                                                                                                         as route_2,
                          datediff('day',to_timestamp(t1.interaction_time / 1000000),t1.traveldate_1)                                                                 as pch_window_day,
                          case when conversion_id = 1 or conversion_id = 2 then 1 else 0 end as conv,
                          case when conversion_id = 1 or conversion_id = 2 then t1.total_conversions else 0 end as tickets

                      from (

                               select
                                   *,
                                   cast(subString(other_data,(instr(other_data,'u9=') + 3),10) as date) as traveldate_1
                               from diap01.mec_us_united_20056.dfa2_activity
                               where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2017-01-01' and '2017-03-21'
                                   and activity_id = 978826
                                   and total_revenue <> 0
                                   and total_conversions <> 0
                                   and advertiser_id <> 0
                                   and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')

                           ) as t1

                          left join diap01.mec_us_united_20056.dfa2_campaigns as c1
                          on t1.campaign_id = c1.campaign_id

                          left join
                          (
                              select
                                  p1.placement,p1.placement_id,p1.campaign_id,p1.site_id_dcm

                              from (
                                   select
                                   campaign_id,
                                   site_id_dcm,
                                   placement_id,
                                   placement,
                                   cast(placement_start_date as date) as thisdate,
                                   row_number() over (partition by campaign_id,site_id_dcm,placement_id
                                                      order by cast(placement_start_date as date) desc ) as x1
                              from diap01.mec_us_united_20056.dfa2_placements

                                   ) as p1
                              where x1 = 1
                          ) as p1
                          on t1.placement_id = p1.placement_id
                              and t1.campaign_id = p1.campaign_id
                              and t1.site_id_dcm = p1.site_id_dcm

                          left join diap01.mec_us_united_20056.dfa2_sites as s1
                          on t1.site_id_dcm = s1.site_id_dcm

                  ) as t2

         ) as t3
    where (length(isnull(t3.traveldate_1,'')) > 0)
    group by

-- cast(t3.date as date),
-- t3.month,
-- t3.site,
-- t3.campaign,
        t3.route

) as t4

order by tickets desc

) as t5
limit 10
