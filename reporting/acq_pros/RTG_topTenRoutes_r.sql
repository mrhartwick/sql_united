
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
        t3.route,
        sum(t3.conv)        as conv,
        sum(t3.tickets)     as tickets,
        avg(pch_window_day) as avg_pch_window

    from (

             select

                 t2.reportDate,
                 t2.reportMonth,
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

                          t1.md_interaction_date_loc as reportDate,
                          cast(date_part(month,(t1.md_interaction_date_loc)) as int) as reportMonth,
                          c1.campaign       as campaign,
                          t1.campaign_id    as campaign_id,
                          s1.site_dcm       as site,
                          t1.site_id_dcm    as site_id,
                          p1.placement      as placement,
                          t1.placement_id   as placement_id,
                          t1.other_data,
                          t1.traveldate_1,

--                        Redshift regex isn't as smart as Vertica - you can't choose which subexpression to keep.
--                        So we have to break the string into smaller pieces before matching
--                        This is actually faster
                          regexp_substr(regexp_replace(regexp_substr(t1.other_data,'(u5\\=)(.*)(\\;u6\\=)'),'(u5\\=)(.*)(\\()','u5=(',0),'([A-Z][A-Z][A-Z])',1,1,'e') as rt_1_orig,
                          regexp_substr(regexp_replace(regexp_substr(t1.other_data,'(u7\\=)(.*)(\\;u8\\=)'),'(u7\\=)(.*)(\\()','u7=(',0),'([A-Z][A-Z][A-Z])',1,1,'e') as rt_1_dest,

                          regexp_substr(regexp_replace(regexp_substr(t1.other_data,'(u6\\=)(.*)(\\;u1\\=)'),'(u6\\=)(.*)(\\()','u6=(',0),'([A-Z][A-Z][A-Z])',1,1,'e') as rt_2_orig,
                          regexp_substr(regexp_replace(regexp_substr(t1.other_data,'(u8\\=)(.*)(\\;u9\\=)'),'(u8\\=)(.*)(\\()','u8=(',0),'([A-Z][A-Z][A-Z])',1,1,'e') as rt_2_dest,


                          regexp_substr(regexp_replace(regexp_substr(t1.other_data,'(u5\\=)(.*)(\\;u6\\=)'),'(u5\\=)(.*)(\\()','u5=(',0),'([A-Z][A-Z][A-Z])',1,1,'e') ||' to '|| regexp_substr(regexp_replace(regexp_substr(t1.other_data,'(u7\\=)(.*)(\\;u8\\=)'),'(u7\\=)(.*)(\\()','u7=(',0),'([A-Z][A-Z][A-Z])',1,1,'e') as route_1,
--                        Redshift handles strings differently; make sure route 2 airports exist before creating new string
                          case
                          when (length(ISNULL(regexp_substr(regexp_replace(regexp_substr(t1.other_data,'(u6\\=)(.*)(\\;u1\\=)'),'(u6\\=)(.*)(\\()','u6=(',0),'([A-Z][A-Z][A-Z])',1,1,'e'),'')) = 0) then ''
                          else regexp_substr(regexp_replace(regexp_substr(t1.other_data,'(u6\\=)(.*)(\\;u1\\=)'),'(u6\\=)(.*)(\\()','u6=(',0),'([A-Z][A-Z][A-Z])',1,1,'e') ||' to '|| regexp_substr(regexp_replace(regexp_substr(t1.other_data,'(u8\\=)(.*)(\\;u9\\=)'),'(u8\\=)(.*)(\\()','u8=(',0),'([A-Z][A-Z][A-Z])',1,1,'e') end as route_2,

                          cast(datediff('day',md_event_date_loc,t1.traveldate_1) as numeric(20,10)) as pch_window_day,
                          case when conversion_id = 1 or conversion_id = 2 then 1 else 0 end as conv,
                          case when conversion_id = 1 or conversion_id = 2 then t1.total_conversions else 0 end as tickets

                      from (

                               select
                                   *,
--                             capture all dates, no matter the format; Vertica only camptured some
                               cast(replace(replace(regexp_substr(other_data,'(u9\\=)(\\d.?\\d.?\\d.?\\d.?\\d.?\\d.?\\d.?\\d)(\\;u10\\=)'),'u9=',''),';u10=','') as date) as traveldate_1
                               from wmprodfeeds.united.dfa2_activity
                               where md_interaction_date_loc between '2018-08-01' and '2018-08-31'
                                   and activity_id = 978826
                                   and total_revenue <> 0
                                   and total_conversions <> 0
                                   and advertiser_id <> 0
                                   and campaign_id = '20606595'
                                   and site_id_dcm = '1239319'
                                   and regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') = 0

                           ) as t1

                          left join wmprodfeeds.united.dfa2_campaigns as c1
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
                              from wmprodfeeds.united.dfa2_placements

                                   ) as p1
                              where x1 = 1
                          ) as p1
                          on t1.placement_id = p1.placement_id
                              and t1.campaign_id = p1.campaign_id
                              and t1.site_id_dcm = p1.site_id_dcm

                          left join wmprodfeeds.united.dfa2_sites as s1
                          on t1.site_id_dcm = s1.site_id_dcm

                  ) as t2
where t2.placement not like '%PROS_FT%' and t2.placement not like '%Route%'
         ) as t3
    where (length(isnull(cast(t3.traveldate_1 as varchar),'')) > 0)

    group by

        t3.route

) as t4

order by tickets desc

) as t5
