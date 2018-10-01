select
final.site                                                    as site,
final.destination                                             as destination,
final.route_1_destination                                     as route_1_destination,
final.route_2_destination                                     as route_2_destination,
final.route_1_origin                                          as route_1_origin,
final.other_data                                              as other_data,
sum(final.tix)                                                as tix,
sum(final.rev)                                                as adj_rev




from (

        select
           cast(report.date as date)                             as "Date"
           ,report.campaign                                      as campaign
           ,report.campaign_id                                   as campaign_id
           ,report.site                                          as site
           ,report.site_id                                       as site_id
           ,report.placement                                     as placement
           ,report.placement_id                                  as placement_id
           ,report.traveldate_1                                  as traveldate_1
           ,case when (length(ISNULL(report.rt_2_dest,''))=0) then report.rt_1_dest
              when report.rt_1_orig = report.rt_2_dest and report.rt_2_orig = report.rt_1_dest then report.rt_1_dest
              else report.rt_2_dest
              end                                                       as destination
           ,report.rt_1_dest                                            as route_1_destination
           ,report.rt_2_dest                                            as route_2_destination
           ,report.rt_1_orig                                            as route_1_origin
           ,report.rt_2_orig                                            as route_2_origin
           ,report.other_data                                           as other_data
           ,sum(report.tix)                                             as tix
           ,sum(cast((report.rev  * 0.0800)  as decimal(10,2)))     as rev


        from (

                select

                    cast (timestamp_trunc(to_timestamp(a.interaction_time / 1000000),'SS') as date ) as "Date"
                   ,campaign.campaign                                                                as campaign
                   ,a.campaign_id                                                                    as campaign_id
                   ,directory.site_dcm                                                               as site
                   ,a.site_id_dcm                                                                    as site_id
                   ,placements.placement                                                             as placement
                   ,a.placement_id                                                                   as placement_id
                   ,sum(total_conversions)                                                           as tix
                   ,sum((total_revenue * 1000000)/rates.exchange_rate)                               as rev
                   ,a.other_data                                                                     as other_data

                   ,case when regexp_like(a.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;','') then
                   subString(regexp_substr(a.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'',2),5,2) || '-' || subString(regexp_substr(a.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'',2),7,2) || '-' || subString(regexp_substr(a.other_data,'(u9=)(\d\d\d\d\d\d\d\d)\;',1,1,'',2),1,4)
                   when regexp_like(a.other_data,'(u9=)(\d\d[/.\-]\d\d[/.\-]\d\d\d\d)\;','') then
                   regexp_substr(a.other_data,'(u9=)(\d\d[/.\-]\d\d[/.\-]\d\d\d\d)\;',1,1,'',2) end           as traveldate_1

                   ,case when regexp_like(a.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(a.other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                   when regexp_like(a.other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(a.other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_1_orig
                   ,case when regexp_like(a.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(a.other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                   when regexp_like(a.other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(a.other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_1_dest

                   ,case when regexp_like(a.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(a.other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                   when regexp_like(a.other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(a.other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_2_orig
                   ,case when regexp_like(a.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(a.other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
                   when regexp_like(a.other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(a.other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_2_dest

                   from diap01.mec_us_united_20056.dfa2_activity as a

                   left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
                   on upper(substring(a.other_data,(instr(a.other_data,'u3=') + 3),3)) = upper(rates.currency)
                   and cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date) = rates.date

                   left join diap01.mec_us_united_20056.dfa2_campaigns as campaign
                   on a.campaign_id = campaign.campaign_id

                    left join
                    (
                    select  placement,  max(placement_id) as placement_id, campaign_id, site_id_dcm
                    from diap01.mec_us_united_20056.dfa2_placements
                        group by placement, campaign_id, site_id_dcm
                    ) as Placements

                    ON a.placement_id = Placements.placement_id
                    and a.campaign_id = Placements.campaign_id
                    and a.site_id_dcm  = Placements.site_id_dcm


                    left join diap01.mec_us_united_20056.dfa2_sites as directory
                    on a.site_id_dcm = directory.site_id_dcm



                    where cast(timestamp_trunc(to_timestamp(a.interaction_time / 1000000),'SS') as date) between '2018-01-01' and '2018-05-03'
                    and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
                    and regexp_like(substring(other_data,(instr(other_data,'u2=') + 3),2),'US','ib')
                    and (a.activity_id = 978826)
                    and a.conversion_id in (1,2)
                    and a.total_revenue <> 0
                    and a.campaign_id = 20606595 -- GM ACQ 2018
                    and (a.advertiser_id <> 0)
                    and (length(isnull (event_sub_type,'')) > 0)
                    and a.site_id_dcm in ('1239319', '1190273')



                group by
                   cast (timestamp_trunc(to_timestamp(a.interaction_time / 1000000),'SS') as date )
                   ,a.campaign_id
                   ,a.site_id_dcm
                   ,a.placement_id
                   ,placements.placement
                   ,directory.site_dcm
                   ,campaign.campaign
                   ,a.other_data


             ) as report

        where report.traveldate_1 is not null

       and report.placement LIKE '%GEN_INT_PROS_FT%' OR report.placement LIKE '%GEN_DOM_PROS_FT%'


        group by

           cast(report.date as date)
           ,report.site
           ,report.site_id
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


where final.destination LIKE '%SYD%'

group by



final.destination
,final.route_1_destination
,final.route_2_destination
,final.route_1_origin
,final.other_data
,final.site
