
--GM ACQ CA/NY DESTINATIONS/ORIGINS: COST-EFFICIENT METRICS
--Step1: Create table that includes imps, rev, tickets, and leads in the route-level (RUN THIS IN VERTICA)

create table diap01.mec_us_united_20056.ual_ca_ny_routes
(
user_id      varchar(50),
con_tim      int,
imp_tim      int,
campaign_id  int,
site_id_dcm  int,
placement_id int,
rt_1_orig    varchar(3),
rt_1_dest    varchar(3),
rt_2_orig    varchar(3),
rt_2_dest    varchar(3),
imp_nbr      int,
clk_led      int,
vew_led      int,
clk_con      int,
clk_tix      int,
clk_rev      decimal(20,10),
vew_con      int,
vew_tix      int,
vew_rev      decimal(20,10),
rev          decimal(20,10)
);

insert into  diap01.mec_us_united_20056.ual_ca_ny_routes
(user_id,
con_tim,
imp_tim,
campaign_id,
site_id_dcm,
placement_id,
rt_1_orig,
rt_1_dest,
rt_2_orig,
rt_2_dest,
imp_nbr,
clk_led,
vew_led,
clk_con,
clk_tix,
clk_rev,
vew_con,
vew_tix,
vew_rev,
rev)

    (select

       ta.user_id
      ,ta.con_tim as con_tim
      ,ti.imp_tim as imp_tim
      ,ta.campaign_id
      ,ta.site_id_dcm
      ,ta.placement_id
      ,ta.rt_1_orig
      ,ta.rt_1_dest
      ,ta.rt_2_orig
      ,ta.rt_2_dest
      ,ti.imp_nbr
      ,ta.clk_led
      ,ta.vew_led
      ,ta.clk_con
      ,ta.clk_tix
      ,ta.clk_rev
      ,ta.vew_con
      ,ta.vew_tix
      ,ta.vew_rev
      ,ta.rev

        from
        (
            select
            user_id,
            interaction_time as con_tim,
            campaign_id,
            site_id_dcm,
            placement_id,
            case when activity_id = 1086066 and (conversion_id = 1) then 1 else 0 end as clk_led,
            case when activity_id = 1086066 and (conversion_id = 2) then 1 else 0 end as vew_led,
            case when activity_id = 978826 and conversion_id = 1 and total_revenue <> 0 then 1 else 0 end as clk_con,
            case when activity_id = 978826 and conversion_id = 1 and total_revenue <> 0 then total_conversions else 0 end as clk_tix,
            case when conversion_id = 1 then (total_revenue * 1000000)/rates.exchange_rate else 0 end as clk_rev,
            case when activity_id = 978826 and conversion_id = 2 and total_revenue <> 0 then 1 else 0 end as vew_con,
            case when activity_id = 978826 and conversion_id = 2 and total_revenue <> 0 then total_conversions else 0 end as vew_tix,
            case when conversion_id = 2 then (total_revenue * 1000000)/rates.exchange_rate else 0 end as vew_rev,
            (total_revenue * 1000000)/rates.exchange_rate as rev,
            case when regexp_like(other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
            when regexp_like(other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_1_orig,
            case when regexp_like(other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
            when regexp_like(other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_1_dest,
            case when regexp_like(other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
            when regexp_like(other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_2_orig,
            case when regexp_like(other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
            when regexp_like(other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_2_dest


            from diap01.mec_us_united_20056.dfa2_activity a

            left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
            on upper(substring(a.other_data,(instr(a.other_data,'u3=') + 3),3)) = upper(rates.currency)
            and cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date) = rates.date

            where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2018-01-01' and '2018-03-31'
            and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
            and (activity_id = 978826 or activity_id = 1086066)
            and campaign_id = 20606595 -- GM ACQ 2018
            and (advertiser_id <> 0)
            and (length(isnull (event_sub_type,'')) > 0)
            ) as ta,

            (
            select
                user_id,
                event_time as imp_tim,
                row_number() over() as imp_nbr
            from diap01.mec_us_united_20056.dfa2_impression
            where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-01-01' and '2018-03-31'
            and campaign_id = 20606595 -- GM ACQ 2018
            and (advertiser_id <> 0)
            ) as ti

      where
      ta.user_id = ti.user_id
      and imp_tim = con_tim);

commit;

--//=======================================================================================================================================
--All GM Acq: CA & NY destinations & origins, with cost-efficient metrics (RUN THIS IN SQL SERVER)

--Step 2: Query to pull Sojern & Adara cost-efficient metrics for the specific-destinations (RUN THIS IN SQL SERVER)

select
    t2.dcmdate               as "Date",
    t2.destination           as destination,
    t2.origin                as origin,
    t2.site                  as site,
    sum(t2.impressions)      as imps,
    sum(t2.led)              as leads,
    sum(t2.tix)              as tix,
    sum(t2.cost)             as cost,
    sum(t2.rev)              as rev

from
    (
        select
            t1.dcmdate                                                                                   as dcmdate,
            cast(month(cast(t1.dcmdate as date)) as int)                                                 as dcmmonth,
            [dbo].udf_dateToInt(t1.dcmdate)                                                              as dcmmatchdate,
            t1.campaign                                                                                  as campaign,
            t1.campaign_id                                                                               as campaign_id,
            case when t1.site_id_dcm = 1190273 then 'Adara'
            when t1.site_id_dcm = 3267410 then 'Quantcast'
            when t1.site_id_dcm = 1239319 and not regexp_like(t1.placement,'.*_PROS_FT.*','ib') then 'Sojern RTG'
            when t1.site_id_dcm = 1239319 and regexp_like(t1.placement,'.*_PROS_FT.*','ib') then 'Sojern PRS'
            else t1.site_dcm                                                                             as site,
            t1.site_id_dcm                                                                               as site_id_dcm,
            t1.plce_id                                                                                   as plce_id,
            t1.placement                                                                                 as placement,
            case when (len(ISNULL(t1.rt_2_dest,'')) = 0) then t1.rt_1_dest
            when t1.rt_1_orig = t1.rt_2_dest and t1.rt_2_orig = t1.rt_1_dest then t1.rt_1_dest
            when t1.rt_2_dest in ('LGA','EWR','FAT','LGB','LAX','SNA','SMF','SAN','SFO','SJC','SBA','BUR','OAK','PSP') then t1.rt_2_dest
            when t1.rt_1_dest in ('LGA','EWR','FAT','LGB','LAX','SNA','SMF','SAN','SFO','SJC','SBA','BUR','OAK','PSP') then t1.rt_1_dest
            else ''  end                                                                                 as destination,
            t1.rt_1_dest                                                                                 as route_1_destination,
            t1.rt_2_dest                                                                                 as route_2_destination,
            t1.rt_1_orig                                                                                 as route_1_origin,
            t1.rt_2_orig                                                                                 as route_2_origin,
            case when t1.rt_1_orig in ('LGA','EWR','FAT','LGB','LAX','SNA','SMF','SAN','SFO','SJC','SBA','BUR','OAK','PSP') then t1.rt_1_orig
            else ''  end                                                                                 as origin,
            t1.placement_id                                                                              as placement_id,
            prs.stdate                                                                                   as stdate,
            prs.eddate                                                                                   as eddate,
            prs.packagecat                                                                               as packagecat,
            prs.costmethod                                                                               as costmethod,
            prs.cost_id                                                                                  as cost_id,
            prs.planned_amt                                                                              as planned_amt,
            prs.planned_cost                                                                             as planned_cost,
            prs.placementstart                                                                           as placementstart,
            prs.placementend                                                                             as placementend,
            cast(prs.rate as decimal(10,2))                                                              as rate,
            sum(t1.impressions)                                                                          as impressions,
            case when t1.site_id_dcm = 1190273 -- Adara
                then cast((sum(cast(impressions as decimal(20,10))) * cast(prs.rate as decimal(20,10))) as
                          decimal(20,10)) * 0.40682684
            when t1.site_id_dcm = 3267410 -- Quantcast
                then cast((sum(cast(impressions as decimal(20,10))) * cast(prs.rate as decimal(20,10))) as
                          decimal(20,10)) * 0.732853744
            when t1.site_id_dcm = 1239319 -- Sojern RTG
            and not regexp_like(t1.placement,'.*_PROS_FT.*','ib')
            then cast((sum(cast(impressions as decimal(20,10))) * (36) as
                          decimal(20,10)) * 0.720099978
            when t1.site_id_dcm = 1239319 -- Sojern PRS
            and regexp_like(t1.placement,'.*_PROS_FT.*','ib')
                then cast((sum(cast(impressions as decimal(20,10))) * cast(prs.rate as decimal(20,10))) as
                          decimal(20,10)) * 0.720099978 end                                              as cost,
            sum(t1.vew_led)                                                                              as vew_led,
            sum(t1.clk_led)                                                                              as clk_led,
            sum(t1.led)                                                                                  as led,
            sum(t1.vew_con)                                                                              as vew_con,
            sum(t1.clk_con)                                                                              as clk_con,
            sum(t1.con)                                                                                  as con,
            sum(t1.vew_tix)                                                                              as vew_tix,
            sum(t1.clk_tix)                                                                              as clk_tix,
            sum(t1.tix)                                                                                  as tix,
            sum(t1.vew_rev)                                                                              as vew_rev,
            sum(t1.clk_rev)                                                                              as clk_rev,
            sum(case
                when t1.site_id_dcm = 1190273 -- Adara
                then (((((t1.vew_rev * 0.652586332)  + t1.clk_rev)  * .08) * .9) as decimal(10,2))
                when t1.site_id_dcm = 3267410 -- Quantcast
                then (((((t1.vew_rev * 0.713885347)  + t1.clk_rev)  * .08) * .9) as decimal(10,2))
                when (t1.site_id_dcm = 1239319) -- Sojern RTG
                and not regexp_like(t1.placement,'.*_PROS_FT.*','ib')
                then (((((t1.vew_rev * 0.773691382)  + t1.clk_rev)  * .08) * .9) as decimal(10,2))
                when t1.site_id_dcm = 1239319 -- Sojern PRS
                and regexp_like(t1.placement,'.*_PROS_FT.*','ib')
                then (((((t1.vew_rev * 0.729118116)  + t1.clk_rev)  * .08) * .9) as decimal(10,2)) end)  as rev,

            case when cast(month(prs.placementend) as int) - cast(month(cast(t1.dcmdate as date)) as int) <= 0 then 0
            else cast(month(prs.placementend) as int) - cast(month(cast(t1.dcmdate as date)) as int) end as diff,
            [dbo].udf_dvMap(t1.campaign_id,t1.site_id_dcm,t1.placement,prs.CostMethod,prs.dv_map)        as dv_map


-- openquery function call must not exceed 8,000 characters; no room for comments inside the function
        from (
                 select *
                 from openQuery(verticaunited,
                                '
                                select
                                cast(timestamp_trunc(to_timestamp(con_tim / 1000000),''SS'') as date) as dcmdate,
                                c1.campaign,
                                t1.campaign_id,
                                s1.site_dcm,
                                t1.site_id_dcm,
                                p1.placement,
                                left(p1.placement,6)                      as plce_id,
                                t1.placement_id,
                                rt_1_orig,
                                rt_1_dest,
                                rt_2_orig,
                                rt_2_dest,
                                count(distinct imp_nbr) as impressions,
                                sum(clk_led) as clk_led,
                                sum(vew_led) as vew_led,
                                sum(clk_led) + sum(vew_led) as led,
                                sum(clk_con) as clk_con,
                                sum(clk_tix) as clk_tix,
                                sum(clk_con) + sum(vew_con) as con,
                                sum(clk_tix) + sum(vew_tix) as tix,
                                sum(clk_rev) as clk_rev,
                                sum(vew_con) as vew_con,
                                sum(vew_tix) as vew_tix,
                                sum(vew_rev) as vew_rev,
                                sum(rev) as rev
                                from diap01.mec_us_united_20056.ual_ca_ny_routes  as t1

                                left join
                                (
                                select
                                cast(campaign as varchar(4000)) as ''campaign'',campaign_id as ''campaign_id''
                                from diap01.mec_us_united_20056.dfa2_campaigns
                                ) as c1
                                on t1.campaign_id = c1.campaign_id

                                left join
                                (
                                select
                                cast(p1.placement as varchar(4000)) as ''placement'',p1.placement_id as ''placement_id'',p1.campaign_id as ''campaign_id'',p1.site_id_dcm as ''site_id_dcm''

                                from (select
                                campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast(placement_start_date as date) as thisdate,
                                row_number() over ( partition by campaign_id,site_id_dcm,placement_id
                                order by cast(placement_start_date as date) desc ) as x1
                                from diap01.mec_us_united_20056.dfa2_placements

                                ) as p1
                                where x1 = 1
                                ) as p1
                                on t1.placement_id = p1.placement_id
                                and t1.campaign_id = p1.campaign_id
                                and t1.site_id_dcm = p1.site_id_dcm

                                left join
                                (
                                select
                                cast(site_dcm as varchar(4000)) as ''site_dcm'',site_id_dcm as ''site_id_dcm''
                                from diap01.mec_us_united_20056.dfa2_sites
                                ) as s1
                                on t1.site_id_dcm = s1.site_id_dcm

                                and t1.site_id_dcm in (1190273, 1239319, 3267410)
                                and t1.campaign_id = 20606595
                                and cast(timestamp_trunc(to_timestamp(con_tim / 1000000),''SS'') as date) between ''2018-01-01'' and ''2018-03-31''

                                group by

                                cast(timestamp_trunc(to_timestamp(con_tim / 1000000),''SS'') as date),
                                t1.campaign_id,
                                c1.campaign,
                                t1.site_id_dcm,
                                s1.site_dcm,
                                t1.placement_id,
                                p1.placement,
                                rt_1_orig,
                                rt_1_dest,
                                rt_2_orig,
                                rt_2_dest

                                ')

             ) as t1

--    Prisma data
            left join
            (
                select *
                from [10.2.186.148\SQLINS02, 4721].dm_1161_unitedairlinesusa.[dbo].prs_summ
            ) as prs
            on t1.placement_id = prs.adserverplacementid


        group by
            t1.dcmdate
            ,cast(month(cast(t1.dcmdate as date)) as int)
            ,t1.campaign
            ,t1.campaign_id
            ,t1.site_dcm
            ,t1.site_id_dcm
            ,t1.plce_id
            ,t1.placement
            ,t1.placement_id
            ,t1.rt_1_orig
            ,t1.rt_1_dest
            ,t1.rt_2_orig
            ,t1.rt_2_dest
            ,prs.packagecat
            ,prs.rate
            ,prs.costmethod
            ,prs.cost_id
            ,prs.planned_amt
            ,prs.planned_cost
            ,prs.placementend
            ,prs.placementstart
            ,prs.stdate
            ,prs.eddate
            ,prs.dv_map

    ) as t2

group by
    t2.dcmdate,
    t2.destination,
    t2.origin,
    t2.site
