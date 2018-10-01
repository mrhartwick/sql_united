declare @report_st date
declare @report_ed date
--
set @report_ed = '2017-10-31';
set @report_st = '2017-08-15';

select
    t2.dcmdate               as "Date",
    t2.destination           as destination,
    t2.route_1_destination   as "Route1_destination",
    t2.route_2_destination   as "Route2_destination",
    t2.site                  as site,
    sum(t2.impressions)      as imps,
    sum(t2.led)              as leads,
    sum(t2.tix)              as tix,
    sum(t2.cost)             as cost,
    sum((t2.rev * .08) * .9) as rev

from
    (
        select
            t1.dcmdate                                                                                   as dcmdate,
            cast(month(cast(t1.dcmdate as date)) as int)                                                 as dcmmonth,
            [dbo].udf_dateToInt(t1.dcmdate)                                                              as dcmmatchdate,
            t1.campaign                                                                                  as campaign,
            t1.campaign_id                                                                               as campaign_id,
            t1.site_dcm                                                                                  as site,
            t1.site_id_dcm                                                                               as site_id_dcm,
            case when t1.plce_id in ('PBKB7J','PBKB7H','PBKB7K') then 'PBKB7J'
            else t1.plce_id end                                                                          as plce_id,
            case when t1.placement like 'PBKB7J%' or t1.placement like 'PBKB7H%' or t1.placement like 'PBKB7K%' or t1.placement = 'United 360 - Polaris 2016 - Q4 - Amobee' then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
            else t1.placement end                                                                        as placement,
--                 t1.rt_1_orig                                as rt_1_orig,
--                 t1.rt_1_dest                                as rt_1_dest,
--                 t1.rt_2_orig                                as rt_2_orig,
--                 t1.rt_2_dest                                as rt_2_dest,
            case when (len(ISNULL(t1.rt_2_dest,'')) = 0) then t1.rt_1_dest
            when t1.rt_1_orig = t1.rt_2_dest and t1.rt_2_orig = t1.rt_1_dest then t1.rt_1_dest
            when t1.rt_2_dest in ('SYD','MEL','PEK','PVG','SHA','CTU','XIY','HKG','EZE','SCL','LCY','LGW','LHR','DUB','GVA','EDI','FRA','HHN','CDG','HNL','KOA','ITO','LIH','AMS','SIN','AKL','MUC','ZRH','ICN','HAM','ITM','KIX','BRU','EWR','DEN','SFO') then t1.rt_2_dest
            when t1.rt_1_dest in ('SYD','MEL','PEK','PVG','SHA','CTU','XIY','HKG','EZE','SCL','LCY','LGW','LHR','DUB','GVA','EDI','FRA','HHN','CDG','HNL','KOA','ITO','LIH','AMS','SIN','AKL','MUC','ZRH','ICN','HAM','ITM','KIX','BRU','EWR','DEN','SFO') then t1.rt_1_dest
            else t1.rt_2_dest
            end                                                                                          as destination,
            t1.rt_1_dest                                                                                 as route_1_destination,
            t1.rt_2_dest                                                                                 as route_2_destination,
            t1.rt_1_orig                                                                                 as route_1_origin,
            t1.rt_2_orig                                                                                 as route_2_origin,
            case when t1.placement_id in (137412510,137412401,137412609) then 137412609
            else t1.placement_id end                                                                     as placement_id,
            prs.stdate                                                                                   as stdate,
            case when t1.campaign_id = 9923634 and t1.site_id_dcm != 1190258 then 20161022
            else prs.eddate end                                                                          as eddate,
            prs.packagecat                                                                               as packagecat,
            prs.costmethod                                                                               as costmethod,
            prs.cost_id                                                                                  as cost_id,
            prs.planned_amt                                                                              as planned_amt,
            prs.planned_cost                                                                             as planned_cost,
            prs.placementstart                                                                           as placementstart,
            case when t1.campaign_id = 9923634 and t1.site_id_dcm != 1190258 then '2016-10-22'
            else prs.placementend end                                                                    as placementend,
            cast(prs.rate as decimal(10,2))                                                              as rate,
            sum(t1.impressions)                                                                          as impressions,
            case when t1.site_id_dcm = 1190273 -- Adara
                then cast((sum(cast(impressions as decimal(20,10))) * cast(prs.rate as decimal(20,10))) as
                          decimal(20,10)) * 0.396921230133226
            when t1.site_id_dcm = 1239319 -- Sojern
                then cast((sum(cast(impressions as decimal(20,10))) * cast(prs.rate as decimal(20,10))) as
                          decimal(20,10)) * 0.61893013091977 end

                                                                                                         as cost,
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
            sum(t1.rev)                                                                                  as rev,
            case when cast(month(prs.placementend) as int) - cast(month(cast(t1.dcmdate as date)) as int) <= 0 then 0
            else cast(month(prs.placementend) as int) - cast(month(cast(t1.dcmdate as date)) as int) end as diff,
            [dbo].udf_dvMap(t1.campaign_id,t1.site_id_dcm,t1.placement,prs.CostMethod,prs.dv_map)        as dv_map


-- =========================================================================================

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
                                from diap01.mec_us_united_20056.ual_route_cost as t1

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

                                where  regexp_like(p1.placement,''.*GEN_INT_PROS_FT.*'',''ib'') or regexp_like(p1.placement,''.*GEN_DOM_PROS_FT.*'',''ib'')

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
                from [10.2.186.148,4721].dm_1161_unitedairlinesusa.[dbo].prs_summ
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


where (

    t2.destination like '%SYD%'
        or t2.destination like '%MEL%'
        or t2.destination like '%PEK%'
        or t2.destination like '%PVG%'
        or t2.destination like '%SHA%'
        or t2.destination like '%CTU%'
        or t2.destination like '%XIY%'
        or t2.destination like '%HKG%'
        or t2.destination like '%EZE%'
        or t2.destination like '%SCL%'
        or t2.destination like '%LCY%'
        or t2.destination like '%LGW%'
        or t2.destination like '%LHR%'
        or t2.destination like '%DUB%'
        or t2.destination like '%GVA%'
        or t2.destination like '%EDI%'
        or t2.destination like '%FRA%'
        or t2.destination like '%HHN%'
        or t2.destination like '%CDG%'
        or t2.destination like '%HNL%'
        or t2.destination like '%KOA%'
        or t2.destination like '%ITO%'
        or t2.destination like '%LIH%'
        or t2.destination like '%AMS%'
        or t2.destination like '%SIN%'
        or t2.destination like '%AKL%'
        or t2.destination like '%MUC%'
        or t2.destination like '%ZRH%'
        or t2.destination like '%ICN%'
        or t2.destination like '%HAM%'
        or t2.destination like '%ITM%'
        or t2.destination like '%KIX%'
        or t2.destination like '%BRU%'

)
group by
    t2.dcmdate,
    t2.destination,
    t2.route_1_destination,
    t2.site,
    t2.route_2_destination

