--EWR version: Route Support

select
    t2.dcmdate               as "Date",
    t2.route_1_origin        as origin,
    t2.destination           as destination,
    t2.site                  as site,
    sum(t2.impressions)      as imps,
    sum(t2.led)              as leads,
    sum(t2.tix)              as tix,
    sum(t2.rev)              as rev

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
            t1.placement                                                                                 as placement,
            case when (len(ISNULL(t1.rt_2_dest,'')) = 0) then t1.rt_1_dest
            when t1.rt_1_orig = t1.rt_2_dest and t1.rt_2_orig = t1.rt_1_dest then t1.rt_1_dest
            when t1.rt_2_dest in ('PEK','BOS','ORD','DEL','DEN','HKG','IAH','LAS','LHR','LAX','MEX','BOM','MCO','SFO','GRU','SEA','PVG','TLV','HND','IAD') then t1.rt_2_dest
            when t1.rt_1_dest in ('PEK','BOS','ORD','DEL','DEN','HKG','IAH','LAS','LHR','LAX','MEX','BOM','MCO','SFO','GRU','SEA','PVG','TLV','HND','IAD') then t1.rt_1_dest
            else t1.rt_2_dest
            end                                                                                          as destination,
            t1.rt_1_dest                                                                                 as route_1_destination,
            t1.rt_2_dest                                                                                 as route_2_destination,
            t1.rt_1_orig                                                                                 as route_1_origin,
            t1.rt_2_orig                                                                                 as route_2_origin,
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
            sum(t1.led)                                                                                  as led,
            sum(t1.tix)                                                                                  as tix,
            sum(case
                when t1.site_id_dcm = 1190273 -- Adara
                then cast(((t1.rev)  * .048)  as decimal(10,2))
                when t1.site_id_dcm = 3267410 -- Quantcast
                then cast(((t1.rev)  * .048)  as decimal(10,2))
                when t1.site_id_dcm = 1239319 -- Sojern
                then cast(((t1.rev)  * .048) as decimal(10,2)) end)  as rev,

--                 case when t1.site_id_dcm = 1190273 -- Adara
--                     then cast(sum(cast(impressions as decimal(20,10))) * 0.43)
--                when t1.site_id_dcm = 3267410 -- Quantcast
--                then cast(sum(cast(impressions as decimal(20,10))) * 0.71)
--                 when t1.site_id_dcm = 1239319 -- Sojern
--                     then cast(sum(cast(impressions as decimal(20,10))) * 0.64) end                      as cost,

            case when cast(month(prs.placementend) as int) - cast(month(cast(t1.dcmdate as date)) as int) <= 0 then 0
            else cast(month(prs.placementend) as int) - cast(month(cast(t1.dcmdate as date)) as int) end as diff,
            [dbo].udf_dvMap(t1.campaign_id,t1.site_id_dcm,t1.placement,prs.CostMethod,prs.dv_map)        as dv_map


-- openquery function call must not exceed 8,000 characters; no room for comments inside the function
        from (
                 select *
                 from openQuery(redshift,
                                '
                                select
                                con_date as dcmdate,
                                c1.campaign,
                                t1.campaign_id,
                                s1.site_dcm,
                                t1.site_id_dcm,
                                p1.placement,
                                t1.placement_id,
                                rt_1_orig,
                                rt_1_dest,
                                rt_2_orig,
                                rt_2_dest,
                                count(imp_nbr) as impressions,
                                sum(led)          as led,
                                sum(tix)          as tix,
                                sum(rev) as rev
                                from wmprodfeeds.united.ual_route_cost1 as t1

                                left join
(
select cast (campaign as varchar (4000)) as campaign,campaign_id as campaign_id
from wmprodfeeds.united.dfa2_campaigns
) as c1
on t1.campaign_id = c1.campaign_id


left join
(
select cast (placement as varchar (4000)) as placement,placement_id as placement_id
from wmprodfeeds.united.dfa2_placements
) as p1
on t1.placement_id = p1.placement_id


left join
(
select cast (site_dcm as varchar (4000)) as site_dcm,site_id_dcm as site_id_dcm
from wmprodfeeds.united.dfa2_sites
) as s1
on t1.site_id_dcm = s1.site_id_dcm

                                where regexp_instr(p1.placement,''.*_Route.*'') > 0
                                and t1.site_id_dcm in (1190273, 1239319, 3267410)
                                and t1.campaign_id = 20606595
                                and con_date between ''2018-06-01'' and ''2018-07-31''

                                group by

                                con_date,
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

          where t1.dcmdate between '2018-06-01' and '2018-07-31'
          and t1.placement like '%Route%'

        group by
            t1.dcmdate
            ,cast(month(cast(t1.dcmdate as date)) as int)
            ,t1.campaign
            ,t1.campaign_id
            ,t1.site_dcm
            ,t1.site_id_dcm
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


where
t2.route_1_origin LIKE '%EWR%'
AND (
  t2.destination like '%PEK%'
  or t2.destination like '%BOS%'
  or t2.destination like '%ORD%'
  or t2.destination like '%DEL%'
  or t2.destination like '%DEN%'
  or t2.destination like '%HKG%'
  or t2.destination like '%IAH%'
  or t2.destination like '%LAS%'
  or t2.destination like '%LHR%'
  or t2.destination like '%LAX%'
  or t2.destination like '%MEX%'
  or t2.destination like '%BOM%'
  or t2.destination like '%MCO%'
  or t2.destination like '%SFO%'
  or t2.destination like '%GRU%'
  or t2.destination like '%SEA%'
  or t2.destination like '%PVG%'
  or t2.destination like '%TLV%'
  or t2.destination like '%HND%'
  or t2.destination like '%IAD%'

)

group by
    t2.dcmdate,
    t2.route_1_origin,
    t2.destination,
    t2.site
