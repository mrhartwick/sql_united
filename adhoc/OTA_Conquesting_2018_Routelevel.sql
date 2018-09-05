
--OTA ORIGIN & DESTINATIONS: COST-EFFICIENT METRICS
--Step1: Create table that includes imps, rev, tickets, and leads in the route-level (RUN THIS IN VERTICA)

create table wmprodfeeds.united.ual_ota_route_1
(
user_id      varchar(50),
con_date      date,
imp_date      date,
campaign_id  int,
site_id_dcm  int,
placement_id int,
rt_1_orig    varchar(3),
rt_1_dest    varchar(3),
rt_2_orig    varchar(3),
rt_2_dest    varchar(3),
imp_nbr      int,
led          int,
tix          int,
rev          decimal(20,10)
);

insert into  wmprodfeeds.united.ual_ota_route_1
(user_id,
con_date,
imp_date,
campaign_id,
site_id_dcm,
placement_id,
rt_1_orig,
rt_1_dest,
rt_2_orig,
rt_2_dest,
imp_nbr,
led,
tix,
rev)

    (select

       ta.user_id
      ,ta.con_date as con_date
      ,ti.imp_date as imp_date
      ,ta.campaign_id
      ,ta.site_id_dcm
      ,ta.placement_id
      ,ta.rt_1_orig
      ,ta.rt_1_dest
      ,ta.rt_2_orig
      ,ta.rt_2_dest
      ,ti.imp_nbr
      ,ta.led
      ,ta.tix
      ,ta.rev

        from
        (
            select
            user_id
            ,interaction_time as con_tim
            ,md_interaction_date_loc as con_date
            ,campaign_id
            ,site_id_dcm
            ,placement_id
            ,case when activity_id = 978826 and total_revenue <> 0 then total_conversions else 0 end as tix
            ,case when activity_id = 1086066 then 1 else 0 end as led
            ,case when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') = 0 then cast(((a.total_revenue * 1000000) / (rates.exchange_rate)) as decimal(20,10))
                  when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') > 0 then cast(((a.total_revenue*1000)/.0103) as decimal(20,10)) end as rev
            ,Left(Right(other_data, Len(other_data) - Position('u5=' IN other_data) -2 ),Position(';' IN Right(other_data, Len(other_data) - Position('u5=' IN other_data) -2 ) ) -1)::VARCHAR(3) AS rt_1_orig
            ,CASE WHEN Len(Left(Right(other_data, Len(other_data) - Position('u7=' IN other_data) -2 ),Position(';' IN Right(other_data, Len(other_data) - Position('u7=' IN other_data) -2 ) ) -1) ) =3
                  THEN Left(Right(other_data, Len(other_data) - Position('u7=' IN other_data) -2 ),Position(';' IN Right(other_data, Len(other_data) - Position('u7=' IN other_data) -2 ) ) -1)
                  ELSE Left(Right(Left(Right(other_data, Len(other_data) - Position('u7=' IN other_data) -2 ),Position(';' IN Right(other_data, Len(other_data) - Position('u7=' IN other_data) -2 ) ) -1),LEN(Left(Right(other_data, Len(other_data) - Position('u7=' IN other_data) -2 ),Position(';' IN Right(other_data, Len(other_data) - Position('u7=' IN other_data) -2 ) ) -1)) - Position('(' IN Left(Right(other_data, Len(other_data) - Position('u7=' IN other_data) -2 ),Position(';' IN Right(other_data, Len(other_data) - Position('u7=' IN other_data) -2 ) ) -1) )),3)
             END ::VARCHAR(3) AS rt_1_dest
             ,CASE WHEN Position('u6=--' IN other_data) > 0
                 THEN NULL
                 ELSE Left(Right(other_data, Len(other_data) - Position('u6=' IN other_data) -2 ),Position(';' IN Right(other_data, Len(other_data) - Position('u6=' IN other_data) -2 ) ) -1)
              END ::VARCHAR(3) AS rt_2_orig
              ,CASE WHEN Position('u8=--' IN other_data) > 0
                  THEN NULL
                  ELSE CASE WHEN Len(Left(Right(other_data, Len(other_data) - Position('u8=' IN other_data) -2 ),Position(';' IN Right(other_data, Len(other_data) - Position('u8=' IN other_data) -2 ) ) -1)) = 3
                            THEN Left(Right(other_data, Len(other_data) - Position('u8=' IN other_data) -2 ),Position(';' IN Right(other_data, Len(other_data) - Position('u8=' IN other_data) -2 ) ) -1)
                            ELSE Left(Right(Left(Right(other_data, Len(other_data) - Position('u8=' IN other_data) -2 ),Position(';' IN Right(other_data, Len(other_data) - Position('u8=' IN other_data) -2 ) ) -1),LEN(Left(Right(other_data, Len(other_data) - Position('u8=' IN other_data) -2 ),Position(';' IN Right(other_data, Len(other_data) - Position('u8=' IN other_data) -2 ) ) -1)) - Position('(' IN Left(Right(other_data, Len(other_data) - Position('u8=' IN other_data) -2 ),Position(';' IN Right(other_data, Len(other_data) - Position('u8=' IN other_data) -2 ) ) -1) )),3)
                       END
               END ::VARCHAR(3) AS rt_2_dest


               from wmprodfeeds.united.dfa2_activity a

               left join wmprodfeeds.exchangerates.exchange_rates as rates
              on upper(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3)) = upper(rates.currency)
              and md_event_date_loc = rates.date

            where md_interaction_date_loc between '2018-07-01' and '2018-07-31'
            and (activity_id = 978826 or activity_id = 1086066)
            and campaign_id = 21375351
            and (advertiser_id <> 0)
            and (length(isnull(event_sub_type,'')) > 0)
            ) as ta,

            (
            select
                user_id
                ,event_time as impressiontime
                ,md_event_date_loc as imp_date
                ,row_number() over() as imp_nbr
            from wmprodfeeds.united.dfa2_impression

             where md_event_date_loc between '2018-07-01' and '2018-07-31'
            and campaign_id = 21375351
            and (advertiser_id <> 0)
            ) as ti

      where
      ta.user_id = ti.user_id
      and impressiontime = con_tim);

commit;

--============================================================================================================================================================================
--RUN THIS IN SQL: FINAL PULL

select
    t2.dcmdate               as "Date",
    t2.campaign              as campaign,
    t2.route_1_origin        as origin,
    t2.destination           as destination,
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
            sum(cast(((t1.rev) * 0.0480) as decimal (10,2)))  as rev,

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
                                from wmprodfeeds.united.ual_ota_route_1 as t1

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

                                where t1.campaign_id = 21375351
                                and con_date between ''2018-07-01'' and ''2018-07-31''

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

          where t1.dcmdate between '2018-07-01' and '2018-07-31'

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

group by
    t2.dcmdate,
    t2.campaign,
    t2.route_1_origin,
    t2.destination
