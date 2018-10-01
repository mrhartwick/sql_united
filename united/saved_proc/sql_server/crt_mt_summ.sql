CREATE procedure dbo.crt_mt_summ
as
if OBJECT_ID('master.dbo.mt_summ',N'U') is not null
  drop table master.dbo.mt_summ;


create table master.dbo.mt_summ
(
  joinKey                     varchar(255),
  mtDate                      date         not null,
  media_property              varchar(255) not null,
  campaign_name               varchar(255),
  placement_code              varchar(255),
  placement_name              varchar(255),
  total_impressions           int          not null,
  groupm_passed_impressions   int          not null,
  groupm_billable_impressions int          not null
);

insert into master.dbo.mt_summ
  select distinct
    replace(left(t2.placement_name,6) + '_' +
              [dbo].udf_siteKey(t2.media_property)
              + '_' + cast(t2.mtDate as varchar(10)),' ','') as joinKey,
    t2.mtDate,
    [dbo].udf_siteName(t2.media_property)                      as media_property,
    t2.campaign_name                                           as campaign_name,
    t2.placement_code                                          as placement_code,
    t2.placement_name                                          as placement_name,
    isnull(sum(t2.total_impressions),0)                        as total_impressions,
    isnull(sum(t2.groupm_passed_impressions),0)                as groupm_passed_impressions,
    isnull(sum(t2.groupm_billable_impressions),0)              as groupm_billable_impressions
  from (


         select
           t1.mtDate                           as mtDate,
           case when
             (LEN(ISNULL(t1.media_property,'')) = 0) then s1.site_dcm
           else t1.media_property end          as media_property,
           c1.campaign                         as campaign_name,
           t1.campaign_id                      as campaign_id,
           t1.site_id                          as site_id,
           t1.placement_code                   as placement_code,
           case when
             (LEN(ISNULL(t1.placement_name,'')) = 0) then p3.placement
           else t1.placement_name end          as placement_name,
           sum(t1.total_impressions)           as total_impressions,
           sum(t1.groupm_passed_impressions)   as groupm_passed_impressions,
           sum(t1.groupm_billable_impressions) as groupm_billable_impressions

         from (

                select
                  MT.mtDate                          as mtDate,
                  MT.site_label                      as media_property,
                  MT.campaign_label                  as campaign_name,
                  case when (
                    (LEN(ISNULL(cast(mt.campaign_id as varchar),'')) != 0)
                      and (LEN(ISNULL(cast(mt.campaign_id as varchar),'')) > 6)
                      and (LEN(ISNULL(cast(mt.campaign_id as varchar),'')) < 9)
                  )
                    then mt.campaign_id
                  when (
                    ((LEN(ISNULL(cast(mt.campaign_id as varchar),'')) = 0) or
                      (LEN(ISNULL(cast(mt.campaign_id as varchar),'')) > 9) or
                      (LEN(ISNULL(cast(mt.campaign_id as varchar),'')) < 6)
                    )
                      and (LEN(ISNULL(cast(p1.campaign_id as varchar),'')) = 0))
                    then p2.campaign_id
                  when (
                    ((LEN(ISNULL(cast(mt.campaign_id as varchar),'')) = 0) or
                      (LEN(ISNULL(cast(mt.campaign_id as varchar),'')) > 8))
                      and (LEN(ISNULL(cast(p1.campaign_id as varchar),'')) != 0))
                    then p1.campaign_id
                  else mt.campaign_id
                  end                                as campaign_id,

                  case when
                    MT.site_label = 'Amobee' then '1853562'
                  when (LEN(ISNULL(cast(MT.site_id as varchar),'')) = 7)
                    then MT.site_id
                  when (LEN(ISNULL(cast(MT.site_id as varchar),'')) < 7) and
                    (LEN(ISNULL(cast(p1.site_id_dcm as varchar),'')) = 0)
                    then p2.site_id_dcm
                  else p1.site_id_dcm
                  end                                as site_id,

                  case when
                    (LEN(ISNULL(cast(mt.placement_id as varchar),'')) < 9)
                    then p2.placement_id
                  else p1.placement_id
                  end                                as placement_code,

                  MT.placement_label                 as placement_name,

                  sum(MT.human_impressions)          as total_impressions,
                  sum(MT.half_duration_impressions)  as groupm_passed_impressions,
                  sum(MT.groupm_payable_impressions) as groupm_billable_impressions

                from (select
                  cast(mt1.mtDate as date)            as mtDate,
                  mt1.campaign_label                  as campaign_label,
                  case when mt1.campaign_id = '33809' then '10121649'
                  when mt1.campaign_id = '33809' then '10121649'
                  when mt1.campaign_id = '34957' then '10276123'
                  when mt1.campaign_id = '26315' then '8955169'
                  else mt1.campaign_id end            as campaign_id,

                  [dbo].udf_siteKey(mt1.site_label)   as site_label,
--                site_label                      as site_label,
                  mt1.site_id                         as site_id,
                  case when mt1.placement_id = 'undefined' then 'undefined'
                  when mt1.placement_id = '137412609' or mt1.placement_id = '300459' or mt1.placement_id = '325988' or mt1.placement_id = '137412510'
                    then '137412609'
                  else mt1.placement_id end           as placement_id,
                  case when mt1.placement_label like 'PBKB7J%' or mt1.placement_label like 'PBKB7H%' or mt1.placement_label like 'PBKB7K%' or mt1.placement_label = 'United 360 - Polaris 2016 - Q4 - Amobee' then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
                  else mt1.placement_label end        as placement_label,
                  sum(mt1.human_impressions)          as human_impressions,
                  sum(mt1.half_duration_impressions)  as half_duration_impressions,
                  sum(mt1.groupm_payable_impressions) as groupm_payable_impressions

                from (
                       select
                         t0.mtDate,
                         t0.campaign_id,
                         t0.campaign_label,
                         t0.site_id,
                         t0.site_label,
--                         case when isnumeric(t0.placement_id) = 1 then t0.placement_id else 0 end as placement_id,
                         t0.placement_id,
                         t0.placement_label,
                         t0.human_impressions,
                         t0.half_duration_impressions,
                         t0.groupm_payable_impressions

                       from (
                              select *
                              from openquery(VerticaUnited,
                               '
                                     select
                     cast(event_date as date)                                    as ''mtDate'',
                     cast(campaign_id as varchar(255))                          as ''campaign_id'',
                     cast(campaign_label as varchar(255))                         as ''campaign_label'',
                     cast(site_id as varchar(255))                        as ''site_id'',
                     cast(site_label as varchar(255))                        as ''site_label'',
                     cast(placement_id as varchar(255))                        as ''placement_id'',
                     cast(placement_label as varchar(255))                        as ''placement_label'',
                     human_impressions                                        as ''human_impressions'',
                     half_duration_impressions                                as ''half_duration_impressions'',
                     groupm_payable_impressions                               as ''groupm_payable_impressions''
                     from diap01.mec_us_united_20056.moat_impression
              where REGEXP_LIKE(cast(placement_id as varchar(255)),''\d'',''ib'')
                                         and campaign_id <> ''undefined''
                                            and site_id <> ''undefined''
                                and placement_id <> ''undefined''

                                         '

                              )) as t0

                     ) as mt1

                group by
                  cast(mt1.mtDate as date),
                  mt1.campaign_label,
                  mt1.campaign_id,
                  mt1.site_label,
                  mt1.site_id,
                  mt1.placement_id,
                  mt1.placement_label,
                  mt1.human_impressions,
                  mt1.half_duration_impressions,
                  mt1.groupm_payable_impressions


                     ) as MT

--                            Placements 1
                  left join
                  (
                    select *
                    from openQuery(VerticaUnited,
                                   '
                  select cast (p1.placement as varchar (4000)) as ''placement'',p1.placement_id as ''placement_id'',p1.campaign_id as ''campaign_id'',p1.site_id_dcm as ''site_id_dcm''
from ( select campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast (placement_start_date as date ) as thisdate,
row_number() over (partition by campaign_id,site_id_dcm,placement_id order by cast (placement_start_date as date ) desc ) as x1
from diap01.mec_us_united_20056.dfa2_placements

) as p1
where x1 = 1
                  and  left(p1.placement,6) like ''P%''
                       '
                    )) as p1
                  on mt.placement_id = cast(p1.placement_id as varchar)

--                            Placements 2
                  left join
                  (
                    select *
                    from openQuery(VerticaUnited,
                                   '
select cast (p1.placement as varchar (4000)) as ''placement'',p1.placement_id as ''placement_id'',p1.campaign_id as ''campaign_id'',p1.site_id_dcm as ''site_id_dcm'',p1.placementnumber
from ( select campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,   left(cast(placement as varchar(4000)), 6) as PlacementNumber,cast (placement_start_date as date ) as thisdate,
row_number() over (partition by campaign_id,site_id_dcm,placement_id order by cast (placement_start_date as date ) desc ) as x1
from diap01.mec_us_united_20056.dfa2_placements

) as p1
where x1 = 1
                  and  left(p1.placement,6) like ''P%''
                       '
                    )) as p2
                  on left(mt.placement_label,6) = p2.PlacementNumber


--                where MT.placement_id != 'undefined'
                group by
-- MT.joinKey,
                  MT.mtDate,
                  MT.site_label,
                  MT.campaign_label,
                  MT.placement_id,
                  MT.site_id,
                  mt.campaign_id,
                  p1.campaign_id,
                  p2.campaign_id,
                  p1.site_id_dcm,
                  p2.site_id_dcm,
                  p1.placement_id,
                  p2.placement_id,
                  MT.placement_label
              ) as t1

--                   Campaigns
           left join
           (
             select *
             from openQuery(VerticaUnited,
                            '
                  select c1.campaign_id, c1.campaign
                  from (

                  select
                  cast(campaign as varchar(4000)) as ''campaign'', campaign_id as ''campaign_id''
                       from diap01.mec_us_united_20056.dfa2_campaigns
                  ) as c1

                  group by c1.campaign_id, c1.campaign'
             )) as c1
           on t1.campaign_id = cast(c1.campaign_id as varchar)

--                   Sites
           left join
           (
             select *
             from openQuery(VerticaUnited,
                            '
                  select s1.site_id_dcm, s1.site_dcm
                  from (

                  select
                  cast(site_dcm as varchar(4000)) as ''site_dcm'', site_id_dcm as ''site_id_dcm''
       from diap01.mec_us_united_20056.dfa2_sites
         ) as s1

                  group by s1.site_dcm, s1.site_id_dcm'
             )) as s1
           on t1.site_id = cast(s1.site_id_dcm as varchar)

--                   Placements 3
           left join
           (
             select *
             from openQuery(VerticaUnited,
                            '
                  select cast (p1.placement as varchar (4000)) as ''placement'',p1.placement_id as ''placement_id'',p1.campaign_id as ''campaign_id'',p1.site_id_dcm as ''site_id_dcm''
                  from ( select campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast (placement_start_date as date ) as thisdate,
                  row_number() over (partition by campaign_id,site_id_dcm,placement_id order by cast (placement_start_date as date ) desc ) as x1
                  from diap01.mec_us_united_20056.dfa2_placements

                  ) as p1
                  where x1 = 1
                  and  left(p1.placement,6) like ''P%''
                  '
             )) as p3
           on t1.placement_code = cast(p3.placement_id as varchar)

         group by
           t1.mtDate,
           t1.media_property,
           s1.site_dcm,
           t1.campaign_id,
           t1.site_id,
           t1.placement_code,
           c1.campaign,
           p3.placement,
           t1.placement_name

       ) as t2
  group by
-- t2.joinKey,
    t2.mtDate,
    t2.media_property,
    t2.campaign_name,
    t2.placement_code,
    t2.placement_name
go
