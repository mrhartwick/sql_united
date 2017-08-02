create procedure dbo.crt_dv_summ
as
if OBJECT_ID('master.dbo.dv_summ',N'U') is not null
    drop table master.dbo.dv_summ;


create table master.dbo.dv_summ
(
    joinKey                     varchar(255) not null,
    dvDate                      date         not null,
    media_property              varchar(255) not null,
    campaign_name               varchar(255),
--     placement_code              varchar(255) not null,
    placement_name              varchar(255),
    total_impressions           int          not null,
    groupm_passed_impressions   int          not null,
    groupm_billable_impressions int          not null

);

insert into master.dbo.dv_summ
    select distinct
        DV.joinKey,
        DV.dvDate,
        DV.media_property,
        DV.campaign_name,
--         DV.placement_code,
        DV.placement_name,
        sum(DV.total_impressions),
                sum(DV.groupm_passed_impressions),
        sum(DV.groupm_billable_impressions)
    from (select
              cast(t1.dvDate as date)                        as dvDate,
              replace(left(t1.placement_name,6) + '_' + [dbo].udf_siteKey(t1.media_property) + '_' +
                          cast(t1.dvDate as varchar),' ','') as joinKey,

              t1.campaign_name                               as campaign_name,
              [dbo].udf_siteName(t1.media_property)          as media_property,
--              Removing placement code - sometimes it matches DCM; sometimes not. Creates duplicates in JOIN to DCM table
--               case when t1.placement_code in ('137412510','137412401','137412609') then '137412609'
--               else t1.placement_code end                     as placement_code,
              case when
                  t1.placement_name like 'PBKB7J%' or t1.placement_name like 'PBKB7H%' or
                      t1.placement_name like 'PBKB7K%' or
                      t1.placement_name =
                          'United 360 - Polaris 2016 - Q4 - Amobee' then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
              else t1.placement_name end                     as placement_name,
              t1.total_impressions                           as total_impressions,
              t1.groupm_passed_impressions as groupm_passed_impressions,
              t1.groupm_billable_impressions                 as groupm_billable_impressions

          from (
                   select
                        [date] as dvDate,
                       campaign_name,
                       media_property,
                       placement_name,
--                        p1.placement,
                       case when
                           (len(isnull(placement_code,''))=0) then cast(0 as int)
                           else cast(placement_code as int) end as placement_code,
--                        p2.placement_id,
                        gm_active_impressions as total_impressions,
                        gm_active_impressions as groupm_passed_impressions,
                       gm_billable_impressions                 as groupm_billable_impressions
--


                from [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DV_SUM as t0

--                                    left join
--                      (
--                          select *
--                          from openQuery(VerticaUnited,
--                                         '
--                                       select cast (p1.placement as varchar (4000)) as ''placement'',p1.placement_id as ''placement_id'',p1.campaign_id as ''campaign_id'',p1.site_id_dcm as ''site_id_dcm''
--                                         from ( select campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast (placement_start_date as date ) as thisdate,
--                                         row_number() over (partition by campaign_id,site_id_dcm,placement_id order by cast (placement_start_date as date ) desc ) as x1
--                                         from diap01.mec_us_united_20056.dfa2_placements
--
--                                         ) as p1
--                                         where x1 = 1
--                                         and  left(p1.placement,6) like ''P%''
--                                         '
--                          )) as p1
--                          on cast(t0.placement_code as int) = p1.placement_id
--
--                                                left join
--                      (
--                          select *
--                          from openQuery(VerticaUnited,
--                                         '
--                                         select cast (p1.placement as varchar (4000)) as ''placement'',p1.placement_id as ''placement_id'',p1.campaign_id as ''campaign_id'',p1.site_id_dcm as ''site_id_dcm'',p1.placementnumber
--                                         from ( select campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,   left(cast(placement as varchar(4000)), 6) as PlacementNumber,cast (placement_start_date as date ) as thisdate,
--                                         row_number() over (partition by campaign_id,site_id_dcm,placement_id order by cast (placement_start_date as date ) desc ) as x1
--                                         from diap01.mec_us_united_20056.dfa2_placements
--                                         ) as p1
--                                         where x1 = 1
--                                         and  left(p1.placement,6) like ''P%''
--                                         '
--                          )) as p2
--                          on left(t0.placement_name,6) = p2.PlacementNumber

                   where t0.placement_name <> 'Rolled-up Placements'


                   ) as t1

         ) as DV

    where DV.placement_name not like '%[Dd][Oo]%[Nn][Oo][Tt]%[Uu][Ss][Ee]%'
        and DV.placement_name not like '%[Nn]o%[Tt]racking%'
--         and dv.placement_code != '300492'
        and cast(DV.dvDate as date) >= '2017-01-01'
    group by
        DV.joinKey,
        DV.dvDate,
        DV.media_property,
        DV.campaign_name,
--         DV.placement_code,
        DV.placement_name
go
