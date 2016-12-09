alter procedure dbo.createDVTbl
as
if OBJECT_ID('master.dbo.DVTable',N'U') is not null
    drop table master.dbo.DVTable;


create table master.dbo.DVTable
(
    joinKey                     varchar(255) not null,
    dvDate                      date         not null,
    media_property              varchar(255) not null,
    campaign_name               varchar(255),
    placement_code              varchar(255) not null,
    placement_name              varchar(255),
    total_impressions           int          not null,
    groupm_passed_impressions   int          not null,
    groupm_billable_impressions int          not null

);

insert into master.dbo.DVTable
    select distinct
        DV.joinKey,
        DV.dvDate,
        DV.media_property,
        DV.campaign_name,
        DV.placement_code,
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
              case when t1.placement_code in ('137412510','137412401','137412609') then '137412609'
              else t1.placement_code end                     as placement_code,
              case when
                  t1.placement_name like 'PBKB7J%' or t1.placement_name like 'PBKB7H%' or
                      t1.placement_name like 'PBKB7K%' or
                      t1.placement_name =
                          'United 360 - Polaris 2016 - Q4 - Amobee' then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
              else t1.placement_name end                     as placement_name,
              t1.total_impressions                           as total_impressions,
              t1.groupm_passed_impressions                   as groupm_passed_impressions,
              t1.groupm_billable_impressions                 as groupm_billable_impressions

          from (
                   select *
                   from openQuery(VerticaUnited,
                                  'select
                                  cast(event_date as date)                                    as ''dvDate'',
                                  cast(campaign_name as varchar(255))                        	as ''campaign_name'',
                                  cast(media_property as varchar(255))                        as ''media_property'',
                                  cast(placement_code as varchar(255))                        as ''placement_code'',
                                  cast(placement_name as varchar(255))                        as ''placement_name'',
                                  total_impressions                                     		as ''total_impressions'',
                                  groupm_passed_impressions                             		as ''groupm_passed_impressions'',
                                  groupm_billable_impressions                           		as ''groupm_billable_impressions''
                                  from diap01.mec_us_united_20056.dv_impression_agg')) as t1

         ) as DV

    where DV.placement_name not like '%[Dd][Oo]%[Nn][Oo][Tt]%[Uu][Ss][Ee]%'
        and DV.placement_name not like '%[Nn]o%[Tt]racking%'
        and dv.placement_code != '300492'
        and cast(DV.dvDate as date) > '2016-01-01'
    group by
        DV.joinKey,
        DV.dvDate,
        DV.media_property,
        DV.campaign_name,
        DV.placement_code,
        DV.placement_name
go