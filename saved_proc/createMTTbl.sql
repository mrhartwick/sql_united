alter procedure dbo.createMTTbl
as
    if OBJECT_ID('master.dbo.MTTable',N'U') is not null
        drop table master.dbo.MTTable;


    create table master.dbo.MTTable
    (
        joinKey                     varchar(255),
        mtDate                      date         not null,
        media_property              varchar(255) not null,
        campaign_name               varchar(255) not null,
        placement_code              varchar(255),
        placement_name              varchar(255),
        total_impressions           int          not null,
        groupm_passed_impressions   int          not null,
        groupm_billable_impressions int          not null
    );

    insert into master.dbo.MTTable
        select distinct
            replace(left(t2.placement_name,6) + '_' +
                        [dbo].udf_siteKey(t2.media_property)
                        + '_' + cast(t2.mtDate as varchar(10)),' ','') as joinKey,
            t2.mtDate,
            [dbo].udf_siteName(t2.media_property)                      as media_property,
            t2.campaign_name                                           as campaign_name,
            t2.placement_code                                          as placement_code,
            t2.placement_name                                          as placement_name,
            isnull(sum(t2.total_impressions),0)                                  as total_impressions,
            isnull(sum(t2.groupm_passed_impressions),0)                          as groupm_passed_impressions,
            isnull(sum(t2.groupm_billable_impressions),0)                        as groupm_billable_impressions
        from (


                 select
                     t1.mtDate                           as mtDate,
                     case when
                         (LEN(ISNULL(t1.media_property,'')) = 0) then s1.directory_site
                     else t1.media_property end          as media_property,
                     c1.buy                              as campaign_name,
                     t1.campaign_id                      as campaign_id,
                     t1.site_id                          as site_id,
                     t1.placement_code                   as placement_code,
                     case when
                         (LEN(ISNULL(t1.placement_name,'')) = 0) then p3.site_placement
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
                                      and (LEN(ISNULL(cast(p1.order_id as varchar),'')) = 0))
                                  then p2.order_id
                              when (
                                  ((LEN(ISNULL(cast(mt.campaign_id as varchar),'')) = 0) or
                                      (LEN(ISNULL(cast(mt.campaign_id as varchar),'')) > 8))
                                      and (LEN(ISNULL(cast(p1.order_id as varchar),'')) != 0))
                                  then p1.order_id
                              else mt.campaign_id
                              end                                as campaign_id,

                              case when
                                  MT.site_label = 'Amobee' then '1853562'
                              when (LEN(ISNULL(cast(mt.site_id as varchar),'')) = 7)
                                  then mt.site_id
                              when (LEN(ISNULL(cast(mt.site_id as varchar),'')) < 7) and
                                  (LEN(ISNULL(cast(p1.site_id as varchar),'')) = 0)
                                  then p2.site_id
                              else p1.site_id
                              end                                as site_id,

                              case when
                                  (LEN(ISNULL(cast(mt.placement_id as varchar),'')) < 9)
                                  then p2.page_id
                              else p1.page_id
                              end                                as placement_code,

                              MT.placement_label                 as placement_name,

                              sum(MT.human_impressions)          as total_impressions,
                              sum(MT.half_duration_impressions)  as groupm_passed_impressions,
                              sum(MT.groupm_payable_impressions) as groupm_billable_impressions

                          from (select
                                    cast(mt1.mtDate as date)              as mtDate,
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
                                             t0.placement_id,
                                             t0.placement_label,
                                             t0.human_impressions,
                                             t0.half_duration_impressions,
                                             t0.groupm_payable_impressions

                                         from (
                                                  select *
                                                  from openquery(VerticaUnited,
                                                                 'select
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
                                                      where REGEXP_LIKE(cast(placement_id as varchar(255)),''\d'',''ib'')'

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

                              --               Placements 1

                              left join
                              (
                                  select *
                                  from openQuery(VerticaUnited,
                                                 '
                                                select cast(p1.site_placement as varchar(4000)) as ''site_placement'',  p1.page_id as ''page_id'', p1.order_id as ''order_id'', p1.site_id as ''site_id''

                                                from (select order_id as order_id, site_id as site_id, page_id as page_id, site_placement as site_placement, cast(start_date as date) as thisDate,
                                                row_number() over (partition by order_id, site_id, page_id  order by cast(start_date as date) desc) as r1
                                                FROM diap01.mec_us_united_20056.dfa_page
                                                ) as p1
                                                where r1 = 1
                                                and  left(site_placement,6) like ''P%''
                                                 '
                                  )) as p1
                                  on cast(mt.placement_id as int) = p1.page_id

                              --               Placements 2

                              left join
                              (
                                  select *
                                  from openQuery(VerticaUnited,
                                                 '
                                                 select cast(p2.site_placement as varchar(4000)) as "site_placement",  p2.page_id as "page_id", p2.order_id as "order_id", p2.PlacementNumber as "PlacementNumber", p2.site_id as "site_id"

                                                 from (select order_id as order_id, site_id as site_id, page_id as page_id, site_placement as site_placement,   left(cast(site_placement as varchar(4000)), 6) as PlacementNumber, cast(start_date as date) as thisDate,
                                                 row_number() over (partition by order_id, site_id, page_id  order by cast(start_date as date) desc) as r1
                                                 FROM diap01.mec_us_united_20056.dfa_page
                                                 ) as p2
                                                 where r1 = 1
                                                 and  left(site_placement,6) like ''P%''
                                                 '
                                  )) as p2
                                  on left(mt.placement_label,6) = p2.PlacementNumber


                          where MT.placement_id != 'undefined'
                          group by
                              -- MT.joinKey,
                              MT.mtDate,
                              MT.site_label,
                              MT.campaign_label,
                              MT.placement_id,
                              MT.site_id,
                              mt.campaign_id,
                              p1.order_id,
                              p2.order_id,
                              p1.site_id,
                              p2.site_id,
                              p1.page_id,
                              p2.page_id,
                              MT.placement_label
                      ) as t1

                     --               Campaigns
                     left join
                     (
                         select *
                         from openQuery(VerticaUnited,
                                        '
                                        select c1.order_id, c1.buy
                                        from (

                                        select
                                        cast(buy as varchar(4000)) as "buy", order_id as "order_id"
                                               from diap01.mec_us_united_20056.dfa_campaign
                                        ) as c1

                                        group by c1.order_id, c1.buy'
                         )) as c1
                         on cast(t1.campaign_id as int) = c1.order_id


                     left join
                     (
                         select *
                         from openQuery(VerticaUnited,
                                        '
                                        select s1.site_id, s1.directory_site
                                        from (

                                        select
                                        cast(directory_site as varchar(4000)) as "directory_site", site_id as "site_id"
                                                from diap01.mec_us_united_20056.dfa_site
                                        ) as s1

                                        group by s1.directory_site, s1.site_id'
                         )) as s1
                         on cast(t1.site_id as int) = s1.site_id

                     -- Placements 3

                     left join
                     (
                         select *
                         from openQuery(VerticaUnited,
                                        '
                                        select cast(p3.site_placement as varchar(4000)) as "site_placement",  p3.page_id as "page_id"
                                        from (select page_id as page_id, site_placement as site_placement, cast(start_date as date) as thisDate,
                                        row_number() over (partition by page_id  order by cast(start_date as date) desc) as r1
                                        FROM diap01.mec_us_united_20056.dfa_page
                                        ) as p3
                                        where r1 = 1
                                        and  left(site_placement,6) like ''P%''
                                        '
                         )) as p3
                         on cast(t1.placement_code as int) = p3.page_id

                 group by
                     t1.mtDate,
                     t1.media_property,
                     s1.directory_site,
                     t1.campaign_id,
                     t1.site_id,
                     t1.placement_code,
                     c1.buy,
                     p3.site_placement,
                     t1.placement_name

             ) as t2
        group by
            -- t2.joinKey,
            t2.mtDate,
            t2.media_property,
            t2.campaign_name,
            t2.placement_code,
            t2.placement_name