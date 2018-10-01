--====================================================================
-- Author:      Seetha Srinivasan
-- Create date:      11 Jul 2016 05:03:57 PM ET
-- Edited:           19 July 2017 by Matt Hartwick
-- Description: {Description}
-- Comments:
--====================================================================

--Placement
if object_id('tempdb..#tmp_placement') is not null
  drop table #tmp_placement

select distinct [placement id],
                placement

into   [#tmp_placement]
from   [dfid041758_ualus_dim_search_floodlight_extracted]

delete from ualus_dim_placement
where  isnull(placement_id, '0') in (select isnull([placement id], '0') placementid
                                     from   #tmp_placement)

insert into ualus_dim_placement
select distinct isnull(convert(nvarchar(50), [placement id]), '0') placementid,
                convert(nvarchar(1000), placement)                 placement

from   [#tmp_placement]
where  [placement id] not in (select placement_id
                              from   ualus_dim_placement)

--Placement from Search Standard Report
if object_id('tempdb..#tmp_placement_2') is not null
  drop table #tmp_placement_2

select distinct [placement id],
                placement

into   [#tmp_placement_2]
from   dfid041796_ualus_dim_search_standard_extracted

delete from ualus_dim_placement
where  isnull(placement_id, '0') in (select isnull([placement id], '0') placementid
                                     from   #tmp_placement_2)

insert into ualus_dim_placement
select distinct isnull(convert(nvarchar(50), [placement id]), '0')           placementid,
                convert(nvarchar(1000), placement)

from   [#tmp_placement_2]
where  [placement id] not in (select placement_id
                              from   ualus_dim_placement)

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

--Dim Campaign
if object_id('tempdb..#tmp_campaign') is not null
    drop table #tmp_campaign

select distinct
    [campaign id],
    campaign

        into [#tmp_campaign]

        from [dfid041758_ualus_dim_search_floodlight_extracted]

delete from ualus_dim_campaign
where isnull(campaign_id,'0') in (
        select isnull([campaign id],'0') campaignid
        from #tmp_campaign
        )

insert into ualus_dim_campaign
    select distinct
        isnull(convert(nvarchar(50),[campaign id]),'0') campaignid,
        convert(nvarchar(1000),campaign)                campaign
    from [#tmp_campaign]
    where [campaign id] not in (
        select campaign_id
        from ualus_dim_campaign
    )

--Campaign from Search Standard Feed
if object_id('tempdb..#tmp_campaign_2') is not null
    drop table #tmp_campaign_2

select distinct
    [campaign id],
    campaign

into [#tmp_campaign_2]
from dfid041796_ualus_dim_search_standard_extracted

delete from ualus_dim_campaign
where isnull(campaign_id,'0') in (
    select isnull([campaign id],'0') campaignid
    from #tmp_campaign_2
)

insert into ualus_dim_campaign
    select distinct
        isnull(convert(nvarchar(50),[campaign id]),'0') campaignid,
        convert(nvarchar(1000),campaign)                campaign
    from [#tmp_campaign_2]
    where [campaign id] not in (
        select campaign_id
        from ualus_dim_campaign
    )

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

--Dim Activity

if object_id('tempdb..#tmp_activity') is not null
  drop table [#tmp_activity]

select distinct [activity id],
                activity
into   [#tmp_activity]
from   [dfid041758_ualus_dim_search_floodlight_extracted]

delete from ualus_dim_activity
where  isnull(activity_id, '0') in (select isnull([activity id], '0') activityid
                                    from   #tmp_activity)

insert into ualus_dim_activity
select distinct isnull(convert(nvarchar(50), [activity id]), '0') activityid,
                convert(nvarchar(1000), activity)                 activity
from   [#tmp_activity]
where  [activity id] not in (select activity_id
                             from   ualus_dim_activity)

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- Dim Paid Search Engine

if object_id('tempdb..#tmp_pdsearchengine') is not null
  drop table #tmp_pdsearchengine

select distinct [paid search engine account id],
                [paid search engine account]
into   [#tmp_pdsearchengine]
from   dfid041796_ualus_dim_search_standard_extracted

delete from ualus_dim_paid_searchengine
where  isnull(paid_searchengine_id, '0') in (select isnull([paid search engine account id], '0') pdsearchengineaccountid
                                             from   [#tmp_pdsearchengine])

insert into ualus_dim_paid_searchengine
select distinct isnull(convert(nvarchar(50), [paid search engine account id]), '0') pdsearchengineaccountid,
                convert(nvarchar(1000), [paid search engine account])               pdsearchengineaccount
from   [#tmp_pdsearchengine]
where  [paid search engine account id] not in (select [paid_searchengine_id]
                                               from   ualus_dim_paid_searchengine)


-- Search Engine from Search Floodlight Feed
if object_id('tempdb..#tmp_pdsearchengine_2') is not null
  drop table #tmp_pdsearchengine_2

select distinct [paid search engine account id],
                [paid search engine account]
into   [#tmp_pdsearchengine_2]
from   DFID041761_UALUS_Search_Floodlight_Extracted

delete from ualus_dim_paid_searchengine
where  isnull(paid_searchengine_id, '0') in (select isnull([paid search engine account id], '0') pdsearchengineaccountid
                                             from   [#tmp_pdsearchengine_2])

insert into ualus_dim_paid_searchengine
select distinct isnull(convert(nvarchar(50), [paid search engine account id]), '0') pdsearchengineaccountid,
                convert(nvarchar(1000), [paid search engine account])               pdsearchengineaccount
from   [#tmp_pdsearchengine_2]
where  [paid search engine account id] not in (select [paid_searchengine_id]
                                               from   ualus_dim_paid_searchengine)

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

--Dim Paid Search Campaign

if object_id('tempdb..#tmp_pdsearchcampaign') is not null
  drop table #tmp_pdsearchcampaign

select distinct
                [paid search campaign id],
                [paid search campaign]
into   [#tmp_pdsearchcampaign]
from   dfid041796_ualus_dim_search_standard_extracted

delete from ualus_dim_paid_searchcampaign
where  isnull(paid_search_campaign_id, '0') in (
                                                select isnull([paid search campaign id], '0') pdsearchcampaignid
                                                from   [#tmp_pdsearchcampaign]
                                                )

insert into ualus_dim_paid_searchcampaign
select distinct
isnull(convert(nvarchar(50), [paid search campaign id]), '0') pdsearchcampaignid,
convert(nvarchar(1000), [paid search campaign])               pdsearchcampaign
from   [#tmp_pdsearchcampaign]
where  [paid search campaign id] not in (select [paid_search_campaign_id]
                                         from   [ualus_dim_paid_searchcampaign])


--Campaign from Search Floodlight Feed
if object_id('tempdb..#tmp_pdsearchcampaign_2') is not null
    drop table #tmp_pdsearchcampaign_2

select distinct
                [paid search campaign id],
                [paid search campaign]
into [#tmp_pdsearchcampaign_2]
from DFID041761_UALUS_Search_Floodlight_Extracted

delete from ualus_dim_paid_searchcampaign
where isnull(paid_search_campaign_id, '0') in (select isnull([paid search campaign id], '0') pdsearchcampaignid
from #tmp_pdsearchcampaign_2)

insert into ualus_dim_paid_searchcampaign
    select distinct
isnull(convert(nvarchar(50), [paid search campaign id]), '0') pdsearchcampaignid,
convert(nvarchar(1000), [paid search campaign])               pdsearchcampaign
    from [#tmp_pdsearchcampaign_2]
where  [paid search campaign id] not in (select [paid_search_campaign_id]
                                         from   [ualus_dim_paid_searchcampaign])




-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

--Dim Package/Roadblock
-- if object_id('tempdb..#tmp_package') is not null
--   drop table #tmp_package

-- select distinct [package/roadblock id],
--                 [package/roadblock]
-- into   #tmp_package
-- from   dfid041796_ualus_dim_search_standard_extracted

-- delete from ualus_dim_package
-- where  isnull(package_id, '0') in (select isnull([package/roadblock id], '0') packageid
--                                    from   #tmp_package)

-- insert into ualus_dim_package
-- select distinct isnull(convert(nvarchar(50), [package/roadblock id]), '0') packageid,
--                 convert(nvarchar(1000), [package/roadblock])               package
-- from   #tmp_package
-- where  [package/roadblock id] not in (select package_id
--                                       from   ualus_dim_package)

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

--Dim Site DCM

if object_id('tempdb..#tmp_sitedcm') is not null
  drop table #tmp_sitedcm

select distinct [site id (dcm)],
                [site (dcm)],
                [site id (site directory)],
                [site (site directory)]
into   #tmp_sitedcm
from   dfid041796_ualus_dim_search_standard_extracted

delete from ualus_dim_site
where  isnull(site_id_dcm, '0') in (select isnull([site id (dcm)], '0') siteid
                                    from   #tmp_sitedcm)

insert into ualus_dim_site
select distinct isnull(convert(nvarchar(50), [site id (dcm)]), '0')            siteid_dcm,
                convert(nvarchar(1000), [site (dcm)])                          site_dcm,
                isnull(convert(nvarchar(50), [site id (site directory)]), '0') siteid_sd,
                convert(nvarchar(1000), [site (dcm)])                          site_sd
from   #tmp_sitedcm
where  [site id (dcm)] not in (select site_id_dcm
                               from   ualus_dim_site)

-- Site from Search Floodlight Feed

if object_id('tempdb..#tmp_sitedcm_2') is not null
  drop table #tmp_sitedcm_2

select distinct [site id (dcm)],
                [site (dcm)],
                [site id (site directory)],
                [site (site directory)]
into   #tmp_sitedcm_2
from   DFID041761_UALUS_Search_Floodlight_Extracted

delete from ualus_dim_site
where  isnull(site_id_dcm, '0') in (select isnull([site id (dcm)], '0') siteid
                                    from   #tmp_sitedcm_2)

insert into ualus_dim_site
select distinct isnull(convert(nvarchar(50), [site id (dcm)]), '0')            siteid_dcm,
                convert(nvarchar(1000), [site (dcm)])                          site_dcm,
                isnull(convert(nvarchar(50), [site id (site directory)]), '0') siteid_sd,
                convert(nvarchar(1000), [site (dcm)])                          site_sd
from   #tmp_sitedcm_2
where  [site id (dcm)] not in (select site_id_dcm
                               from   ualus_dim_site)


-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

--Dim Paid Search Keyword

if object_id('tempdb..#tmp_pdsearchkwd') is not null
  drop table #tmp_pdsearchkwd

select distinct [paid search keyword id],
                [paid search keyword]

into   #tmp_pdsearchkwd
from   dfid041796_ualus_dim_search_standard_extracted

delete from ualus_dim_paid_searchkeyword
where  isnull(paid_search_keyword_id, '0') in (select isnull([paid search keyword id], '0') paidsearchkwid
                                               from   #tmp_pdsearchkwd)

insert into ualus_dim_paid_searchkeyword
select distinct isnull(convert(nvarchar(50), [paid search keyword id]), '0') paidsearchkwid,
                convert(nvarchar(1000), [paid search keyword])               paidsearchkeyword

from   #tmp_pdsearchkwd
where  [paid search keyword id] not in (select paid_search_keyword_id
                                        from   ualus_dim_paid_searchkeyword)


-- Keyword from Search Floodlight Feed

if object_id('tempdb..#tmp_pdsearchkwd_2') is not null
  drop table #tmp_pdsearchkwd_2

select distinct [paid search keyword id],
                [paid search keyword]

into   #tmp_pdsearchkwd_2
from   DFID041761_UALUS_Search_Floodlight_Extracted

delete from ualus_dim_paid_searchkeyword
where  isnull(paid_search_keyword_id, '0') in (select isnull([paid search keyword id], '0') paidsearchkwid
                                               from   #tmp_pdsearchkwd_2)

insert into ualus_dim_paid_searchkeyword
select distinct isnull(convert(nvarchar(50), [paid search keyword id]), '0') paidsearchkwid,
                convert(nvarchar(1000), [paid search keyword])               paidsearchkeyword

from   #tmp_pdsearchkwd_2
where  [paid search keyword id] not in (select paid_search_keyword_id
                                        from   ualus_dim_paid_searchkeyword)


-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

-- Dim Paid Search Ad

if object_id('tempdb..#tmp_pdsearchad') is not null
  drop table #tmp_pdsearchad

select distinct
                [paid search ad id],
                [paid search ad]
into   #tmp_pdsearchad
from   dfid041796_ualus_dim_search_standard_extracted

delete from ualus_dim_paid_searchad
where  exists (select 'x'
               from   #tmp_pdsearchad aa
               where
               aa.[paid search ad id] = paid_search_ad_id)

insert into ualus_dim_paid_searchad
select distinct

                isnull(convert(nvarchar(50), [paid search ad id]), '0')         paidsearchadid,
                convert(nvarchar(1000), [paid search ad])                       paidsearchad
from   #tmp_pdsearchad
where  not exists (select 'x'
                   from   ualus_dim_paid_searchad
                   where
                   [paid search ad id] = paid_search_ad_id)



-- Ad from Search Floodlight Feed

if object_id('tempdb..#tmp_pdsearchad_2') is not null
  drop table #tmp_pdsearchad_2

select distinct
                [paid search ad id],
                [paid search ad]
into   #tmp_pdsearchad_2
from   DFID041761_UALUS_Search_Floodlight_Extracted

delete from ualus_dim_paid_searchad
where  exists (select 'x'
               from   #tmp_pdsearchad_2 aa
               where
               aa.[paid search ad id] = paid_search_ad_id)

insert into ualus_dim_paid_searchad
select distinct

                isnull(convert(nvarchar(50), [paid search ad id]), '0')         paidsearchadid,
                convert(nvarchar(1000), [paid search ad])                       paidsearchad
from   #tmp_pdsearchad_2
where  not exists (select 'x'
                   from   ualus_dim_paid_searchad
                   where
                   [paid search ad id] = paid_search_ad_id)

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

--Dim Paid Search Ad Group

if object_id('tempdb..#tmp_pdsearchadgroup') is not null
  drop table #tmp_pdsearchadgroup

select distinct
                [paid search ad group id],
                [paid search ad group]

into   #tmp_pdsearchadgroup
from   dfid041796_ualus_dim_search_standard_extracted

delete from UALUS_DIM_Paid_SearchAdGroup
where  exists (select 'x'
               from   #tmp_pdsearchadgroup aa
               where
               aa.[paid search ad group id] = paid_search_adgroup_id
               )

insert into UALUS_DIM_Paid_SearchAdGroup
select distinct
                isnull(convert(nvarchar(50), [paid search ad group id]), '0')   paidsearchadgroupid,
                convert(nvarchar(1000), [paid search ad group])                 paidsearchadgroup
from   #tmp_pdsearchadgroup
where  not exists (select 'x'
                   from   UALUS_DIM_Paid_SearchAdGroup
                   where  [paid search ad group id] = paid_search_adgroup_id
                          )



-- Ad Group from Search Floodlight Feed

if object_id('tempdb..#tmp_pdsearchadgroup_2') is not null
  drop table #tmp_pdsearchadgroup_2

select distinct
                [paid search ad group id],
                [paid search ad group]

into   #tmp_pdsearchadgroup_2
from   DFID041761_UALUS_Search_Floodlight_Extracted

delete from UALUS_DIM_Paid_SearchAdGroup
where  exists (select 'x'
               from   #tmp_pdsearchadgroup_2 aa
               where
               aa.[paid search ad group id] = paid_search_adgroup_id
               )

insert into UALUS_DIM_Paid_SearchAdGroup
select distinct
                isnull(convert(nvarchar(50), [paid search ad group id]), '0')   paidsearchadgroupid,
                convert(nvarchar(1000), [paid search ad group])                 paidsearchadgroup
from   #tmp_pdsearchadgroup_2
where  not exists (select 'x'
                   from   UALUS_DIM_Paid_SearchAdGroup
                   where  [paid search ad group id] = paid_search_adgroup_id
                          )

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================

--Output File
SELECT *
INTO   #DT
FROM   UALUS_DIM_PLACEMENT
