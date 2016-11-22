ALTER procedure dbo.createDVTbl
as
if OBJECT_ID('master.dbo.DVTable',N'U') is not null
	drop table master.dbo.DVTable;


create table master.dbo.DVTable
(
	joinKey                     varchar(255) not null,
	dvDate                      date         not null,
	media_property              varchar(255) not null,
	campaign_name               varchar(255) not null,
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
	from ( select distinct


		       cast(date as date)                                as dvDate,
		       replace(left(placement_name,6) + '_' +
		               [dbo].udf_siteKey(media_property)
		               + '_' + cast(date as varchar(10)),' ','') as joinKey,

		       campaign_name                                     as campaign_name,
		       [dbo].udf_siteName(media_property)                           as media_property,
		       case when placement_code in ('137412510', '137412401', '137412609') then '137412609' else placement_code end   as placement_code,
    case when placement_name like 'PBKB7J%' or placement_name like 'PBKB7H%' or placement_name like 'PBKB7K%' or placement_name ='United 360 - Polaris 2016 - Q4 - Amobee'        then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID' else  placement_name end     as placement_name,
		       total_impressions                                 as total_impressions,
		       groupm_passed_impressions                         as groupm_passed_impressions,
		       groupm_billable_impressions                       as groupm_billable_impressions

	       from VerticaGroupM.mec.UnitedUS.dv_impression_agg ) as DV

	where DV.placement_name not like '%[Dd][Oo]%[Nn][Oo][Tt]%[Uu][Ss][Ee]%'
	      and DV.placement_name not like '%[Nn]o%[Tt]racking%'
-- and cast(DV.dvDate AS DATE) BETWEEN '2016-01-01' AND '2016-03-31'
	group by
		DV.joinKey,
		DV.dvDate,
		DV.media_property,
		DV.campaign_name,
		DV.placement_code,
		DV.placement_name
go