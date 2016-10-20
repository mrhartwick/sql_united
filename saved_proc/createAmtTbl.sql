ALTER procedure dbo.createAmtTbl
as
if OBJECT_ID('DM_1161_UnitedAirlinesUSA.dbo.plannedAmtTable',N'U') is not null
	drop table dbo.plannedAmtTable;
create table dbo.plannedAmtTable
(
	ParentId           int     not null,
	SupplierCode       int     not null,
	AdserverCampaignId int     not null,
	PlannedCost        decimal not null,
	PlannedUnits       int     not null,
	PlacementStartDate date    not null,
	YearMo             int     not null
);

insert into dbo.plannedAmtTable

	select
		final.ParentId,
		final.SupplierCode,
		final.AdserverCampaignId,
		final.PlannedCost,
		final.PlannedUnits,
		final.PlacementStartDate,
		final.YearMo
	from (select distinct
		      cast(t1.ParentId as int)                   as ParentId,
		      cast(t1.AdserverSupplierCode as int)       as SupplierCode,
		      cast(t1.AdserverCampaignId as int)         as AdserverCampaignId,
		      case when cast(t1.PlannedAmount as decimal) is null
			      then 0
		      else cast(t1.PlannedAmount as decimal) end as PlannedCost,
		      case when cast(t1.PlannedUnits as int) is null
			      then 0
		      else cast(t1.PlannedUnits as int) end      as PlannedUnits,
		      cast(t1.PlacementStartDate as date)        as PlacementStartDate,
		      convert(int,cast(year(cast(t1.PlacementStartDate as date)) as varchar(4)) +
		                  SUBSTRING((convert(varchar(10),cast(t1.PlacementStartDate as date))),
		                            (CHARINDEX('-',convert(varchar(10),cast(t1.PlacementStartDate as date))) + 1),
		                            2))                  as YearMo
	      from [DFID037724_PrismaPlacementsMonthlyDelivery_Extracted] as t1
	      where PlannedUnits is not null
	            and AdserverSupplierCode is not null
	            and cast(t1.PlacementMonthlyStartDate as date) = (
		      select MAX(cast(t2.PlacementMonthlyStartDate as date))
		      from
			      [dbo].[DFID037724_PrismaPlacementsMonthlyDelivery_Extracted] t2
		      where t2.ParentId = t1.ParentId)
	     ) as final
where final.PlannedCost + final.PlannedUnits > 0
	group by
		final.ParentId
		,final.SupplierCode
		,final.AdserverCampaignId
		,final.PlannedCost
		,final.PlannedUnits
		,final.PlacementStartDate
		,final.YearMo
go
