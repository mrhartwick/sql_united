alter procedure dbo.createAmtTbl
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
            t3.ParentId,
            t3.SupplierCode,
            t3.AdserverCampaignId,
            t3.PlannedCost,
            t3.PlannedUnits,
            t3.PlacementStartDate,
            t3.YearMo

        from (
                 select
                     t2.ParentId,
                     t2.SupplierCode,
                     t2.AdserverCampaignId,
                     sum(t2.PlannedCost) over (partition by t2.ParentId)  as PlannedCost,
                     sum(t2.PlannedUnits) over (partition by t2.ParentId) as PlannedUnits,
                     t2.r1                                                as r1,
                     t2.PlacementStartDate                                as PlacementStartDate,
                     t2.YearMo                                            as YearMo

                 from (
                          select
                              t1.ParentId,
                              t1.SupplierCode,
                              t1.AdserverCampaignId,
                              sum(t1.PlannedCost) over (partition by t1.ParentId)  as PlannedCost,
                              sum(t1.PlannedUnits) over (partition by t1.ParentId) as PlannedUnits,
                              t1.r1                                                as r1,
                              t1.PlacementStartDate                                as PlacementStartDate,
                              t1.YearMo                                            as YearMo

                          from (
                                   select
                                       cast(ParentId as int)                                 as ParentId,
                                       cast(AdserverSupplierCode as int)                     as SupplierCode,
                                       cast(AdserverCampaignId as int)                       as AdserverCampaignId,
                                       isnull(cast(PlannedAmount as decimal(10,2)),0)        as PlannedCost,
                                       isnull(cast(PlannedUnits as int),0)                   as PlannedUnits,
                                       cast(PlacementStartDate as date)                      as PlacementStartDate,
                                       row_number() over (partition by ParentId
                                           order by cast(PlacementMonthlyStartDate as date)) as r1,
                                       convert(int,cast(year(cast(PlacementStartDate as date)) as varchar(4)) +
                                           SUBSTRING((convert(varchar(10),cast(PlacementStartDate as date))),
                                                     (CHARINDEX('-',convert(varchar(10),
                                                                            cast(PlacementStartDate as date))) + 1),
                                                     2))                                     as YearMo
                                   from [DFID037724_PrismaPlacementsMonthlyDelivery_Extracted]
                                   where PlannedUnits is not null
                                       and AdserverSupplierCode is not null
                                       and (PackageType = 'Package' or PackageType = 'Standalone')

                               ) as t1
                      ) as t2

                 where t2.r1 = 1
             ) as t3

        group by
            t3.ParentId
            ,t3.SupplierCode
            ,t3.AdserverCampaignId
            ,t3.PlannedCost
            ,t3.PlannedUnits
            ,t3.PlacementStartDate
            ,t3.YearMo

