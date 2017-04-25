CREATE procedure dbo.crt_prs_summ_vertica
as

execute('
drop table if exists diap01.mec_us_united_20056.ual_prs_summ;
create table diap01.mec_us_united_20056.ual_prs_summ
(
  PlacementId int not null,
  AdserverPlacementId int,
  AdserverCampaignId int,
  PlacementNumber varchar(6) not null,
  PlacementName varchar(4000) not null,
  CampaignName varchar(4000) not null,
  Planned_Cost decimal(20,10),
  Planned_Amt int,
  ParentId int not null,
  PackageCat varchar(100),
  PlacementStart date not null,
  PlacementEnd date not null,
  styrmo int not null,
  edyrmo int not null,
  stDate int not null,
  edDate int not null,
  DV_Map varchar(1),
  Rate decimal(20,10) not null,
  PackageName varchar(4000),
  Cost_ID varchar(6),
  CostMethod varchar(100) not null
);
'
) AT verticaunited;

  insert into VerticaUnited.diap01.mec_us_united_20056.ual_prs_summ
    select *
        from [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.[dbo].prs_summ
go
