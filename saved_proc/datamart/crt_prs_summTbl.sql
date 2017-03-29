alter procedure dbo.crt_prs_summtbl
as
if object_id('dm_1161_unitedairlinesusa.dbo.prs_summ',N'U') is not null
	drop table dbo.prs_summ;
create table dbo.prs_summ
(
	PlacementId         int            NOT NULL,
	AdserverPlacementId int,
	AdserverCampaignId  int,
	PlacementNumber     nvarchar(6)    NOT NULL,
	PlacementName       nvarchar(4000) NOT NULL,
	CampaignName        nvarchar(4000) NOT NULL,
	Planned_Cost        decimal(20,10),
	Planned_Amt         int,
	ParentId            int            NOT NULL,
	PackageCat          nvarchar(100),
	PlacementStart      date           NOT NULL,
	PlacementEnd        date           NOT NULL,
	stYrMo              int            NOT NULL,
	edYrMo              int            NOT NULL,
	stDate              int            NOT NULL,
	edDate              int            NOT NULL,
	DV_Map              nvarchar(1),
	Rate                decimal(20,10) NOT NULL,
	PackageName         nvarchar(4000),
	Cost_ID             nvarchar(6),
	CostMethod          nvarchar(100)  NOT NULL
);

INSERT INTO dbo.prs_summ

	SELECT DISTINCT
		t3.placementid                                                                                       as placementid,
		t3.adserverplacementid                                                                               as adserverplacementid,
		t3.adservercampaignid                                                                                as adservercampaignid,
		t3.placementnumber                                                                                   as placementnumber,
		t3.placementname                                                                                     as placementname,
		t3.campaignname                                                                                      as campaignname,
		t3.plannedcost                                                                                       as planned_cost,
		t3.plannedunits                                                                                      as planned_amt,
		t3.parentid                                                                                          as parentid,
		t3.packagecat                                                                                        as packagecat,
		t3.placementstart                                                                                    as placementstart,
		t3.placementend                                                                                      as placementend,
		t3.styrmo                                                                                            as styrmo,
		t3.edyrmo                                                                                            as edyrmo,
		t3.stdate                                                                                            as stdate,
		t3.eddate                                                                                            as eddate,
		t3.dv_map                                                                                            as dv_map,
		isnull(t3.rate, cast(0 as decimal(20,10))) as rate,
		t3.packagename                                                                                       as packagename,
		t3.cost_id                                                                                           as cost_id,
		t3.costmethod                                                                                        as costmethod
	FROM (

			 select distinct
				 t1.placementid,
				 t1.adserverplacementid,
				 t1.adservercampaignid,
				 t1.placementnumber,
				 t1.placementname,
				 t2.campaignname,
				 amt.plannedcost,
				 amt.plannedunits,
				 t1.parentid,
				 t2.packagecat,
				 cast(t1.placementstartdate as
					  date)                                 as placementstart,
				 cast(t1.placementenddate as
					  date)      as placementend,

				 [dbo].udf_datetoint(t1.placementstartdate) as stdate,
				 [dbo].udf_datetoint(t1.placementenddate)   as eddate,
				 [dbo].udf_yrmotoint(t1.placementstartdate) as styrmo,
				 [dbo].udf_yrmotoint(t1.placementenddate)   as edyrmo,
				 vew.customcolumnvalue                      as dv_map,
				 isnull(t2.rate,cast(0 as decimal(20,10)))  as rate,
				 pak.packagename,
				 pak.cost_id,
				 t2.costmethod
			 from (
					  select distinct
						  placementid,
						  cast(adserverplacementid as int) as adserverplacementid,
						  cast(adservercampaignid as int)  as adservercampaignid,
						  parentid,
						  placementnumber,
						  placementname,
						  placementstartdate,
						  placementenddate,
						  packagetype
					  from dm_1161_unitedairlinesusa.dbo.dfid037723_prismaadvancedplacementdetails_extracted) as t1

				 outer apply (
								 select distinct
									 placementid,
									 parentid,
									 isnull(cast(rate as decimal(20,10)),0) as rate,
									 costmethod,
									 campaignname,
									 packagetype                            as packagecat
								 from dm_1161_unitedairlinesusa.dbo.dfid037722_prismaplacementdetails_extracted as t2
								 where t1.placementid = t2.placementid) as t2

				 outer apply (
								 select
									 placementid,
									 customcolumnvalue
								 from dm_1161_unitedairlinesusa.dbo.prs_view as vew

								 where t1.placementid = vew.placementid) as vew

				 outer apply (
								 select
									 parentid,
									 packagename,
									 cost_id
								 from dm_1161_unitedairlinesusa.dbo.prs_packages as pak
								 where t1.parentid = pak.parentid) as pak

				 outer apply (
								 select
									 parentid,
									 plannedcost,
									 plannedunits,
									 placementstartdate

								 from dm_1161_unitedairlinesusa.[dbo].prs_amt as amt

								 where t1.parentid = amt.parentid
-- 				                       and cast(t1.adserverplacementid as int) = amt.adserverplacementid
							 ) as amt

		           where cast(t1.placementstartdate as date) >= '2016-01-01'
							and (len(isnull(t2.campaignname,'')) > 0)

		     group by
			     t1.placementid,
			     t1.adserverplacementid,
			     t1.adservercampaignid,
			     t1.placementnumber,
			     t1.placementname,
			     t2.campaignname,
			     amt.plannedcost,
			     amt.plannedunits,
			     t1.parentid,
			     t2.packagecat,
			     t1.placementstartdate,
			     t1.placementenddate,
			     vew.customcolumnvalue,
			     t2.rate,
			     pak.packagename,
			     pak.cost_id,
			     t2.costmethod ) as t3

	      where t3.styrmo >= '201601'


	group by
		t3.placementid,
		t3.adserverplacementid,
		t3.adservercampaignid,
		t3.placementnumber,
		t3.placementname,
		t3.campaignname,
		t3.plannedcost,
		t3.plannedunits,
		t3.parentid,
		t3.packagecat,
		t3.placementstart,
		t3.placementend,
		t3.styrmo,
		t3.edyrmo,
		t3.stdate,
		t3.eddate,
		t3.dv_map,
		t3.rate,
		t3.packagename,
		t3.cost_id,
		t3.costmethod