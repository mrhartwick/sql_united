create table DFID037724_PrismaPlacementsMonthlyDelivery_Extracted
(
	StartDate smalldatetime,
	EndDate smalldatetime,
	AgencyName nvarchar(4000),
	AgencyAlphaCode nvarchar(4000),
	LocationCompanyCode nvarchar(4000),
	AdvertiserName nvarchar(4000),
	AdvertiserCode nvarchar(4000),
	AdvertiserBusinessKey nvarchar(4000),
	ProductName nvarchar(4000),
	ProductCode nvarchar(4000),
	ProductBusinessKey nvarchar(4000),
	CampaignName nvarchar(4000),
	CampaignPublicId nvarchar(4000),
	CampaignId bigint,
	CampaignStartDate smalldatetime,
	CampaignEndDate smalldatetime,
	SupplierName nvarchar(4000),
	SupplierCode nvarchar(4000),
	SupplierBusinessKey nvarchar(4000),
	EstimateName nvarchar(4000),
	EstimateCode nvarchar(4000),
	EstimateBusinessKey nvarchar(4000),
	MediaName nvarchar(4000),
	MediaCode nvarchar(4000),
	BuyType nvarchar(4000),
	BuyCategory nvarchar(4000),
	PlacementName nvarchar(4000),
	PlacementNumber nvarchar(4000),
	PlacementId bigint,
	PackageType nvarchar(4000),
	PackageId bigint,
	ParentId bigint,
	PlacementStartDate smalldatetime,
	PlacementEndDate smalldatetime,
	PlacementMonthlyStartDate smalldatetime,
	PlacementMonthlyEndDate smalldatetime,
	PlacementMonth int,
	PlacementYear int,
	PlacementType nvarchar(4000),
	PrimaryPlacement nvarchar(4000),
	AdserverName nvarchar(4000),
	InstanceName nvarchar(4000),
	AdserverCampaignId nvarchar(4000),
	AdserverPackageId nvarchar(4000),
	AdserverPlacementId nvarchar(4000),
	AdserverAdvertiserName nvarchar(4000),
	AdserverAdvertiserCode nvarchar(4000),
	AdserverClientName nvarchar(4000),
	AdserverClientCode nvarchar(4000),
	AdserverSupplierName nvarchar(4000),
	AdserverSupplierCode nvarchar(4000),
	AdserverSiteName nvarchar(4000),
	AdserverSiteCode nvarchar(4000),
	DeliveryExists nvarchar(4000),
	FirstPartyDeliverySource nvarchar(4000),
	ThirdPartyDeliverySource nvarchar(4000),
	PlannedAmount float,
	PlannedUnits bigint,
	PlannedImpressions bigint,
	PlannedClicks bigint,
	PlannedActions bigint,
	IOAmount float,
	SupplierUnits bigint,
	SupplierImpressions bigint,
	SupplierClicks bigint,
	SupplierActions bigint,
	SupplierCost float,
	AdserverUnits bigint,
	AdserverImpressions bigint,
	AdserverClicks bigint,
	AdserverActions bigint,
	AdserverCost float,
	WeightedPostImpressions bigint,
	WeightedPostClicks bigint,
	WeightedActions bigint,
	ConversionPostImpressions bigint,
	ConversionPostClicks bigint,
	ConversionActions bigint,
	ApprovedAmount decimal(19,5),
	ApprovedUnits bigint,
	ApprovedSource nvarchar(4000),
	OriginalAmount decimal(19,5),
	TotalRemainingAmount decimal(19,5),
	TotalRemainingUnits bigint,
	EligibleImpressions bigint,
	MeasurableImpressions bigint,
	ViewableImpressions bigint,
	ReportedImpressions bigint,
	ReportedClicks bigint,
	ReportedConversions bigint,
	AttributedConversion bigint,
	IsDeleted nvarchar(4000),
	AcquireID int
)
go

