

create table if not exists wmprodfeeds.ikea.prisma_placementmonthlydelivery
(
StartDate	date encode zstd,
EndDate	date encode zstd,
AgencyName	varchar(600) encode zstd,
AgencyAlphaCode	varchar(600) encode zstd,
LocationCompanyCode	varchar(600) encode zstd,
AdvertiserName	varchar(600) encode zstd,
AdvertiserCode	varchar(600) encode zstd,
AdvertiserBusinessKey	varchar(600) encode zstd,
ProductName	varchar(600) encode zstd,
ProductCode	varchar(600) encode zstd,
ProductBusinessKey	varchar(600) encode zstd,
CampaignName	varchar(600) encode zstd,
CampaignPublicId	varchar(600) encode zstd,
CampaignId	bigint encode zstd,
CampaignStartDate	date encode zstd,
CampaignEndDate	date encode zstd,
SupplierName	varchar(600) encode zstd,
SupplierCode	varchar(600) encode zstd,
SupplierBusinessKey	varchar(600) encode zstd,
EstimateName	varchar(600) encode zstd,
EstimateCode	varchar(600) encode zstd,
EstimateBusinessKey	varchar(600) encode zstd,
MediaName	varchar(600) encode zstd,
MediaCode	varchar(600) encode zstd,
BuyType	varchar(600) encode zstd,
BuyCategory	varchar(600) encode zstd,
PlacementName	varchar(600) encode zstd,
PlacementNumber	varchar(600) encode zstd,
PlacementId	bigint encode zstd distkey,
PackageType	varchar(600) encode zstd,
PackageId	bigint encode zstd,
ParentId	bigint encode zstd,
PlacementStartDate	date encode zstd,
PlacementEndDate	date encode zstd,
PlacementMonthlyStartDate	date encode zstd,
PlacementMonthlyEndDate	date encode zstd,
PlacementMonth	int encode zstd,
PlacementYear	int encode zstd,
PlacementType	varchar(600) encode zstd,
PrimaryPlacement	varchar(600) encode zstd,
AdserverName	varchar(600) encode zstd,
InstanceName	varchar(600) encode zstd,
AdserverCampaignId	varchar(600) encode zstd,
AdserverPackageId	varchar(600) encode zstd,
AdserverPlacementId	varchar(600) encode zstd,
AdserverAdvertiserName	varchar(600) encode zstd,
AdserverAdvertiserCode	varchar(600) encode zstd,
AdserverClientName	varchar(600) encode zstd,
AdserverClientCode	varchar(600) encode zstd,
AdserverSupplierName	varchar(600) encode zstd,
AdserverSupplierCode	varchar(600) encode zstd,
AdserverSiteName	varchar(600) encode zstd,
AdserverSiteCode	varchar(600) encode zstd,
DeliveryExists	varchar(600) encode zstd,
FirstPartyDeliverySource	varchar(600) encode zstd,
ThirdPartyDeliverySource	varchar(600) encode zstd,
PlannedAmount	double precision encode zstd,
PlannedUnits	bigint encode zstd,
PlannedImpressions	bigint encode zstd,
PlannedClicks	bigint encode zstd,
PlannedActions	bigint encode zstd,
IOAmount	double precision encode zstd,
SupplierUnits	bigint encode zstd,
SupplierImpressions	bigint encode zstd,
SupplierClicks	bigint encode zstd,
SupplierActions	bigint encode zstd,
SupplierCost	double precision encode zstd,
AdserverUnits	bigint encode zstd,
AdserverImpressions	bigint encode zstd,
AdserverClicks	bigint encode zstd,
AdserverActions	bigint encode zstd,
AdserverCost	double precision encode zstd,
WeightedPostImpressions	bigint encode zstd,
WeightedPostClicks	bigint encode zstd,
WeightedActions	bigint encode zstd,
ConversionPostImpressions	bigint encode zstd,
ConversionPostClicks	bigint encode zstd,
ConversionActions	bigint encode zstd,
ApprovedAmount	decimal(19,5) encode zstd,
ApprovedUnits	bigint encode zstd,
ApprovedSource	varchar(600) encode zstd,
OriginalAmount	decimal(19,5) encode zstd,
TotalRemainingAmount	decimal(19,5) encode zstd,
TotalRemainingUnits	bigint encode zstd,
EligibleImpressions	bigint encode zstd,
MeasurableImpressions	bigint encode zstd,
ViewableImpressions	bigint encode zstd,
ReportedImpressions	bigint encode zstd,
ReportedClicks	bigint encode zstd,
ReportedConversions	bigint encode zstd,
AttributedConversion	bigint encode zstd,
IsDeleted	varchar(600) encode zstd,

)
diststyle key
sortkey(PlacementId,StartDate,EndDate)
;

