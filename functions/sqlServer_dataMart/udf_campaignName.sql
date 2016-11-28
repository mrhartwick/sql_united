create function [dbo].udf_campaignName(
    @campaignID int,@campaignName varchar(4000)
)
    returns varchar(4000)
    with execute as caller
as
    begin
        declare @finalCampaignName varchar(4000)
        set @finalCampaignName = case
                                 when @campaignID = 9973506 then 'SFO-AKL'
                                 when @campaignID = 9304728 then 'Trade'
                                 when @campaignID = 9407915 then 'Google PDE'
                                 when @campaignID = 9548151 then 'Smithsonian'
                                 when @campaignID = 9630239 then 'SFO-TLV'
                                 when @campaignID = 9639387 then 'Targeted Marketing'
                                 when @campaignID = 9739006 then 'Spoke Markets'
                                 when @campaignID = 9923634 then 'SFO-SIN'
                                 when @campaignID = 10276123 then 'Polaris'
                                 when @campaignID = 10094548 then 'Marketing Fund'
                                 when @campaignID = 10090315 then 'SME'
                                 when @campaignID = 9994694 then 'SFO-China'
                                 when @campaignID = 9408733 then 'Chile CoOp'
                                 when @campaignID = 10307468 then 'SFO-HGH/XIY'
                                 when @campaignID = 10505745 then 'Brussels'
                                 when @campaignID = 9999841 or @campaignID = 10121649 then 'Olympics'
                                 else @campaignName end;
        return @finalCampaignName
    end
go