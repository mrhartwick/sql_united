alter function [dbo].udf_campaignName(
    @campaignID int,@campaignName varchar(4000)
)
    returns varchar(4000)
    with execute as caller
as
    begin
        declare @finalCampaignName varchar(4000)
        set @finalCampaignName = case
                                 when @campaignID = 9973506  then 'SFO-AKL 2016'
                                 when @campaignID = 9304728  then 'Trade 2016'
                                 when @campaignID = 9407915  then 'Google PDE 2016'
                                 when @campaignID = 9548151  then 'Smithsonian 2016'
                                 when @campaignID = 9630239  then 'SFO-TLV 2016'
                                 when @campaignID = 9639387  then 'Targeted Marketing 2016'
                                 when @campaignID = 9739006  then 'Spoke Markets 2016'
                                 when @campaignID = 9923634  then 'SFO-SIN 2016'
                                 when @campaignID = 10276123 then 'Polaris 2016'
                                 when @campaignID = 10094548 then 'Marketing Fund 2016'
                                 when @campaignID = 10090315 then 'SME 2016'
                                 when @campaignID = 9994694  then 'SFO-China 2016'
                                 when @campaignID = 9408733  then 'Chile CoOp 2016'
                                 when @campaignID = 10307468 then 'SFO-HGH/XIY 2016'
                                 when @campaignID = 10505745 then 'Brussels 2016'
                                 when @campaignID = 9999841 or @campaignID = 10121649 then 'Olympics 2016'
                                 when @campaignID = 10768497 then 'Polaris 2017'
                                 when @campaignID = 9801178  then 'Smithsonian 2017'
                                 when @campaignID = 10742878 then 'GM Acquisition 2017'
                                 when @campaignID = 10740457 then 'People En Espanol 2017'
                                 when @campaignID = 10812738 then 'Marketing Fund 2017'
                                 when @campaignID = 10918234 then 'Win NY 2017'
                                 when @campaignID = 10942240 then 'China Onshore 2017'
                                 when @campaignID = 11069476 then 'Trade 2017'
                                 when @campaignID = 11152017 then 'Hamburg Co-Op 2017'
                                 when @campaignID = 11177760 then 'Chicago Fare Sale 2017'
                                 when @campaignID = 11224605 then 'San Jose Fare Sale 2017'
                                 when @campaignID = 11390108 then 'PR Support 2017'
                                 else @campaignName end;
        return @finalCampaignName
    end
go
