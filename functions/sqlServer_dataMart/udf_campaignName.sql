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
								                 when @campaignID = 20113327 then 'Aruba Co-Op 2017'
                								 when @campaignID = 11382787 then 'Ireland Co-op 2017'
                								 when @campaignID = 11417648 then 'WIN NY Rewards'
                                 when @campaignID = 20177168 then 'Defend SF 2017'
                                 when @campaignID = 20323941 then 'Denver Local Support 2017'
                                 when @campaignID = 20354464 then 'Buenos Aires 2017'
                                 when @campaignID = 20343755 then 'United NFL 2017'
                                 when @campaignID = 20325908 or @campaignID = 20334628 then 'New Zealand Route Support 2017'
                                 when @campaignID = 20334410 or @campaignID = 20337019 or @campaignID = 20336827 then 'Singapore Route Launch 2017'
                								 when @campaignID = 20353010 then 'Chicago 2017'
                                 when @campaignID = 20606595 then 'GM Acquisition 2018'
                                 when @campaignID = 20681311 or @campaignID = 20609762 then 'Winter Olympics 2018'
                                 when @campaignID = 20629714 then 'New Zealand Onshore 2018'
                                 when @campaignID = 20634533 then 'Singapore Onshore 2018'
                                 when @campaignID = 20713692 then 'GM Acquisition Fare Sale 2018'
                                 when @campaignID = 20721526 then 'GM Acquisition Meta 2018'
                                 when @campaignID = 21086860 then 'Trade Jetstream 2018'
                                 when @campaignID = 21206832 then 'Trade Right Sized 2018'
                                 when @campaignID = 21187099 then 'PGA 2018'
                                 when @campaignID = 21182874 then 'Economy Plus 2018'
                                 when @campaignID = 21128474 then 'Personal Threshold 2018'
                                 when @campaignID = 21230517 then 'Sydney Premium Cabin Sale 2018'
                                 when @campaignID = 20820574 then 'BRA Native 2018'
                                 when @campaignID = 20819775 then 'Native Outbrain 2018'
                                 when @campaignID = 20945117 then 'Social Extension 2018'
                                 when @campaignID = 21057553 then 'MileagePlus 2018'

                               else @campaignName end;
        return @finalCampaignName
    end
go
