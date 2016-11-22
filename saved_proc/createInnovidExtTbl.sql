alter procedure dbo.createInnovidExtTbl
as


if OBJECT_ID('DM_1161_UnitedAirlinesUSA.dbo.innovidExtTable',N'U') is not null
    drop table dbo.innovidExtTable;
create table dbo.innovidExtTable
(
    joinKey                 varchar(255),
    ivDate                  date,
    campaign_name           varchar(1000),
    campaign_id             int,
    publisher               varchar(1000),
    publisher_id            int,
    placement_name          varchar(1000),
    impressions             int,
    click_thrus             int,
    engagement_events       int,
    twfive_completion       int,
    fifty_completion        int,
    svfive_completion       int,
    all_completion          int,
    time_earned             float,
    pctad_viewed            float,
    slate_open_by_click     int,
    slate_open_by_rollover  int,
    total_slate_open_events int,
    in_unit_clicks          int
);

insert into dbo.innovidExtTable

    select
        case when placement_name like '%United_360%Amobee%' then replace('PBKB7J' + '_' + [dbo].udf_siteKey(publisher) + '_' + cast(date as varchar(10)),' ','')
        else replace(left(placement_name,6) + '_' + [dbo].udf_siteKey(publisher) + '_' + cast(date as varchar(10)),' ','')
        end as joinKey,
        date                                                          as ivDate,
        campaign_name,
        campaign_id,
        [dbo].udf_siteName(publisher)                                 as publisher,
        publisher_id,

        case when placement_name like 'PBKB7J%' or placement_name like 'PBKB7H%' or placement_name like 'PBKB7K%' or
            placement_name =
                'United 360 - Polaris 2016 - Q4 - Amobee' then 'PBKB7J_UAC_BRA_016_Mobile_AMOBEE_Video360_InViewPackage_640x360_MOB_MOAT_Fixed Placement_Other_P25-54_1 x 1_Standard_Innovid_PUB PAID'
        else placement_name end                                       as placement_name,
        sum(impressions),
        sum(click_thrus),
        sum(engagement_events),
        sum(twfive_completion),
        sum(fifty_completion),
        sum(svfive_completion),
        sum(all_completion),
        sum(time_earned),
        sum(pctad_viewed),
        sum(slate_open_by_click),
        sum(slate_open_by_rollover),
        sum(total_slate_open_events),
        sum(in_unit_clicks)
    from DM_1161_UnitedAirlinesUSA.dbo.innovidTable

    group by
        date,
        campaign_name,
        campaign_id,
        publisher,
        publisher_id,
        placement_name
go