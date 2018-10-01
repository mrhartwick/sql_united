

-- Columns to lose from tables


create table DFID041780_UALUS_Search_Standard_Extracted
(
    Date nvarchar(4000),
    Week nvarchar(4000),
    [Paid Search Engine Account ID] nvarchar(4000),
    [Paid Search Campaign ID] nvarchar(4000),
    [Paid Search Keyword ID] nvarchar(4000),
    [Paid Search Ad Group ID] nvarchar(4000),
    [Paid Search Ad ID] nvarchar(4000),
    [Paid Search Advertiser ID] nvarchar(4000),
    [Paid Search Match Type] nvarchar(4000),
    [Site ID (DCM)] nvarchar(4000),
    [Site ID (Site Directory)] nvarchar(4000),
    [Campaign ID] nvarchar(4000),
    -- [Package/Roadblock ID] nvarchar(4000),
    [Placement ID] nvarchar(4000),
    -- [Placement Total Booked Units] nvarchar(4000),
    [Paid Search Impressions] nvarchar(4000),
    [Paid Search Clicks] nvarchar(4000),
    [Paid Search Cost] nvarchar(4000),
    [Paid Search Revenue] nvarchar(4000),
    -- [Paid Search Click Rate] nvarchar(4000),
    [Paid Search Average Position] nvarchar(4000),
    [Paid Search Transactions] nvarchar(4000),
    -- [Paid Search Actions] nvarchar(4000),
    -- [Paid Search Visits] nvarchar(4000),
    AcquireID int
)
go
-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================
create table DFID041761_UALUS_Search_Floodlight_Extracted
(
    Date nvarchar(4000),
    [Activity ID] nvarchar(4000),
    [Activity Date/Time] nvarchar(4000),
    [Placement ID] nvarchar(4000),
    [Campaign ID] nvarchar(4000),
    [Paid Search Engine Account ID] nvarchar(4000),
    [Paid Search Engine Account] nvarchar(4000),
    [Paid Search Campaign ID] nvarchar(4000),
    [Paid Search Campaign] nvarchar(4000),
    [Paid Search Ad ID] nvarchar(4000),
    [Paid Search Ad] nvarchar(4000),
    [Paid Search Keyword ID] nvarchar(4000),
    [Paid Search Keyword] nvarchar(4000),
    [Site ID (DCM)] nvarchar(4000),
    [Site (DCM)] nvarchar(4000),
    [Paid Search Advertiser ID] nvarchar(4000),
    [Paid Search Advertiser] nvarchar(4000),
    [Paid Search Ad Group ID] nvarchar(4000),
    [Paid Search Ad Group] nvarchar(4000),
    [Site ID (Site Directory)] nvarchar(4000),
    [Site (Site Directory)] nvarchar(4000),
    [Revenue (string)] nvarchar(4000),
    [Currency (string)] nvarchar(4000),
    [PNR_Base64Encoded (string)] nvarchar(4000),
    [Number of Tickets (string)] nvarchar(4000),
    [Origin_1 (string)] nvarchar(4000),
    [Origin_2 (string)] nvarchar(4000),
    [Total Conversions] nvarchar(4000),
    [Total Revenue] nvarchar(4000),
    [Transaction Count] nvarchar(4000),
    [Revenue (number)] nvarchar(4000),
    [PNR_Base64Encoded (number)] nvarchar(4000),
    [Click-through Revenue] nvarchar(4000),
    [Click-through Conversions] nvarchar(4000),
    [Click-through Transaction Count] nvarchar(4000),
    AcquireID int
)
go

-- ====================================================================================================================
-- ====================================================================================================================
-- ====================================================================================================================