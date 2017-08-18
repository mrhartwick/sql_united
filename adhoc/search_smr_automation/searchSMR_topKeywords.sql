-- Search SMR Automation 5 - ALL NETWORKS

declare @report_st date
declare @report_ed date
--
set @report_ed = '2017-04-30';
set @report_st = '2017-04-01';


select
t3.paid_search_campaign,
t3.paid_search_keyword,
t3.r1 as Rank,
sum(t3.imps) as imps,
sum(t3.cost) as cost,
sum(t3.clicks) as clicks,
sum(t3.rev) as rev,
sum(t3.prch) as prch,
sum(t3.roas) as roas,
sum(t3.cpl) as cpl,
sum(t3.lead) as lead,
sum(t3.tix) as Tickets

from (
select

t2.paid_search_campaign as paid_search_campaign,
t2.paid_search_keyword as paid_search_keyword,

sum(t2.imps) as imps,
sum(t2.cost) as cost,
sum(t2.clicks) as Clicks,
isnull(sum(t2.rev),0) as rev,
isnull(sum(cast(t2.prch as int )),0) as prch,
isnull(sum(cast(t2.lead as int )),0) as lead,
isnull(sum(t2.tix),0) as tix,
sum(t2.roas) as roas,
sum(t2.cpl) as cpl,
row_number() over ( partition by t2.paid_search_campaign order by sum(t2.roas) desc ) as r1

from (
--
-- declare @report_st date
-- declare @report_ed date
-- --
-- set @report_ed = '2017-06-02';
-- set @report_st = '2017-06-01';

select
-- t1.date,
--         cast(dateadd(week,datediff(week,0,cast(t1.date as date)),0) as date) as "week",
-- t1.month as [month],
-- t1.week as [Week],
--     e1.Paid_SearchEngine                                                                as Paid_SearchEngine,
--   t1.pdsearch_engineaccount_id as [Search Engine ID],
--     case
--     when
--         (c1.paid_search_campaign like '%[Rr]emarketing%' or ad1.Paid_Search_AdGroup like '%[Rr]emarketing%') then 'Display'
--     when (c1.paid_search_campaign like '%[Rr][Ll][Ss][Aa]%' or ad1.Paid_Search_AdGroup like '%[Rr][Ll][Ss][Aa]%' or e1.Paid_SearchEngine like '%[Rr][Ll][Ss][Aa]%') then 'RLSA'
--     when (c1.paid_search_campaign like '%[Ss]earch%' or ad1.Paid_Search_AdGroup like '%[Ss]earch%') then 'Search'
--     else 'Search' end                                                                   as Network,
--  t1.placement_id,
c1.paid_search_campaign as paid_search_campaign,
--   t1.pdsearch_campaign_id      as [Campaign ID],
k1.paid_search_keyword as paid_search_keyword,
--   t1.pdsearch_keyword_id       as [Keyword ID],
--     ad1.Paid_Search_AdGroup                                                             as Paid_Search_AdGroup,
-- t1.pdsearch_adgroup_id       as pdsearch_adgroup_id,
--     s1.site_dcm                                                                         as site_dcm,
--   t1.siteid_dcm                as [Site ID],
--   t1.pdsearch_ad_id            as [Ad ID],
--     t1.pdsearch_matchtype                                                               as pdsearch_matchtype,
sum(t1.imps) as imps,
sum(t1.cost) as cost,
sum(t1.clicks) as Clicks,
-- avg(t1.avg_pos) as avg_pos,
--     sum(case when t1.imps = 0 then 0 else t1.avg_pos_1 / t1.imps end)                   as avg_pos_2,
--     case when sum(t1.imps) = 0 then 0 else sum(t1.avg_pos * t1.imps) / sum(t1.imps) end as avg_pos_3,
-- isnull( sum( ((fld2.rev) - t1.cost)/ t1.cost) ,0) as roas,
sum( case when isnull(t1.cost,0) =0 then 0 else (isnull(fld2.rev,0)-isnull(t1.cost,0))/isnull(t1.cost,0) end ) as roas,
sum( case when isnull(t1.cost,0) =0 then 0 else (isnull(fld2.lead,0)-isnull(t1.cost,0))/isnull(t1.cost,0) end ) as cpl,
isnull(sum(fld2.rev),0) as rev,
isnull(sum(cast(fld2.prch as int )),0) as prch,
isnull(sum(cast(fld2.lead as int )),0) as lead,
--   sum(cast(fld2.tot_con as int))     as Transactions,
isnull(sum(fld2.tix),0) as tix

from (

--
--
-- declare @report_st date
-- declare @report_ed date
--
-- set @report_ed = '2017-04-29';
-- set @report_st = '2017-04-23';

select
--         cast(std1.date as date)                                                as "Date",
cast(dateadd( week,datediff( week,0,cast(std1.date as date )),-1) as date ) as [week],
datename( month,cast(std1.date as date )) as [month],
std1.pdsearch_engineaccount_id as pdsearch_engineaccount_id,
--        std1.placement_id                                                      as placement_id,
std1.siteid_dcm as siteid_dcm,
std1.pdsearch_campaign_id as pdsearch_campaign_id,
std1.pdsearch_keyword_id as pdsearch_keyword_id,
--           k1.paid_search_keyword as keyword,
std1.pdsearch_adgroup_id as pdsearch_adgroup_id,
std1.pdsearch_ad_id as pdsearch_ad_id,
std1.pdsearch_matchtype as pdsearch_matchtype,
sum (std1.pdsearch_impressions) as imps,
sum (std1.pdsearch_cost) as cost,
sum (std1.pdsearch_clicks) as clicks,
avg (std1.pdsearch_avg_position) as avg_pos,
sum (std1.pdsearch_impressions * std1.pdsearch_avg_position) as avg_pos_1
--         std1.pdsearch_avg_position                                             as avg_pos


from [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_search_standard as std1


where cast(std1.date as date ) between @report_st and @report_ed
--              and std1.pdsearch_advertiser_id <> '0'
--              and std1.pdsearch_campaign_id in (71700000020836115,71700000021770003)
group by
cast(dateadd( week,datediff( week,0,cast(std1.date as date )),-1) as date ),
datename( month,cast(std1.date as date )),
std1.pdsearch_engineaccount_id,
std1.siteid_dcm,
std1.pdsearch_campaign_id,
std1.pdsearch_keyword_id,
--           k1.paid_search_keyword,
std1.pdsearch_adgroup_id,
--      std1.pdsearch_avg_position,
std1.pdsearch_ad_id,
std1.pdsearch_matchtype

) as t1

left join [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.UALUS_DIM_PaidSearch_Campaign as c1
on t1.pdsearch_campaign_id = c1.Paid_Search_Campaign_ID

left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchKeyword as k1
on t1.pdsearch_keyword_id = k1.paid_search_keyword_id

--      left join openQuery(verticaunited,
--                      'select  cast(paid_search_keyword_id as varchar(50)) as paid_search_keyword_id, cast(paid_search_keyword as varchar(4000)) as paid_search_keyword from diap01.mec_us_united_20056.dfa2_paid_search
--                      group by cast(paid_search_keyword_id as varchar(50)), cast(paid_search_keyword as varchar(4000))'
--           ) as k1
--      on t1.pdsearch_keyword_id = k1.paid_search_keyword_id

--     left join master.dbo.sch_keyword_3 as k1
--     on cast(t1.pdsearch_keyword_id as bigint) = k1.paid_search_keyword_id

--     left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchAdGroup as ad1
--     on t1.pdsearch_adgroup_id = ad1.Paid_Search_AdGroup_ID
--
left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Paid_SearchEngine as e1
on t1.pdsearch_engineaccount_id = e1.Paid_SearchEngine_ID
--
--     left join [10.2.186.148,4721].DM_1161_UnitedAirlinesUSA.dbo.UALUS_DIM_Site as s1
--     on t1.siteid_dcm = s1.site_id_dcm


left join
(


select
--  fld1.date     as "date",
--  fld1.pdsearch_advertiser as pdsearch_advertiser,
--  fld1.pdsearch_advertiser_id as pdsearch_advertiser_id,
fld1.month as [month],
fld1.week as [week],
fld1.pdsearch_engineaccount_id,
fld1.pdsearch_ad_id,
fld1.pdsearch_adgroup_id,
--  fld1.placement_id,
fld1.siteid_dcm,
fld1.pdsearch_campaign_id,
--  fld1.pdsearch_keyword,
fld1.pdsearch_keyword_id,
--  fld1.currency as currency,
sum (fld1.total_revenue * .08) as rev,
sum (fld1.prch) as prch,
sum (fld1.lead) as lead,
--  sum(fld1.total_conversions) as tot_con,
sum (fld1.number_of_tickets) as tix

from (

-- declare @report_st date
-- declare @report_ed date
-- --
-- set @report_ed = '2017-05-06';
-- set @report_st = '2017-01-01';
select
--           fld0.pdsearch_advertiser,
--           fld0.pdsearch_advertiser_id,
cast(fld0.date as date ) as "date",
datename( month,cast(fld0.date as date )) as [month],
cast(dateadd( week,datediff( week,0,cast(fld0.date as date )),-1) as date ) as [week],
fld0.pdsearch_engineaccount_id,
fld0.pdsearch_ad_id,
fld0.pdsearch_adgroup_id,
--           fld0.pdsearch_adgroup,
--                 fld0.placement_id,
fld0.siteid_dcm,
fld0.pdsearch_campaign_id,
fld0.pdsearch_keyword_id,
--               fld0.pdsearch_keyword,
--                  fld0.currency,
sum ( case
when fld0.activity_id = 978826 and
fld0.currency not like '%--%' and
fld0.currency <> 'Miles' and
fld0.number_of_tickets <> 0
then fld0.total_revenue / rates.exchange_rate
end ) as total_revenue,
sum ( case
when fld0.activity_id = 978826 and
fld0.currency not like '%--%' and
fld0.currency <> 'Miles' and
fld0.total_revenue > 0
then fld0.transaction_count
end ) as prch,
sum ( case
when fld0.activity_id = 1086066
then fld0.transaction_count
end ) as lead,
--                  sum(fld0.total_conversions) as total_conversions,
--                  count(*)                                      as this_count,
sum ( case
when fld0.activity_id = 978826 and
fld0.currency not like '%--%' and
fld0.currency <> 'Miles'
then number_of_tickets
end ) as number_of_tickets


from [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_search_floodlight as fld0

left join openquery (verticaunited,
'select currency, date, exchange_rate from diap01.mec_us_mecexchangerates_20067.EXCHANGE_RATES
                                        ') as rates
on fld0.currency = rates.currency
and cast(fld0.date as date ) = cast(rates.date as date )

where cast(fld0.date as date ) between @report_st and @report_ed
--                  and (LEN(ISNULL(fld0.pdsearch_engineaccount_id,'')) > 0)
--                  and (LEN(ISNULL(fld0.pdsearch_adgroup_id,'')) > 0)
--                  and (LEN(ISNULL(fld0.pdsearch_campaign_id,'')) > 0)
--                  and (LEN(ISNULL(fld0.pdsearch_keyword_id,'')) > 0)
--                  and fld0.PdSearch_ad_ID <> '0'
--                  and fld0.number_of_tickets <> 0
--                  and fld0.currency <> 'Miles'
--                  and (LEN(ISNULL(fld0.currency,'')) > 0)
--                  and (PdSearch_EngineAccount not like '%[Br][Rr]%[Nn][Dd]%' and PdSearch_EngineAccount not like '%[Ss][Mm][Ee]%')
and (PdSearch_EngineAccount not like '%[Ss][Mm][Ee]%')
--                      and (PdSearch_Campaign not like '%[Pp][Oo][Ll][Aa][Rr][Ii][Ss]%')
--                  and fld0.currency not like '%--%'
and (fld0.activity_id = 978826 or fld0.activity_id = 1086066)
--                  and fld0.activity_id = 1086066

group by
--          cast(fld0.date as date),
cast(fld0.date as date ),
cast(dateadd( week,datediff( week,0,cast(fld0.date as date )),0) as date ),
fld0.pdsearch_engineaccount_id,
--                 fld0.placement_id,
--                 fld0.pdsearch_advertiser,
fld0.siteid_dcm,
--                 fld0.pdsearch_advertiser_id,
--                  fld0.currency,
--                 fld0.pdsearch_keyword,
--                 fld0.pdsearch_adgroup,
--                 fld0.pnr_base64encoded,
fld0.activity_id,
fld0.pdsearch_ad_id,
fld0.pdsearch_adgroup_id,
fld0.pdsearch_keyword_id,
fld0.pdsearch_campaign_id


) as fld1


group by
--            fld1.date,
--          cast(dateadd(week,datediff(week,0,cast(fld1.date as date)),0) as date),
fld1.pdsearch_engineaccount_id,
--           fld1.placement_id,
fld1.week,
fld1.month,
--           fld1.pdsearch_adgroup,
--           fld1.pnr_base64encoded,
--           fld1.pdsearch_keyword,
--           fld1.pdsearch_advertiser_id,
fld1.siteid_dcm,
fld1.pdsearch_ad_id,
fld1.pdsearch_adgroup_id,
fld1.pdsearch_keyword_id,
--           fld1.currency,
--           fld1.pdsearch_advertiser,
fld1.pdsearch_campaign_id
) as fld2
on t1.pdsearch_campaign_id = fld2.pdsearch_campaign_id
and t1.week = fld2.week
and t1.pdsearch_engineaccount_id = fld2.pdsearch_engineaccount_id
and t1.pdsearch_keyword_id = fld2.pdsearch_keyword_id
and t1.pdsearch_ad_id = fld2.pdsearch_ad_id
and t1.pdsearch_adgroup_id = fld2.pdsearch_adgroup_id
and t1.siteid_dcm = fld2.siteid_dcm

-- where c1.paid_search_campaign not like '%[Pp][Oo][Ll][Aa][Rr][Ii][Ss]%'
--     and c1.paid_search_campaign not like '%GDN%'
--     where e1.Paid_SearchEngine not like '%[Ss][Mm][Ee]%'
--       where e1.Paid_SearchEngine like '%TMK%'
where
e1.Paid_SearchEngine not like '%[Bb][Rr][Nn][Dd]%' and
e1.Paid_SearchEngine not like '%[Ss][Mm][Ee]%' and
e1.Paid_SearchEngine not like '%[Pp][Ll][Cc][Yy]%'

group by
-- t1.date,
--         cast(dateadd(week,datediff(week,0,cast(t1.date as date)),0) as date),
-- t1.month,
-- t1.week,
--     t1.pdsearch_matchtype,
--   t1.pdsearch_engineaccount_id,
-- -- -- --  t1.placement_id,
--   t1.siteid_dcm,
k1.paid_search_keyword,
--     s1.site_dcm,
c1.paid_search_campaign
--     e1.Paid_SearchEngine,
-- t1.pdsearch_adgroup_id
--     ad1.Paid_Search_AdGroup

) as t2


group by
-- t1.date,
--         cast(dateadd(week,datediff(week,0,cast(t1.date as date)),0) as date),
--     t2.month,
--     t2.week,
--     t2.pdsearch_matchtype,
--     t2.Network,
t2.paid_search_keyword,
--   t2.pdsearch_engineaccount_id,
-- -- -- --  t2.placement_id,
--   t2.siteid_dcm,
--     t2.site_dcm,
t2.paid_search_campaign
--     t2.Paid_SearchEngine,
--     t2.pdsearch_adgroup_id,
--     t2.Paid_Search_AdGroup
) as t3
    where t3.r1 <= 10
group by
t3.paid_search_campaign,
t3.paid_search_keyword,
t3.r1

order by
    t3.paid_search_campaign asc,
    t3.r1 desc

