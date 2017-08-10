-- Search SMR: top keywords

declare @report_st date
declare @report_ed date
--
set @report_ed = '2017-07-31';
set @report_st = '2017-07-01';


select
t1.pdsearch_campaign,
t1.pdsearch_keyword,
t1.r1 as Rank,
sum(t1.tix) as Tickets

from (
         select
fld1.pdsearch_campaign,
fld1.pdsearch_keyword,
sum (fld1.number_of_tickets) as tix,
row_number() over ( partition by fld1.pdsearch_campaign order by sum (fld1.number_of_tickets) desc ) as r1

from (
         select
             cast(fld0.date as date) as "date",
             fld0.pdsearch_engineaccount_id,
             fld0.pdsearch_ad_id,
             fld0.pdsearch_adgroup_id,
             fld0.siteid_dcm,
             fld0.pdsearch_campaign_id,
             fld0.pdsearch_campaign,
             fld0.pdsearch_keyword_id,
             fld0.pdsearch_keyword,
             sum(case
                 when fld0.activity_id = 978826 and
                     fld0.currency not like '%--%' and
                     fld0.currency <> 'Miles' and
                     fld0.number_of_tickets <> 0
                     then fld0.total_revenue / rates.exchange_rate
                 end)                as total_revenue,
             sum(case
                 when fld0.activity_id = 978826 and
                     fld0.currency not like '%--%' and
                     fld0.currency <> 'Miles' and
                     fld0.total_revenue > 0
                     then fld0.transaction_count
                 end)                as prch,
             sum(case
                 when fld0.activity_id = 1086066
                     then fld0.transaction_count
                 end)                as lead,
             sum(case
                 when fld0.activity_id = 978826 and
                     fld0.currency not like '%--%' and
                     fld0.currency <> 'Miles'
                     then number_of_tickets
                 end)                as number_of_tickets


         from [10.2.186.148,4721].dm_1161_unitedairlinesusa.dbo.ualus_search_floodlight as fld0

left join openquery (verticaunited,
'select currency, date, exchange_rate from diap01.mec_us_mecexchangerates_20067.EXCHANGE_RATES
                                        ') as rates
on fld0.currency = rates.currency
and cast(fld0.date as date ) = cast(rates.date as date )
where cast(fld0.date as date ) between @report_st and @report_ed
and (LEN(ISNULL(fld0.pdsearch_keyword,'')) > 0)
and (PdSearch_EngineAccount not like '%[Ss][Mm][Ee]%')
and (PdSearch_EngineAccount not like '%[Bb][Rr][Nn][Dd]%')
and (PdSearch_EngineAccount not like '%[Pp][Ll][Cc][Yy]%')
and (fld0.activity_id = 978826 or fld0.activity_id = 1086066)

group by
cast(fld0.date as date ),
fld0.pdsearch_engineaccount_id,
fld0.siteid_dcm,
fld0.pdsearch_campaign,
fld0.pdsearch_keyword,
fld0.activity_id,
fld0.pdsearch_ad_id,
fld0.pdsearch_adgroup_id,
fld0.pdsearch_keyword_id,
fld0.pdsearch_campaign_id

) as fld1


group by
fld1.pdsearch_campaign,
fld1.pdsearch_keyword
) as t1
    where t1.r1 <= 10
    and t1.tix >0
group by
    t1.pdsearch_campaign,
t1.pdsearch_keyword,
    t1.r1

order by t1.pdsearch_campaign asc, t1.r1 asc


