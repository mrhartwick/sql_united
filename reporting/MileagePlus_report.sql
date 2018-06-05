
------MILEAGEPLUS REPORT
--      Version incorporating capped cost and DBM cost
--
--  this query is a bit of a hack. non-optimal aspects are necessitated by the particularities of the current tech stack on united.
--   code is most easily read by starting at the "innermost" block, inside the openquery call.
--
--   data must be pulled and reconciled from 1) prisma and moat in datamart, and 2) dfa/dv/moat in vertica*.
--   because of its scale, log-level dfa data (dtf 1.0) must be kept in vertica to preserve performance. dv and moat are stored there as well, mostly for convenience.
--   intermediary summary tables are necessary for this query, so we need to be able to create our own tables and run stored procedures to refresh those tables. but we don't have write access to vertica, so we can't keep routines and tables there.
--   di could do this, but edits to these routines are frequent (esp. in joinkey fields), so keeping this process in-house is more convenient for all parties.
-- */

-- these summary/reference tables can be run once a day as a regular process or before the query is run
-- -- -- --
-- exec master.dbo.crt_dv_summ go    -- crt_ separate dv aggregate table and store it in my instance; joining to the vertica table in the query
-- exec master.dbo.crt_mt_summ go    -- crt_ separate moat aggregate table and store it in my instance; joining to the vertica table in the query
-- exec [10.2.186.148\SQLINS02, 4721].dm_1161_unitedairlinesusa.dbo.crt_ivd_summTbl go
--
-- exec [10.2.186.148\SQLINS02, 4721].DM_1161_UnitedAirlinesUSA.dbo.crt_prs_viewTbl go
-- exec [10.2.186.148\SQLINS02, 4721].dm_1161_unitedairlinesusa.dbo.crt_prs_amttbl go
-- exec [10.2.186.148\SQLINS02, 4721].dm_1161_unitedairlinesusa.dbo.crt_prs_packtbl go
-- exec [10.2.186.148\SQLINS02, 4721].dm_1161_unitedairlinesusa.dbo.crt_prs_summtbl go
-- exec master.dbo.crt_dfa_flatCost_dt2 go
-- exec master.dbo.crt_dbm_cost go
-- exec master.dbo.crt_dfa_cost_dt2 go



declare @report_st date
declare @report_ed date
--
set @report_ed = '2018-05-31';
set @report_st = '2018-05-10';

--
-- set @report_ed = dateadd(day, -datepart(day, getdate()), getdate());
-- set @report_st = dateadd(day, 1 - datepart(day, @report_ed), @report_ed);

select
    cast(t3.dcmdate as date)                                                                           as "date",
    cast(dateadd(week,datediff(week,0,cast(t3.dcmdate as date)),0) as date)                            as "week",
    datename(month,cast(t3.dcmdate as date))                                                           as "month",
    case when t3.placement like '%DEN%' then 'DEN'
         when t3.placement like '%EWR%' then 'EWR'
         when t3.placement like '%IAD%' then 'IAD'
         when t3.placement like '%IAH%' then 'IAH'
         when t3.placement like '%LAX%' then 'LAX'
         when t3.placement like '%ORD%' then 'ORD'
         when t3.placement like '%SFO%' then 'SFO'
         else '' end                                                                                   as "Hubs",

    case when t3.placement like '%Prospecting%' then 'Prospecting'
         when t3.placement like '%Retargeting%' then 'Retargeting'
         else '' end                                                                                   as "Site_Type",   
    case when t3.placement like '%Desktop%' then 'Desktop'
         when t3.placement like '%Mobile%' then 'Mobile'
         when t3.placement like '%Tablet%' then 'Tablet'
         else '' end                                                                                   as "Device_Type",     
    case when t3.placement like '%100-200%' then '100-200'
         when t3.placement like '%200-300%' then '200-300'
         when t3.placement like '%300-400%' then '300-400'
         when t3.placement like '%400-500%' then '400-500'
         when t3.placement like '%500-600%' then '500-600'
         when t3.placement like '%600-700%' then '600-700'
         when t3.placement like '%700-800%' then '700-800'
         else '' end                                                                                   as "Score",     
    t3.dvjoinkey                                                                                       as dvjoinkey,
    t3.mtjoinkey                                                                                       as mtjoinkey,
    t3.packagecat                                                                                      as packagecat,
    t3.cost_id                                                                                         as cost_id,
    [dbo].udf_campaignname(t3.campaign_id,t3.campaign)                                                 as campaign,
    t3.campaign_id                                                                                     as "campaign id",
    [dbo].udf_sitename(t3.site_dcm)                                                                    as "site",
    t3.site_id_dcm                                                                                     as "site id",
    t3.costmethod                                                                                      as "cost method",
    t3.placement                                                                                       as placement,
    t3.placement_id                                                                                    as placement_id,
    t3.placementend                                                                                    as "placement end",
    t3.placementstart                                                                                  as "placement start",
    t3.dv_map                                                                                          as "dv map",
    t3.rate                                                                                            as rate,
    t3.planned_amt                                                                                     as "planned amt",
    t3.planned_cost                                                                                    as "planned cost",
    sum(t3.cost)                                                                                       as cost,
    sum(t3.dlvrimps)                                                                                   as "delivered impressions",
    sum(t3.billimps)                                                                                   as "billable impressions",
    sum(t3.cnslimps)                                                                                   as "dfa impressions",
    sum(t3.iv_impressions)                                                                             as "innovid impressions",
    sum(t3.clicks)                                                                                     as clicks,
    sum(t3.click_reg)                                                                                  as clk_reg,
    sum(t3.vew_reg)                                                                                    as vew_reg,
    sum(t3.click_reg) + sum(t3.vew_reg)                                                                  as tot_reg


from (

-- for running the code here instead of at "f3," above

-- declare @report_st date,
-- @report_ed date;
-- --
-- set @report_ed = '2018-05-31';
-- set @report_st = '2018-05-10';

select
    cast(t2.dcmdate as date)                                                   as dcmdate,
    t2.dcmmonth                                                                as dcmmonth,
    t2.dcmmatchdate                                                            as dcmmatchdate,
    t2.diff                                                                    as diff,
    dv.joinkey                                                                 as dvjoinkey,
    mt.joinkey                                                                 as mtjoinkey,
    iv.joinkey                                                                 as ivjoinkey,
    t2.packagecat                                                              as packagecat,
    t2.cost_id                                                                 as cost_id,
    t2.campaign                                                                as campaign,
    t2.campaign_id                                                             as campaign_id,
    t2.site_dcm                                                                as site_dcm,
    t2.site_id_dcm                                                             as site_id_dcm,
    t2.costmethod                                                              as costmethod,
    sum(1) over (partition by t2.cost_id,t2.dcmmatchdate
        order by t2.dcmmonth asc range between unbounded preceding and current row) as flat_count,
    sum(1) over (partition by t2.cost_id
        order by t2.dcmmonth asc range between unbounded preceding and current row) as amt_count,
    t2.plce_id                                                                 as plce_id,
    t2.placement                                                               as placement,
    t2.placement_id                                                            as placement_id,
    t2.placementend                                                            as placementend,
    t2.placementstart                                                          as placementstart,
    t2.dv_map                                                                  as dv_map,
    t2.planned_amt                                                             as planned_amt,
    t2.planned_cost                                                            as planned_cost,
    sum(cst.flatcost)                                                          as flatcost,
    sum(cst.cost)                                                              as cost,
    t2.rate                                                                    as rate,

    sum(case when t2.costmethod = 'Flat' then t2.impressions else cst.dlvrimps end) as dlvrimps,
    sum(case when t2.costmethod = 'Flat' then t2.impressions else cst.billimps end) as billimps,
    sum(case when t2.costmethod = 'Flat' then t2.impressions else cst.dfa_imps end) as cnslimps,
    sum(iv.impressions)                                                             as iv_impressions,
    sum(cst.iv_imps)                                                                as iv_impressions_chk,
    sum(iv.click_thrus)                                                             as iv_clicks,
      sum(iv.all_completion)                                                          as iv_completes,
    sum(cast(dv.total_impressions as int))                                          as dv_impressions,
    sum(cst.dv_imps)                                                                as dv_impressions_chk,
    sum(dv.groupm_passed_impressions)                                               as dv_viewed,
    sum(cast(dv.groupm_billable_impressions as decimal(10,2)))                      as dv_groupmpayable,
    sum(cast(mt.total_impressions as int))                                          as mt_impressions,
    sum(cst.mt_imps)                                                                as mt_impressions_chk,
    sum(mt.groupm_passed_impressions)                                               as mt_viewed,
    sum(cast(mt.groupm_billable_impressions as decimal(10,2)))                      as mt_groupmpayable,
     sum(t2.click_reg)                                                              as click_reg,
     sum(t2.vew_reg)                                                                as vew_reg,
      sum(case
        when (len(isnull(iv.joinkey,'')) > 0) then iv.click_thrus
        else t2.clicks end)                                                       as clicks


       from
         (
-- =========================================================================================================================
-- --
-- declare @report_st date,
-- @report_ed date;
-- --
-- set @report_ed = '2016-10-21';
-- set @report_st = '2016-10-10';
           select
               t1.dcmdate                                   as dcmdate,
               cast(month(cast(t1.dcmdate as date)) as int) as dcmmonth,
               [dbo].udf_dateToInt(t1.dcmdate)              as dcmmatchdate,
               t1.campaign                                  as campaign,
               t1.campaign_id                               as campaign_id,
               t1.site_dcm                                  as site_dcm,
               t1.site_id_dcm                               as site_id_dcm,
               t1.plce_id                                   as plce_id,
               t1.placement                                 as placement,
               t1.placement_id                              as placement_id,
               prs.stdate                                   as stdate,
               prs.eddate                                   as eddate,
               prs.packagecat                               as packagecat,
               prs.costmethod                               as costmethod,
               prs.cost_id                                  as cost_id,
               prs.planned_amt                              as planned_amt,
               prs.planned_cost                             as planned_cost,
               prs.placementstart                           as placementstart,
               prs.placementend                             as placementend,
               cast(prs.rate as decimal(10,2))              as rate,
               sum(t1.impressions)                          as impressions,
               sum(t1.clicks)                               as clicks,
               sum(t1.click_reg)                            as click_reg, 
               sum(t1.vew_reg)                              as vew_reg, 

               case when cast(month(prs.placementend) as int) - cast(month(cast(t1.dcmdate as date)) as int) <= 0 then 0
                    else cast(month(prs.placementend) as int) - cast(month(cast(t1.dcmdate as date)) as int) end as diff,
               [dbo].udf_dvMap(t1.campaign_id,t1.site_id_dcm,t1.placement,prs.CostMethod,prs.dv_map) as dv_map



-- ==========================================================================================================================================================

-- openquery function call must not exceed 8,000 characters; no room for comments inside the function
    from (
           select *
           from openQuery(verticaunited,
'
select
cast(r1.date as date)                     as dcmdate,
cast(month(cast(r1.date as date)) as int) as reportmonth,
campaign.campaign                         as campaign,
r1.campaign_id                            as campaign_id,
r1.site_id_dcm                            as site_id_dcm,
directory.site_dcm                        as site_dcm,
left(p1.placement,6)                      as plce_id,
replace(replace(p1.placement ,'','', ''''),''"'','''') as placement,
r1.placement_id                           as placement_id,
sum(r1.impressions)                       as impressions,
sum(r1.clicks)                            as clicks,
sum(r1.clk_reg)                           as click_reg,
sum(r1.vew_reg)                           as vew_reg


from (


select
cast (timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),''SS'') as date ) as "date"
,ta.campaign_id as campaign_id
,ta.site_id_dcm as site_id_dcm
,ta.placement_id as placement_id
,0 as impressions
,0 as clicks
,sum(case when ta.conversion_id = 1 then ta.total_conversions else 0 end ) as clk_reg
,sum(case when ta.conversion_id = 2 then ta.total_conversions else 0 end ) as vew_reg




from
(
select *
from diap01.mec_us_united_20056.dfa2_activity
where cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),''SS'') as date ) between ''2018-05-10'' and ''2018-05-31''
and (advertiser_id <> 0)
and (length(isnull(event_sub_type,'''')) > 0)
and (activity_id = 7126762)
) as ta


group by
cast (timestamp_trunc(to_timestamp(ta.interaction_time / 1000000),''SS'') as date )
,ta.campaign_id
,ta.site_id_dcm
,ta.placement_id


union all

select
cast (timestamp_trunc(to_timestamp(ti.event_time / 1000000),''SS'') as date ) as "date"
,ti.campaign_id as campaign_id
,ti.site_id_dcm as site_id_dcm
,ti.placement_id as placement_id
,count (*) as impressions
,0 as clicks
,0 as clk_reg
,0 as vew_reg


from (
select *
from diap01.mec_us_united_20056.dfa2_impression
where cast (timestamp_trunc(to_timestamp(event_time / 1000000),''SS'') as date ) between ''2018-05-10'' and ''2018-05-31''


and (advertiser_id <> 0)
) as ti
group by
cast (timestamp_trunc(to_timestamp(ti.event_time / 1000000),''SS'') as date )
,ti.campaign_id
,ti.site_id_dcm
,ti.placement_id

union all

select
cast (timestamp_trunc(to_timestamp(tc.event_time / 1000000),''SS'') as date ) as "date"
,tc.campaign_id as campaign_id
,tc.site_id_dcm as site_id_dcm
,tc.placement_id as placement_id
,0 as impressions
,count (*) as clicks
,0 as clk_reg
,0 as vew_reg



from (

select *
from diap01.mec_us_united_20056.dfa2_click
where cast (timestamp_trunc(to_timestamp(event_time / 1000000),''SS'') as date ) between ''2018-05-10'' and ''2018-05-31''
and (advertiser_id <> 0)
) as tc

group by
cast (timestamp_trunc(to_timestamp(tc.event_time / 1000000),''SS'') as date )
,tc.campaign_id
,tc.site_id_dcm
,tc.placement_id


) as r1

left join
(
select cast (campaign as varchar (4000)) as ''campaign'',campaign_id as ''campaign_id''
from diap01.mec_us_united_20056.dfa2_campaigns
) as campaign
on r1.campaign_id = campaign.campaign_id

left join
(
select cast (p1.placement as varchar (4000)) as ''placement'',p1.placement_id as ''placement_id'',p1.campaign_id as ''campaign_id'',p1.site_id_dcm as ''site_id_dcm''

from ( select campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast (placement_start_date as date ) as thisdate,
row_number() over (partition by campaign_id,site_id_dcm,placement_id order by cast (placement_start_date as date ) desc ) as x1
from diap01.mec_us_united_20056.dfa2_placements

) as p1
where x1 = 1
) as p1
on r1.placement_id    = p1.placement_id
and r1.campaign_id = p1.campaign_id
and r1.site_id_dcm  = p1.site_id_dcm

left join
(
select cast (site_dcm as varchar (4000)) as ''site_dcm'',site_id_dcm as ''site_id_dcm''
from diap01.mec_us_united_20056.dfa2_sites
) as directory
on r1.site_id_dcm = directory.site_id_dcm

where  regexp_like(p1.placement,''P.?'',''ib'')
and not regexp_like(p1.placement,''.?do\s?not\s?use.?'',''ib'')
and not regexp_like(campaign.campaign,''.*Search.*'',''ib'')
and not regexp_like(campaign.campaign,''.*BidManager.*'',''ib'')
and  regexp_like(campaign.campaign,''.*2018.*'',''ib'')
and r1.campaign_id = 21057553



group by
cast (r1.date as date)
,directory.site_dcm
,r1.site_id_dcm
,r1.campaign_id
,campaign.campaign
,r1.placement_id
,p1.placement
')

         ) as t1

        --    Prisma data
      left join
      (
        select *
        from [10.2.186.148\SQLINS02, 4721].dm_1161_unitedairlinesusa.[dbo].prs_summ
      ) as prs
        on t1.placement_id = prs.adserverplacementid


        where t1.campaign not like '%Search%'
        and t1.campaign not like '%[_]UK[_]%'
        and t1.campaign not like '%2016%'
        and t1.campaign not like '%2015%'

    group by
       t1.dcmdate
      ,cast(month(cast(t1.dcmdate as date)) as int)
      ,t1.campaign
      ,t1.campaign_id
      ,t1.site_dcm
      ,t1.site_id_dcm
      ,t1.plce_id
      ,t1.placement
      ,t1.placement_id
      ,prs.packagecat
      ,prs.rate
      ,prs.costmethod
      ,prs.cost_id
      ,prs.planned_amt
      ,prs.planned_cost
      ,prs.placementend
      ,prs.placementstart
      ,prs.stdate
      ,prs.eddate
      ,prs.dv_map

  ) as t2

--    Cost data
      left join
      (
        select *
        from master.dbo.dfa_cost_dt2
      ) as cst
        on cast(t2.dcmmatchdate as varchar(8)) + t2.plce_id = cast(cst.dcmdate as varchar(8)) + cst.plce_id


-- dv table join ==============================================================================================================================================

  left join (
              select *
              from master.dbo.dv_summ
              where dvdate between @report_st and @report_ed
            ) as dv
      on
      left(t2.placement,6) + '_' + [dbo].udf_sitekey(t2.site_dcm) + '_'
      + cast(t2.dcmdate as varchar(10)) = dv.joinkey

-- moat table join ==============================================================================================================================================

  left join (
              select *
              from master.dbo.mt_summ
              where mtdate between @report_st and @report_ed
            ) as mt
      on
      left(t2.placement,6) + '_' + [dbo].udf_sitekey( t2.site_dcm) + '_'
      + cast(t2.dcmdate as varchar(10)) = mt.joinkey


-- innovid table join ==============================================================================================================================================

  left join (
              select *
              from [10.2.186.148\SQLINS02, 4721].dm_1161_unitedairlinesusa.[dbo].ivd_summ_agg
              where ivdate between @report_st and @report_ed
            ) as iv
      on
      left(t2.placement,6) + '_' + [dbo].udf_sitekey( t2.site_dcm) + '_'
      + cast(t2.dcmdate as varchar(10)) = iv.joinkey


group by

  t2.campaign
  ,t2.campaign_id
  ,t2.cost_id
  ,t2.dv_map
  ,t2.site_dcm
  ,t2.site_id_dcm
  ,t2.packagecat
  ,t2.placementend
  ,t2.placementstart
  ,t2.plce_id
  ,t2.placement_id
  ,t2.planned_amt
  ,t2.planned_cost
  ,t2.rate
  ,t2.placement
  ,t2.eddate
  ,t2.stdate
  ,t2.dcmdate
  ,t2.dcmmatchdate
  ,t2.dcmmonth
  ,t2.diff
  ,dv.joinkey
  ,mt.joinkey
  ,iv.joinkey
  ,cst.plce_id
  ,t2.costmethod


     ) as t3


group by
  t3.dcmdate
  ,t3.dcmmonth
  ,t3.diff
  ,t3.dvjoinkey
  ,t3.mtjoinkey
  ,t3.packagecat
  ,t3.cost_id
  ,t3.campaign
  ,t3.campaign_id
  ,t3.site_dcm
  ,t3.site_id_dcm
  ,t3.costmethod
  ,t3.rate
  ,t3.placement
  ,t3.placement_id
  ,t3.placementend
  ,t3.placementstart
  ,t3.dv_map
  ,t3.planned_amt
  ,t3.planned_cost



order by
  t3.cost_id,
  t3.dcmdate;
