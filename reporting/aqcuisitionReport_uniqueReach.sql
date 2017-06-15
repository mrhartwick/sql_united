------- Unique Reach query--will be used for the bi-weekly Acquisition Report
-------- In addition to updating the dates when running it, need to to include the by month cost, imps, and users every time we enter a new month.

-------Before running this query, need to run this procedure so the table is refreshed; For now, it seems like only Matt H. can run the procedure, since he's the owner of the table. Need to work on this:


------------------------------------------------
-- exec master.dbo.crt_prs_summ go

----------------------------------------------------------

select

                c1.campaign,
                t3.campaign_id,
                s1.site_dcm,
                t3.site_id_dcm,
                sum(case when t3.jan_17_imps > 0 then 1 else 0 end)                         as jan_all_users,
--                 sum(t3.jan_17_imps)                                                         as jan_17_imps,
                sum(jan_17_cst)                                                             as jan_17_cst,
                sum(case when feb_17_imps > 0 then 1 else 0 end)                            as feb_all_users,
--                 sum(feb_17_imps)                                                            as feb_17_imps,
                sum(feb_17_cst)                                                             as feb_17_cst,
                sum(case when mar_17_imps > 0 then 1 else 0 end)                            as mar_all_users,
--                 sum(mar_17_imps)                                                            as mar_17_imps,
                sum(mar_17_cst)                                                             as mar_17_cst,
                sum(case when apr_17_imps > 0 then 1 else 0 end)                            as apr_all_users,
--                 sum(apr_17_imps)                                                            as apr_17_imps,
                sum(apr_17_cst)                                                             as apr_17_cst,
                sum(case when t3.may_17_imps > 0 then 1 else 0 end)                         as may_all_users,
--                 sum(t3.may_17_imps)                                                         as may_17_imps,
                sum(may_17_cst)                                                             as may_17_cst,
                sum(case when t3.jun_17_imps > 0 then 1 else 0 end)                         as jun_all_users,
--                 sum(t3.jun_17_imps)                                                         as jun_17_imps,
                sum(jun_17_cst)                                                             as jun_17_cst


from (

select
                t2.user_id,
                t2.campaign_id,
                t2.site_id_dcm,
                min(t2.year_tim) as min_yr,
                min(t2.mnth_tim) as min_mo,
                sum(case when t2.year_tim = 2017 and t2.mnth_tim = 1 then 1 else 0 end) as jan_17_imps,
                sum(case when t2.year_tim = 2017 and t2.mnth_tim = 2 then 1 else 0 end) as feb_17_imps,
                sum(case when t2.year_tim = 2017  and t2.mnth_tim = 3 then 1 else 0 end) as mar_17_imps,
                sum(case when t2.year_tim = 2017  and t2.mnth_tim = 4 then 1 else 0 end) as apr_17_imps,
                sum(case when t2.year_tim = 2017  and t2.mnth_tim = 5 then 1 else 0 end) as may_17_imps,
                sum(case when t2.year_tim = 2017  and t2.mnth_tim = 6 then 1 else 0 end) as jun_17_imps,
                sum(case
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 1 and
                        (t2.CostMethod = 'dCPM')
                    then t2.dbm_cost
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 1
                    then (t2.rate* cast(t2.total_imp as decimal))/cast(1000 as decimal)
                    else 0 end) as jan_17_cst,
                sum(case
                   when  t2.year_tim = 2017 and
                         t2.mnth_tim = 2 and
                         t2.CostMethod = 'dCPM'
                    then t2.dbm_cost
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 2
                    then (t2.rate* cast(t2.total_imp as decimal))/cast(1000 as decimal)
                    else 0 end) as feb_17_cst,
                sum(case
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 3 and
                         t2.CostMethod = 'dCPM'
                    then t2.dbm_cost
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 3
                    then (t2.rate* cast(t2.total_imp as decimal))/cast(1000 as decimal)
                    else 0 end) as mar_17_cst,
                sum(case
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 4 and
                         t2.CostMethod = 'dCPM'
                    then t2.dbm_cost
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 4
                    then (t2.rate* cast(t2.total_imp as decimal))/cast(1000 as decimal)
                    else 0 end) as apr_17_cst,
                sum(case
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 5 and
                         t2.CostMethod = 'dCPM'
                    then t2.dbm_cost
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 5
                    then (t2.rate* cast(t2.total_imp as decimal))/cast(1000 as decimal)
                    else 0 end) as may_17_cst,
                sum(case
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 6 and
                         t2.CostMethod = 'dCPM'
                    then t2.dbm_cost
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 6
                    then (t2.rate* cast(t2.total_imp as decimal))/cast(1000 as decimal)
                    else 0 end) as jun_17_cst
from (
                  select
             t001.user_id,
             cast(timestamp_trunc(to_timestamp(t001.event_time / 1000000),'SS') as date) as event_time,
             date_part('year', cast(timestamp_trunc(to_timestamp(t001.event_time / 1000000),'SS') as date))  as year_tim,
             date_part('month', cast(timestamp_trunc(to_timestamp(t001.event_time / 1000000),'SS') as date)) as mnth_tim,
             case
--           when t001.campaign_id = 8964059 and regexp_like(p1.placement,'.*Chicago.*','ib') then 11177760
             when t001.campaign_id = 8964059 and (regexp_like(p1.placement,'PG5.*','ib') or regexp_like(p1.placement,'PG6.*','ib')) then 11177760
             when t001.campaign_id = 8964059 and regexp_like(p1.placement,'PFG.*','ib') then 10942240
             when t001.campaign_id = 8964059 and regexp_like(p1.placement,'PG0.*','ib') then 11152017
             when t001.campaign_id = 8958859 then 10742878
                 else t001.campaign_id end as campaign_id,
--              t001.campaign_id,
             case when t001.site_id_dcm = '2202011' or t001.site_id_dcm = '3266673' then '1578478' else t001.site_id_dcm end as site_id_dcm,
--           t001.site_id_dcm,
             t001.placement_id,
             r0.rate,
             case when t001.site_id_dcm = '2202011' or t001.site_id_dcm = '3266673' or t001.site_id_dcm = '1578478' then 'dCPM' else r0.CostMethod end as CostMethod,
             count(*)                                                                    as total_imp,
             cast((sum(t001.dbm_total_media_cost_usd) / 1000000000) as decimal(20,10)) as dbm_cost

         from diap01.mec_us_united_20056.dfa2_impression as t001

             left join (select distinct adserverplacementId, rate, costMethod
             from diap01.mec_us_united_20056.ual_prs_summ) as r0
             on t001.placement_id = r0.adserverplacementId

             LEFT outer JOIN
            diap01.mec_us_united_20056.dfa2_placements AS p1
            ON t001.placement_id = p1.placement_id

         where
             user_id <> '0'
                 and t001.campaign_id in (8964059, 8958859, 11177760, 11224605, 10742878)
                 and t001.advertiser_id <> '0'
                 and cast(timestamp_trunc(to_timestamp(t001.event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-06-06'

         group by
             t001.user_id,
             cast(timestamp_trunc(to_timestamp(t001.event_time / 1000000),'SS') as date),
             t001.campaign_id,
             t001.site_id_dcm,
             r0.rate,
             p1.placement,
              r0.CostMethod,
             t001.placement_id

    ) as t2

group by
    t2.user_id,
    t2.campaign_id,
    t2.placement_id,
    t2.site_id_dcm
    ) as t3


    LEFT outer JOIN
    diap01.mec_us_united_20056.dfa2_campaigns  AS c1
    ON t3.campaign_id = c1.campaign_id

    LEFT outer JOIN
    diap01.mec_us_united_20056.dfa2_sites AS s1
    ON t3.site_id_dcm = s1.site_id_dcm


where
     regexp_like(c1.campaign,'.*2017.*','ib')
and not regexp_like(c1.campaign,'.*Search.*','ib')



group by
    t3.campaign_id,
    t3.site_id_dcm,
    c1.campaign,
    s1.site_dcm
