select
--                 t3.campaign,
--                 t3.site_dcm,
                c1.campaign,
                t3.campaign_id,
                s1.site_dcm,
                t3.site_id_dcm,
--                 sum(case when min_yr = 2017 and min_mo = 1 then 1 else 0 end)           as jan_new_users,
--                 sum(case when min_yr = 2017 and min_mo = 1 then jan_17_imps else 0 end) as jan_new_user_imps,
                sum(case when t3.jan_17_imps > 0 then 1 else 0 end)                        as jan_all_users,
                sum(t3.jan_17_imps)                                                        as jan_17_imps,
                sum(jan_17_cst)                                                        as jan_17_cst,
--                 sum(case when min_yr = 2017 and min_mo = 2 then 1 else 0 end)           as feb_new_users,
--                 sum(case when min_yr = 2017 and min_mo = 2 then feb_17_imps else 0 end) as feb_new_user_imps,
                sum(case when feb_17_imps > 0 then 1 else 0 end)                        as feb_all_users,
                sum(feb_17_imps)                                                        as feb_17_imps,
                sum(feb_17_cst)                                                        as feb_17_cst,
--                 sum(case when min_yr = 2017 and min_mo = 3 then 1 else 0 end)           as mar_new_users,
--                 sum(case when min_yr = 2017 and min_mo = 3 then mar_17_imps else 0 end) as mar_new_user_imps,
                sum(case when mar_17_imps > 0 then 1 else 0 end)                        as mar_all_users,
                sum(mar_17_imps)                                                        as mar_17_imps,
                sum(mar_17_cst)                                                        as mar_17_cst

from (

select
                t2.user_id,
                t2.campaign_id,
                t2.site_id_dcm,
--                 min(event_time) as min_imp_time,
                min(t2.year_tim) as min_yr,
                min(t2.mnth_tim) as min_mo,
                sum(case when t2.year_tim = 2017 and t2.mnth_tim = 1 then 1 else 0 end) as jan_17_imps,
                sum(case when t2.year_tim = 2017 and t2.mnth_tim = 2 then 1 else 0 end) as feb_17_imps,
                sum(case when t2.year_tim = 2017  and t2.mnth_tim = 3 then 1 else 0 end) as mar_17_imps,
                sum(case
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 1 and
                         t2.CostMethod = 'dCPM'
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
                         t2.mnth_tim = 2 and
                         t2.CostMethod = 'dCPM'
                    then t2.dbm_cost
                    when t2.year_tim = 2017 and
                         t2.mnth_tim = 3
                    then (t2.rate* cast(t2.total_imp as decimal))/cast(1000 as decimal)
                    else 0 end) as mar_17_cst
from (
                  select
             t001.user_id,
             cast(timestamp_trunc(to_timestamp(t001.event_time / 1000000),'SS') as date) as event_time,
             date_part('year', cast(timestamp_trunc(to_timestamp(t001.event_time / 1000000),'SS') as date))  as year_tim,
             date_part('month', cast(timestamp_trunc(to_timestamp(t001.event_time / 1000000),'SS') as date)) as mnth_tim,
             t001.campaign_id,
             t001.site_id_dcm,
             t001.placement_id,
             r0.rate,
             r0.CostMethod,
             count(*)                                                                    as total_imp,
             cast((sum(dbm_total_media_cost_usd) / 1000000000) as decimal(20,10)) as dbm_cost

         from diap01.mec_us_united_20056.dfa2_impression as t001

             left join (select distinct adserverplacementId, rate, costMethod
             from diap01.mec_us_united_20056.ual_prs_summ) as r0
             on t001.placement_id = r0.adserverplacementId

         where
             user_id <> '0'
                 and t001.campaign_id <> '10698273' -- UK Acquisition 2017
                 and t001.campaign_id <> '11221036' -- Hong Kong 2017
                 and t001.campaign_id <> '10812738' -- Marketing funds 2017
                 and t001.advertiser_id <> '0'
                 and cast(timestamp_trunc(to_timestamp(t001.event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-03-31'
         group by
             t001.user_id,
             cast(timestamp_trunc(to_timestamp(t001.event_time / 1000000),'SS') as date),
             t001.campaign_id,
             t001.site_id_dcm,
             r0.rate,
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


where   regexp_like(c1.campaign,'.*2017.*','ib')
and not regexp_like(c1.campaign,'.*Search.*','ib')
and not regexp_like(c1.campaign,'.*BidManager.*','ib')

group by
    t3.campaign_id,
    t3.site_id_dcm,
    c1.campaign,
    s1.site_dcm


