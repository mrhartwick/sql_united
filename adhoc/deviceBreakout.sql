select

    t1.operating_system,
    t1.browser_platform,
    t1.dev,
    count(distinct user_id) as users,
    sum(imp_nbr)            as imps
from (
         select
             user_id,
             event_time,
             o1.operating_system,
             b1.browser_platform,
--                                 row_number() over() as imp_nbr
             case when regexp_like(p1.placement,'.*mobile.*','ib') then 'mobile'
             when regexp_like(p1.placement,'.*_mob_.*','ib') then 'mobile'
             when regexp_like(p1.placement,'.*_tab_.*','ib') then 'mobile'
             when regexp_like(p1.placement,'.*tablet.*','ib') then 'mobile'
             else 'desktop' end as dev,
             1                  as imp_nbr
         from
             diap01.mec_us_united_20056.dfa2_impression as i1

             left join diap01.mec_us_united_20056.dfa2_operating_systems as o1
             on i1.operating_system_id = o1.operating_system_id

             left join diap01.mec_us_united_20056.dfa2_browsers as b1
             on i1.browser_platform_id = b1.browser_platform_id

             left join diap01.mec_us_united_20056.dfa2_placements as p1

             on i1.placement_id = p1.placement_id
                 and i1.campaign_id = p1.campaign_id
                 and i1.site_id_dcm = p1.site_id_dcm

         where i1.campaign_id = 10742878
             and cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-10-16'
             and (advertiser_id <> 0)
             and user_id <> '0'

     ) as t1

group by
    t1.operating_system,
    t1.dev,
    t1.browser_platform


