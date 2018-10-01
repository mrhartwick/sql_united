-- Find Impressions served per minute, by campaign and partner
-- Purpose is to determine what level of weather API we need
-- for the weather test (most APIs priced on calls per min)

select
    t3.campaign,
    t3.site_dcm,
    sum(min_cnt / imps) as imps_per_min
from (
         select

             t2.campaign,
             t2.site_dcm,
             count(t2.min_tim) as min_cnt,
             sum(imps)         as imps

         from (
                  select
                      ti.min_tim,
                      count(*) as imps,
                      c1.campaign,
                      s1.site_dcm

                  from (
                           select
                               *,
                               minute(to_timestamp(event_time / 1000000)) as min_tim

                           from diap01.mec_us_united_20056.dfa2_impression
                           where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-07-31'
                               and (advertiser_id <> 0)
                       ) as ti

                      left join
                      diap01.mec_us_united_20056.dfa2_campaigns

                          as c1
                      on ti.campaign_id = c1.campaign_id

                      left join
                      (
                          select
                              cast(p1.placement as varchar(4000)) as 'placement',p1.placement_id as 'placement_id',p1.campaign_id as 'campaign_id',p1.site_id_dcm as 'site_id_dcm'
                          from (select
                              campaign_id                                            as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast(placement_start_date as date) as thisdate,
                              row_number() over ( partition by campaign_id,site_id_dcm,placement_id
                                  order by cast(placement_start_date as date) desc ) as x1
                          from diap01.mec_us_united_20056.dfa2_placements
                               ) as p1
                          where x1 = 1
                      ) as p1
                      on ti.placement_id = p1.placement_id
                          and ti.campaign_id = p1.campaign_id
                          and ti.site_id_dcm = p1.site_id_dcm

                      left join
                      diap01.mec_us_united_20056.dfa2_sites
                          as s1
                      on ti.site_id_dcm = s1.site_id_dcm

                  where regexp_like(p1.placement,'P.?','ib')
                      and not regexp_like(p1.placement,'.?do\s?not\s?use.?','ib')
                      and regexp_like(c1.campaign,'.*2017.*','ib')
                      and not regexp_like(c1.campaign,'.*Search.*','ib')
                      and not regexp_like(c1.campaign,'.*BidManager.*','ib')

                  group by
                      ti.min_tim,
                      c1.campaign,
                      s1.site_dcm
              ) as t2

         group by
             t2.campaign,
             t2.site_dcm
     ) as t3
group by
    t3.campaign,
    t3.site_dcm