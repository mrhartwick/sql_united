
/*
Impressions, cost, and conversions
 */

select
    a.user_id,
--          conversiontime,
--          revenue,
    real_tim,
    sum(real_imp) as real_imp,
    sum(real_cst) as real_cst,
    sum(real_con) as real_con,
    sum(real_rev) as real_rev,
    ghst_tim,
    sum(ghst_imp) as ghst_imp,
    sum(ghst_cst) as ghst_cst,
    sum(ghst_con) as ghst_con,
    sum(ghst_rev) as ghst_rev
--          impressiontime,
--          cvr_nbr,
--          imp_nbr
from
    (select
         user_id,
--               timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as impressiontime,
         cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as real_tim,
--              timestamp_trunc(to_timestamp(event_time / 1000000),'HH') as real_tim,
--              timestamp_trunc(to_timestamp(event_time / 1000000),'MI') as real_tim,
--              timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as real_tim,
         0                                                                      as real_con,
         0                                                                      as real_rev,
         count(*)                                                               as real_imp,
         sum(cast((dbm_media_cost_usd / 1000000000) as decimal(20,10)))         as real_cst
     from
         diap01.mec_us_united_20056.dfa2_impression

--           where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
     where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-07-15'
         and (site_id_dcm = 1578478)
         and user_id <> '0'
         and (advertiser_id <> 0)
     group by
         user_id,
--                  timestamp_trunc(to_timestamp(event_time / 1000000),'HH')
--              timestamp_trunc(to_timestamp(event_time / 1000000),'MI')
--              timestamp_trunc(to_timestamp(event_time / 1000000),'SS')
         cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date)

     union all

     select
         user_id,
         cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) as real_tim,
-- --                   timestamp_trunc(to_timestamp(interaction_time / 1000000),'HH') as real_tim,
--                  timestamp_trunc(to_timestamp(interaction_time / 1000000),'MI') as real_tim,
--                  timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as real_tim,
         count(*)                                                                     as real_con,
         sum((t1.total_revenue * 1000000) / rates.exchange_rate)                      as real_rev,
         0                                                                            as real_imp,
         0                                                                            as real_cst
--              cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as conversiontime,

    from
        diap01.mec_us_united_20056.dfa2_activity as t1

        left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
        on upper(substring(other_data,(instr(other_data,'u3=') + 3),3)) = upper(rates.currency)
            and cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) = rates.date

--           where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
    where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2016-07-15' and '2016-07-15'
        and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
        and total_revenue <> 0
        and activity_id = 978826
        and (advertiser_id <> 0)
        and user_id <> '0'
        and (site_id_dcm = 1578478)

    group by
        user_id,
--                  timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS')
--              timestamp_trunc(to_timestamp(interaction_time / 1000000),'MI')
--              timestamp_trunc(to_timestamp(interaction_time / 1000000),'HH')
        cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date)
    ) as a,
    (select
         user_id,
--               timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as impressiontime,
         cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as ghst_tim,
--              timestamp_trunc(to_timestamp(event_time / 1000000),'HH') as ghst_tim,
--              timestamp_trunc(to_timestamp(event_time / 1000000),'MI') as ghst_tim,
--              timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as ghst_tim,
         0                                                                      as ghst_con,
         0                                                                      as ghst_rev,
         count(*)                                                               as ghst_imp,
         sum(cast((dbm_media_cost_usd / 1000000000) as decimal(20,10)))         as ghst_cst
     from
         diap01.mec_us_united_20056.dfa2_impression

--           where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
     where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-07-15'
--               and (campaign_id = 9639387 or campaign_id = 8958859)
         and (site_id_dcm = 2202011)
         and user_id <> '0'
         and (advertiser_id <> 0)
     group by
         user_id,
--                  timestamp_trunc(to_timestamp(event_time / 1000000),'HH')
--              timestamp_trunc(to_timestamp(event_time / 1000000),'MI')
--              timestamp_trunc(to_timestamp(event_time / 1000000),'SS')
         cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date)

     union all

     select
         user_id,
         cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) as real_tim,
--                  timestamp_trunc(to_timestamp(interaction_time / 1000000),'HH') as ghst_tim,
--                  timestamp_trunc(to_timestamp(interaction_time / 1000000),'MI') as ghst_tim,
--                  timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as ghst_tim,
         count(*)                                                                     as ghst_con,
         sum((t1.total_revenue * 1000000) / rates.exchange_rate)                      as ghst_rev,
         0                                                                            as ghst_imp,
         0                                                                            as ghst_cst
--              cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as conversiontime,

    from
        diap01.mec_us_united_20056.dfa2_activity as t1

        left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
        on upper(substring(other_data,(instr(other_data,'u3=') + 3),3)) = upper(rates.currency)
            and cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) = rates.date

--           where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
    where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2016-07-15' and '2016-07-15'
        and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
        and total_revenue <> 0
        and activity_id = 978826
        and (advertiser_id <> 0)
        and user_id <> '0'
        and (site_id_dcm = 2202011)

    group by
        user_id,
--                  timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS')
--              timestamp_trunc(to_timestamp(interaction_time / 1000000),'MI')
--              timestamp_trunc(to_timestamp(interaction_time / 1000000),'HH')
        cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date)
    ) as b
where
    a.user_id = b.user_id
        and
        real_tim = ghst_tim
--              and
--              conversiontime > impressiontime
group by
    a.user_id,
    real_tim,
    ghst_tim

-- ====================================================================================================================
-- ====================================================================================================================

-- QA

select

    count(*) as con,
    count( distinct user_id) as user_cnt,
--
-- user_id,
--              dbm_auction_id,
                 sum((t1.total_revenue * 1000000) / rates.exchange_rate) as revenue
--               event_time as conversiontime,
--               row_number() over() as cvr_nbr
             from
                 diap01.mec_us_united_20056.dfa2_activity as t1

                 left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
                     on upper(substring(other_data,(instr(other_data,'u3=') + 3),3)) = upper(rates.currency)
                     and cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) = rates.date

                        left join
                    (
                    select p1.placement,p1.placement_id ,p1.campaign_id,p1.site_id_dcm

                    from ( select campaign_id ,site_id_dcm, placement_id ,placement,cast (placement_start_date as date ) as thisdate,
                    row_number() over (partition by campaign_id, site_id_dcm, placement_id order by cast (placement_start_date as date ) desc ) as x1
                    from diap01.mec_us_united_20056.dfa2_placements
                        ) as p1
                    where x1 = 1
                    ) as p2
                    on t1.placement_id = p2.placement_id

             where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
                 and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
                 and total_revenue <> 0
                 and total_conversions <> 0
                 and activity_id = 978826
                 and (advertiser_id <> 0)
                 and user_id <> '0'
                 and (t1.campaign_id = 9639387 or t1.campaign_id = 8958859)
                 and (t1.site_id_dcm = 1578478 or t1.site_id_dcm = 2202011)
                 and (left(p2.placement,6) not like 'P9P%'
                 and left(p2.placement,6) not like 'PBN%')

-- ====================================================================================================================

select
    count(*) as imps,
    count( distinct user_id) as user_cnt,
--              user_id,
--              event_time                                               as impressiontime,
--              row_number() over ()                                     as imp_nbr,
                sum(cast((dbm_total_media_cost_usd / 1000000000) as decimal)) as cost
            from
                diap01.mec_us_united_20056.dfa2_impression as t2

                left join
                (
                    select
                        p1.placement,p1.placement_id,p1.campaign_id,p1.site_id_dcm
                    from (select
                        campaign_id,site_id_dcm,placement_id,placement,cast(placement_start_date as date) as thisdate,
                        row_number() over (partition by campaign_id,site_id_dcm,placement_id order by cast(placement_start_date as date) desc) as x1
                    from diap01.mec_us_united_20056.dfa2_placements
                         ) as p1
                    where x1 = 1
                ) as p3
                on t2.placement_id = p3.placement_id

            where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
                and (t2.campaign_id = 9639387 or t2.campaign_id = 8958859)
                and (t2.site_id_dcm = 1578478 or t2.site_id_dcm = 2202011)
                and user_id <> '0'
                and (advertiser_id <> 0)
--      Big Blue - Google prospecting
                and (left(p3.placement,6) not like 'P9P%'
                and left(p3.placement,6) not like 'PBN%')

-- ====================================================================================================================
-- ====================================================================================================================

/*
Check for dupe conversions
 */

select
    a.user_id,
--          conversiontime,
--          revenue,
    real_tim,
    sum(real_imp) as real_imp,
    sum(real_cst) as real_cst,
    sum(real_con) as real_con,
    sum(real_rev) as real_rev,
    ghst_tim,
    sum(ghst_imp) as ghst_imp,
    sum(ghst_cst) as ghst_cst,
    sum(ghst_con) as ghst_con,
    sum(ghst_rev) as ghst_rev
--          impressiontime,
--          cvr_nbr,
--          imp_nbr
from
    (select
         user_id,
--       cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as real_tim,
-- --                   timestamp_trunc(to_timestamp(event_time / 1000000),'HH') as real_tim,
                    timestamp_trunc(to_timestamp(event_time / 1000000),'MI') as real_tim,
--                  timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as real_tim,
--      to_timestamp(event_time / 1000000) as real_tim,
         count(*)                                                                     as real_con,
         sum((t1.total_revenue * 1000000) / rates.exchange_rate)                      as real_rev,
         0                                                                            as real_imp,
         0                                                                            as real_cst
--              cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as conversiontime,

    from
        diap01.mec_us_united_20056.dfa2_activity as t1

        left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
        on upper(substring(other_data,(instr(other_data,'u3=') + 3),3)) = upper(rates.currency)
            and cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) = rates.date

             where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-12-21' and '2016-12-21'
--  where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-12-21' and '2016-12-21'
        and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
        and total_revenue <> 0
        and activity_id = 978826
        and (advertiser_id <> 0)
        and user_id <> '0'
        and (site_id_dcm = 1578478)

    group by
        user_id,
--      to_timestamp(event_time / 1000000)
--                  timestamp_trunc(to_timestamp(event_time / 1000000),'SS')
                timestamp_trunc(to_timestamp(event_time / 1000000),'MI')
--              timestamp_trunc(to_timestamp(event_time / 1000000),'HH')
--      cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date)
    ) as a,
    (select
         user_id,
--       cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as ghst_tim,
--                  timestamp_trunc(to_timestamp(event_time / 1000000),'HH') as ghst_tim,
                    timestamp_trunc(to_timestamp(event_time / 1000000),'MI') as ghst_tim,
--                  timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as ghst_tim,
--      to_timestamp(event_time / 1000000) as ghst_tim,
         count(*)                                                                     as ghst_con,
         sum((t1.total_revenue * 1000000) / rates.exchange_rate)                      as ghst_rev,
         0                                                                            as ghst_imp,
         0                                                                            as ghst_cst
--              cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as conversiontime,

    from
        diap01.mec_us_united_20056.dfa2_activity as t1

        left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
        on upper(substring(other_data,(instr(other_data,'u3=') + 3),3)) = upper(rates.currency)
            and cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) = rates.date

             where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-12-21' and '2016-12-21'
--  where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-12-21' and '2016-12-21'
        and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
        and total_revenue <> 0
        and activity_id = 978826
        and (advertiser_id <> 0)
        and user_id <> '0'
        and (site_id_dcm = 2202011)
--      and user_id = 'AMsySZYXoqtlHSrjraDIhQeAZGVT'

    group by
        user_id,
        to_timestamp(event_time / 1000000)
--                  timestamp_trunc(to_timestamp(event_time / 1000000),'SS')
--              timestamp_trunc(to_timestamp(event_time / 1000000),'MI')
--              timestamp_trunc(to_timestamp(event_time / 1000000),'HH')
--      cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date)
    ) as b
where
    a.user_id = b.user_id
    and
        abs(datediff(mi, real_tim, ghst_tim)) <= 1
--      and
--      real_tim = ghst_tim
--              and
--              conversiontime > impressiontime
group by
    a.user_id,
    real_tim,
    ghst_tim



-- ====================================================================================================================
-- ====================================================================================================================