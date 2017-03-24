(
select





    from
        (select
            t1.user_id,
            t1.placement_id,
            t1.campaign_id,
            t1.placement_id,
            p1.plce_id,
            t1.site_id_dcm,
            case when d1.site_dcm like 'Google%' then 1 else 2 end as site_rank,
            t1.conversiontime,
            t1.revenue,
            t1.cost,
            t1.impressiontime,
            t1.cvr_nbr,
            t1.imp_nbr


        from
            (select
                a.user_id,
                a.campaign_id,
                a.placement_id,
--              left(p1.placement,6) as plce_id,
                a.site_id_dcm,
                conversiontime,
                revenue,
                cost,
                impressiontime,
                cvr_nbr,
                imp_nbr
            from
                (select
                    user_id,
                    site_id_dcm,
                    campaign_id,
                    placement_id,
                    (t1.total_revenue * 1000000) / rates.exchange_rate       as revenue,
--              cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as conversiontime,
                    timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as conversiontime,
--               interaction_time,
                    row_number() over ()                                     as cvr_nbr
                from
                    diap01.mec_us_united_20056.dfa2_activity as t1

                    left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
                    on upper(substring(other_data,(instr(other_data,'u3=') + 3),3)) = upper(rates.currency)
                        and cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) = rates.date

                where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-07-15'
                    and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
                    and total_revenue <> 0
                    and total_conversions <> 0
                    and activity_id = 978826
                    and (advertiser_id <> 0)
                    and user_id <> '0'
                    and (campaign_id = 9639387 or campaign_id = 8958859)
                    and (site_id_dcm = 1578478 or site_id_dcm = 2202011)

                ) as a,
                (select
                    user_id,
                    site_id_dcm,
                    campaign_id,
                    placement_id,
                    timestamp_trunc(to_timestamp(event_time / 1000000),'SS')  as impressiontime,
--              cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as impressiontime,
                    cast((dbm_media_cost_usd / 1000000000) as decimal(20,10)) as cost,
                    row_number() over ()                                      as imp_nbr
                from
                    diap01.mec_us_united_20056.dfa2_impression

                where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-07-15'
                    and (campaign_id = 9639387 or campaign_id = 8958859)
                    and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
                    and user_id <> '0'
                    and (advertiser_id <> 0)
                ) as b
            where
                a.user_id = b.user_id and
                    conversiontime > impressiontime
--                  and a.site_id_dcm = b.site_id_dcm
--                  and a.campaign_id = b.campaign_id
                    and a.placement_id = b.placement_id

            ) as t1

            left join
            (
                select
                    t0.plce_id,
                    t0.placement_id,
                    t0.campaign_id,
                    t0.site_id_dcm

                from (select
                    campaign_id                                           as campaign_id,
                    site_id_dcm                                           as site_id_dcm,
                    placement_id                                          as placement_id,
                    left(placement,6)                                             as plce_id,
                    cast(placement_start_date as date)                    as thisdate,
                    row_number() over (partition by campaign_id,site_id_dcm,placement_id
                        order by cast(placement_start_date as date) desc) as r1
                from diap01.mec_us_united_20056.dfa2_placements
                    where (campaign_id = 9639387 or campaign_id = 8958859)
                    and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
                     ) as t0
                where r1 = 1
            ) as p1
            on t1.placement_id = p1.placement_id
                and t1.campaign_id = p1.campaign_id
                and t1.site_id_dcm = p1.site_id_dcm
            left join
            (
                select
                    site_dcm,
                    site_id_dcm
                from diap01.mec_us_united_20056.dfa2_sites
                where (site_id_dcm = 1578478 or site_id_dcm = 2202011)
            ) as d1
            on t1.site_id_dcm = d1.site_id_dcm
        ) as t2
    );