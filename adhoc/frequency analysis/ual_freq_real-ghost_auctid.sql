       select
--          a.user_id,
a.site_id_dcm,
           cast(timestamp_trunc(to_timestamp(conversiontime / 1000000),'SS') as date) as conversiontime,
            sum(revenue) as rev,
           sum(b.imp) as imp
--          cost,
--          impressiontime,
--          cvr_nbr,
--          imp_nbr
        from
            (select
                 user_id,
                dbm_auction_id,
                site_id_dcm,
                 (t1.total_revenue * 1000000) / rates.exchange_rate as revenue,
--              cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as conversiontime,
--               timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as conversiontime,
                 event_time as conversiontime,
                 row_number() over() as cvr_nbr
             from
                 diap01.mec_us_united_20056.dfa2_activity as t1

                 left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
                     on upper(substring(other_data,(instr(other_data,'u3=') + 3),3)) = upper(rates.currency)
                     and cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) = rates.date

             where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
                 and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
                 and total_revenue <> 0
                 and total_conversions <> 0
                 and activity_id = 978826
                 and (advertiser_id <> 0)
                 and user_id <> '0'
                 and (campaign_id = 9639387 or campaign_id = 8958859)
                 and (site_id_dcm = 1578478 or site_id_dcm = 2202011)

            ) as a
            left join
            (select t1.dbm_auction_id, count(distinct t1.imp) as imp
                from (select
                *, row_number() over() as imp from diap01.mec_us_united_20056.dfa2_impression
     where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
and (advertiser_id <> 0)
                 and user_id <> '0'
                 and (campaign_id = 9639387 or campaign_id = 8958859)
                 and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
                ) as t1
group by t1.dbm_auction_id
                ) as b
on a.dbm_auction_id =  b.dbm_auction_id
--      where
--          a.user_id = b.user_id and
--              conversiontime > impressiontime

       group by
           a.site_id_dcm,
           cast(timestamp_trunc(to_timestamp(conversiontime / 1000000),'SS') as date)