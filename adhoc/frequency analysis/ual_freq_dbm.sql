create table diap01.mec_us_united_20056.ual_freq_dbm_tbl1
(
    user_id        varchar(50)    not null,
    conversiontime int            not null,
    impressiontime int            not null,
    revenue        decimal(20,10) not null,
    cost           decimal(20,10) not null,
    cvr_nbr        int            not null,
    imp_nbr        int            not null
);

insert into diap01.mec_us_united_20056.ual_freq_dbm_tbl1
(user_id, conversiontime, revenue, cost, impressiontime, cvr_nbr, imp_nbr)

    (
        select
            a.user_id,
            conversiontime,
            revenue,
            cost,
            impressiontime,
            cvr_nbr,
            imp_nbr
        from
            (select
                 user_id,
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
--               and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
                 and (site_id_dcm = 1578478 or site_id_dcm = 2202011)

            ) as a,
            (select
                 user_id,
--               timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as impressiontime,
                event_time as impressiontime,
--              cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) as impressiontime,
                 cast((dbm_media_cost_usd / 1000000000) as decimal (20,10)) as cost,
                 row_number() over() as imp_nbr
             from
                 diap01.mec_us_united_20056.dfa2_impression

             where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
                 and (campaign_id = 9639387 or campaign_id = 8958859)
--               and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
                 and (site_id_dcm = 1578478 or site_id_dcm = 2202011)
                 and user_id <> '0'
                 and (advertiser_id <> 0)
            ) as b
        where
            a.user_id = b.user_id and
                conversiontime > impressiontime
);
commit;
-- ====================================================================================================================
create table diap01.mec_us_united_20056.ual_freq_dbm_tbl2
(
    user_id    varchar(50)    not null,
    cvr_nbr    int            not null,
    cvr_credit decimal(20,10) not null

);

insert into diap01.mec_us_united_20056.ual_freq_dbm_tbl2
(user_id, cvr_nbr, cvr_credit)

    (select
                user_id,
                cvr_nbr,
                1/count(distinct imp_nbr) as cvr_credit
from
                diap01.mec_us_united_20056.ual_freq_dbm_tbl1
group by
1,2);
commit;

-- ====================================================================================================================

create table diap01.mec_us_united_20056.ual_freq_dbm_tbl3
(
    user_id        varchar(50)    not null,
    conversiontime int            not null,
    impressiontime int            not null,
    revenue        decimal(20,10) not null,
    cost           decimal(20,10) not null,
    cvr_nbr        int            not null,
    imp_nbr        int            not null,
    yr             int            not null,
    wk             int            not null,
    cvr_credit     decimal(20,10) not null

);

insert into diap01.mec_us_united_20056.ual_freq_dbm_tbl3
(user_id, conversiontime, impressiontime, revenue, cost, cvr_nbr, imp_nbr, yr, wk, cvr_credit)

    (select
                a.user_id,
                a.conversiontime,
                a.impressiontime,
                a.revenue,
                a.cost,
                a.cvr_nbr,
                a.imp_nbr,
                date_part('year', cast(timestamp_trunc(to_timestamp(a.impressiontime / 1000000),'SS') as date)) as wk,
                date_part('week', cast(timestamp_trunc(to_timestamp(a.impressiontime / 1000000),'SS') as date)) as wk,
                b.cvr_credit
from
                diap01.mec_us_united_20056.ual_freq_dbm_tbl1 a,
                diap01.mec_us_united_20056.ual_freq_dbm_tbl2 b
where
                a.user_id = b.user_id and
                a.cvr_nbr = b.cvr_nbr
    );
commit;




select
                yr,
                wk,
                count(distinct user_id) as user_cnt,
                count(distinct cvr_nbr) as cvr_cnt,
                count(distinct imp_nbr) as imp_cnt,
                sum(cvr_credit) as cvr_credit,
                sum(revenue*cvr_credit) as revenue,
                sum(cost) as cost
from
                diap01.mec_us_united_20056.ual_freq_dbm_tbl3
group by
                1,2;
