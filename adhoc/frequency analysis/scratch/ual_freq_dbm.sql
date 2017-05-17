
-- Choose between real and ghost placements throughout year

create table diap01.mec_us_united_20056.ual_freq_tbl0
(
    user_id        varchar(50)    not null,
    event_time     int            not null,
    dbm_auction_id varchar(255)   not null,
    cost           decimal not null

);

insert into diap01.mec_us_united_20056.ual_freq_tbl0
(user_id,event_time,dbm_auction_id,cost)

    (select
        t3.user_id,
        t3.event_time,
        t3.dbm_auction_id,
        t3.cost
    from (select
        *,
        case when ((cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2016-07-15' and '2016-08-31') and (t2.site_id_dcm = 2202011)) then 1
        when ((cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-09-01' and '2016-12-31') and (t2.site_id_dcm = 1578478)) then 1 else 2 end as rank,
        cast((dbm_total_media_cost_usd / 1000000000) as decimal) as cost
    from
        diap01.mec_us_united_20056.dfa2_impression as t2

--      left join diap01.mec_us_united_20056.dfa2_placements as p2
--      on t2.placement_id = p2.placement_id

        left join
        (
        select p1.placement,p1.placement_id ,p1.campaign_id,p1.site_id_dcm

        from ( select campaign_id ,site_id_dcm, placement_id ,placement,cast (placement_start_date as date ) as thisdate,
        row_number() over (partition by campaign_id, site_id_dcm, placement_id order by cast (placement_start_date as date ) desc ) as x1
        from diap01.mec_us_united_20056.dfa2_placements
            ) as p1
        where x1 = 1
        ) as p2
        on t2.placement_id = p2.placement_id

    where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
        and (t2.campaign_id = 9639387 or t2.campaign_id = 8958859)
        and (t2.site_id_dcm = 1578478 or t2.site_id_dcm = 2202011)
        and user_id <> '0'
        and (advertiser_id <> 0)
--      Big Blue - Google prospecting
        and (left(p2.placement,6) not like 'P9P%'
        and left(p2.placement,6) not like 'PBN%')
         ) as t3
    where t3.rank = 1
    );
commit;


select *  from diap01.mec_us_united_20056.ual_freq_tbl0
    where user_id = 'AMsySZY--_8BpbRhnFqY6S_1_pr8'



create table diap01.mec_us_united_20056.ual_freq_tbl1
(
    user_id        varchar(50)    not null,
    conversiontime int            not null,
    impressiontime int            not null,
    revenue        decimal not null,
    cost           decimal not null,
    cvr_nbr        int            not null,
    imp_nbr        int            not null
);

insert into diap01.mec_us_united_20056.ual_freq_tbl1
(user_id, conversiontime, impressiontime, revenue, cost, cvr_nbr, imp_nbr)

    (
        /*
        At the user_id level, find conversions as well as impressions that occur before those conversions
         */
        select
            a.user_id,
            conversiontime,
            impressiontime,
            revenue,
            cost,
            cvr_nbr,
            imp_nbr
        from
            (select
                 user_id,
--              dbm_auction_id,
                 (t1.total_revenue * 1000000) / rates.exchange_rate as revenue,
                 event_time as conversiontime,
                 row_number() over() as cvr_nbr
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

            ) as a,
            (select
                 user_id,
--              dbm_auction_id,
                event_time as impressiontime,
                 cost,
                 row_number() over() as imp_nbr
             from
                 diap01.mec_us_united_20056.ual_freq_tbl0

            ) as b
        where
            a.user_id = b.user_id and
--              a.dbm_auction_id = b.dbm_auction_id
                conversiontime > impressiontime
);
commit;



select *  from diap01.mec_us_united_20056.ual_freq_tbl1
    where user_id = 'AMsySZY--_8BpbRhnFqY6S_1_pr8'


-- ====================================================================================================================

create table diap01.mec_us_united_20056.ual_freq_tbl2
(
    user_id    varchar(50)    not null,
    cvr_nbr    int            not null,
    cvr_credit decimal not null

);

insert into diap01.mec_us_united_20056.ual_freq_tbl2
(user_id, cvr_nbr, cvr_credit)

    (select
                user_id,
                cvr_nbr,
                1/count(distinct imp_nbr) as cvr_credit
from
                diap01.mec_us_united_20056.ual_freq_tbl1
group by
 user_id,
cvr_nbr);
commit;

-- ====================================================================================================================

create table diap01.mec_us_united_20056.ual_freq_tbl3
(
    user_id        varchar(50)    not null,
    conversiontime int            not null,
    impressiontime int            not null,
    revenue        decimal not null,
    cost           decimal not null,
    cvr_nbr        int            not null,
    imp_nbr        int            not null,
    yr             int            not null,
    wk             int            not null,
    cvr_credit     decimal not null

);

insert into diap01.mec_us_united_20056.ual_freq_tbl3
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
                diap01.mec_us_united_20056.ual_freq_tbl1 a,
                diap01.mec_us_united_20056.ual_freq_tbl2 b
where
                a.user_id = b.user_id and
                a.cvr_nbr = b.cvr_nbr
    );
commit;


select *  from diap01.mec_us_united_20056.ual_freq_tbl3
    where user_id = 'AMsySZY--_8BpbRhnFqY6S_1_pr8'



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
                diap01.mec_us_united_20056.ual_freq_tbl3
group by
                yr,
                wk;


create table diap01.mec_us_united_20056.ual_freq_tbl4
(
    user_id    varchar(50) not null,
    yr             int            not null,
    wk             int            not null,
--  user_cnt   int         not null,
    cvr_cnt    int         not null,
    imp_cnt    int         not null,
    cvr_credit decimal     not null,
    revenue    decimal     not null,
    cost       decimal     not null
);

insert into diap01.mec_us_united_20056.ual_freq_tbl4
(user_id,yr,wk,cvr_cnt,imp_cnt,cvr_credit,revenue,cost)

    (select
        user_id,
        yr,
        wk,
--      count(distinct user_id)   as user_cnt,
        count(distinct cvr_nbr)*sum(cvr_credit)   as cvr_cnt,
        count(distinct imp_nbr)   as imp_cnt,
        sum(cvr_credit)           as cvr_credit,
        sum(revenue * cvr_credit) as revenue,
        sum(cost)                 as cost
    from
        diap01.mec_us_united_20056.ual_freq_tbl3
    group by
        user_id,
        yr,
        wk

    );
commit;

select
    imp_cnt,
    wk,
    sum(imp_cnt) as imps,
    count( user_id) as user_cnt,
    sum(cvr_cnt) as con,
    sum(revenue) as rev,
    sum(cost) as cost
    from diap01.mec_us_united_20056.ual_freq_tbl4

group by
        imp_cnt,
    wk

