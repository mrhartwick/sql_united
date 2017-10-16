
create table diap01.mec_us_united_20056.ual_freq_alt_tbl1
(
    user_id        varchar(50)    not null,
    conversiontime int            not null,
    impressiontime int            not null,
    imp_key varchar not null,
    revenue        decimal not null,
    cost           decimal not null,
    cvr_nbr        int            not null,
    imp_nbr        int            not null
);

insert into diap01.mec_us_united_20056.ual_freq_alt_tbl1
(user_id, conversiontime, impressiontime, imp_key, revenue, cost, cvr_nbr, imp_nbr)

    (
        /*
        At the user_id level, find conversions as well as impressions that occur before those conversions
         */
        select
            a.user_id,
            conversiontime,
            impressiontime,
            md5(a.user_id || cast(impressiontime as varchar)) as imp_key,
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
                event_time                                               as impressiontime,
                row_number() over ()                                     as imp_nbr,
                cast((dbm_total_media_cost_usd / 1000000000) as decimal) as cost
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

            ) as b
        where
            a.user_id = b.user_id and
--              a.dbm_auction_id = b.dbm_auction_id
                conversiontime > impressiontime
);
commit;

select count(*) from diap01.mec_us_united_20056.ual_freq_alt_tbl1
select count(*) from diap01.mec_us_united_20056.ual_freq_tbl1

-- ====================================================================================================================

create table diap01.mec_us_united_20056.ual_freq_alt_tbl2
(
    user_id    varchar(50)    not null,
    cvr_nbr    int            not null,
    cvr_credit decimal not null

);

insert into diap01.mec_us_united_20056.ual_freq_alt_tbl2
(user_id, cvr_nbr, cvr_credit)

    (select
                user_id,
                cvr_nbr,
                1/count(distinct imp_nbr) as cvr_credit
from
                diap01.mec_us_united_20056.ual_freq_alt_tbl1
group by
 user_id,
cvr_nbr);

commit;

-- ====================================================================================================================

create table diap01.mec_us_united_20056.ual_freq_alt_tbl3
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

insert into diap01.mec_us_united_20056.ual_freq_alt_tbl3
(user_id, conversiontime, impressiontime, revenue, cost, cvr_nbr, imp_nbr, yr, wk, cvr_credit)

    (select
                a.user_id,
                a.conversiontime,
                a.impressiontime,
                a.revenue,
                a.cost,
                a.cvr_nbr,
                a.imp_nbr,
                date_part('year', cast(timestamp_trunc(to_timestamp(a.impressiontime / 1000000),'SS') as date)) as yr,
                date_part('week', cast(timestamp_trunc(to_timestamp(a.impressiontime / 1000000),'SS') as date)) as wk,
                b.cvr_credit
from
                diap01.mec_us_united_20056.ual_freq_alt_tbl1 a,
                diap01.mec_us_united_20056.ual_freq_alt_tbl2 b
where
                a.user_id = b.user_id and
                a.cvr_nbr = b.cvr_nbr
    );
commit;

-- ====================================================================================================================
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
                diap01.mec_us_united_20056.ual_freq_alt_tbl3
group by
                yr,
                wk;
-- ====================================================================================================================

create table diap01.mec_us_united_20056.ual_freq_alt_tbl4
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

-- drop table diap01.mec_us_united_20056.ual_freq_alt_tbl4

insert into diap01.mec_us_united_20056.ual_freq_alt_tbl4
(user_id,yr,wk,cvr_cnt,imp_cnt,cvr_credit,revenue,cost)

    (select
        user_id,
        yr,
        wk,
--      count(distinct user_id)   as user_cnt,
        count(distinct cvr_nbr)
--          *sum(cvr_credit)
            as cvr_cnt,
        count(distinct imp_nbr)   as imp_cnt,
        sum(cvr_credit)           as cvr_credit,
        sum(revenue * cvr_credit) as revenue,
        sum(cost)                 as cost
    from
        diap01.mec_us_united_20056.ual_freq_alt_tbl3
    group by
        user_id,
        yr,
        wk

    );
commit;

-- ====================================================================================================================

-- Get Impressions from users not in converter table

create table diap01.mec_us_united_20056.ual_freq_tbl_imp2
(
    user_id    varchar(50) not null,
    event_time int         not null,
    yr int not null,
    wk int not null,
    imp_key    varchar     not null,
    cost       decimal     not null,
    imp        int         not null

);

insert into diap01.mec_us_united_20056.ual_freq_tbl_imp2
(user_id,event_time,yr,wk,imp_key,cost,imp)

    (select
        t4.user_id,
        t4.event_time,
        date_part('year', cast(timestamp_trunc(to_timestamp(t4.event_time / 1000000),'SS') as date)) as yr,
        date_part('week', cast(timestamp_trunc(to_timestamp(t4.event_time / 1000000),'SS') as date)) as wk,
        t4.imp_key,
        t4.cost,
        t4.imp
        from
    (select
        t3.user_id,
        t3.event_time,
        md5(t3.user_id || cast(t3.event_time as varchar)) as imp_key,
        sum(t3.cost) as cost,
        count(*)     as imp
    from (select
        *,
        case when ((cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-08-31') and (t2.site_id_dcm = 2202011)) then 1
        when ((cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-09-01' and '2016-12-31') and (t2.site_id_dcm = 1578478)) then 1 else 2 end as rank,
        cast((dbm_total_media_cost_usd / 1000000000) as decimal)                                                                                                               as cost
    from
        diap01.mec_us_united_20056.dfa2_impression as t2

        left join
        (select
            p1.placement,p1.placement_id,p1.campaign_id,p1.site_id_dcm

        from (select
            campaign_id,site_id_dcm,placement_id,placement,cast(placement_start_date as date)                     as thisdate,
                                                           row_number() over ( partition by campaign_id,site_id_dcm,placement_id
                                                               order by cast(placement_start_date as date) desc ) as x1
        from diap01.mec_us_united_20056.dfa2_placements
             ) as p1
        where x1 = 1
        ) as p2
        on t2.placement_id = p2.placement_id

    where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
        and (t2.campaign_id = 9639387 or t2.campaign_id = 8958859)
        and (t2.site_id_dcm = 1578478 or t2.site_id_dcm = 2202011)
--      and user_id not in (select distinct user_id from diap01.mec_us_united_20056.ual_freq_tbl1)
        and (advertiser_id <> 0)
        and user_id <> '0'
--      Big Blue - Google prospecting
        and (not regexp_like(left(p2.placement,6),'P9P.*','ib')
        and not regexp_like(left(p2.placement,6),'PBN.*','ib'))
         ) as t3
    where t3.rank = 1
    group by t3.user_id,t3.event_time
    )   as t4
        where t4.imp_key not in (select distinct imp_key from diap01.mec_us_united_20056.ual_freq_alt_tbl1)
    );
commit;
-- ====================================================================================================================
-- ====================================================================================================================
create table diap01.mec_us_united_20056.ual_freq_tbl_imp3
(
    user_id    varchar(50) not null,
    yr             int            not null,
    wk             int            not null,
--  cvr_cnt    int         not null,
    imp_cnt    int         not null,
--  cvr_credit decimal     not null,
--  revenue    decimal     not null,
    cost       decimal     not null
);

insert into diap01.mec_us_united_20056.ual_freq_tbl_imp3
(user_id,
yr,
wk,
-- cvr_cnt,
imp_cnt,
-- cvr_credit,
-- revenue,
cost)

    (select
        user_id,
        yr,
        wk,
--      0         as cvr_cnt,
        sum(imp)  as imp_cnt,
--      0         as cvr_credit,
--      0         as revenue,
        sum(cost) as cost
    from
        diap01.mec_us_united_20056.ual_freq_tbl_imp2
    group by
        user_id,
        yr,
        wk

    );
commit;

-- ====================================================================================================================
-- ====================================================================================================================
create table diap01.mec_us_united_20056.ual_freq_tbl5
(
    user_id    varchar(50) not null,
    yr             int            not null,
    wk             int            not null,
    cvr_cnt    int         not null,
    imp_cnt    int         not null,
    cvr_credit decimal     not null,
    revenue    decimal     not null,
    cost       decimal     not null
);

insert into diap01.mec_us_united_20056.ual_freq_tbl5
(user_id,yr,wk,cvr_cnt,imp_cnt,cvr_credit,revenue,cost)

    (
        select
        t2.user_id,
        t2.yr,
        t2.wk,
        sum(t2.cvr_cnt) as cvr_cnt,
        sum(t2.imp_cnt)   as imp_cnt,
        sum(t2.cvr_credit)           as cvr_credit,
        sum(t2.revenue) as revenue,
        sum(t2.cost)                 as cost


        from (select
        t0.user_id,
        t0.yr,
        t0.wk,
        sum(t0.cvr_cnt)   as cvr_cnt,
        sum(t0.imp_cnt)   as imp_cnt,
        sum(t0.cvr_credit)           as cvr_credit,
        sum(t0.revenue) as revenue,
        sum(t0.cost)                 as cost
    from
        diap01.mec_us_united_20056.ual_freq_alt_tbl4 as t0
    group by
        t0.user_id,
        t0.yr,
        t0.wk

            union all

        select
        t1.user_id,
        t1.yr,
        t1.wk,
        0   as cvr_cnt,
        sum(t1.imp_cnt)   as imp_cnt,
        0           as cvr_credit,
        0 as revenue,
        sum(t1.cost)                 as cost
    from
        diap01.mec_us_united_20056.ual_freq_tbl_imp3 as t1
    group by
        t1.user_id,
        t1.yr,
        t1.wk

             ) as t2
group by
            t2.user_id,
        t2.yr,
        t2.wk
    );
commit;

-- ====================================================================================================================
-- ====================================================================================================================
select
    imp_cnt,
    wk,
    sum(imp_cnt) as imps,
    count( user_id) as user_cnt,
    sum(cvr_cnt) as con,
    sum(revenue) as rev,
    sum(cost) as cost
    from diap01.mec_us_united_20056.ual_freq_tbl5

group by
        imp_cnt,
    wk