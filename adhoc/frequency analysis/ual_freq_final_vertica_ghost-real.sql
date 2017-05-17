
/* Sequence that chooses between real and ghost advertiser for DBM
Bid strategy breakout
Excludes 8/5 -> 9/25


 */

create table diap01.mec_us_united_20056.ual_freq_bid_3_imp1
(
    user_id    varchar(50) not null,
    event_time int         not null,
    bid_group    varchar(50) not null,
    cost       decimal     not null

);

insert into diap01.mec_us_united_20056.ual_freq_bid_3_imp1
(user_id,event_time,bid_group,cost)

    (select
        t3.user_id,
        t3.event_time,
        t3.bid_group,
        t3.cost
    from (select
        *,
        case
        when (p2.placement like '%Strategy1%') then '1'
        when (p2.placement like '%Strategy2%') then '2'
        when (p2.placement like '%Strategy3%') then '3'
        when (p2.placement like '%First%') then '0'
        else 'XXX'
        end                                                                                as bid_group,
        case when ((cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-08-31') and (t2.site_id_dcm = 2202011)) then 1
        when ((cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-09-01' and '2016-12-31') and (t2.site_id_dcm = 1578478)) then 1 else 2 end as rank,
        cast((dbm_total_media_cost_usd / 1000000000) as decimal) as cost
    from
        diap01.mec_us_united_20056.dfa2_impression as t2

        left join
        (select p1.placement,p1.placement_id ,p1.campaign_id,p1.site_id_dcm

        from ( select campaign_id ,site_id_dcm, placement_id ,placement,cast (placement_start_date as date ) as thisdate,
        row_number() over (partition by campaign_id, site_id_dcm, placement_id order by cast (placement_start_date as date ) desc ) as x1
        from diap01.mec_us_united_20056.dfa2_placements
            ) as p1
        where x1 = 1
        ) as p2
        on t2.placement_id = p2.placement_id

    where (cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-08-04' or
            cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-09-26' and '2016-12-30')
        and (t2.campaign_id = 9639387 or t2.campaign_id = 8958859)
        and (t2.site_id_dcm = 1578478 or t2.site_id_dcm = 2202011)
        and user_id <> '0'
        and (advertiser_id <> 0)
--      Big Blue - Google prospecting
                and (left(p2.placement,6) not like 'P9P%'
        and left(p2.placement,6) not like 'PBN%')
--      and (not regexp_like(left(p2.placement,6),'P9P.*','ib')
--      and not regexp_like(left(p2.placement,6),'PBN.*','ib'))

         ) as t3
    where t3.rank = 1
        and t3.bid_group != 'XXX'
    );
commit;

-- ====================================================================================================================
-- ====================================================================================================================

-- Get all attributed converters
-- drop table diap01.mec_us_united_20056.ual_freq_tbl1
create table diap01.mec_us_united_20056.ual_freq_bid_3_tbl1
(
    user_id        varchar(50) not null,
    bid_group        varchar(50) not null,
    conversiontime int         not null,
    impressiontime int         not null,
    imp_key varchar not null,
    rev        decimal     not null,
    cost           decimal     not null,
    cvr_nbr        int         not null,
    imp_nbr        int         not null
);

insert into diap01.mec_us_united_20056.ual_freq_bid_3_tbl1
(user_id, bid_group, conversiontime, impressiontime,imp_key,rev, cost, cvr_nbr, imp_nbr)

    (
        /*
        At the user_id level, find conversions as well as impressions that occur before those conversions
         */
        select
            a.user_id,
            b.bid_group,
            conversiontime,
            impressiontime,
            md5(a.user_id || cast(impressiontime as varchar)) as imp_key,
            rev,
            cost,
            cvr_nbr,
            imp_nbr
        from
            (select
                 user_id,
--              dbm_auction_id,
        case
        when (p2.placement like '%Strategy1%') then '1'
        when (p2.placement like '%Strategy2%') then '2'
        when (p2.placement like '%Strategy3%') then '3'
        when (p2.placement like '%First%') then '0'
        else 'XXX'
        end                                                                                as bid_group,
                 (t1.total_revenue * 1000000) / rates.exchange_rate as rev,
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

                where (cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-08-04' or cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-09-26' and '2016-12-30')
                 and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
                 and total_revenue <> 0
                 and total_conversions <> 0
                 and activity_id = 978826
                 and (advertiser_id <> 0)
                 and user_id <> '0'
                 and (t1.campaign_id = 9639387 or t1.campaign_id = 8958859)
                 and (t1.site_id_dcm = 1578478 or t1.site_id_dcm = 2202011)
--               Big Blue
                 and (left(p2.placement,6) not like 'P9P%'
                 and left(p2.placement,6) not like 'PBN%')
--               and (not regexp_like(left(p2.placement,6),'P9P.*','ib')
--               and not regexp_like(left(p2.placement,6),'PBN.*','ib'))

            ) as a,
            (select
                 user_id,
--              dbm_auction_id,
                bid_group,
                event_time as impressiontime,
                 cost,
                 row_number() over() as imp_nbr
             from
                 diap01.mec_us_united_20056.ual_freq_bid_3_imp1

            ) as b
        where
            a.user_id = b.user_id and
--          a.bid_group = b.bid_group and
            conversiontime > impressiontime
        and  a.bid_group != 'XXX'

);
commit;
-- ====================================================================================================================

create table diap01.mec_us_united_20056.ual_freq_bid_3_tbl2
(
    user_id    varchar(50) not null,
    cvr_nbr    int         not null,
    cvr_credit decimal     not null

);

insert into diap01.mec_us_united_20056.ual_freq_bid_3_tbl2
(user_id,cvr_nbr,cvr_credit)

    (select
        user_id,
        cvr_nbr,
        1 / count(distinct imp_nbr) as cvr_credit
    from
        diap01.mec_us_united_20056.ual_freq_bid_3_tbl1
    group by
        user_id,
        cvr_nbr);
commit;

-- ====================================================================================================================

create table diap01.mec_us_united_20056.ual_freq_bid_3_tbl3
(
    user_id        varchar(50) not null,
    bid_group         varchar(50) not null,
    conversiontime int         not null,
    impressiontime int         not null,
    rev        decimal     not null,
    cost           decimal     not null,
    cvr_nbr        int         not null,
    imp_nbr        int         not null,
    yr             int         not null,
    wk             int         not null,
    cvr_credit     decimal     not null

);

insert into diap01.mec_us_united_20056.ual_freq_bid_3_tbl3
(user_id,bid_group,conversiontime,impressiontime,rev,cost,cvr_nbr,imp_nbr,yr,wk,cvr_credit)

    (select
        a.user_id,
        a.bid_group,
        a.conversiontime,
        a.impressiontime,
        a.rev,
        a.cost,
        a.cvr_nbr,
        a.imp_nbr,
        date_part('year',cast(timestamp_trunc(to_timestamp(a.impressiontime / 1000000),'SS') as date)) as yr,
        date_part('week',cast(timestamp_trunc(to_timestamp(a.impressiontime / 1000000),'SS') as date)) as wk,
        b.cvr_credit
    from
        diap01.mec_us_united_20056.ual_freq_bid_3_tbl1 a,
        diap01.mec_us_united_20056.ual_freq_bid_3_tbl2 b
    where
        a.user_id = b.user_id and
            a.cvr_nbr = b.cvr_nbr
    );
commit;

-- select distinct bid_group from diap01.mec_us_united_20056.ual_freq_bid_3_tbl3

-- ====================================================================================================================
-- select
--                 yr,
--                 wk,
--                 count(distinct user_id) as user_cnt,
--                 count(distinct cvr_nbr) as cvr,
--                 count(distinct imp_nbr) as imp,
--                 sum(cvr_credit) as cvr_credit,
--                 sum(rev*cvr_credit) as rev,
--                 sum(cost) as cost
-- from
--                 diap01.mec_us_united_20056.ual_freq_bid_3_tbl3
-- group by
--                 yr,
--                 wk;
-- ====================================================================================================================
create table diap01.mec_us_united_20056.ual_freq_bid_3_tbl4
(
    user_id    varchar(50) not null,
    yr         int         not null,
    wk         int         not null,
    cvr        int         not null,
    cvr_1   int         not null,
    cvr_2   int         not null,
    cvr_3   int         not null,
    cvr_0   int         not null,
    imp        int         not null,
    imp_1   int         not null,
    imp_2   int         not null,
    imp_3   int         not null,
    imp_0   int         not null,
    cvr_credit decimal     not null,
    rev        decimal     not null,
    rev_1   decimal,
    rev_2   decimal,
    rev_3   decimal,
    rev_0   decimal,
    cost       decimal,
    cost_1  decimal,
    cost_2  decimal,
    cost_3  decimal,
    cost_0  decimal

);

-- drop table diap01.mec_us_united_20056.ual_freq_bid_3_tbl4
insert into diap01.mec_us_united_20056.ual_freq_bid_3_tbl4
(
user_id,
yr,
wk,
cvr,
cvr_1,
cvr_2,
cvr_3,
cvr_0,
imp,
imp_1,
imp_2,
imp_3,
imp_0,
cvr_credit,
rev,
rev_1,
rev_2,
rev_3,
rev_0,
cost,
cost_1,
cost_2,
cost_3,
cost_0


)

    (select
        user_id,
        -- bid_group,
        yr,
        wk,
        count(distinct cvr_nbr)   as cvr,
        count(distinct case when bid_group = '1' then cvr_nbr end) as cvr_1,
        count(distinct case when bid_group = '2' then cvr_nbr end) as cvr_2,
        count(distinct case when bid_group = '3' then cvr_nbr end) as cvr_3,
        count(distinct case when bid_group = '0' then cvr_nbr end) as cvr_0,


        count(distinct imp_nbr)                                      as imp,
        count(distinct case when bid_group = '1' then imp_nbr end)   as imp_1,
        count(distinct case when bid_group = '2' then imp_nbr end)   as imp_2,
        count(distinct case when bid_group = '3' then imp_nbr end)   as imp_3,
        count(distinct case when bid_group = '0' then imp_nbr end)   as imp_0,

        sum(cvr_credit)           as cvr_credit,

        sum(rev * cvr_credit)                                        as rev,
        sum(case when bid_group = '1' then rev * cvr_credit end)     as rev_1,
        sum(case when bid_group = '2' then rev * cvr_credit end)     as rev_2,
        sum(case when bid_group = '3' then rev * cvr_credit end)     as rev_3,
        sum(case when bid_group = '0' then rev * cvr_credit end)     as rev_0,

        sum(cost)                                    as cost,
        sum(case when bid_group = '1' then cost end) as cost_1,
        sum(case when bid_group = '2' then cost end) as cost_2,
        sum(case when bid_group = '3' then cost end) as cost_3,
        sum(case when bid_group = '0' then cost end) as cost_0

    from
        diap01.mec_us_united_20056.ual_freq_bid_3_tbl3
    group by
        user_id,
        -- bid_group,
        yr,
        wk

--     limit 100
    );
commit;

-- ====================================================================================================================

-- Get Impressions from users not in converter table

create table diap01.mec_us_united_20056.ual_freq_bid_3_imp2
(
    user_id    varchar(50) not null,
--  bid_group  varchar(50) not null,
    event_time int         not null,
    yr         int         not null,
    wk         int         not null,
    imp_key    varchar     not null,
    cost       decimal     not null,
    cst_1   decimal,
    cst_2   decimal,
    cst_3   decimal,
    cst_0   decimal,
    imp        int         not null,
    imp_1   int,
    imp_2   int,
    imp_3   int,
    imp_0   int


);

insert into diap01.mec_us_united_20056.ual_freq_bid_3_imp2
(
user_id,
event_time,
yr,
wk,
imp_key,
cost,
cst_1,
cst_2,
cst_3,
cst_0,
imp,
imp_1,
imp_2,
imp_3,
imp_0
)

    (select
        t4.user_id,
--      t4.bid_group,
        t4.event_time,
        date_part('year',cast(timestamp_trunc(to_timestamp(t4.event_time / 1000000),'SS') as date)) as yr,
        date_part('week',cast(timestamp_trunc(to_timestamp(t4.event_time / 1000000),'SS') as date)) as wk,
        t4.imp_key,
        t4.cost,
        t4.cst_1,
        t4.cst_2,
        t4.cst_3,
        t4.cst_0,
        t4.imp,
        t4.imp_1,
        t4.imp_2,
        t4.imp_3,
        t4.imp_0

    from
        (select
            t3.user_id,
            t3.event_time,
--             t3.bid_group,
            md5(t3.user_id || cast(t3.event_time as varchar )) as imp_key,
            sum(t3.cost)                                       as cost,
            sum(case when bid_group = '1' then t3.cost end)  as cst_1,
            sum(case when bid_group = '2' then t3.cost end)  as cst_2,
            sum(case when bid_group = '3' then t3.cost end)  as cst_3,
            sum(case when bid_group = '0' then t3.cost end)  as cst_0,

            count(*) as imp,
            count(case when bid_group = '1' then 1 end) as imp_1,
            count(case when bid_group = '2' then 1 end) as imp_2,
            count(case when bid_group = '3' then 1 end) as imp_3,
            count(case when bid_group = '0' then 1 end) as imp_0

        from (select
            *,
        case
        when (p2.placement like '%Strategy1%') then '1'
        when (p2.placement like '%Strategy2%') then '2'
        when (p2.placement like '%Strategy3%') then '3'
        when (p2.placement like '%First%') then '0'
        else 'XXX'
        end                                                                                as bid_group,
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

            where (cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-08-04' or
            cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-09-26' and '2016-12-30')
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
            and t3.bid_group != 'XXX'
        group by t3.user_id,t3.event_time
--          ,t3.bid_group
        ) as t4
    where t4.imp_key not in (select distinct imp_key
    from diap01.mec_us_united_20056.ual_freq_bid_3_tbl1)
    );
commit;
-- ====================================================================================================================
-- ====================================================================================================================
-- Summary non-converter Impressions table

create table diap01.mec_us_united_20056.ual_freq_bid_3_imp3
(
    user_id varchar(50) not null,
    yr      int         not null,
    wk      int         not null,
    cost    decimal     not null,
    cst_1   decimal,
    cst_2   decimal,
    cst_3   decimal,
    cst_0   decimal,
    imp     int         not null,
    imp_1   int,
    imp_2   int,
    imp_3   int,
    imp_0   int
);

insert into diap01.mec_us_united_20056.ual_freq_bid_3_imp3
(
user_id,
yr,
wk,
cost,
cst_1,
cst_2,
cst_3,
cst_0,
imp,
imp_1,
imp_2,
imp_3,
imp_0
)

    (select
        user_id,
        yr,
        wk,
--      0         as cvr_cnt,
        sum(cost)    as cost,
        sum(cst_1)   as cst_1,
        sum(cst_2)   as cst_2,
        sum(cst_3)   as cst_3,
        sum(cst_0)   as cst_0,
        sum(imp)     as imp,
        sum(imp_1)   as imp_1,
        sum(imp_2)   as imp_2,
        sum(imp_3)   as imp_3,
        sum(imp_0)   as imp_0

    from
        diap01.mec_us_united_20056.ual_freq_bid_3_imp2
    group by
        user_id,
        yr,
        wk

    );
commit;

-- ====================================================================================================================
-- ====================================================================================================================
create table diap01.mec_us_united_20056.ual_freq_bid_3_tbl5
(
    user_id    varchar(50) not null,
    yr         int         not null,
    wk         int         not null,
    cvr        int         not null,
    cvr_1      int         not null,
    cvr_2      int         not null,
    cvr_3      int         not null,
    cvr_0      int         not null,
    imp        int         not null,
    imp_1      int         not null,
    imp_2      int         not null,
    imp_3      int         not null,
    imp_0      int         not null,
    cvr_credit decimal     not null,
    rev        decimal     not null,
    rev_1      decimal,
    rev_2      decimal,
    rev_3      decimal,
    rev_0      decimal,
    cost       decimal,
    cost_1     decimal,
    cost_2     decimal,
    cost_3     decimal,
    cost_0     decimal

);

insert into diap01.mec_us_united_20056.ual_freq_bid_3_tbl5
(
    user_id,
    yr,
    wk,
    cvr,
    cvr_1,
    cvr_2,
    cvr_3,
    cvr_0,
    imp,
    imp_1,
    imp_2,
    imp_3,
    imp_0,
    cvr_credit,
    rev,
    rev_1,
    rev_2,
    rev_3,
    rev_0,
    cost,
    cost_1,
    cost_2,
    cost_3,
    cost_0

)

    (
        select
            t2.user_id,
            t2.yr,
            t2.wk,
            sum(t2.cvr)        as cvr,
            sum(t2.cvr_1)      as cvr_1,
            sum(t2.cvr_2)      as cvr_2,
            sum(t2.cvr_3)      as cvr_3,
            sum(t2.cvr_0)      as cvr_0,
            sum(t2.imp)        as imp,
            sum(t2.imp_1)      as imp_1,
            sum(t2.imp_2)      as imp_2,
            sum(t2.imp_3)      as imp_3,
            sum(t2.imp_0)      as imp_0,
            sum(t2.cvr_credit) as cvr_credit,
            sum(t2.rev)        as rev,
            sum(t2.rev_1)      as rev_1,
            sum(t2.rev_2)      as rev_2,
            sum(t2.rev_3)      as rev_3,
            sum(t2.rev_0)      as rev_0,
            sum(t2.cost)       as cost,
            sum(t2.cost_1)     as cost_1,
            sum(t2.cost_2)     as cost_2,
            sum(t2.cost_3)     as cost_3,
            sum(t2.cost_0)     as cost_0


        from (select
                  t0.user_id,
                  t0.yr,
                  t0.wk,
                  sum(t0.cvr)        as cvr,
                  sum(t0.cvr_1)      as cvr_1,
                  sum(t0.cvr_2)      as cvr_2,
                  sum(t0.cvr_3)      as cvr_3,
                  sum(t0.cvr_0)      as cvr_0,
                  sum(t0.imp)        as imp,
                  sum(t0.imp_1)      as imp_1,
                  sum(t0.imp_2)      as imp_2,
                  sum(t0.imp_3)      as imp_3,
                  sum(t0.imp_0)      as imp_0,
                  sum(t0.cvr_credit) as cvr_credit,
                  sum(t0.rev)        as rev,
                  sum(t0.rev_1)      as rev_1,
                  sum(t0.rev_2)      as rev_2,
                  sum(t0.rev_3)      as rev_3,
                  sum(t0.rev_0)      as rev_0,
                  sum(t0.cost)       as cost,
                  sum(t0.cost_1)     as cost_1,
                  sum(t0.cost_2)     as cost_2,
                  sum(t0.cost_3)     as cost_3,
                  sum(t0.cost_0)     as cost_0

              from
                  diap01.mec_us_united_20056.ual_freq_bid_3_tbl4 as t0
              group by
                  t0.user_id,
                  t0.yr,
                  t0.wk

              union all

              select
                  t1.user_id,
                  t1.yr,
                  t1.wk,
                  0             as cvr,
                  0             as cvr_1,
                  0             as cvr_2,
                  0             as cvr_3,
                  0             as cvr_0,
                  sum(t1.imp)   as imp,
                  sum(t1.imp_1) as imp_1,
                  sum(t1.imp_2) as imp_2,
                  sum(t1.imp_3) as imp_3,
                  sum(t1.imp_0) as imp_0,
                  0             as cvr_credit,
                  0             as rev,
                  0             as rev_1,
                  0             as rev_2,
                  0             as rev_3,
                  0             as rev_0,
                  sum(t1.cost)  as cost,
                  sum(t1.cst_1) as cost_1,
                  sum(t1.cst_2) as cost_2,
                  sum(t1.cst_3) as cost_3,
                  sum(t1.cst_0) as cost_0

              from
                  diap01.mec_us_united_20056.ual_freq_bid_3_imp3 as t1
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
    imp,
    wk,
    sum(imp)                                                as imps,
    sum(imp_1)                                           as imps_1,
    sum(imp_2)                                           as imps_2,
    sum(imp_3)                                           as imps_3,
    sum(imp_0)                                           as imps_0,
    count(distinct user_id)                                 as user_cnt,
    count(distinct case when imp_1 > 0 then user_id end) as user_cnt_1,
    count(distinct case when imp_2 > 0 then user_id end) as user_cnt_2,
    count(distinct case when imp_3 > 0 then user_id end) as user_cnt_3,
    count(distinct case when imp_0 > 0 then user_id end) as user_cnt_0,


    sum(cvr)                                                as con,
    sum(cvr_1)                                           as con_1,
    sum(cvr_2)                                           as con_2,
    sum(cvr_3)                                           as con_3,
    sum(cvr_0)                                           as con_0,

--  discounted
    sum(rev * .03)                                          as rev,
    sum(rev_1 * .03)                                     as rev_1,
    sum(rev_2 * .03)                                     as rev_2,
    sum(rev_3 * .03)                                     as rev_3,
    sum(rev_0 * .03)                                     as rev_0,


    sum(cost)                                               as cst,
    sum(cost_1)                                          as cst_1,
    sum(cost_2)                                          as cst_2,
    sum(cost_3)                                          as cst_3,
    sum(cost_0)                                          as cst_0

from diap01.mec_us_united_20056.ual_freq_bid_3_tbl5

group by
    imp,
    wk


--
--
-- SELECT
--     placement
--     FROM diap01.mec_us_united_20056.dfa2_placements
-- GROUP BY
--     placement
--   HAVING COUNT(placement) > 1


select * from diap01.mec_us_united_20056.DIM_calendar
where week = 32