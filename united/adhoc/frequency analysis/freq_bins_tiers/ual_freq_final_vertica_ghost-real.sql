
/* Sequence that chooses between real and ghost advertiser for DBM
Bid strategy breakout
 */

create table diap01.mec_us_united_20056.ual_freq_bid_2_imp1
(
    user_id    varchar(50) not null,
    event_time int         not null,
    bid_group    varchar(50) not null,
    cost       decimal     not null

);

insert into diap01.mec_us_united_20056.ual_freq_bid_2_imp1
(user_id,event_time,bid_group,cost)

    (select
        t3.user_id,
        t3.event_time,
        t3.bid_group,
        t3.cost
    from (select
        *,
        case
        when (p2.placement like '%Test%' and p2.placement like '%Strategy1%') then 'Test-Bid1'
        when (p2.placement like '%Test%' and p2.placement like '%Strategy2%') then 'Test-Bid2'
        when (p2.placement like '%Test%' and p2.placement like '%Strategy3%') then 'Test-Bid3'
        when (p2.placement like '%Test%' and p2.placement like '%First%') then 'Test-Frst'
        when (p2.placement like '%Control%' and p2.placement like '%Strategy1%') then 'Ctrl-Bid1'
        when (p2.placement like '%Control%' and p2.placement like '%Strategy2%') then 'Ctrl-Bid2'
        when (p2.placement like '%Control%' and p2.placement like '%Strategy3%') then 'Ctrl-Bid3'
        when (p2.placement like '%Control%' and p2.placement like '%First%') then 'Ctrl-Frst'
--         when ((p2.placement like '%Test%' or p2.placement like '%Control%') and p2.placement like '%First%') then 'Other'
        else 'XXX'
        end                                                                                as bid_group,
--      case when ((cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-08-31') and (t2.site_id_dcm = 2202011)) then 1
--      when ((cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-09-01' and '2016-12-31') and (t2.site_id_dcm = 1578478)) then 1 else 2 end as rank,
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

    where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
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
--  where t3.rank = 1
        where t3.bid_group != 'XXX'
    );
commit;

-- ====================================================================================================================
-- ====================================================================================================================

-- Get all attributed converters
-- drop table diap01.mec_us_united_20056.ual_freq_tbl1
create table diap01.mec_us_united_20056.ual_freq_bid_2_tbl1
(
    user_id        varchar(50) not null,
    bid_group      varchar(50) not null,
    conversiontime int         not null,
    impressiontime int         not null,
    imp_key        varchar     not null,
    rev            decimal     not null,
    cost           decimal     not null,
    cvr_nbr        int         not null,
    imp_nbr        int         not null
);

insert into diap01.mec_us_united_20056.ual_freq_bid_2_tbl1
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
        when (p2.placement like '%Test%' and p2.placement like '%Strategy1%') then 'Test-Bid1'
        when (p2.placement like '%Test%' and p2.placement like '%Strategy2%') then 'Test-Bid2'
        when (p2.placement like '%Test%' and p2.placement like '%Strategy3%') then 'Test-Bid3'
        when (p2.placement like '%Test%' and p2.placement like '%First%') then 'Test-Frst'
        when (p2.placement like '%Control%' and p2.placement like '%Strategy1%') then 'Ctrl-Bid1'
        when (p2.placement like '%Control%' and p2.placement like '%Strategy2%') then 'Ctrl-Bid2'
        when (p2.placement like '%Control%' and p2.placement like '%Strategy3%') then 'Ctrl-Bid3'
        when (p2.placement like '%Control%' and p2.placement like '%First%') then 'Ctrl-Frst'
--         when ((p2.placement like '%Test%' or p2.placement like '%Control%') and p2.placement like '%First%') then 'Other'
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

             where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-12-31'
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
                 diap01.mec_us_united_20056.ual_freq_bid_2_imp1

            ) as b
        where
            a.user_id = b.user_id and
--          a.bid_group = b.bid_group and
            conversiontime > impressiontime
        and  a.bid_group != 'XXX'

);
commit;
-- ====================================================================================================================

create table diap01.mec_us_united_20056.ual_freq_bid_2_tbl2
(
    user_id    varchar(50) not null,
    cvr_nbr    int         not null,
    cvr_credit decimal     not null

);

insert into diap01.mec_us_united_20056.ual_freq_bid_2_tbl2
(user_id,cvr_nbr,cvr_credit)

    (select
        user_id,
        cvr_nbr,
        1 / count(distinct imp_nbr) as cvr_credit
    from
        diap01.mec_us_united_20056.ual_freq_bid_2_tbl1
    group by
        user_id,
        cvr_nbr);
commit;

-- ====================================================================================================================

create table diap01.mec_us_united_20056.ual_freq_bid_2_tbl3
(
    user_id        varchar(50) not null,
    bid_group      varchar(50) not null,
    conversiontime int         not null,
    impressiontime int         not null,
    rev            decimal     not null,
    cost           decimal     not null,
    cvr_nbr        int         not null,
    imp_nbr        int         not null,
    yr             int         not null,
    wk             int         not null,
    cvr_credit     decimal     not null

);

insert into diap01.mec_us_united_20056.ual_freq_bid_2_tbl3
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
        diap01.mec_us_united_20056.ual_freq_bid_2_tbl1 a,
        diap01.mec_us_united_20056.ual_freq_bid_2_tbl2 b
    where
        a.user_id = b.user_id and
            a.cvr_nbr = b.cvr_nbr
    );
commit;

-- select distinct bid_group from diap01.mec_us_united_20056.ual_freq_bid_2_tbl3

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
--                 diap01.mec_us_united_20056.ual_freq_bid_2_tbl3
-- group by
--                 yr,
--                 wk;
-- ====================================================================================================================

create table diap01.mec_us_united_20056.ual_freq_bid_2_tbl4
(
    user_id       varchar(50) not null,
    yr            int         not null,
    wk            int         not null,
    cvr           int         not null,
    cvr_tst_bid1  int         not null,
    cvr_tst_bid2  int         not null,
    cvr_tst_bid3  int         not null,
    cvr_tst_frst  int         not null,
    cvr_ctl_bid1  int         not null,
    cvr_ctl_bid2  int         not null,
    cvr_ctl_bid3  int         not null,
    cvr_ctl_frst  int         not null,
--     cvr_othr      int         not null,
    imp           int         not null,
    imp_tst_bid1  int         not null,
    imp_tst_bid2  int         not null,
    imp_tst_bid3  int         not null,
    imp_tst_frst  int         not null,
    imp_ctl_bid1  int         not null,
    imp_ctl_bid2  int         not null,
    imp_ctl_bid3  int         not null,
    imp_ctl_frst  int         not null,
--     imp_othr      int         not null,
    cvr_credit    decimal     not null,
    rev           decimal     not null,
    rev_tst_bid1  decimal,
    rev_tst_bid2  decimal,
    rev_tst_bid3  decimal,
    rev_tst_frst  decimal,
    rev_ctl_bid1  decimal,
    rev_ctl_bid2  decimal,
    rev_ctl_bid3  decimal,
    rev_ctl_frst  decimal,
--     rev_othr      decimal,
    cost          decimal,
    cost_tst_bid1 decimal,
    cost_tst_bid2 decimal,
    cost_tst_bid3 decimal,
    cost_tst_frst decimal,
    cost_ctl_bid1 decimal,
    cost_ctl_bid2 decimal,
    cost_ctl_bid3 decimal,
    cost_ctl_frst decimal
--     cost_othr     decimal
);

-- drop table diap01.mec_us_united_20056.ual_freq_bid_2_tbl4
insert into diap01.mec_us_united_20056.ual_freq_bid_2_tbl4
(
user_id,
yr,
wk,
cvr,
cvr_tst_bid1,
cvr_tst_bid2,
cvr_tst_bid3,
cvr_tst_frst,
cvr_ctl_bid1,
cvr_ctl_bid2,
cvr_ctl_bid3,
cvr_ctl_frst,
imp,
imp_tst_bid1,
imp_tst_bid2,
imp_tst_bid3,
imp_tst_frst,
imp_ctl_bid1,
imp_ctl_bid2,
imp_ctl_bid3,
imp_ctl_frst,
cvr_credit,
rev,
rev_tst_bid1,
rev_tst_bid2,
rev_tst_bid3,
rev_tst_frst,
rev_ctl_bid1,
rev_ctl_bid2,
rev_ctl_bid3,
rev_ctl_frst,
cost,
cost_tst_bid1,
cost_tst_bid2,
cost_tst_bid3,
cost_tst_frst,
cost_ctl_bid1,
cost_ctl_bid2,
cost_ctl_bid3,
cost_ctl_frst
)

    (select
        user_id,
        -- bid_group,
        yr,
        wk,
        count(distinct cvr_nbr)   as cvr,
        count(distinct case when bid_group = 'Test-Bid1' then cvr_nbr end) as cvr_tst_bid1,
        count(distinct case when bid_group = 'Test-Bid2' then cvr_nbr end) as cvr_tst_bid2,
        count(distinct case when bid_group = 'Test-Bid3' then cvr_nbr end) as cvr_tst_bid3,
        count(distinct case when bid_group = 'Test-Frst' then cvr_nbr end) as cvr_tst_frst,
        count(distinct case when bid_group = 'Ctrl-Bid1' then cvr_nbr end) as cvr_ctl_bid1,
        count(distinct case when bid_group = 'Ctrl-Bid2' then cvr_nbr end) as cvr_ctl_bid2,
        count(distinct case when bid_group = 'Ctrl-Bid3' then cvr_nbr end) as cvr_ctl_bid3,
        count(distinct case when bid_group = 'Ctrl-Frst' then cvr_nbr end) as cvr_ctl_frst,
--         count(distinct case when bid_group = 'Other'     then cvr_nbr end) as cvr_othr,
--          *sum(cvr_credit)
--                                as cvr,
        count(distinct imp_nbr)   as imp,
        count(distinct case when bid_group = 'Test-Bid1' then imp_nbr end) as imp_tst_bid1,
        count(distinct case when bid_group = 'Test-Bid2' then imp_nbr end) as imp_tst_bid2,
        count(distinct case when bid_group = 'Test-Bid3' then imp_nbr end) as imp_tst_bid3,
        count(distinct case when bid_group = 'Test-Frst' then imp_nbr end) as imp_tst_frst,

        count(distinct case when bid_group = 'Ctrl-Bid1' then imp_nbr end) as imp_ctl_bid1,
        count(distinct case when bid_group = 'Ctrl-Bid2' then imp_nbr end) as imp_ctl_bid2,
        count(distinct case when bid_group = 'Ctrl-Bid3' then imp_nbr end) as imp_ctl_bid3,
        count(distinct case when bid_group = 'Ctrl-Frst' then imp_nbr end) as imp_ctl_frst,

--         count(distinct case when bid_group = 'Other'     then imp_nbr end) as imp_othr,

        sum(cvr_credit)           as cvr_credit,

        sum(rev * cvr_credit) as rev,
        sum(case when bid_group = 'Test-Bid1' then rev * cvr_credit end) as rev_tst_bid1,
        sum(case when bid_group = 'Test-Bid2' then rev * cvr_credit end) as rev_tst_bid2,
        sum(case when bid_group = 'Test-Bid3' then rev * cvr_credit end) as rev_tst_bid3,
        sum(case when bid_group = 'Test-Frst' then rev * cvr_credit end) as rev_tst_frst,

        sum(case when bid_group = 'Ctrl-Bid1' then rev * cvr_credit end) as rev_ctl_bid1,
        sum(case when bid_group = 'Ctrl-Bid2' then rev * cvr_credit end) as rev_ctl_bid2,
        sum(case when bid_group = 'Ctrl-Bid3' then rev * cvr_credit end) as rev_ctl_bid3,
        sum(case when bid_group = 'Ctrl-Frst' then rev * cvr_credit end) as rev_ctl_frst,

--         sum(case when bid_group = 'Other'     then rev * cvr_credit end) as rev_othr,

        sum(cost)                 as cost,
        sum(case when bid_group = 'Test-Bid1' then cost end) as cost_tst_bid1,
        sum(case when bid_group = 'Test-Bid2' then cost end) as cost_tst_bid2,
        sum(case when bid_group = 'Test-Bid3' then cost end) as cost_tst_bid3,
        sum(case when bid_group = 'Test-Frst' then cost end) as cost_tst_frst,

        sum(case when bid_group = 'Ctrl-Bid1' then cost end) as cost_ctl_bid1,
        sum(case when bid_group = 'Ctrl-Bid2' then cost end) as cost_ctl_bid2,
        sum(case when bid_group = 'Ctrl-Bid3' then cost end) as cost_ctl_bid3,
        sum(case when bid_group = 'Ctrl-Frst' then cost end) as cost_ctl_frst

--         sum(case when bid_group = 'Other'     then cost end) as cost_othr
    from
        diap01.mec_us_united_20056.ual_freq_bid_2_tbl3
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

create table diap01.mec_us_united_20056.ual_freq_bid_2_imp2
(
    user_id    varchar(50) not null,
--  bid_group  varchar(50) not null,
    event_time int         not null,
    yr         int         not null,
    wk         int         not null,
    imp_key    varchar     not null,
    cost       decimal     not null,
    cst_tst_bid1   decimal,
    cst_tst_bid2   decimal,
    cst_tst_bid3   decimal,
    cst_tst_frst   decimal,
    cst_ctl_bid1   decimal,
    cst_ctl_bid2   decimal,
    cst_ctl_bid3   decimal,
    cst_ctl_frst   decimal,
    imp        int         not null,
    imp_tst_bid1   int,
    imp_tst_bid2   int,
    imp_tst_bid3   int,
    imp_tst_frst   int,
    imp_ctl_bid1   int,
    imp_ctl_bid2   int,
    imp_ctl_bid3   int,
    imp_ctl_frst   int

);

insert into diap01.mec_us_united_20056.ual_freq_bid_2_imp2
(user_id,event_time,yr,wk,imp_key,
    cost,
cst_tst_bid1,
cst_tst_bid2,
cst_tst_bid3,
cst_tst_frst,
cst_ctl_bid1,
cst_ctl_bid2,
cst_ctl_bid3,
cst_ctl_frst,
    imp,
imp_tst_bid1,
imp_tst_bid2,
imp_tst_bid3,
imp_tst_frst,
imp_ctl_bid1,
imp_ctl_bid2,
imp_ctl_bid3,
imp_ctl_frst

    )

    (select
        t4.user_id,
--      t4.bid_group,
        t4.event_time,
        date_part('year',cast(timestamp_trunc(to_timestamp(t4.event_time / 1000000),'SS') as date)) as yr,
        date_part('week',cast(timestamp_trunc(to_timestamp(t4.event_time / 1000000),'SS') as date)) as wk,
        t4.imp_key,
        t4.cost,
        t4.cst_tst_bid1,
        t4.cst_tst_bid2,
        t4.cst_tst_bid3,
        t4.cst_tst_frst,
        t4.cst_ctl_bid1,
        t4.cst_ctl_bid2,
        t4.cst_ctl_bid3,
        t4.cst_ctl_frst,
        t4.imp,
        t4.imp_tst_bid1,
        t4.imp_tst_bid2,
        t4.imp_tst_bid3,
        t4.imp_tst_frst,
        t4.imp_ctl_bid1,
        t4.imp_ctl_bid2,
        t4.imp_ctl_bid3,
        t4.imp_ctl_frst

    from
        (select
            t3.user_id,
            t3.event_time,
--             t3.bid_group,
            md5(t3.user_id || cast(t3.event_time as varchar )) as imp_key,
            sum(t3.cost)                                       as cost,
            sum(case when bid_group = 'Test-Bid1' then t3.cost end)  as cst_tst_bid1,
            sum(case when bid_group = 'Test-Bid2' then t3.cost end)  as cst_tst_bid2,
            sum(case when bid_group = 'Test-Bid3' then t3.cost end)  as cst_tst_bid3,
            sum(case when bid_group = 'Test-Frst' then t3.cost end)  as cst_tst_frst,
            sum(case when bid_group = 'Ctrl-Bid1' then t3.cost end)  as cst_ctl_bid1,
            sum(case when bid_group = 'Ctrl-Bid2' then t3.cost end)  as cst_ctl_bid2,
            sum(case when bid_group = 'Ctrl-Bid3' then t3.cost end)  as cst_ctl_bid3,
            sum(case when bid_group = 'Ctrl-Frst' then t3.cost end)  as cst_ctl_frst,

            count(*) as imp,
            count(case when bid_group = 'Test-Bid1' then 1 end) as imp_tst_bid1,
            count(case when bid_group = 'Test-Bid2' then 1 end) as imp_tst_bid2,
            count(case when bid_group = 'Test-Bid3' then 1 end) as imp_tst_bid3,
            count(case when bid_group = 'Test-Frst' then 1 end) as imp_tst_frst,
            count(case when bid_group = 'Ctrl-Bid1' then 1 end) as imp_ctl_bid1,
            count(case when bid_group = 'Ctrl-Bid2' then 1 end) as imp_ctl_bid2,
            count(case when bid_group = 'Ctrl-Bid3' then 1 end) as imp_ctl_bid3,
            count(case when bid_group = 'Ctrl-Frst' then 1 end) as imp_ctl_frst

--             count(case when bid_group = 'Other'     then 1 end) as imp_othr
        from (select
            *,
        case
        when (p2.placement like '%Test%' and p2.placement like '%Strategy1%') then 'Test-Bid1'
        when (p2.placement like '%Test%' and p2.placement like '%Strategy2%') then 'Test-Bid2'
        when (p2.placement like '%Test%' and p2.placement like '%Strategy3%') then 'Test-Bid3'
        when (p2.placement like '%Test%' and p2.placement like '%First%') then 'Test-Frst'
        when (p2.placement like '%Control%' and p2.placement like '%Strategy1%') then 'Ctrl-Bid1'
        when (p2.placement like '%Control%' and p2.placement like '%Strategy2%') then 'Ctrl-Bid2'
        when (p2.placement like '%Control%' and p2.placement like '%Strategy3%') then 'Ctrl-Bid3'
        when (p2.placement like '%Control%' and p2.placement like '%First%') then 'Ctrl-Frst'
--         when ((p2.placement like '%Test%' or p2.placement like '%Control%') and p2.placement like '%First%') then 'Other'
        else 'XXX'
        end                                                                                as bid_group,
--             case when ((cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-07-15' and '2016-08-31') and (t2.site_id_dcm = 2202011)) then 1
--             when ((cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-09-01' and '2016-12-31') and (t2.site_id_dcm = 1578478)) then 1 else 2 end as rank,
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
--         where t3.rank = 1
            where t3.bid_group != 'XXX'
        group by t3.user_id,t3.event_time
--          ,t3.bid_group
        ) as t4
    where t4.imp_key not in (select distinct imp_key
    from diap01.mec_us_united_20056.ual_freq_bid_2_tbl1)
    );
commit;
-- ====================================================================================================================
-- ====================================================================================================================
-- Summary non-converter Impressions table

create table diap01.mec_us_united_20056.ual_freq_bid_2_imp3
(
    user_id    varchar(50) not null,
    yr         int         not null,
    wk         int         not null,
    cost       decimal     not null,
    cst_tst_bid1   decimal,
    cst_tst_bid2   decimal,
    cst_tst_bid3   decimal,
    cst_tst_frst   decimal,
    cst_ctl_bid1   decimal,
    cst_ctl_bid2   decimal,
    cst_ctl_bid3   decimal,
    cst_ctl_frst   decimal,
    -- cst_othr   decimal,
    imp        int         not null,
    imp_tst_bid1   int,
    imp_tst_bid2   int,
    imp_tst_bid3   int,
    imp_tst_frst   int,
    imp_ctl_bid1   int,
    imp_ctl_bid2   int,
    imp_ctl_bid3   int,
    imp_ctl_frst   int
    -- imp_othr   int
);

insert into diap01.mec_us_united_20056.ual_freq_bid_2_imp3
(
user_id,yr,wk,
cost,
cst_tst_bid1,
cst_tst_bid2,
cst_tst_bid3,
cst_tst_frst,
cst_ctl_bid1,
cst_ctl_bid2,
cst_ctl_bid3,
cst_ctl_frst,
imp,
imp_tst_bid1,
imp_tst_bid2,
imp_tst_bid3,
imp_tst_frst,
imp_ctl_bid1,
imp_ctl_bid2,
imp_ctl_bid3,
imp_ctl_frst
)

    (select
        user_id,
        yr,
        wk,
--      0         as cvr,
        sum(cost)       as cost,
        sum(cst_tst_bid1)   as cst_tst_bid1,
        sum(cst_tst_bid2)   as cst_tst_bid2,
        sum(cst_tst_bid3)   as cst_tst_bid3,
        sum(cst_tst_frst)   as cst_tst_frst,
        sum(cst_ctl_bid1)   as cst_ctl_bid1,
        sum(cst_ctl_bid2)   as cst_ctl_bid2,
        sum(cst_ctl_bid3)   as cst_ctl_bid3,
        sum(cst_ctl_frst)   as cst_ctl_frst,

        sum(imp)        as imp,
        sum(imp_tst_bid1)   as imp_tst_bid1,
        sum(imp_tst_bid2)   as imp_tst_bid2,
        sum(imp_tst_bid3)   as imp_tst_bid3,
        sum(imp_tst_frst)   as imp_tst_frst,
        sum(imp_ctl_bid1)   as imp_ctl_bid1,
        sum(imp_ctl_bid2)   as imp_ctl_bid2,
        sum(imp_ctl_bid3)   as imp_ctl_bid3,
        sum(imp_ctl_frst)   as imp_ctl_frst
    from
        diap01.mec_us_united_20056.ual_freq_bid_2_imp2
    group by
        user_id,
        yr,
        wk

    );
commit;

-- ====================================================================================================================
-- ====================================================================================================================


create table diap01.mec_us_united_20056.ual_freq_bid_2_tbl5
(
    user_id       varchar(50) not null,
    yr            int         not null,
    wk            int         not null,
    cvr           int         not null,
    cvr_tst_bid1  int         not null,
    cvr_tst_bid2  int         not null,
    cvr_tst_bid3  int         not null,
    cvr_tst_frst  int         not null,
    cvr_ctl_bid1  int         not null,
    cvr_ctl_bid2  int         not null,
    cvr_ctl_bid3  int         not null,
    cvr_ctl_frst  int         not null,
    imp           int         not null,
    imp_tst_bid1  int         not null,
    imp_tst_bid2  int         not null,
    imp_tst_bid3  int         not null,
    imp_tst_frst  int         not null,
    imp_ctl_bid1  int         not null,
    imp_ctl_bid2  int         not null,
    imp_ctl_bid3  int         not null,
    imp_ctl_frst  int         not null,
    cvr_credit    decimal     not null,
    rev           decimal     not null,
    rev_tst_bid1  decimal,
    rev_tst_bid2  decimal,
    rev_tst_bid3  decimal,
    rev_tst_frst  decimal,
    rev_ctl_bid1  decimal,
    rev_ctl_bid2  decimal,
    rev_ctl_bid3  decimal,
    rev_ctl_frst  decimal,
    cost          decimal,
    cost_tst_bid1 decimal,
    cost_tst_bid2 decimal,
    cost_tst_bid3 decimal,
    cost_tst_frst decimal,
    cost_ctl_bid1 decimal,
    cost_ctl_bid2 decimal,
    cost_ctl_bid3 decimal,
    cost_ctl_frst decimal
);

insert into diap01.mec_us_united_20056.ual_freq_bid_2_tbl5
(
user_id,
yr,
wk,
cvr,
cvr_tst_bid1,
cvr_tst_bid2,
cvr_tst_bid3,
cvr_tst_frst,
cvr_ctl_bid1,
cvr_ctl_bid2,
cvr_ctl_bid3,
cvr_ctl_frst,
imp,
imp_tst_bid1,
imp_tst_bid2,
imp_tst_bid3,
imp_tst_frst,
imp_ctl_bid1,
imp_ctl_bid2,
imp_ctl_bid3,
imp_ctl_frst,
cvr_credit,
rev,
rev_tst_bid1,
rev_tst_bid2,
rev_tst_bid3,
rev_tst_frst,
rev_ctl_bid1,
rev_ctl_bid2,
rev_ctl_bid3,
rev_ctl_frst,
cost,
cost_tst_bid1,
cost_tst_bid2,
cost_tst_bid3,
cost_tst_frst,
cost_ctl_bid1,
cost_ctl_bid2,
cost_ctl_bid3,
cost_ctl_frst
)

    (
        select
            t2.user_id,
            t2.yr,
            t2.wk,
          sum(t2.cvr)      as cvr,
                  sum(t2.cvr_tst_bid1) as cvr_tst_bid1,
                  sum(t2.cvr_tst_bid2) as cvr_tst_bid2,
                  sum(t2.cvr_tst_bid3) as cvr_tst_bid3,
                  sum(t2.cvr_ctl_bid1) as cvr_tst_frst,
                  sum(t2.cvr_ctl_bid2) as cvr_ctl_bid1,
                  sum(t2.cvr_ctl_bid3) as cvr_ctl_bid2,
                  sum(t2.cvr_ctl_bid2) as cvr_ctl_bid3,
                  sum(t2.cvr_ctl_bid3) as cvr_ctl_frst,
                  sum(t2.imp)      as imp,
                  sum(t2.imp_tst_bid1) as imp_tst_bid1,
                  sum(t2.imp_tst_bid2) as imp_tst_bid2,
                  sum(t2.imp_tst_bid3) as imp_tst_bid3,
                  sum(t2.imp_ctl_bid1) as imp_tst_frst,
                  sum(t2.imp_ctl_bid2) as imp_ctl_bid1,
                  sum(t2.imp_ctl_bid3) as imp_ctl_bid2,
                  sum(t2.imp_ctl_bid2) as imp_ctl_bid3,
                  sum(t2.imp_ctl_bid3) as imp_ctl_frst,
                  sum(t2.cvr_credit)   as cvr_credit,
                  sum(t2.rev)      as rev,
                  sum(t2.rev_tst_bid1) as rev_tst_bid1,
                  sum(t2.rev_tst_bid2) as rev_tst_bid2,
                  sum(t2.rev_tst_bid3) as rev_tst_bid3,
                  sum(t2.rev_ctl_bid1) as rev_tst_frst,
                  sum(t2.rev_ctl_bid2) as rev_ctl_bid1,
                  sum(t2.rev_ctl_bid3) as rev_ctl_bid2,
                  sum(t2.rev_ctl_bid2) as rev_ctl_bid3,
                  sum(t2.rev_ctl_bid3) as rev_ctl_frst,
                  sum(t2.cost)         as cost,
                  sum(t2.cost_tst_bid1) as cost_tst_bid1,
                  sum(t2.cost_tst_bid2) as cost_tst_bid2,
                  sum(t2.cost_tst_bid3) as cost_tst_bid3,
                  sum(t2.cost_ctl_bid1) as cost_tst_frst,
                  sum(t2.cost_ctl_bid2) as cost_ctl_bid1,
                  sum(t2.cost_ctl_bid3) as cost_ctl_bid2,
                  sum(t2.cost_ctl_bid2) as cost_ctl_bid3,
                  sum(t2.cost_ctl_bid3) as cost_ctl_frst

        from (select
                  t0.user_id,
                  t0.yr,
                  t0.wk,
                  sum(t0.cvr)      as cvr,
                  sum(t0.cvr_tst_bid1) as cvr_tst_bid1,
                  sum(t0.cvr_tst_bid2) as cvr_tst_bid2,
                  sum(t0.cvr_tst_bid3) as cvr_tst_bid3,
                  sum(t0.cvr_ctl_bid1) as cvr_tst_frst,
                  sum(t0.cvr_ctl_bid2) as cvr_ctl_bid1,
                  sum(t0.cvr_ctl_bid3) as cvr_ctl_bid2,
                  sum(t0.cvr_ctl_bid2) as cvr_ctl_bid3,
                  sum(t0.cvr_ctl_bid3) as cvr_ctl_frst,
                  sum(t0.imp)      as imp,
                  sum(t0.imp_tst_bid1) as imp_tst_bid1,
                  sum(t0.imp_tst_bid2) as imp_tst_bid2,
                  sum(t0.imp_tst_bid3) as imp_tst_bid3,
                  sum(t0.imp_ctl_bid1) as imp_tst_frst,
                  sum(t0.imp_ctl_bid2) as imp_ctl_bid1,
                  sum(t0.imp_ctl_bid3) as imp_ctl_bid2,
                  sum(t0.imp_ctl_bid2) as imp_ctl_bid3,
                  sum(t0.imp_ctl_bid3) as imp_ctl_frst,
                  sum(t0.cvr_credit)   as cvr_credit,
                  sum(t0.rev)      as rev,
                  sum(t0.rev_tst_bid1) as rev_tst_bid1,
                  sum(t0.rev_tst_bid2) as rev_tst_bid2,
                  sum(t0.rev_tst_bid3) as rev_tst_bid3,
                  sum(t0.rev_ctl_bid1) as rev_tst_frst,
                  sum(t0.rev_ctl_bid2) as rev_ctl_bid1,
                  sum(t0.rev_ctl_bid3) as rev_ctl_bid2,
                  sum(t0.rev_ctl_bid2) as rev_ctl_bid3,
                  sum(t0.rev_ctl_bid3) as rev_ctl_frst,
                  sum(t0.cost)         as cost,
                  sum(t0.cost_tst_bid1) as cost_tst_bid1,
                  sum(t0.cost_tst_bid2) as cost_tst_bid2,
                  sum(t0.cost_tst_bid3) as cost_tst_bid3,
                  sum(t0.cost_ctl_bid1) as cost_tst_frst,
                  sum(t0.cost_ctl_bid2) as cost_ctl_bid1,
                  sum(t0.cost_ctl_bid3) as cost_ctl_bid2,
                  sum(t0.cost_ctl_bid2) as cost_ctl_bid3,
                  sum(t0.cost_ctl_bid3) as cost_ctl_frst
              from
                  diap01.mec_us_united_20056.ual_freq_bid_2_tbl4 as t0
              group by
                  t0.user_id,
                  t0.yr,
                  t0.wk

              union all

              select
                  t1.user_id,
                  t1.yr,
                  t1.wk,
                  0                as cvr,
                  0                as cvr_tst_bid1,
                  0                as cvr_tst_bid2,
                  0                as cvr_tst_bid3,
                  0                as cvr_tst_frst,
                  0                as cvr_ctl_bid1,
                  0                as cvr_ctl_bid2,
                  0                as cvr_ctl_bid3,
                  0                as cvr_ctl_frst,
                  sum(t1.imp)      as imp,
                  sum(t1.imp_tst_bid1) as imp_tst_bid1,
                  sum(t1.imp_tst_bid2) as imp_tst_bid2,
                  sum(t1.imp_tst_bid3) as imp_tst_bid3,
                  sum(t1.imp_tst_frst) as imp_tst_frst,
                  sum(t1.imp_ctl_bid1) as imp_ctl_bid1,
                  sum(t1.imp_ctl_bid2) as imp_ctl_bid2,
                  sum(t1.imp_ctl_bid3) as imp_ctl_bid3,
                  sum(t1.imp_ctl_frst) as imp_ctl_frst,
                  0                as cvr_credit,
                  0                as rev,
                  0                as rev_tst_bid1,
                  0                as rev_tst_bid2,
                  0                as rev_tst_bid3,
                  0                as rev_tst_frst,
                  0                as rev_ctl_bid1,
                  0                as rev_ctl_bid2,
                  0                as rev_ctl_bid3,
                  0                as rev_ctl_frst,
                  sum(t1.cost)     as cost,
                  sum(t1.cst_tst_bid1) as cst_tst_bid1,
                  sum(t1.cst_tst_bid2) as cst_tst_bid2,
                  sum(t1.cst_tst_bid3) as cst_tst_bid3,
                  sum(t1.cst_tst_frst) as cst_tst_frst,
                  sum(t1.cst_ctl_bid1) as cst_ctl_bid1,
                  sum(t1.cst_ctl_bid2) as cst_ctl_bid2,
                  sum(t1.cst_ctl_bid3) as cst_ctl_bid3,
                  sum(t1.cst_ctl_frst) as cst_ctl_frst
              from
                  diap01.mec_us_united_20056.ual_freq_bid_2_imp3 as t1
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
    sum(imp)                                                    as imps,
    sum(imp_tst_bid1)                                           as imp_tst_bid1,
    sum(imp_tst_bid2)                                           as imp_tst_bid2,
    sum(imp_tst_bid3)                                           as imp_tst_bid3,
    sum(imp_tst_frst)                                           as imp_tst_frst,
    sum(imp_ctl_bid1)                                           as imp_ctl_bid1,
    sum(imp_ctl_bid2)                                           as imp_ctl_bid2,
    sum(imp_ctl_bid3)                                           as imp_ctl_bid3,
    sum(imp_ctl_frst)                                           as imp_ctl_frst,

    count(distinct user_id)                                     as user_cnt,
    count(distinct case when imp_tst_bid1 > 0 then user_id end) as user_cnt_tst_bid1,
    count(distinct case when imp_tst_bid2 > 0 then user_id end) as user_cnt_tst_bid2,
    count(distinct case when imp_tst_bid3 > 0 then user_id end) as user_cnt_tst_bid3,
    count(distinct case when imp_tst_frst > 0 then user_id end) as user_cnt_tst_frst,
    count(distinct case when imp_ctl_bid1 > 0 then user_id end) as user_cnt_ctl_bid1,
    count(distinct case when imp_ctl_bid2 > 0 then user_id end) as user_cnt_ctl_bid2,
    count(distinct case when imp_ctl_bid3 > 0 then user_id end) as user_cnt_ctl_bid3,
    count(distinct case when imp_ctl_frst > 0 then user_id end) as user_cnt_ctl_frst,


    sum(cvr)                                                    as con,
    sum(cvr_tst_bid1)                                           as con_tst_bid1,
    sum(cvr_tst_bid2)                                           as con_tst_bid2,
    sum(cvr_tst_bid3)                                           as con_tst_bid3,
    sum(cvr_tst_frst)                                           as con_tst_frst,
    sum(cvr_ctl_bid1)                                           as con_ctl_bid1,
    sum(cvr_ctl_bid2)                                           as con_ctl_bid2,
    sum(cvr_ctl_bid3)                                           as con_ctl_bid3,
    sum(cvr_ctl_frst)                                           as con_ctl_frst,

    sum(rev * .03)                                              as rev,
    sum(rev_tst_bid1 * .015)                                    as rev_tst_bid1,
    sum(rev_tst_bid2 * .0225)                                   as rev_tst_bid2,
    sum(rev_tst_bid3 * .06)                                     as rev_tst_bid3,
    sum(rev_tst_frst * .06)                                     as rev_tst_frst,

    sum(rev_ctl_bid1 * .015)                                    as rev_ctl_bid1,
    sum(rev_ctl_bid2 * .0225)                                   as rev_ctl_bid2,
    sum(rev_ctl_bid3 * .06)                                     as rev_ctl_bid3,
    sum(rev_ctl_frst * .06)                                     as rev_ctl_frst,

    sum(cost)                                                   as cst,
    sum(cost_tst_bid1)                                          as cost_tst_bid1,
    sum(cost_tst_bid2)                                          as cost_tst_bid2,
    sum(cost_tst_bid3)                                          as cost_tst_bid3,
    sum(cost_tst_frst)                                          as cost_tst_frst,
    sum(cost_ctl_bid1)                                          as cost_ctl_bid1,
    sum(cost_ctl_bid2)                                          as cost_ctl_bid2,
    sum(cost_ctl_bid3)                                          as cost_ctl_bid3,
    sum(cost_ctl_frst)                                          as cost_ctl_frst

from diap01.mec_us_united_20056.ual_freq_bid_2_tbl5

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