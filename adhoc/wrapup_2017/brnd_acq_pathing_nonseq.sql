-- The goal of this query is to find the number of unique users, per partner, who
-- 1.) were served a Brand impression and a GM Acq impression, in whatever order
-- 2.) then within 7 days, purchased a ticket.
--


-- Table 1: users who were served a Brand Impression

drop table diap01.mec_us_united_20056.ual_pathing_brnd
drop table diap01.mec_us_united_20056.ual_pathing_gacq
drop table diap01.mec_us_united_20056.ual_pathing_conv


create table diap01.mec_us_united_20056.ual_pathing_brnd_nonseq
(
    user_id        varchar(50) not null,
--     campaign_id    int         not null,
    brandtime      int         not null
);

insert into diap01.mec_us_united_20056.ual_pathing_brnd_nonseq
(user_id,
--  campaign_id,
 brandtime)

(select
                a.user_id,
--                 a.campaign_id,
                a.brandtime
from
                (select
                                user_id,
--                                 campaign_id,
                                event_time                                        as brandtime
                from
                                diap01.mec_us_united_20056.dfa2_impression

                where
                                user_id <> '0'
                                and campaign_id in (10918234, 11152017, 11069476, 20177168, 20353010, 10768497, 10740457, 10942240, 11382787, 20113327, 20354464, 20323941, 20334628, 20325908, 20334410, 20337019, 20336827, 20493104)
                                and cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-12-31'
                                and advertiser_id <> 0
                ) as a

    );
commit;

-- // =================================================================================================================
---then served a gm acq imp

create table diap01.mec_us_united_20056.ual_pathing_gacq_nonseq
(
    user_id        varchar(50) not null,
--     campaign_id    int         not null,
    gmacqtime      int         not null,
    imp_nbr        int         not null,
    cvr_window_day int         not null
);

insert into diap01.mec_us_united_20056.ual_pathing_gacq_nonseq
(user_id,
--  campaign_id,
 gmacqtime,imp_nbr,cvr_window_day)

(select
                a.user_id,
--                 a.campaign_id,
                b.gmacqtime,
                b.imp_nbr,
                datediff('day',cast(timestamp_trunc(to_timestamp(a.brandtime/ 1000000),'SS') as date),cast(timestamp_trunc(to_timestamp(b.gmacqtime/ 1000000),'SS') as date)) as cvr_window_day


from
                (select
                                user_id,
--                                 campaign_id,
                                brandtime
                from
                                diap01.mec_us_united_20056.ual_pathing_brnd_nonseq
                ) as a,

                (select
                                t2.user_id,
--                                 t2.campaign_id,
                                t2.event_time        as gmacqtime,
                                row_number() over () as imp_nbr

                from
                                diap01.mec_us_united_20056.dfa2_impression as t2

                where
                                t2.user_id <>'0'
                                -- GM Acq
                                and t2.campaign_id = 10742878
                                and cast(timestamp_trunc(to_timestamp(t2.event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-12-31'
                                and t2.advertiser_id <> 0
                ) as b
where
                b.user_id=a.user_id
                -- make sure Brand time comes before GM Acq time
--                 and a.brandtime < b.gmacqtime
);
commit;

-- // =================================================================================================================

create table diap01.mec_us_united_20056.ual_pathing_tbl3
(
    user_id        varchar(50) not null,
--     campaign_id    int         not null,
    gmacqtime      int         not null,
    imp_nbr        int         not null,
    cvr_window_day int         not null
);

insert into diap01.mec_us_united_20056.ual_pathing_tbl3
(user_id,
--  campaign_id,
 imp_nbr,cvr_cred)

(select
                user_id,
--                 a.campaign_id,
            imp_nbr,
                datediff('day',cast(timestamp_trunc(to_timestamp(a.brandtime/ 1000000),'SS') as date),cast(timestamp_trunc(to_timestamp(b.gmacqtime/ 1000000),'SS') as date)) as cvr_window_day



                from
                                diap01.mec_us_united_20056.ual_pathing_gacq

);
commit;

-- // =================================================================================================================
--puchased a ticket
drop table diap01.mec_us_united_20056.ual_pathing_conv_nonseq
create table diap01.mec_us_united_20056.ual_pathing_conv_nonseq
(
    user_id        varchar(50) not null,
--     campaign_id    int         not null,
    conversiontime int         not null,
    rev             decimal not null,
    cvr_nbr        int         not null
);

insert into diap01.mec_us_united_20056.ual_pathing_conv_nonseq
(user_id,
--  campaign_id,
 conversiontime,
 rev,
 cvr_nbr)

(select distinct
                a.user_id,
--                 b.campaign_id,
--                 sum(a.rev * (1/count(distinct cvr_nbr))) as rev,
                a.rev,
                a.conversiontime,
                cvr_nbr
select count(distinct user_id) as users, sum(cvr_nbr) as conv, sum(rev) as rev, sum(tickets) as tix

from
                (select
                                user_id,
--                                 t1.campaign_id,
                                interaction_time     as conversiontime,
                                sum(total_revenue * 1000000/rates.exchange_rate) as rev,
                                sum(total_conversions) as tickets,
                                count(*) as cvr_nbr
--                                 row_number() over () as cvr_nbr
                from
                                diap01.mec_us_united_20056.dfa2_activity    as t1

                left join
                                diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
                                on upper ( substring (other_data,(instr(other_data,'u3=')+3),3)) = upper (rates.currency)
                                and cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date ) = rates.date

                where
                                t1.activity_id = 978826
                                    and t1.campaign_id in (10742878,10918234, 11152017, 11069476, 20177168, 20353010, 10768497, 10740457, 10942240, 11382787, 20113327, 20354464, 20323941, 20334628, 20325908, 20334410, 20337019, 20336827, 20493104)
                                and t1.user_id <> '0'
                                and t1.total_revenue <> '0'
                                and (length(isnull(t1.event_sub_type,'')) > 0)
                                and cast(timestamp_trunc(to_timestamp(t1.interaction_time / 1000000),'SS') as date) between '2017-01-01' and '2017-12-31'
                                and t1.advertiser_id <> 0
                                and not regexp_like(substring(t1.other_data,(instr(t1.other_data,'u3=') + 3),5),'mil.*','ib')
                    and (user_id in (select distinct user_id from diap01.mec_us_united_20056.ual_pathing_gacq_nonseq)
                    and interaction_time in (select distinct gmacqtime from diap01.mec_us_united_20056.ual_pathing_gacq_nonseq))


                    group by user_id, interaction_time

--                     limit 100

                ) as a

--                 (select
--                                 user_id,
-- --                                 campaign_id,
--                                 gmacqtime
--                 from
--                                 diap01.mec_us_united_20056.ual_pathing_gacq
--                 ) as b
-- where
--                 a.user_id=b.user_id
--                 and a.conversiontime = b.gmacqtime
--
--     group by
--         b.user_id,
--          a.conversiontime,
--                 a.cvr_nbr

);
commit;

-- // =================================================================================================================
-- check to see that user counts go down with each successive query
select count(distinct user_id) from diap01.mec_us_united_20056.ual_pathing_brnd
select count(distinct user_id), count(distinct imp_nbr) from diap01.mec_us_united_20056.ual_pathing_gacq_nonseq
select count(distinct user_id) from diap01.mec_us_united_20056.ual_pathing_conv

----------------------------------------
-- Brand unique reach
select
count (distinct user_id) as brand_users,
count(*) as imps

                from
                                diap01.mec_us_united_20056.dfa2_impression as t2

                where
                                t2.user_id <>'0'
                                -- GM Acq
                                and t2.campaign_id in (10918234, 11152017, 11069476, 20177168, 20353010, 10768497, 10740457, 10942240, 11382787, 20113327, 20354464, 20323941, 20334628, 20325908, 20334410, 20337019, 20336827, 20493104)
                                and cast(timestamp_trunc(to_timestamp(t2.event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-12-31'
                                and t2.advertiser_id <> 0


----------------------------------------
--GM Acq unique reach
select
count (distinct user_id) as gm_acq_users,
count(*) as imps

                from
                                diap01.mec_us_united_20056.dfa2_impression as t2

                where
                                t2.user_id <>'0'
                                -- GM Acq
                                and t2.campaign_id = 10742878
                                and cast(timestamp_trunc(to_timestamp(t2.event_time / 1000000),'SS') as date) between '2017-01-01' and '2017-12-31'
                                and t2.advertiser_id <> 0

---------------------------------------------
--Gm Acq unique converters
select
count (distinct user_id) as gm_acq_converters,
count(*) as conversions,
sum(total_revenue * 1000000/rates.exchange_rate) as rev,
sum(total_conversions) as tickets

                from
                                diap01.mec_us_united_20056.dfa2_activity as t2
                left join
                                diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
                                on upper ( substring (other_data,(instr(other_data,'u3=')+3),3)) = upper (rates.currency)
                                and cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date ) = rates.date
                where
                                t2.user_id <>'0'
                                -- GM Acq
                                and t2.campaign_id = 10742878
                                and t2.advertiser_id <> 0
                                and t2.activity_id = 978826
                                and t2.total_revenue <> '0'
                                and (length(isnull(t2.event_sub_type,'')) > 0)
                                and cast(timestamp_trunc(to_timestamp(t2.interaction_time / 1000000),'SS') as date) between '2017-01-01' and '2017-12-31'
                                and not regexp_like(substring(t2.other_data,(instr(t2.other_data,'u3=') + 3),5),'mil.*','ib')


-----------------------------------------------------------------------------------------------
--Brand campaigns unique converters

select
count (distinct user_id) as brand_converters,
count(*) as conversions,
sum(total_revenue * 1000000/rates.exchange_rate) as rev,
sum(total_conversions) as tickets

                from
                                diap01.mec_us_united_20056.dfa2_activity as t2
                left join
                                diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
                                on upper ( substring (other_data,(instr(other_data,'u3=')+3),3)) = upper (rates.currency)
                                and cast (timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date ) = rates.date

                where
                                t2.user_id <>'0'
                                -- GM Acq
                                and t2.campaign_id in (10918234, 11152017, 11069476, 20177168, 20353010, 10768497, 10740457, 10942240, 11382787, 20113327, 20354464, 20323941, 20334628, 20325908, 20334410, 20337019, 20336827, 20493104)
                                and t2.advertiser_id <> 0
                                and t2.activity_id = 978826
                                and t2.total_revenue <> '0'
                                and (length(isnull(t2.event_sub_type,'')) > 0)
                                and cast(timestamp_trunc(to_timestamp(t2.interaction_time / 1000000),'SS') as date) between '2017-01-01' and '2017-12-31'
                                and not regexp_like(substring(t2.other_data,(instr(t2.other_data,'u3=') + 3),5),'mil.*','ib')


-----------------------------------------------
--Brand Campaigns & its overlap with GM Acq

select
                -- c1.campaign,
                count(distinct user_id) as users,
                sum( cvr_nbr) as conversions,
                sum(  rev) as rev
from
                diap01.mec_us_united_20056.ual_pathing_conv    as t1

-- left join diap01.mec_us_united_20056.dfa2_campaigns as c1
-- on t1.campaign_id = c1.campaign_id

-- group by
-- c1.campaign



select count(distinct user_id) as users,
count(distinct imp_nbr) as imps

from diap01.mec_us_united_20056.ual_pathing_gacq