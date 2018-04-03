--For the Quantcast prospecting Test 2018:
--Goal: Compare exposed unique users, tickets, impressions, and leads between the Control and Test Group

--===================================================================================================================================================

--Quantcast Test: CONTROL GROUP
--1. First exposed to the Control QC placements
--2. Then, within 7 days, purchased a ticket


-- Table 1: users who were served a QC control placement:


create table diap01.mec_us_united_20056.ual_qc_control
(
    users           varchar(50)    not null,
    imps            int            not null,
    exposuretime    int            not null
);

insert into diap01.mec_us_united_20056.ual_qc_control
(users,
imps,
exposuretime)

(select
                a.users,
                a.imps,
                a.exposuretime
from
                (select
                                t2.user_id as users,
                                count(*)  as imps,
                                t2.event_time as exposuretime

                from diap01.mec_us_united_20056.dfa2_impression as t2


                where
                                cast(timestamp_trunc(to_timestamp(t2.event_time / 1000000),'SS') as date) between '2018-03-07' and '2018-03-14'
                                and t2.advertiser_id <> 0
                                and t2.user_id <> '0'
                                and t2.campaign_id =  20606595 --GM Acq 2018
                                and t2.site_id_dcm =  3267410 --Quantcast
                                and t2.placement_id in (215129150, 215144749, 215145031, 215145553, 215145556, 215222640, 215222910, 215225244, 215225256) --Control group placement_ids

                              group by t2.user_id, t2.event_time

                ) as a

    );
commit;



-----------------------------------------------------------------
--To check unique users and total imps count:
select count(distinct users) as users, sum(imps) as imps
              from diap01.mec_us_united_20056.ual_qc_control
-----------------------------------------------------------------



--Table 2: Those exposed users then purchased a ticket:



create table diap01.mec_us_united_20056.ual_qc_control_cvr
(
    users          varchar(50) not null,
    conversiontime int         not null,
    tix            int         not null,
    led            int         not null,
    exposuretime   int         not null

);

insert into diap01.mec_us_united_20056.ual_qc_control_cvr
(users,conversiontime,tix,led,exposuretime)

(select
                b.users,
                a.conversiontime,
                a.tix,
                a.led,
                b.exposuretime


                from(


                select
                                t1.user_id,
                                t1.interaction_time    as conversiontime,
                                case when t1.activity_id = 1086066 then 1 else 0 end as led,
                                case when t1.activity_id = 978826  and t1.total_revenue <> 0 then t1.total_conversions else 0 end as tix
                from
                                diap01.mec_us_united_20056.dfa2_activity    as t1

                where
                                (length(isnull(t1.event_sub_type,'')) > 0)
                                and cast(timestamp_trunc(to_timestamp(t1.interaction_time / 1000000),'SS') as date) between '2018-03-07' and '2018-03-14'
                                and t1.advertiser_id <> 0


                ) as a,

                (select
                                users,
                                exposuretime
                from
                                diap01.mec_us_united_20056.ual_qc_control
                ) as b
where
               a.conversiontime = b.exposuretime

);
commit;


--------------------------------------------------------------------------
--To check converted unique users, tix, and leads:
select sum(led) as leads, sum(tix) as tix, count(distinct users) as users
from diap01.mec_us_united_20056.ual_qc_control_cvr
--------------------------------------------------------------------------




--=========================================================================================================================================================================

--Quantcast Test: TEST GROUP
--1. First exposed to the Test QC placements
--2. Then, within 7 days, purchased a ticket


-- Table 1: users who were served a QC Test placement:


create table diap01.mec_us_united_20056.ual_qc_test
(
    users           varchar(50)    not null,
    imps            int            not null,
    exposuretime    int            not null
);

insert into diap01.mec_us_united_20056.ual_qc_test
(users,
imps,
exposuretime)

(select
                a.users,
                a.imps,
                a.exposuretime
from
                (select
                                t2.user_id as users,
                                count(*)  as imps,
                                t2.event_time as exposuretime

                from diap01.mec_us_united_20056.dfa2_impression as t2


                where
                                cast(timestamp_trunc(to_timestamp(t2.event_time / 1000000),'SS') as date) between '2018-03-07' and '2018-03-14'
                                and t2.advertiser_id <> 0
                                and t2.user_id <> '0'
                                and t2.campaign_id =  20606595 --GM Acq 2018
                                and t2.site_id_dcm =  3267410 --Quantcast
                                and t2.placement_id in (211543190, 211593748, 211594354, 211602330, 211605807, 211609899, 211609902, 211609905, 211610649) --Test group placement_ids


                              group by t2.user_id, t2.event_time

                ) as a

    );
commit;



-----------------------------------------------------------------
--To check unique users and total imps count:
select count(distinct users) as users, sum(imps) as imps
              diap01.mec_us_united_20056.ual_qc_test
-----------------------------------------------------------------



--Table 2: Those exposed users then purchased a ticket:


create table diap01.mec_us_united_20056.ual_qc_test_cvr
(
    users          varchar(50) not null,
    conversiontime int         not null,
    tix            int         not null,
    led            int         not null,
    exposuretime   int         not null
);

insert into diap01.mec_us_united_20056.ual_qc_test_cvr
(users,conversiontime,tix,led,exposuretime)

(select
                b.users,
                a.conversiontime,
                a.tix,
                a.led,
                b.exposuretime


                from(


                select
                                t1.user_id,
                                t1.interaction_time    as conversiontime,
                                case when t1.activity_id = 1086066 then 1 else 0 end as led,
                                case when t1.activity_id = 978826  and t1.total_revenue <> 0 then t1.total_conversions else 0 end as tix
                from
                                diap01.mec_us_united_20056.dfa2_activity    as t1

                where
                                (length(isnull(t1.event_sub_type,'')) > 0)
                                and cast(timestamp_trunc(to_timestamp(t1.interaction_time / 1000000),'SS') as date) between '2018-03-07' and '2018-03-14'
                                and t1.advertiser_id <> 0


                ) as a,

                (select
                                users,
                                exposuretime
                from
                                diap01.mec_us_united_20056.ual_qc_test
                ) as b
where
                a.conversiontime = b.exposuretime

);
commit;


--------------------------------------------------------------------------
--To check converted unique users, tix, and leads:
select sum(led) as leads, sum(tix) as tix, count(distinct users) as users
from diap01.mec_us_united_20056.ual_qc_test_cvr
--------------------------------------------------------------------------
