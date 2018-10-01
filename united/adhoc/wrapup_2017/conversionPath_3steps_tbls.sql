
create table diap01.mec_us_united_20056.ual_acq_exposure
(
    user_id     varchar(50) not null,
    event_time  int         not null,
--  activity_id int         not null,
    imp_nbr     int         not null,
    cvr_nbr     int         not null,
    rev         decimal     not null,
    site_id_dcm int         not null,
    campaign_id int not null,
        placement_id int not null
--  type        varchar(20) not null
)
;-- -. . -..- - / . -. - .-. -.--
insert into diap01.mec_us_united_20056.ual_acq_exposure
(
user_id,
event_time,
-- activity_id,
imp_nbr,
cvr_nbr,
rev,
site_id_dcm,
    campaign_id,
    placement_id
-- type
)

    (
        select
            a.user_id,
            a.event_time,
--          b.activity_id,
            a.imp_nbr,
            b.cvr_nbr,
            b.rev,
            a.site_id_dcm,
            a.campaign_id,
            a.placement_id

        from (
                 select
                     t1.event_time,
                     row_number() over () as imp_nbr,
                     t1.user_id,
                     t1.site_id_dcm,
                     t1.campaign_id,
                     t1.placement_id
--                   p1.placement

                 from diap01.mec_us_united_20056.dfa2_impression as t1


                 where cast(timestamp_trunc(to_timestamp(t1.event_time / 1000000), 'SS') as date) between '2017-08-15' and '2017-12-31'
                     and t1.campaign_id in (10742878, 11177760, 11224605)
                     and t1.site_id_dcm in (1853562, 1190273, 3267410, 1578478, 1239319)
                     and t1.advertiser_id <> 0
                     and t1.user_id <> '0'

             ) as a,

            (
                select *
                from diap01.mec_us_united_20056.ual_acq_activity
--              limit 100

            ) as b

        where a.user_id = b.user_id
            and datediff('day', cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date), cast(timestamp_trunc(to_timestamp(b.interaction_time / 1000000),'SS') as date)) < 8


    )
;
commit;
-- ===========================================================================
create table diap01.mec_us_united_20056.ual_acq_exposure_2
(
    user_id     varchar(50) not null,
    cvr_nbr     int         not null,
    cvr_credit         decimal     not null
)
;-- -. . -..- - / . -. - .-. -.--
insert into diap01.mec_us_united_20056.ual_acq_exposure_2
(
user_id,
cvr_nbr,
cvr_credit
)
    (
        select
            user_id,
            cvr_nbr,
            1 / count(distinct imp_nbr) as cvr_credit
        from
            diap01.mec_us_united_20056.ual_acq_exposure
        group by
            user_id, cvr_nbr
)
;
commit;
-- ===========================================================================

create table diap01.mec_us_united_20056.ual_acq_exposure_3

(
    user_id     varchar(50) not null,
    event_time  int         not null,
--  activity_id int         not null,
    imp_nbr     int         not null,
    cvr_nbr     int         not null,
    rev         decimal     not null,
    site_id_dcm int         not null,
    site_dcm        varchar(6000) not null,
    type        varchar(20) not null,
    cvr_credit         decimal     not null,
    tp_rank int         not null
)
;-- -. . -..- - / . -. - .-. -.--
insert into diap01.mec_us_united_20056.ual_acq_exposure_3
(
user_id,
event_time,
-- activity_id,
imp_nbr,
cvr_nbr,
rev,
site_id_dcm,
site_dcm,
type,
cvr_credit,
tp_rank
)

    (
        select
            user_id,
            event_time,
-- activity_id,
            imp_nbr,
            cvr_nbr,
            rev,
            site_id_dcm,
            site_dcm,
            type,
            cvr_credit,
            tp_rank

        from (
                 select
                     c.*,
                     row_number() over ( partition by c.user_id
                         order by c.event_time desc ) as tp_rank

                 from (select
                     a.*,
                     case
                     when a.site_id_dcm = 1190273 then 'Adara'
                     when a.site_id_dcm = 1578478 then 'Google'
                     else s.site_dcm end as site_dcm,

                     case
                     when (
                         a.campaign_id in (10742878, 11177760, 11224605) and
                             (
                                 (a.site_id_dcm = 1578478) or
                                     (a.site_id_dcm = 1239319 and regexp_like(p1.placement, '.*_RON_.*', 'ib'))
                             )
                     ) then 'rtg'
                     when (
                         ((a.campaign_id in (10742878, 11177760, 11224605)) and
                             ((a.site_id_dcm in (1853562, 1190273, 3267410)) or
                                 (a.site_id_dcm = 1239319 and regexp_like(p1.placement, '.*_PROS_.*', 'ib'))
                             )) or
                             (
                                 (regexp_like(p1.placement, '.*GEN_INT_PROS_FT.*', 'ib'))
                             )
                     ) then 'prs'
                     else 'zzz' end      as type,
                     b.cvr_credit
                 from
                     diap01.mec_us_united_20056.ual_acq_exposure as a

                     left join diap01.mec_us_united_20056.dfa2_sites as s
                     on a.site_id_dcm = s.site_id_dcm

                     left join
                     diap01.mec_us_united_20056.dfa2_placements as p1
                     on a.placement_id = p1.placement_id
                     ,
                     diap01.mec_us_united_20056.ual_acq_exposure_2 as b

                 where
                     a.user_id = b.user_id
                         and
                         a.cvr_nbr = b.cvr_nbr
                      ) as c) as d
-- where tp_rank <= 3) ;
    )
;
commit;