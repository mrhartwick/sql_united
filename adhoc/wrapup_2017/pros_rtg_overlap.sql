drop table diap01.mec_us_united_20056.ual_overlap_prs
create table diap01.mec_us_united_20056.ual_overlap_prs
(
    user_id        varchar(50) not null,
    site_id_dcm    int         not null,
    placement_id int not null,
    impressiontime int         not null,
    imp_nbr        int         not null
);

insert into diap01.mec_us_united_20056.ual_overlap_prs
(user_id,site_id_dcm,placement_id,impressiontime,imp_nbr)

(select
                                t2.user_id,
                                t2.site_id_dcm,
                                t2.placement_id,
                                t2.event_time                                   as impressiontime,
                                row_number() over ()                            as imp_nbr
                from
                                diap01.mec_us_united_20056.dfa2_impression      as t2
                left join
                                diap01.mec_us_united_20056.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                where
                                t2.user_id <> '0'
                                and t2.campaign_id = 10742878
                                and t2.site_id_dcm in (1190273, 1578478, 1239319, 1853562, 3267410)
                                and regexp_like(p1.placement,'.*_PROS_FT.*','ib')
                                and cast(timestamp_trunc(to_timestamp(t2.event_time / 1000000),'SS') as date) between '2017-08-15' and '2017-12-31'
                                and advertiser_id <> 0

    );
commit;

-- // =================================================================================================================
drop table diap01.mec_us_united_20056.ual_overlap_rtg
create table diap01.mec_us_united_20056.ual_overlap_rtg
(
    user_id        varchar(50) not null,
    site_id_dcm    int         not null,
        placement_id int not null,
        impressiontime int         not null,
    imp_nbr        int         not null
);

insert into diap01.mec_us_united_20056.ual_overlap_rtg
(user_id,site_id_dcm,placement_id,impressiontime,imp_nbr)

(select
                                t2.user_id,
                                t2.site_id_dcm,
                                t2.placement_id,
                                t2.event_time                                   as impressiontime,
                                row_number() over ()                            as imp_nbr
                from
                                diap01.mec_us_united_20056.dfa2_impression      as t2
                left join
                                diap01.mec_us_united_20056.dfa2_placements      as p1
                                on t2.placement_id = p1.placement_id
                where
                                t2.campaign_id = 10742878
                                and t2.site_id_dcm in (1578478, 1239319)
                                and not regexp_like(p1.placement,'.*_PROS_FT.*','ib')
                                and cast(timestamp_trunc(to_timestamp(t2.event_time / 1000000),'SS') as date) between '2017-08-14' and '2017-12-31'
                                and t2.advertiser_id <> 0
                                and t2.user_id <> '0'

    );
commit;

-- -- // =================================================================================================================
drop table diap01.mec_us_united_20056.ual_overlap
create table diap01.mec_us_united_20056.ual_overlap
(
    user_id        varchar(50) not null,
    site_id_dcm    int         not null,
    placement_id int not null,
    impressiontime int         not null,
    imp_nbr        int         not null
);

insert into diap01.mec_us_united_20056.ual_overlap
(user_id,site_id_dcm,placement_id,impressiontime,imp_nbr)

(select
                                a.user_id,
                                a.site_id_dcm,
                                a.placement_id,
                                a.impressiontime,
                                a.imp_nbr
                from
                                (select *
                                from diap01.mec_us_united_20056.ual_overlap_rtg) as a,

                                (select *
                                    from diap01.mec_us_united_20056.ual_overlap_prs) as b

                                where a.user_id = b.user_id
                                and datediff('day',cast(timestamp_trunc(to_timestamp(b.impressiontime / 1000000),'SS') as date),cast(timestamp_trunc(to_timestamp(a.impressiontime / 1000000),'SS') as date))  < 8
    );
commit;

-- // =================================================================================================================
drop table diap01.mec_us_united_20056.ual_overlap_cnv
create table diap01.mec_us_united_20056.ual_overlap_cnv
(
    user_id          varchar(50) not null,
    interaction_time int         not null,
    rev              decimal     not null,
    tickets          int         not null,
    cvr_nbr          int         not null
);

insert into diap01.mec_us_united_20056.ual_overlap_cnv
(user_id,
 interaction_time,
 rev,
 tickets,
 cvr_nbr)

    (select
        a.user_id,
        a.interaction_time,
        a.rev,
        a.tickets,
        a.cvr_nbr
-- select sum(rev), sum(tickets)

    from
        (select
            user_id,
            interaction_time,
            (total_revenue * 1000000) / rates.exchange_rate as rev,
            total_conversions                               as tickets,
--                                 count(*) as cvr_nbr
            row_number() over ()                            as cvr_nbr
        from
            diap01.mec_us_united_20056.dfa2_activity as t1

            left join
            diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
            on upper(substring(other_data, (instr(other_data, 'u3=') + 3), 3)) = upper(rates.currency)
                and cast(timestamp_trunc(to_timestamp(interaction_time / 1000000), 'SS') as date) = rates.date

        where
                t1.activity_id = 978826
                and t1.total_revenue <> '0'
                and (length(isnull (t1.event_sub_type, '')) > 0)
                and cast(timestamp_trunc(to_timestamp(t1.interaction_time / 1000000), 'SS') as date) between '2017-08-14' and '2017-12-31'
                and t1.advertiser_id <> 0
                and not regexp_like(substring(t1.other_data, (instr(t1.other_data, 'u3=') + 3), 5), 'mil.*', 'ib')
--                     group by user_id, interaction_time

        ) as a,

        (select
            user_id,
            impressiontime
        from
            diap01.mec_us_united_20056.ual_overlap
        ) as b
    where
        a.user_id = b.user_id
        and a.interaction_time = b.impressiontime

    );
commit;



-- // =================================================================================================================
drop table diap01.mec_us_united_20056.ual_overlap_prs_cnv
create table diap01.mec_us_united_20056.ual_overlap_prs_cnv
(
    user_id          varchar(50) not null,
    interaction_time int         not null,
    rev              decimal     not null,
    tickets          int         not null,
    cvr_nbr          int         not null
);

insert into diap01.mec_us_united_20056.ual_overlap_prs_cnv
(user_id,
 interaction_time,
 rev,
 tickets,
 cvr_nbr)

    (select
        a.user_id,
        a.interaction_time,
        a.rev,
        a.tickets,
        a.cvr_nbr
-- select sum(rev), sum(tickets)

    from
        (select
            user_id,
            interaction_time,
            (total_revenue * 1000000) / rates.exchange_rate as rev,
            total_conversions                               as tickets,
--                                 count(*) as cvr_nbr
            row_number() over ()                            as cvr_nbr
        from
            diap01.mec_us_united_20056.dfa2_activity as t1

            left join
            diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
            on upper(substring(other_data, (instr(other_data, 'u3=') + 3), 3)) = upper(rates.currency)
                and cast(timestamp_trunc(to_timestamp(interaction_time / 1000000), 'SS') as date) = rates.date

        where
                t1.activity_id = 978826
                and t1.total_revenue <> '0'
                and (length(isnull (t1.event_sub_type, '')) > 0)
                and cast(timestamp_trunc(to_timestamp(t1.interaction_time / 1000000), 'SS') as date) between '2017-08-14' and '2017-12-31'
                and t1.advertiser_id <> 0
                and not regexp_like(substring(t1.other_data, (instr(t1.other_data, 'u3=') + 3), 5), 'mil.*', 'ib')
--                     group by user_id, interaction_time

        ) as a,

        (select
            user_id,
            impressiontime
        from
            diap01.mec_us_united_20056.ual_overlap_prs
        ) as b
    where
        a.user_id = b.user_id
        and a.interaction_time = b.impressiontime

    );
commit;

--
-- select * from diap01.mec_us_united_20056.ual_overlap_prs_cnv
--     limit 100


-- // =================================================================================================================
drop table diap01.mec_us_united_20056.ual_overlap_rtg_cnv
create table diap01.mec_us_united_20056.ual_overlap_rtg_cnv
(
    user_id          varchar(50) not null,
    interaction_time int         not null,
    rev              decimal     not null,
    tickets          int         not null,
    cvr_nbr          int         not null
);

insert into diap01.mec_us_united_20056.ual_overlap_rtg_cnv
(user_id,
 interaction_time,
 rev,
 tickets,
 cvr_nbr)

    (select
        a.user_id,
        a.interaction_time,
        a.rev,
        a.tickets,
        a.cvr_nbr
-- select sum(rev), sum(tickets)

    from
        (select
            user_id,
            interaction_time,
            (total_revenue * 1000000) / rates.exchange_rate as rev,
            total_conversions                               as tickets,
--                                 count(*) as cvr_nbr
            row_number() over ()                            as cvr_nbr
        from
            diap01.mec_us_united_20056.dfa2_activity as t1

            left join
            diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
            on upper(substring(other_data, (instr(other_data, 'u3=') + 3), 3)) = upper(rates.currency)
                and cast(timestamp_trunc(to_timestamp(interaction_time / 1000000), 'SS') as date) = rates.date

        where
                t1.activity_id = 978826
                and t1.total_revenue <> '0'
                and (length(isnull (t1.event_sub_type, '')) > 0)
                and cast(timestamp_trunc(to_timestamp(t1.interaction_time / 1000000), 'SS') as date) between '2017-08-14' and '2017-12-31'
                and t1.advertiser_id <> 0
                and not regexp_like(substring(t1.other_data, (instr(t1.other_data, 'u3=') + 3), 5), 'mil.*', 'ib')
--                     group by user_id, interaction_time

        ) as a,

        (select
            user_id,
            impressiontime
        from
            diap01.mec_us_united_20056.ual_overlap_rtg
        ) as b
    where
        a.user_id = b.user_id
        and a.interaction_time = b.impressiontime

    );
commit;

-- // =================================================================================================================
-- check user counts for each table
select count(distinct user_id) as users, count(distinct imp_nbr) as imps from diap01.mec_us_united_20056.ual_overlap_prs
select count(distinct user_id) as users, count(distinct imp_nbr) as imps from diap01.mec_us_united_20056.ual_overlap_rtg
select count(distinct user_id) as users, count(distinct imp_nbr) as imps from diap01.mec_us_united_20056.ual_overlap

select count(distinct user_id) as users, count(distinct cvr_nbr) as con, sum(tickets) as tickets, sum(rev) as rev from diap01.mec_us_united_20056.ual_overlap_prs_cnv
select count(distinct user_id) as users, count(distinct cvr_nbr) as con, sum(tickets) as tickets, sum(rev) as rev from diap01.mec_us_united_20056.ual_overlap_rtg_cnv
select count(distinct user_id) as users, count(distinct cvr_nbr) as con, sum(tickets) as tickets, sum(rev) as rev from diap01.mec_us_united_20056.ual_overlap_cnv



-- // =================================================================================================================

select placement_id, site_id_dcm, count(distinct imp_nbr) as imps from diap01.mec_us_united_20056.ual_overlap_prs group by placement_id, site_id_dcm
select placement_id, site_id_dcm, count(distinct imp_nbr) as imps from diap01.mec_us_united_20056.ual_overlap_rtg group by placement_id, site_id_dcm
select placement_id, site_id_dcm, count(distinct imp_nbr) as imps from diap01.mec_us_united_20056.ual_overlap group by placement_id, site_id_dcm