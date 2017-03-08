create table diap01.mec_us_united_20056.ual_freq_imps
(
    user_id        varchar(1000)  not null,
    imps           int          not null
);

insert into diap01.mec_us_united_20056.ual_freq_imps
(user_id,imps)

(select
--     s1.site_dcm,
--     cast(to_timestamp(i1.event_time / 1000000) as date) as "imp_date",
    i1.user_id as user_id,
    count(*)                                            as imps
from diap01.mec_us_united_20056.dfa2_impression as i1
--     left join diap01.mec_us_united_20056.dfa2_sites as s1
--         on i1.site_id_dcm = s1.site_id_dcm

where cast(to_timestamp(i1.event_time / 1000000) as date) between '2016-07-15' and '2016-12-31'
    and i1.advertiser_id <> 0
--                    and regexp_like(s1.site_dcm,'.*Google.*','ib')
-- Google only
    and i1.site_id_dcm = 1578478
-- TMK only
    and i1.campaign_id = 9639387
    and i1.user_id <> '0'
group by
    i1.user_id);
--     cast(to_timestamp(i1.event_time/1000000) as date ),
-- s1.site_dcm);
commit;