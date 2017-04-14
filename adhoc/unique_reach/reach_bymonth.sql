
create table diap01.mec_us_united_20056.ual_temp_reach_det_2
(
    user_id      varchar(50) not null,
    campaign     varchar(6000) not null,
    site_dcm     varchar(6000) not null,
    min_imp_time timestamp   not null,
    min_yr       int         not null,
    min_mo       int         not null,
    jan_17_imps  int         not null,
    feb_17_imps  int         not null,
    mar_17_imps  int         not null
);

insert into diap01.mec_us_united_20056.ual_temp_reach_det_2
(user_id, campaign, site_dcm, min_imp_time, min_yr, min_mo, jan_17_imps, feb_17_imps, mar_17_imps)

    (


select
                t2.user_id,
                t2.campaign,
                t2.site_dcm,
                min(to_timestamp(event_time / 1000000))                                                                                                   as min_imp_time,
                date_part('year',  min(to_timestamp(event_time / 1000000)))                                                                                 as min_yr,
                date_part('month', min(to_timestamp(event_time / 1000000)))                                                                                as min_mo,
                sum(case when date_part('year', to_timestamp(event_time / 1000000)) = '2017' and date_part('month', to_timestamp(event_time / 1000000)) = 1 then 1 else 0 end) as jan_17_imps,
                sum(case when date_part('year', to_timestamp(event_time / 1000000)) = '2017' and date_part('month', to_timestamp(event_time / 1000000)) = 2 then 1 else 0 end) as feb_17_imps,
                sum(case when date_part('year', to_timestamp(event_time / 1000000)) = '2017' and date_part('month', to_timestamp(event_time / 1000000)) = 3 then 1 else 0 end) as mar_17_imps
from

        (
            select  t0.user_id,
                    t0.event_time,
                    c1.campaign,
                    s1.site_dcm
            from diap01.mec_us_united_20056.dfa2_impression as t0
            left join
            (select
                c0.campaign,
                c0.campaign_id
            from diap01.mec_us_united_20056.dfa2_campaigns c0
            ) as c1
            on t0.campaign_id = c1.campaign_id

            left join
            (select
                p1.placement,
                p1.placement_id,
                p1.campaign_id,
                p1.site_id_dcm
            from (select
                campaign_id,
                site_id_dcm,
                placement_id,
                placement,
                cast(placement_start_date as date)                     as thisdate,
                row_number() over ( partition by campaign_id,site_id_dcm,placement_id
                    order by cast(placement_start_date as date) desc ) as x1
            from diap01.mec_us_united_20056.dfa2_placements
                 ) as p1
            where x1 = 1
            ) as p1
            on t0.placement_id = p1.placement_id
                and t0.campaign_id = p1.campaign_id
                and t0.site_id_dcm = p1.site_id_dcm

            left join
            (select distinct
                s0.site_dcm,
                s0.site_id_dcm
            from diap01.mec_us_united_20056.dfa2_sites s0
            ) as s1
            on t0.site_id_dcm = s1.site_id_dcm

        where
            not regexp_like(p1.placement,'.?do\s?not\s?use.?','ib')
             and regexp_like(c1.campaign,'.*2017.*','ib')
            and not regexp_like(c1.campaign,'.*Search.*','ib')
            and not regexp_like(c1.campaign,'.*BidManager.*','ib')
            and user_id <> '0'
            and advertiser_id <> 0
            and cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-12-01' and '2017-03-31'
--      ) as t1
    ) as t2



group by
                t2.user_id,
                t2.campaign,
                t2.site_dcm
    );
commit;
-- ====================================================================================================================

select
                campaign,
                site_dcm,
                sum(case when min_yr = 2017 and min_mo = 1 then 1 else 0 end)           as jan_new_users,
                sum(case when min_yr = 2017 and min_mo = 1 then jan_17_imps else 0 end) as jan_new_user_imps,
                sum(case when jan_17_imps > 0 then 1 else 0 end)                        as jan_all_users,
                sum(jan_17_imps)                                                        as jan_17_imps,
                sum(case when min_yr = 2017 and min_mo = 2 then 1 else 0 end)           as feb_new_users,
                sum(case when min_yr = 2017 and min_mo = 2 then feb_17_imps else 0 end) as feb_new_user_imps,
                sum(case when feb_17_imps > 0 then 1 else 0 end)                        as feb_all_users,
                sum(feb_17_imps)                                                        as feb_17_imps,
                sum(case when min_yr = 2017 and min_mo = 3 then 1 else 0 end)           as mar_new_users,
                sum(case when min_yr = 2017 and min_mo = 3 then mar_17_imps else 0 end) as mar_new_user_imps,
                sum(case when mar_17_imps > 0 then 1 else 0 end)                        as mar_all_users,
                sum(mar_17_imps)                                                        as mar_17_imps
from
                diap01.mec_us_united_20056.ual_temp_reach_det_2
group by campaign, site_dcm
;
