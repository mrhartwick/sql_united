drop table if exists mec_us_united_20056.dpe_tmp_reach_01;

create table mec_us_united_20056.dpe_tmp_reach_01 as
select
                a.user_id,
                a.campaign_id,
                b.campaign,
                a.site_id_dcm,
                c.site_dcm,
                min(md_event_time) as min_time,
                date_part('year', min(to_timestamp(event_time / 1000000))) as min_yr,
                date_part('month',min(to_timestamp(event_time / 1000000))) as min_mo,
                sum(case when date_part('year', to_timestamp(event_time / 1000000)) = 2017
                         and  date_part('month',to_timestamp(event_time / 1000000)) = 1 then 1 else 0 end) as jan_17_imps,
                sum(case when date_part('year', to_timestamp(event_time / 1000000)) = 2017
                         and  date_part('month',to_timestamp(event_time / 1000000)) = 2 then 1 else 0 end) as feb_17_imps,
                sum(case when date_part('year', to_timestamp(event_time / 1000000)) = 2017
                         and  date_part('month',to_timestamp(event_time / 1000000)) = 3 then 1 else 0 end) as mar_17_imps
from
                mec_us_united_20056.dfa2_impression a
Left Outer Join
                mec_us_united_20056.dfa2_campaigns b
                on a.advertiser_id = b.advertiser_id and a.campaign_id = b.campaign_id
Left Outer Join
                mec_us_united_20056.dfa2_sites c
                on a.site_id_dcm = c.site_id_dcm

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
            on a.placement_id = p1.placement_id
                and a.campaign_id = p1.campaign_id
                and a.site_id_dcm = p1.site_id_dcm

        where
            not regexp_like(p1.placement,'.?do\s?not\s?use.?','ib')
             and regexp_like(b.campaign,'.*2017.*','ib')
            and not regexp_like(b.campaign,'.*Search.*','ib')
            and not regexp_like(b.campaign,'.*BidManager.*','ib')
            and user_id <> '0'
            and a.advertiser_id <> 0
            and cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2016-12-01' and '2017-03-31'
group by
                a.user_id,
                a.campaign_id,
                b.campaign,
                a.site_id_dcm,
                c.site_dcm;


select
--                 campaign_id,
--                 campaign,
--                 site_id_dcm,
--                 site_dcm,
                sum(case when min_yr = 2017 and min_mo = 1 then 1 else 0 end) jan_new_users,
                sum(case when min_yr = 2017 and min_mo = 1 then jan_17_imps else 0 end) jan_new_user_imps,
                sum(case when jan_17_imps > 0 then 1 else 0 end) as jan_all_users,
                sum(jan_17_imps) as jan_17_imps,
                sum(case when min_yr = 2017 and min_mo = 2 then 1 else 0 end) feb_new_users,
                sum(case when min_yr = 2017 and min_mo = 2 then feb_17_imps else 0 end) feb_new_user_imps,
                sum(case when feb_17_imps > 0 then 1 else 0 end) as feb_all_users,
                sum(feb_17_imps) as feb_17_imps,
                sum(case when min_yr = 2017 and min_mo = 3 then 1 else 0 end) mar_new_users,
                sum(case when min_yr = 2017 and min_mo = 3 then mar_17_imps else 0 end) mar_new_user_imps,
                sum(case when mar_17_imps > 0 then 1 else 0 end) as mar_all_users,
                sum(mar_17_imps) as mar_17_imps
from
                mec_us_united_20056.dpe_tmp_reach_01
-- group by
--                 1,2,3,4;
--



select
                count(1)
from
                mec_us_united_20056.dfa2_impression a
Left Outer Join
                mec_us_united_20056.dfa2_campaigns b on a.advertiser_id = b.advertiser_id and a.campaign_id = b.campaign_id
Left Outer Join
                mec_us_united_20056.dfa2_sites c on a.site_id_dcm = c.site_id_dcm
where
                user_id! = '0' and
                md_event_time::date between '1/1/2017' and '3/31/2017';




