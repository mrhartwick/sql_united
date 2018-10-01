select
                case when b.user_id is not null then 1 else 0 end as site_a_flag,
                case when f.user_id is not null then 1 else 0 end as site_b_flag,
                case when d.user_id is not null then 1 else 0 end as site_f_flag,
                case when e.user_id is not null then 1 else 0 end as site_d_flag,
                sum(1) as user_cnt,
                coalesce(sum(site_Ada_cnt),0) as site_Ada_imps,
                coalesce(sum(site_Amob_cnt),0) as site_Amob_imps,
                coalesce(sum(site_Quan_cnt),0) as site_Quan_imps,
                coalesce(sum(site_Soj_cnt),0) as site_Soj_imps
from
                (select
                                distinct user_id
                from
                                diap01.mec_us_united_20056.dfa2_impression
                where
                                user_id <> '0'
                                and advertiser_id <> '0'
                                and cast(to_timestamp(event_time / 1000000) as date) between '2017-08-14' and '2017-09-25'
                                and campaign_id = 10742878
                                and site_id_dcm in (1190273,1853562,3267410,1239319))  a

Left Outer Join
                (select
                                user_id,
                                sum(1) as site_Ada_cnt
                from
                               diap01.mec_us_united_20056.dfa2_impression
                where
                                campaign_id = 10742878
                        ---Adara
                                and site_id_dcm = 1190273
                                and user_id <> '0'
                                and advertiser_id <> '0'
                                and cast(to_timestamp(event_time / 1000000) as date) between '2017-08-14' and '2017-09-25'
                group by
                                1) b On a.user_id = b.user_id
Left Outer Join
                (select
                                user_id,
                                sum(1) as site_Amob_cnt
                from
                               diap01.mec_us_united_20056.dfa2_impression
                where
                                campaign_id = 10742878
                      ---Amobee
                                and site_id_dcm = 1853562
                                and user_id <> '0'
                                and advertiser_id <> '0'
                                and cast(to_timestamp(event_time / 1000000) as date) between '2017-08-14' and '2017-09-25'




                group by
                                1) f On a.user_id = f.user_id
Left Outer Join
                (select
                                user_id,
                                sum(1) as site_Quan_cnt
                from
                               diap01.mec_us_united_20056.dfa2_impression
                where
                                campaign_id = 10742878
                        ----Quantcast
                                and site_id_dcm = 3267410
                                and user_id <> '0'
                                and advertiser_id <> '0'
                                and cast(to_timestamp(event_time / 1000000) as date) between '2017-08-14' and '2017-09-25'
                group by
                                1) d On a.user_id=d.user_id
Left Outer Join
                (select
                                user_id,
                                sum(1) as site_Soj_cnt
                from
                               diap01.mec_us_united_20056.dfa2_impression
                where
                                campaign_id = 10742878
                        ----Sojern
                                and site_id_dcm = 1239319
                                and user_id <> '0'
                                and advertiser_id <> '0'
                                and cast(to_timestamp(event_time / 1000000) as date) between '2017-08-14' and '2017-09-25'
                group by
                                1) e On a.user_id = e.user_id




group by
                1,2,3,4
order by
                1,2,3,4;
