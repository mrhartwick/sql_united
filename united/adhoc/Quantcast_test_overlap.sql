----Quantcast Prospecting Test 2018: To check the overlap between the control and test group.

select
                case when b.user_id is not null then 1 else 0 end as Control_flag,
                case when f.user_id is not null then 1 else 0 end as Test_flag,
                sum(1) as user_cnt,
                coalesce(sum(Control_cnt),0) as Control_imps,
                coalesce(sum(Test_cnt),0) as Test_imps
from
                (select
                                distinct user_id
                from
                                diap01.mec_us_united_20056.dfa2_impression
                where
                                user_id <> '0'
                                and advertiser_id <> '0'
                                and cast(to_timestamp(event_time / 1000000) as date) between '2018-03-07' and '2018-03-14'
                                and campaign_id = 20606595
                                and site_id_dcm = 3267410
                                and placement_id in (215129150, 215144749, 215145031, 215145553, 215145556, 215222640, 215222910, 215225244, 215225256,
                                211543190, 211593748, 211594354, 211602330, 211605807, 211609899, 211609902, 211609905, 211610649))  a

Left Outer Join
                (select
                                user_id,
                                sum(1) as Control_cnt
                from
                               diap01.mec_us_united_20056.dfa2_impression
                where
                                campaign_id = 20606595
                                and site_id_dcm = 3267410
                                and placement_id in (215129150, 215144749, 215145031, 215145553, 215145556, 215222640, 215222910, 215225244, 215225256)
                                and user_id <> '0'
                                and advertiser_id <> '0'
                                and cast(to_timestamp(event_time / 1000000) as date) between '2018-03-07' and '2018-03-14'
                group by
                                1) b On a.user_id = b.user_id
Left Outer Join
                (select
                                user_id,
                                sum(1) as Test_cnt
                from
                               diap01.mec_us_united_20056.dfa2_impression
                where
                                campaign_id = 20606595
                                and site_id_dcm = 3267410
                                and placement_id in (211543190, 211593748, 211594354, 211602330, 211605807, 211609899, 211609902, 211609905, 211610649)
                                and user_id <> '0'
                                and advertiser_id <> '0'
                                and cast(to_timestamp(event_time / 1000000) as date) between '2018-03-07' and '2018-03-14'




                group by
                                1) f On a.user_id = f.user_id


group by
                1,2
order by
                1,2;


------------------------------------------------------------------------------------------------------------------------------------------------------------

--To double check on the overlap numbers above:


select count(distinct user_id)
from diap01.mec_us_united_20056.dfa2_impression
where user_id in (select users from diap01.mec_us_united_20056.ual_qc_control)
and user_id in (select users from diap01.mec_us_united_20056.ual_qc_test)
and user_id <> '0'
and cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-03-07' and '2018-03-14'
and advertiser_id <> 0
and campaign_id =  20606595
and site_id_dcm =  3267410
