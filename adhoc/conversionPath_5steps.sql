-- Pathing, Five steps

select
t11.path,
sum(t11.path_rank)
--  sum(t11.awesome)

from (
select
  t10.user_id,
  t10.path,
  row_number() over (partition by t10.path, t10.user_id order by t10.path  ) as path_rank
--  count(*) as awesome

from
( select
t9.user_id,
t9.path_1 || '->' || t9.path_2 || '->' || t9.path_3 || '->' || t9.path_4 || '->' || t9.path_5 as path

from ( select
t8.user_id,
case when t8.path_1 = 1 then 'Retargeting' else 'Prospecting' end as path_1,
case when t8.path_2 = 1 then 'Retargeting' else 'Prospecting' end as path_2,
case when t8.path_3 = 1 then 'Retargeting' else 'Prospecting' end as path_3,
case when t8.path_4 = 1 then 'Retargeting' else 'Prospecting' end as path_4,
case when t8.path_5 = 1 then 'Retargeting' else 'Prospecting' end as path_5

from (

select
t7.user_id,
sum (t7.path_1) as path_1,
sum (t7.path_2) as path_2,
sum (t7.path_3) as path_3,
sum (t7.path_4) as path_4,
sum (t7.path_5) as path_5

from (
select
t6.user_id,
--     t6.tp_rank,
--     t6.site_cat

case when t6.path_1 = 'Prospecting' then 2 when t6.path_1 = 'Retargeting' then 1 else 0 end as path_1,
case when t6.path_2 = 'Prospecting' then 2 when t6.path_2 = 'Retargeting' then 1 else 0 end as path_2,
case when t6.path_3 = 'Prospecting' then 2 when t6.path_3 = 'Retargeting' then 1 else 0 end as path_3,
case when t6.path_4 = 'Prospecting' then 2 when t6.path_4 = 'Retargeting' then 1 else 0 end as path_4,
case when t6.path_5 = 'Prospecting' then 2 when t6.path_5 = 'Retargeting' then 1 else 0 end as path_5
--     t6.path_1 || t6.path_2 || t6.path_3 || t6.path_4 || t6.path_5 as path
-- t6.path_1|| t6.path_2|| t6.path_3|| t6.path_4|| t6.path_5 as path

from (
select
--     t5.date,
--     t5.site_dcm,
t5.user_id,
t5.tp_rank,
t5.site_cat,
--  lag(f1.flatcost,1,0) over (partition by f1.Cost_ID order by f1.dcmDate) as ff,
case when t5.tp_rank = 5 then t5.site_cat else 'zzz' end as path_1,
case when t5.tp_rank = 4 then t5.site_cat else 'zzz' end as path_2,
case when t5.tp_rank = 3 then t5.site_cat else 'zzz' end as path_3,
case when t5.tp_rank = 2 then t5.site_cat else 'zzz' end as path_4,
case when t5.tp_rank = 1 then t5.site_cat else 'zzz' end as path_5
--     t5.site_id,
--     count(t5.user_cnt) as user_cnt,
--     sum(t5.user_cnt5)  as user_cnt5,
--     sum(t5.imp)   as imp


from (
select
t4.date,
--         t4.site_dcm,
--         t4.site_id,
--         t4.placement,
--         t4.page_id,
t4.site_cat,
t4.user_id,
--         row_number() over (partition by t4.user_id order by t4.user_id) as user_cnt,
--         row_number() over (partition by t4.user_id, t4.site_cat  order by t4.date desc) as tp_rank,
t4.tp_rank,
t4.imps,
t4.clks


from (
select
t3.date as "date",
--                  case when t3.site_id = 1578478 then 'Google' else j1.site_dcm end as site_dcm,
--                  case when t3.site_id = 1578478 then 'Retargeting' else 'Prospecting' end as site_cat,
--                  t3.site_id                                                        as site_id,
--                     j2.placement as placement,
t3.site_cat as site_cat,
--                  t3.page_id as page_id,
t3.user_id as user_id,
--                  row_number() over (partition by t3.user_id, t3.site_cat  order by t3.date desc) as tp_rank,
row_number() over (partition by t3.user_id order by t3.date desc ) as tp_rank,
sum (t3.impressions) as imps,
sum (t3.clicks) as clks
--                       sum(case when t3.user_cnt3 = 1 then t3.user_cnt3 else 0 end)                     as user_cnt3
from
(
select
cast (impression_time as date ) as "date",
count (*) as impressions,
0 as clicks,
t1.user_id as user_id,
t1.site_id as site_id,
case when t1.site_id = 1578478 then 'Retargeting' else 'Prospecting' end as site_cat,
t1.order_id as order_id,
t1.page_id as page_id

from ( select *
from diap01.mec_us_united_20056.dfa_impression

where cast (impression_time as date ) between '2016-01-01' and '2016-12-31'
and order_id = 9639387
--                             and site_id = 1578478
and advertiser_id <> 0
and user_id <> '0'
) as t1

group by
cast (t1.impression_time as date ),
t1.user_id,
t1.page_id,
t1.order_id,
t1.site_id

union all

select
cast (t2.click_time as date ) as "date",
0 as impressions,
count (*) as clicks,

t2.user_id as user_id,
t2.site_id as site_id,
case when t2.site_id = 1578478 then 'Retargeting' else 'Prospecting' end as site_cat,
t2.order_id as order_id,
t2.page_id as page_id
from ( select *
from diap01.mec_us_united_20056.dfa_click

where cast (click_time as date ) between '2016-01-01' and '2016-12-31'
and order_id = 9639387
--                             and site_id = 1578478
and advertiser_id <> 0
and user_id <> '0'
) as t2

group by
cast (t2.click_time as date ),
t2.user_id,
t2.page_id,
t2.order_id,
t2.site_id

) as t3

left join
(
select
cast (site as varchar (4000)) as 'site_dcm',
site_id as 'site_id_dcm'
from diap01.mec_us_united_20056.dfa_site
) as j1
on t3.site_id = j1.site_id_dcm


left join
(
select cast (t1.placement as varchar (4000)) as placement,t1.page_id as page_id,t1.order_id as order_id,t1.site_id as site_id

from ( select
order_id as order_id,
site_id as site_id,
page_id as page_id,
site_placement as placement,
cast (start_date as date ) as thisdate,
row_number() over (partition by order_id,site_id,page_id order by cast (start_date as date ) desc ) as r1
from diap01.mec_us_united_20056.dfa_page

) as t1
where r1 = 1
) as j2
on t3.page_id  = j2.page_id
and t3.order_id = j2.order_id
and t3.site_id  = j2.site_id

where t3.user_id in (
select user_id
from diap01.mec_us_united_20056.dfa_activity as t4
where
cast (activity_time as date ) between '2016-01-01' and '2016-12-31'
and advertiser_id <> 0
--  Google
--                        and site_id <> 1578478
and revenue <> 0
and quantity <> 0
and (activity_type = 'ticke498')
and (activity_sub_type = 'unite820')
and advertiser_id <> 0
and order_id = 9639387
and user_id <> '0'
and not regexp_like( substring (other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
and t3.user_id = t4.user_id
--                                     and (
-- --                       Retargeting conversion within 7 days of prospecting conversion
--                       and datediff('dd', cast(a.activity_time as date), cast(t4.activity_time as date)) < 8
  and datediff('dd', cast(t3.date as date), cast(t4.activity_time as date)) < 8
-- and cast (t3.date as date ) < cast (t4.click_time as date )
-- --                       or
-- --                       prospecting conversion within 7 days of Retargeting conversion
-- --                       datediff('dd', cast(t99.click_time as date), cast(a.click_time as date)) < 8
--                   )

)
group by
t3.date,
t3.site_cat,
--                  t3.site_id,
--                  j1.site_dcm,
--                  t3.page_id,
--                  j2.placement,
t3.user_id
) as t4
--     where t4.site_cat = 'Retargeting'
where t4.tp_rank < 6
-- and t4.user_id = 'AMsySZY--0U1zvHjRwtDIE3i-rEI'
) as t5

group by
t5.user_id,
t5.tp_rank,
t5.site_cat

) as t6

--
-- group by
--         t6.user_id,
--     t6.path_1,
--     t6.path_2,
--     t6.path_3,
--     t6.path_4,
--     t6.path_5
) as t7
group by
t7.user_id
) as t8
) as t9
) as t10
)as t11
group by t11.path