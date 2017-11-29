
-- route cost: combined activity and impression
-- drop table diap01.mec_us_united_20056.ual_route_cost
create table diap01.mec_us_united_20056.ual_route_cost
(
    user_id      varchar(50),
    con_tim      int,
    imp_tim      int,
    campaign_id  int,
    site_id_dcm  int,
    placement_id int,
    rt_1_orig    varchar(3),
    rt_1_dest    varchar(3),
    rt_2_orig    varchar(3),
    rt_2_dest    varchar(3),
    imp_nbr      int,
    clk_led      int,
    vew_led      int,
    clk_con      int,
    clk_tix      int,
    clk_rev      decimal(20,10),
    vew_con      int,
    vew_tix      int,
    vew_rev      decimal(20,10),
    rev          decimal(20,10)
);

insert into  diap01.mec_us_united_20056.ual_route_cost
(user_id,
con_tim,
imp_tim,
campaign_id,
site_id_dcm,
placement_id,
rt_1_orig,
rt_1_dest,
rt_2_orig,
rt_2_dest,
imp_nbr,
clk_led,
vew_led,
clk_con,
clk_tix,
clk_rev,
vew_con,
vew_tix,
vew_rev,
rev)

    (select
    ta.user_id
    ,ta.con_tim as con_tim
    ,ti.imp_tim as imp_tim
-- cast(timestamp_trunc(to_timestamp(ta.event_time / 1000000),'SS') as date) as con_dte
,ta.campaign_id
,ta.site_id_dcm
,ta.placement_id
,ta.rt_1_orig
,ta.rt_1_dest
,ta.rt_2_orig
,ta.rt_2_dest
-- ,count(distinct ti.imp_nbr) as impressions
-- ,sum(clk_led) as clk_led
-- ,sum(vew_led) as vew_led
-- ,sum(clk_con) as clk_con
-- ,sum(clk_tix) as clk_tix
-- ,sum(clk_rev) as clk_rev
-- ,sum(vew_con) as vew_con
-- ,sum(vew_tix) as vew_tix
-- ,sum(vew_rev) as vew_rev
-- ,sum(rev) as rev

,ti.imp_nbr
,ta.clk_led
,ta.vew_led
,ta.clk_con
,ta.clk_tix
,ta.clk_rev
,ta.vew_con
,ta.vew_tix
,ta.vew_rev
,ta.rev
from
(
select
user_id,
interaction_time as con_tim,
--  to_timestamp(event_time / 1000000) as con_tim,
    campaign_id,
    site_id_dcm,
    placement_id,
case when activity_id = 1086066 and (conversion_id = 1) then 1 else 0 end as clk_led,
case when activity_id = 1086066 and (conversion_id = 2) then 1 else 0 end as vew_led,
case when activity_id = 978826 and conversion_id = 1 and total_revenue <> 0 then 1 else 0 end as clk_con,
case when activity_id = 978826 and conversion_id = 1 and total_revenue <> 0 then total_conversions else 0 end as clk_tix,
case when conversion_id = 1 then (total_revenue * 1000000)/rates.exchange_rate else 0 end as clk_rev,
case when activity_id = 978826 and conversion_id = 2 and total_revenue <> 0 then 1 else 0 end as vew_con,
case when activity_id = 978826 and conversion_id = 2 and total_revenue <> 0 then total_conversions else 0 end as vew_tix,
case when conversion_id = 2 then (total_revenue * 1000000)/rates.exchange_rate else 0 end as vew_rev,
        (total_revenue * 1000000)/rates.exchange_rate as rev,
case when regexp_like(other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
when regexp_like(other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_1_orig,
case when regexp_like(other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
when regexp_like(other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_1_dest,

case when regexp_like(other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
when regexp_like(other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_2_orig,
case when regexp_like(other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
when regexp_like(other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_2_dest
--  rates.EXCHANGE_RATE

from diap01.mec_us_united_20056.dfa2_activity a

left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
on upper(substring(a.other_data,(instr(a.other_data,'u3=') + 3),3)) = upper(rates.currency)
and cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date) = rates.date

where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2017-08-15' and '2017-10-31'
and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
and (activity_id = 978826 or activity_id = 1086066)
and campaign_id = 10742878 -- display 2017
and (advertiser_id <> 0)
and (length(isnull (event_sub_type,'')) > 0)
    and site_id_dcm in (1190273,1239319)
) as ta,
--  limit 100


(
select
    user_id,
    event_time as imp_tim,
--  to_timestamp(event_time/1000000) as imp_tim,
    row_number() over() as imp_nbr
from diap01.mec_us_united_20056.dfa2_impression
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2017-08-15' and '2017-10-31'
and campaign_id = 10742878 -- display 2017
and (advertiser_id <> 0)
    and site_id_dcm in (1190273,1239319)
) as ti

where
    ta.user_id = ti.user_id and
--  datediff('day',to_timestamp(ti.event_time/1000000),to_timestamp(ta.event_time/1000000)) <= 7
imp_tim = con_tim);
commit;


group by
cast(timestamp_trunc(to_timestamp(ta.event_time / 1000000),'SS') as date)
,ta.campaign_id
,ta.site_id_dcm
,ta.placement_id
,ta.rt_1_orig
,ta.rt_1_dest
,ta.rt_2_orig
,ta.rt_2_dest




-- ) as r1

left join
(
select
cast(campaign as varchar(4000)) as 'campaign',campaign_id as 'campaign_id'
from diap01.mec_us_united_20056.dfa2_campaigns
) as campaign
on t1.campaign_id = campaign.campaign_id

left join
(
select
cast(p1.placement as varchar(4000)) as 'placement',p1.placement_id as 'placement_id',p1.campaign_id as 'campaign_id',p1.site_id_dcm as 'site_id_dcm'

from (select
campaign_id as campaign_id,site_id_dcm as site_id_dcm,placement_id as placement_id,placement as placement,cast(placement_start_date as date) as thisdate,
row_number() over ( partition by campaign_id,site_id_dcm,placement_id
order by cast(placement_start_date as date) desc ) as x1
from diap01.mec_us_united_20056.dfa2_placements

) as p1
where x1 = 1
) as p1
on t1.placement_id = p1.placement_id
and t1.campaign_id = p1.campaign_id
and t1.site_id_dcm = p1.site_id_dcm

left join
(
select
cast(site_dcm as varchar(4000)) as 'site_dcm',site_id_dcm as 'site_id_dcm'
from diap01.mec_us_united_20056.dfa2_sites
) as directory
on t1.site_id_dcm = directory.site_id_dcm

where  regexp_like(p1.placement,'.*GEN_INT_PROS_FT.*','ib') or regexp_like(p1.placement,'.*GEN_DOM_PROS_FT.*','ib')

-- ==============================================================================================================
-- ==============================================================================================================
-- ==============================================================================================================

-- Separate tables - not used
-- route cost: activity
create table diap01.mec_us_united_20056.ual_route_cost_con
(
    user_id      varchar(50),
    con_tim      int,
--  imp_tim      int,
    campaign_id  int,
    site_id_dcm  int,
    placement_id int,
    rt_1_orig    varchar(3),
    rt_1_dest    varchar(3),
    rt_2_orig    varchar(3),
    rt_2_dest    varchar(3),
--  imp_nbr      int,
    clk_led      int,
    vew_led      int,
    clk_con      int,
    clk_tix      int,
    clk_rev      int,
    vew_con      int,
    vew_tix      int,
    vew_rev      int,
    rev          int,
    currency varchar(3)
);

insert into  diap01.mec_us_united_20056.ual_route_cost_con
(user_id,
con_tim,
-- imp_tim,
campaign_id,
site_id_dcm,
placement_id,
rt_1_orig,
rt_1_dest,
rt_2_orig,
rt_2_dest,
-- imp_nbr,
clk_led,
vew_led,
clk_con,
clk_tix,
clk_rev,
vew_con,
vew_tix,
vew_rev,
rev,
currency)

    (select
    ta.user_id
    ,ta.con_tim as con_tim
--  ,ti.imp_tim as imp_tim
-- cast(timestamp_trunc(to_timestamp(ta.event_time / 1000000),'SS') as date) as con_dte
,ta.campaign_id
,ta.site_id_dcm
,ta.placement_id
,ta.rt_1_orig
,ta.rt_1_dest
,ta.rt_2_orig
,ta.rt_2_dest
-- ,count(distinct ti.imp_nbr) as impressions
-- ,sum(clk_led) as clk_led
-- ,sum(vew_led) as vew_led
-- ,sum(clk_con) as clk_con
-- ,sum(clk_tix) as clk_tix
-- ,sum(clk_rev) as clk_rev
-- ,sum(vew_con) as vew_con
-- ,sum(vew_tix) as vew_tix
-- ,sum(vew_rev) as vew_rev
-- ,sum(rev) as rev

-- ,ti.imp_nbr
,ta.clk_led
,ta.vew_led
,ta.clk_con
,ta.clk_tix
,ta.clk_rev
,ta.vew_con
,ta.vew_tix
,ta.vew_rev
,ta.rev
    ,ta.currency
from
(
select
user_id,
interaction_time as con_tim,
--  to_timestamp(event_time / 1000000) as con_tim,
    campaign_id,
    site_id_dcm,
    placement_id,
case when activity_id = 1086066 and (conversion_id = 1) then 1 else 0 end as clk_led,
case when activity_id = 1086066 and (conversion_id = 2) then 1 else 0 end as vew_led,
case when activity_id = 978826 and conversion_id = 1 and total_revenue <> 0 then 1 else 0 end as clk_con,
case when activity_id = 978826 and conversion_id = 1 and total_revenue <> 0 then total_conversions else 0 end as clk_tix,
case when conversion_id = 1 then total_revenue else 0 end as clk_rev,
case when activity_id = 978826 and conversion_id = 2 and total_revenue <> 0 then 1 else 0 end as vew_con,
case when activity_id = 978826 and conversion_id = 2 and total_revenue <> 0 then total_conversions else 0 end as vew_tix,
case when conversion_id = 2 then total_revenue else 0 end as vew_rev,
total_revenue as rev,
case when regexp_like(other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(other_data,'(u5=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
when regexp_like(other_data,'(u5=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(other_data,'(u5=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_1_orig,
case when regexp_like(other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(other_data,'(u7=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
when regexp_like(other_data,'(u7=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(other_data,'(u7=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_1_dest,

case when regexp_like(other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(other_data,'(u6=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
when regexp_like(other_data,'(u6=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(other_data,'(u6=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_2_orig,
case when regexp_like(other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])','ib') then regexp_substr(other_data,'(u8=)(.+?)\(([A-Z][A-Z][A-Z])',1,1,'ib',3)
when regexp_like(other_data,'(u8=)([A-Z][A-Z][A-Z])\;','ib') then regexp_substr(other_data,'(u8=)([A-Z][A-Z][A-Z])\;',1,1,'ib',2) end as rt_2_dest,
        upper(substring(a.other_data,(instr(a.other_data,'u3=') + 3),3)) as currency
--  rates.EXCHANGE_RATE

from diap01.mec_us_united_20056.dfa2_activity a

-- left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
-- on upper(substring(a.other_data,(instr(a.other_data,'u3=') + 3),3)) = upper(rates.currency)
-- and cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date) = rates.date

where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2017-08-15' and '2017-08-31'
and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
and (activity_id = 978826 or activity_id = 1086066)
and campaign_id = 10742878 -- display 2017
and (advertiser_id <> 0)
and (length(isnull (event_sub_type,'')) > 0)
    and site_id_dcm in (1190273,1239319)
) as ta);
--  limit 100

--
-- (
-- select
--  user_id,
--  event_time as imp_tim,
-- --   to_timestamp(event_time/1000000) as imp_tim,
--  row_number() over() as imp_nbr
-- from diap01.mec_us_united_20056.dfa2_impression
-- where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2017-08-15' and '2017-08-31'
-- and campaign_id = 10742878 -- display 2017
-- and (advertiser_id <> 0)
-- ) as ti
--
-- where
--  ta.user_id = ti.user_id and
-- --   datediff('day',to_timestamp(ti.event_time/1000000),to_timestamp(ta.event_time/1000000)) <= 7
-- imp_tim = con_tim);
commit;

-- ==================================================================================================================
-- route cost: impression

create table diap01.mec_us_united_20056.ual_route_cost_imp
(
    user_id      varchar(50),
    imp_tim      int,
    imp_nbr int
);

insert into  diap01.mec_us_united_20056.ual_route_cost_imp
(user_id,
imp_tim,
-- campaign_id,
-- site_id_dcm,
-- placement_id,
-- rt_1_orig,
-- rt_1_dest,
-- rt_2_orig,
-- rt_2_dest,
-- -- imp_nbr,
-- clk_led,
-- vew_led,
-- clk_con,
-- clk_tix,
-- clk_rev,
-- vew_con,
-- vew_tix,
-- vew_rev,
-- rev
imp_nbr
)

--
(
select
    user_id,
    event_time as imp_tim,
--  to_timestamp(event_time/1000000) as imp_tim,
    row_number() over() as imp_nbr
from diap01.mec_us_united_20056.dfa2_impression
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2017-08-15' and '2017-08-31'
and campaign_id = 10742878 -- display 2017
and (advertiser_id <> 0)
    and site_id_dcm in (1190273,1239319)
);

-- where
--  ta.user_id = ti.user_id and
-- --   datediff('day',to_timestamp(ti.event_time/1000000),to_timestamp(ta.event_time/1000000)) <= 7
-- imp_tim = con_tim);
commit;
