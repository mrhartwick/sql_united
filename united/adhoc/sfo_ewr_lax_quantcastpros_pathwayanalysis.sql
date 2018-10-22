--Conversion Pathway Analysis: SFO/EWR/LAX

--Sojern:
--Goal: To find the amount of conversions General Prospecting was stealing from the SFO/EWR/LAX campaign given it's last touch attribution
--Step 1: get the user pool who were exposed to the SFO/EWR/LAX campaign (all partners)

create table wmprodfeeds.united.ual_routesojern
(
user_id      varchar(50) not null,
routetime    timestamp   not null
);


insert into  wmprodfeeds.united.ual_routesojern
(user_id,
routetime
)


            (select
                 t1.user_id as user_id
                ,t1.md_event_time_loc  as routetime
            from wmprodfeeds.united.dfa2_impression as t1

            left join wmprodfeeds.united.dfa2_placements as p1
            on t1.placement_id = p1.placement_id

            where t1.md_event_date_loc between '2018-06-01' and '2018-06-30'
            and t1.campaign_id = 20606595 -- GM ACQ 2018
            and (t1.advertiser_id <> 0)
            and t1.user_id <> '0'
            and p1.placement like '%Route%'
            and t1.site_id_dcm = 1239319 --Sojern
          );

commit;

---------------------------------------------------------------------------------------------------------

--Step 2: Exposed to general prospecting

create table wmprodfeeds.united.ual_prossojern
(
user_id        varchar(50) not null,
prostime       timestamp   not null,
imp_nbr        int         not null,
cvr_window_day int         not null
);


insert into  wmprodfeeds.united.ual_prossojern
(user_id,
prostime,
imp_nbr,
cvr_window_day
)

(select
       a.user_id
      ,b.prostime
      ,b.imp_nbr
      ,datediff('day',routetime, prostime) as cvr_window_day

      from

        (select
               user_id
               ,routetime
               from wmprodfeeds.united.ual_routesojern
                 ) as a,

            (
            select
            t2.user_id as user_id
            ,t2.md_event_time_loc  as prostime
            ,row_number() over () as imp_nbr

            from wmprodfeeds.united.dfa2_impression as t2

            left join wmprodfeeds.united.dfa2_placements as p1
            on t2.placement_id = p1.placement_id

            where t2.md_event_date_loc between '2018-06-01' and '2018-06-30'
            and t2.campaign_id = 20606595 -- GM ACQ 2018
            and (t2.advertiser_id <> 0)
            and t2.user_id <> '0'
            and p1.placement like '%PROS%'
            and t2.site_id_dcm = 3267410  --Quantcast
          ) as b

          where b.user_id = a.user_id
          and a.routetime < b.prostime
          );

commit;


---------------------------------------------------------------------------------------------------------------------------
--Step 3: Purchased a ticket


create table wmprodfeeds.united.ual_conversiontbl2
(
user_id        varchar(50) not null,
rev            decimal(20,10),
tix            int,
convtime       timestamp   not null
);


insert into  wmprodfeeds.united.ual_conversiontbl2
(user_id,
rev,
tix,
convtime
)

(select
       a.user_id
      ,a.rev
      ,a.tix
      ,a.convtime

      from

            (
            select
             t3.user_id as user_id
            ,t3.md_interaction_time_loc  as convtime
            ,case when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') = 0 then cast(((t3.total_revenue * 1000000) / (rates.exchange_rate)) as decimal(20,10))
                  when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') > 0 then cast(((t3.total_revenue*1000)/.0103) as decimal(20,10)) end as rev
            ,t3.total_conversions as tix

            from wmprodfeeds.united.dfa2_activity as t3

            left join wmprodfeeds.exchangerates.exchange_rates as rates
            on upper(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3)) = upper(rates.currency)
            and md_event_date_loc = rates.date


            where md_interaction_date_loc between '2018-06-01' and '2018-06-30'
            and t3.campaign_id = 20606595 -- GM ACQ 2018
            and (t3.advertiser_id <> 0)
            and t3.user_id <> '0'
            and t3.activity_id = 978826
            and t3.total_revenue <> '0'
            and (length(isnull(event_sub_type,'')) > 0)
            and user_id in (select distinct user_id from wmprodfeeds.united.ual_prossojern)
            and md_interaction_time_loc in (select distinct prostime from wmprodfeeds.united.ual_prossojern)
          ) as a);

commit;

--------------------------------------------------------------------------------------

--Check to see user counts go down for each table:

select count(distinct user_id) from wmprodfeeds.united.ual_routesojern
select count(distinct user_id) from wmprodfeeds.united.ual_prossojern
select count(distinct user_id) from wmprodfeeds.united.ual_conversiontbl2

------------------------------------------------------------------------------------------------
--Get unique conversions and tickets volume:

select count(distinct user_id) as users, sum(tix) as tickets, sum(rev) as rev  from wmprodfeeds.united.ual_conversiontbl2

--============================================================================================================================================================================================================================================

--Quantcast:
--Goal: To find the amount of conversions General Prospecting was stealing from the SFO/EWR/LAX campaign given it's last touch attribution
--Step 1: get the user pool who were exposed to the SFO/EWR/LAX campaign (all partners)

create table wmprodfeeds.united.ual_routeq
(
user_id      varchar(50) not null,
routetime    timestamp   not null
);


insert into  wmprodfeeds.united.ual_routeq
(user_id,
routetime
)


            (select
                 t1.user_id as user_id
                ,t1.md_event_time_loc  as routetime
            from wmprodfeeds.united.dfa2_impression as t1

            left join wmprodfeeds.united.dfa2_placements as p1
            on t1.placement_id = p1.placement_id

            where t1.md_event_date_loc between '2018-07-09' and '2018-08-09'
            and t1.campaign_id = 20606595 -- GM ACQ 2018
            and (t1.advertiser_id <> 0)
            and t1.user_id <> '0'
            and p1.placement like '%Route%'
            and t1.site_id_dcm = 3267410 --Quantcast
          );

commit;

---------------------------------------------------------------------------------------------------------

--Step 2: Exposed to general prospecting

create table wmprodfeeds.united.ual_prossq
(
user_id        varchar(50) not null,
prostime       timestamp   not null,
imp_nbr        int         not null,
cvr_window_day int         not null
);


insert into  wmprodfeeds.united.ual_prossq
(user_id,
prostime,
imp_nbr,
cvr_window_day
)

(select
       a.user_id
      ,b.prostime
      ,b.imp_nbr
      ,datediff('day',routetime, prostime) as cvr_window_day

      from

        (select
               user_id
               ,routetime
               from wmprodfeeds.united.ual_routeq
                 ) as a,

            (
            select
            t2.user_id as user_id
            ,t2.md_event_time_loc  as prostime
            ,row_number() over () as imp_nbr

            from wmprodfeeds.united.dfa2_impression as t2

            left join wmprodfeeds.united.dfa2_placements as p1
            on t2.placement_id = p1.placement_id

            where t2.md_event_date_loc between '2018-07-09' and '2018-08-09'
            and t2.campaign_id = 20606595 -- GM ACQ 2018
            and (t2.advertiser_id <> 0)
            and t2.user_id <> '0'
            and p1.placement like '%PROS%'
            and t2.site_id_dcm = 3267410  --Quantcast
          ) as b

          where b.user_id = a.user_id
          and a.routetime < b.prostime
          );

commit;


---------------------------------------------------------------------------------------------------------------------------
--Step 3: Purchased a ticket


create table wmprodfeeds.united.ual_conversiontbl4
(
user_id        varchar(50) not null,
rev            decimal(20,10),
tix            int,
convtime       timestamp   not null
);


insert into  wmprodfeeds.united.ual_conversiontbl4
(user_id,
rev,
tix,
convtime
)

(select
       a.user_id
      ,a.rev
      ,a.tix
      ,a.convtime

      from

            (
            select
             t3.user_id as user_id
            ,t3.md_interaction_time_loc  as convtime
            ,case when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') = 0 then cast(((t3.total_revenue * 1000000) / (rates.exchange_rate)) as decimal(20,10))
                  when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') > 0 then cast(((t3.total_revenue*1000)/.0103) as decimal(20,10)) end as rev
            ,t3.total_conversions as tix

            from wmprodfeeds.united.dfa2_activity as t3

            left join wmprodfeeds.exchangerates.exchange_rates as rates
            on upper(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3)) = upper(rates.currency)
            and md_event_date_loc = rates.date


            where md_interaction_date_loc between '2018-07-09' and '2018-08-09'
            and t3.campaign_id = 20606595 -- GM ACQ 2018
            and (t3.advertiser_id <> 0)
            and t3.user_id <> '0'
            and t3.activity_id = 978826
            and t3.total_revenue <> '0'
            and (length(isnull(event_sub_type,'')) > 0)
            and user_id in (select distinct user_id from wmprodfeeds.united.ual_prossq)
            and md_interaction_time_loc in (select distinct prostime from wmprodfeeds.united.ual_prossq)
          ) as a);

commit;

--------------------------------------------------------------------------------------

--Check to see user counts go down for each table:

select count(distinct user_id) from wmprodfeeds.united.ual_routeq
select count(distinct user_id) from wmprodfeeds.united.ual_prossq
select count(distinct user_id) from wmprodfeeds.united.ual_conversiontbl4

------------------------------------------------------------------------------------------------
--Get unique conversions and tickets volume:

select count(distinct user_id) as users, sum(tix) as tickets, sum(rev) as rev  from wmprodfeeds.united.ual_conversiontbl4

--=====================================================================================================================================================================================================================================

--Adara:
--Goal: To find the amount of conversions General Prospecting was stealing from the SFO/EWR/LAX campaign given it's last touch attribution
--Step 1: get the user pool who were exposed to the SFO/EWR/LAX campaign (all partners)

create table wmprodfeeds.united.ual_routeadara
(
user_id      varchar(50) not null,
routetime    timestamp   not null
);


insert into  wmprodfeeds.united.ual_routeadara
(user_id,
routetime
)


            (select
                 t1.user_id as user_id
                ,t1.md_event_time_loc  as routetime
            from wmprodfeeds.united.dfa2_impression as t1

            left join wmprodfeeds.united.dfa2_placements as p1
            on t1.placement_id = p1.placement_id

            where t1.md_event_date_loc between '2018-06-01' and '2018-06-30'
            and t1.campaign_id = 20606595 -- GM ACQ 2018
            and (t1.advertiser_id <> 0)
            and t1.user_id <> '0'
            and p1.placement like '%Route%'
            and t1.site_id_dcm =1190273 --Adara
          );

commit;

---------------------------------------------------------------------------------------------------------

--Step 2: Exposed to general prospecting

create table wmprodfeeds.united.ual_prosexpose
(
user_id        varchar(50) not null,
prostime       timestamp   not null,
imp_nbr        int         not null,
cvr_window_day int         not null
);


insert into  wmprodfeeds.united.ual_prosexpose
(user_id,
prostime,
imp_nbr,
cvr_window_day
)

(select
       a.user_id
      ,b.prostime
      ,b.imp_nbr
      ,datediff('day',routetime, prostime) as cvr_window_day

      from

        (select
               user_id
               ,routetime
               from wmprodfeeds.united.ual_routeadara
                 ) as a,

            (
            select
            t2.user_id as user_id
            ,t2.md_event_time_loc  as prostime
            ,row_number() over () as imp_nbr

            from wmprodfeeds.united.dfa2_impression as t2

            left join wmprodfeeds.united.dfa2_placements as p1
            on t2.placement_id = p1.placement_id

            where t2.md_event_date_loc between '2018-06-01' and '2018-06-30'
            and t2.campaign_id = 20606595 -- GM ACQ 2018
            and (t2.advertiser_id <> 0)
            and t2.user_id <> '0'
            and p1.placement like '%PROS%'
            and t2.site_id_dcm = 3267410  --Quantcast
          ) as b

          where b.user_id = a.user_id
          and a.routetime < b.prostime
          );

commit;


---------------------------------------------------------------------------------------------------------------------------
--Step 3: Purchased a ticket


create table wmprodfeeds.united.ual_conversiontbl
(
user_id        varchar(50) not null,
rev            decimal(20,10),
tix            int,
convtime       timestamp   not null
);


insert into  wmprodfeeds.united.ual_conversiontbl
(user_id,
rev,
tix,
convtime
)

(select
       a.user_id
      ,a.rev
      ,a.tix
      ,a.convtime

      from

            (
            select
             t3.user_id as user_id
            ,t3.md_interaction_time_loc  as convtime
            ,case when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') = 0 then cast(((t3.total_revenue * 1000000) / (rates.exchange_rate)) as decimal(20,10))
                  when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') > 0 then cast(((t3.total_revenue*1000)/.0103) as decimal(20,10)) end as rev
            ,t3.total_conversions as tix

            from wmprodfeeds.united.dfa2_activity as t3

            left join wmprodfeeds.exchangerates.exchange_rates as rates
            on upper(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3)) = upper(rates.currency)
            and md_event_date_loc = rates.date


            where md_interaction_date_loc between '2018-06-01' and '2018-06-30'
            and t3.campaign_id = 20606595 -- GM ACQ 2018
            and (t3.advertiser_id <> 0)
            and t3.user_id <> '0'
            and t3.activity_id = 978826
            and t3.total_revenue <> '0'
            and (length(isnull(event_sub_type,'')) > 0)
            and user_id in (select distinct user_id from wmprodfeeds.united.ual_prosexpose)
            and md_interaction_time_loc in (select distinct prostime from wmprodfeeds.united.ual_prosexpose)
          ) as a);

commit;

--------------------------------------------------------------------------------------

--Check to see user counts go down for each table:

select count(distinct user_id) from wmprodfeeds.united.ual_routeadara
select count(distinct user_id) from wmprodfeeds.united.ual_prosexpose
select count(distinct user_id) from wmprodfeeds.united.ual_conversiontbl

------------------------------------------------------------------------------------------------
--Get unique conversions and tickets volume:

select count(distinct user_id) as users, sum(tix) as tickets, sum(rev) as rev  from wmprodfeeds.united.ual_conversiontbl
