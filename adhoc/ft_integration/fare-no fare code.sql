---Match conversion data with impression table using user_id/timestammp

create table wmprodfeeds.united.ual_fare_tbl_3
(
user_id           varchar(50),
con_date           date,
imp_date           date,
campaign_id       int,
site_id           int,
placement_id      int,
imps              int,
creative_ver_id   varchar(50),
tix               int,
rev               decimal(20,10)
);



insert into  wmprodfeeds.united.ual_fare_tbl_3
(user_id,
con_date,
imp_date,
campaign_id,
site_id,
placement_id,
imps,
creative_ver_id,
tix,
rev)

    (
 select
       ta.user_id
      ,ti.con_date as con_date
      ,ta.imp_date as imp_date
      ,ta.campaign_id
      ,ta.site_id
      ,ta.placement_id
      ,ta.imps
      ,ta.creative_ver_id as creative_ver_id
      ,ti.tix
      ,ti.rev

        from
        (
            select
            event_time as impressiontime,
            md_event_date_loc as imp_date,
            row_number() over() as imps,
            campaign_id as campaign_id,
            site_id_dcm as site_id,
            t1.placement_id as placement_id,
            substring(u_value,3,7) AS creative_ver_id,
            user_id as user_id

            from wmprodfeeds.united.dfa2_impression as t1

            where md_event_date_loc between '2018-06-27' and '2018-07-31'
            and campaign_id = 20606595 -- GM ACQ 2018
            and (advertiser_id <> 0)
            and site_id_dcm = 1239319
            and user_id <> '0'



            ) as ta,

            (
            select
            a.user_id,
            a.interaction_time as contim,
            a.md_interaction_date_loc as con_date,
            total_conversions as tix,
            case when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') = 0 then cast(((a.total_revenue * 1000000) / (rates.exchange_rate)) as decimal(20,10))
            when regexp_instr(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3),'Mil.*') > 0 then cast(((a.total_revenue*1000)/.0103) as decimal(20,10)) end as rev


            from wmprodfeeds.united.dfa2_activity a

            left join wmprodfeeds.exchangerates.exchange_rates as rates
           on upper(substring(other_data,(regexp_instr(other_data,'u3\\=') + 3),3)) = upper(rates.currency)
           and md_event_date_loc = rates.date

            where md_interaction_date_loc between '2018-06-27' and '2018-07-31'
            and activity_id = 978826
            and campaign_id = 20606595 -- GM ACQ 2018
            and (advertiser_id <> 0)
            and user_id <> '0'
            and (length(isnull(event_sub_type,'')) > 0)
            and site_id_dcm = 1239319
            and total_revenue <> 0
--             and conversion_id in (1,2)
            ) as ti

      where
      ta.user_id = ti.user_id
      and impressiontime = contim
      );

commit;
--=========================================================================================================================================================

--Step 2: Query to pull in placement level data

select
    cast(t3.dcmdate as date)                                                   as "date",
    t3.placement             as placement,
    t3.creative_ver_id       as creative_ver_id,
--     sum(t3.impressions)      as imps,
    sum(t3.tix)              as tix,
    sum(t3.rev)              as rev

from
    (
        select
            t2.dcmdate                                                                                   as dcmdate,
            t2.campaign_id                                                                               as campaign_id,
            t2.site_id                                                                                   as site_id,
            t2.placement                                                                                 as placement,
            t2.creative_ver_id                                                                           as creative_ver_id,
            t2.placement_id                                                                              as placement_id,
            sum(t2.impressions)                                                                          as impressions,
            sum(t2.tix)                                                                                  as tix,
            sum(case
             when (placement LIKE '%BidStrategy1%')
             then cast(((t2.rev)  *  0.0070)  as decimal(10,2))
             when (placement LIKE '%BidStrategy2%')
             then cast(((t2.rev)  * 0.0088)  as decimal(10,2))
             when (placement LIKE '%BidStrategy3%')
             then cast(((t2.rev)  *  0.0175)  as decimal(10,2))
             when (placement LIKE '%First and Business%' OR placement LIKE '%First_Business%'
                          OR placement LIKE '%BusinessClass%' OR placement LIKE '%FirstClass%')
             then cast(((t2.rev)  * 0.0175)  as decimal(10,2))
             when (placement LIKE '%CatchAll1%' OR placement LIKE '%NoBidStrategy%')
             then cast(((t2.rev)  * 0.0105)  as decimal(10,2)) end)                                      as rev


-- openquery function call must not exceed 8,000 characters; no room for comments inside the function
        from (
                 select *
                 from openQuery(redshift,
                                '
                                select
                                con_date as dcmdate,
                                t1.campaign_id,
                                t1.site_id,
                                p1.placement,
                                t1.placement_id,
                                count(distinct imps) as impressions,
                                t1.creative_ver_id,
                                sum(tix) as tix,
                                sum(rev) as rev
                                from wmprodfeeds.united.ual_fare_tbl_3 as t1


                               left join
                                (
                                select cast (placement as varchar (4000)) as placement,placement_id as placement_id
                                from wmprodfeeds.united.dfa2_placements
                                ) as p1
                                on t1.placement_id = p1.placement_id


                                where p1.placement not like ''%PROS_FT%'' or p1.placement not like ''%Route%''
                                and t1.site_id = 1239319
                                and t1.campaign_id = 20606595
                                and con_date between ''2018-04-25'' and ''2018-07-27''

                                group by

                                con_date,
                                t1.campaign_id,
                                t1.site_id,
                                t1.placement_id,
                                p1.placement,
                                t1.creative_ver_id


                                ')

             ) as t2


        group by
            t2.dcmdate
            ,t2.campaign_id
            ,t2.site_id
            ,t2.placement
            ,t2.placement_id
            ,t2.creative_ver_id

    ) as t3


group by
    t3.dcmdate,
    t3.creative_ver_id,
    t3.placement

--=========================================================================================================
--To pull in imps & clicks

select
t01.date as "date",
p1.placement as placement,
t01.creative_ver_id as creative_ver_id,
sum(t01.impressions) as impressions,
sum(t01.clicks) as clicks

from (
select
ti.md_event_date_loc                                                             as "date",
ti.campaign_id                                                            as campaign_id,
ti.site_id_dcm                                                            as site_id_dcm,
ti.placement_id                                                           as placement_id,
substring(u_value,3,7) AS creative_ver_id,
count(*)                                                                  as impressions,
0                                                                         as clicks

from (
select *
from wmprodfeeds.united.dfa2_impression
where md_event_date_loc between '2018-04-25' and '2018-07-27'
and (advertiser_id <> 0)

) as ti
group by
ti.campaign_id,
ti.site_id_dcm,
ti.md_event_date_loc,
substring(u_value,3,7),
ti.placement_id

union all

select
tc.md_event_date_loc                                                             as "date",
tc.campaign_id                                                            as campaign_id,
tc.site_id_dcm                                                            as site_id_dcm,
tc.placement_id                                                           as placement_id,
substring(u_value,3,7) AS creative_ver_id,
0                                                                         as impressions,
count(*)                                                                  as clicks

from (

select *
from wmprodfeeds.united.dfa2_click
where md_event_date_loc between '2018-04-25' and '2018-07-27'
and (advertiser_id <> 0)

) as tc

group by
substring(u_value,3,7),
tc.campaign_id,
tc.md_event_date_loc,
tc.site_id_dcm,
tc.placement_id
) as t01

 left join
(
select cast (placement as varchar (4000)) as placement,placement_id as placement_id
from wmprodfeeds.united.dfa2_placements
) as p1
on t01.placement_id = p1.placement_id
where t01.campaign_id = 20606595
and t01.site_id_dcm = 1239319
and p1.placement not like '%PROS_FT' OR p1.placement not like '%Route%'


group by
t01.date,
p1.placement,
t01.creative_ver_id
