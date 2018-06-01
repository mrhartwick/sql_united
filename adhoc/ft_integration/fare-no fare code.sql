---Match conversion data with impression table using user_id/timestammp 

create table diap01.mec_us_united_20056.ual_fare_tbl_3
(
user_id           varchar(50),
con_tim           int,
imp_tim           int,
campaign_id       int,
site_id           int,
placement_id      int,
imps              int,
creative_ver_id   varchar(50),
tix               int,
rev               decimal(20,10)
);

insert into  diap01.mec_us_united_20056.ual_fare_tbl_3
(user_id,
con_tim,
imp_tim,
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
      ,ti.contim as con_tim
      ,ta.eventtime as imp_tim
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
            event_time as eventtime,
            row_number() over() as imps,
            campaign_id as campaign_id,
            site_id_dcm as site_id,
            placement_id as placement_id,
            substring(u_value,(instr(u_value,'U=') + 2),7) as creative_ver_id,
            user_id as user_id

            from diap01.mec_us_united_20056.dfa2_impression
            where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-04-25' and '2018-05-18'
            and campaign_id = 20606595 -- GM ACQ 2018
            and (advertiser_id <> 0)
            and site_id_dcm = 1239319
            and user_id <> '0'

            ) as ta,

            (
            select
            a.user_id,
            a.interaction_time as contim,
            total_conversions as tix,
            (total_revenue * 1000000)/rates.exchange_rate as rev


            from diap01.mec_us_united_20056.dfa2_activity a

            left join diap01.mec_us_mecexchangerates_20067.exchange_rates as rates
            on upper(substring(a.other_data,(instr(a.other_data,'u3=') + 3),3)) = upper(rates.currency)
            and cast(timestamp_trunc(to_timestamp(a.event_time / 1000000),'SS') as date) = rates.date

            where cast(timestamp_trunc(to_timestamp(interaction_time / 1000000),'SS') as date) between '2018-04-25' and '2018-05-18'
            and not regexp_like(substring(other_data,(instr(other_data,'u3=') + 3),5),'mil.*','ib')
            and activity_id = 978826
            and campaign_id = 20606595 -- GM ACQ 2018
            and (advertiser_id <> 0)
            and user_id <> '0'
            and (length(isnull (event_sub_type,'')) > 0)
            and site_id_dcm = 1239319
            and total_revenue <> 0
            ) as ti

      where
      ta.user_id = ti.user_id
      and eventtime = contim
      );

commit;

--=========================================================================================================================================================

--Step 2: Query to pull in placement level data

select
    t3.dcmdate               as "Date",
    t3.placement             as placement,
    t3.creative_ver_id       as creative_ver_id,
    sum(t3.impressions)      as imps,
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
                 from openQuery(verticaunited,
                                '
                                select
                                cast(timestamp_trunc(to_timestamp(con_tim / 1000000),''SS'') as date) as dcmdate,
                                t1.campaign_id,
                                t1.site_id,
                                p1.placement,
                                t1.placement_id,
                                count(imps) as impressions,
                                t1.creative_ver_id,
                                sum(tix) as tix,
                                sum(rev) as rev
                                from diap01.mec_us_united_20056.ual_fare_tbl_3 as t1


                                left join
                                (
                                select
                                cast(p1.placement as varchar(4000)) as ''placement'',p1.placement_id as ''placement_id'',p1.campaign_id as ''campaign_id'',p1.site_id_dcm as ''site_id_dcm''

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
                                and t1.site_id = p1.site_id_dcm


                                where not regexp_like(p1.placement,''.*_PROS_FT.*'',''ib'')
                                and t1.site_id = 1239319
                                and t1.campaign_id = 20606595
                                and cast(timestamp_trunc(to_timestamp(con_tim / 1000000),''SS'') as date) between ''2018-04-25'' and ''2018-05-18''

                                group by

                                cast(timestamp_trunc(to_timestamp(con_tim / 1000000),''SS'') as date),
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
cast(timestamp_trunc(to_timestamp(t01.event_time / 1000000),'SS') as date) as "date",
p1.placement as placement,
t01.creative_ver_id as creative_ver_id,
sum(t01.impressions) as impressions,
sum(t01.clicks) as clicks

from (
select
ti.event_time                                                             as event_time,
ti.campaign_id                                                            as campaign_id,
ti.site_id_dcm                                                            as site_id_dcm,
ti.placement_id                                                           as placement_id,
substring(u_value,(instr(u_value,'U=') + 2),7)                            as creative_ver_id,
count(*)                                                                  as impressions,
0                                                                         as clicks

from (
select *
from diap01.mec_us_united_20056.dfa2_impression
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-04-25' and '2018-05-18'
and (advertiser_id <> 0)

) as ti
group by
ti.campaign_id,
ti.site_id_dcm,
ti.event_time,
substring(u_value,(instr(u_value,'U=') + 2),7),
ti.placement_id

union all

select
tc.event_time                                                             as event_time,
tc.campaign_id                                                            as campaign_id,
tc.site_id_dcm                                                            as site_id_dcm,
tc.placement_id                                                           as placement_id,
substring(u_value,(instr(u_value,'U=') + 2),7)                            as creative_ver_id,
0                                                                         as impressions,
count(*)                                                                  as clicks

from (

select *
from diap01.mec_us_united_20056.dfa2_click
where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-04-25' and '2018-05-18'
and (advertiser_id <> 0)

) as tc

group by
substring(u_value,(instr(u_value,'U=') + 2),7),
tc.campaign_id,
tc.event_time,
tc.site_id_dcm,
tc.placement_id
) as t01

left join diap01.mec_us_united_20056.dfa2_placements as p1
on t01.placement_id = p1.placement_id
where t01.campaign_id = 20606595
and t01.site_id_dcm = 1239319
and not regexp_like(p1.placement,'.*_PROS_FT.*','ib')


group by
cast(timestamp_trunc(to_timestamp(t01.event_time / 1000000),'SS') as date),
p1.placement,
t01.creative_ver_id
