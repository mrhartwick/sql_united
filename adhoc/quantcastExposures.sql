
-- Create table to hold Quantcast's conversion data
-- drop table diap01.mec_us_united_20056.ual_quantcast_conv
create table diap01.mec_us_united_20056.ual_quantcast_conv (
    site            varchar(1000),
    conv_time_utc   timestamp,
    treatment       varchar(1000),
    orderid         varchar(1000),
    revenue         decimal(20, 10),
    flight_class    varchar(1000),
    currency        varchar(100),
    conv_subchannel varchar(1000),
    pcat            integer
);

-- load Quantcast conversion data CSV into created table
copy diap01.mec_us_united_20056.ual_quantcast_conv from local 'C:\Users\matthew.hartwick\Dropbox\MEC_Work\Projects\United\20180410_Quantcast\conversion_data_united_formatted.csv' with DELIMITER ',' DIRECT commit;

-- =============================================================================================
-- =============================================================================================

-- Match Quantcast w/ activity table

-- drop table diap01.mec_us_united_20056.ual_quantcast_dcm_usr
create table diap01.mec_us_united_20056.ual_quantcast_dcm_usr (
    user_id            varchar(50),
    treatment varchar(1000)
);

insert into diap01.mec_us_united_20056.ual_quantcast_dcm_usr
(user_id, treatment)

    (select
        distinct
        user_id,

        qq.treatment
--      substring(other_data,(instr(other_data,'u18=')+4),8) as PNR
            from diap01.mec_us_united_20056.dfa2_activity

            left join diap01.mec_us_united_20056.ual_quantcast_conv as qq
--          on upper(substring(other_data,(instr(other_data,'u18=')+4),8)) = upper(qq.orderid)
            on md_event_time = qq.conv_time_utc

            where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date ) between '2018-02-27' and '2018-03-14'
            and user_id != '0'
            -- exclude null PNR codes
            and (length(isnull(substring(other_data,(instr(other_data,'u18=')+4),8),'')) > 0)
            and activity_id = 978826
            -- duplicate PNR code in Quantcast file
            and qq.orderid != 'Q04yRURK'
    );
commit;
-- =============================================================================================
-- =============================================================================================

-- QA MEASURES

-- Check matches across quantcast file and DCM activity - 19,680 matches
--  (select
--      distinct
--      user_id,
-- --       qq.treatment
--      substring(other_data,(instr(other_data,'u18=')+4),8) as PNR,
--      qq.orderid,
--      other_data
--          from diap01.mec_us_united_20056.dfa2_activity
--
--          full outer join diap01.mec_us_united_20056.ual_quantcast_conv as qq
--          on substring(other_data,(instr(other_data,'u18=')+4),8) = qq.orderid
--
--          where cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date ) between '2018-02-27' and '2018-03-14'
--          and user_id != '0'
--          -- exclude null PNR codes
--          and (length(isnull(substring(other_data,(instr(other_data,'u18=')+4),8),'')) > 0)
--          and activity_id = 978826
--             -- duplicate PNR code in Quantcast file
--          and qq.orderid != 'Q04yRURK'
--  );
-- =============================================================================================
-- =============================================================================================
-- Check matches across quantcast file and activity, with time join
--  (select
--      distinct
--      user_id,
-- --       qq.treatment
--      substring(other_data,(instr(other_data,'u18=')+4),8) as PNR,
--      md_event_time,
--      to_timestamp(event_time / 1000000) as conv_time,
--      qq.orderid,
--      qq.conv_time_utc,
--      other_data
--          from diap01.mec_us_united_20056.dfa2_activity
--
--          left join diap01.mec_us_united_20056.ual_quantcast_conv as qq
--          on md_event_time = qq.conv_time_utc
--
--          where cast(md_event_time as date) between '2018-02-27' and '2018-03-14'
--
-- --           to_timestamp converts to EST
-- --           cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date ) between '2018-02-27' and '2018-03-14'
--          and user_id != '0'
--          -- exclude null PNR codes
--          and (length(isnull(substring(other_data,(instr(other_data,'u18=')+4),8),'')) > 0)
--          and activity_id = 978826
--             -- duplicate PNR code in Quantcast file
--          and qq.orderid != 'Q04yRURK'
--  );

-- =============================================================================================
-- =============================================================================================

-- Test Group

-- drop table diap01.mec_us_united_20056.ual_quantcast_dcm_usr_test
create table diap01.mec_us_united_20056.ual_quantcast_dcm_usr_test (
    user_id            varchar(50),
    impressions int

);
insert into diap01.mec_us_united_20056.ual_quantcast_dcm_usr_test
(user_id,impressions)

    (
        select
            t2.user_id,
            count(t2.impressions) as impressions
        from
            (select
                t1.user_id,

                case
                when t1.placement_id in (215129150, 215144749, 215145031, 215145553, 215145556, 215222640, 215222910, 215225244, 215225256) then 'control'
                when t1.placement_id in (211543190, 211593748, 211594354, 211602330, 211605807, 211609899, 211609902, 211609905, 211610649) then 'test'
                end                  as dcm_type,
                row_number() over () as impressions

            from
                (
                    select *
                    from diap01.mec_us_united_20056.dfa2_impression
                    where cast(timestamp_trunc(to_timestamp(event_time / 1000000), 'SS') as date) between '2018-02-27' and '2018-03-14'
                        and site_id_dcm = 3267410
                        and placement_id in (211543190, 211593748, 211594354, 211602330, 211605807, 211609899, 211609902, 211609905, 211610649, 215129150, 215144749, 215145031, 215145553, 215145556, 215222640, 215222910, 215225244, 215225256)
                ) as t1
            where t1.user_id in (select user_id
            from diap01.mec_us_united_20056.ual_quantcast_dcm_usr)
            ) as t2
        where t2.dcm_type = 'test'
        group by t2.user_id
    );
commit;
-- =============================================================================================
-- =============================================================================================

-- Control Group

-- drop table diap01.mec_us_united_20056.ual_quantcast_dcm_usr_ctrl
create table diap01.mec_us_united_20056.ual_quantcast_dcm_usr_ctrl (
    user_id            varchar(50),
    impressions int

);
insert into diap01.mec_us_united_20056.ual_quantcast_dcm_usr_ctrl
(user_id,impressions)

    (
        select
            t2.user_id,
            count(t2.impressions) as impressions
        from
            (select
                t1.user_id,
                case
                when t1.placement_id in (215129150, 215144749, 215145031, 215145553, 215145556, 215222640, 215222910, 215225244, 215225256) then 'control'
                when t1.placement_id in (211543190, 211593748, 211594354, 211602330, 211605807, 211609899, 211609902, 211609905, 211610649) then 'test'
                end                  as dcm_type,
                row_number() over () as impressions

            from
                (
                    select *
                    from diap01.mec_us_united_20056.dfa2_impression
                    where cast(timestamp_trunc(to_timestamp(event_time / 1000000), 'SS') as date) between '2018-02-27' and '2018-03-14'
                        and site_id_dcm = 3267410
                        and placement_id in (211543190, 211593748, 211594354, 211602330, 211605807, 211609899, 211609902, 211609905, 211610649, 215129150, 215144749, 215145031, 215145553, 215145556, 215222640, 215222910, 215225244, 215225256)
                ) as t1
            where t1.user_id in (select user_id
            from diap01.mec_us_united_20056.ual_quantcast_dcm_usr)
            ) as t2
        where t2.dcm_type = 'control'
        group by t2.user_id
    );
commit;

-- =============================================================================================
-- =============================================================================================

-- Summary counts

select t2.dcm_type,
    count(distinct t2.user_id) as users,
    count(t2.impressions) as impressions

from
(select
            t1.user_id,
            case
                    when t1.placement_id in (215129150,215144749,215145031,215145553,215145556,215222640,215222910,215225244,215225256) then 'control'
                    when t1.placement_id in (211543190,211593748,211594354,211602330,211605807,211609899,211609902,211609905,211610649) then 'test'
            end     as dcm_type,

            row_number() over() as impressions

            from
            (
                        select *
            from diap01.mec_us_united_20056.dfa2_impression
            where cast (timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date ) between '2018-02-27' and '2018-03-14'
            and site_id_dcm = 3267410
            and placement_id in (211543190,211593748,211594354,211602330,211605807,211609899,211609902,211609905,211610649,215129150,215144749,215145031,215145553,215145556,215222640,215222910,215225244,215225256)
            ) as t1
            where t1.user_id in (select user_id from diap01.mec_us_united_20056.ual_quantcast_dcm_usr)

) as t2

group by
    t2.dcm_type
-- =============================================================================================
-- =============================================================================================

-- Overlap
select distinct user_id from diap01.mec_us_united_20056.ual_quantcast_dcm_usr
where user_id in (select user_id from diap01.mec_us_united_20056.ual_quantcast_dcm_usr_ctrl)
and user_id in (select user_id from diap01.mec_us_united_20056.ual_quantcast_dcm_usr_test)


