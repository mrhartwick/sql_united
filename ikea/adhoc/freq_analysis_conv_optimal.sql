drop table if exists wmprodfeeds.ikea.freq_conv_tbl1;
create table wmprodfeeds.ikea.freq_conv_tbl1 (

  userid varchar(108) encode zstd,
  siteid int encode zstd,
  campaignid int encode zstd,
  conversiontime timestamp encode zstd,
  impressiontime timestamp encode zstd distkey,
  yr int encode zstd,
  mt int encode zstd,
  wk int encode zstd,
  dy timestamp encode zstd,
  cvr_nbr bigint encode zstd,
  imp_nbr bigint encode zstd
)

diststyle key
sortkey(impressiontime)
;


insert into wmprodfeeds.ikea.freq_conv_tbl1
(userid, siteid, campaignid, conversiontime, impressiontime, yr, mt, wk, dy,
 cvr_nbr, imp_nbr)


    (select b.userid,
            b.siteid,
            b.campaignid,
            a.conversiontime,
            b.impressiontime,
            b.yr,
            b.mt,
            b.wk,
            b.dy,
            a.cvr_nbr,
            b.imp_nbr
     from (select userid,
--                                 s1.sitename,
--                              t1.siteid,
                  winnereventdatedefaulttimezone as conversiontime,
--                                 date_part(year, conversiondatedefaulttimezone) as yr,
--                                 date_part(month,conversiondatedefaulttimezone) as mt,
--                                 date_part(week, conversiondatedefaulttimezone) as wk,
--                                 conversiondatedefaulttimezone::date as dy,
                  row_number() over ()           as cvr_nbr
           from wmprodfeeds.ikea.sizmek_conversion_events t1
--                 left join
--                                 wmprodfeeds.ikea.sizmek_sites s1
--                                 on t1.siteid = s1.siteid
           where eventtypeid = 1 and isconversion = true and userid != '0' and advertiserid != 0 and
                 winnereventdatedefaulttimezone::date
--                       = '2018-09-30'
                    between '2018-09-01' and '2018-12-31'
                   and t1.campaignid in (913509, 913772, 913792, 913795, 914697, 939667) and
                 t1.conversiontagid in (593715, 596537, 598734, 598735, 598736, 598737, 598738, 598739, 598740, 598741,
                                        598742, 598743, 598744, 598745, 598746, 598747, 598748, 598749, 598750, 598751,
                                        598752, 598753, 598754, 598755, 598756, 598757, 598758, 598759, 598760, 598761,
                                        598762, 598763, 598764, 598765, 598766, 598767, 598768, 598769, 598770, 598771,
                                        598772, 598773, 598774, 612254, 612256, 736352, 930091, 1018765, 1076168,
                                        1084971, 1098595, 1100473, 1174087, 1061036)
          ) a,
          (select userid,
                  -- s1.sitename,
                  t1.campaignid,
                  t1.siteid,
                  eventdatedefaulttimezone                   as impressiontime,
                  date_part(year, eventdatedefaulttimezone)  as yr,
                  date_part(month, eventdatedefaulttimezone) as mt,
                  date_part(week, eventdatedefaulttimezone)  as wk,
                  eventdatedefaulttimezone::date             as dy,
                  row_number() over ()                       as imp_nbr
           from wmprodfeeds.ikea.sizmek_standard_events t1
                -- left join
                --                 wmprodfeeds.ikea.sizmek_sites s1
                --                 on t1.siteid = s1.siteid

                -- left join
                --                 wmprodfeeds.ikea.sizmek_display_campaigns c1
                --                 on t1.campaignid = c1.campaignid

           where eventtypeid = 1 and userid != '0' and advertiserid != 0 and
                 eventdatedefaulttimezone::date
--                       = '2018-09-30'
                    between '2018-09-01' and '2018-12-31'
                   and t1.campaignid in (913509, 913772, 913792, 913795, 914697, 939667)
          ) b
     where a.userid = b.userid and
           conversiontime = impressiontime
--                 datediff('day',impressiontime,conversiontime) >= 7
--                 conversiontime > impressiontime
    );



-- ==================================================================================================================
drop table if exists wmprodfeeds.ikea.freq_conv_tbl2;
create table wmprodfeeds.ikea.freq_conv_tbl2 (

  userid varchar(108) encode zstd,
  siteid int encode zstd,
  campaignid int encode zstd,
  conversiontime timestamp encode zstd,
  impressiontime timestamp encode zstd distkey,
  yr int encode zstd,
  mt int encode zstd,
  wk int encode zstd,
  dy timestamp encode zstd,
  cvr_nbr bigint encode zstd,
  imp_nbr bigint encode zstd
)

diststyle key
sortkey(impressiontime)
;


insert into wmprodfeeds.ikea.freq_conv_tbl2
(userid, siteid, campaignid, conversiontime, impressiontime, yr, mt, wk, dy,
 cvr_nbr, imp_nbr)


    (select b.userid,
            b.siteid,
            b.campaignid,
            a.conversiontime,
            b.impressiontime,
            b.yr,
            b.mt,
            b.wk,
            b.dy,
            a.cvr_nbr,
            b.imp_nbr
     from (select userid,
--                                 s1.sitename,
--                              t1.siteid,
                  conversiondatedefaulttimezone as conversiontime,
--                                 date_part(year, conversiondatedefaulttimezone) as yr,
--                                 date_part(month,conversiondatedefaulttimezone) as mt,
--                                 date_part(week, conversiondatedefaulttimezone) as wk,
--                                 conversiondatedefaulttimezone::date as dy,
                  row_number() over ()           as cvr_nbr
           from wmprodfeeds.ikea.sizmek_conversion_events t1
--                 left join
--                                 wmprodfeeds.ikea.sizmek_sites s1
--                                 on t1.siteid = s1.siteid
           where eventtypeid = 1 and isconversion = true and userid != '0' and advertiserid != 0 and
                 conversiondatedefaulttimezone::date
--                       = '2018-09-30'
                    between '2018-09-01' and '2018-12-31'
                   and t1.campaignid in (913509, 913772, 913792, 913795, 914697, 939667) and
                 t1.conversiontagid in (593715, 596537, 598734, 598735, 598736, 598737, 598738, 598739, 598740, 598741,
                                        598742, 598743, 598744, 598745, 598746, 598747, 598748, 598749, 598750, 598751,
                                        598752, 598753, 598754, 598755, 598756, 598757, 598758, 598759, 598760, 598761,
                                        598762, 598763, 598764, 598765, 598766, 598767, 598768, 598769, 598770, 598771,
                                        598772, 598773, 598774, 612254, 612256, 736352, 930091, 1018765, 1076168,
                                        1084971, 1098595, 1100473, 1174087, 1061036)
          ) a,
          (select userid,
                  -- s1.sitename,
                  t1.campaignid,
                  t1.siteid,
                  eventdatedefaulttimezone                   as impressiontime,
                  date_part(year, eventdatedefaulttimezone)  as yr,
                  date_part(month, eventdatedefaulttimezone) as mt,
                  date_part(week, eventdatedefaulttimezone)  as wk,
                  eventdatedefaulttimezone::date             as dy,
                  row_number() over ()                       as imp_nbr
           from wmprodfeeds.ikea.sizmek_standard_events t1
                -- left join
                --                 wmprodfeeds.ikea.sizmek_sites s1
                --                 on t1.siteid = s1.siteid

                -- left join
                --                 wmprodfeeds.ikea.sizmek_display_campaigns c1
                --                 on t1.campaignid = c1.campaignid

           where eventtypeid = 1 and userid != '0' and advertiserid != 0 and
                 eventdatedefaulttimezone::date
--                       = '2018-09-30'
                    between '2018-09-01' and '2018-12-31'
                   and t1.campaignid in (913509, 913772, 913792, 913795, 914697, 939667)
          ) b
     where a.userid = b.userid and
           datediff('second',impressiontime,conversiontime) > 0 and
           datediff('day',impressiontime,conversiontime) between 0 and 30

    );

-- ==================================================================================================================


select
            dy,
            c1.campaignname,
            s1.sitename,
            count(distinct userid)  as users,
            count(distinct imp_nbr) as imps,
            count(distinct cvr_nbr) as cnvs

from
            wmprodfeeds.ikea.freq_conv_tbl1 t1
left join
            wmprodfeeds.ikea.sizmek_display_campaigns c1
            on t1.campaignid = c1.campaignid
left join
            wmprodfeeds.ikea.sizmek_sites s1
            on t1.siteid = s1.siteid
group by
            c1.campaignname,
            dy,
            s1.sitename