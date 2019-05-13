-- Frequency Table

drop table if exists wmprodfeeds.ikea.freq_tbl1;
create table wmprodfeeds.ikea.freq_tbl1 (

  userid varchar(108) encode zstd,
  sitename varchar(300) encode zstd,
  impressiontime timestamp encode zstd distkey,
  yr int encode zstd,
  mt int encode zstd,
  wk int encode zstd,
  dy timestamp encode zstd,
  imp_nbr bigint encode zstd
)

diststyle key
sortkey(impressiontime)
;
insert into wmprodfeeds.ikea.freq_tbl1
(userid, sitename, impressiontime, yr, mt, wk, dy,
 imp_nbr)

  (select
                                userid,
                              s1.sitename,
--                              t1.siteid,
                                eventdatedefaulttimezone as impressiontime,
                              date_part(year,eventdatedefaulttimezone) as yr,
                              date_part(month,eventdatedefaulttimezone) as mt,
                              date_part(week,eventdatedefaulttimezone) as wk,
                              eventdatedefaulttimezone::date as dy,
                                row_number() over() as imp_nbr
                from
                               wmprodfeeds.ikea.sizmek_standard_events t1
        left join
                wmprodfeeds.ikea.sizmek_sites s1
                on t1.siteid = s1.siteid
              where
                              eventtypeid = 1
              and             eventdatedefaulttimezone::date between '2017-09-01' and '2018-08-31'
              and             t1.campaignid in (813500, 858670, 822379, 817039, 813465, 862917, 823989, 835032, 876944, 815457, 832183, 908018, 850408)
--      limit 100
      );


-- ===============================================================================================
-- Frequency output

  select
                yr,
                wk,
                sitename,
                count(distinct userid) as user_cnt,
                count(distinct imp_nbr) as imp_cnt

from
                wmprodfeeds.ikea.freq_tbl1
group by
                1,2,3;