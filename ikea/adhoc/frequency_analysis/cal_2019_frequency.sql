
select
imps as frequency,
       count(distinct cookieid) as users,
       sum(imps) as imps,
       sum(conv) as conv


from (
select
       cookieid,
       count(distinct cvr_nbr) as conv,
       count(distinct imp_nbr) as imps
	     from
	          (
	      select a.cookieid,
-- 	      conversiontime,
-- 	      impressiontime,
-- 	             date_diff('day',impressiontime,conversiontime) as dayz,
	      cvr_nbr,
	      imp_nbr
	      from
                (select
                                cookieid,
                                date_parse(timestamp, '%Y-%m-%d %k:%i:%s') as conversiontime,
                                row_number() over() as cvr_nbr
                from
                               ikea_adform.trackingpoint
					where   yyyymmdd between 20190517 and 20190831 and
					        "placementid-activityid" in (4971356,4971366,4971365,4971353,4971367) and
							mediaid = 1468774 and        -- Xaxis
							campaignid = 1644150 and      -- eComm campaign only
							isrobot = 'No' and
	                        trackingpointid in (42298048,41957571,42018125,42297915,42298439,42297827,42297759,42355226,42298432,42297828,42299302,41966502,42298328,42297895,46121828,42297930,41860429,49015359,42297678,41861245,42075240,42297800,42298730,42318668,41861226,42073412,42298853,42298012,42298517,42298875,42297814,42297718,42313480,42298611,41860886,42298025,42297721,42131571,42298197,42297960,42076249,42298338,41861228,42297779,42074021,42299163,42299675,43477666,42298337,42074407,42387171,42298036,41856943,42298015,42300421)
                    ) a,
		     (select
                                cookieid,
                                date_parse(timestamp, '%Y-%m-%d %k:%i:%s') as impressiontime,
                                row_number() over() as imp_nbr
                from
                               AwsDataCatalog.ikea_adform.impression
                    where   yyyymmdd between 20190517 and 20190831 and
                            "placementid-activityid" in (4971356,4971366,4971365,4971353,4971367) and
							mediaid = 1468774 and        -- Xaxis
							campaignid = 1644150 and      -- eComm campaign only
							isrobot = 'No'
                    ) b
where
--       a.cookieid = 7347804250344177187 and
                a.cookieid = b.cookieid and
      date_diff('second',impressiontime,conversiontime) > 0 and
      date_diff('day',impressiontime,conversiontime) between 0 and 30
-- limit 100
) t1
group by cookieid
	) t2
group by imps
