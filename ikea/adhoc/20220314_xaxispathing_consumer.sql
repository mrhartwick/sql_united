drop table if exists ikea_adform.temp_cons_conv;
create table ikea_adform.temp_cons_conv as

select
		t1.cookieid,
        cast(date_parse(t1.timestamp,'%Y-%m-%d %k:%i:%s') as timestamp) as conv_time,
        t1.attributedtransactionid as transactionid,
-- 		count(1) as conv
		row_number() over ()                       as conv_nbr
from
		AwsDataCatalog.ikea_adform.trackingpoint t1
where
		cast(date_parse(t1.timestamp,'%Y-%m-%d %k:%i:%s') as date) between date '2021-09-01' and date '2022-01-31'
		and t1.isrobot = 'No'
		and cast(t1.trackingpointid as bigint) in (41857156, 41932269, 106320275)
		and cast(t1.cookieid as bigint) !=0
;

-- select * from ikea_adform.temp_pvi_conv
-- -- order by cookieid
-- limit 20
-- ==================================================================================================================

drop table if exists ikea_adform.temp_cons_imp_1;
create table ikea_adform.temp_cons_imp_1 as

select
		cookieid,
		last_site,
		last_imp_time,
        conv_time,
        conv_nbr,
		last_imp_time_diff,
        last_imp_cnt
from
		(select
				a.cookieid,
				cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp) as last_imp_time,
		        b.conv_time,
		        b.conv_nbr,
				case when cast("placementid-activityid" as bigint) in (8123655, 8123643, 8123653, 8123651, 8123656, 8123645, 8123641, 8123644, 8123646, 8102835, 8102892, 8102930, 8102951, 8102913, 8102863, 8102884, 8102857, 8102860, 8102948, 8102931, 8102935, 8102895, 8102886, 8102936, 8102887, 8102858, 8102893, 8102914, 8102864, 8102885, 8102915, 8102894, 8102859, 8102933, 8102938, 8102862, 8102952, 8102888, 8102953, 8102865, 8102896, 8102861, 8102932, 8102843, 8102949, 8102937, 8102950, 8102917, 8102844, 8123639, 8123649, 8123642, 8123650, 8123647, 8123654, 8123648, 8123640, 8102916, 8102897, 8102943, 8102926, 8102908, 8102838, 8102922, 8102830, 8102845, 8102878, 8102836, 8102921, 8102837, 8102874, 8102829, 8102905, 8102880, 8102870, 8102906, 8102939, 8102831, 8102923, 8102910, 8102903, 8102927, 8102847, 8102909, 8102846, 8102869, 8102879, 8102902, 8102866, 8102889, 8102848, 8102875, 8102839, 8102918, 8102929, 8102852, 8102924, 8102899, 8102876, 8102919, 8102945, 8102877, 8102890, 8102872, 8102911, 8102868, 8102841, 8102882, 8102826, 8102849, 8102944, 8102881, 8102928, 8102850, 8102907, 8102851, 8102840, 8102832, 8102867, 8102871, 8102904, 8102855, 8102856, 8102912, 8102873, 8102901, 8102854, 8102842, 8102925, 8102883, 8102828, 8102891, 8102834, 8102942, 8102900, 8102946, 8102853, 8102920, 8102941, 8102833, 8102827, 8102934) then 'verizon' else 'other' end as last_site,
		        date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.conv_time) as last_imp_time_diff,
				row_number() over(partition by a.cookieid, b.conv_nbr order by date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), cast(b.conv_time as timestamp))) as r1,
		        row_number() over () as last_imp_cnt
		from
				AwsDataCatalog.ikea_adform.impression a
		inner join ikea_adform.temp_cons_conv b
		    on a.cookieid = b.cookieid and
-- 		    date_diff('day', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.conv_time) between 0 and 30 and
-- 		    date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.conv_time) > 0
			date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.conv_time) between 0 and 30 * 24 * 60 * 60 -- seconds to days
		where
			    a.isrobot = 'No' and
				a.campaignid = '2444615' and -- Consumer
				a.mediaid = '1468774' -- Xaxis

		    ) t1
where
r1 = 1
;

-- select * from ikea_adform.temp_pvi_imp_1
-- -- order by cookieid
-- limit 100

-- select * from AwsDataCatalog.ikea_adform.impression
--     limit 20

-- ==================================================================================================================

drop table if exists ikea_adform.temp_cons_imp_2;
create table ikea_adform.temp_cons_imp_2 as

select
		cookieid,
        conv_time,
        conv_nbr,
		last_minus1_site,
		last_minus1_imp_time,
		last_minus1_time_diff,
        last_minus1_imp_cnt
from
		(select
				a.cookieid,
		        b.conv_time,
		        b.conv_nbr,
				cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp) as last_minus1_imp_time,
				case when cast("placementid-activityid" as bigint) in (8123655, 8123643, 8123653, 8123651, 8123656, 8123645, 8123641, 8123644, 8123646, 8102835, 8102892, 8102930, 8102951, 8102913, 8102863, 8102884, 8102857, 8102860, 8102948, 8102931, 8102935, 8102895, 8102886, 8102936, 8102887, 8102858, 8102893, 8102914, 8102864, 8102885, 8102915, 8102894, 8102859, 8102933, 8102938, 8102862, 8102952, 8102888, 8102953, 8102865, 8102896, 8102861, 8102932, 8102843, 8102949, 8102937, 8102950, 8102917, 8102844, 8123639, 8123649, 8123642, 8123650, 8123647, 8123654, 8123648, 8123640, 8102916, 8102897, 8102943, 8102926, 8102908, 8102838, 8102922, 8102830, 8102845, 8102878, 8102836, 8102921, 8102837, 8102874, 8102829, 8102905, 8102880, 8102870, 8102906, 8102939, 8102831, 8102923, 8102910, 8102903, 8102927, 8102847, 8102909, 8102846, 8102869, 8102879, 8102902, 8102866, 8102889, 8102848, 8102875, 8102839, 8102918, 8102929, 8102852, 8102924, 8102899, 8102876, 8102919, 8102945, 8102877, 8102890, 8102872, 8102911, 8102868, 8102841, 8102882, 8102826, 8102849, 8102944, 8102881, 8102928, 8102850, 8102907, 8102851, 8102840, 8102832, 8102867, 8102871, 8102904, 8102855, 8102856, 8102912, 8102873, 8102901, 8102854, 8102842, 8102925, 8102883, 8102828, 8102891, 8102834, 8102942, 8102900, 8102946, 8102853, 8102920, 8102941, 8102833, 8102827, 8102934) then 'verizon' else 'other' end as last_minus1_site,
		        date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.last_imp_time) as last_minus1_time_diff,
				row_number() over(partition by a.cookieid, b.conv_nbr order by date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), cast(b.last_imp_time as timestamp))) as r1,
		        row_number() over() as last_minus1_imp_cnt
		from
				AwsDataCatalog.ikea_adform.impression a
		inner join ikea_adform.temp_cons_imp_1 b
		    on a.cookieid = b.cookieid and
		    cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp) != b.last_imp_time and
		    date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.last_imp_time) between 0 and 30 * 24 * 60 * 60 and
		    date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.last_imp_time) > 0
		where
			    a.isrobot = 'No' and
				a.campaignid = '2444615' and -- Consumer
				a.mediaid = '1468774' -- Xaxis

		    ) t1
where
r1 = 1
;

-- ==================================================================================================================
drop table if exists ikea_adform.temp_cons_final;
create table ikea_adform.temp_cons_final as

select
		a.cookieid,
        a.conv_nbr,
		a.conv_time,
		case when b.last_site is not null then b.last_site else 'no last site' end as last_site,
		b.last_imp_time,
        b.last_imp_cnt,
		case when c.last_minus1_site is not null then c.last_minus1_site else 'no last minus1 site' end as last_minus1_site,
		c.last_minus1_imp_time,
        c.last_minus1_imp_cnt
from
		ikea_adform.temp_cons_conv a
left outer join
		ikea_adform.temp_cons_imp_1 b
		on a.cookieid=b.cookieid
		    and
		a.conv_nbr = b.conv_nbr
left outer join
		ikea_adform.temp_cons_imp_2 c
		on a.cookieid=c.cookieid
		    and
		a.conv_nbr = c.conv_nbr
;

-- select * from ikea_adform.temp_pvi_final
-- -- order by cookieid
-- limit 100
-- ==================================================================================================================
--
-- create table ikea_adform.dpe_04_v2_last_imp_minus1 as
-- select cookieid,
--        conv_nbr,
--        case when last_minus1_site_check >= 1000000 and mod(last_minus1_site_check, 1000000) = 0 then 'verizon only'
--             when last_minus1_site_check > 1000000 and mod(last_minus1_site_check, 1000000) != 0
-- 	            then 'verizon and other sites'
--             when last_minus1_site_check between 1 and 999999 then 'other sites only'
--             else 'xxx' end as last_minus1_sites
-- from (select a.cookieid,
--              b.conv_nbr,
--              sum(case when cast("placementid-activityid" as bigint) in (8447992, 8447988, 8447965, 8447967, 8447986,
--                                                                         8447981, 8447968, 8447978, 8447991, 8447984,
--                                                                         8447972, 8447979, 8447982, 8447985, 8447970,
--                                                                         8447976, 8447987, 8447994, 8447971, 8447969,
--                                                                         8447993, 8447990, 8447966, 8447973, 8447963,
--                                                                         8447980, 8447975, 8447964, 8447983, 8447989,
--                                                                         8447977, 8447974) then 1000000
--                       else 1 end) as last_minus1_site_check
--       from ikea_adform.temp_pvi_imp_1 a
-- 	           inner join
--            ikea_adform.temp_pvi_imp_2 b on a.cookieid = b.cookieid and
-- 	           imp_time != last_imp_time and
-- 	           date_diff('second', last_minus1_imp_time, last_imp_time) >= 0 and
-- 	           date_diff('second', last_minus1_imp_time, a.conv_time) between 0 and 30 * 24 * 60 * 60
--       group by 1, 2) t1;
-- -- ==================================================================================================================
--
-- create table ikea_adform.dpe_04_v3_last_imp_minus1 as
-- select cookieid,
--        cvr_id,
--        imps,
--        case when last_minus1_site_check >= 1000000 and mod(last_minus1_site_check, 1000000) = 0 then 'verizon only'
--             when last_minus1_site_check > 1000000 and mod(last_minus1_site_check, 1000000) != 0
-- 	            then 'verizon and other sites'
--             when last_minus1_site_check between 1 and 999999 then 'other sites only'
--             else 'xxx' end as last_minus1_sites
-- from (select a.cookieid,
--              b.cvr_id,
--              sum(case when cast("placementid-activityid" as bigint) in (8447992, 8447988, 8447965, 8447967, 8447986,
--                                                                         8447981, 8447968, 8447978, 8447991, 8447984,
--                                                                         8447972, 8447979, 8447982, 8447985, 8447970,
--                                                                         8447976, 8447987, 8447994, 8447971, 8447969,
--                                                                         8447993, 8447990, 8447966, 8447973, 8447963,
--                                                                         8447980, 8447975, 8447964, 8447983, 8447989,
--                                                                         8447977, 8447974) then 1000000
--                       else 1 end) as last_minus1_site_check,
--              count(distinct imp_id) as imps
--       from ikea_adform.dpe_02_ikea_all_imps a
-- 	           inner join
--            ikea_adform.dpe_03_last_imp b on a.cookieid = b.cookieid and
-- 	           imp_time != last_imp_time and
-- 	           date_diff('second', imp_time, last_imp_time) >= 0 and
-- 	           date_diff('second', imp_time, conv_time) between 0 and 30 * 24 * 60 * 60
--       group by 1, 2) t1;
-- -- ==================================================
-- select count(distinct cookieid),
--        count(distinct cvr_id),
--        sum(imps) as imps,
--        last_minus1_sites
--
-- from ikea_adform.dpe_04_v3_last_imp_minus1
-- group by last_minus1_sites
-- ==================================================

select
        count(distinct cookieid) as users,
        count(distinct conv_nbr) as conversions,
        last_site,
        count(distinct last_imp_cnt) as final_imps,
        last_minus1_site,
        count(distinct last_minus1_imp_cnt) as prior_imps
from
		ikea_adform.temp_cons_final
group by
		last_site,
		last_minus1_site;






