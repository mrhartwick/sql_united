drop table if exists ikea_adform.temp_pvi_conv;
create table ikea_adform.temp_pvi_conv as

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
		and cast(t1.trackingpointid as bigint) in (101141975, 107060168, 107304955)
		and cast(t1.cookieid as bigint) !=0
;

-- select * from ikea_adform.temp_pvi_conv
-- -- order by cookieid
-- limit 20
-- ==================================================================================================================

drop table if exists ikea_adform.temp_pvi_imp_1;
create table ikea_adform.temp_pvi_imp_1 as

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
				case when cast("placementid-activityid" as bigint) in (8447992, 8447988, 8447965, 8447967, 8447986, 8447981, 8447968, 8447978, 8447991, 8447984, 8447972, 8447979, 8447982, 8447985, 8447970, 8447976, 8447987, 8447994, 8447971, 8447969, 8447993, 8447990, 8447966, 8447973, 8447963, 8447980, 8447975, 8447964, 8447983, 8447989, 8447977, 8447974) then 'verizon' else 'other' end as last_site,
		        date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.conv_time) as last_imp_time_diff,
				row_number() over(partition by a.cookieid, b.conv_nbr order by date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), cast(b.conv_time as timestamp))) as r1,
		        row_number() over () as last_imp_cnt
		from
				AwsDataCatalog.ikea_adform.impression a
		inner join ikea_adform.temp_pvi_conv b
		    on a.cookieid = b.cookieid and
-- 		    date_diff('day', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.conv_time) between 0 and 30 and
-- 		    date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.conv_time) > 0
			date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.conv_time) between 0 and 30 * 24 * 60 * 60
		where
			    a.isrobot = 'No' and
				a.campaignid = '2409784' and -- PVI
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

drop table if exists ikea_adform.temp_pvi_imp_2;
create table ikea_adform.temp_pvi_imp_2 as

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
				case when cast("placementid-activityid" as bigint) in (8447992, 8447988, 8447965, 8447967, 8447986, 8447981, 8447968, 8447978, 8447991, 8447984, 8447972, 8447979, 8447982, 8447985, 8447970, 8447976, 8447987, 8447994, 8447971, 8447969, 8447993, 8447990, 8447966, 8447973, 8447963, 8447980, 8447975, 8447964, 8447983, 8447989, 8447977, 8447974) then 'verizon' else 'other' end as last_minus1_site,
		        date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.last_imp_time) as last_minus1_time_diff,
				row_number() over(partition by a.cookieid, b.conv_nbr order by date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), cast(b.last_imp_time as timestamp))) as r1,
		        row_number() over() as last_minus1_imp_cnt
		from
				AwsDataCatalog.ikea_adform.impression a
		inner join ikea_adform.temp_pvi_imp_1 b
		    on a.cookieid = b.cookieid and
		    cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp) != b.last_imp_time and
		    date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.last_imp_time) between 0 and 30 * 24 * 60 * 60 and
		    date_diff('second', cast(date_parse(timestamp,'%Y-%m-%d %k:%i:%s') as timestamp), b.last_imp_time) > 0
		where
			    a.isrobot = 'No' and
				a.campaignid = '2409784' and -- PVI
				a.mediaid = '1468774' -- Xaxis

		    ) t1
where
r1 = 1
;

-- ==================================================================================================================
drop table if exists ikea_adform.temp_pvi_final;
create table ikea_adform.temp_pvi_final as

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
		ikea_adform.temp_pvi_conv a
left outer join
		ikea_adform.temp_pvi_imp_1 b
		on a.cookieid=b.cookieid
		    and
		a.conv_nbr = b.conv_nbr
left outer join
		ikea_adform.temp_pvi_imp_2 c
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
		ikea_adform.temp_pvi_final
group by
		last_site,
		last_minus1_site;






