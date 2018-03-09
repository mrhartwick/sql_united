
-- // =================================================================================================================
drop table diap01.mec_us_united_20056.ual_weather_results
create table diap01.mec_us_united_20056.ual_weather_results
(
    user_id        varchar(50) not null,
    conversiontime int         not null,
    impressiontime int         not null,
	cvr_nbr        int         not null,
	imp_nbr        int         not null,
	division       varchar(20)  not null,
	weather_group  varchar(100) not null,
    cvr_window_day int         not null
);

insert into diap01.mec_us_united_20056.ual_weather_results
(user_id, conversiontime, impressiontime, cvr_nbr, imp_nbr, division, weather_group, cvr_window_day)

(select
                a.user_id,
--                 a.site_id_dcm,
                conversiontime,
                impressiontime,
				a.cvr_nbr,
				b.imp_nbr,
				b.division,
				b.weather_group,
                datediff('day',cast(timestamp_trunc(to_timestamp(a.conversiontime / 1000000),'SS') as date),cast(timestamp_trunc(to_timestamp(b.impressiontime / 1000000),'SS') as date)) as cvr_window_day
from
                (select
                                user_id,
--                                 site_id_dcm,
	                            cvr_nbr,
                                event_time as conversiontime
                from
	                (select *,
		                row_number() over() as cvr_nbr
		                from diap01.mec_us_united_20056.dfa2_activity) as ta

-- 	                                left join
--                                 diap01.mec_us_united_20056.dfa2_placements  as p1
--                                 on ta.placement_id = p1.placement_id
					                where
                                ta.activity_id = 1086066 and
--                                 ta.site_id_dcm = 1853562 and
                                user_id != '0' and
	                                (cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-01-02' and '2018-01-08' or cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-01-12' and '2018-01-31') and
                                advertiser_id <> 0 and
                                (length(isnull (event_sub_type,'')) > 0)
-- 	                                and
-- 		                        regexp_like(p1.placement, '.*_Weather_.*', 'ib')
                ) as a,

                (select
                                user_id,
--                                 t2.site_id_dcm,
								case when t2.reg in ('ak', 'hi') then 'pac'
								when      t2.reg in ('il', 'in', 'mi', 'oh', 'wi') then 'enc'
								when      t2.reg in ('al', 'ky', 'ms', 'tn') then 'esc'
								when      t2.reg in ('nj', 'ny', 'pa') then 'mid'
								when      t2.reg in ('az', 'co', 'id', 'mt', 'nv', 'nm', 'ut', 'wy') then 'mtn'
								when      t2.reg in ('ct', 'me', 'ma', 'nh', 'ri', 'vt') then 'new'
								when      t2.reg in ('ca', 'or', 'wa') then 'pac'
								when      t2.reg in ('de', 'fl', 'ga', 'md', 'nc', 'sc', 'va', 'wv', 'dc') then 'sat'
								when      t2.reg in ('ia', 'ks', 'mn', 'mo', 'ne', 'nd', 'sd') then 'wnc'
								when      t2.reg in ('ar', 'la', 'ok', 'tx') then 'wsc'
								else 'xxx' end  as division,
	                            t2.weather_group as weather_group,
                                event_time as impressiontime,
                imp_nbr
-- 	                            row_number() over() as imp_nbr
                from
	                (
		                select
			                            *,
										case when (length(isnull (ti.state_region, '')) = 0) then lower(w2.state) else lower(ti.state_region) end as reg,
                                        case when w1.trigger_status = 'trigger' then 'test'
	                                         when w1.trigger_status = 'no trigger' then 'control'
		                                     else 'other' end as weather_group,
-- 			                            w1.trigger_status as trigger
			                            row_number() over () as imp_nbr
		                from
			                            diap01.mec_us_united_20056.dfa2_impression as ti

						left join
			                            diap01.mec_us_united_20056.ual_weather_zip_triggers as w1
						on              diap01.mec_us_united_20056.udf_dateToInt(cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000), 'SS') as date)) = w1.date
						and             ti.zip_postal_code = w1.zip

						left join       diap01.mec_us_united_20056.ual_weather_zip_map as w2
						on              ti.zip_postal_code = w2.zip

		                where           user_id != '0' and
                                        ti.site_id_dcm = 1853562 and
                                        country_code = 'US' and
	                                (cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-01-02' and '2018-01-08' or cast(timestamp_trunc(to_timestamp(event_time / 1000000),'SS') as date) between '2018-01-12' and '2018-01-31') and
                                        advertiser_id <> 0 and
                                        (length(isnull (w1.trigger_status, '')) > 0) and
                                        (length(isnull (ti.zip_postal_code, '')) > 0)
--                                         ti.state_region != 'AK' and
--                                         ti.state_region != 'HI'

	                ) as t2

                left join
                                diap01.mec_us_united_20056.dfa2_placements as p1
                                on t2.placement_id = p1.placement_id
                where
		                        regexp_like(p1.placement, '.*_Weather_.*', 'ib')
                ) as b
where
                b.user_id = a.user_id
	                and
                -- make sure RTG Impression is served to user within 30 days of flight search
                datediff('day',cast(timestamp_trunc(to_timestamp(a.conversiontime / 1000000),'SS') as date),cast(timestamp_trunc(to_timestamp(b.impressiontime / 1000000),'SS') as date)) between 0 and 7
-- 	limit 100
);
commit;

-- // =================================================================================================================



select
	cast(timestamp_trunc(to_timestamp(ual_weather_results.conversiontime / 1000000), 'SS') as date) as date,
	division,
	weather_group,
	count(distinct user_id) as users,
	count(distinct cvr_nbr) as leads,
	count(distinct imp_nbr) as imps

	from diap01.mec_us_united_20056.ual_weather_results
		where cvr_window_day between 0 and 7

group by
	cast(timestamp_trunc(to_timestamp(ual_weather_results.conversiontime / 1000000), 'SS') as date),
		division,
	weather_group


-- // =================================================



select
-- 	cast(timestamp_trunc(to_timestamp(ual_weather_results.conversiontime / 1000000), 'SS') as date) as date,
-- 	division,
-- 	weather_group,
	count(distinct user_id) as users,
	count(distinct cvr_nbr) as leads,
	count(distinct imp_nbr) as imps

	from diap01.mec_us_united_20056.ual_weather_results

-- group by
-- 	cast(timestamp_trunc(to_timestamp(ual_weather_results.conversiontime / 1000000), 'SS') as date),
-- 		division,
-- 	weather_group

-- // =================================================================================================================
-- Zip code check

drop table diap01.mec_us_united_20056.ual_weather_zip_triggers

create table diap01.mec_us_united_20056.ual_weather_zip_triggers (
    zip            varchar(10),
    trigger_status varchar(1000),
    region         varchar(3),
    date           int

);

copy diap01.mec_us_united_20056.ual_weather_zip_triggers from local 'C:\Users\matthew.hartwick\Documents\20180222_weather_ziptriggers.csv' with DELIMITER ',' DIRECT commit;
-- // =================================================

select
	date,
	res,
	count(zip_postal_code) as zips

from (
	     select
		     cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000), 'SS') as date)      as date,
		     ti.zip_postal_code,
		     w1.trigger_status                                                               as trigger,
		     case when (length(isnull (w1.trigger_status, '')) = 0) then 'no' else 'yes' end as res
	     from diap01.mec_us_united_20056.dfa2_impression as ti

		     left join diap01.mec_us_united_20056.ual_weather_zip_triggers as w1
		     on diap01.mec_us_united_20056.udf_dateToInt(cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000), 'SS') as date)) = w1.date
			     and ti.zip_postal_code = w1.zip

		     left join
		     diap01.mec_us_united_20056.dfa2_placements as p1
		     on ti.placement_id = p1.placement_id

	     where user_id != '0' and
		     ti.site_id_dcm = 1853562 and
		     country_code = 'US' and
		     cast(timestamp_trunc(to_timestamp(event_time / 1000000), 'SS') as date) between '2018-01-02' and '2018-01-31' and
		     advertiser_id <> 0 and
		     regexp_like(p1.placement, '.*_Weather_.*', 'ib')
	     group by
		     cast(timestamp_trunc(to_timestamp(ti.event_time / 1000000), 'SS') as date),
		     ti.zip_postal_code,
		     w1.trigger_status
     ) as t1

group by
	date,
	res