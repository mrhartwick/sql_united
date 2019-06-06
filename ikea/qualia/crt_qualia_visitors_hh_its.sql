set statement_timeout to 0;
drop table if exists wmprodfeeds.ikea.qualia_visitors_hh_its;
create table wmprodfeeds.ikea.qualia_visitors_hh_its (

-- 	impressiondate timestamp encode zstd,
-- 	hh_imp_rank    int encode zstd,
-- 	cc_hh_rank     int encode zstd,
-- 	siz_flag       int encode zstd,
-- 	siz_cid        int encode zstd,
-- 	siz_plc        int encode zstd,
-- 	siz_sid        int encode zstd,
	householdid    varchar(128) encode zstd,
-- 	consumerid     varchar(128) encode zstd,
	store          varchar(400) encode zstd,
	cvr_time     timestamp encode zstd,
	visit_time     timestamp encode zstd distkey,
-- 	hh_vis_rank    int encode zstd,
-- 	cc_vis_rank    int encode zstd,
-- 	days_to_vis    int encode zstd,
	its_con        bigint encode zstd
)
diststyle key
sortkey(visit_time,cvr_time)
;



insert into wmprodfeeds.ikea.qualia_visitors_hh_its
(
--  impressiondate,
--  hh_imp_rank,
--  cc_hh_rank,
--  siz_flag,
--  siz_cid,
--  siz_plc,
--  siz_sid,
 householdid,
--  consumerid,
 store,
 cvr_time,
 visit_time,
--  hh_vis_rank,
--  cc_vis_rank,
--  days_to_vis,
 its_con
 )

(select
-- t7.impressiondate,
-- hh_imp_rank,
-- t7.siz_flag,
-- t7.siz_cid,
-- t7.siz_plc,
-- t7.siz_sid,
t7.householdid,
t7.store,
t7.cvr_time,
t7.visit_time,
-- hh_vis_rank,
-- days_to_vis,
its_con

from (
select
--        t6.impressiondate,
-- dense_rank() over (partition by t6.householdid order by impressiondate asc) as hh_imp_rank,
-- t6.siz_flag,
-- t6.siz_cid,
-- t6.siz_plc,
-- t6.siz_sid,
t6.cvr_time,
t6.householdid,
t6.store,
t6.visit_time,
t6.its_con
-- datediff('day',impressiondate,visit_time) as days_to_vis,
-- dense_rank() over (partition by t6.householdid, t6.store_name order by t6.visit_time asc) 		as hh_vis_rank

-- select count(*)
		from (
				 select
-- t5.deviceid,
-- t5.siz_uid,
-- t5.impressiondate,
-- t5.siz_flag,
-- t5.siz_cid,
-- t5.siz_plc,
-- t5.siz_sid,
t5.cvr_time,
-- t5.cvr_date,
t5.householdid,
-- t5.consumerid,
q3.store_name,
split_part(q3.store_name, '-', 2) as store,
q3.timestamp_2                    as visit_time,
sum(t5.its_con) as its_con


				 from (
						  select t3.deviceid,
-- 								 t3.impressiondate,
-- 								 t3.siz_flag,
-- 						         t3.siz_cid,
-- 								 t3.siz_plc,
-- 								 t3.siz_sid,
-- 						         t3.siz_uid,
						         t3.cvr_time,
-- 						         t3.cvr_date,
						         t3.its_con,
								 t3.householdid,
-- 								 t4.consumerid,
								 t4.platformid
-- 						t4.platformtype

						  from (
								   select t1.deviceid,
										  t3.cvr_time,
-- 								          t3.cvr_time::date as cvr_date,
-- 								          t1.siz_uid,
-- 								          t1.siz_cid,
-- 								          t1.siz_plc,
-- 								          t1.siz_sid,
-- 										  case when (len(isnull (t1.siz_uid, '')) > 0) then 1
-- 											   else 2 end as siz_flag,
										  sum(t3.its_con) as its_con,
-- 						                  case when t3.its_con > 0 then 1 else 0 end as its_con,
										  t2.householdid
-- 								          t3.cvr_time

								   from wmprodfeeds.ikea.qualia_impressions t1
-- 						           pull householdids associated with impression file
								   left join wmprodfeeds.ikea.qualia_graph t2
											  on t1.deviceid = t2.platformid

								   left join wmprodfeeds.ikea.qualia_its_cvr_uid t3
											  on t1.siz_uid = t3.siz_uid


								   where t1.isbot = 'false'
						           and t2.platformtype = 'BlueCava Id'
								   and t3.cvr_time::date between '2017-09-01' and '2018-12-31'
								   and (len(isnull (t1.siz_uid, '')) > 0)
								   and cast(browserapptohouseholdscore as int) >= 70

								   group by t1.deviceid,
-- 								            t3.cvr_time::date,
-- 											t1.siz_uid,
-- 								            t1.siz_cid,
-- 											t1.siz_plc,
-- 											t1.siz_sid,
								            t3.cvr_time,
-- 											t1.impressiondate,
-- 						                    t3.its_con,
											t2.householdid
							   ) t3
-- 			pull in graph data again, this time populating all metadata associated with household ids from first select
								   left join wmprodfeeds.ikea.qualia_graph t4
											 on t3.householdid = t4.householdid

-- 									where t4.platformtype = 'BlueCava Id'

					  ) as t5



						  left join wmprodfeeds.ikea.qualia_location q3
									on t5.platformid = q3.device_id

-- 				        left join wmprodfeeds.ikea.siz_its_cvr_uid c1
-- 				        on t5.siz_uid = c1.c_id

				 where t5.cvr_time::date <= q3.timestamp_2::date

				 group by
-- don't need device/platform IDs now, as we have the individuals they represent
-- t5.deviceid,
-- t5.impressiondate,
-- t5.siz_flag,
-- t5.siz_cid,
-- t5.siz_plc,
-- t5.siz_sid,
t5.cvr_time,
-- t5.cvr_date,
t5.householdid,
-- t5.consumerid,
q3.store_name,
q3.timestamp_2
-- 				 limit 1000
			 ) t6
	) as t7


	);



select count(distinct householdid)
from wmprodfeeds.ikea.qualia_visitors_hh_its
where cvr_time is not null

-- select count(distinct )

select count(*) from wmprodfeeds.ikea.qualia_visitors_hh_its