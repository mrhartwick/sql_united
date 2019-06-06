drop table if exists wmprodfeeds.ikea.qualia_visitors_hh;
create table wmprodfeeds.ikea.qualia_visitors_hh (

	impressiondate timestamp encode zstd,
	hh_imp_rank    int encode zstd,
-- 	cc_hh_rank     int encode zstd,
	siz_flag       int encode zstd,
	siz_cid        int encode zstd,
	siz_plc        int encode zstd,
	siz_sid        int encode zstd,
	householdid    varchar(128) encode zstd,
-- 	consumerid     varchar(128) encode zstd,
	store          varchar(400) encode zstd,
	visit_time     timestamp encode zstd distkey,
	hh_vis_rank    int encode zstd,
-- 	cc_vis_rank    int encode zstd,
	days_to_vis    int encode zstd
)
diststyle key
sortkey(visit_time,impressiondate)
;



insert into wmprodfeeds.ikea.qualia_visitors_hh
(impressiondate, hh_imp_rank,
--  cc_hh_rank,
 siz_flag, siz_cid, siz_plc, siz_sid, householdid,
--  consumerid,
 store, visit_time, hh_vis_rank,
--  cc_vis_rank,
 days_to_vis)

(select
t7.impressiondate,
hh_imp_rank,
-- cc_hh_rank,
t7.siz_flag,
t7.siz_cid,
t7.siz_plc,
t7.siz_sid,
t7.householdid,
-- t7.consumerid,
t7.store,
t7.visit_time,
hh_vis_rank,
-- cc_vis_rank,
days_to_vis

from (
select t6.impressiondate,
-- order in which household was hit with impression
dense_rank() over (partition by t6.householdid order by impressiondate asc) as hh_imp_rank,
-- order in which consumer was hit with impression
-- dense_rank() over (partition by t6.consumerid order by impressiondate asc ) 	as cc_hh_rank,
-- order in which consumer was hit with impression, within household
-- dense_rank() over (partition by t6.householdid, t6.consumerid, impressiondate order by impressiondate asc) 		as ch_imp_rank,
t6.siz_flag,
t6.siz_cid,
t6.siz_plc,
t6.siz_sid,
t6.householdid,
-- t6.consumerid,
t6.store,
t6.visit_time,
datediff('day',impressiondate,visit_time) as days_to_vis,
-- order in which household visited store
dense_rank() over (partition by t6.householdid, t6.store_name order by t6.visit_time asc) 		as hh_vis_rank
-- -- order in which consumer visited store
-- dense_rank() over (partition by t6.consumerid, t6.store_name order by t6.visit_time asc) 		as cc_vis_rank
-- -- order in which consumer visited store, within household

		from (
				 select
-- t5.deviceid,
-- t5.siz_uid,
t5.impressiondate,
t5.siz_flag,
t5.siz_cid,
t5.siz_plc,
t5.siz_sid,
t5.householdid,
-- t5.consumerid,
q3.store_name,
split_part(q3.store_name, '-', 2) as store,
q3.timestamp_2                    as visit_time


				 from (
						  select t3.deviceid,
								 t3.impressiondate,
								 t3.siz_flag,
						         t3.siz_cid,
								 t3.siz_plc,
								 t3.siz_sid,
								 t3.householdid,
-- 								 t4.consumerid,
								 t4.platformid
-- 						t4.platformtype

						  from (
								   select t1.deviceid,
										  t1.impressiondate,
								          t1.siz_cid,
								          t1.siz_plc,
								          t1.siz_sid,
										  case when (len(isnull (t1.siz_uid, '')) > 0) then 1
											   else 2 end as siz_flag,
										  t2.householdid
-- 								          t3.cvr_time

							   from wmprodfeeds.ikea.qualia_impressions t1
-- 						  pull householdids associated with impression file
								   left join wmprodfeeds.ikea.qualia_graph t2
											  on t1.deviceid = t2.platformid

-- 								   left join wmprodfeeds.ikea.cvr_uid t3
-- 											  on t1.siz_uid = t3.c_id

								   where t1.isbot = 'false'
								         and t2.platformtype = 'BlueCava Id'
								   and t1.impressiondate::date between '2017-09-01' and '2018-12-31'

								   group by t1.deviceid,
											t1.siz_uid,
								            t1.siz_cid,
											t1.siz_plc,
											t1.siz_sid,
											t1.impressiondate,
											t2.householdid
							   ) t3
-- 			pull in graph data again, this time populating all metadata associated with household ids from first select
								   left join wmprodfeeds.ikea.qualia_graph t4
											 on t3.householdid = t4.householdid
					  ) as t5

						  left join wmprodfeeds.ikea.qualia_location q3
									on t5.platformid = q3.device_id
-- 				 where q3.device_type in ('idfa', 'aaid')
-- 				 and q3.timestamp_2::date between '2017-09-01' and '2018-08-31'

				 group by
-- don't need device/platform IDs now, as we have the individuals they represent
-- t5.deviceid,
t5.impressiondate,
t5.siz_flag,
t5.siz_cid,
t5.siz_plc,
t5.siz_sid,
t5.householdid,
-- t5.consumerid,
q3.store_name,
q3.timestamp_2
-- 				 limit 1000
			 ) t6
	) as t7


	);



-- ===============================================================================================
-- (select
-- t7.impressiondate,
-- hh_imp_rank,
-- -- cc_hh_rank,
-- t7.siz_flag,
-- t7.siz_cid,
-- t7.siz_plc,
-- t7.siz_sid,
-- t7.householdid,
-- -- t7.consumerid,
-- t7.store,
-- t7.visit_time,
-- hh_vis_rank,
-- -- cc_vis_rank,
-- days_to_vis
--
-- from (
-- select t6.impressiondate,
-- -- order in which household was hit with impression
-- dense_rank() over (partition by t6.householdid order by impressiondate asc) as hh_imp_rank,
-- -- order in which consumer was hit with impression
-- -- dense_rank() over (partition by t6.consumerid order by impressiondate asc ) 	as cc_hh_rank,
-- -- order in which consumer was hit with impression, within household
-- -- dense_rank() over (partition by t6.householdid, t6.consumerid, impressiondate order by impressiondate asc) 		as ch_imp_rank,
-- t6.siz_flag,
-- t6.siz_cid,
-- t6.siz_plc,
-- t6.siz_sid,
-- t6.householdid,
-- -- t6.consumerid,
-- t6.store,
-- t6.visit_time,
-- datediff('day',impressiondate,visit_time) as days_to_vis,
-- -- order in which household visited store
-- dense_rank() over (partition by t6.householdid, t6.store_name order by t6.visit_time asc) 		as hh_vis_rank
-- -- -- order in which consumer visited store
-- -- dense_rank() over (partition by t6.consumerid, t6.store_name order by t6.visit_time asc) 		as cc_vis_rank
-- -- -- order in which consumer visited store, within household
--
-- 		from (
-- 				 select
-- -- t5.deviceid,
-- -- t5.siz_uid,
-- t5.impressiondate,
-- t5.siz_flag,
-- t5.siz_cid,
-- t5.siz_plc,
-- t5.siz_sid,
-- t5.householdid,
-- -- t5.consumerid,
-- q3.store_name,
-- split_part(q3.store_name, '-', 2) as store,
-- q3.timestamp_2                    as visit_time
--
--
-- 				 from (
-- 						  select t3.deviceid,
-- 								 t3.impressiondate,
-- 								 t3.siz_flag,
-- 						         t3.siz_cid,
-- 								 t3.siz_plc,
-- 								 t3.siz_sid,
-- 								 t3.householdid,
-- -- 								 t4.consumerid,
-- 								 t4.platformid
-- -- 						t4.platformtype
--
-- from (
-- select 									  t1.deviceid,
-- 										  t1.impressiondate,
-- 								          t1.siz_cid,
-- 								          t1.siz_plc,
-- 								          t1.siz_sid,
-- 										  case when (len(isnull (t1.siz_uid, '')) > 0) then 1
-- 											   else 2 end as siz_flag,
-- 										  case when t99.its_con > 1 then 1
-- 											   else 2 end as its_flag,
-- 										  t2.householdid
--
-- 								   from wmprodfeeds.ikea.qualia_impressions t1
-- -- 						  pull householdids associated with impression file
-- 								   left join wmprodfeeds.ikea.qualia_graph t2
-- 											  on t1.deviceid = t2.platformid
--
-- 								   left join wmprodfeeds.ikea.qualia_its_cvr_uid t99
-- 											  on t1.siz_uid = t99.siz_uid
--
-- 								   where t1.isbot = 'false'
-- 								         and t2.platformtype = 'BlueCava Id'
-- 								   and t1.impressiondate::date between '2017-09-01' and '2018-12-31'
--
-- -- 								   group by t1.deviceid,
-- -- 											t1.siz_uid,
-- -- 								            t1.siz_cid,
-- -- 											t1.siz_plc,
-- -- 											t1.siz_sid,
-- -- 											t1.impressiondate,
-- -- 											t2.householdid,
-- -- 								            t3.its_con
--     ) t3
-- -- 			pull in graph data again, this time populating all metadata associated with household ids from first select
-- 								   left join wmprodfeeds.ikea.qualia_graph t4
-- 											 on t3.householdid = t4.householdid
--
--
-- 					  ) as t5
--
-- 						  left join wmprodfeeds.ikea.qualia_location q3
-- 									on t5.platformid = q3.device_id
-- -- 				 where q3.device_type in ('idfa', 'aaid')
-- -- 				 and q3.timestamp_2::date between '2017-09-01' and '2018-08-31'
--
-- 				 group by
-- -- don't need device/platform IDs now, as we have the individuals they represent
-- -- t5.deviceid,
-- t5.impressiondate,
-- t5.siz_flag,
-- t5.siz_cid,
-- t5.siz_plc,
-- t5.siz_sid,
-- t5.householdid,
-- -- t5.consumerid,
-- q3.store_name,
-- q3.timestamp_2
-- -- 				 limit 1000
-- 			 ) t6
-- 	) as t7
--
--
-- 	);