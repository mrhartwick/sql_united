drop table if exists wmprodfeeds.ikea.qualia_its_cvr_uid;
create table if not exists wmprodfeeds.ikea.qualia_its_cvr_uid
(
	siz_uid   varchar(128),
	its_con int
);



insert into wmprodfeeds.ikea.qualia_its_cvr_uid
(siz_uid,its_con)

	(
		select siz_uid,
			   sum(t2.its_con) as its_con
		from wmprodfeeds.ikea.qualia_impressions t1

				 left join wmprodfeeds.ikea.siz_its_cvr_uid t2
						   on t1.siz_uid = t2.c_id

		where t2.cvr_time > t1.impressiondate and
			  t1.impressiondate::date between '2017-09-01' and '2018-12-31'
		group by siz_uid
	);