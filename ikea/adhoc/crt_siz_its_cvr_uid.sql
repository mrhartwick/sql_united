drop table if exists wmprodfeeds.ikea.siz_its_cvr_uid;
create table if not exists wmprodfeeds.ikea.siz_its_cvr_uid
(
	c_id   varchar(128),
	cvr_time timestamp,
	its_con int
);



insert into wmprodfeeds.ikea.siz_its_cvr_uid
(c_id,cvr_time,its_con)

(				select
								cast(userid as varchar(128)) as c_id,
            					winnereventdatedefaulttimezone as cvr_time,
            					sum(1) as its_con

                from
                               wmprodfeeds.ikea.sizmek_conversion_events       as t2
                where             userid != '0'
                and eventyppeid = 1
                -- campaignids in Qualia impression
                and campaignid in (876944, 913795, 913509, 850408, 157710, 914697, 908875, 148109, 912805, 835032, 908018, 858670, 913792, 139540, 939667, 862917, 913772, 955274, 813465, 813500, 162125, 822379, 823989)
			 				 -- ITS Conversions
    			and 			conversiontagid in (593715, 598734, 598735, 598736, 598737, 598738, 598739, 598740, 598741, 598742, 598743, 598744, 598745, 598746, 598747, 598748, 598749, 598750, 598751, 598752, 598753, 598754, 598755, 598756, 598757, 598758, 598759, 598760, 598761, 598762, 598763, 598764, 598765, 598766, 598767, 598768, 598769, 598770, 598771, 598772, 598773, 598774, 612254, 612256, 736352, 930091, 1018765, 1076168, 1084971, 1098595, 1100473, 1174087)
    			and isconversion = TRUE
    			and				winnereventdatedefaulttimezone::date between '2017-09-01' and '2018-12-31'
    			group by cast(userid as varchar(128)), winnereventdatedefaulttimezone

    );