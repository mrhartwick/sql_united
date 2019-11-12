
-- Unlike DFA and Sizmek, AdForm doesn't provide native site -> placement -> creative metadata mapping out of the box.
-- So this does that


select
s1.name as ste_nm,
t1.mediaid,
p1.name as plc_nm,
plc_id,
a1.name as crt_nm,
crt_id

from
(
select
"placementid-activityid" as plc_id,
"bannerid-adgroupid" as crt_id,
mediaid

from AwsDataCatalog.ikea_adform.impression
where yyyymmdd between 20190801 and 20191112
and campaignid in (1772184, 82989, 1779832)
and isrobot = 'No'
group by
"placementid-activityid",
"bannerid-adgroupid",
mediaid
	) t1

left join AwsDataCatalog.ikea_adform.meta_placements_activities as p1
on t1.plc_id = p1.id

left join AwsDataCatalog.ikea_adform.meta_banners_adgroups a1
on t1.crt_id = a1.id

left join AwsDataCatalog.ikea_adform.meta_medias s1
on t1.mediaid = s1.id

group by
s1.name,
t1.mediaid,
p1.name,
plc_id,
a1.name,
crt_id